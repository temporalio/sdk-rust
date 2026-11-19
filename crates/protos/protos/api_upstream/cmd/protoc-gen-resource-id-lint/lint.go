package main

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

const (
	annotationsFile  = "temporal/api/protometa/v1/annotations.proto"
	resourceIDHeader = "temporal-resource-id"
)

const howToFix = `Add to the RPC:
    option (temporal.api.protometa.v1.request_header) = {
        header: "temporal-resource-id"
        value: "<type>:{<field path in the request>}"
    };
If the RPC has no single resource to route by, add it to
cmd/protoc-gen-resource-id-lint/exemptions.go instead.`

func lint(gen *protogen.Plugin) error {
	xt, resolver, err := requestHeaderExtension(gen)
	if err != nil {
		return err
	}

	var problems []string
	seen := make(map[string]bool)
	for _, f := range gen.Files {
		if !f.Generate {
			continue
		}
		for _, svc := range f.Services {
			for _, m := range svc.Methods {
				key := string(svc.Desc.Name()) + "." + string(m.Desc.Name())
				seen[key] = true
				annotated, err := hasResourceID(m.Desc, xt, resolver)
				if err != nil {
					return fmt.Errorf("%s: %w", key, err)
				}
				_, exempt := exemptions[key]
				switch {
				case !annotated && !exempt:
					problems = append(problems, key+": missing temporal-resource-id request_header annotation")
				case annotated && exempt:
					problems = append(problems, key+": has a temporal-resource-id annotation, so remove it from exemptions")
				}
			}
		}
	}
	for _, key := range slices.Sorted(maps.Keys(exemptions)) {
		if !seen[key] {
			problems = append(problems, key+": listed in exemptions but no such RPC exists")
		}
	}

	if len(problems) > 0 {
		return errors.New("temporal-resource-id annotation check failed:\n  " +
			strings.Join(problems, "\n  ") + "\n\n" + howToFix)
	}
	return nil
}

// requestHeaderExtension builds a dynamic type for the request_header extension from the
// descriptors protoc sent, so this plugin does not depend on generated Go code for this repo.
func requestHeaderExtension(gen *protogen.Plugin) (protoreflect.ExtensionType, *protoregistry.Types, error) {
	f, ok := gen.FilesByPath[annotationsFile]
	if !ok {
		return nil, nil, fmt.Errorf("%s is not imported by the files being checked", annotationsFile)
	}
	xd := f.Desc.Extensions().ByName("request_header")
	if xd == nil {
		return nil, nil, fmt.Errorf("extension request_header not found in %s", annotationsFile)
	}
	xt := dynamicpb.NewExtensionType(xd)
	resolver := new(protoregistry.Types)
	if err := resolver.RegisterExtension(xt); err != nil {
		return nil, nil, err
	}
	return xt, resolver, nil
}

// hasResourceID reports whether a method has a temporal-resource-id request_header annotation.
func hasResourceID(
	md protoreflect.MethodDescriptor,
	xt protoreflect.ExtensionType,
	resolver *protoregistry.Types,
) (bool, error) {
	opts, ok := md.Options().(*descriptorpb.MethodOptions)
	if !ok || opts == nil {
		return false, nil
	}
	// The extension is not registered globally, so it arrives as unknown fields. Re-parse the
	// options with a resolver that knows it.
	b, err := proto.Marshal(opts)
	if err != nil {
		return false, err
	}
	parsed := &descriptorpb.MethodOptions{}
	if err := (proto.UnmarshalOptions{Resolver: resolver}).Unmarshal(b, parsed); err != nil {
		return false, err
	}

	list := parsed.ProtoReflect().Get(xt.TypeDescriptor()).List()
	for i := range list.Len() {
		ann := list.Get(i).Message()
		if ann.Get(ann.Descriptor().Fields().ByName("header")).String() == resourceIDHeader {
			return true, nil
		}
	}
	return false, nil
}
