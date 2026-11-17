// protoc-gen-resource-id-lint is a protoc plugin that checks every RPC in the given proto files
// has a temporal-resource-id request_header annotation (see
// temporal/api/protometa/v1/annotations.proto), or is listed in exemptions.go.
// It writes no files and fails the protoc run when a check fails.
package main

import (
	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/types/pluginpb"
)

func main() {
	protogen.Options{}.Run(func(gen *protogen.Plugin) error {
		gen.SupportedFeatures = uint64(pluginpb.CodeGeneratorResponse_FEATURE_PROTO3_OPTIONAL)
		return lint(gen)
	})
}
