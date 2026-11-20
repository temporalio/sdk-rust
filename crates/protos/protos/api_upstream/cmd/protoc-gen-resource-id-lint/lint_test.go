package main

import (
	"slices"
	"strings"
	"testing"

	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/pluginpb"
)

func TestLint(t *testing.T) {
	tests := []struct {
		name         string
		methods      []*descriptorpb.MethodDescriptorProto
		exemptions   []string
		wantProblems []string
	}{
		{
			name: "annotated and exempted RPCs pass",
			methods: []*descriptorpb.MethodDescriptorProto{
				method("DescribeNexusOperationExecution", requestHeader("temporal-resource-id", "nexus-operation:{operation_id}")),
				method("ListNexusOperationExecutions", nil),
			},
			exemptions: []string{"WorkflowService.ListNexusOperationExecutions"},
		},
		{
			name: "RPC without the annotation fails",
			methods: []*descriptorpb.MethodDescriptorProto{
				method("DescribeNexusOperationExecution", requestHeader("temporal-resource-id", "nexus-operation:{operation_id}")),
				method("PollNexusOperationExecution", nil),
			},
			wantProblems: []string{
				"WorkflowService.PollNexusOperationExecution: missing temporal-resource-id request_header annotation",
			},
		},
		{
			name: "annotation for another header does not count",
			methods: []*descriptorpb.MethodDescriptorProto{
				method("StartNexusOperationExecution", requestHeader("temporal-namespace", "{namespace}")),
			},
			wantProblems: []string{
				"WorkflowService.StartNexusOperationExecution: missing temporal-resource-id request_header annotation",
			},
		},
		{
			name: "exempted RPC with the annotation fails",
			methods: []*descriptorpb.MethodDescriptorProto{
				method("DeleteNexusOperationExecution", requestHeader("temporal-resource-id", "nexus-operation:{operation_id}")),
			},
			exemptions: []string{"WorkflowService.DeleteNexusOperationExecution"},
			wantProblems: []string{
				"WorkflowService.DeleteNexusOperationExecution: has a temporal-resource-id annotation, so remove it from exemptions",
			},
		},
		{
			name: "exemption for an RPC that does not exist fails",
			methods: []*descriptorpb.MethodDescriptorProto{
				method("DescribeNexusOperationExecution", requestHeader("temporal-resource-id", "nexus-operation:{operation_id}")),
			},
			exemptions: []string{"WorkflowService.CountNexusOperationExecutions"},
			wantProblems: []string{
				"WorkflowService.CountNexusOperationExecutions: listed in exemptions but no such RPC exists",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			saved := exemptions
			t.Cleanup(func() { exemptions = saved })
			exemptions = make(map[string]struct{})
			for _, key := range tt.exemptions {
				exemptions[key] = struct{}{}
			}

			err := lint(newPlugin(t, tt.methods))

			if len(tt.wantProblems) == 0 {
				if err != nil {
					t.Fatalf("lint() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("lint() = nil, want problems %q", tt.wantProblems)
			}
			if got := problems(err); !slices.Equal(got, tt.wantProblems) {
				t.Fatalf("lint() problems = %q, want %q", got, tt.wantProblems)
			}
		})
	}
}

// problems returns the problem lines of a lint error: the lines between the first line and the
// blank line before the fix instructions.
func problems(err error) []string {
	report, _, _ := strings.Cut(err.Error(), "\n\n")
	lines := strings.Split(report, "\n")
	for i := range lines {
		lines[i] = strings.TrimSpace(lines[i])
	}
	return lines[1:]
}

// newPlugin returns the plugin protoc would run for a WorkflowService with the given methods.
func newPlugin(t *testing.T, methods []*descriptorpb.MethodDescriptorProto) *protogen.Plugin {
	t.Helper()
	service := &descriptorpb.FileDescriptorProto{
		Name:       proto.String("temporal/api/workflowservice/v1/service.proto"),
		Package:    proto.String("temporal.api.workflowservice.v1"),
		Dependency: []string{annotationsFile},
		Syntax:     proto.String("proto3"),
		Options:    &descriptorpb.FileOptions{GoPackage: proto.String("go.temporal.io/api/workflowservice/v1;workflowservice")},
		MessageType: []*descriptorpb.DescriptorProto{
			{Name: proto.String("Request")},
			{Name: proto.String("Response")},
		},
		Service: []*descriptorpb.ServiceDescriptorProto{
			{Name: proto.String("WorkflowService"), Method: methods},
		},
	}
	gen, err := protogen.Options{}.New(&pluginpb.CodeGeneratorRequest{
		FileToGenerate: []string{service.GetName()},
		ProtoFile: []*descriptorpb.FileDescriptorProto{
			protodesc.ToFileDescriptorProto(descriptorpb.File_google_protobuf_descriptor_proto),
			annotationsProto,
			service,
		},
	})
	if err != nil {
		t.Fatalf("protogen.Options.New: %v", err)
	}
	return gen
}

// annotationsProto is temporal/api/protometa/v1/annotations.proto as protoc sends it.
var annotationsProto = &descriptorpb.FileDescriptorProto{
	Name:       proto.String(annotationsFile),
	Package:    proto.String("temporal.api.protometa.v1"),
	Dependency: []string{"google/protobuf/descriptor.proto"},
	Syntax:     proto.String("proto3"),
	Options:    &descriptorpb.FileOptions{GoPackage: proto.String("go.temporal.io/api/protometa/v1;protometa")},
	MessageType: []*descriptorpb.DescriptorProto{{
		Name: proto.String("RequestHeaderAnnotation"),
		Field: []*descriptorpb.FieldDescriptorProto{
			{
				Name:     proto.String("header"),
				JsonName: proto.String("header"),
				Number:   proto.Int32(1),
				Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				Type:     descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
			},
			{
				Name:     proto.String("value"),
				JsonName: proto.String("value"),
				Number:   proto.Int32(2),
				Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				Type:     descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
			},
		},
	}},
	Extension: []*descriptorpb.FieldDescriptorProto{{
		Name:     proto.String("request_header"),
		JsonName: proto.String("requestHeader"),
		Number:   proto.Int32(7234001),
		Label:    descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum(),
		Type:     descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
		TypeName: proto.String(".temporal.api.protometa.v1.RequestHeaderAnnotation"),
		Extendee: proto.String(".google.protobuf.MethodOptions"),
	}},
}

func method(name string, options *descriptorpb.MethodOptions) *descriptorpb.MethodDescriptorProto {
	return &descriptorpb.MethodDescriptorProto{
		Name:       proto.String(name),
		InputType:  proto.String(".temporal.api.workflowservice.v1.Request"),
		OutputType: proto.String(".temporal.api.workflowservice.v1.Response"),
		Options:    options,
	}
}

// requestHeader returns method options with one request_header annotation, encoded the way protoc
// sends an extension the plugin has no Go type for: as unknown fields.
func requestHeader(header, value string) *descriptorpb.MethodOptions {
	var annotation []byte
	annotation = protowire.AppendTag(annotation, 1, protowire.BytesType)
	annotation = protowire.AppendString(annotation, header)
	annotation = protowire.AppendTag(annotation, 2, protowire.BytesType)
	annotation = protowire.AppendString(annotation, value)

	var raw []byte
	raw = protowire.AppendTag(raw, 7234001, protowire.BytesType)
	raw = protowire.AppendBytes(raw, annotation)

	options := &descriptorpb.MethodOptions{}
	options.ProtoReflect().SetUnknown(raw)
	return options
}
