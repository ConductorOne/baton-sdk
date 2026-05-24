//go:build batonsdkv2

package v3

import (
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

// Test helpers — small constructors so envelope_test.go stays readable.

type fileDescriptorProtoForTest struct {
	Name       string
	Dependency []string
}

func newFileDescriptorSetForTest(files ...*fileDescriptorProtoForTest) *descriptorpb.FileDescriptorSet {
	out := &descriptorpb.FileDescriptorSet{}
	for _, f := range files {
		out.File = append(out.File, &descriptorpb.FileDescriptorProto{
			Name:       proto.String(f.Name),
			Dependency: f.Dependency,
		})
	}
	return out
}
