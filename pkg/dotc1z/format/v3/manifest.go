package v3

import (
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"

	c1zv3 "github.com/conductorone/baton-sdk/pb/c1/c1z/v3"
)

// Sentinel errors for manifest operations. These are duplicated from
// pkg/dotc1z/engine/pebble's sentinel set because importing the engine
// package from format/v3 would create a cycle (engine imports format/v3
// for the save protocol). The two sentinels are kept structurally
// equivalent; callers can use errors.Is against either.
var (
	ErrManifestInvalid               = errors.New("c1z v3: manifest unmarshal failed")
	ErrManifestIncompleteDescriptors = errors.New("c1z v3: manifest descriptor closure incomplete")
	ErrManifestEmptyEngine           = errors.New("c1z v3: manifest engine field is empty")
)

// BuildDescriptorClosure walks every c1.storage.v3 file currently
// registered in protoregistry.GlobalFiles and returns a transitively-
// closed FileDescriptorSet containing them plus every file they
// transitively import. The result is what gets pinned into a v3
// manifest's `descriptors` field at save time.
//
// The closure invariant: for every file F in the result, every file F
// imports is also in the result. Reader-side verification can detect
// any missing import and return ErrManifestIncompleteDescriptors.
func BuildDescriptorClosure() (*descriptorpb.FileDescriptorSet, error) {
	// Collect all files whose package is c1.storage.v3 OR which any
	// such file transitively imports.
	seen := map[string]protoreflect.FileDescriptor{}
	var visit func(fd protoreflect.FileDescriptor)
	visit = func(fd protoreflect.FileDescriptor) {
		if _, ok := seen[fd.Path()]; ok {
			return
		}
		seen[fd.Path()] = fd
		imports := fd.Imports()
		for i := 0; i < imports.Len(); i++ {
			visit(imports.Get(i).FileDescriptor)
		}
	}

	protoregistry.GlobalFiles.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		if fd.Package() == "c1.storage.v3" {
			visit(fd)
		}
		return true
	})

	set := &descriptorpb.FileDescriptorSet{
		File: make([]*descriptorpb.FileDescriptorProto, 0, len(seen)),
	}
	for _, fd := range seen {
		set.File = append(set.File, protodesc.ToFileDescriptorProto(fd))
	}
	return set, nil
}

// VerifyDescriptorClosure checks that every file referenced by every
// other file in the set is itself in the set. Returns
// ErrManifestIncompleteDescriptors on the first missing import.
func VerifyDescriptorClosure(set *descriptorpb.FileDescriptorSet) error {
	if set == nil {
		return ErrManifestIncompleteDescriptors
	}
	have := make(map[string]bool, len(set.File))
	for _, f := range set.File {
		have[f.GetName()] = true
	}
	for _, f := range set.File {
		for _, dep := range f.GetDependency() {
			if !have[dep] {
				return fmt.Errorf("%w: file %s depends on %s, which is not in the set",
					ErrManifestIncompleteDescriptors, f.GetName(), dep)
			}
		}
	}
	return nil
}

// MarshalManifest serializes m to deterministic proto bytes.
func MarshalManifest(m *c1zv3.C1ZManifestV3) ([]byte, error) {
	if m.GetEngine() == "" {
		return nil, ErrManifestEmptyEngine
	}
	return proto.MarshalOptions{Deterministic: true}.Marshal(m)
}

// UnmarshalManifest parses bytes into a fresh C1ZManifestV3.
func UnmarshalManifest(b []byte) (*c1zv3.C1ZManifestV3, error) {
	m := &c1zv3.C1ZManifestV3{}
	if err := proto.Unmarshal(b, m); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrManifestInvalid, err)
	}
	if m.GetEngine() == "" {
		return nil, ErrManifestEmptyEngine
	}
	return m, nil
}
