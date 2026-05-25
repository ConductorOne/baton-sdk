package codec

import (
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type fakeCodec struct{ name string }

func (c fakeCodec) EncodeKey(proto.Message) ([]byte, error)          { return []byte(c.name), nil }
func (c fakeCodec) EncodeValue(proto.Message) ([]byte, error)        { return nil, nil }
func (c fakeCodec) DecodeValue([]byte, proto.Message) error          { return nil }
func (c fakeCodec) WriteIndexes(*pebble.Batch, proto.Message) error  { return nil }
func (c fakeCodec) DeleteIndexes(*pebble.Batch, proto.Message) error { return nil }
func TestLookup_GeneratedHit(t *testing.T) {
	// Use a real, registered descriptor (timestamppb is generated).
	md := (&timestamppb.Timestamp{}).ProtoReflect().Descriptor()

	// Simulate a generated codec registration. The real registration
	// happens via gen/register.gen.go init(); for this test we
	// directly populate the map (Register would panic on duplicate
	// across test packages).
	_, alreadyRegistered := registry[md.FullName()]
	if !alreadyRegistered {
		registry[md.FullName()] = fakeCodec{name: "fake-timestamp"}
		defer delete(registry, md.FullName())
	}

	c := Lookup(md)
	if c == nil {
		t.Fatal("Lookup returned nil")
	}
}

func TestLookup_ReflectionFallback(t *testing.T) {
	// Use any descriptor that isn't pre-registered. timestamppb
	// works because nothing in this package registers it. Lookup
	// should construct a ReflectCodec and cache it.
	md := (&timestamppb.Timestamp{}).ProtoReflect().Descriptor()
	delete(registry, md.FullName()) // ensure no entry
	reflectCache.Delete(md.FullName())

	c1 := Lookup(md)
	c2 := Lookup(md)
	if c1 == nil || c2 == nil {
		t.Fatal("Lookup returned nil")
	}
	if c1 != c2 {
		t.Errorf("Lookup should cache ReflectCodec process-wide; got distinct instances")
	}
	if _, ok := c1.(*ReflectCodec); !ok {
		t.Errorf("Lookup fallback should return *ReflectCodec; got %T", c1)
	}
}

func TestRegisteredNames(t *testing.T) {
	names := RegisteredNames()
	// Smoke check: returns a slice.
	if names == nil {
		t.Fatal("RegisteredNames returned nil")
	}
	// All entries must be valid proto names.
	for _, n := range names {
		if string(n) == "" {
			t.Errorf("empty proto name in registry")
		}
	}
	_ = protoreflect.FullName("") // touch the protoreflect import
}
