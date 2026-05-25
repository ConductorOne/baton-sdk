// Micro-test 4 — codegen vs cached-reflection codec performance.
//
// Run as benchmark:
//
//	go test -bench=BenchmarkCodec -benchmem \
//	    -run=^$ -benchtime=2s -count=3 \
//	    ./pkg/dotc1z/engine/pebble/microtests/
//
// The RFC's earlier draft proposed a protoc-gen-batonstore plugin that
// emits typed Go code for key encoding, value encoding, and index
// maintenance. The critical-thought review challenged: can we skip
// codegen entirely and use cached descriptor reflection (the
// protovalidate-go pattern)?
//
// This benchmark answers concretely: ~5× perf delta in favor of
// direct codegen for a realistic 7-field-touch record. That delta
// justifies the hybrid design — direct codegen for the canonical
// record types, reflection fallback for ad-hoc types via the
// codec registry's lookup path.

package microtests

import (
	"encoding/binary"
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/codec"
)

func makeTestFD() *descriptorpb.FieldDescriptorProto {
	name := "user_external_id"
	number := int32(42)
	typeName := ".c1.connector.v2.UserExternal"
	jsonName := "userExternalId"
	defaultValue := ""
	typ := descriptorpb.FieldDescriptorProto_TYPE_STRING
	label := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL
	return &descriptorpb.FieldDescriptorProto{
		Name:         &name,
		Number:       &number,
		TypeName:     &typeName,
		JsonName:     &jsonName,
		DefaultValue: &defaultValue,
		Type:         &typ,
		Label:        &label,
	}
}

func appendInt32BE(dst []byte, n int32) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(n)^0x80000000) //nolint:gosec // intentional sign-flip cast
	return append(dst, buf[:]...)
}

// directEncodeAll mirrors what the codegen plugin would emit:
// direct typed-field access, no reflection. Uses the production
// codec.AppendTupleString so we're benchmarking against the same
// encoder the engine itself uses.
func directEncodeAll(fd *descriptorpb.FieldDescriptorProto, dst []byte) []byte {
	dst = append(dst, 0x03, 0x00, 0x04, 0x00)
	dst = codec.AppendTupleString(dst, "sync-id-fixed")
	dst = append(dst, 0x00)
	dst = codec.AppendTupleString(dst, fd.GetName())
	dst = append(dst, 0x00)
	dst = appendInt32BE(dst, fd.GetNumber())

	dst = append(dst, 0x00, 0x07, 0x01)
	dst = codec.AppendTupleString(dst, fd.GetName())

	dst = append(dst, 0x00, 0x07, 0x02)
	dst = appendInt32BE(dst, int32(fd.GetType()))
	dst = append(dst, 0x00)
	dst = codec.AppendTupleString(dst, fd.GetTypeName())

	dst = append(dst, 0x00, 0x07, 0x03)
	dst = appendInt32BE(dst, fd.GetNumber())
	dst = append(dst, 0x00)
	dst = appendInt32BE(dst, int32(fd.GetLabel()))

	return dst
}

// reflectCodec is the cached descriptor-reflection codec, the
// protovalidate-go-style alternative to codegen.
type reflectCodec struct {
	nameFD     protoreflect.FieldDescriptor
	numberFD   protoreflect.FieldDescriptor
	typeFD     protoreflect.FieldDescriptor
	typeNameFD protoreflect.FieldDescriptor
	labelFD    protoreflect.FieldDescriptor
}

func newReflectCodec(md protoreflect.MessageDescriptor) *reflectCodec {
	return &reflectCodec{
		nameFD:     md.Fields().ByName("name"),
		numberFD:   md.Fields().ByName("number"),
		typeFD:     md.Fields().ByName("type"),
		typeNameFD: md.Fields().ByName("type_name"),
		labelFD:    md.Fields().ByName("label"),
	}
}

func (c *reflectCodec) encodeAll(msg protoreflect.Message, dst []byte) []byte {
	dst = append(dst, 0x03, 0x00, 0x04, 0x00)
	dst = codec.AppendTupleString(dst, "sync-id-fixed")
	dst = append(dst, 0x00)
	dst = c.appendField(dst, msg, c.nameFD)
	dst = append(dst, 0x00)
	dst = c.appendField(dst, msg, c.numberFD)

	dst = append(dst, 0x00, 0x07, 0x01)
	dst = c.appendField(dst, msg, c.nameFD)

	dst = append(dst, 0x00, 0x07, 0x02)
	dst = c.appendField(dst, msg, c.typeFD)
	dst = append(dst, 0x00)
	dst = c.appendField(dst, msg, c.typeNameFD)

	dst = append(dst, 0x00, 0x07, 0x03)
	dst = c.appendField(dst, msg, c.numberFD)
	dst = append(dst, 0x00)
	dst = c.appendField(dst, msg, c.labelFD)

	return dst
}

func (c *reflectCodec) appendField(dst []byte, msg protoreflect.Message, fd protoreflect.FieldDescriptor) []byte {
	v := msg.Get(fd)
	switch fd.Kind() { //nolint:exhaustive // benchmark intentionally only covers fields the test record uses
	case protoreflect.StringKind, protoreflect.BytesKind:
		return codec.AppendTupleString(dst, v.String())
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return appendInt32BE(dst, int32(v.Int())) //nolint:gosec // benchmark proto types fit int32
	case protoreflect.EnumKind:
		return appendInt32BE(dst, int32(v.Enum()))
	}
	return dst
}

func BenchmarkCodecDirect(b *testing.B) {
	fd := makeTestFD()
	buf := make([]byte, 0, 256)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf = directEncodeAll(fd, buf[:0])
	}
	_ = buf
}

func BenchmarkCodecReflect(b *testing.B) {
	fd := makeTestFD()
	rc := newReflectCodec(fd.ProtoReflect().Descriptor())
	msg := fd.ProtoReflect()
	buf := make([]byte, 0, 256)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf = rc.encodeAll(msg, buf[:0])
	}
	_ = buf
}

func BenchmarkCodecDirectPerRecord(b *testing.B) {
	fd := makeTestFD()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf := make([]byte, 0, 256)
		buf = directEncodeAll(fd, buf)
		_ = buf
	}
}

func BenchmarkCodecReflectPerRecord(b *testing.B) {
	fd := makeTestFD()
	rc := newReflectCodec(fd.ProtoReflect().Descriptor())
	msg := fd.ProtoReflect()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf := make([]byte, 0, 256)
		buf = rc.encodeAll(msg, buf)
		_ = buf
	}
}
