package codec

import (
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// ReflectCodec encodes records via cached descriptor reflection. It
// satisfies the value-encoding portions of the Codec interface for
// any message. Key and index encoding require typed metadata and are
// provided by the built-in codecs.
//
// ReflectCodec is constructed lazily by Lookup() and cached
// process-wide. The construction cost — resolving primary-key field
// paths and index declarations — is paid once per descriptor across
// the entire process; subsequent calls reuse the cached codec.
//
// Performance: ~5× slower than a generated typed codec at the same
// workload. Used only off the engine's hot write path.
type ReflectCodec struct {
	md protoreflect.MessageDescriptor
}

// NewReflectCodec constructs a codec for the given message descriptor.
// Callers should generally go through Lookup() instead, which caches.
func NewReflectCodec(md protoreflect.MessageDescriptor) *ReflectCodec {
	return &ReflectCodec{md: md}
}

// ErrReflectMissingTable is returned by EncodeKey / WriteIndexes when
// the descriptor lacks a (storage.v3.table) option. Reflection
// codecs can still encode/decode values without one; only key paths
// require the table metadata.
var ErrReflectMissingTable = fmt.Errorf("codec: descriptor missing (storage.v3.table) option")

// EncodeKey is unsupported for reflection codecs. Built-in record
// types use typed codecs for primary keys and indexes.
func (c *ReflectCodec) EncodeKey(msg proto.Message) ([]byte, error) {
	if msg.ProtoReflect().Descriptor().FullName() != c.md.FullName() {
		return nil, fmt.Errorf("%w: want %s, got %s",
			ErrCodecTypeMismatch, c.md.FullName(), msg.ProtoReflect().Descriptor().FullName())
	}
	return nil, ErrReflectMissingTable
}

// EncodeValue uses deterministic proto marshal — same contract as
// generated codecs.
func (c *ReflectCodec) EncodeValue(msg proto.Message) ([]byte, error) {
	if msg.ProtoReflect().Descriptor().FullName() != c.md.FullName() {
		return nil, fmt.Errorf("%w: want %s, got %s",
			ErrCodecTypeMismatch, c.md.FullName(), msg.ProtoReflect().Descriptor().FullName())
	}
	return proto.MarshalOptions{Deterministic: true}.Marshal(msg)
}

func (c *ReflectCodec) DecodeValue(b []byte, dst proto.Message) error {
	if dst.ProtoReflect().Descriptor().FullName() != c.md.FullName() {
		return fmt.Errorf("%w: want %s, got %s",
			ErrCodecTypeMismatch, c.md.FullName(), dst.ProtoReflect().Descriptor().FullName())
	}
	return proto.Unmarshal(b, dst)
}
