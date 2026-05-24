//go:build batonsdkv2

package codec

import (
	"fmt"

	"github.com/cockroachdb/pebble"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// ReflectCodec encodes records via cached descriptor reflection. It
// satisfies the Codec interface for any message — including messages
// that don't carry the (storage.v3.table) option (in which case key
// encoding returns ErrReflectMissingTable).
//
// ReflectCodec is constructed lazily by Lookup() and cached
// process-wide. The construction cost — resolving primary-key field
// paths and index declarations — is paid once per descriptor across
// the entire process; subsequent calls reuse the cached codec.
//
// Performance: ~5× slower than a generated typed codec at the same
// workload (measured in `/tmp/baton-rfc-microtests/codec_perf_test.go`
// — see `research/import-13.md` in the pebble-baton-sdk plan). Used
// only off the engine's hot write path.
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

// EncodeKey is a placeholder — the full implementation resolves
// the (storage.v3.table).primary_key field paths against the
// descriptor and tuple-encodes each component. For Stack 1's MVP
// scope this returns ErrReflectMissingTable; Stack 3 wires up the
// full implementation once the engine layer needs reflection-based
// key encoding for non-built-in descriptors.
func (c *ReflectCodec) EncodeKey(msg proto.Message) ([]byte, error) {
	if msg.ProtoReflect().Descriptor().FullName() != c.md.FullName() {
		return nil, fmt.Errorf("%w: want %s, got %s",
			ErrCodecTypeMismatch, c.md.FullName(), msg.ProtoReflect().Descriptor().FullName())
	}
	// TODO(Stack 3): walk (storage.v3.table).primary_key paths and
	// emit tuple bytes for each. For now reflection codecs are
	// value-only.
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

func (c *ReflectCodec) WriteIndexes(batch *pebble.Batch, msg proto.Message) error {
	// TODO(Stack 3): walk fields for (storage.v3.index) options and
	// emit index entries.
	return ErrReflectMissingTable
}

func (c *ReflectCodec) DeleteIndexes(batch *pebble.Batch, msg proto.Message) error {
	return ErrReflectMissingTable
}
