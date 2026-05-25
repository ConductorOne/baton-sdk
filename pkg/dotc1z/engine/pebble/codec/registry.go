package codec

import (
	"sync"

	"github.com/cockroachdb/pebble/v2"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Codec is the per-record-type interface every storage codec implements.
// Methods return errors on type-assertion mismatch (ErrCodecTypeMismatch)
// rather than panicking; the engine write path plumbs errors and
// surfaces a DataLoss-class gRPC code to upstream callers.
//
// Generated codecs hold a private type-assertion at the entry point;
// reflection codecs evaluate against the descriptor at call time.
type Codec interface {
	// EncodeKey returns the primary-key bytes for the message.
	// The bytes are tuple-encoded per the codec's record-type
	// declaration. Returns ErrCodecTypeMismatch if msg is not the
	// type the codec was registered for.
	EncodeKey(msg proto.Message) ([]byte, error)

	// EncodeValue returns deterministic proto wire bytes for the
	// message. Generated codecs use proto.MarshalOptions{Deterministic:
	// true} so two equal records produce equal bytes — required for
	// the equivalence harness's byte-equality assertion.
	EncodeValue(msg proto.Message) ([]byte, error)

	// DecodeValue parses bytes into dst. dst must be the same type
	// as the registered codec; mismatches return ErrCodecTypeMismatch.
	DecodeValue(b []byte, dst proto.Message) error

	// WriteIndexes appends all secondary-index entries for msg to
	// batch. Called inside a pebble.Batch alongside the primary write.
	WriteIndexes(batch *pebble.Batch, msg proto.Message) error

	// DeleteIndexes appends index-entry deletions for msg to batch.
	// Called during overwrite (after reading the previous value) and
	// during explicit Delete. Same atomicity as WriteIndexes.
	DeleteIndexes(batch *pebble.Batch, msg proto.Message) error
}

// registry holds the codecs registered by generated init() functions.
// It is frozen after package init — populated via Register() and
// thereafter read-only. A plain map with no lock is the fastest
// hot-path lookup (sync.Map's contention-tolerant design is wasted
// here, since the map is write-once).
//
// The frozen-after-init contract: Register MUST be called only from
// generated register.gen.go init() functions. Calling Register at
// runtime is a programming error and will be caught by the duplicate
// check.
var registry = map[protoreflect.FullName]Codec{}

// reflectCache is the lazy descriptor-reflection cache. Populated on
// first Lookup miss; entries live for the lifetime of the process
// (descriptors don't change). Concurrent Lookups race on construction
// but the result is deterministic, so race resolution is "last writer
// wins" (LoadOrStore ensures only one wins).
var reflectCache sync.Map // map[protoreflect.FullName]*ReflectCodec

// Register installs a codec under its proto full-name. Called only
// from generated init() functions. Panics on duplicate registration
// — that's a build error, not a runtime concern.
func Register(name protoreflect.FullName, c Codec) {
	if _, exists := registry[name]; exists {
		panic("codec: duplicate registration for " + string(name))
	}
	registry[name] = c
}

// Lookup returns the codec for the given message descriptor. If a
// generated codec is registered, returns it (hot path, lock-free read
// from a frozen map). Otherwise constructs a ReflectCodec lazily,
// caches it in reflectCache, and returns it.
//
// Lookup never returns nil and never returns an error; an unknown
// descriptor produces a working reflection codec. Whether that codec
// can actually encode keys depends on whether the descriptor has the
// required (storage.v3.table) option — ReflectCodec's methods return
// errors at call time if the descriptor lacks the necessary metadata.
func Lookup(md protoreflect.MessageDescriptor) Codec {
	name := md.FullName()
	if c, ok := registry[name]; ok {
		return c
	}
	if v, ok := reflectCache.Load(name); ok {
		return v.(*ReflectCodec)
	}
	c := NewReflectCodec(md)
	actual, _ := reflectCache.LoadOrStore(name, c)
	return actual.(*ReflectCodec)
}

// RegisteredNames returns the proto full-names of all generated
// codecs registered in the binary. Intended for test introspection
// and for the manifest's RecordTypeInfo population.
func RegisteredNames() []protoreflect.FullName {
	names := make([]protoreflect.FullName, 0, len(registry))
	for n := range registry {
		names = append(names, n)
	}
	return names
}
