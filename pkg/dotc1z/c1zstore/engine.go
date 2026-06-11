package c1zstore

import (
	"fmt"

	c1zv3 "github.com/conductorone/baton-sdk/pb/c1/c1z/v3"
)

// Engine identifies a storage engine implementation. The engine is
// chosen by callers via dotc1z.WithEngine(...) on write; on read, the
// engine is dictated by the file's magic byte and (for v3) the
// manifest's engine field.
type Engine string

const (
	// EngineSQLite is the default engine: the v1 .c1z format backed by
	// a zstd-compressed SQLite database. Connectors use this; backend
	// infra can opt out.
	EngineSQLite Engine = "sqlite"

	// EnginePebble is the v3 engine: a Pebble LSM wrapped in the v3
	// envelope.
	EnginePebble Engine = "pebble"
)

// PayloadEncoding selects the v3 envelope payload framing. Only the
// Pebble engine consults this; SQLite engines ignore it. Every value
// is DERIVED from the matching c1.c1z.v3.PayloadEncoding proto enum
// value, so the Go constants cannot drift from the wire format.
type PayloadEncoding int

const (
	// PayloadEncodingUnspecified is the zero value. Means "use the
	// engine's default" — IndexedZstd for Pebble.
	PayloadEncodingUnspecified = PayloadEncoding(c1zv3.PayloadEncoding_PAYLOAD_ENCODING_UNSPECIFIED)

	// PayloadEncodingTarZstd is the classic Pebble v3 envelope
	// encoding: tar of the Pebble directory, compressed with zstd.
	PayloadEncodingTarZstd = PayloadEncoding(c1zv3.PayloadEncoding_PAYLOAD_ENCODING_TAR_ZSTD)

	// PayloadEncodingTar is uncompressed tar. Useful when Pebble's
	// L5/L6 SSTs are already zstd-compressed at the engine layer
	// (avoids double-compression CPU), or when the storage target
	// compresses in transit.
	PayloadEncodingTar = PayloadEncoding(c1zv3.PayloadEncoding_PAYLOAD_ENCODING_TAR)

	// PayloadEncodingIndexedZstd stores each payload file as an
	// independent zstd frame, indexed by a trailing frame table
	// (byte ranges, sizes, SHA-256 identities) for parallel decode,
	// frame splicing, and ranged/chunked object storage.
	PayloadEncodingIndexedZstd = PayloadEncoding(c1zv3.PayloadEncoding_PAYLOAD_ENCODING_INDEXED_ZSTD)
)

// String returns a stable human-readable name for the encoding.
func (e PayloadEncoding) String() string {
	switch e {
	case PayloadEncodingTarZstd:
		return "tar_zstd"
	case PayloadEncodingTar:
		return "tar"
	case PayloadEncodingIndexedZstd:
		return "indexed_zstd"
	case PayloadEncodingUnspecified:
		return "unspecified"
	default:
		return fmt.Sprintf("PayloadEncoding(%d)", int(e))
	}
}
