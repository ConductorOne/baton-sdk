package c1zstore

import "fmt"

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
// Pebble engine consults this; SQLite engines ignore it. The wire
// numbers match the matching proto enum values in
// c1.c1z.v3.PayloadEncoding.
type PayloadEncoding int

const (
	// PayloadEncodingUnspecified is the zero value. Means "use the
	// engine's default" — TarZstd for Pebble.
	PayloadEncodingUnspecified PayloadEncoding = 0

	// 1 and 2 are reserved in the proto (formerly RAW + single-stream
	// ZSTD). Don't reuse.

	// PayloadEncodingTarZstd is the default Pebble v3 envelope
	// encoding: tar of the Pebble directory, compressed with zstd.
	PayloadEncodingTarZstd PayloadEncoding = 3

	// PayloadEncodingTar is uncompressed tar. Useful when Pebble's
	// L5/L6 SSTs are already zstd-compressed at the engine layer
	// (avoids double-compression CPU), or when the storage target
	// compresses in transit.
	PayloadEncodingTar PayloadEncoding = 4
)

// String returns a stable human-readable name for the encoding.
func (e PayloadEncoding) String() string {
	switch e {
	case PayloadEncodingTarZstd:
		return "tar_zstd"
	case PayloadEncodingTar:
		return "tar"
	case PayloadEncodingUnspecified:
		return "unspecified"
	default:
		return fmt.Sprintf("PayloadEncoding(%d)", int(e))
	}
}
