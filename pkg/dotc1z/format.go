package dotc1z

import (
	"bytes"
	"fmt"
	"io"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// C1ZFormat identifies the on-disk format of a .c1z file. The format byte
// is the first 5 bytes of the file; see ReadHeaderFormat.
type C1ZFormat int

const (
	// C1ZFormatUnknown is the zero value. Returned when the header bytes
	// match neither the v1 nor the v3 magic, or when the read failed.
	C1ZFormatUnknown C1ZFormat = iota

	// C1ZFormatV1 is the original .c1z format: 5-byte magic "C1ZF\x00"
	// followed by a zstd-compressed SQLite database.
	C1ZFormatV1

	// C1ZFormatV3 is the v3 format introduced by the storage-engine-v4
	// RFC: 5-byte magic "C1Z3\x00", a length-prefixed proto manifest,
	// and a zstd-tar payload of a Pebble Checkpoint directory.
	C1ZFormatV3
)

// String returns a stable human-readable name for the format.
func (f C1ZFormat) String() string {
	switch f {
	case C1ZFormatV1:
		return "v1"
	case C1ZFormatV3:
		return "v3"
	default:
		return "unknown"
	}
}

// C1Z3FileHeader is the magic byte sequence for v3 files.
var C1Z3FileHeader = []byte("C1Z3\x00")

// Engine identifies a storage engine implementation. The engine is
// chosen by callers via WithEngine(...) on write; on read, the engine
// is dictated by the file's magic byte and (for v3) the manifest's
// engine field. The type lives in pkg/dotc1z/c1zstore so engine
// packages can name it without importing dotc1z.
type Engine = c1zstore.Engine

const (
	// EngineSQLite is the default engine: the v1 .c1z format backed by
	// a zstd-compressed SQLite database. Connectors use this; backend
	// infra can opt out.
	EngineSQLite = c1zstore.EngineSQLite

	// EnginePebble is the v3 engine: a Pebble LSM wrapped in the v3
	// envelope.
	EnginePebble = c1zstore.EnginePebble

	// PebbleManifestEngine is the engine name recorded in a single-sync
	// Pebble v3 manifest. It deliberately differs from EnginePebble so
	// pre-single-sync readers reject the file at dispatch instead of
	// reading its keys as empty. See c1zstore.PebbleManifestEngine.
	PebbleManifestEngine = c1zstore.PebbleManifestEngine
)

// ErrEngineNotAvailable is returned when a caller requests an engine
// that the binary does not support.
var ErrEngineNotAvailable = fmt.Errorf("dotc1z: engine not available")

// PayloadEncoding selects the v3 envelope payload framing. Only the
// Pebble engine consults this; SQLite engines ignore it. See
// c1zstore.PayloadEncoding.
type PayloadEncoding = c1zstore.PayloadEncoding

const (
	// PayloadEncodingUnspecified is the zero value. Means "use the
	// engine's default" — IndexedZstd for Pebble.
	PayloadEncodingUnspecified = c1zstore.PayloadEncodingUnspecified

	// PayloadEncodingTarZstd is the default Pebble v3 envelope
	// encoding: tar of the Pebble directory, compressed with zstd.
	PayloadEncodingTarZstd = c1zstore.PayloadEncodingTarZstd

	// PayloadEncodingTar is uncompressed tar. Useful when Pebble's
	// L5/L6 SSTs are already zstd-compressed at the engine layer
	// (avoids double-compression CPU), or when the storage target
	// compresses in transit.
	PayloadEncodingTar = c1zstore.PayloadEncodingTar

	// PayloadEncodingIndexedZstd stores each payload file as an
	// independent zstd frame with a self-describing header. Opens
	// decode frames in parallel, and rewrites of a store opened from
	// an indexed file splice unchanged frames verbatim instead of
	// re-compressing them (incremental fold compaction relies on
	// this). Readers older than this encoding reject the file.
	PayloadEncodingIndexedZstd = c1zstore.PayloadEncodingIndexedZstd
)

// ReadHeaderFormat reads the first 5 bytes of reader and returns the
// detected format. On return, the reader is positioned immediately
// after the header bytes. If reader is also an io.Seeker, it is
// rewound to offset 0 before reading.
//
// Returns:
//   - C1ZFormatV1, nil — file starts with "C1ZF\x00".
//   - C1ZFormatV3, nil — file starts with "C1Z3\x00".
//   - C1ZFormatUnknown, ErrInvalidFile — header matched no known magic.
//   - C1ZFormatUnknown, err — underlying read error.
func ReadHeaderFormat(reader io.Reader) (C1ZFormat, error) {
	if rs, ok := reader.(io.Seeker); ok {
		if _, err := rs.Seek(0, io.SeekStart); err != nil {
			return C1ZFormatUnknown, err
		}
	}

	headerBytes := make([]byte, len(C1ZFileHeader))
	if _, err := io.ReadFull(reader, headerBytes); err != nil {
		return C1ZFormatUnknown, err
	}

	switch {
	case bytes.Equal(headerBytes, C1ZFileHeader):
		return C1ZFormatV1, nil
	case bytes.Equal(headerBytes, C1Z3FileHeader):
		return C1ZFormatV3, nil
	default:
		return C1ZFormatUnknown, ErrInvalidFile
	}
}
