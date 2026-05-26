package pebble

import (
	"context"
	"errors"

	"github.com/conductorone/baton-sdk/pkg/dotc1z"
)

// FileOps returns the FileOps sub-store backed by the Pebble
// adapter. Implements dotc1z.C1ZStore.FileOps().
//
// CloneSync materializes one sync's data into a fresh c1z (used by
// `baton clone`). GenerateSyncDiff returns ErrFileOpsUnsupported on
// Pebble today — the diff walker is separate work tracked in
// tracker.md; the local differ CLI is the only caller and still
// routes through the SQLite path.
func (a *Adapter) FileOps() dotc1z.FileOps {
	return pebbleFileOps{a: a, encoding: dotc1z.PayloadEncodingTarZstd}
}

// FileOpsWithEncoding returns a FileOps that emits new c1z files
// using the given payload encoding. Used by registeredStore to
// propagate its configured encoding (e.g. PayloadEncodingTar for
// callers that want uncompressed envelopes) through to CloneSync's
// output file.
func (a *Adapter) FileOpsWithEncoding(encoding dotc1z.PayloadEncoding) dotc1z.FileOps {
	return pebbleFileOps{a: a, encoding: encoding}
}

// ErrFileOpsUnsupported signals that a FileOps method isn't
// implemented for the Pebble engine yet. Tracker.md captures the
// follow-up.
var ErrFileOpsUnsupported = errors.New("pebble engine: FileOps method not implemented")

type pebbleFileOps struct {
	a        *Adapter
	encoding dotc1z.PayloadEncoding
}

// CloneSync materializes the given sync's data into a fresh
// Pebble-backed c1z at outPath. See cloneSync for the strategy:
// range-copy every sync-scoped keyspace into a fresh engine,
// checkpoint, and emit a v3 envelope.
func (f pebbleFileOps) CloneSync(ctx context.Context, outPath string, syncID string) error {
	return cloneSync(ctx, f.a, f.encoding, outPath, syncID)
}

// GenerateSyncDiff is not implemented for the Pebble engine yet.
// The diff walker between two sync key ranges is separate work
// tracked in tracker.md. The local differ CLI is the only caller
// and still routes through the SQLite path.
func (f pebbleFileOps) GenerateSyncDiff(ctx context.Context, baseSyncID, appliedSyncID string) (string, error) {
	return "", ErrFileOpsUnsupported
}
