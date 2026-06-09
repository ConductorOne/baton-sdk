package pebble

import (
	"context"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// FileOps returns the FileOps sub-store backed by the Pebble
// adapter. Implements c1zstore.Store.FileOps(). Both methods are
// fully implemented: CloneSync materializes one sync's data into a
// fresh c1z (used by `baton clone`), and GenerateSyncDiff computes
// the additions-only set difference between two ended syncs and
// emits a new SyncTypePartial sync (used by the local differ CLI).
func (a *Adapter) FileOps() c1zstore.FileOps {
	return pebbleFileOps{a: a, encoding: c1zstore.PayloadEncodingTarZstd}
}

// FileOpsWithEncoding returns a FileOps that emits new c1z files
// using the given payload encoding. Used by the registered store in
// pkg/dotc1z to propagate its configured encoding (e.g.
// PayloadEncodingTar for callers that want uncompressed envelopes)
// through to CloneSync's output file.
func (a *Adapter) FileOpsWithEncoding(encoding c1zstore.PayloadEncoding) c1zstore.FileOps {
	return pebbleFileOps{a: a, encoding: encoding}
}

type pebbleFileOps struct {
	a        *Adapter
	encoding c1zstore.PayloadEncoding
}

// CloneSync materializes the given sync's data into a fresh
// Pebble-backed c1z at outPath. See cloneSync for the strategy:
// range-copy every sync-scoped keyspace into a fresh engine,
// checkpoint, and emit a v3 envelope.
func (f pebbleFileOps) CloneSync(ctx context.Context, outPath string, syncID string, opts ...c1zstore.CloneSyncOption) error {
	return cloneSync(ctx, f.a, f.encoding, outPath, syncID, opts...)
}

// GenerateSyncDiff computes the additions-only set difference
// between two ended syncs and emits a new SyncTypePartial sync
// containing them. Matches the SQLite contract in
// pkg/dotc1z/diff.go (no modifications or deletions captured).
// Returns the diff sync's ID.
func (f pebbleFileOps) GenerateSyncDiff(ctx context.Context, baseSyncID, appliedSyncID string) (string, error) {
	return generateSyncDiff(ctx, f.a, baseSyncID, appliedSyncID)
}
