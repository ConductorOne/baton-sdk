package pebble

import (
	"context"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// FileOps returns the FileOps sub-store backed by the Pebble
// adapter. Implements c1zstore.Store.FileOps(). CloneSync materializes
// the single sync's data into a fresh c1z (used by `baton clone`);
// GenerateSyncDiff is unsupported (single-sync contract — see
// ErrDiffUnsupported).
func (e *Engine) FileOps() c1zstore.FileOps {
	return pebbleFileOps{e: e, encoding: c1zstore.PayloadEncodingTarZstd}
}

// FileOpsWithEncoding returns a FileOps that emits new c1z files
// using the given payload encoding. Used by the registered store in
// pkg/dotc1z to propagate its configured encoding (e.g.
// PayloadEncodingTar for callers that want uncompressed envelopes)
// through to CloneSync's output file.
func (e *Engine) FileOpsWithEncoding(encoding c1zstore.PayloadEncoding) c1zstore.FileOps {
	return pebbleFileOps{e: e, encoding: encoding}
}

type pebbleFileOps struct {
	e        *Engine
	encoding c1zstore.PayloadEncoding
}

// CloneSync materializes the given sync's data into a fresh
// Pebble-backed c1z at outPath. See cloneSync for the strategy:
// range-copy every sync-scoped keyspace into a fresh engine,
// checkpoint, and emit a v3 envelope.
func (f pebbleFileOps) CloneSync(ctx context.Context, outPath string, syncID string, opts ...c1zstore.CloneSyncOption) error {
	return cloneSync(ctx, f.e, f.encoding, outPath, syncID, opts...)
}

// CopyIsolateSync falls back to cloneSync for the Pebble engine. The
// copy-isolation optimization is SQLite-specific (it copies the SQLite working
// file and deletes other syncs); the Pebble engine already materializes only
// the target sync's keyspace into a fresh engine, so it has no whale-rebuild
// cost to avoid.
func (f pebbleFileOps) CopyIsolateSync(ctx context.Context, outPath string, syncID string, opts ...c1zstore.CloneSyncOption) error {
	return cloneSync(ctx, f.e, f.encoding, outPath, syncID, opts...)
}

// GenerateSyncDiff is unsupported on the Pebble v3 engine — a c1z
// holds exactly one sync by contract, so base + applied syncs can't be
// co-resident in one file. Always returns ErrDiffUnsupported.
func (f pebbleFileOps) GenerateSyncDiff(ctx context.Context, baseSyncID, appliedSyncID string) (string, error) {
	return generateSyncDiff(ctx, f.e, baseSyncID, appliedSyncID)
}
