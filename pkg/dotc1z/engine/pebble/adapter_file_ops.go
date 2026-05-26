package pebble

import (
	"context"
	"errors"

	"github.com/conductorone/baton-sdk/pkg/dotc1z"
)

// FileOps returns the FileOps sub-store backed by the Pebble
// adapter. Implements dotc1z.C1ZStore.FileOps().
//
// Both methods (CloneSync, GenerateSyncDiff) return
// ErrFileOpsUnsupported today — these operations are SQLite-only
// machinery (SQL ATTACH / VACUUM INTO patterns). They aren't on the
// pkg/sync.NewSyncer hot path so a sync end-to-end through Pebble
// works without them; callers that explicitly need cross-sync
// clone or diff (the c1 archive activity, the local differ CLI)
// still route through the SQLite path.
func (a *Adapter) FileOps() dotc1z.FileOps {
	return pebbleFileOps{a: a}
}

// ErrFileOpsUnsupported signals that a FileOps method isn't
// implemented for the Pebble engine yet. Tracker.md captures the
// follow-up.
var ErrFileOpsUnsupported = errors.New("pebble engine: FileOps method not implemented")

type pebbleFileOps struct {
	a *Adapter
}

// CloneSync would materialize the given sync's data into a fresh
// c1z file at outPath. The Pebble equivalent is an IngestAndExcise
// range-scoped to the sync's key prefix; not wired yet — tracker.md
// has the follow-up.
func (f pebbleFileOps) CloneSync(ctx context.Context, outPath string, syncID string) error {
	return ErrFileOpsUnsupported
}

// GenerateSyncDiff would compute the diff between two sync runs in
// this file and emit a new SyncTypePartial sync. The Pebble
// equivalent is a range-pair walk; not wired yet.
func (f pebbleFileOps) GenerateSyncDiff(ctx context.Context, baseSyncID, appliedSyncID string) (string, error) {
	return "", ErrFileOpsUnsupported
}
