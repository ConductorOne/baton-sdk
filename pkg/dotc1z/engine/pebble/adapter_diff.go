package pebble

import (
	"context"
	"errors"
)

// ErrDiffUnsupported is returned by the Pebble v3 engine's
// GenerateSyncDiff. A v3 c1z holds exactly one sync by contract, so the
// precondition GenerateSyncDiff needs — two ended syncs (base + applied)
// co-resident in one file — can never be satisfied. Diffs must be
// computed a layer up, across two separate c1z files.
var ErrDiffUnsupported = errors.New("pebble v3 engine: GenerateSyncDiff is unsupported (single-sync contract)")

// generateSyncDiff is unsupported on the single-sync Pebble engine; see
// ErrDiffUnsupported. The previous additions-only set-difference
// implementation was removed when the keyspace dropped its sync_id
// region (a second sync can no longer coexist with the base).
func generateSyncDiff(_ context.Context, _ *Adapter, _, _ string) (string, error) {
	return "", ErrDiffUnsupported
}
