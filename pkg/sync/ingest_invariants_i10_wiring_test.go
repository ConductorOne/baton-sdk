package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// newI10WiringStore opens a Pebble store with one open full sync, the
// shape runIngestionInvariants expects.
func newI10WiringStore(t *testing.T, ctx context.Context) (c1zstore.Store, string) {
	t.Helper()
	tmpDir := t.TempDir()
	store, err := dotc1z.NewStore(ctx, filepath.Join(tmpDir, "i10-wiring.c1z"),
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithTmpDir(tmpDir),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close(ctx) })
	syncID, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	return store, syncID
}

// TestRunIngestionInvariantsI10EvidenceWiring pins how
// runIngestionInvariants hands I10 its evidence.
//
// IngestInvariantsPolicy.undrainedSpawned is documented as "nil skips
// the check" — a caller that never ran the scheduler has no subject.
// Reading s.run.undrainedSpawnedCursors unconditionally cannot express
// that: a bound method value on a nil *runState is itself non-nil, so
// checkSpawnedCursorDrain's nil test passes and the call dereferences
// the receiver at r.mu.RLock().
//
// Sync assigns s.run before the pass runs, so only a hand-assembled
// syncer reaches it. Two literals in ingest_invariant_verification_test.go
// grew run: newRunState() for exactly this reason, which is why the
// behavior is pinned here instead of left to the next literal.
func TestRunIngestionInvariantsI10EvidenceWiring(t *testing.T) {
	ctx := context.Background()

	t.Run("no run state skips I10 rather than panicking", func(t *testing.T) {
		store, syncID := newI10WiringStore(t, ctx)
		s := &syncer{syncID: syncID, cfg: syncConfig{syncType: connectorstore.SyncTypeFull}}
		s.setStore(store)
		require.NoError(t, s.runIngestionInvariants(ctx))
	})

	// The guard must not become a way to switch I10 off: with evidence
	// present, an undrained cursor still blocks the seal.
	t.Run("an undrained cursor still fails I10", func(t *testing.T) {
		store, syncID := newI10WiringStore(t, ctx)
		run := newRunState()
		run.pushAction(ctx, Action{
			Op:             SyncGrantsOp,
			ResourceTypeID: "group",
			PageToken:      "dropped",
			Spawned:        true,
			TypeScoped:     true,
		})
		s := &syncer{
			syncID: syncID,
			run:    run, stats: newRunStats(), graph: newExpansionGraph(),
			cfg: syncConfig{syncType: connectorstore.SyncTypeFull},
		}
		s.setStore(store)
		err := s.runIngestionInvariants(ctx)
		require.Error(t, err)
		require.Contains(t, err.Error(), "I10")
		require.Contains(t, err.Error(), `token="dropped"`)
	})
}
