package synccompactor

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	enginepkg "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
)

// stampSyncStats writes timing stats onto the (finished) sync's stats
// sidecar so the input mimics a real sync whose seal recorded them —
// the sidecar is where every sync's timings live; ledgered syncs write
// no token at all.
func stampSyncStats(t *testing.T, ctx context.Context, path, syncID string, grantStepMs, callTotalMs, callMaxMs, callCount int64) {
	t.Helper()
	w, err := dotc1z.NewStore(ctx, path, dotc1z.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	eng, ok := enginepkg.AsEngine(w)
	require.True(t, ok, "store at %s is not a pebble engine", path)
	rec, err := enginepkg.ReadSyncStatsRecord(ctx, eng, syncID)
	require.NoError(t, err)
	require.NotNil(t, rec, "sealed sync must have a stats sidecar")
	rec.SetStepDurationsMs(map[string]int64{"list-grants": grantStepMs})
	rec.SetConnectorCallStats(map[string]*v3.CallStat{
		"list-grants": v3.CallStat_builder{Count: callCount, TotalMs: callTotalMs, MaxMs: callMaxMs}.Build(),
	})
	require.NoError(t, eng.PersistComputedSyncStats(ctx, syncID, rec))
	require.True(t, enginepkg.MarkStoreDirty(w))
	require.NoError(t, w.Close(ctx))
}

func readSyncStats(t *testing.T, ctx context.Context, path, syncID string) *v3.SyncStatsRecord {
	t.Helper()
	w, err := dotc1z.NewStore(ctx, path, dotc1z.WithReadOnly(true), dotc1z.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	defer w.Close(ctx)
	eng, ok := enginepkg.AsEngine(w)
	require.True(t, ok, "store at %s is not a pebble engine", path)
	rec, err := enginepkg.ReadSyncStatsRecord(ctx, eng, syncID)
	require.NoError(t, err)
	require.NotNil(t, rec, "compacted output must have a stats sidecar under its sync id")
	return rec
}

func readSyncToken(t *testing.T, ctx context.Context, path, syncID string) string {
	t.Helper()
	w, err := dotc1z.NewStore(ctx, path, dotc1z.WithReadOnly(true), dotc1z.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	defer w.Close(ctx)
	eng, ok := enginepkg.AsEngine(w)
	require.True(t, ok, "store at %s is not a pebble engine", path)
	rec, err := eng.GetSyncRunRecord(ctx, syncID)
	require.NoError(t, err)
	return rec.GetSyncToken()
}

// TestCompactPebbleFoldWritesProvenance pins the fold output's stats
// sidecar: the base sync's timing stats survive re-attributed via
// stats_sync_id, partial timings are folded into the maps, per-type record
// counts carry added/replaced/carried provenance, and no token is written
// to carry any of it.
func TestCompactPebbleFoldWritesProvenance(t *testing.T) {
	ctx := context.Background()
	inDir := t.TempDir()

	basePath := filepath.Join(inDir, "base.c1z")
	partialPath := filepath.Join(inDir, "partial.c1z")
	baseSyncID := buildPebbleInput(t, ctx, basePath, connectorstore.SyncTypeFull, "g-shared", "g-base-only")
	partialSyncID := buildPebbleInput(t, ctx, partialPath, connectorstore.SyncTypePartial, "g-shared", "g-partial-only")

	stampSyncStats(t, ctx, basePath, baseSyncID, 90_000, 2_000, 2_000, 1)
	stampSyncStats(t, ctx, partialPath, partialSyncID, 5_000, 1_000, 1_000, 2)

	t.Setenv("BATON_EXPERIMENTAL_PEBBLE_COMPACTOR", "fold")
	out := compactPairOnce(t, ctx,
		&CompactableSync{FilePath: basePath, SyncID: baseSyncID},
		&CompactableSync{FilePath: partialPath, SyncID: partialSyncID},
	)

	stats := readSyncStats(t, ctx, out.FilePath, out.SyncID)
	require.EqualValues(t, 95_000, stats.GetStepDurationsMs()["list-grants"])
	calls := stats.GetConnectorCallStats()["list-grants"]
	require.NotNil(t, calls)
	require.EqualValues(t, 3, calls.GetCount())
	require.EqualValues(t, 3_000, calls.GetTotalMs())
	require.EqualValues(t, 2_000, calls.GetMaxMs())

	comp := stats.GetCompaction()
	require.NotNil(t, comp, "fold output must carry compaction provenance")
	require.Equal(t, "fold", comp.GetMode())
	require.Equal(t, baseSyncID, comp.GetStatsSyncId())
	require.Equal(t, baseSyncID, comp.GetBaseSyncId())
	require.Equal(t, []string{partialSyncID}, comp.GetPartialSyncIds())
	require.EqualValues(t, 1, comp.GetPartialCount())

	// Grants: base {g-shared, g-base-only} + partial {g-shared newer,
	// g-partial-only} → output 3, added 1, replaced 1, carried 1.
	grants := comp.GetRecordCounts()["grants"]
	require.NotNil(t, grants)
	require.EqualValues(t, 3, grants.GetOutput())
	require.EqualValues(t, 1, grants.GetAdded())
	require.EqualValues(t, 1, grants.GetReplaced())
	require.EqualValues(t, 1, grants.GetCarried())
	for name, counts := range comp.GetRecordCounts() {
		require.Equal(t, counts.GetOutput(), counts.GetAdded()+counts.GetReplaced()+counts.GetCarried(),
			"record counts for %s must partition the output", name)
	}

	require.Empty(t, readSyncToken(t, ctx, out.FilePath, out.SyncID),
		"provenance lives on the stats sidecar; the compactor writes no token")
}

// TestCompactPebbleChainedFoldAccumulatesProvenance pins chained-fold
// semantics: the original collection sync stays the stats attribution, and
// partial counts / timings accumulate across folds.
func TestCompactPebbleChainedFoldAccumulatesProvenance(t *testing.T) {
	ctx := context.Background()
	inDir := t.TempDir()

	basePath := filepath.Join(inDir, "base.c1z")
	p1Path := filepath.Join(inDir, "p1.c1z")
	p2Path := filepath.Join(inDir, "p2.c1z")
	baseSyncID := buildPebbleInput(t, ctx, basePath, connectorstore.SyncTypeFull, "g-shared", "g-base-only")
	p1SyncID := buildPebbleInput(t, ctx, p1Path, connectorstore.SyncTypePartial, "g-shared")
	p2SyncID := buildPebbleInput(t, ctx, p2Path, connectorstore.SyncTypePartial, "g-p2-only")

	stampSyncStats(t, ctx, basePath, baseSyncID, 90_000, 2_000, 2_000, 1)
	stampSyncStats(t, ctx, p1Path, p1SyncID, 5_000, 1_000, 1_000, 2)
	stampSyncStats(t, ctx, p2Path, p2SyncID, 3_000, 500, 500, 1)

	t.Setenv("BATON_EXPERIMENTAL_PEBBLE_COMPACTOR", "fold")
	first := compactPairOnce(t, ctx,
		&CompactableSync{FilePath: basePath, SyncID: baseSyncID},
		&CompactableSync{FilePath: p1Path, SyncID: p1SyncID},
	)
	second := compactPairOnce(t, ctx,
		first,
		&CompactableSync{FilePath: p2Path, SyncID: p2SyncID},
	)

	stats := readSyncStats(t, ctx, second.FilePath, second.SyncID)
	require.EqualValues(t, 98_000, stats.GetStepDurationsMs()["list-grants"])

	comp := stats.GetCompaction()
	require.NotNil(t, comp)
	require.Equal(t, baseSyncID, comp.GetStatsSyncId(), "chained folds must keep the original attribution")
	require.Equal(t, first.SyncID, comp.GetBaseSyncId(), "the immediate base is the first fold's output")
	require.Equal(t, []string{p1SyncID, p2SyncID}, comp.GetPartialSyncIds())
	require.EqualValues(t, 2, comp.GetPartialCount())
}

// TestCompactPebbleRebuildWritesProvenance pins the rebuild (overlay) output:
// no inherited timing stats, but mode, source ids, and output record counts.
func TestCompactPebbleRebuildWritesProvenance(t *testing.T) {
	ctx := context.Background()
	inDir := t.TempDir()

	basePath := filepath.Join(inDir, "base.c1z")
	partialPath := filepath.Join(inDir, "partial.c1z")
	baseSyncID := buildPebbleInput(t, ctx, basePath, connectorstore.SyncTypeFull, "g-shared", "g-base-only")
	partialSyncID := buildPebbleInput(t, ctx, partialPath, connectorstore.SyncTypePartial, "g-shared", "g-partial-only")

	stampSyncStats(t, ctx, basePath, baseSyncID, 90_000, 2_000, 2_000, 1)

	t.Setenv("BATON_EXPERIMENTAL_PEBBLE_COMPACTOR", "overlay")
	out := compactPairOnce(t, ctx,
		&CompactableSync{FilePath: basePath, SyncID: baseSyncID},
		&CompactableSync{FilePath: partialPath, SyncID: partialSyncID},
	)

	stats := readSyncStats(t, ctx, out.FilePath, out.SyncID)
	require.Empty(t, stats.GetStepDurationsMs(), "rebuild outputs carry no inherited timing stats")

	comp := stats.GetCompaction()
	require.NotNil(t, comp, "rebuild output must carry compaction provenance")
	require.Equal(t, "overlay", comp.GetMode())
	require.Equal(t, baseSyncID, comp.GetBaseSyncId())
	require.Equal(t, []string{partialSyncID}, comp.GetPartialSyncIds())
	require.EqualValues(t, 1, comp.GetPartialCount())

	grants := comp.GetRecordCounts()["grants"]
	require.NotNil(t, grants)
	require.EqualValues(t, 3, grants.GetOutput())
	require.Zero(t, grants.GetAdded(), "rebuild has no per-source attribution")
	require.Zero(t, grants.GetReplaced())
	require.Zero(t, grants.GetCarried())
	require.Empty(t, readSyncToken(t, ctx, out.FilePath, out.SyncID))
}
