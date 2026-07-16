package synccompactor

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	enginepkg "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	sdksync "github.com/conductorone/baton-sdk/pkg/sync"
)

// fixtureStatsToken hand-writes the stable v1 token wire format so this
// package pins it without reaching into pkg/sync internals.
func fixtureStatsToken(grantStepMs, callTotalMs, callMaxMs, callCount int64) string {
	return fmt.Sprintf(
		`{"step_durations_ms":{"list-grants":%d},"connector_call_stats":{"list-grants":{"count":%d,"total_ms":%d,"max_ms":%d}},"version":1}`,
		grantStepMs, callCount, callTotalMs, callMaxMs,
	)
}

// stampSyncToken writes token onto the (finished) sync's run record so the
// input mimics a real sync whose final checkpoint carried timing stats.
func stampSyncToken(t *testing.T, ctx context.Context, path, syncID, token string) {
	t.Helper()
	w, err := dotc1z.NewStore(ctx, path, dotc1z.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	eng, ok := enginepkg.AsEngine(w)
	require.True(t, ok, "store at %s is not a pebble engine", path)
	rec, err := eng.GetSyncRunRecord(ctx, syncID)
	require.NoError(t, err)
	rec.SetSyncToken(token)
	require.NoError(t, eng.PutSyncRunRecord(ctx, rec))
	require.True(t, enginepkg.MarkStoreDirty(w))
	require.NoError(t, w.Close(ctx))
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

// tokenTopLevelStepDurations parses the token's top-level timing map the way
// an external consumer would.
func tokenTopLevelStepDurations(t *testing.T, token string) map[string]int64 {
	t.Helper()
	var parsed struct {
		StepDurationsMs map[string]int64 `json:"step_durations_ms"`
	}
	require.NoError(t, json.Unmarshal([]byte(token), &parsed))
	return parsed.StepDurationsMs
}

// TestCompactPebbleFoldWritesProvenance pins the fold output's token: the
// base sync's timing stats survive re-attributed via stats_sync_id, partial
// timings are folded into the top-level maps, and per-type record counts
// carry added/replaced/carried provenance.
func TestCompactPebbleFoldWritesProvenance(t *testing.T) {
	ctx := context.Background()
	inDir := t.TempDir()

	basePath := filepath.Join(inDir, "base.c1z")
	partialPath := filepath.Join(inDir, "partial.c1z")
	baseSyncID := buildPebbleInput(t, ctx, basePath, connectorstore.SyncTypeFull, "g-shared", "g-base-only")
	partialSyncID := buildPebbleInput(t, ctx, partialPath, connectorstore.SyncTypePartial, "g-shared", "g-partial-only")

	stampSyncToken(t, ctx, basePath, baseSyncID, fixtureStatsToken(90_000, 2_000, 2_000, 1))
	stampSyncToken(t, ctx, partialPath, partialSyncID, fixtureStatsToken(5_000, 1_000, 1_000, 2))

	t.Setenv("BATON_EXPERIMENTAL_PEBBLE_COMPACTOR", "fold")
	out := compactPairOnce(t, ctx,
		&CompactableSync{FilePath: basePath, SyncID: baseSyncID},
		&CompactableSync{FilePath: partialPath, SyncID: partialSyncID},
	)

	token := readSyncToken(t, ctx, out.FilePath, out.SyncID)
	require.NotEmpty(t, token)

	require.EqualValues(t, 95_000, tokenTopLevelStepDurations(t, token)["list-grants"])

	comp, err := sdksync.CompactionStatsFromToken(token)
	require.NoError(t, err)
	require.NotNil(t, comp, "fold output must carry a compaction section")
	require.Equal(t, "fold", comp.Mode)
	require.Equal(t, baseSyncID, comp.StatsSyncID)
	require.Equal(t, baseSyncID, comp.BaseSyncID)
	require.Equal(t, []string{partialSyncID}, comp.PartialSyncIDs)
	require.EqualValues(t, 1, comp.PartialCount)

	// Grants: base {g-shared, g-base-only} + partial {g-shared newer,
	// g-partial-only} → output 3, added 1, replaced 1, carried 1.
	grants := comp.RecordCounts["grants"]
	require.NotNil(t, grants)
	require.EqualValues(t, 3, grants.Output)
	require.EqualValues(t, 1, grants.Added)
	require.EqualValues(t, 1, grants.Replaced)
	require.EqualValues(t, 1, grants.Carried)
	for name, counts := range comp.RecordCounts {
		require.Equal(t, counts.Output, counts.Added+counts.Replaced+counts.Carried,
			"record counts for %s must partition the output", name)
	}
}

// TestCompactPebbleChainedFoldAccumulatesProvenance pins chained-fold
// semantics: the original collection sync stays the stats attribution, and
// partial counts / top-level timings accumulate across folds.
func TestCompactPebbleChainedFoldAccumulatesProvenance(t *testing.T) {
	ctx := context.Background()
	inDir := t.TempDir()

	basePath := filepath.Join(inDir, "base.c1z")
	p1Path := filepath.Join(inDir, "p1.c1z")
	p2Path := filepath.Join(inDir, "p2.c1z")
	baseSyncID := buildPebbleInput(t, ctx, basePath, connectorstore.SyncTypeFull, "g-shared", "g-base-only")
	p1SyncID := buildPebbleInput(t, ctx, p1Path, connectorstore.SyncTypePartial, "g-shared")
	p2SyncID := buildPebbleInput(t, ctx, p2Path, connectorstore.SyncTypePartial, "g-p2-only")

	stampSyncToken(t, ctx, basePath, baseSyncID, fixtureStatsToken(90_000, 2_000, 2_000, 1))
	stampSyncToken(t, ctx, p1Path, p1SyncID, fixtureStatsToken(5_000, 1_000, 1_000, 2))
	stampSyncToken(t, ctx, p2Path, p2SyncID, fixtureStatsToken(3_000, 500, 500, 1))

	t.Setenv("BATON_EXPERIMENTAL_PEBBLE_COMPACTOR", "fold")
	first := compactPairOnce(t, ctx,
		&CompactableSync{FilePath: basePath, SyncID: baseSyncID},
		&CompactableSync{FilePath: p1Path, SyncID: p1SyncID},
	)
	second := compactPairOnce(t, ctx,
		first,
		&CompactableSync{FilePath: p2Path, SyncID: p2SyncID},
	)

	token := readSyncToken(t, ctx, second.FilePath, second.SyncID)
	require.EqualValues(t, 98_000, tokenTopLevelStepDurations(t, token)["list-grants"])

	comp, err := sdksync.CompactionStatsFromToken(token)
	require.NoError(t, err)
	require.NotNil(t, comp)
	require.Equal(t, baseSyncID, comp.StatsSyncID, "chained folds must keep the original attribution")
	require.Equal(t, first.SyncID, comp.BaseSyncID, "the immediate base is the first fold's output")
	require.Equal(t, []string{p1SyncID, p2SyncID}, comp.PartialSyncIDs)
	require.EqualValues(t, 2, comp.PartialCount)
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

	stampSyncToken(t, ctx, basePath, baseSyncID, fixtureStatsToken(90_000, 2_000, 2_000, 1))

	t.Setenv("BATON_EXPERIMENTAL_PEBBLE_COMPACTOR", "overlay")
	out := compactPairOnce(t, ctx,
		&CompactableSync{FilePath: basePath, SyncID: baseSyncID},
		&CompactableSync{FilePath: partialPath, SyncID: partialSyncID},
	)

	token := readSyncToken(t, ctx, out.FilePath, out.SyncID)
	require.NotEmpty(t, token)
	require.Empty(t, tokenTopLevelStepDurations(t, token), "rebuild outputs carry no inherited timing stats")

	comp, err := sdksync.CompactionStatsFromToken(token)
	require.NoError(t, err)
	require.NotNil(t, comp, "rebuild output must carry a compaction section")
	require.Equal(t, "overlay", comp.Mode)
	require.Equal(t, baseSyncID, comp.BaseSyncID)
	require.Equal(t, []string{partialSyncID}, comp.PartialSyncIDs)
	require.EqualValues(t, 1, comp.PartialCount)

	grants := comp.RecordCounts["grants"]
	require.NotNil(t, grants)
	require.EqualValues(t, 3, grants.Output)
	require.Zero(t, grants.Added, "rebuild has no per-source attribution")
	require.Zero(t, grants.Replaced)
	require.Zero(t, grants.Carried)
}
