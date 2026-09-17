package synccompactor

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	enginepkg "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	sdksync "github.com/conductorone/baton-sdk/pkg/sync"
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
	assertCompactedCollectionStatsEmpty(t, stats)

	comp := stats.GetCompaction()
	require.NotNil(t, comp, "fold output must carry compaction provenance")
	require.Equal(t, "fold", comp.GetMode())
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
	assertCompactedCollectionStatsEmpty(t, stats)

	comp := stats.GetCompaction()
	require.NotNil(t, comp)
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
	assertCompactedCollectionStatsEmpty(t, stats)

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

// A base compacted by an SDK that wrote provenance into the token chains
// through the fold like a sidecar-provenance base does.
func TestProvenanceFromTokenSectionChains(t *testing.T) {
	tok, err := sdksync.BuildCompactedToken("", sdksync.CompactionTokenInput{
		Mode:           "fold",
		BaseSyncID:     "base-0",
		PartialSyncIDs: []string{"p1", "p2"},
		RecordCounts:   map[string]sdksync.CompactionRecordCounts{"grants": {Output: 7, Added: 2}},
	})
	require.NoError(t, err)

	prior := provenanceFromTokenSection(context.Background(), tok)
	require.NotNil(t, prior)
	require.EqualValues(t, 2, prior.GetPartialCount())
	require.EqualValues(t, 7, prior.GetRecordCounts()["grants"].GetOutput())

	next := buildCompactionProvenance(prior, "fold", "base-1", []string{"p3"}, nil)
	require.EqualValues(t, 3, next.GetPartialCount())
	require.Equal(t, []string{"p1", "p2", "p3"}, next.GetPartialSyncIds())

	require.Nil(t, provenanceFromTokenSection(context.Background(), ""))
	stripped, err := sdksync.ClearCompactionSection(tok)
	require.NoError(t, err)
	require.Nil(t, provenanceFromTokenSection(context.Background(), stripped))
}

func assertCompactedCollectionStatsEmpty(t *testing.T, stats *v3.SyncStatsRecord) {
	t.Helper()
	require.Empty(t, stats.GetStepDurationsMs())
	require.Empty(t, stats.GetConnectorCallStats())
	require.Empty(t, stats.GetSessionStoreStats())
	require.False(t, stats.HasIngestQuality())
}

func TestCompactionStatsWithAndWithoutExpansion(t *testing.T) {
	for _, mode := range []PebbleCompactorMode{PebbleCompactorModeFold, PebbleCompactorModeOverlay, PebbleCompactorModeKWay} {
		for _, expand := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/expand=%t", mode, expand), func(t *testing.T) {
				ctx := context.Background()
				entries := buildIncrementalFixtures(t, ctx, t.TempDir())
				for _, entry := range entries {
					w, err := dotc1z.NewStore(ctx, entry.FilePath, dotc1z.WithTmpDir(t.TempDir()))
					require.NoError(t, err)
					eng, ok := enginepkg.AsEngine(w)
					require.True(t, ok)
					sr, err := eng.GetSyncRunRecord(ctx, entry.SyncID)
					require.NoError(t, err)
					sr.SetSyncToken(`{
"version":1,
"step_durations_ms":{"list-grants":90000},
"connector_call_stats":{"ListGrants":{"count":1,"total_ms":100,"max_ms":100}},
"session_store_stats":{"Get":{"count":1,"total_ms":10,"max_ms":10}},
"ingest_quality":{"source_cache_replay_blocked":false}}`)
					require.NoError(t, eng.PutSyncRunRecord(ctx, sr))
					require.NoError(t, eng.PersistSyncStats(ctx, entry.SyncID))
					require.True(t, enginepkg.MarkStoreDirty(w))
					require.NoError(t, w.Close(ctx))
				}
				opts := []Option{WithTmpDir(t.TempDir()), WithEngine(c1zstore.EnginePebble), WithPebbleCompactorMode(mode)}
				if !expand {
					opts = append(opts, WithSkipGrantExpansion())
				}
				c, cleanup, err := NewCompactor(ctx, t.TempDir(), entries, opts...)
				require.NoError(t, err)
				defer func() { require.NoError(t, cleanup()) }()
				out, err := c.Compact(ctx)
				require.NoError(t, err)
				stats := readSyncStats(t, ctx, out.FilePath, out.SyncID)
				assertCompactedCollectionStatsEmpty(t, stats)
				require.EqualValues(t, 2, stats.GetResourceTypes())
				require.EqualValues(t, 5, stats.GetResources())
				require.EqualValues(t, 3, stats.GetEntitlements())
				require.Equal(t, map[string]int64{"group": 3, "user": 2}, stats.GetResourcesByResourceType())
				require.Equal(t, map[string]int64{"group": 3}, stats.GetEntitlementsByResourceType())
				grants := grantOutcome(t, ctx, out.FilePath, out.SyncID)
				require.EqualValues(t, len(grants), stats.GetGrants())
				require.EqualValues(t, len(grants), stats.GetGrantsByEntitlementResourceType()["group"])
				if expand {
					require.Greater(t, len(grants), 5)
					hasGrant(t, grants, "ent-c|user|sam")
				} else {
					require.Len(t, grants, 5)
				}
				comp := stats.GetCompaction()
				require.NotNil(t, comp)
				require.Equal(t, string(mode), comp.GetMode())
				require.Equal(t, entries[0].SyncID, comp.GetBaseSyncId())
				require.Equal(t, []string{entries[1].SyncID}, comp.GetPartialSyncIds())
				require.EqualValues(t, 1, comp.GetPartialCount())
				require.EqualValues(t, 5, comp.GetRecordCounts()["grants"].GetOutput())
				original := readSyncStats(t, ctx, entries[0].FilePath, entries[0].SyncID)
				require.NotEmpty(t, original.GetStepDurationsMs())
				require.NotEmpty(t, original.GetConnectorCallStats())
				require.NotEmpty(t, original.GetSessionStoreStats())
				require.True(t, original.HasIngestQuality())
				if mode == PebbleCompactorModeFold && !expand {
					w, err := dotc1z.NewStore(ctx, out.FilePath, dotc1z.WithTmpDir(t.TempDir()))
					require.NoError(t, err)
					eng, ok := enginepkg.AsEngine(w)
					require.True(t, ok)
					stats.ClearCompaction()
					stats.SetStepDurationsMs(original.GetStepDurationsMs())
					stats.SetConnectorCallStats(original.GetConnectorCallStats())
					stats.SetSessionStoreStats(original.GetSessionStoreStats())
					stats.SetIngestQuality(original.GetIngestQuality())
					require.NoError(t, eng.PersistComputedSyncStats(ctx, out.SyncID, stats))
					assertCompactedCollectionStatsEmpty(t, stats)
					require.NoError(t, eng.PersistSyncStats(ctx, out.SyncID))
					recomputed, err := enginepkg.ReadSyncStatsRecord(ctx, eng, out.SyncID)
					require.NoError(t, err)
					assertCompactedCollectionStatsEmpty(t, recomputed)
					require.Nil(t, recomputed.GetCompaction())
					require.Equal(t, stats.GetGrants(), recomputed.GetGrants())
					require.True(t, enginepkg.MarkStoreDirty(w))
					require.NoError(t, w.Close(ctx))
				}
			})
		}
	}
}
