package synccompactor

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	sdksync "github.com/conductorone/baton-sdk/pkg/sync"
)

func TestCompactPebbleFoldLegacyProvenance(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join("testdata", "legacy-compaction")
	data, err := os.ReadFile(filepath.Join(dir, "metadata.json"))
	require.NoError(t, err)
	var ids map[string]string
	require.NoError(t, json.Unmarshal(data, &ids))
	basePath := filepath.Join(dir, "old-fold.c1z")
	partialPath := filepath.Join(dir, "old-partial.c1z")
	base := &CompactableSync{FilePath: basePath, SyncID: ids["fold_id"]}
	partial := &CompactableSync{FilePath: partialPath, SyncID: ids["next_partial_id"]}
	oldStats := readSyncStats(t, ctx, basePath, base.SyncID)
	require.Nil(t, oldStats.GetCompaction())
	token := readSyncToken(t, ctx, basePath, base.SyncID)
	prior, err := sdksync.CompactionStatsFromToken(token)
	require.NoError(t, err)
	require.NotNil(t, prior)
	require.EqualValues(t, 1, prior.PartialCount)

	t.Setenv("BATON_EXPERIMENTAL_PEBBLE_COMPACTOR", "fold")
	first := compactPairOnce(t, ctx, base, partial)
	stats := readSyncStats(t, ctx, first.FilePath, first.SyncID)
	assertCompactedCollectionStatsEmpty(t, stats)
	comp := stats.GetCompaction()
	require.NotNil(t, comp)
	require.Equal(t, base.SyncID, comp.GetBaseSyncId())
	require.Equal(t, []string{ids["first_partial_id"], partial.SyncID}, comp.GetPartialSyncIds())
	require.EqualValues(t, 2, comp.GetPartialCount())
	require.EqualValues(t, 3, stats.GetGrants())
	require.EqualValues(t, 3, comp.GetRecordCounts()["grants"].GetOutput())
	stripped, err := sdksync.CompactionStatsFromToken(readSyncToken(t, ctx, first.FilePath, first.SyncID))
	require.NoError(t, err)
	require.Nil(t, stripped)

	second := compactPairOnce(t, ctx, first, partial)
	stats = readSyncStats(t, ctx, second.FilePath, second.SyncID)
	assertCompactedCollectionStatsEmpty(t, stats)
	require.EqualValues(t, 3, stats.GetCompaction().GetPartialCount())
}
