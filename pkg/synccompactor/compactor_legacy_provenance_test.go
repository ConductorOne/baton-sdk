package synccompactor

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
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
	require.EqualValues(t, 90000, oldStats.GetStepDurationsMs()["list-grants"])
	token := readSyncToken(t, ctx, basePath, base.SyncID)
	c1zstore.ApplySyncTokenStatsRecord(oldStats, token)
	require.EqualValues(t, 95000, oldStats.GetStepDurationsMs()["list-grants"])
	prior, err := sdksync.CompactionStatsFromToken(token)
	require.NoError(t, err)
	require.NotNil(t, prior)
	require.EqualValues(t, 1, prior.PartialCount)

	t.Setenv("BATON_EXPERIMENTAL_PEBBLE_COMPACTOR", "fold")
	first := compactPairOnce(t, ctx, base, partial)
	stats := readSyncStats(t, ctx, first.FilePath, first.SyncID)
	require.EqualValues(t, 98000, stats.GetStepDurationsMs()["list-grants"])
	require.EqualValues(t, 4, stats.GetConnectorCallStats()["list-grants"].GetCount())
	require.EqualValues(t, 3500, stats.GetConnectorCallStats()["list-grants"].GetTotalMs())
	require.EqualValues(t, 2000, stats.GetConnectorCallStats()["list-grants"].GetMaxMs())
	session := stats.GetSessionStoreStats()["Get"]
	require.EqualValues(t, 4, session.GetCount())
	require.EqualValues(t, 3500, session.GetTotalMs())
	require.EqualValues(t, 2000, session.GetMaxMs())
	require.EqualValues(t, 3, session.GetErrors())
	require.EqualValues(t, 3, session.GetTimeouts())
	comp := stats.GetCompaction()
	require.NotNil(t, comp)
	require.Equal(t, ids["base_id"], comp.GetStatsSyncId())
	require.Equal(t, base.SyncID, comp.GetBaseSyncId())
	require.Equal(t, []string{ids["first_partial_id"], partial.SyncID}, comp.GetPartialSyncIds())
	require.EqualValues(t, 2, comp.GetPartialCount())
	require.EqualValues(t, 3, stats.GetGrants())
	require.EqualValues(t, 3, comp.GetRecordCounts()["grants"].GetOutput())
	stripped, err := sdksync.CompactionStatsFromToken(readSyncToken(t, ctx, first.FilePath, first.SyncID))
	require.NoError(t, err)
	require.Nil(t, stripped)

	// The new sidecar includes the first fold's partial; the inherited token does not.
	second := compactPairOnce(t, ctx, first, partial)
	stats = readSyncStats(t, ctx, second.FilePath, second.SyncID)
	require.EqualValues(t, 101000, stats.GetStepDurationsMs()["list-grants"])
	require.EqualValues(t, 5, stats.GetConnectorCallStats()["list-grants"].GetCount())
	require.EqualValues(t, 5, stats.GetSessionStoreStats()["Get"].GetCount())
	require.EqualValues(t, 4, stats.GetSessionStoreStats()["Get"].GetErrors())
	require.Equal(t, ids["base_id"], stats.GetCompaction().GetStatsSyncId())
	require.EqualValues(t, 3, stats.GetCompaction().GetPartialCount())
}
