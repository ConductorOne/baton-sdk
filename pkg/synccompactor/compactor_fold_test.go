package synccompactor

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	enginepkg "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	formatv3 "github.com/conductorone/baton-sdk/pkg/dotc1z/format/v3"
)

// grantDiscoveredAt reads one grant's discovered_at from the Pebble c1z
// at path, under syncID.
func grantDiscoveredAt(t *testing.T, ctx context.Context, path, syncID, grantID string) time.Time {
	t.Helper()
	w, err := dotc1z.NewStore(ctx, path, dotc1z.WithReadOnly(true), dotc1z.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	defer w.Close(ctx)
	eng, ok := enginepkg.AsEngine(w)
	require.True(t, ok, "store at %s is not a pebble engine", path)
	rec, err := eng.GetGrantRecord(ctx, syncID, grantID)
	require.NoError(t, err)
	return rec.GetDiscoveredAt().AsTime()
}

// TestCompactPebbleFoldMintsFreshSync covers the in-place fold strategy
// (BATON_EXPERIMENTAL_PEBBLE_COMPACTOR=fold): the compacted output is a
// copy of the base input into which the partials' winners are folded
// via the keep-newer path. Base keys are never rewritten (the data
// keyspace carries no sync_id), but the single sync-run record is
// re-keyed to a FRESH sync id, so C1's compaction bookkeeping sees a
// new sync.
func TestCompactPebbleFoldMintsFreshSync(t *testing.T) {
	ctx := context.Background()
	inDir := t.TempDir()

	basePath := filepath.Join(inDir, "base.c1z")
	partialPath := filepath.Join(inDir, "partial.c1z")
	// The partial is built after the base, so its "g-shared" carries a
	// strictly newer discovered_at and must win the fold.
	baseSyncID := buildPebbleInput(t, ctx, basePath, connectorstore.SyncTypeFull, "g-shared", "g-base-only")
	partialSyncID := buildPebbleInput(t, ctx, partialPath, connectorstore.SyncTypePartial, "g-shared", "g-partial-only")

	t.Setenv("BATON_EXPERIMENTAL_PEBBLE_COMPACTOR", "fold")
	c, cleanup, err := NewCompactor(ctx, t.TempDir(), []*CompactableSync{
		{FilePath: basePath, SyncID: baseSyncID},
		{FilePath: partialPath, SyncID: partialSyncID},
	}, WithTmpDir(t.TempDir()), WithEngine(dotc1z.EnginePebble), WithSkipGrantExpansion())
	require.NoError(t, err)
	defer func() { require.NoError(t, cleanup()) }()

	out, err := c.Compact(ctx)
	require.NoError(t, err)
	require.NotNil(t, out)

	// Fresh sync id (not the base's).
	require.NotEqual(t, baseSyncID, out.SyncID, "fold output must mint a fresh sync id")
	require.NotEmpty(t, out.SyncID)

	// Union of grants, union sync type (full beats partial).
	count, syncType := verifyCompacted(t, ctx, out.FilePath, out.SyncID)
	require.Equal(t, 3, count)
	require.Equal(t, string(connectorstore.SyncTypeFull), syncType)

	// The overlapping grant's winner is the partial's record (strictly
	// newer discovered_at); the base-only grant keeps the base's stamp.
	// (Reads ignore the sync id arg — the keyspace carries one sync.)
	require.Equal(t,
		grantDiscoveredAt(t, ctx, partialPath, partialSyncID, "g-shared"),
		grantDiscoveredAt(t, ctx, out.FilePath, out.SyncID, "g-shared"),
		"fold winner for overlapping grant must be the partial's (newer) record")
	require.Equal(t,
		grantDiscoveredAt(t, ctx, basePath, baseSyncID, "g-base-only"),
		grantDiscoveredAt(t, ctx, out.FilePath, out.SyncID, "g-base-only"),
		"base-only grant must be preserved untouched")

	// ended_at advances to the max across inputs (the partial's).
	require.Equal(t,
		latestEndedAt(t, ctx, partialPath).UTC(),
		latestEndedAt(t, ctx, out.FilePath).UTC())

	// Fold semantics: the base sync's data is preserved wholesale, so
	// the base's asset survives (unlike the rebuild strategies, which
	// drop assets). Partial assets are not copied.
	require.Equal(t, 1, countPebbleAssets(t, ctx, out.FilePath, out.SyncID))

	// The folded sync-run record stands alone (no dangling parent link —
	// the base sync's record was overwritten by the rename), and its
	// stats sidecar was recomputed under the fresh id.
	statsW, err := dotc1z.NewStore(ctx, out.FilePath, dotc1z.WithReadOnly(true), dotc1z.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	defer statsW.Close(ctx)
	statsEng, ok := enginepkg.AsEngine(statsW)
	require.True(t, ok)
	rec, err := statsEng.GetSyncRunRecord(ctx, out.SyncID)
	require.NoError(t, err)
	require.Empty(t, rec.GetParentSyncId(), "folded sync must not carry a parent link to the overwritten base sync")
	stats, err := enginepkg.ReadSyncStatsRecord(ctx, statsEng, out.SyncID)
	require.NoError(t, err)
	require.NotNil(t, stats)
	require.Equal(t, int64(3), stats.GetGrants())

	// Header-only contract: the fresh sync id and its stats must be
	// readable from the envelope manifest WITHOUT unpacking the payload
	// (this is what compaction source selection and any "what's in this
	// c1z?" tooling rely on). The renamed-away base id must be gone.
	f, err := os.Open(out.FilePath)
	require.NoError(t, err)
	defer f.Close()
	m, err := formatv3.ReadManifestHeader(f)
	require.NoError(t, err)
	runs := m.GetSyncRuns()
	require.Len(t, runs, 1, "folded c1z manifest must project exactly one sync run")
	require.Equal(t, out.SyncID, runs[0].GetSyncId(), "manifest projection must carry the fresh sync id")
	require.NotEqual(t, baseSyncID, runs[0].GetSyncId(), "the overwritten base sync id must not appear in the manifest")
	require.NotNil(t, runs[0].GetStats(), "manifest projection must carry the recomputed stats sidecar")
	require.Equal(t, int64(3), runs[0].GetStats().GetGrants())
}
