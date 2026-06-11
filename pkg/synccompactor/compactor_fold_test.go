package synccompactor

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	enginepkg "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
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

// TestCompactPebbleFoldAdoptsBaseSync covers the in-place fold strategy
// (BATON_EXPERIMENTAL_PEBBLE_COMPACTOR=fold): the compacted output is a
// copy of the base input whose base sync ADOPTS the partials' winners,
// keeping the base sync id. Base keys are never rewritten; partial
// records land via the keep-newer path.
func TestCompactPebbleFoldAdoptsBaseSync(t *testing.T) {
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

	// Sync-id adoption: the output's sync IS the base sync.
	require.Equal(t, baseSyncID, out.SyncID, "fold output must adopt the base sync id")

	// Union of grants, union sync type (full beats partial).
	count, syncType := verifyCompacted(t, ctx, out.FilePath, out.SyncID)
	require.Equal(t, 3, count)
	require.Equal(t, string(connectorstore.SyncTypeFull), syncType)

	// The overlapping grant's winner is the partial's record (strictly
	// newer discovered_at); the base-only grant keeps the base's stamp.
	require.Equal(t,
		grantDiscoveredAt(t, ctx, partialPath, partialSyncID, "g-shared"),
		grantDiscoveredAt(t, ctx, out.FilePath, baseSyncID, "g-shared"),
		"fold winner for overlapping grant must be the partial's (newer) record")
	require.Equal(t,
		grantDiscoveredAt(t, ctx, basePath, baseSyncID, "g-base-only"),
		grantDiscoveredAt(t, ctx, out.FilePath, baseSyncID, "g-base-only"),
		"base-only grant must be preserved untouched")

	// ended_at advances to the max across inputs (the partial's).
	require.Equal(t,
		latestEndedAt(t, ctx, partialPath).UTC(),
		latestEndedAt(t, ctx, out.FilePath).UTC())

	// Fold semantics: the base sync is preserved wholesale, so the
	// base's asset survives (unlike the rebuild strategies, which drop
	// assets). Partial assets are not copied.
	require.Equal(t, 1, countPebbleAssets(t, ctx, out.FilePath, baseSyncID))

	// Stats sidecar was recomputed for the folded sync.
	statsW, err := dotc1z.NewStore(ctx, out.FilePath, dotc1z.WithReadOnly(true), dotc1z.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	defer statsW.Close(ctx)
	statsEng, ok := enginepkg.AsEngine(statsW)
	require.True(t, ok)
	stats, err := enginepkg.ReadSyncStatsRecord(ctx, statsEng, baseSyncID)
	require.NoError(t, err)
	require.NotNil(t, stats)
	require.Equal(t, int64(3), stats.GetGrants())
}
