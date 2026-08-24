package dotc1z

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// TestLegacyDiffSyncRowsReadAndConvert pins how the current SDK handles a
// SQLite c1z carrying the removed diff-sync shape: a partial_upserts /
// partial_deletions pair with bidirectional linked_sync_id, supports_diff,
// and a parent_sync_id referencing a sync in another file — exactly what the
// deleted GenerateSyncDiffFromFile wrote. Old SDKs could produce such files;
// StartNewSync rejects the types now, so the rows are seeded with raw SQL.
//
// Pinned behavior:
//  1. The file opens and ListSyncRuns returns the legacy rows cleanly, with
//     their type strings preserved.
//  2. ToPebble with syncID "" ignores them: when they are all the file
//     holds, conversion fails loudly ("no convertible sync found") and
//     leaves no output — never a silent empty artifact.
//  3. Once a convertible sync exists, "" resolves to it and the legacy rows
//     are simply left behind.
func TestLegacyDiffSyncRowsReadAndConvert(t *testing.T) {
	ctx := context.Background()

	dir := t.TempDir()
	src, err := NewC1ZFile(ctx, filepath.Join(dir, "legacy-diff.c1z"), WithTmpDir(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, src.Close(ctx)) }()

	// Seed two finished partials, then rewrite them into the legacy
	// diff-pair shape the removed writer produced.
	upsertsID, err := src.StartNewSync(ctx, connectorstore.SyncTypePartial, "")
	require.NoError(t, err)
	require.NoError(t, src.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "user"}.Build()))
	require.NoError(t, src.EndSync(ctx))

	deletionsID, err := src.StartNewSync(ctx, connectorstore.SyncTypePartial, "")
	require.NoError(t, err)
	require.NoError(t, src.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "group"}.Build()))
	require.NoError(t, src.EndSync(ctx))

	rewrite := func(syncID, syncType, linkedID string) {
		t.Helper()
		res, err := src.db.ExecContext(ctx, fmt.Sprintf(
			`UPDATE %s
			 SET sync_type = ?, linked_sync_id = ?, parent_sync_id = 'external-base-sync',
			     supports_diff = 1, grants_backfilled = 1
			 WHERE sync_id = ?`, syncRuns.Name()),
			syncType, linkedID, syncID)
		require.NoError(t, err)
		n, err := res.RowsAffected()
		require.NoError(t, err)
		require.EqualValues(t, 1, n)
	}
	rewrite(upsertsID, "partial_upserts", deletionsID)
	rewrite(deletionsID, "partial_deletions", upsertsID)

	// 1. The legacy rows list cleanly, types preserved verbatim.
	runs, _, err := src.ListSyncRuns(ctx, "", 10)
	require.NoError(t, err)
	require.Len(t, runs, 2)
	typesByID := map[string]connectorstore.SyncType{}
	for _, r := range runs {
		typesByID[r.ID] = r.Type
		require.True(t, r.SupportsDiff)
		require.Equal(t, "external-base-sync", r.ParentSyncID,
			"a parent in another file must survive the read")
	}
	require.Equal(t, connectorstore.SyncType("partial_upserts"), typesByID[upsertsID])
	require.Equal(t, connectorstore.SyncType("partial_deletions"), typesByID[deletionsID])

	// 2. Auto-resolve refuses a diff-only file: loud error, no output.
	outPath := filepath.Join(dir, "out.c1z")
	_, err = src.ToPebble(ctx, outPath, "")
	require.ErrorContains(t, err, "no convertible sync found")
	_, statErr := os.Stat(outPath)
	require.ErrorIs(t, statErr, os.ErrNotExist)

	// 3. With a real full sync present, "" resolves past the legacy rows.
	fullID, err := src.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, src.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "role"}.Build()))
	require.NoError(t, src.EndSync(ctx))

	stats, err := src.ToPebble(ctx, outPath, "")
	require.NoError(t, err)
	require.Equal(t, fullID, stats.SourceSyncID,
		"auto-resolve must select the full sync and ignore legacy diff rows")
}
