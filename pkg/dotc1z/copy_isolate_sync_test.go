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

// writeSync writes one full sync to f containing a single resource type, one
// resource, one entitlement, and one grant, all tagged with idSuffix so syncs
// written to the same file are distinguishable. It returns the new sync's id.
func writeSync(ctx context.Context, t *testing.T, f *C1File, idSuffix string) string {
	t.Helper()

	syncID, err := f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	groupRT := v2.ResourceType_builder{Id: "group"}.Build()
	require.NoError(t, f.PutResourceTypes(ctx, groupRT))

	g := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g-" + idSuffix}.Build(),
	}.Build()
	require.NoError(t, f.PutResources(ctx, g))

	ent := v2.Entitlement_builder{Id: "ent-" + idSuffix, Resource: g}.Build()
	require.NoError(t, f.PutEntitlements(ctx, ent))

	grant := v2.Grant_builder{Id: "grant-" + idSuffix, Entitlement: ent, Principal: g}.Build()
	require.NoError(t, f.PutGrants(ctx, grant))

	require.NoError(t, f.EndSync(ctx))
	return syncID
}

func countRows(ctx context.Context, t *testing.T, f *C1File, query string, args ...any) int {
	t.Helper()
	var n int
	require.NoError(t, f.rawDb.QueryRowContext(ctx, query, args...).Scan(&n))
	return n
}

// TestCopyIsolateSyncSingleSync verifies the single-sync passthrough: the copy
// is used as-is (no per-sync deletes) and the output is a correct single-sync
// c1z with the data intact.
func TestCopyIsolateSyncSingleSync(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	src, err := NewC1File(ctx, filepath.Join(dir, "source.db"), WithC1FTmpDir(dir))
	require.NoError(t, err)
	syncID := writeSync(ctx, t, src, "a")

	outPath := filepath.Join(dir, "out.c1z")
	require.NoError(t, src.CopyIsolateSync(ctx, outPath, syncID))
	require.NoError(t, src.rawDb.Close())

	out, err := NewC1ZFile(ctx, outPath)
	require.NoError(t, err)
	defer func() { require.NoError(t, out.Close(ctx)) }()

	require.Equal(t, 1, countRows(ctx, t, out, fmt.Sprintf("SELECT COUNT(DISTINCT sync_id) FROM %s", syncRuns.Name())))
	require.Equal(t, 1, countRows(ctx, t, out, fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE sync_id = ?", resources.Name()), syncID))
	require.Equal(t, 1, countRows(ctx, t, out, fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE sync_id = ?", grants.Name()), syncID))

	out.currentSyncID = ""
	require.NoError(t, out.ViewSync(ctx, syncID))
	resp, err := out.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, resp.GetList(), 1)
	require.Equal(t, "grant-a", resp.GetList()[0].GetId())
}

// TestCopyIsolateSyncMultiSync verifies that for a multi-sync source the other
// syncs are deleted from the copy (only the target sync survives in the output)
// and that the source file is left untouched (input immutability).
func TestCopyIsolateSyncMultiSync(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	src, err := NewC1File(ctx, filepath.Join(dir, "source.db"), WithC1FTmpDir(dir))
	require.NoError(t, err)
	targetSyncID := writeSync(ctx, t, src, "target")
	otherSyncID := writeSync(ctx, t, src, "other")
	require.NotEqual(t, targetSyncID, otherSyncID)

	outPath := filepath.Join(dir, "out.c1z")
	require.NoError(t, src.CopyIsolateSync(ctx, outPath, targetSyncID))

	// Input immutability: the source still has BOTH syncs after the call.
	require.Equal(t, 2, countRows(ctx, t, src, fmt.Sprintf("SELECT COUNT(DISTINCT sync_id) FROM %s", syncRuns.Name())))
	require.NoError(t, src.rawDb.Close())

	out, err := NewC1ZFile(ctx, outPath)
	require.NoError(t, err)
	defer func() { require.NoError(t, out.Close(ctx)) }()

	// Sync isolation: only the target sync remains, across every data table.
	require.Equal(t, 1, countRows(ctx, t, out, fmt.Sprintf("SELECT COUNT(DISTINCT sync_id) FROM %s", syncRuns.Name())))
	for _, table := range []string{resources.Name(), entitlements.Name(), grants.Name(), syncRuns.Name()} {
		require.Equal(t, 0, countRows(ctx, t, out, fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE sync_id = ?", table), otherSyncID),
			"table %s should have no rows from the non-target sync", table)
	}
	require.Equal(t, 1, countRows(ctx, t, out, fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE sync_id = ?", grants.Name()), targetSyncID))

	out.currentSyncID = ""
	require.NoError(t, out.ViewSync(ctx, targetSyncID))
	resp, err := out.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, resp.GetList(), 1)
	require.Equal(t, "grant-target", resp.GetList()[0].GetId())
}

// TestCopyIsolateSyncMigratesOlderSchema verifies requirement (c): the copy is
// opened through NewC1File, so schema migrations run on it. A source whose
// grants table is missing the migration-added expansion/needs_expansion columns
// (an older schema) must produce an output whose grants table has them.
func TestCopyIsolateSyncMigratesOlderSchema(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	src, err := NewC1File(ctx, filepath.Join(dir, "source.db"), WithC1FTmpDir(dir))
	require.NoError(t, err)
	syncID := writeSync(ctx, t, src, "a")

	// Simulate an older schema: drop the migration-added columns (and the
	// partial indexes that depend on them) from the source grants table. getSync
	// and the sync-count read only touch sync_runs, so the source stays readable
	// for CopyIsolateSync's preconditions.
	for _, idx := range []string{"idx_grants_sync_expansion_v1", "idx_grants_sync_needs_expansion_v1"} {
		_, err = src.db.ExecContext(ctx, fmt.Sprintf("DROP INDEX IF EXISTS %s", idx))
		require.NoError(t, err)
	}
	for _, col := range []string{"expansion", "needs_expansion"} {
		_, err = src.db.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", grants.Name(), col))
		require.NoError(t, err)
	}
	require.Equal(t, 0, countRows(ctx, t, src,
		fmt.Sprintf("SELECT COUNT(*) FROM pragma_table_info('%s') WHERE name IN ('expansion','needs_expansion')", grants.Name())),
		"precondition: source grants should be missing the migration columns")

	outPath := filepath.Join(dir, "out.c1z")
	require.NoError(t, src.CopyIsolateSync(ctx, outPath, syncID))
	require.NoError(t, src.rawDb.Close())

	out, err := NewC1ZFile(ctx, outPath)
	require.NoError(t, err)
	defer func() { require.NoError(t, out.Close(ctx)) }()

	require.Equal(t, 2, countRows(ctx, t, out,
		fmt.Sprintf("SELECT COUNT(*) FROM pragma_table_info('%s') WHERE name IN ('expansion','needs_expansion')", grants.Name())),
		"migration should have re-added expansion and needs_expansion on the copy")
	require.Equal(t, 1, countRows(ctx, t, out, fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE sync_id = ?", grants.Name()), syncID),
		"grant data should survive the copy + migration")
}

// TestCopyIsolateSyncRejectsExistingOutput verifies the output-path precheck.
func TestCopyIsolateSyncRejectsExistingOutput(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	src, err := NewC1File(ctx, filepath.Join(dir, "source.db"), WithC1FTmpDir(dir))
	require.NoError(t, err)
	syncID := writeSync(ctx, t, src, "a")
	defer func() { require.NoError(t, src.rawDb.Close()) }()

	outPath := filepath.Join(dir, "exists.c1z")
	f, ferr := os.Create(outPath)
	require.NoError(t, ferr)
	require.NoError(t, f.Close())

	err = src.CopyIsolateSync(ctx, outPath, syncID)
	require.Error(t, err)
	require.Contains(t, err.Error(), "must not exist")
}
