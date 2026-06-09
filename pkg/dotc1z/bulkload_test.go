package dotc1z

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// writeAllTables writes one sync's worth of rows across every object table
// that carries deferrable secondary indexes, then ends the sync.
func writeAllTables(ctx context.Context, t *testing.T, c *C1File) {
	t.Helper()
	_, err := c.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	require.NoError(t, c.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build(),
		v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build(),
	))
	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build()}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build()}.Build()
	require.NoError(t, c.PutResources(ctx, g1, u1))
	ent1 := v2.Entitlement_builder{Id: "ent1", Resource: g1}.Build()
	require.NoError(t, c.PutEntitlements(ctx, ent1))
	require.NoError(t, c.PutGrants(ctx,
		v2.Grant_builder{Id: "grant1", Entitlement: ent1, Principal: u1}.Build(),
	))
	require.NoError(t, c.EndSync(ctx))
}

// indexNamesByTable reopens nothing; it queries the live db for the index
// name set of each object table.
func indexNamesByTable(ctx context.Context, t *testing.T, c *C1File) map[string][]string {
	t.Helper()
	out := map[string][]string{}
	for _, td := range allTableDescriptors {
		rows, err := c.db.QueryContext(ctx, fmt.Sprintf("PRAGMA index_list(%s)", td.Name()))
		require.NoError(t, err)
		var names []string
		for rows.Next() {
			var seq, unique, partial int
			var name, origin string
			require.NoError(t, rows.Scan(&seq, &name, &unique, &origin, &partial))
			names = append(names, fmt.Sprintf("%s|unique=%d|origin=%s|partial=%d", name, unique, origin, partial))
		}
		require.NoError(t, rows.Err())
		_ = rows.Close()
		sort.Strings(names)
		out[td.Name()] = names
	}
	return out
}

func tmpC1ZPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "test.c1z")
}

// Bulk-load output has byte-identical index sets and row counts to a
// normal-mode control, and InitTables is idempotent on re-run.
func TestBulkLoadIndexParity(t *testing.T) {
	ctx := context.Background()

	build := func(bulk bool) string {
		path := tmpC1ZPath(t)
		opts := []C1ZOption{}
		if bulk {
			opts = append(opts, WithBulkLoad(true), WithSkipVacuum(true))
		}
		c, err := NewC1ZFile(ctx, path, opts...)
		require.NoError(t, err)
		writeAllTables(ctx, t, c)
		require.NoError(t, c.Close(ctx))
		return path
	}

	normalPath := build(false)
	bulkPath := build(true)

	normal, err := NewC1ZFile(ctx, normalPath)
	require.NoError(t, err)
	defer normal.Close(ctx)
	bulk, err := NewC1ZFile(ctx, bulkPath)
	require.NoError(t, err)
	defer bulk.Close(ctx)

	normalIdx := indexNamesByTable(ctx, t, normal)
	bulkIdx := indexNamesByTable(ctx, t, bulk)
	require.Equal(t, normalIdx, bulkIdx, "bulk-load final index set must equal the normal-mode control (T5/T9)")

	// Row-count parity per object table.
	for _, td := range []tableDescriptor{resourceTypes, resources, entitlements, grants} {
		var nc, bc int
		require.NoError(t, normal.db.QueryRowContext(ctx, fmt.Sprintf("SELECT count(*) FROM %s", td.Name())).Scan(&nc))
		require.NoError(t, bulk.db.QueryRowContext(ctx, fmt.Sprintf("SELECT count(*) FROM %s", td.Name())).Scan(&bc))
		require.Equal(t, nc, bc, "row count mismatch for %s", td.Name())
	}

	// InitTables is idempotent — re-running it must not error and must
	// leave the index set unchanged (all DDL is IF NOT EXISTS).
	_, err = bulk.InitTables(ctx)
	require.NoError(t, err)
	require.Equal(t, normalIdx, indexNamesByTable(ctx, t, bulk), "InitTables re-run must be a no-op on indexes")
}

// Opening a POPULATED db with bulkLoad must NOT drop indexes — the
// per-table emptiness guard keeps them live.
func TestBulkLoadPopulatedTableKeepsIndexes(t *testing.T) {
	ctx := context.Background()
	path := tmpC1ZPath(t)

	// Populate normally first.
	c, err := NewC1ZFile(ctx, path)
	require.NoError(t, err)
	writeAllTables(ctx, t, c)
	require.NoError(t, c.Close(ctx))

	// Capture the control index set.
	ctl, err := NewC1ZFile(ctx, path)
	require.NoError(t, err)
	want := indexNamesByTable(ctx, t, ctl)
	require.NoError(t, ctl.Close(ctx))

	// Reopen the populated db WITH bulkLoad: indexes must be retained.
	reopened, err := NewC1ZFile(ctx, path, WithBulkLoad(true), WithSkipVacuum(true))
	require.NoError(t, err)
	defer reopened.Close(ctx)

	require.Equal(t, want, indexNamesByTable(ctx, t, reopened),
		"bulk load on a populated db must keep indexes live (B3)")
	require.Empty(t, reopened.deferredIndexTables,
		"no tables should be marked for deferral when the db is non-empty (B3)")
}

// The deferred-index rebuild runs on a detached context, so Close
// completes even when the caller's context is already canceled.
func TestBulkLoadRebuildSurvivesCanceledContext(t *testing.T) {
	ctx := context.Background()
	path := tmpC1ZPath(t)

	c, err := NewC1ZFile(ctx, path, WithBulkLoad(true), WithSkipVacuum(true))
	require.NoError(t, err)
	writeAllTables(ctx, t, c)

	canceled, cancel := context.WithCancel(ctx)
	cancel() // already canceled before Close
	require.NoError(t, c.Close(canceled), "Close must complete despite a canceled caller context (B4)")

	// The output must be a fully-indexed, valid c1z.
	reopened, err := NewC1ZFile(ctx, path)
	require.NoError(t, err)
	defer reopened.Close(ctx)

	control := tmpC1ZPath(t)
	cc, err := NewC1ZFile(ctx, control)
	require.NoError(t, err)
	writeAllTables(ctx, t, cc)
	require.NoError(t, cc.Close(ctx))
	ccRO, err := NewC1ZFile(ctx, control)
	require.NoError(t, err)
	defer ccRO.Close(ctx)

	require.Equal(t, indexNamesByTable(ctx, t, ccRO), indexNamesByTable(ctx, t, reopened),
		"rebuilt index set must match the control after a canceled-context Close")
}

// If the deferred-index rebuild fails, the working database is
// PRESERVED (not cleaned up) so the just-loaded data is recoverable.
func TestBulkLoadRebuildFailurePreservesDB(t *testing.T) {
	ctx := context.Background()
	path := tmpC1ZPath(t)

	c, err := NewC1ZFile(ctx, path, WithBulkLoad(true), WithSkipVacuum(true))
	require.NoError(t, err)
	writeAllTables(ctx, t, c)
	require.NotEmpty(t, c.deferredIndexTables, "precondition: tables were deferred")

	dbPath := c.dbFilePath
	require.FileExists(t, dbPath)

	// Force the rebuild's Exec to fail by closing the underlying handle.
	require.NoError(t, c.rawDb.Close())

	err = c.buildDeferredIndexes(ctx)
	require.Error(t, err, "rebuild must surface the failure")
	require.Contains(t, err.Error(), "working db preserved")
	require.FileExists(t, dbPath, "working database must be preserved on rebuild failure (B4)")
}
