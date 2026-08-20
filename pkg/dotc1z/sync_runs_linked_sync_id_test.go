package dotc1z

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// TestLinkedSyncIDColumnMigratedAndCloneable pins two invariants around the
// vestigial linked_sync_id column (see the comment in syncRunsTable.Migrations):
//
//  1. A sync_runs table missing the column gets it re-added by migration, so
//     every opened file converges on one schema shape.
//  2. CloneSync and SnapshotTo work from such a file. This matters because
//     cloneCopy builds its INSERT column list from the SOURCE's
//     PRAGMA table_info and inserts into a fresh-DDL destination — any
//     column present in a source but absent from the current DDL breaks
//     cloning of every file that predates the removal.
func TestLinkedSyncIDColumnMigratedAndCloneable(t *testing.T) {
	ctx := context.Background()

	srcDir := t.TempDir()
	srcDBPath := filepath.Join(srcDir, "source.db")

	srcFile, err := NewC1File(ctx, srcDBPath, WithC1FTmpDir(srcDir))
	require.NoError(t, err)

	// Recreate sync_runs without linked_sync_id, simulating a file whose
	// schema diverged from the current DDL.
	_, err = srcFile.db.ExecContext(ctx, fmt.Sprintf("DROP TABLE %s", syncRuns.Name()))
	require.NoError(t, err)
	_, err = srcFile.db.ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			id integer primary key,
			sync_id text not null,
			started_at datetime not null,
			ended_at datetime,
			sync_token text not null,
			sync_type text not null default 'full',
			parent_sync_id text not null default '',
			supports_diff integer not null default 0,
			grants_backfilled integer not null default 0,
			stats text,
			ingest_invariant_generation text not null default '',
			ingest_invariant_coverage text not null default '',
			ingest_invariant_mode text not null default ''
		)`, syncRuns.Name()))
	require.NoError(t, err)

	// Re-run schema + migrations, as any open of the file would.
	_, err = srcFile.InitTables(ctx)
	require.NoError(t, err)

	var count int
	err = srcFile.db.QueryRowContext(ctx, fmt.Sprintf(
		"select count(*) from pragma_table_info('%s') where name='linked_sync_id'", syncRuns.Name())).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 1, count, "migration should re-add linked_sync_id")

	syncID, err := srcFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	rt := v2.ResourceType_builder{Id: "user"}.Build()
	require.NoError(t, srcFile.PutResourceTypes(ctx, rt))
	require.NoError(t, srcFile.EndSync(ctx))

	err = srcFile.CloneSync(ctx, filepath.Join(srcDir, "clone.c1z"), syncID)
	require.NoError(t, err, "CloneSync should succeed after migration normalizes the schema")

	err = srcFile.SnapshotTo(ctx, filepath.Join(srcDir, "snapshot.c1z"))
	require.NoError(t, err, "SnapshotTo should succeed after migration normalizes the schema")

	require.NoError(t, srcFile.rawDb.Close())
}
