package pebble

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// A Lambda-hosted connector can change release while a sync is running. The
// next run must not resume the unfinished sync the old release started:
// resuming mixes both releases' data into one sync. These tests interrupt a
// sync (no EndSync), reopen the same directory in a "new process" with a
// connector version, and check what StartOrResumeSync does.

// interruptSyncWithVersion opens dbDir with version, starts a full sync, writes
// one resource type, checkpoints, and closes without EndSync, as a crashed run
// leaves the file. Returns the sync id.
func interruptSyncWithVersion(t *testing.T, dbDir, version string) string {
	t.Helper()
	ctx := context.Background()
	e, err := Open(ctx, dbDir, WithConnectorVersion(version))
	require.NoError(t, err)
	syncID, newSync, err := e.StartOrResumeSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.True(t, newSync)
	require.NoError(t, e.records.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "stale", DisplayName: "Stale"}.Build()))
	require.NoError(t, e.CheckpointSync(ctx, "page-token-from-old-version"))
	rec, err := e.syncRuns.GetSyncRunRecord(ctx, syncID)
	require.NoError(t, err)
	require.Equal(t, version, rec.GetConnectorVersion(), "StartNewSync must stamp the connector version")
	require.NoError(t, e.Close())
	return syncID
}

// reopenWithVersion reopens dbDir the way pebbleStore does (Open, then
// InitCurrentSync) and registers Close.
func reopenWithVersion(t *testing.T, dbDir, version string) *Engine {
	t.Helper()
	ctx := context.Background()
	e, err := Open(ctx, dbDir, WithConnectorVersion(version))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, e.Close()) })
	require.NoError(t, e.InitCurrentSync(ctx))
	return e
}

func resourceTypeIDs(t *testing.T, e *Engine) []string {
	t.Helper()
	resp, err := e.records.ListResourceTypes(context.Background(), v2.ResourceTypesServiceListResourceTypesRequest_builder{}.Build())
	require.NoError(t, err)
	var ids []string
	for _, rt := range resp.GetList() {
		ids = append(ids, rt.GetId())
	}
	return ids
}

func TestStartOrResumeSyncConnectorVersion(t *testing.T) {
	for _, tc := range []struct {
		name       string
		oldVersion string
		newVersion string
		wantResume bool
	}{
		{name: "same version resumes", oldVersion: "v1", newVersion: "v1", wantResume: true},
		{name: "different version starts new", oldVersion: "v1", newVersion: "v2", wantResume: false},
		{name: "empty stored version resumes", oldVersion: "", newVersion: "v2", wantResume: true},
		{name: "empty caller version resumes", oldVersion: "v1", newVersion: "", wantResume: true},
		{name: "both empty resumes", oldVersion: "", newVersion: "", wantResume: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			dbDir := filepath.Join(t.TempDir(), "db")
			oldID := interruptSyncWithVersion(t, dbDir, tc.oldVersion)

			e := reopenWithVersion(t, dbDir, tc.newVersion)
			id, newSync, err := e.StartOrResumeSync(ctx, connectorstore.SyncTypeFull, "")
			require.NoError(t, err)

			if tc.wantResume {
				require.False(t, newSync)
				require.Equal(t, oldID, id)
				step, err := e.CurrentSyncStep(ctx)
				require.NoError(t, err)
				require.Equal(t, "page-token-from-old-version", step)
				require.Equal(t, []string{"stale"}, resourceTypeIDs(t, e))
				return
			}

			require.True(t, newSync)
			require.NotEqual(t, oldID, id)
			_, err = e.syncRuns.GetSyncRunRecord(ctx, oldID)
			require.Error(t, err, "the old sync's record must be wiped")
			require.Empty(t, resourceTypeIDs(t, e), "the old sync's records must be wiped")
			step, err := e.CurrentSyncStep(ctx)
			require.NoError(t, err)
			require.Empty(t, step, "the new sync must start from the beginning")
			rec, err := e.syncRuns.GetSyncRunRecord(ctx, id)
			require.NoError(t, err)
			require.Equal(t, tc.newVersion, rec.GetConnectorVersion())
		})
	}
}

// TestStartOrResumeSyncExplicitIDConnectorVersion: an explicit sync id from a
// different version is not resumed either.
func TestStartOrResumeSyncExplicitIDConnectorVersion(t *testing.T) {
	ctx := context.Background()
	dbDir := filepath.Join(t.TempDir(), "db")
	oldID := interruptSyncWithVersion(t, dbDir, "v1")

	e := reopenWithVersion(t, dbDir, "v2")
	id, newSync, err := e.StartOrResumeSync(ctx, connectorstore.SyncTypeFull, oldID)
	require.NoError(t, err)
	require.True(t, newSync)
	require.NotEqual(t, oldID, id)
	require.Empty(t, resourceTypeIDs(t, e))
}

// TestReopenedUnfinishedSyncIsNotFresh answers whether ResetForNewSync's
// IsFreshSync refusal blocks the wipe after a restart. The fresh flag is
// in-memory only: Open starts unbound, and InitCurrentSync binds through
// SetCurrentSync, which clears it. So a reopened unfinished sync can be reset.
func TestReopenedUnfinishedSyncIsNotFresh(t *testing.T) {
	ctx := context.Background()
	dbDir := filepath.Join(t.TempDir(), "db")
	oldID := interruptSyncWithVersion(t, dbDir, "v1")

	e := reopenWithVersion(t, dbDir, "v2")
	require.Equal(t, oldID, e.CurrentSyncID(), "InitCurrentSync binds the unfinished sync")
	require.False(t, e.IsFreshSync(), "a reopened unfinished sync must not be flagged fresh")
	require.NoError(t, e.ResetForNewSync(ctx))
	require.Empty(t, resourceTypeIDs(t, e))
}
