package pebble

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// TestSyncStatsSidecarRoundtrip writes a small full sync through the
// Adapter and confirms that Stats() returns the cached sidecar values
// (one Get) and that the values match what a fresh iteration would
// produce.
func TestSyncStatsSidecarRoundtrip(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	store := NewAdapter(e)
	syncID, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoErrorf(t, err, "StartNewSync")
	require.NoErrorf(t, store.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "user"}.Build(),
		v2.ResourceType_builder{Id: "group"}.Build(),
	), "PutResourceTypes")
	require.NoErrorf(t, store.PutResources(ctx,
		v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build()}.Build(),
		v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u2"}.Build()}.Build(),
		v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build()}.Build(),
	), "PutResources")
	require.NoErrorf(t, store.PutGrants(ctx,
		mkV2Grant("gr1", "ent-A", "user", "u1"),
		mkV2Grant("gr2", "ent-A", "user", "u2"),
		mkV2Grant("gr3", "ent-B", "group", "g1"),
	), "PutGrants")
	require.NoErrorf(t, store.EndSync(ctx), "EndSync")

	// Sidecar should now exist. Read it directly.
	stats, err := e.readSyncStats(ctx, syncID)
	require.NoErrorf(t, err, "readSyncStats")
	require.NotNil(t, stats, "sidecar missing after EndSync")
	require.Equal(t, int64(3), stats.GetResources(), "resources")
	require.Equal(t, int64(3), stats.GetGrants(), "grants")
	require.Equal(t, int64(2), stats.GetResourceTypes(), "resource_types")
	require.Equal(t, int64(2), stats.GetResourcesByResourceType()["user"], "resources[user]")
	require.Equal(t, int64(1), stats.GetResourcesByResourceType()["group"], "resources[group]")

	// Stats() through the adapter must match the sidecar.
	m, err := store.SyncMeta().Stats(ctx, connectorstore.SyncTypeAny, syncID)
	require.NoErrorf(t, err, "Stats")
	require.Equal(t, int64(3), m["resources"], "Stats resources")
	require.Equal(t, int64(3), m["grants"], "Stats grants")
	require.Equal(t, int64(2), m["resource_types"], "Stats resource_types")
	require.Equal(t, int64(2), m["user"], "Stats user count")
	require.Equal(t, int64(1), m["group"], "Stats group count")
	_ = store.Close(ctx)
}

// TestSyncStatsSidecarBackfillOnOpen exercises the migration path:
// write a c1z, surgically delete the sidecar key + applied-version
// stamp, re-open, and verify that the migration runs and the
// sidecar reappears.
func TestSyncStatsSidecarBackfillOnOpen(t *testing.T) {
	// Skipped: the indexMigrations registry is intentionally empty
	// (no existing Pebble data to backfill), so the sync_stats_sidecar
	// backfill is not registered and on-Open migration is a no-op. The
	// sidecar's normal write path (PersistSyncStats at EndSync) is
	// still covered by TestSyncStatsSidecarRoundtrip. Re-enable when a
	// migration is added back to the registry.
	t.Skip("indexMigrations registry intentionally empty: no existing Pebble data to backfill")

	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "engine")

	e, err := Open(ctx, dir)
	require.NoErrorf(t, err, "Open")
	syncID := ksuid.New().String()
	require.NoErrorf(t, e.MarkFreshSync(syncID), "MarkFreshSync")
	// Need a SyncRunRecord so collectSyncIDs (used by the migration)
	// can find this sync during backfill.
	require.NoErrorf(t, e.PutSyncRunRecord(ctx, v3.SyncRunRecord_builder{
		SyncId: syncID,
		Type:   v3.SyncType_SYNC_TYPE_FULL,
	}.Build()), "PutSyncRunRecord")
	require.NoErrorf(t, e.PutGrantRecord(ctx, makeGrant(syncID, "g1", "ent-A", "alice")), "PutGrantRecord")
	require.NoErrorf(t, e.PersistSyncStats(ctx, syncID), "PersistSyncStats")
	// Surgically delete the sidecar and the migration's applied-version
	// stamp so the next Open's migrator re-runs.
	require.NoError(t, e.db.UnsafeForTesting().Delete(encodeSyncStatsKey(), nil))
	require.NoError(t, e.db.UnsafeForTesting().Delete(encodeIndexAppliedKey("sync_stats_sidecar"), nil))
	require.NoErrorf(t, e.Close(), "Close")

	// Re-open. The migration framework should backfill the sidecar.
	e2, err := Open(ctx, dir)
	require.NoErrorf(t, err, "Open 2")
	defer e2.Close()
	stats, err := e2.readSyncStats(ctx, syncID)
	require.NoErrorf(t, err, "readSyncStats")
	require.NotNil(t, stats, "sidecar not backfilled by on-Open migration")
	require.Equal(t, int64(1), stats.GetGrants(), "backfilled grants")
	v, err := e2.readAppliedIndexVersion("sync_stats_sidecar")
	require.NoErrorf(t, err, "readAppliedIndexVersion")
	require.Equal(t, 1, v, "applied-version after migration")
}

// TestSyncStatsSidecarFallback verifies that a sync without a
// sidecar (e.g. partial sync that bypassed EndFreshSync) falls
// through to the iteration path correctly.
func TestSyncStatsSidecarFallback(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	syncID := ksuid.New().String()
	require.NoError(t, e.MarkFreshSync(syncID))
	require.NoError(t, e.PutGrantRecord(ctx, makeGrant(syncID, "g1", "ent", "u1")))
	// Don't call PersistSyncStats — leaves the sidecar missing.

	// The Adapter.statsFromIteration helper still produces correct
	// counts. We exercise it directly to avoid the Stats() fast-path.
	a := &Adapter{engine: e}
	got, err := a.statsFromIteration(ctx, syncID)
	require.NoErrorf(t, err, "statsFromIteration")
	require.Equal(t, int64(1), got.GetGrants(), "fallback grants")
}

var _ = v3.SyncStatsRecord{} // keep import used in test
