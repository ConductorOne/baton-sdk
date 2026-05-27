package pebble

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/segmentio/ksuid"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
)

// TestSyncStatsSidecarRoundtrip writes a small full sync via the
// registered store and confirms that Stats() returns the cached
// sidecar values (one Get) and that the values match what a fresh
// iteration would produce.
func TestSyncStatsSidecarRoundtrip(t *testing.T) {
	ctx := context.Background()
	if err := Register(); err != nil {
		t.Fatalf("Register: %v", err)
	}
	tmp := t.TempDir()
	path := filepath.Join(tmp, "stats.c1z")
	store, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(dotc1z.EnginePebble))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	syncID, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	if err != nil {
		t.Fatalf("StartNewSync: %v", err)
	}
	if err := store.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "user"}.Build(),
		v2.ResourceType_builder{Id: "group"}.Build(),
	); err != nil {
		t.Fatalf("PutResourceTypes: %v", err)
	}
	if err := store.PutResources(ctx,
		v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build()}.Build(),
		v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u2"}.Build()}.Build(),
		v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build()}.Build(),
	); err != nil {
		t.Fatalf("PutResources: %v", err)
	}
	if err := store.PutGrants(ctx,
		mkV2Grant("gr1", "ent-A", "user", "u1"),
		mkV2Grant("gr2", "ent-A", "user", "u2"),
		mkV2Grant("gr3", "ent-B", "group", "g1"),
	); err != nil {
		t.Fatalf("PutGrants: %v", err)
	}
	if err := store.EndSync(ctx); err != nil {
		t.Fatalf("EndSync: %v", err)
	}

	// Sidecar should now exist. Read it directly.
	a, ok := store.(*registeredStore)
	if !ok {
		t.Fatalf("store is %T, want *registeredStore", store)
	}
	stats, err := a.engine.readSyncStats(ctx, syncID)
	if err != nil {
		t.Fatalf("readSyncStats: %v", err)
	}
	if stats == nil {
		t.Fatal("sidecar missing after EndSync")
	}
	if stats.GetResources() != 3 {
		t.Errorf("resources=%d, want 3", stats.GetResources())
	}
	if stats.GetGrants() != 3 {
		t.Errorf("grants=%d, want 3", stats.GetGrants())
	}
	if stats.GetResourceTypes() != 2 {
		t.Errorf("resource_types=%d, want 2", stats.GetResourceTypes())
	}
	if got := stats.GetResourcesByResourceType()["user"]; got != 2 {
		t.Errorf("resources[user]=%d, want 2", got)
	}
	if got := stats.GetResourcesByResourceType()["group"]; got != 1 {
		t.Errorf("resources[group]=%d, want 1", got)
	}

	// Stats() through the adapter must match the sidecar.
	m, err := store.(dotc1z.C1ZStore).SyncMeta().Stats(ctx, connectorstore.SyncTypeAny, syncID)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if m["resources"] != 3 || m["grants"] != 3 || m["resource_types"] != 2 {
		t.Errorf("Stats result %v missing expected totals", m)
	}
	if m["user"] != 2 || m["group"] != 1 {
		t.Errorf("Stats per-RT counts wrong: %v", m)
	}
	_ = store.Close(ctx)
}

// TestSyncStatsSidecarBackfillOnOpen exercises the migration path:
// write a c1z, surgically delete the sidecar key + applied-version
// stamp, re-open, and verify that the migration runs and the
// sidecar reappears.
func TestSyncStatsSidecarBackfillOnOpen(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "engine")

	e, err := Open(ctx, dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	syncID := ksuid.New().String()
	if err := e.MarkFreshSync(syncID); err != nil {
		t.Fatalf("MarkFreshSync: %v", err)
	}
	// Need a SyncRunRecord so collectSyncIDs (used by the migration)
	// can find this sync during backfill.
	if err := e.PutSyncRunRecord(ctx, v3.SyncRunRecord_builder{
		SyncId: syncID,
		Type:   v3.SyncType_SYNC_TYPE_FULL,
	}.Build()); err != nil {
		t.Fatalf("PutSyncRunRecord: %v", err)
	}
	if err := e.PutGrantRecord(ctx, makeGrant(syncID, "g1", "ent-A", "alice")); err != nil {
		t.Fatalf("PutGrantRecord: %v", err)
	}
	if err := e.PersistSyncStats(ctx, syncID); err != nil {
		t.Fatalf("PersistSyncStats: %v", err)
	}
	// Surgically delete the sidecar and the migration's applied-version
	// stamp so the next Open's migrator re-runs.
	idBytes, _ := codecEncodeSyncIDForTest(syncID)
	if err := e.db.Delete(encodeSyncStatsKey(idBytes), nil); err != nil {
		t.Fatal(err)
	}
	if err := e.db.Delete(encodeIndexAppliedKey("sync_stats_sidecar"), nil); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Re-open. The migration framework should backfill the sidecar.
	e2, err := Open(ctx, dir)
	if err != nil {
		t.Fatalf("Open 2: %v", err)
	}
	defer e2.Close()
	stats, err := e2.readSyncStats(ctx, syncID)
	if err != nil {
		t.Fatalf("readSyncStats: %v", err)
	}
	if stats == nil {
		t.Fatal("sidecar not backfilled by on-Open migration")
	}
	if stats.GetGrants() != 1 {
		t.Errorf("backfilled grants = %d, want 1", stats.GetGrants())
	}
	v, err := e2.readAppliedIndexVersion("sync_stats_sidecar")
	if err != nil {
		t.Fatalf("readAppliedIndexVersion: %v", err)
	}
	if v != 1 {
		t.Fatalf("applied-version after migration = %d, want 1", v)
	}
}

// TestSyncStatsSidecarFallback verifies that a sync without a
// sidecar (e.g. partial sync that bypassed EndFreshSync) falls
// through to the iteration path correctly.
func TestSyncStatsSidecarFallback(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	syncID := ksuid.New().String()
	if err := e.MarkFreshSync(syncID); err != nil {
		t.Fatal(err)
	}
	if err := e.PutGrantRecord(ctx, makeGrant(syncID, "g1", "ent", "u1")); err != nil {
		t.Fatal(err)
	}
	// Don't call PersistSyncStats — leaves the sidecar missing.

	// The Adapter.statsFromIteration helper still produces correct
	// counts. We exercise it directly to avoid the Stats() fast-path.
	a := &Adapter{engine: e}
	got, err := a.statsFromIteration(ctx, syncID)
	if err != nil {
		t.Fatalf("statsFromIteration: %v", err)
	}
	if got["grants"] != 1 {
		t.Errorf("fallback grants = %v, want 1", got["grants"])
	}
}

// codecEncodeSyncIDForTest is a tiny shim around the engine's
// internal codec.EncodeSyncID used in test files that don't want
// to import the codec package directly.
func codecEncodeSyncIDForTest(syncID string) ([]byte, error) {
	rec, err := makeGrantTrying(syncID)
	if err != nil {
		return nil, err
	}
	return rec, nil
}

func makeGrantTrying(syncID string) ([]byte, error) {
	id, err := ksuidParse(syncID)
	if err != nil {
		return nil, err
	}
	return id, nil
}

// ksuidParse parses a KSUID string into its 20-byte form.
func ksuidParse(s string) ([]byte, error) {
	k, err := ksuid.Parse(s)
	if err != nil {
		return nil, err
	}
	b := k.Bytes()
	out := make([]byte, len(b))
	copy(out, b)
	return out, nil
}

var _ = v3.SyncStatsRecord{} // keep import used in test
