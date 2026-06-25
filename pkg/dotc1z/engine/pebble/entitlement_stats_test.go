package pebble

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

func mkV2Entitlement(id, resourceTypeID, resourceID string) *v2.Entitlement {
	return v2.Entitlement_builder{
		Id: id,
		Resource: v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: resourceTypeID, Resource: resourceID}.Build(),
		}.Build(),
		Purpose: v2.Entitlement_PURPOSE_VALUE_PERMISSION,
	}.Build()
}

// TestSidecarEntitlementsByResourceType drives a full sync with
// entitlements spread across two resource types and asserts the sidecar
// compute path (computeSyncStats → scanEntitlementResourceTypeRaw)
// groups entitlement counts per resource type and that the total equals
// the sum.
func TestSidecarEntitlementsByResourceType(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	store := NewAdapter(e)
	syncID, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err, "StartNewSync")

	require.NoError(t, store.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "group"}.Build(),
		v2.ResourceType_builder{Id: "user"}.Build(),
	), "PutResourceTypes")
	require.NoError(t, store.PutResources(ctx,
		v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "eng"}.Build()}.Build(),
		v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "alice"}.Build()}.Build(),
	), "PutResources")
	require.NoError(t, store.PutEntitlements(ctx,
		mkV2Entitlement("ent-admin", "group", "eng"),
		mkV2Entitlement("ent-member", "group", "eng"),
		mkV2Entitlement("ent-view", "user", "alice"),
	), "PutEntitlements")
	require.NoError(t, store.EndSync(ctx), "EndSync")

	stats, err := e.readSyncStats(ctx, syncID)
	require.NoError(t, err, "readSyncStats")
	require.NotNil(t, stats, "sidecar missing after EndSync")

	require.Equal(t, int64(3), stats.GetEntitlements(), "entitlements total")
	require.Equal(t, map[string]int64{"group": 2, "user": 1}, stats.GetEntitlementsByResourceType())

	_ = store.Close(ctx)
}

// TestBulkImportComputedStatsEntitlementsByResourceType exercises the
// streaming bulk-import accumulation of entitlementsByRT and its
// rendering into ComputedStats().
func TestBulkImportComputedStatsEntitlementsByResourceType(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	// StartBulkSyncImport requires a fresh sync that is the engine's
	// current sync; the Adapter's StartNewSync sets that up.
	store := NewAdapter(e)
	syncID, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err, "StartNewSync")

	b, err := e.StartBulkSyncImport(ctx, syncID, t.TempDir())
	require.NoError(t, err, "StartBulkSyncImport")
	defer b.Abort()

	require.NoError(t, b.AddResourceTypes(ctx,
		v2.ResourceType_builder{Id: "group"}.Build(),
		v2.ResourceType_builder{Id: "user"}.Build(),
	), "AddResourceTypes")
	require.NoError(t, b.AddResources(ctx,
		v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "eng"}.Build()}.Build(),
		v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "alice"}.Build()}.Build(),
		v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "bob"}.Build()}.Build(),
	), "AddResources")
	// Entitlements must arrive sorted by external id.
	require.NoError(t, b.AddEntitlements(ctx,
		mkV2Entitlement("ent-admin", "group", "eng"),
		mkV2Entitlement("ent-member", "group", "eng"),
		mkV2Entitlement("ent-view", "user", "alice"),
	), "AddEntitlements")

	// ComputedStats is valid only after all grant shards are closed.
	shard, err := b.NewGrantShard()
	require.NoError(t, err, "NewGrantShard")
	shard.Close()

	stats := b.ComputedStats()
	require.Equal(t, int64(3), stats.GetEntitlements(), "entitlements total")
	require.Equal(t, map[string]int64{"group": 2, "user": 1}, stats.GetEntitlementsByResourceType())
}

// TestSyncStatsFromRecordMapsEntitlementsByResourceType pins the
// engine→reader conversion: the new EntitlementsByResourceType field is
// carried straight through, while grants are aliased from the
// by-entitlement-resource-type record field (existing behavior).
func TestSyncStatsFromRecordMapsEntitlementsByResourceType(t *testing.T) {
	rec := v3.SyncStatsRecord_builder{
		ResourceTypes:                   2,
		Resources:                       3,
		Entitlements:                    3,
		Grants:                          5,
		ResourcesByResourceType:         map[string]int64{"group": 1, "user": 2},
		EntitlementsByResourceType:      map[string]int64{"group": 2, "user": 1},
		GrantsByEntitlementResourceType: map[string]int64{"group": 5},
	}.Build()

	got := SyncStatsFromRecord(rec)
	require.NotNil(t, got)
	require.Equal(t, int64(3), got.GetEntitlements())
	require.Equal(t, map[string]int64{"group": 2, "user": 1}, got.GetEntitlementsByResourceType())
	require.Equal(t, map[string]int64{"group": 1, "user": 2}, got.GetResourcesByResourceType())
	require.Equal(t, map[string]int64{"group": 5}, got.GetGrantsByResourceType(), "grants aliased from by-entitlement-RT")

	require.Nil(t, SyncStatsFromRecord(nil), "nil record → nil stats")
}
