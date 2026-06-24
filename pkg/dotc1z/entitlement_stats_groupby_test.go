package dotc1z

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// writeEntitlementStatsC1Z builds a SQLite-backed c1z with entitlements
// spread across multiple resource types (and matching grants), returning
// the file and the sync id.
func writeEntitlementStatsC1Z(t *testing.T, ctx context.Context) (*C1File, string) {
	t.Helper()
	testFilePath := filepath.Join(t.TempDir(), "ent-stats.c1z")
	f, err := NewC1ZFile(ctx, testFilePath, WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close(ctx) })

	syncID, err := f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	// group: 2 entitlements, user: 1 entitlement, app: 3 entitlements.
	specs := []struct {
		rt    string
		count int
	}{
		{"group", 2},
		{"user", 1},
		{"app", 3},
	}
	for _, s := range specs {
		require.NoError(t, f.PutResourceTypes(ctx, v2.ResourceType_builder{Id: s.rt}.Build()))
		res := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: s.rt, Resource: "r"}.Build()}.Build()
		require.NoError(t, f.PutResources(ctx, res))
		for i := 0; i < s.count; i++ {
			ent := v2.Entitlement_builder{
				Id:       s.rt + "-ent-" + string(rune('a'+i)),
				Resource: res,
			}.Build()
			require.NoError(t, f.PutEntitlements(ctx, ent))
			// One grant per entitlement so grant grouping is non-trivial too.
			require.NoError(t, f.PutGrants(ctx, v2.Grant_builder{
				Id:          s.rt + "-grant-" + string(rune('a'+i)),
				Entitlement: ent,
				Principal:   v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: s.rt, Resource: "p"}.Build()}.Build(),
			}.Build()))
		}
	}
	require.NoError(t, f.EndSync(ctx))
	return f, syncID
}

// TestC1ZEntitlementStatsByResourceType verifies the SQLite stats path
// groups entitlement counts per resource type (via the shared
// countBySyncAndResourceType GROUP BY) and that the Entitlements total
// equals the sum of the per-type counts.
func TestC1ZEntitlementStatsByResourceType(t *testing.T) {
	ctx := t.Context()
	f, syncID := writeEntitlementStatsC1Z(t, ctx)

	_, stats, err := f.stats(ctx, connectorstore.SyncTypeFull, syncID, false)
	require.NoError(t, err)

	wantByRT := map[string]int64{"group": 2, "user": 1, "app": 3}
	require.Equal(t, wantByRT, stats.GetEntitlementsByResourceType(), "entitlements_by_resource_type")
	require.Equal(t, int64(6), stats.GetEntitlements(), "entitlements total")

	var sum int64
	for _, n := range stats.GetEntitlementsByResourceType() {
		sum += n
	}
	require.Equal(t, stats.GetEntitlements(), sum, "total must equal sum of per-type counts")

	// Grants are grouped by the entitlement's resource type and should
	// mirror the entitlement distribution for this fixture.
	require.Equal(t, wantByRT, stats.GetGrantsByResourceType(), "grants_by_resource_type")
}

// TestC1ZEntitlementStatsResourcesOnly confirms a resources-only sync
// computes no entitlement grouping.
func TestC1ZEntitlementStatsResourcesOnly(t *testing.T) {
	ctx := t.Context()
	testFilePath := filepath.Join(t.TempDir(), "ent-stats-ro.c1z")
	f, err := NewC1ZFile(ctx, testFilePath, WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close(ctx) })

	syncID, err := f.StartNewSync(ctx, connectorstore.SyncTypeResourcesOnly, "")
	require.NoError(t, err)
	require.NoError(t, f.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "group"}.Build()))
	require.NoError(t, f.PutResources(ctx, v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "group", Resource: "r"}.Build(),
	}.Build()))
	require.NoError(t, f.EndSync(ctx))

	_, stats, err := f.stats(ctx, connectorstore.SyncTypeResourcesOnly, syncID, false)
	require.NoError(t, err)
	require.Empty(t, stats.GetEntitlementsByResourceType(), "resources-only must not group entitlements")
	require.Zero(t, stats.GetEntitlements())
}

// TestStatsToMapKeySet pins the flat output of statsToMap: only
// resource_types, the per-resource-type resource counts, and (for
// non-resources-only syncs) entitlements/grants totals. The per-type
// _entitlement_count / _grant_count keys are intentionally NOT emitted;
// per-type aggregation is exposed only through the SyncStats proto map
// fields.
func TestStatsToMapKeySet(t *testing.T) {
	stats := reader_v2.SyncStats_builder{
		ResourceTypes:              2,
		Resources:                  3,
		Entitlements:               6,
		Grants:                     6,
		ResourcesByResourceType:    map[string]int64{"group": 1, "user": 2},
		EntitlementsByResourceType: map[string]int64{"group": 2, "user": 4},
		GrantsByResourceType:       map[string]int64{"group": 2, "user": 4},
	}.Build()

	t.Run("full sync", func(t *testing.T) {
		got := statsToMap(stats, connectorstore.SyncTypeFull)
		require.Equal(t, map[string]int64{
			"resource_types": 2,
			"entitlements":   6,
			"grants":         6,
			"group":          1,
			"user":           2,
		}, got)
	})

	t.Run("resources only", func(t *testing.T) {
		got := statsToMap(stats, connectorstore.SyncTypeResourcesOnly)
		require.Equal(t, map[string]int64{
			"resource_types": 2,
			"group":          1,
			"user":           2,
		}, got)
	})
}
