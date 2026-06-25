package pebble

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
)

// TestMergeStatsAccumulatorGroupEntitlement covers the per-resource-type
// entitlement grouping the accumulator now tracks alongside resources
// and grants.
func TestMergeStatsAccumulatorGroupEntitlement(t *testing.T) {
	a := newMergeStatsAccumulator()
	a.groupEntitlement([]byte("group"))
	a.groupEntitlement([]byte("group"))
	a.groupEntitlement([]byte("user"))

	got := a.record().GetEntitlementsByResourceType()
	require.Equal(t, map[string]int64{"group": 2, "user": 1}, got)
}

// TestMergeStatsAccumulatorRegroupEntitlement covers the overlay
// replacement path that moves an entitlement between resource-type
// groups when a replacement value carries a different resource type.
// Totals are owned elsewhere, so only the grouping moves.
func TestMergeStatsAccumulatorRegroupEntitlement(t *testing.T) {
	t.Run("moves count between types", func(t *testing.T) {
		a := newMergeStatsAccumulator()
		a.groupEntitlement([]byte("group"))
		a.groupEntitlement([]byte("group"))

		a.regroupEntitlement([]byte("group"), []byte("user"))

		got := a.record().GetEntitlementsByResourceType()
		require.Equal(t, map[string]int64{"group": 1, "user": 1}, got)
	})

	t.Run("no-op when types equal", func(t *testing.T) {
		a := newMergeStatsAccumulator()
		a.groupEntitlement([]byte("group"))

		a.regroupEntitlement([]byte("group"), []byte("group"))

		got := a.record().GetEntitlementsByResourceType()
		require.Equal(t, map[string]int64{"group": 1}, got)
	})

	t.Run("nil accumulator is safe", func(t *testing.T) {
		var a *mergeStatsAccumulator
		require.NotPanics(t, func() {
			a.groupEntitlement([]byte("group"))
			a.regroupEntitlement([]byte("group"), []byte("user"))
		})
	})
}

// TestMergeStatsAccumulatorResetBucketEntitlements pins that clearing
// the entitlements bucket only touches the entitlement grouping — the
// resources and grants groupings are per-record-type maps and must
// survive.
func TestMergeStatsAccumulatorResetBucketEntitlements(t *testing.T) {
	a := newMergeStatsAccumulator()
	a.groupResource([]byte("user"))
	a.groupEntitlement([]byte("group"))
	a.groupGrant([]byte("group"))

	a.resetBucket(runBucketEntitlements)

	rec := a.record()
	require.Empty(t, rec.GetEntitlementsByResourceType(), "entitlements grouping should be cleared")
	require.Equal(t, map[string]int64{"user": 1}, rec.GetResourcesByResourceType(), "resources untouched")
	require.Equal(t, map[string]int64{"group": 1}, rec.GetGrantsByEntitlementResourceType(), "grants untouched")
}

// TestMergeStatsAccumulatorCountWinnerEntitlement covers the K-way path
// that derives an entitlement's resource type from its marshaled value.
func TestMergeStatsAccumulatorCountWinnerEntitlement(t *testing.T) {
	rec := v3.EntitlementRecord_builder{
		ExternalId: "member",
		Resource:   v3.ResourceRef_builder{ResourceTypeId: "group", ResourceId: "engineering"}.Build(),
	}.Build()
	value, err := proto.Marshal(rec)
	require.NoError(t, err)

	a := newMergeStatsAccumulator()
	require.NoError(t, a.countWinner(entitlementBucket(), nil, nil, value))
	require.NoError(t, a.countWinner(entitlementBucket(), nil, nil, value))

	out := a.record()
	require.Equal(t, int64(2), out.GetEntitlements(), "entitlement total")
	require.Equal(t, map[string]int64{"group": 2}, out.GetEntitlementsByResourceType())
}

// TestMergeStatsAccumulatorAddRecordEntitlements covers summing
// entitlement groupings across pre-computed source records (the merge's
// whole-source fast path), exercising both the new-key and
// existing-key branches.
func TestMergeStatsAccumulatorAddRecordEntitlements(t *testing.T) {
	a := newMergeStatsAccumulator()
	a.addRecord(v3.SyncStatsRecord_builder{
		Entitlements:               3,
		EntitlementsByResourceType: map[string]int64{"group": 2, "user": 1},
	}.Build())
	a.addRecord(v3.SyncStatsRecord_builder{
		Entitlements:               2,
		EntitlementsByResourceType: map[string]int64{"group": 1, "app": 1},
	}.Build())

	out := a.record()
	require.Equal(t, int64(5), out.GetEntitlements())
	require.Equal(t, map[string]int64{"group": 3, "user": 1, "app": 1}, out.GetEntitlementsByResourceType())
}
