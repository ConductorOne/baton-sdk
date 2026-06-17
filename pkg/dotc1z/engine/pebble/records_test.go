package pebble

import (
	"context"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
)

func TestPutGetResourceType(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	syncID := ksuid.New().String()
	require.NoError(t, e.SetCurrentSync(syncID))

	r := v3.ResourceTypeRecord_builder{
		ExternalId:   "user",
		DisplayName:  "User",
		Traits:       []string{"trait-user"},
		DiscoveredAt: timestamppb.Now(),
	}.Build()
	require.NoErrorf(t, e.PutResourceTypeRecord(ctx, r), "PutResourceTypeRecord")
	got, err := e.GetResourceTypeRecord(ctx, "user")
	require.NoErrorf(t, err, "GetResourceTypeRecord")
	require.Equal(t, "User", got.GetDisplayName(), "display_name")
}

func TestIterateResourceTypesBySync(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	syncID := ksuid.New().String()
	require.NoError(t, e.SetCurrentSync(syncID))
	for i := 0; i < 20; i++ {
		r := v3.ResourceTypeRecord_builder{
			ExternalId:  ksuid.New().String(),
			DisplayName: "RT",
		}.Build()
		require.NoError(t, e.PutResourceTypeRecord(ctx, r))
	}
	count := 0
	require.NoError(t, e.IterateResourceTypes(ctx, func(*v3.ResourceTypeRecord) bool {
		count++
		return true
	}))
	require.Equal(t, 20, count, "resource_types count")
}

func TestResourceWithParentIndex(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	syncID := ksuid.New().String()
	require.NoError(t, e.SetCurrentSync(syncID))

	parent := v3.ResourceRecord_builder{
		ResourceTypeId: "group",
		ResourceId:     "admins",
		DisplayName:    "Admins",
	}.Build()
	require.NoError(t, e.PutResourceRecord(ctx, parent))

	// 5 user resources with `parent` pointing at the admins group.
	for i := 0; i < 5; i++ {
		child := v3.ResourceRecord_builder{
			ResourceTypeId: "user",
			ResourceId:     ksuid.New().String(),
			DisplayName:    "User",
			Parent: v3.ResourceRef_builder{
				ResourceTypeId: "group",
				ResourceId:     "admins",
			}.Build(),
		}.Build()
		require.NoError(t, e.PutResourceRecord(ctx, child))
	}
	// 3 resources NOT under the admins group.
	for i := 0; i < 3; i++ {
		other := v3.ResourceRecord_builder{
			ResourceTypeId: "user",
			ResourceId:     ksuid.New().String(),
		}.Build()
		require.NoError(t, e.PutResourceRecord(ctx, other))
	}

	childCount := 0
	require.NoError(t, e.IterateResourcesByParent(ctx, "group", "admins", func(*v3.ResourceRecord) bool {
		childCount++
		return true
	}))
	require.Equal(t, 5, childCount, "IterateResourcesByParent")

	// Total resources: parent + 5 children + 3 others = 9.
	total := 0
	require.NoError(t, e.IterateResources(ctx, func(*v3.ResourceRecord) bool {
		total++
		return true
	}))
	require.Equal(t, 9, total, "IterateResourcesBySync")
}

func TestEntitlementByResourceIndex(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	syncID := ksuid.New().String()
	require.NoError(t, e.SetCurrentSync(syncID))

	// 4 entitlements on group/admins, 2 on group/devs.
	for i := 0; i < 4; i++ {
		r := v3.EntitlementRecord_builder{
			ExternalId: ksuid.New().String(),
			Resource: v3.ResourceRef_builder{
				ResourceTypeId: "group",
				ResourceId:     "admins",
			}.Build(),
			DisplayName: "ent",
		}.Build()
		require.NoError(t, e.PutEntitlementRecord(ctx, r))
	}
	for i := 0; i < 2; i++ {
		r := v3.EntitlementRecord_builder{
			ExternalId: ksuid.New().String(),
			Resource: v3.ResourceRef_builder{
				ResourceTypeId: "group",
				ResourceId:     "devs",
			}.Build(),
		}.Build()
		require.NoError(t, e.PutEntitlementRecord(ctx, r))
	}

	var a, d int
	require.NoError(t, e.IterateEntitlementsByResource(ctx, "group", "admins", func(*v3.EntitlementRecord) bool {
		a++
		return true
	}))
	require.NoError(t, e.IterateEntitlementsByResource(ctx, "group", "devs", func(*v3.EntitlementRecord) bool {
		d++
		return true
	}))
	require.Equal(t, 4, a, "admins count")
	require.Equal(t, 2, d, "devs count")
}

func TestAssetRoundtrip(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	syncID := ksuid.New().String()
	require.NoError(t, e.SetCurrentSync(syncID))

	payload := []byte("PNGfakebytes\x00\x01\x02")
	r := v3.AssetRecord_builder{
		SyncId:      syncID,
		ExternalId:  "icon-1",
		ContentType: "image/png",
		Data:        payload,
	}.Build()
	require.NoError(t, e.PutAssetRecord(ctx, r))
	got, err := e.GetAssetRecord(ctx, "icon-1")
	require.NoErrorf(t, err, "GetAssetRecord")
	require.Equal(t, "image/png", got.GetContentType(), "content_type")
	require.Equal(t, string(payload), string(got.GetData()), "data roundtrip lost bytes")
}

func TestSyncRunRecord(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)

	id1 := ksuid.New().String()
	id2 := ksuid.New().String()
	put := func(sid string) {
		r := v3.SyncRunRecord_builder{
			SyncId:    sid,
			Type:      v3.SyncType_SYNC_TYPE_FULL,
			StartedAt: timestamppb.Now(),
		}.Build()
		require.NoErrorf(t, e.PutSyncRunRecord(ctx, r), "PutSyncRunRecord(%s)", sid)
	}

	// One sync-run record per file: the key is fixed, so writing id2
	// replaces id1.
	put(id1)
	got, err := e.GetSyncRunRecord(ctx, id1)
	require.NoErrorf(t, err, "GetSyncRunRecord(id1)")
	require.Equal(t, id1, got.GetSyncId(), "sync_id")

	put(id2)
	// id1 is gone (replaced); the id-match guard makes its lookup miss.
	_, err = e.GetSyncRunRecord(ctx, id1)
	require.ErrorIs(t, err, pebble.ErrNotFound, "GetSyncRunRecord(id1) after replacement")
	_, err = e.GetSyncRunRecord(ctx, id2)
	require.NoError(t, err, "GetSyncRunRecord(id2)")
	count := 0
	require.NoError(t, e.IterateAllSyncRuns(ctx, func(*v3.SyncRunRecord) bool {
		count++
		return true
	}))
	require.Equal(t, 1, count, "sync_runs count")

	require.NoError(t, e.DeleteSyncRunRecord(ctx, id2))
	_, err = e.GetSyncRunRecord(ctx, id2)
	require.ErrorIs(t, err, pebble.ErrNotFound, "expected ErrNotFound after delete")
}
