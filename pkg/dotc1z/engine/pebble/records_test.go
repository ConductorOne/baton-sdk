//go:build batonsdkv2

package pebble

import (
	"context"
	"errors"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/segmentio/ksuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
)

func TestPutGetResourceType(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	syncID := ksuid.New().String()
	if err := e.SetCurrentSync(syncID); err != nil {
		t.Fatal(err)
	}

	r := v3.ResourceTypeRecord_builder{
		SyncId:       syncID,
		ExternalId:   "user",
		DisplayName:  "User",
		Traits:       []string{"trait-user"},
		DiscoveredAt: timestamppb.Now(),
	}.Build()
	if err := e.PutResourceTypeRecord(ctx, r); err != nil {
		t.Fatalf("PutResourceTypeRecord: %v", err)
	}
	got, err := e.GetResourceTypeRecord(ctx, syncID, "user")
	if err != nil {
		t.Fatalf("GetResourceTypeRecord: %v", err)
	}
	if got.GetDisplayName() != "User" {
		t.Errorf("display_name: got %q", got.GetDisplayName())
	}
}

func TestIterateResourceTypesBySync(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	syncID := ksuid.New().String()
	if err := e.SetCurrentSync(syncID); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		r := v3.ResourceTypeRecord_builder{
			SyncId:      syncID,
			ExternalId:  ksuid.New().String(),
			DisplayName: "RT",
		}.Build()
		if err := e.PutResourceTypeRecord(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	count := 0
	if err := e.IterateResourceTypesBySync(ctx, syncID, func(*v3.ResourceTypeRecord) bool {
		count++
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if count != 20 {
		t.Errorf("got %d resource_types, want 20", count)
	}
}

func TestResourceWithParentIndex(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	syncID := ksuid.New().String()
	if err := e.SetCurrentSync(syncID); err != nil {
		t.Fatal(err)
	}

	parent := v3.ResourceRecord_builder{
		SyncId:         syncID,
		ResourceTypeId: "group",
		ResourceId:     "admins",
		DisplayName:    "Admins",
	}.Build()
	if err := e.PutResourceRecord(ctx, parent); err != nil {
		t.Fatal(err)
	}

	// 5 user resources with `parent` pointing at the admins group.
	for i := 0; i < 5; i++ {
		child := v3.ResourceRecord_builder{
			SyncId:         syncID,
			ResourceTypeId: "user",
			ResourceId:     ksuid.New().String(),
			DisplayName:    "User",
			Parent: v3.ResourceRef_builder{
				ResourceTypeId: "group",
				ResourceId:     "admins",
			}.Build(),
		}.Build()
		if err := e.PutResourceRecord(ctx, child); err != nil {
			t.Fatal(err)
		}
	}
	// 3 resources NOT under the admins group.
	for i := 0; i < 3; i++ {
		other := v3.ResourceRecord_builder{
			SyncId:         syncID,
			ResourceTypeId: "user",
			ResourceId:     ksuid.New().String(),
		}.Build()
		if err := e.PutResourceRecord(ctx, other); err != nil {
			t.Fatal(err)
		}
	}

	childCount := 0
	if err := e.IterateResourcesByParent(ctx, syncID, "group", "admins", func(*v3.ResourceRecord) bool {
		childCount++
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if childCount != 5 {
		t.Errorf("IterateResourcesByParent: got %d, want 5", childCount)
	}

	// Total resources: parent + 5 children + 3 others = 9.
	total := 0
	if err := e.IterateResourcesBySync(ctx, syncID, func(*v3.ResourceRecord) bool {
		total++
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if total != 9 {
		t.Errorf("IterateResourcesBySync: got %d, want 9", total)
	}
}

func TestEntitlementByResourceIndex(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	syncID := ksuid.New().String()
	if err := e.SetCurrentSync(syncID); err != nil {
		t.Fatal(err)
	}

	// 4 entitlements on group/admins, 2 on group/devs.
	for i := 0; i < 4; i++ {
		r := v3.EntitlementRecord_builder{
			SyncId:     syncID,
			ExternalId: ksuid.New().String(),
			Resource: v3.ResourceRef_builder{
				ResourceTypeId: "group",
				ResourceId:     "admins",
			}.Build(),
			DisplayName: "ent",
		}.Build()
		if err := e.PutEntitlementRecord(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 2; i++ {
		r := v3.EntitlementRecord_builder{
			SyncId:     syncID,
			ExternalId: ksuid.New().String(),
			Resource: v3.ResourceRef_builder{
				ResourceTypeId: "group",
				ResourceId:     "devs",
			}.Build(),
		}.Build()
		if err := e.PutEntitlementRecord(ctx, r); err != nil {
			t.Fatal(err)
		}
	}

	var a, d int
	if err := e.IterateEntitlementsByResource(ctx, syncID, "group", "admins", func(*v3.EntitlementRecord) bool {
		a++
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if err := e.IterateEntitlementsByResource(ctx, syncID, "group", "devs", func(*v3.EntitlementRecord) bool {
		d++
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if a != 4 || d != 2 {
		t.Errorf("admins=%d (want 4), devs=%d (want 2)", a, d)
	}
}

func TestAssetRoundtrip(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	syncID := ksuid.New().String()
	if err := e.SetCurrentSync(syncID); err != nil {
		t.Fatal(err)
	}

	payload := []byte("PNGfakebytes\x00\x01\x02")
	r := v3.AssetRecord_builder{
		SyncId:      syncID,
		ExternalId:  "icon-1",
		ContentType: "image/png",
		Data:        payload,
	}.Build()
	if err := e.PutAssetRecord(ctx, r); err != nil {
		t.Fatal(err)
	}
	got, err := e.GetAssetRecord(ctx, syncID, "icon-1")
	if err != nil {
		t.Fatalf("GetAssetRecord: %v", err)
	}
	if got.GetContentType() != "image/png" {
		t.Errorf("content_type: got %q", got.GetContentType())
	}
	if string(got.GetData()) != string(payload) {
		t.Errorf("data roundtrip lost bytes")
	}
}

func TestSyncRunRecord(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)

	id1 := ksuid.New().String()
	id2 := ksuid.New().String()
	for _, sid := range []string{id1, id2} {
		r := v3.SyncRunRecord_builder{
			SyncId:    sid,
			Type:      v3.SyncType_SYNC_TYPE_FULL,
			StartedAt: timestamppb.Now(),
		}.Build()
		if err := e.PutSyncRunRecord(ctx, r); err != nil {
			t.Fatalf("PutSyncRunRecord: %v", err)
		}
	}
	got, err := e.GetSyncRunRecord(ctx, id1)
	if err != nil {
		t.Fatalf("GetSyncRunRecord: %v", err)
	}
	if got.GetSyncId() != id1 {
		t.Errorf("sync_id: got %q want %q", got.GetSyncId(), id1)
	}

	count := 0
	if err := e.IterateAllSyncRuns(ctx, func(*v3.SyncRunRecord) bool {
		count++
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Errorf("iterated %d sync_runs, want 2", count)
	}

	if err := e.DeleteSyncRunRecord(ctx, id1); err != nil {
		t.Fatal(err)
	}
	if _, err := e.GetSyncRunRecord(ctx, id1); !errors.Is(err, pebble.ErrNotFound) {
		t.Errorf("expected ErrNotFound after delete, got %v", err)
	}
}
