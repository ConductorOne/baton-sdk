package pebble

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/segmentio/ksuid"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

func newAdapter(t testing.TB) *Adapter {
	t.Helper()
	e, _ := newTestEngine(t)
	return NewAdapter(e)
}

func mkV2Grant(id, entID, principalRT, principalID string) *v2.Grant {
	return v2.Grant_builder{
		Id: id,
		Entitlement: v2.Entitlement_builder{
			Id: entID,
			Resource: v2.Resource_builder{
				Id: v2.ResourceId_builder{
					ResourceType: "app",
					Resource:     "github",
				}.Build(),
			}.Build(),
		}.Build(),
		Principal: v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: principalRT,
				Resource:     principalID,
			}.Build(),
		}.Build(),
	}.Build()
}

func TestAdapterStartSyncAndPutGrants(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)

	syncID, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	if err != nil {
		t.Fatalf("StartNewSync: %v", err)
	}
	if syncID == "" {
		t.Fatal("StartNewSync returned empty id")
	}

	// PutGrants
	grants := []*v2.Grant{
		mkV2Grant("g1", "ent-A", "user", "alice"),
		mkV2Grant("g2", "ent-A", "user", "bob"),
		mkV2Grant("g3", "ent-B", "user", "alice"),
	}
	if err := a.PutGrants(ctx, grants...); err != nil {
		t.Fatalf("PutGrants: %v", err)
	}

	// ListGrants (no filter) → 3 grants.
	resp, err := a.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
	if err != nil {
		t.Fatalf("ListGrants: %v", err)
	}
	if len(resp.GetList()) != 3 {
		t.Errorf("ListGrants count: got %d, want 3", len(resp.GetList()))
	}

	// ListGrants filtered by entitlement-side resource (app/github)
	// → all 3 grants live on this entitlement's resource. This is
	// the SQLite-compatible semantic; principal-side filtering is
	// what ListGrantsForPrincipal is for (see below).
	resp, err = a.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{
		Resource: v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: "app",
				Resource:     "github",
			}.Build(),
		}.Build(),
	}.Build())
	if err != nil {
		t.Fatalf("ListGrants by entitlement-resource: %v", err)
	}
	if len(resp.GetList()) != 3 {
		t.Errorf("ListGrants entitlement-resource count: got %d, want 3", len(resp.GetList()))
	}

	// ListGrantsForPrincipal filtered by principal alice → 2 grants.
	gforP, err := a.ListGrantsForPrincipal(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
		PrincipalId: v2.ResourceId_builder{
			ResourceType: "user",
			Resource:     "alice",
		}.Build(),
	}.Build())
	if err != nil {
		t.Fatalf("ListGrantsForPrincipal alice: %v", err)
	}
	if len(gforP.GetList()) != 2 {
		t.Errorf("ListGrantsForPrincipal alice count: got %d, want 2", len(gforP.GetList()))
	}

	// GetGrant single.
	g, err := a.GetGrant(ctx, reader_v2.GrantsReaderServiceGetGrantRequest_builder{
		GrantId: "g1",
	}.Build())
	if err != nil {
		t.Fatalf("GetGrant: %v", err)
	}
	if g.GetGrant().GetId() != "g1" {
		t.Errorf("GetGrant id: got %q", g.GetGrant().GetId())
	}

	// DeleteGrant
	if err := a.DeleteGrant(ctx, "g1"); err != nil {
		t.Fatalf("DeleteGrant: %v", err)
	}
	resp, err = a.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.GetList()) != 2 {
		t.Errorf("post-delete count: got %d, want 2", len(resp.GetList()))
	}

	// EndSync stamps ended_at.
	if err := a.EndSync(ctx); err != nil {
		t.Fatalf("EndSync: %v", err)
	}
}

func TestAdapterEndSyncClearsEngineCurrentSync(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)

	syncID, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	if err != nil {
		t.Fatalf("StartNewSync: %v", err)
	}
	if err := a.PutGrants(ctx, mkV2Grant("g1", "ent-A", "user", "alice")); err != nil {
		t.Fatalf("PutGrants: %v", err)
	}
	if err := a.EndSync(ctx); err != nil {
		t.Fatalf("EndSync: %v", err)
	}

	if err := a.engine.PutGrantRecord(ctx, makeGrant(syncID, "g2", "ent-B", "bob")); !errors.Is(err, ErrNoCurrentSync) {
		t.Fatalf("direct engine write after EndSync: got %v, want ErrNoCurrentSync", err)
	}
	if err := a.engine.IterateGrantsByEntitlement(ctx, "", "ent-A", func(*v3.GrantRecord) bool {
		t.Fatal("iterator should not resolve an ended sync via empty sync id")
		return false
	}); !errors.Is(err, ErrNoCurrentSync) {
		t.Fatalf("direct engine index read after EndSync: got %v, want ErrNoCurrentSync", err)
	}

	count := 0
	if err := a.engine.IterateGrantsByEntitlement(ctx, syncID, "ent-A", func(*v3.GrantRecord) bool {
		count++
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("ended sync grant index count = %d, want 1", count)
	}
}

func TestAdapterUnsafePutUniqueGrantsRequiresFreshSync(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)

	syncID, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	if err != nil {
		t.Fatalf("StartNewSync: %v", err)
	}

	// SetCurrentSync intentionally clears the engine's fresh-sync flag. The
	// fresh path skips read-before-write index cleanup, so using it after a
	// resume/rebind would risk stale secondary indexes.
	if err := a.SetCurrentSync(ctx, syncID); err != nil {
		t.Fatalf("SetCurrentSync: %v", err)
	}

	err = a.UnsafePutUniqueGrants(ctx, mkV2Grant("g1", "ent-A", "user", "alice"))
	if err == nil {
		t.Fatal("UnsafePutUniqueGrants unexpectedly succeeded on non-fresh sync")
	}
	if !strings.Contains(err.Error(), "sync is not fresh") {
		t.Fatalf("UnsafePutUniqueGrants error = %v, want non-fresh error", err)
	}
}

func TestAdapterResourcesAndEntitlements(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	if _, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, ""); err != nil {
		t.Fatal(err)
	}

	// 2 ResourceTypes.
	rts := []*v2.ResourceType{
		v2.ResourceType_builder{Id: "user", DisplayName: "User", Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER}}.Build(),
		v2.ResourceType_builder{Id: "group", DisplayName: "Group", Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_GROUP}}.Build(),
	}
	if err := a.PutResourceTypes(ctx, rts...); err != nil {
		t.Fatalf("PutResourceTypes: %v", err)
	}
	rtResp, err := a.ListResourceTypes(ctx, v2.ResourceTypesServiceListResourceTypesRequest_builder{}.Build())
	if err != nil {
		t.Fatal(err)
	}
	if len(rtResp.GetList()) != 2 {
		t.Errorf("ListResourceTypes: got %d, want 2", len(rtResp.GetList()))
	}

	// 3 Resources.
	resources := []*v2.Resource{
		v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: "user", Resource: "alice"}.Build(),
		}.Build(),
		v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: "user", Resource: "bob"}.Build(),
		}.Build(),
		v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: "group", Resource: "admins"}.Build(),
		}.Build(),
	}
	if err := a.PutResources(ctx, resources...); err != nil {
		t.Fatal(err)
	}
	all, err := a.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{}.Build())
	if err != nil {
		t.Fatal(err)
	}
	if len(all.GetList()) != 3 {
		t.Errorf("ListResources all: got %d, want 3", len(all.GetList()))
	}

	// Filter by resource type.
	users, err := a.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
		ResourceTypeId: "user",
	}.Build())
	if err != nil {
		t.Fatal(err)
	}
	if len(users.GetList()) != 2 {
		t.Errorf("ListResources users: got %d, want 2", len(users.GetList()))
	}

	// Entitlements.
	ents := []*v2.Entitlement{
		v2.Entitlement_builder{
			Id: "read", Resource: resources[0],
		}.Build(),
		v2.Entitlement_builder{
			Id: "admin", Resource: resources[2],
		}.Build(),
	}
	if err := a.PutEntitlements(ctx, ents...); err != nil {
		t.Fatal(err)
	}
	allEnt, err := a.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{}.Build())
	if err != nil {
		t.Fatal(err)
	}
	if len(allEnt.GetList()) != 2 {
		t.Errorf("ListEntitlements: got %d, want 2", len(allEnt.GetList()))
	}

	// Filter entitlements by resource.
	adminEnts, err := a.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{
		Resource: resources[2],
	}.Build())
	if err != nil {
		t.Fatal(err)
	}
	if len(adminEnts.GetList()) != 1 {
		t.Errorf("ListEntitlements by resource: got %d, want 1", len(adminEnts.GetList()))
	}
}

func TestAdapterAssetRoundtrip(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	if _, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, ""); err != nil {
		t.Fatal(err)
	}

	ref := v2.AssetRef_builder{Id: "icon-github"}.Build()
	if err := a.PutAsset(ctx, ref, "image/png", []byte("PNG\x00\x01")); err != nil {
		t.Fatalf("PutAsset: %v", err)
	}

	ct, r, err := a.GetAsset(ctx, v2.AssetServiceGetAssetRequest_builder{
		Asset: ref,
	}.Build())
	if err != nil {
		t.Fatalf("GetAsset: %v", err)
	}
	if ct != "image/png" {
		t.Errorf("content_type: got %q", ct)
	}
	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "PNG\x00\x01" {
		t.Errorf("data: got %q", data)
	}
}

func TestAdapterResumeSync(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	id1, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := a.PutGrants(ctx, mkV2Grant("g1", "ent", "user", "alice")); err != nil {
		t.Fatal(err)
	}
	if err := a.CheckpointSync(ctx, "step-7"); err != nil {
		t.Fatalf("CheckpointSync: %v", err)
	}
	// Simulate restart by zeroing in-memory state.
	a.current = syncRunState{}

	// Resume.
	id2, err := a.ResumeSync(ctx, connectorstore.SyncTypeFull, id1)
	if err != nil {
		t.Fatalf("ResumeSync: %v", err)
	}
	if id2 != id1 {
		t.Errorf("ResumeSync id mismatch: %q vs %q", id2, id1)
	}
	// Existing grants should be visible.
	resp, err := a.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.GetList()) != 1 {
		t.Errorf("post-resume grants: got %d, want 1", len(resp.GetList()))
	}
}

func TestAdapterLatestFinishedSyncID(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)

	got, err := a.LatestFinishedSyncID(ctx, connectorstore.SyncTypeAny)
	if err != nil {
		t.Fatal(err)
	}
	if got != "" {
		t.Fatalf("LatestFinishedSyncID with no syncs = %q, want empty", got)
	}

	openID, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	if err != nil {
		t.Fatal(err)
	}
	got, err = a.LatestFinishedSyncID(ctx, connectorstore.SyncTypeFull)
	if err != nil {
		t.Fatal(err)
	}
	if got != "" {
		t.Fatalf("LatestFinishedSyncID with only open sync %q = %q, want empty", openID, got)
	}

	// Ending it makes it the single finished sync, resolvable by its
	// type and by Any.
	if err := a.EndSync(ctx); err != nil {
		t.Fatal(err)
	}
	for _, st := range []connectorstore.SyncType{connectorstore.SyncTypeFull, connectorstore.SyncTypeAny} {
		got, err = a.LatestFinishedSyncID(ctx, st)
		if err != nil {
			t.Fatal(err)
		}
		if got != openID {
			t.Errorf("LatestFinishedSyncID(%s): got %q, want %q", st, got, openID)
		}
	}

	// A type filter that doesn't match the one sync resolves nothing.
	got, err = a.LatestFinishedSyncID(ctx, connectorstore.SyncTypePartial)
	if err != nil {
		t.Fatal(err)
	}
	if got != "" {
		t.Errorf("LatestFinishedSyncID(partial) with only a full sync = %q, want empty", got)
	}

	// Single-sync contract: StartNewSync REPLACES the prior sync. The
	// finished full sync is wiped and the replacement is in-progress, so
	// there is no finished sync of any type.
	replacementID, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	if err != nil {
		t.Fatal(err)
	}
	got, err = a.LatestFinishedSyncID(ctx, connectorstore.SyncTypeAny)
	if err != nil {
		t.Fatal(err)
	}
	if got != "" {
		t.Errorf("LatestFinishedSyncID after replacement (in-progress %q) = %q, want empty", replacementID, got)
	}
}

func TestAdapterCurrentDBSizeBytes(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	a := NewAdapter(e)

	initial, err := a.CurrentDBSizeBytes()
	if err != nil {
		t.Fatalf("CurrentDBSizeBytes initial: %v", err)
	}
	if initial <= 0 {
		t.Fatalf("CurrentDBSizeBytes initial = %d, want > 0", initial)
	}
	if want := regularFileSizeUnder(t, e.dbDir); initial != want {
		t.Fatalf("CurrentDBSizeBytes initial = %d, want regular file sum %d", initial, want)
	}

	if _, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, ""); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 25; i++ {
		if err := a.PutGrants(ctx, mkV2Grant(ksuid.New().String(), "ent", "user", ksuid.New().String())); err != nil {
			t.Fatalf("PutGrants: %v", err)
		}
	}

	after, err := a.CurrentDBSizeBytes()
	if err != nil {
		t.Fatalf("CurrentDBSizeBytes after writes: %v", err)
	}
	if want := regularFileSizeUnder(t, e.dbDir); after != want {
		t.Fatalf("CurrentDBSizeBytes after writes = %d, want regular file sum %d", after, want)
	}
	if after < initial {
		t.Fatalf("CurrentDBSizeBytes after writes = %d, want >= initial %d", after, initial)
	}
}

func TestAdapterNoCurrentSyncErrors(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)

	// Without StartNewSync, writes refuse.
	err := a.PutGrants(ctx, mkV2Grant("g1", "e", "user", "a"))
	if err == nil {
		t.Error("PutGrants without sync should error")
	}
}
