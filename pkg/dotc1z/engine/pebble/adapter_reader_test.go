package pebble

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"testing"

	"google.golang.org/protobuf/types/known/timestamppb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

func TestGetEntitlementResourceResourceType(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	if _, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, ""); err != nil {
		t.Fatal(err)
	}

	rt := v2.ResourceType_builder{Id: "user", DisplayName: "User",
		Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER}}.Build()
	if err := a.PutResourceTypes(ctx, rt); err != nil {
		t.Fatal(err)
	}
	r := v2.Resource_builder{
		Id:          v2.ResourceId_builder{ResourceType: "user", Resource: "alice"}.Build(),
		DisplayName: "Alice",
	}.Build()
	if err := a.PutResources(ctx, r); err != nil {
		t.Fatal(err)
	}
	ent := v2.Entitlement_builder{
		Id: "github-read",
		Resource: v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: "app", Resource: "github"}.Build(),
		}.Build(),
		DisplayName: "Read",
		Purpose:     v2.Entitlement_PURPOSE_VALUE_PERMISSION,
	}.Build()
	if err := a.PutEntitlements(ctx, ent); err != nil {
		t.Fatal(err)
	}

	// GetResourceType
	rtResp, err := a.GetResourceType(ctx, reader_v2.ResourceTypesReaderServiceGetResourceTypeRequest_builder{
		ResourceTypeId: "user",
	}.Build())
	if err != nil {
		t.Fatalf("GetResourceType: %v", err)
	}
	if got := rtResp.GetResourceType().GetDisplayName(); got != "User" {
		t.Errorf("GetResourceType display: got %q want User", got)
	}
	if len(rtResp.GetResourceType().GetTraits()) != 1 {
		t.Errorf("GetResourceType traits: got %d want 1", len(rtResp.GetResourceType().GetTraits()))
	}

	// GetResource
	rResp, err := a.GetResource(ctx, reader_v2.ResourcesReaderServiceGetResourceRequest_builder{
		ResourceId: v2.ResourceId_builder{ResourceType: "user", Resource: "alice"}.Build(),
	}.Build())
	if err != nil {
		t.Fatalf("GetResource: %v", err)
	}
	if got := rResp.GetResource().GetDisplayName(); got != "Alice" {
		t.Errorf("GetResource display: got %q want Alice", got)
	}

	// GetEntitlement
	eResp, err := a.GetEntitlement(ctx, reader_v2.EntitlementsReaderServiceGetEntitlementRequest_builder{
		EntitlementId: "github-read",
	}.Build())
	if err != nil {
		t.Fatalf("GetEntitlement: %v", err)
	}
	if got := eResp.GetEntitlement().GetId(); got != "github-read" {
		t.Errorf("GetEntitlement id: got %q", got)
	}
	if got := eResp.GetEntitlement().GetPurpose(); got != v2.Entitlement_PURPOSE_VALUE_PERMISSION {
		t.Errorf("GetEntitlement purpose: got %v want PERMISSION", got)
	}

	// Missing entity returns sql.ErrNoRows — the Adapter normalizes
	// pebble.ErrNotFound at the boundary so engine-agnostic
	// consumers (pkg/sync, pkg/sync/expand) can use one sentinel
	// across both engines. See adapter_errors.go.
	if _, err := a.GetEntitlement(ctx, reader_v2.EntitlementsReaderServiceGetEntitlementRequest_builder{
		EntitlementId: "does-not-exist",
	}.Build()); !errors.Is(err, sql.ErrNoRows) {
		t.Errorf("missing entitlement: got %v, want sql.ErrNoRows", err)
	}
}

func TestListGrantsForEntitlementAndResourceType(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	if _, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, ""); err != nil {
		t.Fatal(err)
	}

	// 5 grants on ent-A with user principals, 3 on ent-A with group
	// principals, 2 on ent-B with user principals.
	grants := []*v2.Grant{}
	for i := 0; i < 5; i++ {
		grants = append(grants, mkV2Grant("u-a-"+strconv.Itoa(i), "ent-A", "user", "u"+strconv.Itoa(i)))
	}
	for i := 0; i < 3; i++ {
		grants = append(grants, mkV2Grant("g-a-"+strconv.Itoa(i), "ent-A", "group", "g"+strconv.Itoa(i)))
	}
	for i := 0; i < 2; i++ {
		grants = append(grants, mkV2Grant("u-b-"+strconv.Itoa(i), "ent-B", "user", "u"+strconv.Itoa(i)))
	}
	if err := a.PutGrants(ctx, grants...); err != nil {
		t.Fatal(err)
	}

	// ListGrantsForEntitlement(ent-A) → 8 grants total
	resp, err := a.ListGrantsForEntitlement(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
		Entitlement: v2.Entitlement_builder{Id: "ent-A"}.Build(),
		PageSize:    100,
	}.Build())
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.GetList()) != 8 {
		t.Errorf("ListGrantsForEntitlement(ent-A): got %d, want 8", len(resp.GetList()))
	}

	// ListGrantsForEntitlement(ent-A) filtered by principal RT=user → 5
	respFiltered, err := a.ListGrantsForEntitlement(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
		Entitlement:              v2.Entitlement_builder{Id: "ent-A"}.Build(),
		PrincipalResourceTypeIds: []string{"user"},
		PageSize:                 100,
	}.Build())
	if err != nil {
		t.Fatal(err)
	}
	if len(respFiltered.GetList()) != 5 {
		t.Errorf("ListGrantsForEntitlement(ent-A, user): got %d, want 5", len(respFiltered.GetList()))
	}

	// ListGrantsForResourceType(user) → 7 grants (5 ent-A + 2 ent-B)
	rtResp, err := a.ListGrantsForResourceType(ctx, reader_v2.GrantsReaderServiceListGrantsForResourceTypeRequest_builder{
		ResourceTypeId: "user",
		PageSize:       100,
	}.Build())
	if err != nil {
		t.Fatal(err)
	}
	if len(rtResp.GetList()) != 7 {
		t.Errorf("ListGrantsForResourceType(user): got %d, want 7", len(rtResp.GetList()))
	}
}

// TestListGrantsForResourceTypePagination drives the new
// idxGrantByPrincipalResourceType index across a page boundary
// and verifies that updates flip-flop the index correctly.
func TestListGrantsForResourceTypePagination(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	if _, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, ""); err != nil {
		t.Fatal(err)
	}

	const userCount = 50
	const groupCount = 20
	grants := make([]*v2.Grant, 0, userCount+groupCount)
	for i := 0; i < userCount; i++ {
		grants = append(grants, mkV2Grant("u-"+strconv.Itoa(i), "ent-A", "user", "u"+strconv.Itoa(i)))
	}
	for i := 0; i < groupCount; i++ {
		grants = append(grants, mkV2Grant("g-"+strconv.Itoa(i), "ent-A", "group", "g"+strconv.Itoa(i)))
	}
	if err := a.PutGrants(ctx, grants...); err != nil {
		t.Fatal(err)
	}

	// Walk all user grants across multiple pages.
	collected := map[string]bool{}
	cursor := ""
	for i := 0; i < 20; i++ {
		resp, err := a.ListGrantsForResourceType(ctx, reader_v2.GrantsReaderServiceListGrantsForResourceTypeRequest_builder{
			ResourceTypeId: "user",
			PageSize:       7,
			PageToken:      cursor,
		}.Build())
		if err != nil {
			t.Fatalf("page %d: %v", i, err)
		}
		for _, g := range resp.GetList() {
			if g.GetPrincipal().GetId().GetResourceType() != "user" {
				t.Errorf("non-user grant returned: %v", g)
			}
			if collected[g.GetId()] {
				t.Errorf("duplicate grant id %q", g.GetId())
			}
			collected[g.GetId()] = true
		}
		cursor = resp.GetNextPageToken()
		if cursor == "" {
			break
		}
	}
	if len(collected) != userCount {
		t.Errorf("paginated user grants: got %d, want %d", len(collected), userCount)
	}

	// Re-put a subset with a different principal RT to verify index
	// flip on overwrite. The first 10 "user" grants become "group"
	// principals; ListGrantsForResourceType(user) should drop to 40
	// and ListGrantsForResourceType(group) should rise to 30.
	updated := make([]*v2.Grant, 0, 10)
	for i := 0; i < 10; i++ {
		updated = append(updated, mkV2Grant("u-"+strconv.Itoa(i), "ent-A", "group", "moved-"+strconv.Itoa(i)))
	}
	if err := a.PutGrants(ctx, updated...); err != nil {
		t.Fatal(err)
	}

	respUser, err := a.ListGrantsForResourceType(ctx, reader_v2.GrantsReaderServiceListGrantsForResourceTypeRequest_builder{
		ResourceTypeId: "user", PageSize: 1000,
	}.Build())
	if err != nil {
		t.Fatal(err)
	}
	if got := len(respUser.GetList()); got != userCount-10 {
		t.Errorf("after move, user grants = %d, want %d", got, userCount-10)
	}
	respGroup, err := a.ListGrantsForResourceType(ctx, reader_v2.GrantsReaderServiceListGrantsForResourceTypeRequest_builder{
		ResourceTypeId: "group", PageSize: 1000,
	}.Build())
	if err != nil {
		t.Fatal(err)
	}
	if got := len(respGroup.GetList()); got != groupCount+10 {
		t.Errorf("after move, group grants = %d, want %d", got, groupCount+10)
	}
}

// TestBulkByIdsRoundtripPebble exercises the Adapter's
// ListResourcesByIds / ListEntitlementsByIds / GetResourceTypes
// (RFC §A3 — collapse N point Gets into one round-trip).
func TestBulkByIdsRoundtripPebble(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	if _, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, ""); err != nil {
		t.Fatal(err)
	}

	rtUser := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()
	rtGroup := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	rtApp := v2.ResourceType_builder{Id: "app", DisplayName: "App"}.Build()
	if err := a.PutResourceTypes(ctx, rtUser, rtGroup, rtApp); err != nil {
		t.Fatal(err)
	}
	mkRes := func(rt, id string) *v2.Resource {
		return v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: rt, Resource: id}.Build(),
		}.Build()
	}
	if err := a.PutResources(ctx,
		mkRes("user", "u1"), mkRes("user", "u2"),
		mkRes("group", "g1"),
	); err != nil {
		t.Fatal(err)
	}
	if err := a.PutEntitlements(ctx,
		v2.Entitlement_builder{Id: "ent-A", Resource: mkRes("app", "github")}.Build(),
		v2.Entitlement_builder{Id: "ent-B", Resource: mkRes("app", "github")}.Build(),
	); err != nil {
		t.Fatal(err)
	}

	t.Run("GetResourceTypes", func(t *testing.T) {
		resp, err := a.GetResourceTypes(ctx, reader_v2.ResourceTypesReaderServiceGetResourceTypesRequest_builder{
			ResourceTypeIds: []string{"user", "missing", "group"},
		}.Build())
		if err != nil {
			t.Fatal(err)
		}
		if len(resp.GetList()) != 2 {
			t.Errorf("len=%d, want 2", len(resp.GetList()))
		}
	})
	t.Run("ListEntitlementsByIds", func(t *testing.T) {
		resp, err := a.ListEntitlementsByIds(ctx, reader_v2.EntitlementsReaderServiceListEntitlementsByIdsRequest_builder{
			EntitlementIds: []string{"ent-A", "ent-zzz", "ent-B"},
		}.Build())
		if err != nil {
			t.Fatal(err)
		}
		if len(resp.GetList()) != 2 {
			t.Errorf("len=%d, want 2", len(resp.GetList()))
		}
	})
	t.Run("ListResourcesByIds", func(t *testing.T) {
		ids := []*v2.ResourceId{
			v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(),
			v2.ResourceId_builder{ResourceType: "user", Resource: "u-missing"}.Build(),
			v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(),
		}
		resp, err := a.ListResourcesByIds(ctx, reader_v2.ResourcesReaderServiceListResourcesByIdsRequest_builder{
			ResourceIds: ids,
		}.Build())
		if err != nil {
			t.Fatal(err)
		}
		if len(resp.GetList()) != 2 {
			t.Errorf("len=%d, want 2", len(resp.GetList()))
		}
	})
}

// TestListGrantsForEntitlementsPebble exercises the new batched
// reader against the Pebble adapter (RFC §A4).
func TestListGrantsForEntitlementsPebble(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	if _, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, ""); err != nil {
		t.Fatal(err)
	}
	mkRes := func(rt, id string) *v2.Resource {
		return v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: rt, Resource: id}.Build(),
		}.Build()
	}
	appRes := mkRes("app", "gh")
	if err := a.PutResources(ctx, appRes); err != nil {
		t.Fatal(err)
	}
	entA := v2.Entitlement_builder{Id: "ent-A", Resource: appRes, Purpose: v2.Entitlement_PURPOSE_VALUE_PERMISSION, Slug: "A"}.Build()
	entB := v2.Entitlement_builder{Id: "ent-B", Resource: appRes, Purpose: v2.Entitlement_PURPOSE_VALUE_PERMISSION, Slug: "B"}.Build()
	entC := v2.Entitlement_builder{Id: "ent-C", Resource: appRes, Purpose: v2.Entitlement_PURPOSE_VALUE_PERMISSION, Slug: "C"}.Build()
	if err := a.PutEntitlements(ctx, entA, entB, entC); err != nil {
		t.Fatal(err)
	}
	mkGrant := func(id, entID, princRT, princID string) *v2.Grant {
		return v2.Grant_builder{
			Id:          id,
			Entitlement: v2.Entitlement_builder{Id: entID, Resource: appRes}.Build(),
			Principal:   mkRes(princRT, princID),
		}.Build()
	}
	grants := []*v2.Grant{}
	for i := 0; i < 5; i++ {
		grants = append(grants, mkGrant("a-u-"+strconv.Itoa(i), "ent-A", "user", "u"+strconv.Itoa(i)))
	}
	for i := 0; i < 3; i++ {
		grants = append(grants, mkGrant("b-g-"+strconv.Itoa(i), "ent-B", "group", "g"+strconv.Itoa(i)))
	}
	for i := 0; i < 7; i++ {
		grants = append(grants, mkGrant("c-u-"+strconv.Itoa(i), "ent-C", "user", "uc"+strconv.Itoa(i)))
	}
	if err := a.PutGrants(ctx, grants...); err != nil {
		t.Fatal(err)
	}

	t.Run("aggregate all", func(t *testing.T) {
		resp, err := a.ListGrantsForEntitlements(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementsRequest_builder{
			Entitlements: []*v2.Entitlement{entA, entB, entC},
			PageSize:     100,
		}.Build())
		if err != nil {
			t.Fatal(err)
		}
		if len(resp.GetList()) != 15 {
			t.Errorf("len=%d, want 15", len(resp.GetList()))
		}
	})
	t.Run("pagination crosses entitlement boundary", func(t *testing.T) {
		seen := map[string]bool{}
		token := ""
		for i := 0; i < 10; i++ {
			resp, err := a.ListGrantsForEntitlements(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementsRequest_builder{
				Entitlements: []*v2.Entitlement{entA, entB, entC},
				PageSize:     4,
				PageToken:    token,
			}.Build())
			if err != nil {
				t.Fatal(err)
			}
			for _, g := range resp.GetList() {
				if seen[g.GetId()] {
					t.Errorf("dup grant %s", g.GetId())
				}
				seen[g.GetId()] = true
			}
			token = resp.GetNextPageToken()
			if token == "" {
				break
			}
		}
		if len(seen) != 15 {
			t.Errorf("seen=%d, want 15", len(seen))
		}
	})
	t.Run("checksum mismatch resets", func(t *testing.T) {
		resp1, err := a.ListGrantsForEntitlements(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementsRequest_builder{
			Entitlements: []*v2.Entitlement{entA, entB, entC},
			PageSize:     2,
		}.Build())
		if err != nil {
			t.Fatal(err)
		}
		if resp1.GetNextPageToken() == "" {
			t.Fatal("first page should overflow")
		}
		resp2, err := a.ListGrantsForEntitlements(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementsRequest_builder{
			Entitlements: []*v2.Entitlement{entA, entB},
			PageSize:     2,
			PageToken:    resp1.GetNextPageToken(),
		}.Build())
		if err != nil {
			t.Fatal(err)
		}
		if len(resp2.GetList()) != 2 {
			t.Errorf("after checksum mismatch len=%d, want 2", len(resp2.GetList()))
		}
		if resp2.GetList()[0].GetEntitlement().GetId() != "ent-A" {
			t.Errorf("after reset first ent=%q, want ent-A", resp2.GetList()[0].GetEntitlement().GetId())
		}
	})
}

func TestSyncsReaderMethods(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)

	// Create 3 syncs; finish 2.
	id1, _ := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	if err := a.EndSync(ctx); err != nil {
		t.Fatal(err)
	}
	id2, _ := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	if err := a.EndSync(ctx); err != nil {
		t.Fatal(err)
	}
	id3, _ := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	// Don't end id3.

	// GetSync(id2) → has ended_at
	resp, err := a.GetSync(ctx, reader_v2.SyncsReaderServiceGetSyncRequest_builder{
		SyncId: id2,
	}.Build())
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetSync().GetId() != id2 {
		t.Errorf("GetSync id: got %q want %q", resp.GetSync().GetId(), id2)
	}
	if resp.GetSync().GetEndedAt() == nil {
		t.Errorf("GetSync ended_at: want non-nil")
	}

	// GetSync(id3) → no ended_at
	resp3, err := a.GetSync(ctx, reader_v2.SyncsReaderServiceGetSyncRequest_builder{
		SyncId: id3,
	}.Build())
	if err != nil {
		t.Fatal(err)
	}
	if resp3.GetSync().GetEndedAt() != nil {
		t.Errorf("GetSync(id3) ended_at: want nil, got %v", resp3.GetSync().GetEndedAt().AsTime())
	}

	// ListSyncs → all 3
	listResp, err := a.ListSyncs(ctx, reader_v2.SyncsReaderServiceListSyncsRequest_builder{
		PageSize: 100,
	}.Build())
	if err != nil {
		t.Fatal(err)
	}
	if len(listResp.GetSyncs()) != 3 {
		t.Errorf("ListSyncs: got %d, want 3", len(listResp.GetSyncs()))
	}

	// GetLatestFinishedSync → id2 (most recent ended)
	latest, err := a.GetLatestFinishedSync(ctx, reader_v2.SyncsReaderServiceGetLatestFinishedSyncRequest_builder{}.Build())
	if err != nil {
		t.Fatal(err)
	}
	if latest.GetSync().GetId() != id2 {
		t.Errorf("GetLatestFinishedSync: got %q, want %q (id1=%q)", latest.GetSync().GetId(), id2, id1)
	}

	// GetLatestFinishedSync filtered by sync_type=full → id2
	latestFull, err := a.GetLatestFinishedSync(ctx, reader_v2.SyncsReaderServiceGetLatestFinishedSyncRequest_builder{
		SyncType: string(connectorstore.SyncTypeFull),
	}.Build())
	if err != nil {
		t.Fatal(err)
	}
	if latestFull.GetSync().GetId() != id2 {
		t.Errorf("GetLatestFinishedSync(full): got %q want %q", latestFull.GetSync().GetId(), id2)
	}

	// GetLatestFinishedSync filtered by a sync_type that doesn't exist
	latestNone, err := a.GetLatestFinishedSync(ctx, reader_v2.SyncsReaderServiceGetLatestFinishedSyncRequest_builder{
		SyncType: string(connectorstore.SyncTypePartial),
	}.Build())
	if err != nil {
		t.Fatal(err)
	}
	if latestNone.GetSync() != nil {
		t.Errorf("GetLatestFinishedSync(partial): expected nil sync, got %v", latestNone.GetSync())
	}
}

func TestStatsAndGrantStats(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	syncID, _ := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")

	if err := a.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "user"}.Build(),
		v2.ResourceType_builder{Id: "group"}.Build()); err != nil {
		t.Fatal(err)
	}
	if err := a.PutResources(ctx,
		v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "a"}.Build()}.Build(),
		v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "b"}.Build()}.Build(),
		v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "c"}.Build()}.Build()); err != nil {
		t.Fatal(err)
	}
	if err := a.PutGrants(ctx,
		mkV2Grant("g1", "e1", "user", "a"),
		mkV2Grant("g2", "e1", "user", "b"),
		mkV2Grant("g3", "e2", "group", "c")); err != nil {
		t.Fatal(err)
	}

	stats, err := a.Stats(ctx, connectorstore.SyncTypeAny, syncID)
	if err != nil {
		t.Fatal(err)
	}
	if stats["resource_types"] != 2 {
		t.Errorf("stats[resource_types] = %d, want 2", stats["resource_types"])
	}
	if stats["resources"] != 3 {
		t.Errorf("stats[resources] = %d, want 3", stats["resources"])
	}
	if stats["grants"] != 3 {
		t.Errorf("stats[grants] = %d, want 3", stats["grants"])
	}

	// GrantStats partitions by entitlement resource type. Our
	// mkV2Grant always sets the entitlement's resource to (app, github),
	// so all 3 grants count under "app".
	gs, err := a.GrantStats(ctx, connectorstore.SyncTypeAny, syncID)
	if err != nil {
		t.Fatal(err)
	}
	if gs["app"] != 3 {
		t.Errorf("GrantStats[app] = %d, want 3", gs["app"])
	}
}

// TestIfNewerSkipsStaleGrants exercises the engine-level
// PutGrantRecordsIfNewer directly so the freshSync flag doesn't
// short-circuit the read-before-write. Verifies the SQLite semantics:
// older discovered_at is silently dropped; newer discovered_at wins.
func TestIfNewerSkipsStaleGrants(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	syncID := "2BnzhUf3aJZ9Y6cQ7vWmRk0hZJa"
	if err := e.SetCurrentSync(syncID); err != nil {
		t.Fatal(err)
	}
	// Build NEW record with timestamp T+1h.
	now := timestamppb.Now()
	older := timestamppb.New(now.AsTime().Add(-1 * 3600 * 1e9))
	newer := timestamppb.New(now.AsTime().Add(1 * 3600 * 1e9))

	mid := makeGrant(syncID, "g1", "ent-mid", "alice")
	mid.SetDiscoveredAt(now)
	if err := e.PutGrantRecord(ctx, mid); err != nil {
		t.Fatal(err)
	}

	// IfNewer with an OLDER timestamp must be a no-op.
	stale := makeGrant(syncID, "g1", "ent-stale", "alice")
	stale.SetDiscoveredAt(older)
	if err := e.PutGrantRecordsIfNewer(ctx, stale); err != nil {
		t.Fatal(err)
	}
	got, err := e.GetGrantRecord(ctx, syncID, "g1")
	if err != nil {
		t.Fatal(err)
	}
	if got.GetEntitlement().GetEntitlementId() != "ent-mid" {
		t.Errorf("after stale IfNewer: got entitlement %q want ent-mid", got.GetEntitlement().GetEntitlementId())
	}

	// IfNewer with a STRICTLY NEWER timestamp must overwrite.
	fresh := makeGrant(syncID, "g1", "ent-fresh", "alice")
	fresh.SetDiscoveredAt(newer)
	if err := e.PutGrantRecordsIfNewer(ctx, fresh); err != nil {
		t.Fatal(err)
	}
	got, err = e.GetGrantRecord(ctx, syncID, "g1")
	if err != nil {
		t.Fatal(err)
	}
	if got.GetEntitlement().GetEntitlementId() != "ent-fresh" {
		t.Errorf("after newer IfNewer: got entitlement %q want ent-fresh", got.GetEntitlement().GetEntitlementId())
	}

	// IfNewer with the SAME timestamp must NOT overwrite (strictly
	// newer required, matches SQLite EXCLUDED.discovered_at > X).
	same := makeGrant(syncID, "g1", "ent-same", "alice")
	same.SetDiscoveredAt(newer)
	if err := e.PutGrantRecordsIfNewer(ctx, same); err != nil {
		t.Fatal(err)
	}
	got, err = e.GetGrantRecord(ctx, syncID, "g1")
	if err != nil {
		t.Fatal(err)
	}
	if got.GetEntitlement().GetEntitlementId() != "ent-fresh" {
		t.Errorf("equal-timestamp IfNewer should be no-op: got %q want ent-fresh", got.GetEntitlement().GetEntitlementId())
	}
}
