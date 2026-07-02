package pebble

import (
	"context"
	"io"
	"testing"

	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/require"

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
	canonicalEntID := canonicalTestEntID(entID)
	return v2.Grant_builder{
		Id: canonicalTestGrantID(entID, principalRT, principalID),
		Entitlement: v2.Entitlement_builder{
			Id: canonicalEntID,
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

func canonicalTestEntID(entID string) string {
	return "app:github:" + entID
}

func canonicalTestGrantID(entID, principalRT, principalID string) string {
	return canonicalTestEntID(entID) + ":" + principalRT + ":" + principalID
}

func TestAdapterStartSyncAndPutGrants(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)

	syncID, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err, "StartNewSync")
	require.NotEmpty(t, syncID, "StartNewSync returned empty id")

	// PutGrants
	grants := []*v2.Grant{
		mkV2Grant("g1", "ent-A", "user", "alice"),
		mkV2Grant("g2", "ent-A", "user", "bob"),
		mkV2Grant("g3", "ent-B", "user", "alice"),
	}
	require.NoError(t, a.PutGrants(ctx, grants...), "PutGrants")

	// ListGrants (no filter) → 3 grants.
	resp, err := a.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
	require.NoError(t, err, "ListGrants")
	require.Equal(t, 3, len(resp.GetList()), "ListGrants count")

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
	require.NoError(t, err, "ListGrants by entitlement-resource")
	require.Equal(t, 3, len(resp.GetList()), "ListGrants entitlement-resource count")

	// ListGrantsForPrincipal filtered by principal alice → 2 grants.
	gforP, err := a.ListGrantsForPrincipal(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
		PrincipalId: v2.ResourceId_builder{
			ResourceType: "user",
			Resource:     "alice",
		}.Build(),
	}.Build())
	require.NoError(t, err, "ListGrantsForPrincipal alice")
	require.Equal(t, 2, len(gforP.GetList()), "ListGrantsForPrincipal alice count")

	// GetGrant single.
	g1ID := canonicalTestGrantID("ent-A", "user", "alice")
	g, err := a.GetGrant(ctx, reader_v2.GrantsReaderServiceGetGrantRequest_builder{
		GrantId: g1ID,
	}.Build())
	require.NoError(t, err, "GetGrant")
	require.Equal(t, g1ID, g.GetGrant().GetId(), "GetGrant id")

	// DeleteGrant
	require.NoError(t, a.DeleteGrant(ctx, g1ID), "DeleteGrant")
	resp, err = a.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
	require.NoError(t, err)
	require.Equal(t, 2, len(resp.GetList()), "post-delete count")

	// EndSync stamps ended_at.
	require.NoError(t, a.EndSync(ctx), "EndSync")
}

func TestAdapterEndSyncClearsEngineCurrentSync(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)

	syncID, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err, "StartNewSync")
	require.NoError(t, a.PutGrants(ctx, mkV2Grant("g1", "ent-A", "user", "alice")), "PutGrants")
	require.NoError(t, a.EndSync(ctx), "EndSync")

	// EndSync cleared the engine's bound sync: a direct record write
	// must now fail rather than land an orphan record with no sync run.
	err = a.engine.PutGrantRecord(ctx, makeGrant(syncID, "g2", "ent-B", "bob"))
	require.ErrorIs(t, err, ErrNoCurrentSync, "direct engine write after EndSync: got %v, want ErrNoCurrentSync", err)

	// Reads do NOT gate on the bound sync: the finished sync's data
	// persists and stays readable through the engine after EndSync.
	count := 0
	err = a.engine.IterateGrantsByEntitlement(ctx, canonicalTestEntID("ent-A"), func(*v3.GrantRecord) bool {
		count++
		return true
	})
	require.NoError(t, err)
	require.Equal(t, 1, count, "ended sync grant index count")
}

func TestAdapterUnsafePutUniqueGrantsRequiresFreshSync(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)

	syncID, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err, "StartNewSync")

	// SetCurrentSync intentionally clears the engine's fresh-sync flag. The
	// fresh path skips read-before-write index cleanup, so using it after a
	// resume/rebind would risk stale secondary indexes.
	require.NoError(t, a.SetCurrentSync(ctx, syncID), "SetCurrentSync")

	err = a.UnsafePutUniqueGrants(ctx, mkV2Grant("g1", "ent-A", "user", "alice"))
	require.Error(t, err, "UnsafePutUniqueGrants unexpectedly succeeded on non-fresh sync")
	require.ErrorContains(t, err, "sync is not fresh", "UnsafePutUniqueGrants error = %v, want non-fresh error", err)
}

func TestAdapterResourcesAndEntitlements(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	// 2 ResourceTypes.
	rts := []*v2.ResourceType{
		v2.ResourceType_builder{Id: "user", DisplayName: "User", Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER}}.Build(),
		v2.ResourceType_builder{Id: "group", DisplayName: "Group", Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_GROUP}}.Build(),
	}
	require.NoError(t, a.PutResourceTypes(ctx, rts...), "PutResourceTypes")
	rtResp, err := a.ListResourceTypes(ctx, v2.ResourceTypesServiceListResourceTypesRequest_builder{}.Build())
	require.NoError(t, err)
	require.Equal(t, 2, len(rtResp.GetList()), "ListResourceTypes")

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
	require.NoError(t, a.PutResources(ctx, resources...))
	all, err := a.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{}.Build())
	require.NoError(t, err)
	require.Equal(t, 3, len(all.GetList()), "ListResources all")

	// Filter by resource type.
	users, err := a.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
		ResourceTypeId: "user",
	}.Build())
	require.NoError(t, err)
	require.Equal(t, 2, len(users.GetList()), "ListResources users")

	// Entitlements.
	ents := []*v2.Entitlement{
		v2.Entitlement_builder{
			Id: "read", Resource: resources[0],
		}.Build(),
		v2.Entitlement_builder{
			Id: "admin", Resource: resources[2],
		}.Build(),
	}
	require.NoError(t, a.PutEntitlements(ctx, ents...))
	allEnt, err := a.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{}.Build())
	require.NoError(t, err)
	require.Equal(t, 2, len(allEnt.GetList()), "ListEntitlements")

	// Filter entitlements by resource.
	adminEnts, err := a.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{
		Resource: resources[2],
	}.Build())
	require.NoError(t, err)
	require.Equal(t, 1, len(adminEnts.GetList()), "ListEntitlements by resource")
}

func TestAdapterAssetRoundtrip(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	ref := v2.AssetRef_builder{Id: "icon-github"}.Build()
	require.NoError(t, a.PutAsset(ctx, ref, "image/png", []byte("PNG\x00\x01")), "PutAsset")

	ct, r, err := a.GetAsset(ctx, v2.AssetServiceGetAssetRequest_builder{
		Asset: ref,
	}.Build())
	require.NoError(t, err, "GetAsset")
	require.Equal(t, "image/png", ct, "content_type")
	data, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, "PNG\x00\x01", string(data), "data")
}

func TestAdapterResumeSync(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	id1, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, a.PutGrants(ctx, mkV2Grant("g1", "ent", "user", "alice")))
	require.NoError(t, a.CheckpointSync(ctx, "step-7"), "CheckpointSync")
	// Simulate restart by zeroing in-memory state.
	a.current = syncRunState{}

	// Resume.
	id2, err := a.ResumeSync(ctx, connectorstore.SyncTypeFull, id1)
	require.NoError(t, err, "ResumeSync")
	require.Equal(t, id1, id2, "ResumeSync id mismatch")
	// Existing grants should be visible.
	resp, err := a.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
	require.NoError(t, err)
	require.Equal(t, 1, len(resp.GetList()), "post-resume grants")
}

func TestAdapterLatestFinishedSyncID(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)

	got, err := a.LatestFinishedSyncID(ctx, connectorstore.SyncTypeAny)
	require.NoError(t, err)
	require.Empty(t, got, "LatestFinishedSyncID with no syncs")

	openID, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	got, err = a.LatestFinishedSyncID(ctx, connectorstore.SyncTypeFull)
	require.NoError(t, err)
	require.Empty(t, got, "LatestFinishedSyncID with only open sync %q", openID)

	// Ending it makes it the single finished sync, resolvable by its
	// type and by Any.
	require.NoError(t, a.EndSync(ctx))
	for _, st := range []connectorstore.SyncType{connectorstore.SyncTypeFull, connectorstore.SyncTypeAny} {
		got, err = a.LatestFinishedSyncID(ctx, st)
		require.NoError(t, err)
		require.Equal(t, openID, got, "LatestFinishedSyncID(%s)", st)
	}

	// A type filter that doesn't match the one sync resolves nothing.
	got, err = a.LatestFinishedSyncID(ctx, connectorstore.SyncTypePartial)
	require.NoError(t, err)
	require.Empty(t, got, "LatestFinishedSyncID(partial) with only a full sync")

	// Single-sync contract: StartNewSync REPLACES the prior sync. The
	// finished full sync is wiped and the replacement is in-progress, so
	// there is no finished sync of any type.
	replacementID, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	got, err = a.LatestFinishedSyncID(ctx, connectorstore.SyncTypeAny)
	require.NoError(t, err)
	require.Empty(t, got, "LatestFinishedSyncID after replacement (in-progress %q)", replacementID)
}

func TestAdapterCurrentDBSizeBytes(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	a := NewAdapter(e)

	initial, err := a.CurrentDBSizeBytes()
	require.NoError(t, err, "CurrentDBSizeBytes initial")
	require.Greater(t, initial, int64(0), "CurrentDBSizeBytes initial")
	want := regularFileSizeUnder(t, e.dbDir)
	require.Equal(t, want, initial, "CurrentDBSizeBytes initial")

	_, err = a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	for i := 0; i < 25; i++ {
		require.NoError(t, a.PutGrants(ctx, mkV2Grant(ksuid.New().String(), "ent", "user", ksuid.New().String())), "PutGrants")
	}

	after, err := a.CurrentDBSizeBytes()
	require.NoError(t, err, "CurrentDBSizeBytes after writes")
	want = regularFileSizeUnder(t, e.dbDir)
	require.Equal(t, want, after, "CurrentDBSizeBytes after writes")
	require.GreaterOrEqual(t, after, initial, "CurrentDBSizeBytes after writes")
}

func TestAdapterNoCurrentSyncErrors(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)

	// Without StartNewSync, writes refuse.
	err := a.PutGrants(ctx, mkV2Grant("g1", "e", "user", "a"))
	require.Error(t, err, "PutGrants without sync should error")
}
