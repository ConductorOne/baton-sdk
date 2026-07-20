package pebble

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// TestEntitlementResourceAnnotationsHydrated locks in that every reader path
// returns an entitlement whose embedded Resource carries the full resource's
// annotations (here a connector RawId), matching the SQLite engine.
//
// The Pebble engine stores an entitlement's resource as an id-only ref, so
// without read-side hydration the resource annotations are dropped. That
// silently broke uplift's raw_baton_id derivation and match_baton_id
// placeholder merging, which both read the connector RawId off
// entitlement.Resource.GetAnnotations().
func TestEntitlementResourceAnnotationsHydrated(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	require.NoError(t, a.PutResourceTypes(ctx, v2.ResourceType_builder{
		Id:          "group",
		DisplayName: "Group",
		Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_GROUP},
	}.Build()))

	resAnnos := annotations.Annotations{}
	resAnnos.Update(&v2.RawId{Id: "00gRAWID"})
	res := v2.Resource_builder{
		Id:          v2.ResourceId_builder{ResourceType: "group", Resource: "00gRAWID"}.Build(),
		DisplayName: "Engineering",
		Annotations: resAnnos,
	}.Build()
	require.NoError(t, a.PutResources(ctx, res))

	// Connectors emit the entitlement carrying its full resource; the engine
	// normalizes that to an id-only ref on write.
	const entID = "group:00gRAWID:member"
	require.NoError(t, a.PutEntitlements(ctx, v2.Entitlement_builder{
		Id:          entID,
		Slug:        "member",
		DisplayName: "Group Member",
		Purpose:     v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT,
		Resource:    res,
	}.Build()))

	requireRawID := func(t *testing.T, ent *v2.Entitlement) {
		t.Helper()
		require.NotNil(t, ent)
		require.NotNil(t, ent.GetResource(), "embedded resource present")
		gotAnnos := annotations.Annotations(ent.GetResource().GetAnnotations())
		rawID := &v2.RawId{}
		ok, err := gotAnnos.Pick(rawID)
		require.NoError(t, err)
		require.True(t, ok, "embedded resource must carry the RawId annotation")
		require.Equal(t, "00gRAWID", rawID.GetId())
	}

	t.Run("GetEntitlement", func(t *testing.T) {
		resp, err := a.GetEntitlement(ctx, reader_v2.EntitlementsReaderServiceGetEntitlementRequest_builder{
			EntitlementId: entID,
		}.Build())
		require.NoError(t, err)
		requireRawID(t, resp.GetEntitlement())
	})

	t.Run("ListEntitlements", func(t *testing.T) {
		resp, err := a.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{}.Build())
		require.NoError(t, err)
		require.Len(t, resp.GetList(), 1)
		requireRawID(t, resp.GetList()[0])
	})

	t.Run("ListEntitlementsByIds", func(t *testing.T) {
		resp, err := a.ListEntitlementsByIds(ctx, reader_v2.EntitlementsReaderServiceListEntitlementsByIdsRequest_builder{
			EntitlementIds: []string{entID},
		}.Build())
		require.NoError(t, err)
		require.Len(t, resp.GetList(), 1)
		requireRawID(t, resp.GetList()[0])
	})

	t.Run("StreamEntitlements", func(t *testing.T) {
		var got []*v2.Entitlement
		for ent, err := range a.StreamEntitlements(ctx, "") {
			require.NoError(t, err)
			got = append(got, ent)
		}
		require.Len(t, got, 1)
		requireRawID(t, got[0])
	})
}
