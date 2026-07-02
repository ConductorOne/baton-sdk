package pebble_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
)

// TestV2GrantRoundTrip_V3Contract documents and locks in the v3 grant
// round-trip contract on Pebble.
//
// Pebble persists grants as c1.storage.v3.GrantRecord, whose links to
// other entities are identity-only refs (EntitlementRef, PrincipalRef).
// On read, V3GrantToV2 synthesizes stub Entitlement / Principal
// messages. This is by design — rich entity data (DisplayName,
// traits, slug, description, grantable_to, ...) lives in the
// resources / entitlements tables and is fetched via
// GetResource / GetEntitlement, NOT redundantly embedded in every
// grant. That slimness is what lets Pebble scale to very large grant
// counts; embedding rich data per grant would defeat it.
//
// The contract this test enforces:
//
//   - Grant identity round-trips: grant id, entitlement id, the
//     entitlement's resource id, and the principal id.
//   - The principal's ParentResourceId round-trips. This is part of
//     the principal's canonical identity (its bid) — the syncer's
//     processGrantsWithExternalPrincipals builds bid.MakeBid from the
//     grant's principal, which encodes the parent — so it is
//     preserved on PrincipalRef rather than requiring a resources-
//     table lookup.
//   - Rich, display-only fields are NOT carried on the embedded grant
//     entities. Consumers that need them fetch the owning record. The
//     assertions below document this so a future change that starts
//     reading these off a grant is caught.
func TestV2GrantRoundTrip_V3Contract(t *testing.T) {
	ctx := context.Background()

	store, err := dotc1z.NewStore(ctx,
		filepath.Join(t.TempDir(), "grant-roundtrip.c1z"),
		dotc1z.WithEngine(dotc1z.EnginePebble),
	)
	require.NoError(t, err)
	defer func() { _ = store.Close(ctx) }()

	_, err = store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	require.NoError(t, store.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "org"}.Build(),
		v2.ResourceType_builder{Id: "group"}.Build(),
		v2.ResourceType_builder{Id: "user"}.Build(),
	))

	groupTraitAny, err := anypb.New(&v2.GroupTrait{})
	require.NoError(t, err)
	userTraitAny, err := anypb.New(&v2.UserTrait{})
	require.NoError(t, err)

	entResource := v2.Resource_builder{
		Id:               v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(),
		ParentResourceId: v2.ResourceId_builder{ResourceType: "org", Resource: "acme"}.Build(),
		DisplayName:      "Group One",
		Annotations:      []*anypb.Any{groupTraitAny},
	}.Build()
	ent := v2.Entitlement_builder{
		Id:          "ent-A",
		Resource:    entResource,
		DisplayName: "Members of Group One",
		Description: "Membership in the engineering org group",
		Purpose:     v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT,
		Slug:        "member",
		GrantableTo: []*v2.ResourceType{
			v2.ResourceType_builder{Id: "user"}.Build(),
		},
	}.Build()

	// The principal carries a parent ResourceId. The syncer's
	// processGrantsWithExternalPrincipals computes
	// bid.MakeBid(grant.Principal); that bid encodes the parent when
	// present, so the parent MUST round-trip on the grant.
	principal := v2.Resource_builder{
		Id:               v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(),
		ParentResourceId: v2.ResourceId_builder{ResourceType: "org", Resource: "acme"}.Build(),
		DisplayName:      "User One",
		Annotations:      []*anypb.Any{userTraitAny},
	}.Build()

	require.NoError(t, store.PutResources(ctx, entResource, principal))
	require.NoError(t, store.PutEntitlements(ctx, ent))

	grant := v2.Grant_builder{
		Id:          "grant-1",
		Entitlement: ent,
		Principal:   principal,
	}.Build()
	require.NoError(t, store.PutGrants(ctx, grant))

	resp, err := store.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{
		PageSize: 100,
	}.Build())
	require.NoError(t, err)
	require.Len(t, resp.GetList(), 1)
	got := resp.GetList()[0]
	require.Equal(t, "group:g1:custom:ent-A:user:u1", got.GetId())

	t.Run("grant identity round-trips", func(t *testing.T) {
		require.Equal(t, "group:g1:custom:ent-A", got.GetEntitlement().GetId())
		require.Equal(t, "group", got.GetEntitlement().GetResource().GetId().GetResourceType())
		require.Equal(t, "g1", got.GetEntitlement().GetResource().GetId().GetResource())
		require.Equal(t, "user", got.GetPrincipal().GetId().GetResourceType())
		require.Equal(t, "u1", got.GetPrincipal().GetId().GetResource())
	})

	t.Run("principal ParentResourceId round-trips", func(t *testing.T) {
		parent := got.GetPrincipal().GetParentResourceId()
		require.NotNil(t, parent,
			"principal parent must survive the v3 round-trip; bid.MakeBid(grant.Principal) in "+
				"processGrantsWithExternalPrincipals encodes it, so dropping it silently breaks "+
				"external-resource-match expansion for parented principals")
		require.Equal(t, "org", parent.GetResourceType())
		require.Equal(t, "acme", parent.GetResource())
	})

	// The following document the v3 slim-grant contract: rich,
	// display-only fields are intentionally NOT embedded on a grant —
	// they live in the resources / entitlements tables. If any of
	// these starts being populated off the grant, a consumer began
	// depending on embedded rich data and this test should be
	// revisited (along with the read-path cost it implies).
	t.Run("rich entity fields are not embedded on the grant (fetched separately)", func(t *testing.T) {
		require.Empty(t, got.GetPrincipal().GetDisplayName(),
			"principal DisplayName is not embedded on the grant; fetch the resource record")
		require.Empty(t, got.GetPrincipal().GetAnnotations(),
			"principal annotations/traits are not embedded on the grant; fetch the resource record")
		require.Empty(t, got.GetEntitlement().GetDisplayName(),
			"entitlement DisplayName is not embedded on the grant; fetch the entitlement record")
		require.Empty(t, got.GetEntitlement().GetSlug(),
			"entitlement slug is not embedded on the grant; fetch the entitlement record")
		require.Empty(t, got.GetEntitlement().GetResource().GetDisplayName(),
			"entitlement-resource DisplayName is not embedded on the grant; fetch the resource record")
	})

	// The rich data IS available from the owning tables — the v3
	// design just relocates it off the grant edge.
	t.Run("rich data is recoverable from the owning records", func(t *testing.T) {
		rresp, err := store.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
			ResourceTypeId: "group",
		}.Build())
		require.NoError(t, err)
		var g1 *v2.Resource
		for _, r := range rresp.GetList() {
			if r.GetId().GetResource() == "g1" {
				g1 = r
				break
			}
		}
		require.NotNil(t, g1)
		require.Equal(t, "Group One", g1.GetDisplayName())
		require.NotNil(t, g1.GetParentResourceId())
		require.NotEmpty(t, g1.GetAnnotations())

		eresp, err := store.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{}.Build())
		require.NoError(t, err)
		require.Len(t, eresp.GetList(), 1)
		require.Equal(t, "member", eresp.GetList()[0].GetSlug())
		require.Equal(t, "Members of Group One", eresp.GetList()[0].GetDisplayName())
	})
}
