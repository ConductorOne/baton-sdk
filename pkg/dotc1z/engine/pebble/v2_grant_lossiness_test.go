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
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
)

// TestV2GrantRoundTrip_DropsFieldsProtectedByUnsafeForSlim is a direct
// round-trip test that locks in the fact that Pebble silently drops
// the v2.Grant fields the SQLite slim-blob writer's `unsafeForSlim`
// opt-out is designed to protect (see pkg/dotc1z/grants.go).
//
// Pebble persists grants as c1.storage.v3.GrantRecord, whose links
// to other entities are identity-only refs (EntitlementRef,
// PrincipalRef). On read, V3GrantToV2 synthesizes stub Entitlement
// and Principal messages carrying only the identity columns. The
// remaining rich fields the connector contract allows on those
// embedded messages — Principal.ParentResourceId, Principal.Annotations,
// Principal.DisplayName, Entitlement.DisplayName, Entitlement.Slug,
// Entitlement.Purpose, Entitlement.Resource.ParentResourceId,
// Entitlement.Resource.Annotations, Entitlement.Resource.DisplayName —
// are unrecoverable from the v3 record.
//
// This is the same lossiness SQLite's slim-blob mode produces, but
// SQLite is opt-in (`WithV2GrantsWriter`) and has a per-grant
// safety valve (`unsafeForSlim`). Pebble has neither: every grant
// is stored slim, with no escape hatch. The two syncer paths the
// SQLite opt-out defends against — `InsertResourceGrants` etag-
// replay (rebuilds `v1_resources` from `grant.Entitlement.Resource`)
// and `ExternalResourceMatch{All,Match,ID}` (builds bid keys from
// `grant.Principal.ParentResourceId`) — are therefore broken on
// Pebble whenever they read grants back from the store.
//
// The assertions below demonstrate the lossiness mechanically. The
// two follow-on tests (pkg/sync/pebble_etag_replay_corruption_test.go
// and pkg/sync/pebble_external_match_test.go) demonstrate the
// concrete user-visible corruption that results.
func TestV2GrantRoundTrip_DropsFieldsProtectedByUnsafeForSlim(t *testing.T) {
	ctx := context.Background()
	require.NoError(t, pebble.Register())

	store, err := dotc1z.NewStore(ctx,
		filepath.Join(t.TempDir(), "grant-lossiness.c1z"),
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

	// The entitlement's resource has a non-trivial DisplayName, a
	// parent ResourceId, and a GroupTrait annotation. The
	// InsertResourceGrants syncer path extracts this exact embedded
	// message from each grant and rewrites it into the resources
	// table on etag-replay.
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

	// The principal has a non-trivial DisplayName, a parent
	// ResourceId, and a UserTrait annotation. The
	// processGrantsWithExternalPrincipals syncer path computes
	// bid.MakeBid(grant.Principal) — that bid encodes the parent
	// resource id when present. Dropping the parent silently
	// changes the bid.
	principal := v2.Resource_builder{
		Id:               v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(),
		ParentResourceId: v2.ResourceId_builder{ResourceType: "org", Resource: "acme"}.Build(),
		DisplayName:      "User One",
		Annotations:      []*anypb.Any{userTraitAny},
	}.Build()

	// Write the resource + entitlement on their own (they round-trip
	// correctly via the resource_types/resources/entitlements tables;
	// the lossiness we're demonstrating is specifically the embedded
	// copies on the Grant message).
	require.NoError(t, store.PutResources(ctx, entResource, principal))
	require.NoError(t, store.PutEntitlements(ctx, ent))

	grant := v2.Grant_builder{
		Id:          "grant-1",
		Entitlement: ent,
		Principal:   principal,
	}.Build()
	require.NoError(t, store.PutGrants(ctx, grant))

	// Read the grant back via ListGrants. This is the path the
	// syncer's processGrantsWithExternalPrincipals and the
	// InsertResourceGrants etag-replay code paths take.
	resp, err := store.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{
		PageSize: 100,
	}.Build())
	require.NoError(t, err)
	require.Len(t, resp.GetList(), 1)
	got := resp.GetList()[0]
	require.Equal(t, "grant-1", got.GetId())

	t.Run("Principal.ParentResourceId is dropped", func(t *testing.T) {
		require.NotNil(t, got.GetPrincipal())
		require.NotNil(t, got.GetPrincipal().GetParentResourceId(),
			"Pebble round-trip dropped Principal.ParentResourceId; bid.MakeBid will now produce a no-parent bid, breaking ExternalResourceMatch expansion")
		require.Equal(t, "org", got.GetPrincipal().GetParentResourceId().GetResourceType())
		require.Equal(t, "acme", got.GetPrincipal().GetParentResourceId().GetResource())
	})

	t.Run("Principal.DisplayName is dropped", func(t *testing.T) {
		require.Equal(t, "User One", got.GetPrincipal().GetDisplayName(),
			"Pebble round-trip dropped Principal.DisplayName")
	})

	t.Run("Principal.Annotations are dropped", func(t *testing.T) {
		require.NotEmpty(t, got.GetPrincipal().GetAnnotations(),
			"Pebble round-trip dropped Principal.Annotations (UserTrait); downstream consumers reading user traits off the embedded principal silently see no traits")
	})

	t.Run("Entitlement.Resource.ParentResourceId is dropped", func(t *testing.T) {
		entRes := got.GetEntitlement().GetResource()
		require.NotNil(t, entRes)
		require.NotNil(t, entRes.GetParentResourceId(),
			"Pebble round-trip dropped Entitlement.Resource.ParentResourceId; InsertResourceGrants etag-replay will rewrite v1_resources with a parent-less resource, corrupting the hierarchy")
		require.Equal(t, "org", entRes.GetParentResourceId().GetResourceType())
		require.Equal(t, "acme", entRes.GetParentResourceId().GetResource())
	})

	t.Run("Entitlement.Resource.DisplayName is dropped", func(t *testing.T) {
		require.Equal(t, "Group One", got.GetEntitlement().GetResource().GetDisplayName(),
			"Pebble round-trip dropped Entitlement.Resource.DisplayName; InsertResourceGrants etag-replay rewrites v1_resources with empty DisplayName")
	})

	t.Run("Entitlement.Resource.Annotations are dropped", func(t *testing.T) {
		require.NotEmpty(t, got.GetEntitlement().GetResource().GetAnnotations(),
			"Pebble round-trip dropped Entitlement.Resource.Annotations (GroupTrait); InsertResourceGrants etag-replay rewrites v1_resources with no GroupTrait")
	})

	t.Run("Entitlement.DisplayName is dropped", func(t *testing.T) {
		require.Equal(t, "Members of Group One", got.GetEntitlement().GetDisplayName(),
			"Pebble round-trip dropped Entitlement.DisplayName")
	})

	t.Run("Entitlement.Description is dropped", func(t *testing.T) {
		require.Equal(t, "Membership in the engineering org group", got.GetEntitlement().GetDescription(),
			"Pebble round-trip dropped Entitlement.Description")
	})

	t.Run("Entitlement.Purpose is dropped", func(t *testing.T) {
		require.Equal(t, v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT, got.GetEntitlement().GetPurpose(),
			"Pebble round-trip dropped Entitlement.Purpose")
	})

	t.Run("Entitlement.Slug is dropped", func(t *testing.T) {
		require.Equal(t, "member", got.GetEntitlement().GetSlug(),
			"Pebble round-trip dropped Entitlement.Slug on the grant's embedded entitlement; bid.MakeBid on the embedded Entitlement now fails")
	})

	t.Run("Entitlement.GrantableTo is dropped", func(t *testing.T) {
		require.NotEmpty(t, got.GetEntitlement().GetGrantableTo(),
			"Pebble round-trip dropped Entitlement.GrantableTo on the grant's embedded entitlement")
	})
}
