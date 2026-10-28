package c1zsanitize

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// Fixture identifiers. The cross-engine matrix sanitizes the same logical
// graph through every engine combination under one secret, so the
// transformed ids are identical across arms and the outputs are directly
// comparable.
const (
	pfRTUser  = "user"  // declared resource type -> token preserved
	pfRTRole  = "role"  // declared resource type
	pfRTGroup = "group" // NOT declared -> token HMAC'd; used to exercise multiset

	pfEntAdmin  = "ent-admin"
	pfEntReader = "ent-reader"

	pfGrantMultiExpand = "grant-multi-expand" // multi-Ent + multi-RT (with a dup RT)
	pfGrantDivergence  = "grant-divergence"   // GrantExpandable AND populated Sources
	pfGrantWhitespace  = "grant-whitespace"   // only blank entitlement ids -> not expandable
	pfGrantSourced     = "grant-sourced"      // multi-edge Sources, no expansion
	pfGrantPlain       = "grant-plain"        // neither

	pfAssetID = "icon-asset-1"
)

// pfSourceIconBytes is the original asset payload. The sanitizer must
// REPLACE it with a content-type placeholder, never copy these bytes
// through (that would be an identity leak).
var pfSourceIconBytes = []byte{0xde, 0xad, 0xbe, 0xef, 0x01, 0x02, 0x03, 0x04}

// buildParityFixture writes the same logical graph into store through the
// engine-agnostic connectorstore.Writer surface, so the GrantExpandable
// annotations and Sources land in each engine's real side-state exactly as
// production files store them.
func buildParityFixture(t *testing.T, ctx context.Context, store c1zstore.Store) {
	t.Helper()

	_, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	require.NoError(t, store.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: pfRTUser, DisplayName: "User", Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER}}.Build(),
		v2.ResourceType_builder{Id: pfRTRole, DisplayName: "Role", Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_ROLE}}.Build(),
	))

	// A resolvable asset: PutAsset writes the bytes; a UserTrait icon ref on
	// a resource carries the id so the sanitizer's resource walk registers it
	// and copyAssets fetches it (a dangling ref would be skipped and break the
	// count assertion).
	require.NoError(t, store.PutAsset(ctx, v2.AssetRef_builder{Id: pfAssetID}.Build(), "image/png", pfSourceIconBytes))

	parent := v2.Resource_builder{
		Id:          v2.ResourceId_builder{ResourceType: pfRTRole, Resource: "admins"}.Build(),
		DisplayName: "Admins",
	}.Build()
	// children parent_resource_id points at parent -> structural-linkage check.
	children := make([]*v2.Resource, 0, 5)
	for i, id := range []string{"alice", "bob", "carol", "dave", "erin"} {
		display := "User " + id
		if i == 0 {
			display = "Alice"
		}
		var annos []*anypb.Any
		if i == 0 {
			annos = []*anypb.Any{anyTB(t, v2.UserTrait_builder{
				Login: "alice@acme.com",
				Icon:  v2.AssetRef_builder{Id: pfAssetID}.Build(),
			}.Build())}
		}
		children = append(children, v2.Resource_builder{
			Id:               v2.ResourceId_builder{ResourceType: pfRTUser, Resource: id}.Build(),
			ParentResourceId: v2.ResourceId_builder{ResourceType: pfRTRole, Resource: "admins"}.Build(),
			DisplayName:      display,
			Annotations:      annos,
		}.Build())
	}
	require.NoError(t, store.PutResources(ctx, append([]*v2.Resource{parent}, children...)...))

	entAdmin := v2.Entitlement_builder{Id: pfEntAdmin, Resource: parent, DisplayName: "admin", Slug: "admin"}.Build()
	entReader := v2.Entitlement_builder{Id: pfEntReader, Resource: parent, DisplayName: "reader", Slug: "reader"}.Build()
	require.NoError(t, store.PutEntitlements(ctx, entAdmin, entReader))

	sources := v2.GrantSources_builder{Sources: map[string]*v2.GrantSources_GrantSource{
		pfEntAdmin:  v2.GrantSources_GrantSource_builder{IsDirect: true}.Build(),
		pfEntReader: v2.GrantSources_GrantSource_builder{IsDirect: false}.Build(),
	}}.Build()

	grants := []*v2.Grant{
		// multi-EntitlementId + multi-ResourceTypeId, with pfRTGroup duplicated
		// so the resource_type_ids multiset (multiplicity 2) is exercised.
		v2.Grant_builder{
			Id: pfGrantMultiExpand, Entitlement: entAdmin, Principal: children[0],
			Annotations: annotations.New(v2.GrantExpandable_builder{
				EntitlementIds:  []string{pfEntAdmin, pfEntReader},
				Shallow:         true,
				ResourceTypeIds: []string{pfRTUser, pfRTGroup, pfRTGroup},
			}.Build()),
		}.Build(),
		// Divergence trigger: carries a GrantExpandable AND already-populated
		// Sources (expanded-but-still-annotated). Proves the first-write
		// needs_expansion projection is identical across engines for this shape.
		v2.Grant_builder{
			Id: pfGrantDivergence, Entitlement: entAdmin, Principal: children[1],
			Sources: sources,
			Annotations: annotations.New(v2.GrantExpandable_builder{
				EntitlementIds: []string{pfEntReader},
				Shallow:        false,
			}.Build()),
		}.Build(),
		// Blank entitlement ids only -> annotation stripped, needs_expansion=false
		// on both engines.
		v2.Grant_builder{
			Id: pfGrantWhitespace, Entitlement: entReader, Principal: children[2],
			Annotations: annotations.New(v2.GrantExpandable_builder{
				EntitlementIds: []string{"  ", ""},
			}.Build()),
		}.Build(),
		// Multi-edge Sources, no expansion.
		v2.Grant_builder{Id: pfGrantSourced, Entitlement: entReader, Principal: children[3], Sources: sources}.Build(),
		// Neither.
		v2.Grant_builder{Id: pfGrantPlain, Entitlement: entAdmin, Principal: children[4]}.Build(),
	}
	require.NoError(t, store.PutGrants(ctx, grants...))
	require.NoError(t, store.EndSync(ctx))
}

// newEngineStore opens a fresh writable store for the given engine.
func newEngineStore(t *testing.T, ctx context.Context, path string, eng c1zstore.Engine) c1zstore.Store {
	t.Helper()
	s, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(eng))
	require.NoError(t, err)
	return s
}

// openEngineStoreRO reopens a store read-only with its latest finished sync
// selected, so the neutral reader + Grants() surfaces resolve content.
func openEngineStoreRO(t *testing.T, ctx context.Context, path string) c1zstore.Store {
	t.Helper()
	s, err := dotc1z.NewStore(ctx, path, dotc1z.WithReadOnly(true))
	require.NoError(t, err)
	latest, err := s.SyncMeta().LatestFinishedSyncOfAnyType(ctx)
	require.NoError(t, err)
	require.NotNil(t, latest, "sanitized output must carry a finished sync")
	require.NoError(t, s.SetCurrentSync(ctx, latest.ID))
	return s
}

func grantCount(t *testing.T, ctx context.Context, store c1zstore.Store) int {
	t.Helper()
	n := 0
	pageToken := ""
	for {
		resp, err := store.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{PageSize: 1000, PageToken: pageToken}.Build())
		require.NoError(t, err)
		n += len(resp.GetList())
		if resp.GetNextPageToken() == "" {
			return n
		}
		pageToken = resp.GetNextPageToken()
	}
}

func resourceCount(t *testing.T, ctx context.Context, store c1zstore.Store) int {
	t.Helper()
	resp, err := store.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{PageSize: 1000}.Build())
	require.NoError(t, err)
	return len(resp.GetList())
}

func resourceTypeCount(t *testing.T, ctx context.Context, store c1zstore.Store) int {
	t.Helper()
	resp, err := store.ListResourceTypes(ctx, v2.ResourceTypesServiceListResourceTypesRequest_builder{PageSize: 1000}.Build())
	require.NoError(t, err)
	return len(resp.GetList())
}

func entitlementCount(t *testing.T, ctx context.Context, store c1zstore.Store) int {
	t.Helper()
	resp, err := store.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{PageSize: 1000}.Build())
	require.NoError(t, err)
	return len(resp.GetList())
}
