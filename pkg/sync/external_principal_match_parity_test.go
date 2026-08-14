package sync //nolint:revive,nolintlint // matches the existing package name

import (
	"context"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/bid"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	et "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	gt "github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
)

// The external-principal index (WithExternalPrincipalIndex) is a drop-in
// replacement for the linear scan that preceded it: same grants, found a
// faster way. That claim is the entire basis for being willing to flip the
// flag on a live tenant, so it is asserted directly here -- one fixture, run
// through processGrantsWithExternalPrincipals once in each mode, with the
// resulting store contents required to be byte-identical.
//
// The fixture deliberately covers every arm of both matchers:
//
//   - a user matched on a profile key that is not "email"
//   - a user matched on a user-trait email address
//   - a user matched BOTH ways at once, which must still yield one grant
//   - a user whose user trait cannot be read, which must be skipped outright
//     (including for the profile key it would otherwise match)
//   - group principals matched on a profile key, case-insensitively, one with
//     a resolvable expandable entitlement and one without
//   - a match key no principal carries
//   - a match against a trait that is not configured for matching
//   - ExternalResourceMatchAll and ExternalResourceMatchID grants, which the
//     flag does not fork and which must therefore also come out identical

// parityPrincipal marks a resource as an external principal. Only resources
// carrying a BatonID annotation are considered by the matcher.
func parityPrincipal(t *testing.T, principal *v2.Resource) *v2.Resource {
	t.Helper()
	annos := annotations.Annotations(principal.GetAnnotations())
	annos.Update(&v2.BatonID{})
	principal.SetAnnotations(annos)
	return principal
}

// parityUnreadableUserTrait replaces the resource's user-trait annotation with
// one that still claims to be a UserTrait but cannot be unmarshalled, which is
// the shape resource.GetUserTrait fails on in production. The resource stays a
// candidate principal (the annotation is still present and of the right type)
// but neither matcher may match it.
func parityUnreadableUserTrait(t *testing.T, principal *v2.Resource) *v2.Resource {
	t.Helper()
	annos := principal.GetAnnotations()
	replaced := false
	for i, anno := range annos {
		if !anno.MessageIs(&v2.UserTrait{}) {
			continue
		}
		annos[i] = &anypb.Any{TypeUrl: anno.GetTypeUrl(), Value: []byte{0xff, 0xff}}
		replaced = true
	}
	require.True(t, replaced, "fixture premise: principal must carry a user trait annotation")
	principal.SetAnnotations(annos)

	_, err := rs.GetUserTrait(principal)
	require.Error(t, err, "fixture premise: user trait must be unreadable")
	return principal
}

func parityPrincipals(t *testing.T) []*v2.Resource {
	t.Helper()
	return []*v2.Resource{
		// Matches "upn"/target@example.com, differing in case.
		parityPrincipal(t, testUserPrincipal(t, "user_profile", map[string]any{"upn": "Target@Example.com"})),
		// Matches "email"/trait@example.com via the user trait only.
		parityPrincipal(t, testUserPrincipal(t, "user_trait", nil,
			rs.WithEmail("trait@example.com", true),
			rs.WithEmail("secondary@example.com", false),
		)),
		// Matches "email"/shared@example.com both ways: one grant, not two.
		parityPrincipal(t, testUserPrincipal(t, "user_both", map[string]any{"email": "shared@example.com"},
			rs.WithEmail("SHARED@example.com", true),
		)),
		// Would match "upn"/target@example.com, but its user trait is
		// unreadable, so it is skipped before the profile is considered.
		parityPrincipal(t, parityUnreadableUserTrait(t,
			testUserPrincipal(t, "user_unreadable", map[string]any{"upn": "target@example.com"}))),
		// Matches nothing.
		parityPrincipal(t, testUserPrincipal(t, "user_none", map[string]any{"upn": "nobody@example.com"})),

		parityPrincipal(t, testGroupPrincipal(t, "group_expandable", map[string]any{"external_id": "ext_123"})),
		// Same value in different case: matched, but with no entitlement to
		// remap the expandable annotation onto.
		parityPrincipal(t, testGroupPrincipal(t, "group_unexpandable", map[string]any{"external_id": "EXT_123"})),
		parityPrincipal(t, testGroupPrincipal(t, "group_none", map[string]any{"external_id": "ext_999"})),
	}
}

// parityCarrierGrants builds the grants the matcher scans. Each one is
// annotated so it is a candidate, and each targets its own entitlement so the
// digest attributes every produced grant to the carrier that produced it.
func parityCarrierGrants(t *testing.T, expandableEntitlementBID string) []*v2.Grant {
	t.Helper()

	placeholderUser := v2.ResourceId_builder{
		ResourceType: userResourceType.GetId(),
		Resource:     "placeholder_user",
	}.Build()
	placeholderGroup := v2.ResourceId_builder{
		ResourceType: groupResourceType.GetId(),
		Resource:     "placeholder_group",
	}.Build()

	sourceGroup := testGroupPrincipal(t, "source_group", map[string]any{"external_id": "source"})

	match := func(slug string, principalID *v2.ResourceId, anno *v2.ExternalResourceMatch, extra ...gt.GrantOption) *v2.Grant {
		opts := append([]gt.GrantOption{gt.WithAnnotation(anno)}, extra...)
		return gt.NewGrant(sourceGroup, slug, principalID, opts...)
	}

	return []*v2.Grant{
		match("user-profile-key", placeholderUser, v2.ExternalResourceMatch_builder{
			Key: "upn", Value: "target@example.com", ResourceType: v2.ResourceType_TRAIT_USER,
		}.Build()),
		match("user-trait-email", placeholderUser, v2.ExternalResourceMatch_builder{
			Key: "email", Value: "TRAIT@example.com", ResourceType: v2.ResourceType_TRAIT_USER,
		}.Build()),
		match("user-email-both-ways", placeholderUser, v2.ExternalResourceMatch_builder{
			Key: "email", Value: "shared@example.com", ResourceType: v2.ResourceType_TRAIT_USER,
		}.Build()),
		match("user-unknown-key", placeholderUser, v2.ExternalResourceMatch_builder{
			Key: "employeeNumber", Value: "12345", ResourceType: v2.ResourceType_TRAIT_USER,
		}.Build()),
		match("group-profile", placeholderGroup, v2.ExternalResourceMatch_builder{
			Key: "external_id", Value: "ext_123", ResourceType: v2.ResourceType_TRAIT_GROUP,
		}.Build()),
		// Same match, but expandable: the new grant's GrantExpandable
		// annotation is remapped onto whichever matched principal actually has
		// the entitlement.
		match("group-profile-expandable", placeholderGroup, v2.ExternalResourceMatch_builder{
			Key: "external_id", Value: "ext_123", ResourceType: v2.ResourceType_TRAIT_GROUP,
		}.Build(), gt.WithAnnotation(v2.GrantExpandable_builder{
			EntitlementIds: []string{expandableEntitlementBID},
		}.Build())),
		// A trait nobody is configured to match on: logged and dropped.
		match("unconfigured-trait", placeholderUser, v2.ExternalResourceMatch_builder{
			Key: "external_id", Value: "ext_123", ResourceType: v2.ResourceType_TRAIT_SECRET,
		}.Build()),

		// Neither of the following is forked by the flag; they are here so the
		// digest would catch a refactor that disturbed them.
		gt.NewGrant(sourceGroup, "match-all-groups", placeholderGroup,
			gt.WithAnnotation(v2.ExternalResourceMatchAll_builder{
				ResourceType: v2.ResourceType_TRAIT_GROUP,
			}.Build())),
		gt.NewGrant(sourceGroup, "match-by-id", placeholderUser,
			gt.WithAnnotation(v2.ExternalResourceMatchID_builder{
				Id: "user_none",
			}.Build())),
	}
}

// parityRun seeds a store, runs the external-principal match once in the
// requested mode, and returns the resulting grants both as a deterministic
// digest (proto bytes, sorted) and as a readable entitlement -> principals map.
func parityRun(
	t *testing.T,
	engine c1zstore.Engine,
	traits []v2.ResourceType_Trait,
	indexed bool,
) ([]string, map[string][]string) {
	t.Helper()
	ctx := t.Context()

	dir := t.TempDir()
	path := filepath.Join(dir, "parity.c1z")
	store, err := dotc1z.NewStore(ctx, path,
		dotc1z.WithEngine(engine),
		dotc1z.WithTmpDir(dir),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close(ctx) })

	syncID, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NotEmpty(t, syncID)

	principals := parityPrincipals(t)

	// The expandable carrier grant points at an entitlement on the group it
	// was granted from; only group_expandable has the matching entitlement, so
	// the other matched group exercises the not-found arm.
	sourceGroup := testGroupPrincipal(t, "source_group", map[string]any{"external_id": "source"})
	sourceEntitlement := et.NewAssignmentEntitlement(sourceGroup, "member")
	expandableBID, err := bid.MakeBid(sourceEntitlement)
	require.NoError(t, err)

	var expandableTarget *v2.Resource
	for _, principal := range principals {
		if principal.GetId().GetResource() == "group_expandable" {
			expandableTarget = principal
		}
	}
	require.NotNil(t, expandableTarget)
	require.NoError(t, store.PutEntitlements(ctx,
		sourceEntitlement,
		et.NewAssignmentEntitlement(expandableTarget, "member"),
	))

	require.NoError(t, store.PutGrants(ctx, parityCarrierGrants(t, expandableBID)...))

	state := newState()
	state.SetHasExternalResourcesGrants()
	state.PushAction(ctx, Action{Op: SyncExternalResourcesOp})

	s := &syncer{
		store:                         store,
		state:                         state,
		externalResourceTraits:        traits,
		externalPrincipalIndexEnabled: indexed,
	}
	require.NoError(t, s.processGrantsWithExternalPrincipals(ctx, principals))

	return parityGrantDigest(ctx, t, store)
}

func parityGrantDigest(ctx context.Context, t *testing.T, store c1zstore.Store) ([]string, map[string][]string) {
	t.Helper()

	digest := make([]string, 0)
	byEntitlement := make(map[string][]string)
	pageToken := ""
	for {
		resp, err := store.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{
			PageToken: pageToken,
		}.Build())
		require.NoError(t, err)
		for _, grant := range resp.GetList() {
			wire, err := (proto.MarshalOptions{Deterministic: true}).Marshal(grant)
			require.NoError(t, err)
			digest = append(digest, hex.EncodeToString(wire))

			slug := grant.GetEntitlement().GetSlug()
			if slug == "" {
				slug = grant.GetEntitlement().GetId()
			}
			byEntitlement[slug] = append(byEntitlement[slug], fmt.Sprintf("%s:%s",
				grant.GetPrincipal().GetId().GetResourceType(),
				grant.GetPrincipal().GetId().GetResource(),
			))
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	slices.Sort(digest)
	for slug := range byEntitlement {
		slices.Sort(byEntitlement[slug])
	}
	return digest, byEntitlement
}

// TestExternalPrincipalMatchLegacyIndexedParity is the safety proof for
// WithExternalPrincipalIndex: flipping the flag must not change a single
// grant. It is not a substitute for the unit tests of either matcher -- it
// asserts they agree, and separately pins what they agree ON, so that a change
// breaking both in the same direction still fails.
func TestExternalPrincipalMatchLegacyIndexedParity(t *testing.T) {
	for _, engine := range []c1zstore.Engine{c1zstore.EngineSQLite, c1zstore.EnginePebble} {
		t.Run(string(engine), func(t *testing.T) {
			for _, tc := range []struct {
				name   string
				traits []v2.ResourceType_Trait
			}{
				// TRAIT_USER/TRAIT_GROUP, the default when a connector never
				// calls WithExternalResourceTraits.
				{name: "default_traits"},
				// A TRAIT_USER match arrives for a trait that is not configured
				// for matching: the legacy scan walks an empty principal slice,
				// the indexed path has no index to consult. Neither matches.
				{name: "group_trait_only", traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_GROUP}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					legacyDigest, legacyGrants := parityRun(t, engine, tc.traits, false)
					indexedDigest, indexedGrants := parityRun(t, engine, tc.traits, true)

					require.Equal(t, legacyGrants, indexedGrants,
						"indexed matcher produced a different grant set than the linear scan")
					require.Equal(t, legacyDigest, indexedDigest,
						"indexed matcher produced grants that differ in their bytes")
					require.NotEmpty(t, legacyDigest, "fixture premise: the run must produce grants")
				})
			}
		})
	}
}

// TestExternalPrincipalMatchParityFixtureExpectations pins what both matchers
// are expected to agree on. Without it the parity assertion above could be
// satisfied by two matchers that are equally wrong.
func TestExternalPrincipalMatchParityFixtureExpectations(t *testing.T) {
	// Keyed by entitlement id ("<resource type>:<resource>:<slug>"), which is
	// how the carrier grants above are addressed.
	expected := map[string][]string{
		"group:source_group:user-profile-key":     {"user:user_profile"},
		"group:source_group:user-trait-email":     {"user:user_trait"},
		"group:source_group:user-email-both-ways": {"user:user_both"},
		"group:source_group:group-profile": {
			"group:group_expandable",
			"group:group_unexpandable",
		},
		"group:source_group:group-profile-expandable": {
			"group:group_expandable",
			"group:group_unexpandable",
		},
		"group:source_group:match-all-groups": {
			"group:group_expandable",
			"group:group_none",
			"group:group_unexpandable",
		},
		"group:source_group:match-by-id": {"user:user_none"},
		// user-unknown-key and unconfigured-trait match nothing, so their
		// carrier grants are deleted and leave no entry at all.
	}

	for _, indexed := range []bool{false, true} {
		t.Run(fmt.Sprintf("indexed=%t", indexed), func(t *testing.T) {
			_, grants := parityRun(t, c1zstore.EngineSQLite, nil, indexed)
			require.Equal(t, expected, grants)
		})
	}
}

// A user principal whose user trait cannot be read is skipped by both
// matchers even when its profile would have matched -- the behavior the
// linear scan had, preserved by the index's skip list. It is asserted
// separately because it is an absence, and an absence is exactly what a
// digest comparison of two identically-broken matchers would not catch.
func TestExternalPrincipalMatchParitySkipsUnreadableUserTrait(t *testing.T) {
	for _, indexed := range []bool{false, true} {
		t.Run(fmt.Sprintf("indexed=%t", indexed), func(t *testing.T) {
			_, grants := parityRun(t, c1zstore.EngineSQLite, nil, indexed)
			for slug, principals := range grants {
				require.NotContains(t, principals, "user:user_unreadable",
					"principal with an unreadable user trait must not be granted %q", slug)
			}
		})
	}
}
