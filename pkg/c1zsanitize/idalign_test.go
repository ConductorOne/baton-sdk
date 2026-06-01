package c1zsanitize

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/types/entitlement"
	"github.com/conductorone/baton-sdk/pkg/types/grant"
)

// Canonical baton IDs are composite: an entitlement ID is
// "resourceType:resourceID:permission" and a grant ID is
// "entitlementID:principalType:principalID". This proves the
// component-aware transform keeps a sanitized grant's embedded
// entitlement and principal references equal to the separately-
// sanitized entitlement row, resource rows, and grant-sources keys —
// not an opaque whole-string hash that would no longer decompose.
func TestSanitizeCanonicalIDComponentAlignment(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	srcPath := filepath.Join(tmp, "src.c1z")
	dstPath := filepath.Join(tmp, "dst.c1z")
	secret := bytes32("idalign")

	groupRes := v2.Resource_builder{
		Id:          v2.ResourceId_builder{ResourceType: "group", Resource: "eng"}.Build(),
		DisplayName: "Engineering",
	}.Build()
	alice := v2.Resource_builder{
		Id:          v2.ResourceId_builder{ResourceType: "user", Resource: "alice"}.Build(),
		DisplayName: "Alice",
	}.Build()
	bob := v2.Resource_builder{
		Id:          v2.ResourceId_builder{ResourceType: "user", Resource: "bob"}.Build(),
		DisplayName: "Bob",
	}.Build()

	entID := entitlement.NewEntitlementID(groupRes, "member")
	ent := v2.Entitlement_builder{
		Id:          entID,
		DisplayName: "Member of engineering",
		Resource:    groupRes,
		Slug:        "member",
		Purpose:     v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT,
	}.Build()

	directGrant := v2.Grant_builder{
		Id:          grant.NewGrantID(alice, ent),
		Entitlement: ent,
		Principal:   alice,
	}.Build()
	// A grant whose source is the same entitlement, exercising the
	// GrantSources map-key path.
	sourcedGrant := v2.Grant_builder{
		Id:          grant.NewGrantID(bob, ent),
		Entitlement: ent,
		Principal:   bob,
		Sources: v2.GrantSources_builder{Sources: map[string]*v2.GrantSources_GrantSource{
			entID: v2.GrantSources_GrantSource_builder{IsDirect: true}.Build(),
		}}.Build(),
	}.Build()

	src := mustOpen(t, ctx, srcPath, false)
	_, err := src.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, src.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "group", DisplayName: "Group", Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_GROUP}}.Build(),
		v2.ResourceType_builder{Id: "user", DisplayName: "User", Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER}}.Build(),
	))
	require.NoError(t, src.PutResources(ctx, groupRes, alice, bob))
	require.NoError(t, src.PutEntitlements(ctx, ent))
	require.NoError(t, src.PutGrants(ctx, directGrant, sourcedGrant))
	require.NoError(t, src.EndSync(ctx))
	require.NoError(t, src.Close(ctx))

	srcRO := mustOpen(t, ctx, srcPath, true)
	defer srcRO.Close(ctx)
	dst := mustOpen(t, ctx, dstPath, false)
	require.NoError(t, Sanitize(ctx, srcRO, dst, Options{Secret: secret}))
	require.NoError(t, dst.Close(ctx))

	dstRO := mustOpen(t, ctx, dstPath, true)
	defer dstRO.Close(ctx)
	rec := collectRecords(t, ctx, dstRO)

	h := func(x string) string { return SanitizeID(secret, x) }
	wantEntID := strings.Join([]string{"group", h("eng"), h("member")}, ":")
	wantAliceGrantID := strings.Join([]string{wantEntID, "user", h("alice")}, ":")

	require.Len(t, rec.entitlements, 1)
	gotEnt := rec.entitlements[0]
	require.Equal(t, wantEntID, gotEnt.GetId(),
		"entitlement id must be transformed component-wise, not hashed whole")

	// The resourceID embedded in the entitlement id must equal the
	// separately-sanitized resource row's id.
	require.Equal(t, "group", gotEnt.GetResource().GetId().GetResourceType())
	require.Equal(t, h("eng"), gotEnt.GetResource().GetId().GetResource())
	require.Equal(t, gotEnt.GetResource().GetId().GetResource(), strings.Split(gotEnt.GetId(), ":")[1],
		"entitlement id's resourceID component must match its resource row")

	byPrincipal := map[string]*v2.Grant{}
	for _, g := range rec.grants {
		byPrincipal[g.GetPrincipal().GetId().GetResource()] = g
	}
	gotDirect := byPrincipal[h("alice")]
	require.NotNil(t, gotDirect, "alice's grant must be present under the sanitized principal id")

	require.Equal(t, wantAliceGrantID, gotDirect.GetId(),
		"grant id must reconstruct from sanitized entitlement + preserved principal type + sanitized principal id")
	// Embedded references align with the separately-sanitized rows.
	require.Equal(t, gotEnt.GetId(), gotDirect.GetEntitlement().GetId(),
		"grant's entitlement reference must equal the entitlement row id")
	require.Equal(t, gotEnt.GetId(), grantIDEntitlementPrefix(gotDirect.GetId()),
		"grant id's embedded entitlement prefix must equal the entitlement row id")
	require.Equal(t, gotDirect.GetPrincipal().GetId().GetResource(), lastField(gotDirect.GetId()),
		"grant id's principal-id component must equal the principal resource row id")

	// GrantSources map keys are source entitlement IDs and must match
	// the entitlement row id too.
	gotSourced := byPrincipal[h("bob")]
	require.NotNil(t, gotSourced)
	require.Contains(t, gotSourced.GetSources().GetSources(), gotEnt.GetId(),
		"grant-sources key must equal the sanitized entitlement row id")
}

// Connectors emit non-canonical composite IDs. Microsoft Entra grant
// IDs carry tenant group/user UUIDs in slots the canonical grammar
// reserves for type tokens, e.g.
// "group-grant:<groupUUID>:<principalType>:<userUUID>:<...>:<perm>".
// Positional trust would preserve the second-to-last "type" field
// verbatim and leak a tenant group UUID. This asserts the fail-closed
// transform HMACs every component that is not a declared resource
// type, so no raw source UUID survives anywhere in the sanitized ID
// strings.
func TestSanitizeGrantIDNoUUIDLeak(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	srcPath := filepath.Join(tmp, "src.c1z")
	dstPath := filepath.Join(tmp, "dst.c1z")
	secret := bytes32("entra-leak")

	const (
		groupUUID = "11111111-1111-1111-1111-111111111111"
		userUUID  = "22222222-2222-2222-2222-222222222222"
		extraUUID = "33333333-3333-3333-3333-333333333333"
	)
	srcUUIDs := []string{groupUUID, userUUID, extraUUID}

	groupRes := v2.Resource_builder{
		Id:          v2.ResourceId_builder{ResourceType: "group", Resource: groupUUID}.Build(),
		DisplayName: "A group",
	}.Build()
	userRes := v2.Resource_builder{
		Id:          v2.ResourceId_builder{ResourceType: "user", Resource: userUUID}.Build(),
		DisplayName: "A user",
	}.Build()

	// Entitlement id carries the group UUID in a non-type slot; grant id
	// is the Entra shape with the group UUID sitting where canonical
	// grammar expects a type, plus an extra opaque UUID field.
	entID := strings.Join([]string{"group", groupUUID, "member"}, ":")
	ent := v2.Entitlement_builder{
		Id:          entID,
		DisplayName: "Member",
		Resource:    groupRes,
		Slug:        "member",
		Purpose:     v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT,
	}.Build()
	leakyGrantID := strings.Join([]string{"group-grant", groupUUID, "user", userUUID, extraUUID, "member"}, ":")
	g := v2.Grant_builder{
		Id:          leakyGrantID,
		Entitlement: ent,
		Principal:   userRes,
	}.Build()

	src := mustOpen(t, ctx, srcPath, false)
	_, err := src.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, src.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "group", DisplayName: "Group", Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_GROUP}}.Build(),
		v2.ResourceType_builder{Id: "user", DisplayName: "User", Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER}}.Build(),
	))
	require.NoError(t, src.PutResources(ctx, groupRes, userRes))
	require.NoError(t, src.PutEntitlements(ctx, ent))
	require.NoError(t, src.PutGrants(ctx, g))
	require.NoError(t, src.EndSync(ctx))
	require.NoError(t, src.Close(ctx))

	srcRO := mustOpen(t, ctx, srcPath, true)
	defer srcRO.Close(ctx)
	dst := mustOpen(t, ctx, dstPath, false)
	require.NoError(t, Sanitize(ctx, srcRO, dst, Options{Secret: secret}))
	require.NoError(t, dst.Close(ctx))

	dstRO := mustOpen(t, ctx, dstPath, true)
	defer dstRO.Close(ctx)
	rec := collectRecords(t, ctx, dstRO)

	require.Len(t, rec.grants, 1)
	require.Len(t, rec.entitlements, 1)
	gotGrant := rec.grants[0]
	gotEnt := rec.entitlements[0]

	// No raw source UUID may survive in any sanitized id string.
	scan := []string{
		gotGrant.GetId(),
		gotGrant.GetEntitlement().GetId(),
		gotGrant.GetPrincipal().GetId().GetResource(),
		gotEnt.GetId(),
		gotEnt.GetResource().GetId().GetResource(),
	}
	for _, field := range scan {
		for _, u := range srcUUIDs {
			require.NotContains(t, field, u, "raw source UUID leaked in sanitized id %q", field)
		}
	}

	// The non-type leading token is opaque and must be HMAC'd, not kept.
	require.NotContains(t, gotGrant.GetId(), "group-grant", "non-type leading token must be HMAC'd")
	// Declared resource-type tokens are structural and stay in cleartext
	// so the id still lines up with the sanitized principal/resource type
	// fields.
	grantParts := strings.Split(gotGrant.GetId(), ":")
	require.Contains(t, grantParts, "user", "declared principal type must be preserved")
	require.Equal(t, "user", gotGrant.GetPrincipal().GetId().GetResourceType())
	require.Equal(t, "group", strings.Split(gotEnt.GetId(), ":")[0], "declared resource type must be preserved")

	// Structured references still resolve after the fail-closed rewrite.
	require.Equal(t, gotEnt.GetId(), gotGrant.GetEntitlement().GetId())
}

// grantIDEntitlementPrefix returns the entitlement-ID portion of a
// grant ID, i.e. everything before the final principalType:principalID
// pair.
func grantIDEntitlementPrefix(grantID string) string {
	lastColon := strings.LastIndex(grantID, ":")
	head := grantID[:lastColon]
	typeColon := strings.LastIndex(head, ":")
	return head[:typeColon]
}

func lastField(s string) string {
	return s[strings.LastIndex(s, ":")+1:]
}
