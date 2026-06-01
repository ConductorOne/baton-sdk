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
