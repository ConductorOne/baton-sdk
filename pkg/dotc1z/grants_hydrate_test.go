package dotc1z

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

func TestNormalizeGrantForCompare_NilGrant(t *testing.T) {
	require.NotPanics(t, func() {
		NormalizeGrantForCompare(nil)
	})
}

func TestNormalizeGrantForCompare_FullGrantBecomesStub(t *testing.T) {
	ent := v2.Entitlement_builder{
		Id: "ent-1",
		Resource: v2.Resource_builder{
			Id:          v2.ResourceId_builder{ResourceType: "group", Resource: "g-1"}.Build(),
			DisplayName: "Group One",
		}.Build(),
		DisplayName: "Read access",
		Description: "Lets the principal read",
		Slug:        "read",
	}.Build()
	princ := v2.Resource_builder{
		Id:          v2.ResourceId_builder{ResourceType: "user", Resource: "u-1"}.Build(),
		DisplayName: "User One",
	}.Build()
	grant := v2.Grant_builder{
		Id:          "grant-1",
		Entitlement: ent,
		Principal:   princ,
	}.Build()

	NormalizeGrantForCompare(grant)

	require.Equal(t, "ent-1", grant.GetEntitlement().GetId())
	require.Equal(t, "group", grant.GetEntitlement().GetResource().GetId().GetResourceType())
	require.Equal(t, "g-1", grant.GetEntitlement().GetResource().GetId().GetResource())
	require.Empty(t, grant.GetEntitlement().GetDisplayName())
	require.Empty(t, grant.GetEntitlement().GetDescription())
	require.Empty(t, grant.GetEntitlement().GetSlug())
	require.Empty(t, grant.GetEntitlement().GetResource().GetDisplayName())

	require.Equal(t, "user", grant.GetPrincipal().GetId().GetResourceType())
	require.Equal(t, "u-1", grant.GetPrincipal().GetId().GetResource())
	require.Empty(t, grant.GetPrincipal().GetDisplayName())
}

func TestNormalizeGrantForCompare_AlreadyStubIsIdempotent(t *testing.T) {
	stub := v2.Grant_builder{
		Id: "grant-1",
		Entitlement: v2.Entitlement_builder{
			Id: "ent-1",
			Resource: v2.Resource_builder{
				Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g-1"}.Build(),
			}.Build(),
		}.Build(),
		Principal: v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u-1"}.Build(),
		}.Build(),
	}.Build()

	before := proto.Clone(stub).(*v2.Grant)
	NormalizeGrantForCompare(stub)

	require.True(t, proto.Equal(before, stub), "normalize on already-stub grant should leave it equal to the original")
}

func TestNormalizeGrantForCompare_NilEntitlementLeftAlone(t *testing.T) {
	grant := v2.Grant_builder{
		Id: "grant-1",
		Principal: v2.Resource_builder{
			Id:          v2.ResourceId_builder{ResourceType: "user", Resource: "u-1"}.Build(),
			DisplayName: "User One",
		}.Build(),
	}.Build()

	NormalizeGrantForCompare(grant)

	require.Nil(t, grant.GetEntitlement())
	require.Equal(t, "u-1", grant.GetPrincipal().GetId().GetResource())
	require.Empty(t, grant.GetPrincipal().GetDisplayName())
}

func TestNormalizeGrantForCompare_NilPrincipalLeftAlone(t *testing.T) {
	grant := v2.Grant_builder{
		Id: "grant-1",
		Entitlement: v2.Entitlement_builder{
			Id: "ent-1",
			Resource: v2.Resource_builder{
				Id:          v2.ResourceId_builder{ResourceType: "group", Resource: "g-1"}.Build(),
				DisplayName: "Group One",
			}.Build(),
			DisplayName: "Read access",
		}.Build(),
	}.Build()

	NormalizeGrantForCompare(grant)

	require.Nil(t, grant.GetPrincipal())
	require.Equal(t, "ent-1", grant.GetEntitlement().GetId())
	require.Empty(t, grant.GetEntitlement().GetDisplayName())
	require.Equal(t, "g-1", grant.GetEntitlement().GetResource().GetId().GetResource())
	require.Empty(t, grant.GetEntitlement().GetResource().GetDisplayName())
}

func TestNormalizeGrantForCompare_SlimVsFullCompareEqual(t *testing.T) {
	full := v2.Grant_builder{
		Id: "grant-1",
		Entitlement: v2.Entitlement_builder{
			Id: "ent-1",
			Resource: v2.Resource_builder{
				Id:          v2.ResourceId_builder{ResourceType: "group", Resource: "g-1"}.Build(),
				DisplayName: "Group One",
			}.Build(),
			DisplayName: "Read access",
			Slug:        "read",
		}.Build(),
		Principal: v2.Resource_builder{
			Id:          v2.ResourceId_builder{ResourceType: "user", Resource: "u-1"}.Build(),
			DisplayName: "User One",
		}.Build(),
	}.Build()
	slim := v2.Grant_builder{
		Id: "grant-1",
		Entitlement: v2.Entitlement_builder{
			Id: "ent-1",
			Resource: v2.Resource_builder{
				Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g-1"}.Build(),
			}.Build(),
		}.Build(),
		Principal: v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u-1"}.Build(),
		}.Build(),
	}.Build()

	NormalizeGrantForCompare(full)
	NormalizeGrantForCompare(slim)

	require.True(t, proto.Equal(full, slim), "slim and full of the same logical grant should compare equal after normalization")
}

func TestNormalizeGrantForCompare_DifferentEntitlementIDStillUnequal(t *testing.T) {
	// Identity changes must survive normalization. If they didn't, a
	// diff would silently treat reassigned entitlements as no-ops.
	a := v2.Grant_builder{
		Id: "grant-1",
		Entitlement: v2.Entitlement_builder{
			Id: "ent-1",
			Resource: v2.Resource_builder{
				Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g-1"}.Build(),
			}.Build(),
		}.Build(),
		Principal: v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u-1"}.Build(),
		}.Build(),
	}.Build()
	b := proto.Clone(a).(*v2.Grant)
	b.GetEntitlement().SetId("ent-2")

	NormalizeGrantForCompare(a)
	NormalizeGrantForCompare(b)

	require.False(t, proto.Equal(a, b), "grants differing in Entitlement.Id should still compare unequal after normalization")
}
