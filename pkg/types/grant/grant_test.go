package grant

import (
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	entitlement "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	resource "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/stretchr/testify/require"
)

func TestNewGrant(t *testing.T) {
	rt := resource.NewResourceType("Group", []v2.ResourceType_Trait{v2.ResourceType_TRAIT_GROUP})
	ur, err := resource.NewResource("test-group", rt, 1234)
	require.NoError(t, err)
	require.NotNil(t, ur)

	en := entitlement.NewPermissionEntitlement(ur, "admin", entitlement.WithGrantableTo(rt))
	require.NotNil(t, en)

	grant := NewGrant(ur, en.GetSlug(), v2.ResourceId_builder{
		ResourceType: "user",
		Resource:     "567",
	}.Build())
	require.NotNil(t, grant)
	require.NotNil(t, grant.GetEntitlement())
	require.Equal(t, "group:1234:admin", grant.GetEntitlement().GetId())
	require.NotNil(t, grant.GetPrincipal())
	require.Equal(t, "user", grant.GetPrincipal().GetId().GetResourceType())
	require.Equal(t, "567", grant.GetPrincipal().GetId().GetResource())
	require.Equal(t, "group:1234:admin:user:567", grant.GetId())
}

func TestGrantIDCodec(t *testing.T) {
	entParts := entitlement.EntitlementIDParts{
		ResourceTypeID: "group",
		ResourceID:     "1234",
		Kind:           entitlement.EntitlementKindSDK,
		Name:           "admin",
	}
	id := EncodeGrantID(entParts, "user", "567")
	require.Equal(t, "group:1234:admin:user:567", id)

	parts, err := DecodeGrantID(id)
	require.NoError(t, err)
	require.Equal(t, GrantIDParts{
		Entitlement:     entParts,
		PrincipalTypeID: "user",
		PrincipalID:     "567",
	}, parts)
}

func TestGrantIDCodecEscapesAndTagsCustomEntitlement(t *testing.T) {
	entParts := entitlement.EntitlementIDParts{
		ResourceTypeID: `group:type`,
		ResourceID:     `12\34`,
		Kind:           entitlement.EntitlementKindCustom,
		Name:           `external:id`,
	}
	id := EncodeGrantID(entParts, `user:type`, `56\7`)
	require.Equal(t, `group\:type:12\\34:custom:external\:id:user\:type:56\\7`, id)

	parts, err := DecodeGrantID(id)
	require.NoError(t, err)
	require.Equal(t, GrantIDParts{
		Entitlement:     entParts,
		PrincipalTypeID: `user:type`,
		PrincipalID:     `56\7`,
	}, parts)
}

func TestGrantIDCodecFoldedLegacyTuplesAreDistinct(t *testing.T) {
	a := EncodeGrantID(entitlement.EntitlementIDParts{
		ResourceTypeID: "rt",
		ResourceID:     "rid:name",
		Kind:           entitlement.EntitlementKindSDK,
		Name:           "pRT",
	}, "pID", "x")
	b := EncodeGrantID(entitlement.EntitlementIDParts{
		ResourceTypeID: "rt",
		ResourceID:     "rid",
		Kind:           entitlement.EntitlementKindSDK,
		Name:           "name:pRT",
	}, "pID", "x")

	require.Equal(t, `rt:rid\:name:pRT:pID:x`, a)
	require.Equal(t, `rt:rid:name\:pRT:pID:x`, b)
	require.NotEqual(t, a, b)
}
