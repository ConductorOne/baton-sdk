package grant

import (
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
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

func TestNewGrantIDFromRecord(t *testing.T) {
	ent := &v3.EntitlementRef{ResourceTypeId: "group", ResourceId: "g1", EntitlementId: "group:g1:member"}
	princ := &v3.PrincipalRef{ResourceTypeId: "user", ResourceId: "u1"}

	t.Run("external id wins", func(t *testing.T) {
		r := &v3.GrantRecord{ExternalId: "custom", Entitlement: ent, Principal: princ}
		require.Equal(t, "custom", NewGrantIDFromRecord(r))
	})
	t.Run("reconstructed", func(t *testing.T) {
		r := &v3.GrantRecord{Entitlement: ent, Principal: princ}
		require.Equal(t, "group:g1:member:user:u1", NewGrantIDFromRecord(r))
	})
	t.Run("missing refs", func(t *testing.T) {
		require.Empty(t, NewGrantIDFromRecord(nil))
		require.Empty(t, NewGrantIDFromRecord(&v3.GrantRecord{}))
		require.Empty(t, NewGrantIDFromRecord(&v3.GrantRecord{Entitlement: ent}))
		require.Empty(t, NewGrantIDFromRecord(&v3.GrantRecord{Principal: princ}))
		require.Empty(t, NewGrantIDFromRecord(&v3.GrantRecord{Entitlement: ent, Principal: &v3.PrincipalRef{ResourceTypeId: "user"}}))
	})
}
