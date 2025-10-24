package entitlement

import (
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	resource "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/stretchr/testify/require"
)

func TestNewAssignmentEntitlement(t *testing.T) {
	rt := resource.NewResourceType("Group", []v2.ResourceType_Trait{v2.ResourceType_TRAIT_GROUP})
	ur, err := resource.NewResource("test-group", rt, 1234)
	require.NoError(t, err)
	require.NotNil(t, ur)

	en := NewAssignmentEntitlement(ur, "member", WithGrantableTo(rt))
	require.NotNil(t, en)
	require.Equal(t, v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT, en.GetPurpose())
	require.Equal(t, ur, en.GetResource())
	require.Equal(t, "member", en.GetDisplayName())
	require.Equal(t, "member", en.GetSlug())
	require.Len(t, en.GetGrantableTo(), 1)
	require.Equal(t, rt, en.GetGrantableTo()[0])
}

func TestNewEntitlementID(t *testing.T) {
	type args struct {
		resource   *v2.Resource
		permission string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			"ID for role member",
			args{
				resource: v2.Resource_builder{
					Id: v2.ResourceId_builder{
						ResourceType: "foo",
						Resource:     "1234",
					}.Build(),
				}.Build(),
				permission: "member",
			},
			"foo:1234:member",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewEntitlementID(tt.args.resource, tt.args.permission); got != tt.want {
				t.Errorf("NewEntitlementID() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewPermissionEntitlement(t *testing.T) {
	rt := resource.NewResourceType("Group", []v2.ResourceType_Trait{v2.ResourceType_TRAIT_GROUP})
	ur, err := resource.NewResource("test-group", rt, 1234)
	require.NoError(t, err)
	require.NotNil(t, ur)

	en := NewPermissionEntitlement(ur, "admin", WithGrantableTo(rt))
	require.NotNil(t, en)
	require.Equal(t, v2.Entitlement_PURPOSE_VALUE_PERMISSION, en.GetPurpose())
	require.Equal(t, ur, en.GetResource())
	require.Equal(t, "admin", en.GetDisplayName())
	require.Equal(t, "admin", en.GetSlug())
	require.Len(t, en.GetGrantableTo(), 1)
	require.Equal(t, rt, en.GetGrantableTo()[0])
}
