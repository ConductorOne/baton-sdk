package entitlement

import (
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
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
			got := NewEntitlementID(tt.args.resource, tt.args.permission)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestEntitlementIDCodec(t *testing.T) {
	id := EncodeEntitlementID("foo", "1234", EntitlementKindSDK, "member")
	require.Equal(t, "foo:1234:member", id)

	parts, err := DecodeEntitlementID(id)
	require.NoError(t, err)
	require.Equal(t, EntitlementIDParts{
		ResourceTypeID: "foo",
		ResourceID:     "1234",
		Kind:           EntitlementKindSDK,
		Name:           "member",
	}, parts)
}

func TestEntitlementIDCodecEscapesColonsAndBackslashes(t *testing.T) {
	id := EncodeEntitlementID(`foo:type`, `12\34`, EntitlementKindSDK, `mem:ber\name`)
	require.Equal(t, `foo\:type:12\\34:mem\:ber\\name`, id)

	parts, err := DecodeEntitlementID(id)
	require.NoError(t, err)
	require.Equal(t, EntitlementIDParts{
		ResourceTypeID: `foo:type`,
		ResourceID:     `12\34`,
		Kind:           EntitlementKindSDK,
		Name:           `mem:ber\name`,
	}, parts)
}

func TestEntitlementIDCodecCustomDoesNotCollideWithSDK(t *testing.T) {
	sdk := EncodeEntitlementID("rt", "rid", EntitlementKindSDK, "admin")
	custom := EncodeEntitlementID("rt", "rid", EntitlementKindCustom, "admin")
	require.Equal(t, "rt:rid:admin", sdk)
	require.Equal(t, "rt:rid:custom:admin", custom)
	require.NotEqual(t, sdk, custom)

	sdkParts, err := DecodeEntitlementID(sdk)
	require.NoError(t, err)
	require.Equal(t, EntitlementKindSDK, sdkParts.Kind)

	customParts, err := DecodeEntitlementID(custom)
	require.NoError(t, err)
	require.Equal(t, EntitlementKindCustom, customParts.Kind)
}

func TestDeriveEntitlementIDPartsPreservesLegacySDKColonName(t *testing.T) {
	parts := DeriveEntitlementIDParts("rt", "rid", "rt:rid:custom:admin")
	require.Equal(t, EntitlementIDParts{
		ResourceTypeID: "rt",
		ResourceID:     "rid",
		Kind:           EntitlementKindCustom,
		Name:           "admin",
	}, parts)
}

func TestDeriveLegacyEntitlementIDPartsPreservesLegacySDKColonName(t *testing.T) {
	parts := DeriveLegacyEntitlementIDParts("rt", "rid", "rt:rid:custom:admin")
	require.Equal(t, EntitlementIDParts{
		ResourceTypeID: "rt",
		ResourceID:     "rid",
		Kind:           EntitlementKindSDK,
		Name:           "custom:admin",
	}, parts)
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

func TestNewOwnershipEntitlement(t *testing.T) {
	rt := resource.NewResourceType("Group", []v2.ResourceType_Trait{v2.ResourceType_TRAIT_GROUP})
	ur, err := resource.NewResource("test-group", rt, 1234)
	require.NoError(t, err)
	require.NotNil(t, ur)

	en := NewOwnershipEntitlement(ur, "admin", WithGrantableTo(rt))
	require.NotNil(t, en)
	require.Equal(t, v2.Entitlement_PURPOSE_VALUE_OWNERSHIP, en.GetPurpose())
	require.Equal(t, ur, en.GetResource())
	require.Equal(t, "admin", en.GetDisplayName())
	require.Equal(t, "admin", en.GetSlug())
	require.Len(t, en.GetGrantableTo(), 1)
	require.Equal(t, rt, en.GetGrantableTo()[0])
}

func TestWithExclusionGroup(t *testing.T) {
	rt := resource.NewResourceType("Role", []v2.ResourceType_Trait{v2.ResourceType_TRAIT_ROLE})
	ur, err := resource.NewResource("test-role", rt, 1)
	require.NoError(t, err)

	en := NewPermissionEntitlement(ur, "standard", WithExclusionGroup("access-tier"))
	require.NotNil(t, en)

	annos := annotations.Annotations(en.GetAnnotations())
	eg := &v2.EntitlementExclusionGroup{}
	found, err := annos.Pick(eg)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "access-tier", eg.GetExclusionGroupId())
	require.Equal(t, uint32(0), eg.GetOrder())
	require.False(t, eg.GetIsDefault())
}

func TestWithExclusionGroupOrder(t *testing.T) {
	rt := resource.NewResourceType("Role", []v2.ResourceType_Trait{v2.ResourceType_TRAIT_ROLE})
	ur, err := resource.NewResource("test-role", rt, 1)
	require.NoError(t, err)

	en := NewPermissionEntitlement(ur, "admin", WithExclusionGroupOrder("access-tier", 30))
	require.NotNil(t, en)

	annos := annotations.Annotations(en.GetAnnotations())
	eg := &v2.EntitlementExclusionGroup{}
	found, err := annos.Pick(eg)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "access-tier", eg.GetExclusionGroupId())
	require.Equal(t, uint32(30), eg.GetOrder())
	require.False(t, eg.GetIsDefault())
}

func TestWithExclusionGroupDefault(t *testing.T) {
	rt := resource.NewResourceType("Role", []v2.ResourceType_Trait{v2.ResourceType_TRAIT_ROLE})
	ur, err := resource.NewResource("test-role", rt, 1)
	require.NoError(t, err)

	en := NewPermissionEntitlement(ur, "read-only", WithExclusionGroupDefault("access-tier", 10))
	require.NotNil(t, en)

	annos := annotations.Annotations(en.GetAnnotations())
	eg := &v2.EntitlementExclusionGroup{}
	found, err := annos.Pick(eg)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "access-tier", eg.GetExclusionGroupId())
	require.Equal(t, uint32(10), eg.GetOrder())
	require.True(t, eg.GetIsDefault())
}
