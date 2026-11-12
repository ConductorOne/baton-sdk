package entitlement

import (
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/duty"
	resource "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"
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

func dutyAnnotations(t *testing.T, en *v2.Entitlement) []*v2.DutyMappingsAnnotation {
	t.Helper()
	var out []*v2.DutyMappingsAnnotation
	for _, a := range en.GetAnnotations() {
		if a.MessageIs(&v2.DutyMappingsAnnotation{}) {
			ann := &v2.DutyMappingsAnnotation{}
			require.NoError(t, a.UnmarshalTo(ann))
			out = append(out, ann)
		}
	}
	return out
}

func dutyIDs(ann *v2.DutyMappingsAnnotation) []string {
	var ids []string
	for _, m := range ann.GetMappings() {
		ids = append(ids, m.GetDutyId())
	}
	return ids
}

func TestWithDuties(t *testing.T) {
	rt := resource.NewResourceType("Role", []v2.ResourceType_Trait{v2.ResourceType_TRAIT_ROLE})
	r, err := resource.NewResource("payments", rt, "payments")
	require.NoError(t, err)

	t.Run("repeated calls share one annotation", func(t *testing.T) {
		en := NewPermissionEntitlement(r, "release",
			WithExclusionGroup("roles"),
			WithDuties(duty.Exact(duty.PaymentRelease)),
			WithDuties(duty.Conditional(duty.PaymentAuthorize, "only below the approval threshold")),
		)
		annos := annotations.Annotations(en.GetAnnotations())
		require.True(t, annos.Contains(&v2.EntitlementExclusionGroup{}))
		anns := dutyAnnotations(t, en)
		require.Len(t, anns, 1)
		require.Equal(t, duty.CatalogVersion, anns[0].GetCatalogVersion())
		require.Equal(t, []string{string(duty.PaymentRelease), string(duty.PaymentAuthorize)}, dutyIDs(anns[0]))
		require.NoError(t, duty.Validate(anns[0]))
	})

	t.Run("duplicates keep first position and last mapping", func(t *testing.T) {
		en := NewPermissionEntitlement(r, "release",
			WithDuties(duty.Exact(duty.PaymentRelease), duty.Inclusive(duty.PaymentPrepare)),
			WithDuties(duty.Inclusive(duty.PaymentRelease).WithDescription("admin"), duty.Exact("custom.duty")),
			WithDuties(duty.Exact(duty.PaymentPrepare), duty.Inclusive(duty.PaymentPrepare)),
		)
		anns := dutyAnnotations(t, en)
		require.Len(t, anns, 1)
		require.Equal(t, []string{string(duty.PaymentRelease), string(duty.PaymentPrepare), "custom.duty"}, dutyIDs(anns[0]))
		m := anns[0].GetMappings()
		require.Equal(t, v2.DutyMappingPrecision_DUTY_MAPPING_PRECISION_INCLUSIVE, m[0].GetPrecision())
		require.Equal(t, "admin", m[0].GetDescription())
		require.Equal(t, v2.DutyMappingPrecision_DUTY_MAPPING_PRECISION_INCLUSIVE, m[1].GetPrecision())
		require.NoError(t, duty.Validate(anns[0]))
	})

	t.Run("no mappings attach nothing", func(t *testing.T) {
		en := NewPermissionEntitlement(r, "release", WithDuties())
		require.Empty(t, en.GetAnnotations())
		en = NewPermissionEntitlement(r, "release", WithDuties(duty.Exact(duty.PaymentRelease)), WithDuties())
		require.Len(t, dutyAnnotations(t, en), 1)
	})

	t.Run("undecodable annotation is kept", func(t *testing.T) {
		bad := &anypb.Any{TypeUrl: "type.googleapis.com/c1.connector.v2.DutyMappingsAnnotation", Value: []byte{0xff, 0xff}}

		en := NewPermissionEntitlement(r, "release")
		en.SetAnnotations([]*anypb.Any{bad})
		WithDuties(duty.Exact(duty.PaymentPrepare))(en)
		annos := en.GetAnnotations()
		require.Len(t, annos, 2)
		require.Same(t, bad, annos[0], "the undecodable annotation is left untouched")
		ann := &v2.DutyMappingsAnnotation{}
		require.NoError(t, annos[1].UnmarshalTo(ann))
		require.Equal(t, []string{string(duty.PaymentPrepare)}, dutyIDs(ann))

		en = NewPermissionEntitlement(r, "release", WithDuties(duty.Exact(duty.PaymentRelease)))
		en.SetAnnotations(append([]*anypb.Any{bad}, en.GetAnnotations()...))
		WithDuties(duty.Exact(duty.PaymentPrepare))(en)
		annos = en.GetAnnotations()
		require.Len(t, annos, 2)
		require.Same(t, bad, annos[0])
		require.NoError(t, annos[1].UnmarshalTo(ann))
		require.Equal(t, []string{string(duty.PaymentRelease), string(duty.PaymentPrepare)}, dutyIDs(ann), "merges into the first decodable annotation")
	})
}
