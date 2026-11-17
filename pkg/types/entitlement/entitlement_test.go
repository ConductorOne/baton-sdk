package entitlement

import (
	"testing"
	"unicode/utf8"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/duty"
	"github.com/conductorone/baton-sdk/pkg/duty/dutytest"
	resource "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
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

// recorder captures failures instead of failing the enclosing test.
type recorder struct {
	testing.TB
	failed bool
}

func (r *recorder) Helper() {}

func (r *recorder) Errorf(string, ...any) { r.failed = true }

func TestWithDuties(t *testing.T) {
	rt := resource.NewResourceType("Role", []v2.ResourceType_Trait{v2.ResourceType_TRAIT_ROLE})
	r, err := resource.NewResource("payments", rt, "payments")
	require.NoError(t, err)

	t.Run("no mappings attach nothing", func(t *testing.T) {
		en := NewPermissionEntitlement(r, "release", WithDuties())
		require.Empty(t, en.GetAnnotations())
	})

	t.Run("only nil mappings attach nothing", func(t *testing.T) {
		en := NewPermissionEntitlement(r, "release", WithDuties(nil, nil))
		require.Empty(t, en.GetAnnotations())
	})

	t.Run("one call attaches one annotation in order", func(t *testing.T) {
		described := duty.Exact("payment.authorize")
		described.SetDescription("Finance Administrator role")
		mappings := []*v2.DutyMapping{
			duty.Inclusive("payment.release"),
			described,
			duty.Inclusive("vendor.activate"),
		}
		en := NewPermissionEntitlement(r, "release", WithExclusionGroup("roles"), WithDuties(mappings...))
		annos := annotations.Annotations(en.GetAnnotations())
		require.True(t, annos.Contains(&v2.EntitlementExclusionGroup{}))
		anns := dutyAnnotations(t, en)
		require.Len(t, anns, 1)
		require.Len(t, anns[0].GetMappings(), len(mappings))
		for i, m := range mappings {
			require.True(t, proto.Equal(m, anns[0].GetMappings()[i]), "mapping %d", i)
		}
		dutytest.AssertEntitlement(t, en)
	})

	t.Run("nil mappings are skipped", func(t *testing.T) {
		en := NewPermissionEntitlement(r, "release", WithDuties(nil, duty.Exact("payment.release"), nil, duty.Inclusive("payment.prepare")))
		anns := dutyAnnotations(t, en)
		require.Len(t, anns, 1)
		got := anns[0].GetMappings()
		require.Len(t, got, 2)
		require.Equal(t, "payment.release", got[0].GetDutyId())
		require.Equal(t, "payment.prepare", got[1].GetDutyId())
		dutytest.AssertEntitlement(t, en)
	})

	t.Run("does not alias the caller's mappings", func(t *testing.T) {
		m := duty.Exact("payment.release")
		opt := WithDuties(m)
		m.SetDutyId("payment.prepare")
		anns := dutyAnnotations(t, NewPermissionEntitlement(r, "release", opt))
		require.Len(t, anns, 1)
		require.Equal(t, "payment.release", anns[0].GetMappings()[0].GetDutyId())
	})

	t.Run("invalid UTF-8 in a description does not panic", func(t *testing.T) {
		m := duty.Exact("payment.release")
		m.SetDescription("x\xff")
		before := proto.Clone(m)
		var en *v2.Entitlement
		require.NotPanics(t, func() {
			en = NewPermissionEntitlement(r, "release", WithDuties(m))
		})
		require.True(t, proto.Equal(before, m), "the caller's message is unchanged")
		require.Equal(t, "x\xff", m.GetDescription())
		anns := dutyAnnotations(t, en)
		require.Len(t, anns, 1)
		got := anns[0].GetMappings()[0]
		require.True(t, utf8.ValidString(got.GetDescription()))
		require.Equal(t, "x\uFFFD", got.GetDescription())
		require.Equal(t, "payment.release", got.GetDutyId())
		dutytest.AssertEntitlement(t, en)
	})

	t.Run("invalid UTF-8 in a duty ID does not panic and fails Validate", func(t *testing.T) {
		m := duty.Inclusive("payment.rel\xffease")
		var en *v2.Entitlement
		require.NotPanics(t, func() {
			en = NewPermissionEntitlement(r, "release", WithDuties(m))
		})
		require.Equal(t, "payment.rel\xffease", m.GetDutyId(), "the caller's message is unchanged")
		anns := dutyAnnotations(t, en)
		require.Len(t, anns, 1)
		require.Equal(t, "payment.rel\uFFFDease", anns[0].GetMappings()[0].GetDutyId())
		require.Error(t, duty.Validate(anns[0]))
		rec := &recorder{TB: t}
		dutytest.AssertEntitlement(rec, en)
		require.True(t, rec.failed)
	})

	t.Run("two calls attach two annotations", func(t *testing.T) {
		en := NewPermissionEntitlement(r, "release",
			WithDuties(duty.Exact("payment.release")),
			WithDuties(duty.Exact("payment.prepare")),
		)
		require.Len(t, dutyAnnotations(t, en), 2)
		rec := &recorder{TB: t}
		dutytest.AssertEntitlement(rec, en)
		require.True(t, rec.failed, "AssertEntitlement rejects a second duty annotation")
	})
}
