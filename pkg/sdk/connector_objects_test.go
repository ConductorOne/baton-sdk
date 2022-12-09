package sdk

import (
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	eopt "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	ropt "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/stretchr/testify/require"
)

func TestNewAppResource(t *testing.T) {
	profile := map[string]interface{}{
		"app_name": "Test",
	}
	rt := NewResourceType("App", []v2.ResourceType_Trait{v2.ResourceType_TRAIT_APP})
	ar, err := NewAppResource("test app", rt, nil, 1234, "https://example.com", profile, ropt.WithAnnotation(&v2.V1Identifier{Id: "v1"}))
	require.NoError(t, err)
	require.NotNil(t, ar)
	require.Equal(t, rt.Id, ar.Id.ResourceType)
	require.Equal(t, "1234", ar.Id.Resource)

	require.Len(t, ar.Annotations, 2)
	v1ID := &v2.V1Identifier{}
	annos := annotations.Annotations(ar.Annotations)
	ok, err := annos.Pick(v1ID)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "v1", v1ID.Id)

	roleTrait, err := GetRoleTrait(ar)
	require.Error(t, err)
	require.Nil(t, roleTrait)
	appTrait, err := GetAppTrait(ar)
	require.NoError(t, err)
	require.NotNil(t, appTrait)
	require.Equal(t, "https://example.com", appTrait.HelpUrl)
	require.NotNil(t, appTrait.Profile)
	fName, foundProfileData := GetProfileStringValue(appTrait.Profile, "app_name")
	require.True(t, foundProfileData)
	require.Equal(t, "Test", fName)
	mName, foundProfileData := GetProfileStringValue(appTrait.Profile, "first_name")
	require.False(t, foundProfileData)
	require.Equal(t, "", mName)
}

func TestNewAssignmentEntitlement(t *testing.T) {
	rt := NewResourceType("Group", []v2.ResourceType_Trait{v2.ResourceType_TRAIT_GROUP})
	ur, err := NewResource("test-group", rt, nil, 1234)
	require.NoError(t, err)
	require.NotNil(t, ur)

	en := NewEntitlement(ur, "member", AssignmentEntitlement, eopt.WithGrantableTo(rt))
	require.NotNil(t, en)
	require.Equal(t, v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT, en.Purpose)
	require.Equal(t, ur, en.Resource)
	require.Equal(t, "member", en.DisplayName)
	require.Equal(t, "member", en.Slug)
	require.Len(t, en.GrantableTo, 1)
	require.Equal(t, rt, en.GrantableTo[0])
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
				resource: &v2.Resource{
					Id: &v2.ResourceId{
						ResourceType: "foo",
						Resource:     "1234",
					},
				},
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

func TestNewGrant(t *testing.T) {
	rt := NewResourceType("Group", []v2.ResourceType_Trait{v2.ResourceType_TRAIT_GROUP})
	ur, err := NewResource("test-group", rt, nil, 1234)
	require.NoError(t, err)
	require.NotNil(t, ur)

	en := NewEntitlement(ur, "admin", PermissionEntitlement, eopt.WithGrantableTo(rt))
	require.NotNil(t, en)

	grant := NewGrant(ur, en.Slug, &v2.ResourceId{
		ResourceType: "user",
		Resource:     "567",
	})
	require.NotNil(t, grant)
	require.NotNil(t, grant.Entitlement)
	require.Equal(t, "group:1234:admin", grant.Entitlement.Id)
	require.NotNil(t, grant.Principal)
	require.Equal(t, "user", grant.Principal.Id.ResourceType)
	require.Equal(t, "567", grant.Principal.Id.Resource)
	require.Equal(t, "group:1234:admin:user:567", grant.Id)
}

func TestNewGroupResource(t *testing.T) {
	profile := map[string]interface{}{
		"group_name": "Test",
	}
	rt := NewResourceType("Group", []v2.ResourceType_Trait{v2.ResourceType_TRAIT_GROUP})
	gr, err := NewGroupResource("test group", rt, nil, 1234, profile, ropt.WithAnnotation(&v2.V1Identifier{Id: "v1"}))
	require.NoError(t, err)
	require.NotNil(t, gr)
	require.Equal(t, rt.Id, gr.Id.ResourceType)
	require.Equal(t, "1234", gr.Id.Resource)

	require.Len(t, gr.Annotations, 2)
	v1ID := &v2.V1Identifier{}
	annos := annotations.Annotations(gr.Annotations)
	ok, err := annos.Pick(v1ID)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "v1", v1ID.Id)

	roleTrait, err := GetRoleTrait(gr)
	require.Error(t, err)
	require.Nil(t, roleTrait)
	groupTrait, err := GetGroupTrait(gr)
	require.NoError(t, err)
	require.NotNil(t, groupTrait)
	require.NotNil(t, groupTrait.Profile)
	fName, foundProfileData := GetProfileStringValue(groupTrait.Profile, "group_name")
	require.True(t, foundProfileData)
	require.Equal(t, "Test", fName)
	mName, foundProfileData := GetProfileStringValue(groupTrait.Profile, "first_name")
	require.False(t, foundProfileData)
	require.Equal(t, "", mName)
}

func TestNewPermissionEntitlement(t *testing.T) {
	rt := NewResourceType("Group", []v2.ResourceType_Trait{v2.ResourceType_TRAIT_GROUP})
	ur, err := NewResource("test-group", rt, nil, 1234)
	require.NoError(t, err)
	require.NotNil(t, ur)

	en := NewEntitlement(ur, "admin", PermissionEntitlement, eopt.WithGrantableTo(rt))
	require.NotNil(t, en)
	require.Equal(t, v2.Entitlement_PURPOSE_VALUE_PERMISSION, en.Purpose)
	require.Equal(t, ur, en.Resource)
	require.Equal(t, "admin", en.DisplayName)
	require.Equal(t, "admin", en.Slug)
	require.Len(t, en.GrantableTo, 1)
	require.Equal(t, rt, en.GrantableTo[0])
}

func TestNewResource(t *testing.T) {
	parentID := &v2.ResourceId{
		ResourceType: "foo",
		Resource:     "567",
	}
	rt := NewResourceType("Role", []v2.ResourceType_Trait{v2.ResourceType_TRAIT_ROLE})
	rr, err := NewResource("test resource", rt, parentID, "1234", ropt.WithAnnotation(&v2.V1Identifier{Id: "v1"}))
	require.NoError(t, err)
	require.NotNil(t, rr)
	require.Equal(t, rt.Id, rr.Id.ResourceType)
	require.Equal(t, "1234", rr.Id.Resource)
	require.Equal(t, parentID, rr.ParentResourceId)

	require.Len(t, rr.Annotations, 1)
	v1ID := &v2.V1Identifier{}
	annos := annotations.Annotations(rr.Annotations)
	ok, err := annos.Pick(v1ID)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "v1", v1ID.Id)

	groupTrait, err := GetGroupTrait(rr)
	require.Error(t, err)
	require.Nil(t, groupTrait)

	userTrait, err := GetUserTrait(rr)
	require.Error(t, err)
	require.Nil(t, userTrait)

	roleTrait, err := GetRoleTrait(rr)
	require.Error(t, err)
	require.Nil(t, roleTrait)

	appTrait, err := GetAppTrait(rr)
	require.Error(t, err)
	require.Nil(t, appTrait)
}

func TestNewResourceID(t *testing.T) {
	rt := NewResourceType("Foo", []v2.ResourceType_Trait{v2.ResourceType_TRAIT_ROLE})
	id, err := NewResourceID(rt, 1234)
	require.NoError(t, err)
	require.Equal(t, "foo", id.ResourceType)
	require.Equal(t, "1234", id.Resource)
}

func TestNewRoleResource(t *testing.T) {
	profile := map[string]interface{}{
		"role_name": "Test",
	}
	rt := NewResourceType("Role", []v2.ResourceType_Trait{v2.ResourceType_TRAIT_ROLE})
	rr, err := NewRoleResource("test role", rt, nil, "1234", profile, ropt.WithAnnotation(&v2.V1Identifier{Id: "v1"}))
	require.NoError(t, err)
	require.NotNil(t, rr)
	require.Equal(t, rt.Id, rr.Id.ResourceType)
	require.Equal(t, "1234", rr.Id.Resource)

	require.Len(t, rr.Annotations, 2)
	v1ID := &v2.V1Identifier{}
	annos := annotations.Annotations(rr.Annotations)
	ok, err := annos.Pick(v1ID)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "v1", v1ID.Id)

	groupTrait, err := GetGroupTrait(rr)
	require.Error(t, err)
	require.Nil(t, groupTrait)

	roleTrait, err := GetRoleTrait(rr)
	require.NoError(t, err)
	require.NotNil(t, roleTrait)
	require.NotNil(t, roleTrait.Profile)
	fName, foundProfileData := GetProfileStringValue(roleTrait.Profile, "role_name")
	require.True(t, foundProfileData)
	require.Equal(t, "Test", fName)
	mName, foundProfileData := GetProfileStringValue(roleTrait.Profile, "first_name")
	require.False(t, foundProfileData)
	require.Equal(t, "", mName)
}

func TestNewUserResource(t *testing.T) {
	userEmail := "user@example.com"
	profile := map[string]interface{}{
		"first_name": "Test",
		"last_name":  "User",
	}
	rt := NewResourceType("User", []v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER})
	ur, err := NewUserResource("test user", rt, nil, 1234, userEmail, profile, ropt.WithAnnotation(&v2.V1Identifier{Id: "v1"}))
	require.NoError(t, err)
	require.NotNil(t, ur)
	require.Equal(t, rt.Id, ur.Id.ResourceType)
	require.Equal(t, "1234", ur.Id.Resource)

	require.Len(t, ur.Annotations, 2)
	v1ID := &v2.V1Identifier{}
	annos := annotations.Annotations(ur.Annotations)
	ok, err := annos.Pick(v1ID)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "v1", v1ID.Id)

	roleTrait, err := GetRoleTrait(ur)
	require.Error(t, err)
	require.Nil(t, roleTrait)

	ut, err := GetUserTrait(ur)
	require.NoError(t, err)
	require.NotNil(t, ut)
	require.Len(t, ut.Emails, 1)
	require.Equal(t, userEmail, ut.Emails[0].Address)
	require.NotNil(t, ut.Profile)
	fName, foundProfileData := GetProfileStringValue(ut.Profile, "first_name")
	require.True(t, foundProfileData)
	require.Equal(t, "Test", fName)
	mName, foundProfileData := GetProfileStringValue(ut.Profile, "middle_name")
	require.False(t, foundProfileData)
	require.Equal(t, "", mName)
}

func Test_convertIDToString(t *testing.T) {
	type args struct {
		id interface{}
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			"string ID",
			args{
				"myID",
			},
			"myID",
			false,
		},
		{
			"int ID",
			args{
				1234,
			},
			"1234",
			false,
		},
		{
			"int64 ID",
			args{
				int64(12345),
			},
			"12345",
			false,
		},
		{
			"float64 ID (unsupported)",
			args{
				12.23123,
			},
			"",
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := convertIDToString(tt.args.id)
			if (err != nil) != tt.wantErr {
				t.Errorf("convertIDToString() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("convertIDToString() got = %v, want %v", got, tt.want)
			}
		})
	}
}
