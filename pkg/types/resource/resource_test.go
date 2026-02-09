package resource

import (
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/stretchr/testify/require"
)

func TestNewAppResource(t *testing.T) {
	profile := map[string]interface{}{
		"app_name": "Test",
	}
	rt := NewResourceType("App", []v2.ResourceType_Trait{v2.ResourceType_TRAIT_APP})
	ar, err := NewAppResource(
		"test app",
		rt,
		1234,
		[]AppTraitOption{
			WithAppHelpURL("https://example.com"),
			WithAppProfile(profile),
		},
		WithAnnotation(v2.V1Identifier_builder{Id: "v1"}.Build()),
		WithAliases("oldid1", "oldid2", "oldid1"),
	)
	require.NoError(t, err)
	require.NotNil(t, ar)
	require.Equal(t, rt.GetId(), ar.GetId().GetResourceType())
	require.Equal(t, "1234", ar.GetId().GetResource())

	require.Len(t, ar.GetAnnotations(), 3)
	v1ID := &v2.V1Identifier{}
	annos := annotations.Annotations(ar.GetAnnotations())
	ok, err := annos.Pick(v1ID)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "v1", v1ID.GetId())

	roleTrait, err := GetRoleTrait(ar)
	require.Error(t, err)
	require.Nil(t, roleTrait)
	appTrait, err := GetAppTrait(ar)
	require.NoError(t, err)
	require.NotNil(t, appTrait)
	require.Equal(t, "https://example.com", appTrait.GetHelpUrl())
	require.NotNil(t, appTrait.GetProfile())
	fName, foundProfileData := GetProfileStringValue(appTrait.GetProfile(), "app_name")
	require.True(t, foundProfileData)
	require.Equal(t, "Test", fName)
	mName, foundProfileData := GetProfileStringValue(appTrait.GetProfile(), "first_name")
	require.False(t, foundProfileData)
	require.Equal(t, "", mName)

	aliases := &v2.Aliases{}
	ok, err = annos.Pick(aliases)
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, aliases.GetIds(), 2)
}

func TestNewGroupResource(t *testing.T) {
	profile := map[string]interface{}{
		"group_name": "Test",
	}
	rt := NewResourceType("Group", []v2.ResourceType_Trait{v2.ResourceType_TRAIT_GROUP})
	gr, err := NewGroupResource(
		"test group",
		rt,
		1234,
		[]GroupTraitOption{
			WithGroupProfile(profile),
		},
		WithAnnotation(v2.V1Identifier_builder{Id: "v1"}.Build()),
	)
	require.NoError(t, err)
	require.NotNil(t, gr)
	require.Equal(t, rt.GetId(), gr.GetId().GetResourceType())
	require.Equal(t, "1234", gr.GetId().GetResource())

	require.Len(t, gr.GetAnnotations(), 2)
	v1ID := &v2.V1Identifier{}
	annos := annotations.Annotations(gr.GetAnnotations())
	ok, err := annos.Pick(v1ID)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "v1", v1ID.GetId())

	roleTrait, err := GetRoleTrait(gr)
	require.Error(t, err)
	require.Nil(t, roleTrait)
	groupTrait, err := GetGroupTrait(gr)
	require.NoError(t, err)
	require.NotNil(t, groupTrait)
	require.NotNil(t, groupTrait.GetProfile())
	fName, foundProfileData := GetProfileStringValue(groupTrait.GetProfile(), "group_name")
	require.True(t, foundProfileData)
	require.Equal(t, "Test", fName)
	mName, foundProfileData := GetProfileStringValue(groupTrait.GetProfile(), "first_name")
	require.False(t, foundProfileData)
	require.Equal(t, "", mName)
}

func TestNewResource(t *testing.T) {
	parentID := v2.ResourceId_builder{
		ResourceType: "foo",
		Resource:     "567",
	}.Build()
	rt := NewResourceType("Role", []v2.ResourceType_Trait{v2.ResourceType_TRAIT_ROLE})
	rr, err := NewResource("test resource", rt, "1234", WithParentResourceID(parentID), WithAnnotation(v2.V1Identifier_builder{Id: "v1"}.Build()))
	require.NoError(t, err)
	require.NotNil(t, rr)
	require.Equal(t, rt.GetId(), rr.GetId().GetResourceType())
	require.Equal(t, "1234", rr.GetId().GetResource())
	require.Equal(t, parentID, rr.GetParentResourceId())

	require.Len(t, rr.GetAnnotations(), 1)
	v1ID := &v2.V1Identifier{}
	annos := annotations.Annotations(rr.GetAnnotations())
	ok, err := annos.Pick(v1ID)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "v1", v1ID.GetId())

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
	require.Equal(t, "foo", id.GetResourceType())
	require.Equal(t, "1234", id.GetResource())
}

func TestNewRoleResource(t *testing.T) {
	profile := map[string]interface{}{
		"role_name": "Test",
	}
	rt := NewResourceType("Role", []v2.ResourceType_Trait{v2.ResourceType_TRAIT_ROLE})
	rr, err := NewRoleResource(
		"test role",
		rt,
		"1234",
		[]RoleTraitOption{
			WithRoleProfile(profile),
		},
		WithAnnotation(v2.V1Identifier_builder{Id: "v1"}.Build()),
	)
	require.NoError(t, err)
	require.NotNil(t, rr)
	require.Equal(t, rt.GetId(), rr.GetId().GetResourceType())
	require.Equal(t, "1234", rr.GetId().GetResource())

	require.Len(t, rr.GetAnnotations(), 2)
	v1ID := &v2.V1Identifier{}
	annos := annotations.Annotations(rr.GetAnnotations())
	ok, err := annos.Pick(v1ID)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "v1", v1ID.GetId())

	groupTrait, err := GetGroupTrait(rr)
	require.Error(t, err)
	require.Nil(t, groupTrait)

	roleTrait, err := GetRoleTrait(rr)
	require.NoError(t, err)
	require.NotNil(t, roleTrait)
	require.NotNil(t, roleTrait.GetProfile())
	fName, foundProfileData := GetProfileStringValue(roleTrait.GetProfile(), "role_name")
	require.True(t, foundProfileData)
	require.Equal(t, "Test", fName)
	mName, foundProfileData := GetProfileStringValue(roleTrait.GetProfile(), "first_name")
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
	ur, err := NewUserResource(
		"test user",
		rt,
		1234,
		[]UserTraitOption{
			WithEmail(userEmail, true),
			WithUserProfile(profile),
		},
		WithAnnotation(v2.V1Identifier_builder{Id: "v1"}.Build()),
	)
	require.NoError(t, err)
	require.NotNil(t, ur)
	require.Equal(t, rt.GetId(), ur.GetId().GetResourceType())
	require.Equal(t, "1234", ur.GetId().GetResource())

	require.Len(t, ur.GetAnnotations(), 2)
	v1ID := &v2.V1Identifier{}
	annos := annotations.Annotations(ur.GetAnnotations())
	ok, err := annos.Pick(v1ID)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "v1", v1ID.GetId())

	roleTrait, err := GetRoleTrait(ur)
	require.Error(t, err)
	require.Nil(t, roleTrait)

	ut, err := GetUserTrait(ur)
	require.NoError(t, err)
	require.NotNil(t, ut)
	require.Len(t, ut.GetEmails(), 1)
	require.Equal(t, userEmail, ut.GetEmails()[0].GetAddress())
	require.NotNil(t, ut.GetProfile())
	fName, foundProfileData := GetProfileStringValue(ut.GetProfile(), "first_name")
	require.True(t, foundProfileData)
	require.Equal(t, "Test", fName)
	mName, foundProfileData := GetProfileStringValue(ut.GetProfile(), "middle_name")
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
