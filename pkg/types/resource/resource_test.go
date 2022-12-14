package resource

import (
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	sdk "github.com/conductorone/baton-sdk/pkg/sdk"
	"github.com/stretchr/testify/require"
)

func TestNewAppResource(t *testing.T) {
	profile := map[string]interface{}{
		"app_name": "Test",
	}
	rt := NewResourceType("App", []v2.ResourceType_Trait{v2.ResourceType_TRAIT_APP})
	ar, err := NewAppResource("test app", rt, nil, 1234, "https://example.com", profile, WithAnnotation(&v2.V1Identifier{Id: "v1"}))
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

	roleTrait, err := sdk.GetRoleTrait(ar)
	require.Error(t, err)
	require.Nil(t, roleTrait)
	appTrait, err := sdk.GetAppTrait(ar)
	require.NoError(t, err)
	require.NotNil(t, appTrait)
	require.Equal(t, "https://example.com", appTrait.HelpUrl)
	require.NotNil(t, appTrait.Profile)
	fName, foundProfileData := sdk.GetProfileStringValue(appTrait.Profile, "app_name")
	require.True(t, foundProfileData)
	require.Equal(t, "Test", fName)
	mName, foundProfileData := sdk.GetProfileStringValue(appTrait.Profile, "first_name")
	require.False(t, foundProfileData)
	require.Equal(t, "", mName)
}

func TestNewGroupResource(t *testing.T) {
	profile := map[string]interface{}{
		"group_name": "Test",
	}
	rt := NewResourceType("Group", []v2.ResourceType_Trait{v2.ResourceType_TRAIT_GROUP})
	gr, err := NewGroupResource("test group", rt, nil, 1234, profile, WithAnnotation(&v2.V1Identifier{Id: "v1"}))
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

	roleTrait, err := sdk.GetRoleTrait(gr)
	require.Error(t, err)
	require.Nil(t, roleTrait)
	groupTrait, err := sdk.GetGroupTrait(gr)
	require.NoError(t, err)
	require.NotNil(t, groupTrait)
	require.NotNil(t, groupTrait.Profile)
	fName, foundProfileData := sdk.GetProfileStringValue(groupTrait.Profile, "group_name")
	require.True(t, foundProfileData)
	require.Equal(t, "Test", fName)
	mName, foundProfileData := sdk.GetProfileStringValue(groupTrait.Profile, "first_name")
	require.False(t, foundProfileData)
	require.Equal(t, "", mName)
}

func TestNewResource(t *testing.T) {
	parentID := &v2.ResourceId{
		ResourceType: "foo",
		Resource:     "567",
	}
	rt := NewResourceType("Role", []v2.ResourceType_Trait{v2.ResourceType_TRAIT_ROLE})
	rr, err := NewResource("test resource", rt, parentID, "1234", WithAnnotation(&v2.V1Identifier{Id: "v1"}))
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

	groupTrait, err := sdk.GetGroupTrait(rr)
	require.Error(t, err)
	require.Nil(t, groupTrait)

	userTrait, err := sdk.GetUserTrait(rr)
	require.Error(t, err)
	require.Nil(t, userTrait)

	roleTrait, err := sdk.GetRoleTrait(rr)
	require.Error(t, err)
	require.Nil(t, roleTrait)

	appTrait, err := sdk.GetAppTrait(rr)
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
	rr, err := NewRoleResource("test role", rt, nil, "1234", profile, WithAnnotation(&v2.V1Identifier{Id: "v1"}))
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

	groupTrait, err := sdk.GetGroupTrait(rr)
	require.Error(t, err)
	require.Nil(t, groupTrait)

	roleTrait, err := sdk.GetRoleTrait(rr)
	require.NoError(t, err)
	require.NotNil(t, roleTrait)
	require.NotNil(t, roleTrait.Profile)
	fName, foundProfileData := sdk.GetProfileStringValue(roleTrait.Profile, "role_name")
	require.True(t, foundProfileData)
	require.Equal(t, "Test", fName)
	mName, foundProfileData := sdk.GetProfileStringValue(roleTrait.Profile, "first_name")
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
	ur, err := NewUserResource("test user", rt, nil, 1234, userEmail, profile, WithAnnotation(&v2.V1Identifier{Id: "v1"}))
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

	roleTrait, err := sdk.GetRoleTrait(ur)
	require.Error(t, err)
	require.Nil(t, roleTrait)

	ut, err := sdk.GetUserTrait(ur)
	require.NoError(t, err)
	require.NotNil(t, ut)
	require.Len(t, ut.Emails, 1)
	require.Equal(t, userEmail, ut.Emails[0].Address)
	require.NotNil(t, ut.Profile)
	fName, foundProfileData := sdk.GetProfileStringValue(ut.Profile, "first_name")
	require.True(t, foundProfileData)
	require.Equal(t, "Test", fName)
	mName, foundProfileData := sdk.GetProfileStringValue(ut.Profile, "middle_name")
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
