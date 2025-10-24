package resource

import (
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/stretchr/testify/require"
)

func TestAppTrait(t *testing.T) {
	at, err := NewAppTrait()
	require.NoError(t, err)
	require.Empty(t, at.GetHelpUrl())
	require.Nil(t, at.GetProfile())
	require.Nil(t, at.GetIcon())
	require.Nil(t, at.GetLogo())

	at, err = NewAppTrait(WithAppIcon(v2.AssetRef_builder{Id: "iconID"}.Build()))
	require.NoError(t, err)
	require.Empty(t, at.GetHelpUrl())
	require.Nil(t, at.GetProfile())
	require.Nil(t, at.GetLogo())

	require.NotNil(t, at.GetIcon())
	require.Equal(t, "iconID", at.GetIcon().GetId())

	at, err = NewAppTrait(WithAppLogo(v2.AssetRef_builder{Id: "logoID"}.Build()))
	require.NoError(t, err)
	require.Empty(t, at.GetHelpUrl())
	require.Nil(t, at.GetProfile())
	require.Nil(t, at.GetIcon())

	require.NotNil(t, at.GetLogo())
	require.Equal(t, "logoID", at.GetLogo().GetId())

	at, err = NewAppTrait(
		WithAppHelpURL("https://example.com/help"),
		WithAppLogo(v2.AssetRef_builder{Id: "logoID"}.Build()),
	)
	require.NoError(t, err)
	require.Nil(t, at.GetProfile())
	require.Nil(t, at.GetIcon())

	require.Equal(t, "https://example.com/help", at.GetHelpUrl())
	require.NotNil(t, at.GetLogo())
	require.Equal(t, "logoID", at.GetLogo().GetId())

	appProfile := make(map[string]interface{})
	appProfile["test"] = "app-profile-field"

	at, err = NewAppTrait(
		WithAppHelpURL("https://example.com/help"),
		WithAppLogo(v2.AssetRef_builder{Id: "logoID"}.Build()),
		WithAppProfile(appProfile),
	)
	require.NoError(t, err)
	require.Nil(t, at.GetIcon())

	require.NotNil(t, at.GetProfile())
	val, ok := GetProfileStringValue(at.GetProfile(), "test")
	require.True(t, ok)
	require.Equal(t, "app-profile-field", val)
	require.Equal(t, "https://example.com/help", at.GetHelpUrl())
	require.NotNil(t, at.GetLogo())
	require.Equal(t, "logoID", at.GetLogo().GetId())
}
