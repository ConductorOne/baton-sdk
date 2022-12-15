package resource

import (
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/stretchr/testify/require"
)

func TestAppTrait(t *testing.T) {
	at, err := NewAppTrait()
	require.NoError(t, err)
	require.Empty(t, at.HelpUrl)
	require.Nil(t, at.Profile)
	require.Nil(t, at.Icon)
	require.Nil(t, at.Logo)

	at, err = NewAppTrait(WithAppIcon(&v2.AssetRef{Id: "iconID"}))
	require.NoError(t, err)
	require.Empty(t, at.HelpUrl)
	require.Nil(t, at.Profile)
	require.Nil(t, at.Logo)

	require.NotNil(t, at.Icon)
	require.Equal(t, "iconID", at.Icon.Id)

	at, err = NewAppTrait(WithAppLogo(&v2.AssetRef{Id: "logoID"}))
	require.NoError(t, err)
	require.Empty(t, at.HelpUrl)
	require.Nil(t, at.Profile)
	require.Nil(t, at.Icon)

	require.NotNil(t, at.Logo)
	require.Equal(t, "logoID", at.Logo.Id)

	at, err = NewAppTrait(
		WithAppHelpURL("https://example.com/help"),
		WithAppLogo(&v2.AssetRef{Id: "logoID"}),
	)
	require.NoError(t, err)
	require.Nil(t, at.Profile)
	require.Nil(t, at.Icon)

	require.Equal(t, "https://example.com/help", at.HelpUrl)
	require.NotNil(t, at.Logo)
	require.Equal(t, "logoID", at.Logo.Id)

	appProfile := make(map[string]interface{})
	appProfile["test"] = "profile-field"

	at, err = NewAppTrait(
		WithAppHelpURL("https://example.com/help"),
		WithAppLogo(&v2.AssetRef{Id: "logoID"}),
		WithAppProfile(appProfile),
	)
	require.NoError(t, err)
	require.Nil(t, at.Icon)

	require.NotNil(t, at.Profile)
	val, ok := GetProfileStringValue(at.Profile, "test")
	require.True(t, ok)
	require.Equal(t, "profile-field", val)
	require.Equal(t, "https://example.com/help", at.HelpUrl)
	require.NotNil(t, at.Logo)
	require.Equal(t, "logoID", at.Logo.Id)
}
