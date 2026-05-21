package resource

import (
	"errors"
	"strings"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
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

func TestAppTraitSourceType(t *testing.T) {
	at, err := NewAppTrait()
	require.NoError(t, err)
	require.Empty(t, at.GetAppSourceType())
	require.Empty(t, at.GetRawAppSourceType())

	at, err = NewAppTrait(
		WithAppSourceType("native"),
		WithRawAppSourceType("OKTA_APP"),
	)
	require.NoError(t, err)
	require.Equal(t, "native", at.GetAppSourceType())
	require.Equal(t, "OKTA_APP", at.GetRawAppSourceType())
}

func TestAppTraitValidateSourceType(t *testing.T) {
	cases := []struct {
		name     string
		srcType  string
		rawType  string
		errField string
	}{
		{"empty passes", "", "", ""},
		{"valid passes", "saml", "OKTA_APP", ""},
		{"normalized too long", strings.Repeat("a", 65), "", "AppSourceType"},
		{"raw too long", "saml", strings.Repeat("a", 257), "RawAppSourceType"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			at := &v2.AppTrait{}
			at.SetAppSourceType(tc.srcType)
			at.SetRawAppSourceType(tc.rawType)
			err := at.Validate()
			if tc.errField == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			var vErr v2.AppTraitValidationError
			require.True(t, errors.As(err, &vErr))
			require.Equal(t, tc.errField, vErr.Field())
		})
	}
}

func TestAppTraitAnnotationRoundTrip(t *testing.T) {
	src, err := NewAppTrait(
		WithAppSourceType("saml"),
		WithRawAppSourceType("OKTA_APP"),
	)
	require.NoError(t, err)
	resource := &v2.Resource{Annotations: annotations.New(src)}
	got, err := GetAppTrait(resource)
	require.NoError(t, err)
	require.Equal(t, "saml", got.GetAppSourceType())
	require.Equal(t, "OKTA_APP", got.GetRawAppSourceType())
}
