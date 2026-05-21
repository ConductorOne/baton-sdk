package resource

import (
	"errors"
	"strings"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestGroupTrait(t *testing.T) {
	gt, err := NewGroupTrait()
	require.NoError(t, err)
	require.Nil(t, gt.GetProfile())
	require.Nil(t, gt.GetIcon())

	gt, err = NewGroupTrait(WithGroupIcon(v2.AssetRef_builder{Id: "iconID"}.Build()))
	require.NoError(t, err)
	require.Nil(t, gt.GetProfile())

	require.NotNil(t, gt.GetIcon())
	require.Equal(t, "iconID", gt.GetIcon().GetId())

	groupProfile := make(map[string]interface{})
	groupProfile["test"] = "group-profile-field"

	gt, err = NewGroupTrait(
		WithGroupIcon(v2.AssetRef_builder{Id: "iconID"}.Build()),
		WithGroupProfile(groupProfile),
	)
	require.NoError(t, err)

	require.NotNil(t, gt.GetIcon())
	require.Equal(t, "iconID", gt.GetIcon().GetId())
	require.NotNil(t, gt.GetProfile())
	val, ok := GetProfileStringValue(gt.GetProfile(), "test")
	require.True(t, ok)
	require.Equal(t, "group-profile-field", val)
}

func TestGroupTraitSourceType(t *testing.T) {
	gt, err := NewGroupTrait()
	require.NoError(t, err)
	require.Empty(t, gt.GetGroupSourceType())
	require.Empty(t, gt.GetRawGroupSourceType())

	gt, err = NewGroupTrait(
		WithGroupSourceType(GroupSourceTypeAppImported),
		WithRawGroupSourceType("OKTA_GROUP"),
	)
	require.NoError(t, err)
	require.Equal(t, "app_imported", gt.GetGroupSourceType())
	require.Equal(t, "OKTA_GROUP", gt.GetRawGroupSourceType())
}

// Marshals a GroupTrait built from each constant and asserts the wire
// bytes round-trip back to the documented literal string. A constant
// rename or typo trips this; a same-value alias does not.
func TestGroupSourceTypeWireFormat(t *testing.T) {
	cases := []struct {
		c    GroupSourceType
		want string
	}{
		{GroupSourceTypeNative, "native"},
		{GroupSourceTypeAppImported, "app_imported"},
		{GroupSourceTypeBuiltIn, "built_in"},
		{GroupSourceTypeDirectorySynced, "directory_synced"},
		{GroupSourceTypeDynamic, "dynamic"},
		{GroupSourceTypeDistribution, "distribution"},
	}
	for _, tc := range cases {
		t.Run(tc.want, func(t *testing.T) {
			src, err := NewGroupTrait(WithGroupSourceType(tc.c))
			require.NoError(t, err)
			wire, err := proto.Marshal(src)
			require.NoError(t, err)
			got := &v2.GroupTrait{}
			require.NoError(t, proto.Unmarshal(wire, got))
			require.Equal(t, tc.want, got.GetGroupSourceType())
		})
	}
}

func TestGroupTraitValidateSourceType(t *testing.T) {
	cases := []struct {
		name     string
		srcType  string
		rawType  string
		errField string
	}{
		{"empty passes", "", "", ""},
		{"valid passes", "native", "OKTA_GROUP", ""},
		{"normalized too long", strings.Repeat("a", 65), "", "GroupSourceType"},
		{"raw too long", "native", strings.Repeat("a", 257), "RawGroupSourceType"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gt := &v2.GroupTrait{}
			gt.SetGroupSourceType(tc.srcType)
			gt.SetRawGroupSourceType(tc.rawType)
			err := gt.Validate()
			if tc.errField == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			var vErr v2.GroupTraitValidationError
			require.True(t, errors.As(err, &vErr))
			require.Equal(t, tc.errField, vErr.Field())
		})
	}
}

func TestGroupTraitAnnotationRoundTrip(t *testing.T) {
	src, err := NewGroupTrait(
		WithGroupSourceType(GroupSourceTypeBuiltIn),
		WithRawGroupSourceType("OKTA_GROUP"),
	)
	require.NoError(t, err)
	resource := &v2.Resource{Annotations: annotations.New(src)}
	got, err := GetGroupTrait(resource)
	require.NoError(t, err)
	require.Equal(t, "built_in", got.GetGroupSourceType())
	require.Equal(t, "OKTA_GROUP", got.GetRawGroupSourceType())
}
