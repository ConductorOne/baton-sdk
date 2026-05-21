package resource

import (
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/stretchr/testify/require"
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

// Locks the wire-format strings of the normalized vocabulary: a change
// here is a cross-stack compatibility break for any connector or
// downstream consumer that hardcodes one of the values.
func TestGroupSourceTypeVocabulary(t *testing.T) {
	require.Equal(t, GroupSourceType("native"), GroupSourceTypeNative)
	require.Equal(t, GroupSourceType("app_imported"), GroupSourceTypeAppImported)
	require.Equal(t, GroupSourceType("built_in"), GroupSourceTypeBuiltIn)
	require.Equal(t, GroupSourceType("directory_synced"), GroupSourceTypeDirectorySynced)
	require.Equal(t, GroupSourceType("dynamic"), GroupSourceTypeDynamic)
	require.Equal(t, GroupSourceType("distribution"), GroupSourceTypeDistribution)
}
