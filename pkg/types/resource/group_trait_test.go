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
