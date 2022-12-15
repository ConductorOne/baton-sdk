package resource

import (
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/stretchr/testify/require"
)

func TestGroupTrait(t *testing.T) {
	gt, err := NewGroupTrait()
	require.NoError(t, err)
	require.Nil(t, gt.Profile)
	require.Nil(t, gt.Icon)

	gt, err = NewGroupTrait(WithGroupIcon(&v2.AssetRef{Id: "iconID"}))
	require.NoError(t, err)
	require.Nil(t, gt.Profile)

	require.NotNil(t, gt.Icon)
	require.Equal(t, "iconID", gt.Icon.Id)

	groupProfile := make(map[string]interface{})
	groupProfile["test"] = "group-profile-field"

	gt, err = NewGroupTrait(
		WithGroupIcon(&v2.AssetRef{Id: "iconID"}),
		WithGroupProfile(groupProfile),
	)
	require.NoError(t, err)

	require.NotNil(t, gt.Icon)
	require.Equal(t, "iconID", gt.Icon.Id)
	require.NotNil(t, gt.Profile)
	val, ok := GetProfileStringValue(gt.Profile, "test")
	require.True(t, ok)
	require.Equal(t, "group-profile-field", val)
}
