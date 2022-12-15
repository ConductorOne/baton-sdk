package resource

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRoleTrait(t *testing.T) {
	rt, err := NewRoleTrait()
	require.NoError(t, err)
	require.Nil(t, rt.Profile)

	roleProfile := make(map[string]interface{})
	roleProfile["test"] = "role-profile-field"

	rt, err = NewRoleTrait(WithRoleProfile(roleProfile))
	require.NoError(t, err)

	require.NotNil(t, rt.Profile)
	val, ok := GetProfileStringValue(rt.Profile, "test")
	require.True(t, ok)
	require.Equal(t, "role-profile-field", val)
}
