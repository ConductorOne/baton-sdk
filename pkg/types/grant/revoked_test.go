package grant

import (
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/stretchr/testify/require"
)

func TestNewGrantsRevoked(t *testing.T) {
	got := NewGrantsRevoked("role:role-2:member")
	require.NotNil(t, got)
	require.Equal(t, []string{"role:role-2:member"}, got.GetEntitlementIds())
}

func TestAppendGrantsRevoked(t *testing.T) {
	annos := annotations.Annotations{}
	annos = AppendGrantsRevoked(annos, "role:role-2:member")

	got := &v2.GrantsRevoked{}
	found, err := annos.Pick(got)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []string{"role:role-2:member"}, got.GetEntitlementIds())
}
