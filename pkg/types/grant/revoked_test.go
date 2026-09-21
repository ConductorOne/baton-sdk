package grant

import (
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/stretchr/testify/require"
)

func TestNewGrantsRevoked(t *testing.T) {
	roleID := &v2.ResourceId{ResourceType: "role", Resource: "role-2"}
	got := NewGrantsRevoked(roleID)
	require.NotNil(t, got)
	require.Equal(t, []*v2.ResourceId{roleID}, got.GetResourceIds())
}

func TestAppendGrantsRevoked(t *testing.T) {
	roleID := &v2.ResourceId{ResourceType: "role", Resource: "role-2"}
	annos := annotations.Annotations{}
	annos = AppendGrantsRevoked(annos, roleID)

	got := &v2.GrantsRevoked{}
	found, err := annos.Pick(got)
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, got.GetResourceIds(), 1)
	require.Equal(t, "role", got.GetResourceIds()[0].GetResourceType())
	require.Equal(t, "role-2", got.GetResourceIds()[0].GetResource())
}
