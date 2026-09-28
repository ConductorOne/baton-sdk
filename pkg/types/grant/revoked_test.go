package grant

import (
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/stretchr/testify/require"
)

func TestNewGrantsRevoked(t *testing.T) {
	removed := NewGrant(
		&v2.Resource{Id: &v2.ResourceId{ResourceType: "role", Resource: "role-2"}},
		"member",
		&v2.ResourceId{ResourceType: "user", Resource: "user-1"},
	)
	got := NewGrantsRevoked(removed)
	require.NotNil(t, got)
	require.Len(t, got.GetGrants(), 1)
	require.Equal(t, "role:role-2:member", got.GetGrants()[0].GetEntitlement().GetId())
	require.Equal(t, "user", got.GetGrants()[0].GetPrincipal().GetId().GetResourceType())
	require.Equal(t, "user-1", got.GetGrants()[0].GetPrincipal().GetId().GetResource())
}

func TestAppendGrantsRevoked(t *testing.T) {
	removed := NewGrant(
		&v2.Resource{Id: &v2.ResourceId{ResourceType: "role", Resource: "role-2"}},
		"member",
		&v2.ResourceId{ResourceType: "user", Resource: "user-1"},
	)
	annos := annotations.Annotations{}
	annos = AppendGrantsRevoked(annos, removed)

	got := &v2.GrantsRevoked{}
	found, err := annos.Pick(got)
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, got.GetGrants(), 1)
	require.Equal(t, "role:role-2:member", got.GetGrants()[0].GetEntitlement().GetId())
	require.Equal(t, removed.GetId(), got.GetGrants()[0].GetId())
}

func TestAppendGrantsRevokedMergesIntoExisting(t *testing.T) {
	member := NewGrant(
		&v2.Resource{Id: &v2.ResourceId{ResourceType: "role", Resource: "role-2"}},
		"member",
		&v2.ResourceId{ResourceType: "user", Resource: "user-1"},
	)
	owner := NewGrant(
		&v2.Resource{Id: &v2.ResourceId{ResourceType: "role", Resource: "role-2"}},
		"owner",
		&v2.ResourceId{ResourceType: "user", Resource: "user-1"},
	)

	annos := annotations.Annotations{}
	annos = AppendGrantsRevoked(annos, member)
	annos = AppendGrantsRevoked(annos, owner)
	require.Len(t, annos, 1)

	got := &v2.GrantsRevoked{}
	found, err := annos.Pick(got)
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, got.GetGrants(), 2)
	require.Equal(t, "role:role-2:member", got.GetGrants()[0].GetEntitlement().GetId())
	require.Equal(t, "role:role-2:owner", got.GetGrants()[1].GetEntitlement().GetId())
}
