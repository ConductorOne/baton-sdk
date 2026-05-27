package grant

import (
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/stretchr/testify/require"
)

func TestNewGrantRejected(t *testing.T) {
	gr := NewGrantRejected("rejected by policy")
	require.NotNil(t, gr)
	require.Equal(t, "rejected by policy", gr.GetReason())
}

func TestAppendGrantRejected(t *testing.T) {
	annos := annotations.Annotations{}
	annos = AppendGrantRejected(annos, "rejected by policy")

	got := &v2.GrantRejected{}
	found, err := annos.Pick(got)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "rejected by policy", got.GetReason())
}

func TestWithGrantRejected(t *testing.T) {
	resource := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: "role",
			Resource:     "admin",
		},
	}
	principal := &v2.ResourceId{
		ResourceType: "user",
		Resource:     "user-1",
	}

	got := NewGrant(resource, "member", principal, WithGrantRejected("rejected by policy"))

	rejected := &v2.GrantRejected{}
	annos := annotations.Annotations(got.GetAnnotations())
	found, err := annos.Pick(rejected)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "rejected by policy", rejected.GetReason())
}
