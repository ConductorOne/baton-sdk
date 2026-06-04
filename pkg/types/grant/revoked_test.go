package grant

import (
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/stretchr/testify/require"
)

func TestNewAdditionalGrantsRevoked(t *testing.T) {
	gr := NewAdditionalGrantsRevoked("grant-a", "grant-b")
	require.NotNil(t, gr)
	require.Equal(t, []string{"grant-a", "grant-b"}, gr.GetRevokedGrantIds())
}

func TestAppendAdditionalGrantsRevoked(t *testing.T) {
	annos := annotations.Annotations{}
	annos = AppendAdditionalGrantsRevoked(annos, "old-grant-1", "old-grant-2")

	got := &v2.AdditionalGrantsRevoked{}
	found, err := annos.Pick(got)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []string{"old-grant-1", "old-grant-2"}, got.GetRevokedGrantIds())
}
