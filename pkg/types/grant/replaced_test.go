package grant

import (
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/stretchr/testify/require"
)

func TestNewGrantReplaced(t *testing.T) {
	gr := NewGrantReplaced("grant-a")
	require.NotNil(t, gr)
	require.Equal(t, "grant-a", gr.GetReplacedGrantId())
}

func TestAppendGrantReplaced(t *testing.T) {
	annos := annotations.Annotations{}
	annos = AppendGrantReplaced(annos, "old-grant-1")

	got := &v2.GrantReplaced{}
	found, err := annos.Pick(got)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "old-grant-1", got.GetReplacedGrantId())
}
