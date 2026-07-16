package sync //nolint:revive,nolintlint // package name kept for compatibility

import (
	"context"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/sync/expand"
	"github.com/stretchr/testify/require"
)

// TestGraphFromToken: a graph put into a state token round-trips out via
// GraphFromToken, so an incremental run can reload a prior sync's graph.
func TestGraphFromToken(t *testing.T) {
	ctx := context.Background()

	g := expand.NewEntitlementGraph(ctx)
	g.AddEntitlementID("eng:manager")
	g.AddEntitlementID("eng:member")
	require.NoError(t, g.AddEdge(ctx, "eng:manager", "eng:member", false, nil))

	st := newState()
	st.entitlementGraph = g
	token, err := st.Marshal()
	require.NoError(t, err)

	loaded, err := GraphFromToken(token)
	require.NoError(t, err)
	require.NotNil(t, loaded)
	require.NotNil(t, loaded.GetNode("eng:manager"))
	require.NotNil(t, loaded.GetNode("eng:member"))
	require.Len(t, loaded.Edges, 1)
}

// TestGraphFromToken_Empty: a token with no graph yields nil, not an error.
func TestGraphFromToken_Empty(t *testing.T) {
	st := newState()
	token, err := st.Marshal()
	require.NoError(t, err)

	loaded, err := GraphFromToken(token)
	require.NoError(t, err)
	require.Nil(t, loaded)
}
