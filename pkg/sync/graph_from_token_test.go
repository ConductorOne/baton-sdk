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

// TestPrepareExpansionReplayToken_ClearsPreservedGraph (U2): a token that
// preserved its entitlement graph (WithPreserveEntitlementGraph) must have that
// graph cleared when rewritten for replay — otherwise the replay skips graph
// loading (graph already "expanded") and silently no-ops.
func TestPrepareExpansionReplayToken_ClearsPreservedGraph(t *testing.T) {
	ctx := context.Background()

	// A finished sync that preserved its (fully-expanded) graph.
	g := expand.NewEntitlementGraph(ctx)
	g.AddEntitlementID("eng:manager")
	g.AddEntitlementID("eng:member")
	require.NoError(t, g.AddEdge(ctx, "eng:manager", "eng:member", false, nil))
	g.MarkEdgeExpanded("eng:manager", "eng:member")
	g.Loaded = true

	st := newState()
	st.entitlementGraph = g
	token, err := st.Marshal()
	require.NoError(t, err)
	// Sanity: the token really does carry a graph.
	pre, err := GraphFromToken(token)
	require.NoError(t, err)
	require.NotNil(t, pre)

	// Rewriting for replay must strip the graph so the replay rebuilds it.
	replay, err := PrepareExpansionReplayToken(token)
	require.NoError(t, err)

	got, err := GraphFromToken(replay)
	require.NoError(t, err)
	require.Nil(t, got, "replay token must not carry the preserved graph")

	needs, err := NeedsExpansion(replay)
	require.NoError(t, err)
	require.True(t, needs, "replay token must be marked needs-expansion")
}
