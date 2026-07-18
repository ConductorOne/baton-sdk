package expand

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestGraphBlobRoundTrip: marshal/unmarshal preserves the graph; the sync-id
// guard rejects a blob from a different sync.
func TestGraphBlobRoundTrip(t *testing.T) {
	ctx := context.Background()
	g := NewEntitlementGraph(ctx)
	g.AddEntitlementID("ent-a")
	g.AddEntitlementID("ent-b")
	require.NoError(t, g.AddEdge(ctx, "ent-a", "ent-b", false, nil))

	data, err := MarshalGraphBlob("sync-1", g)
	require.NoError(t, err)

	got, err := UnmarshalGraphBlob(data, "sync-1")
	require.NoError(t, err)
	require.NotNil(t, got)
	require.NotNil(t, got.GetNode("ent-a"))
	require.Len(t, got.Edges, 1)
	// reinitMaps ran: absent maps are usable, not nil.
	require.NotNil(t, got.EntitlementsToNodes)

	// Wrong sync id -> nil (stale inherited sidecar).
	stale, err := UnmarshalGraphBlob(data, "sync-2")
	require.NoError(t, err)
	require.Nil(t, stale)

	// Empty want skips the guard.
	unguarded, err := UnmarshalGraphBlob(data, "")
	require.NoError(t, err)
	require.NotNil(t, unguarded)
}

// TestMarshalGraphBlob_StripsTransientState: the blob never carries the
// expansion scaffolding.
func TestMarshalGraphBlob_StripsTransientState(t *testing.T) {
	ctx := context.Background()
	g := NewEntitlementGraph(ctx)
	g.AddEntitlementID("ent-a")
	g.Actions = []*EntitlementGraphAction{{}}

	data, err := MarshalGraphBlob("s", g)
	require.NoError(t, err)
	got, err := UnmarshalGraphBlob(data, "s")
	require.NoError(t, err)
	require.Nil(t, got.Actions, "transient state must be stripped from the blob")
}

// TestGraphBlobSizeAtScale measures the sidecar blob for a nested-groups graph
// at increasing node counts — the measurement behind moving the graph out of
// the sync token (tokens travel through workflow state; the c1z does not).
func TestGraphBlobSizeAtScale(t *testing.T) {
	ctx := context.Background()
	for _, n := range []int{1_000, 10_000, 50_000} {
		g := NewEntitlementGraph(ctx)
		for i := 0; i < n; i++ {
			g.AddEntitlementID(entName(i))
		}
		// Nested chains: every node points at the next, 10-deep trees.
		for i := 0; i+1 < n; i++ {
			if i%10 != 9 {
				require.NoError(t, g.AddEdge(ctx, entName(i), entName(i+1), false, nil))
			}
		}
		data, err := MarshalGraphBlob("sync", g)
		require.NoError(t, err)
		require.NotEmpty(t, data)
		t.Logf("nodes=%d edges=%d blob=%d bytes (%.0f B/node)", n, len(g.Edges), len(data), float64(len(data))/float64(n))
	}
}

func entName(i int) string {
	return fmt.Sprintf("group:g%06d:member", i)
}
