package expand

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGraphBlobRejectsMissingAndUnknownVersions(t *testing.T) {
	ctx := context.Background()
	g := NewEntitlementGraph(ctx)
	g.AddEntitlementID("ent-a")

	legacy, err := json.Marshal(map[string]any{"sync_id": "sync-1", "graph": g})
	require.NoError(t, err)
	got, err := UnmarshalGraphBlob(legacy, "sync-1")
	require.NoError(t, err)
	require.Nil(t, got, "an unversioned graph must fall back to full expansion")

	future, err := json.Marshal(map[string]any{"format_version": 999, "sync_id": "sync-1", "graph": g})
	require.NoError(t, err)
	got, err = UnmarshalGraphBlob(future, "sync-1")
	require.NoError(t, err)
	require.Nil(t, got, "an unknown graph version must fall back to full expansion")
}

func TestValidateCompletedRejectsInconsistentAdjacency(t *testing.T) {
	ctx := context.Background()
	g := NewEntitlementGraph(ctx)
	g.AddEntitlementID("ent-a")
	g.AddEntitlementID("ent-b")
	require.NoError(t, g.AddEdge(ctx, "ent-a", "ent-b", false, nil))
	g.Loaded = true
	g.MarkExpansionComplete()
	require.NoError(t, g.ValidateCompleted())

	delete(g.SourcesToDestinations[g.GetNode("ent-a").Id], g.GetNode("ent-b").Id)
	require.ErrorContains(t, g.ValidateCompleted(), "missing from source adjacency")
}

func TestValidateCompletedRejectsStaleIDCounters(t *testing.T) {
	ctx := context.Background()
	build := func(t *testing.T) *EntitlementGraph {
		t.Helper()
		g := NewEntitlementGraph(ctx)
		g.AddEntitlementID("ent-a")
		g.AddEntitlementID("ent-b")
		require.NoError(t, g.AddEdge(ctx, "ent-a", "ent-b", false, nil))
		g.Loaded = true
		g.MarkExpansionComplete()
		return g
	}

	t.Run("node counter", func(t *testing.T) {
		g := build(t)
		g.NextNodeID = 0
		require.ErrorContains(t, g.ValidateCompleted(), "next node id")
	})

	t.Run("edge counter", func(t *testing.T) {
		g := build(t)
		g.NextEdgeID = 0
		require.ErrorContains(t, g.ValidateCompleted(), "next edge id")
	})
}

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
	actions := []*EntitlementGraphAction{{}}
	plan := &EntitlementGraphPlan{}
	metrics := &EntitlementGraphMetrics{}
	g.Actions = actions
	g.ExpansionPlan = plan
	g.ExpansionMetrics = metrics

	data, err := MarshalGraphBlob("s", g)
	require.NoError(t, err)
	require.Equal(t, actions, g.Actions, "marshal must not change the caller's actions")
	require.Same(t, plan, g.ExpansionPlan, "marshal must not change the caller's plan")
	require.Same(t, metrics, g.ExpansionMetrics, "marshal must not change the caller's metrics")
	got, err := UnmarshalGraphBlob(data, "s")
	require.NoError(t, err)
	require.Nil(t, got.Actions, "transient state must be stripped from the blob")
	require.Nil(t, got.ExpansionPlan, "transient plan must be stripped from the blob")
	require.Nil(t, got.ExpansionMetrics, "transient metrics must be stripped from the blob")
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

func BenchmarkGraphClone(b *testing.B) {
	ctx := context.Background()
	for _, n := range []int{1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("nodes=%d", n), func(b *testing.B) {
			g := NewEntitlementGraph(ctx)
			for i := 0; i < n; i++ {
				g.AddEntitlementID(entName(i))
			}
			for i := 0; i+1 < n; i++ {
				require.NoError(b, g.AddEdge(ctx, entName(i), entName(i+1), false, nil))
			}
			g.MarkExpansionComplete()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				clone, err := g.Clone()
				if err != nil {
					b.Fatal(err)
				}
				if !clone.IsExpanded() {
					b.Fatal("clone lost completion state")
				}
			}
		})
	}
}

func TestGraphCloneIsStructurallyIndependent(t *testing.T) {
	ctx := context.Background()
	graph := NewEntitlementGraph(ctx)
	graph.AddEntitlementID("a")
	graph.AddEntitlementID("b")
	require.NoError(t, graph.AddEdge(ctx, "a", "b", false, []string{"user"}))
	graph.Loaded = true
	graph.MarkExpansionComplete()
	graph.Actions = []*EntitlementGraphAction{{
		SourceEntitlementID: "a",
		Descendants:         []ActionDescendant{{EntitlementID: "b"}},
		ResourceTypeIDs:     []string{"user"},
	}}
	graph.ExpansionPlan = &EntitlementGraphPlan{Order: []int{0, 1}, ProjectionSources: []string{"a"}}
	graph.ExpansionMetrics = &EntitlementGraphMetrics{Algorithm: "test"}

	clone, err := graph.Clone()
	require.NoError(t, err)
	clone.Nodes[graph.GetNode("a").Id] = Node{Id: 99, EntitlementIDs: []string{"changed"}}
	clone.EntitlementsToNodes["a"] = 99
	for edgeID, edge := range clone.Edges {
		edge.ResourceTypeIDs[0] = "service"
		clone.Edges[edgeID] = edge
	}
	clone.Actions[0].Descendants[0].EntitlementID = "changed"
	clone.Actions[0].ResourceTypeIDs[0] = "service"
	clone.ExpansionPlan.Order[0] = 99
	clone.ExpansionPlan.ProjectionSources[0] = "changed"
	clone.ExpansionMetrics.Algorithm = "changed"

	require.Equal(t, "a", graph.Nodes[graph.GetNode("a").Id].EntitlementIDs[0])
	require.Equal(t, graph.GetNode("a").Id, graph.EntitlementsToNodes["a"])
	for _, edge := range graph.Edges {
		require.Equal(t, []string{"user"}, edge.ResourceTypeIDs)
	}
	require.Equal(t, "b", graph.Actions[0].Descendants[0].EntitlementID)
	require.Equal(t, []string{"user"}, graph.Actions[0].ResourceTypeIDs)
	require.Equal(t, 0, graph.ExpansionPlan.Order[0])
	require.Equal(t, "a", graph.ExpansionPlan.ProjectionSources[0])
	require.Equal(t, "test", graph.ExpansionMetrics.Algorithm)
}

func BenchmarkMarshalGraphBlob(b *testing.B) {
	ctx := context.Background()
	for _, n := range []int{1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("nodes=%d", n), func(b *testing.B) {
			g := NewEntitlementGraph(ctx)
			for i := 0; i < n; i++ {
				g.AddEntitlementID(entName(i))
			}
			for i := 0; i+1 < n; i++ {
				require.NoError(b, g.AddEdge(ctx, entName(i), entName(i+1), false, nil))
			}
			g.Loaded = true
			g.MarkExpansionComplete()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				data, err := MarshalGraphBlob("benchmark-sync", g)
				if err != nil {
					b.Fatal(err)
				}
				if len(data) == 0 {
					b.Fatal("empty graph blob")
				}
			}
		})
	}
}

func entName(i int) string {
	return fmt.Sprintf("group:g%06d:member", i)
}

// TestUnmarshalGraphBlob_RejectsPreDanglingVersion: a v2 blob predates
// dangling bookkeeping, so what it skipped is unknowable and it
// must not be reused as an incremental base. Rejection is silent — (nil, nil)
// — so the reader falls back to full expansion rather than failing.
func TestUnmarshalGraphBlob_RejectsPreDanglingVersion(t *testing.T) {
	ctx := context.Background()
	g := NewEntitlementGraph(ctx)
	g.AddEntitlementID("ent-a")

	v2Blob, err := json.Marshal(map[string]any{
		"format_version": 2,
		"sync_id":        "sync-1",
		"graph":          g,
	})
	require.NoError(t, err)

	got, digest, err := UnmarshalGraphBlobWithGrantDigest(v2Blob, "sync-1")
	require.NoError(t, err)
	require.Nil(t, got)
	require.Nil(t, digest)
}

// TestGraphBlob_RoundTripsDanglingState: the recorded ids are what the next run
// seeds from, so losing them in serialization would silently reintroduce the
// divergence this whole mechanism exists to prevent.
func TestGraphBlob_RoundTripsDanglingState(t *testing.T) {
	ctx := context.Background()
	g := NewEntitlementGraph(ctx)
	g.AddEntitlementID("ent-a")
	g.NoteDanglingReference("missing:one")
	g.NoteDanglingReference("missing:two")

	data, err := MarshalGraphBlob("sync-1", g)
	require.NoError(t, err)
	got, err := UnmarshalGraphBlob(data, "sync-1")
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Contains(t, got.DanglingEntitlementIDs, "missing:one")
	require.Contains(t, got.DanglingEntitlementIDs, "missing:two")
	require.False(t, got.DanglingOverflow)
}

// TestGraphBlob_RoundTripsDanglingOverflow: overflow is the flag that makes the
// compactor decline outright, so it must survive too.
func TestGraphBlob_RoundTripsDanglingOverflow(t *testing.T) {
	ctx := context.Background()
	g := NewEntitlementGraph(ctx)
	g.AddEntitlementID("ent-a")
	g.NoteUnrecoverableDangling()

	data, err := MarshalGraphBlob("sync-1", g)
	require.NoError(t, err)
	got, err := UnmarshalGraphBlob(data, "sync-1")
	require.NoError(t, err)
	require.NotNil(t, got)
	require.True(t, got.DanglingOverflow)
}
