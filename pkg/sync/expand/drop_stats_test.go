package expand

// DroppedEdgeStats counting semantics: totals count EDGES, not drop
// events. The legacy expander drops a whole action's edge batch when the
// shared SOURCE entitlement is missing, and the topological path skips a
// destination's entire reduction (every incoming edge) when the
// DESTINATION is missing — both previously incremented the aggregate
// once per event, undercounting the very numbers the per-sync warning
// exists to report.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDroppedEdgeStatsCountsEdgesNotEvents(t *testing.T) {
	s := &DroppedEdgeStats{}

	// A missing source shared by a 5-destination batch drops 5 edges.
	s.RecordSourceMissingEdges("ent-src", 5)
	// A missing destination with 3 incoming edges skips 3 edges.
	s.RecordDestinationMissingEdges("ent-dst", 3)
	// Single-edge sites still count one.
	s.RecordSourceMissing("ent-src-2")
	s.RecordDestinationMissing("ent-dst-2")
	// Zero/negative counts are no-ops (defensive: empty batches).
	s.RecordSourceMissingEdges("ent-none", 0)
	s.RecordDestinationMissingEdges("ent-none", -1)

	s.mu.Lock()
	defer s.mu.Unlock()
	require.Equal(t, int64(6), s.sourceMissing, "source total must count every edge in a dropped batch")
	require.Equal(t, int64(4), s.destinationMissing, "destination total must count every skipped incoming edge")
	require.Len(t, s.seen, 4, "distinct-id set counts entitlements, not edges; no-op records add nothing")
	require.NotContains(t, s.seen, "ent-none")
}

func TestDroppedEdgeStatsExamplesDedupeAcrossRepeats(t *testing.T) {
	s := &DroppedEdgeStats{}
	for i := 0; i < 3; i++ {
		s.RecordSourceMissingEdges("ent-repeat", 2)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	require.Equal(t, int64(6), s.sourceMissing, "repeat drops of one id still count their edges")
	require.Equal(t, []string{"ent-repeat"}, s.examples, "examples list one entry per distinct id")
}

// TestTopologicalSourceMissingRecordedOnDropStats pins the topological
// reduce's SOURCE-missing path onto the aggregate: a graph edge whose
// source entitlement has no row must complete (drop-don't-fail), be
// counted on DroppedEdgeStats, and not fail the run — the migration
// that moved the destination-side to the aggregate had left this path
// as a per-edge Warn invisible to LogSummary.
func TestTopologicalSourceMissingRecordedOnDropStats(t *testing.T) {
	ctx := context.Background()
	store := NewMockExpanderStore()

	group := makeResource("group", "org")
	source := makeEntitlement("ent:source", group)
	dest := makeEntitlement("ent:dest", group)
	// The DESTINATION row exists; the SOURCE row deliberately does not.
	store.AddEntitlement(dest)

	graph := NewEntitlementGraph(ctx)
	graph.AddEntitlementID(source.GetId())
	graph.AddEntitlementID(dest.GetId())
	require.NoError(t, graph.AddEdge(ctx, source.GetId(), dest.GetId(), false, nil))

	e := NewExpander(store, graph)
	stats := &DroppedEdgeStats{}
	e.SetDropStats(stats)
	require.NoError(t, e.RunTopologicalMergeStreaming(ctx), "a missing source must complete, not error")

	stats.mu.Lock()
	defer stats.mu.Unlock()
	require.Equal(t, int64(1), stats.sourceMissing, "the dropped source edge must land on the aggregate")
	require.Contains(t, stats.seen, source.GetId())
}
