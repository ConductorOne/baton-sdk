package expand

// DroppedEdgeStats counting semantics: totals count EDGES, not drop
// events. The legacy expander drops a whole action's edge batch when the
// shared SOURCE entitlement is missing, and the topological path skips a
// destination's entire reduction (every incoming edge) when the
// DESTINATION is missing — both previously incremented the aggregate
// once per event, undercounting the very numbers the per-sync warning
// exists to report.

import (
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
