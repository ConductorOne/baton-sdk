package expand

import (
	"context"
	"sync"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

// DroppedEdgeStats aggregates expansion edges dropped because a
// referenced entitlement has no row — connector magic-id bugs and
// disabled-by-default resource types. Production measured ~2.9M
// per-edge warnings a week from these sites, dominated by a few
// thousand distinct ids, so per-edge logging is demoted to Debug and
// ONE aggregated warning per sync (emitted by the syncer when
// expansion completes) carries the totals and a capped sample of
// distinct ids — the same reporting shape as the ingestion invariants'
// dangling-reference warnings.
//
// One instance lives on the syncer for the whole sync (the Expander is
// reconstructed every step); guarded for parallel use. In-memory only:
// a resume after a crash undercounts, which is acceptable for
// observability.
type DroppedEdgeStats struct {
	mu                 sync.Mutex
	sourceMissing      int64
	destinationMissing int64
	seen               map[string]struct{}
	examples           []string
}

const maxDroppedEdgeExamples = 25

func (s *DroppedEdgeStats) record(entitlementID string, dest bool, edges int64) {
	if s == nil || edges <= 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if dest {
		s.destinationMissing += edges
	} else {
		s.sourceMissing += edges
	}
	if s.seen == nil {
		s.seen = map[string]struct{}{}
	}
	if _, ok := s.seen[entitlementID]; ok {
		return
	}
	s.seen[entitlementID] = struct{}{}
	if len(s.examples) < maxDroppedEdgeExamples {
		s.examples = append(s.examples, entitlementID)
	}
}

// RecordSourceMissing counts ONE edge dropped because its SOURCE
// entitlement has no row.
func (s *DroppedEdgeStats) RecordSourceMissing(entitlementID string) {
	s.record(entitlementID, false, 1)
}

// RecordSourceMissingEdges counts a BATCH of edges sharing one missing
// source entitlement (the legacy expander drops a whole action's edges
// when the shared source is gone — the total must count every edge, not
// the batch).
func (s *DroppedEdgeStats) RecordSourceMissingEdges(entitlementID string, edges int) {
	s.record(entitlementID, false, int64(edges))
}

// RecordDestinationMissing counts ONE edge dropped because its
// DESTINATION entitlement has no row.
func (s *DroppedEdgeStats) RecordDestinationMissing(entitlementID string) {
	s.record(entitlementID, true, 1)
}

// RecordDestinationMissingEdges counts every incoming edge skipped when
// one destination entitlement has no row (the topological path skips the
// destination's whole reduction — all of its incoming edges — at once).
func (s *DroppedEdgeStats) RecordDestinationMissingEdges(entitlementID string, edges int) {
	s.record(entitlementID, true, int64(edges))
}

// LogSummary emits the one aggregated warning for the sync, if any
// edges were dropped. Safe on nil.
func (s *DroppedEdgeStats) LogSummary(ctx context.Context) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sourceMissing == 0 && s.destinationMissing == 0 {
		return
	}
	ctxzap.Extract(ctx).Warn("grant expansion: DROPPED edges referencing entitlements with no entitlement row (connector bug or resource type not enabled — check magic-id construction)",
		zap.Int64("edges_missing_source_entitlement", s.sourceMissing),
		zap.Int64("edges_missing_destination_entitlement", s.destinationMissing),
		zap.Int("distinct_missing_entitlements", len(s.seen)),
		zap.Strings("entitlement_id_examples", s.examples),
	)
}
