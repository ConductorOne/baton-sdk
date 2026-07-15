package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

// Source-cache counters, following the sync-stats model (see the stats
// fields on state and syncSummaryFields): recorded through the state so
// they persist in the checkpointed sync token — a warm sync that suspends
// and resumes keeps its counts — and reported once in the sync summary.

import (
	"context"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

// SourceCacheStats accumulates per-sync source-cache counters: replay
// effectiveness, delta tombstones, spawn fan-out, and the lookup
// continuation (ask/answer) protocol. All fields are cumulative for the
// sync; the zero value means "feature unused".
type SourceCacheStats struct {
	// ScopesReplayed counts pages answered with SourceCacheReplay. With
	// ScopesStamped it gives the sync's replay hit ratio — the headline
	// warm-sync health metric.
	ScopesReplayed int64 `json:"scopes_replayed,omitempty"`
	// ScopesStamped counts manifest entries written for NON-replayed
	// (cold/fresh) pages. Interim pages with no validator count in
	// neither bucket.
	ScopesStamped int64 `json:"scopes_stamped,omitempty"`
	// RowsReplayed counts rows bulk-copied from the previous sync, keyed
	// by row kind (resources / entitlements / grants).
	RowsReplayed map[string]int64 `json:"rows_replayed,omitempty"`
	// OverlayRows counts connector-emitted rows upserted on top of a
	// replayed base (delta adds/changes).
	OverlayRows int64 `json:"overlay_rows,omitempty"`

	// TombstoneIDs / TombstonePrincipals count tombstones received on
	// replay/scope annotations; RowsDeleted counts rows they removed
	// (where the delete path reports it). A delta connector applying zero
	// tombstones for weeks is a signal worth charting.
	TombstoneIDs        int64 `json:"tombstone_ids,omitempty"`
	TombstonePrincipals int64 `json:"tombstone_principals,omitempty"`
	RowsDeleted         int64 `json:"rows_deleted,omitempty"`

	// SpawnedCursors counts sibling actions enqueued via EnqueuePageTokens
	// (pairs with the per-response cap).
	SpawnedCursors int64 `json:"spawned_cursors,omitempty"`

	// Lookup continuation (ask/answer) counters. BouncesByOp splits
	// bounces by list-op kind, distinguishing planner asks (expected: one
	// per planning page) from per-row asks (a batching opportunity).
	// AnswersTruncated counts found answers degraded to not-found (cold
	// fetch) because their etags did not fit the per-request answer size
	// budget; nonzero means a connector is batching more fat-token scopes
	// per ask than the transport can carry and should shard via
	// EnqueuePageTokens.
	LookupBounces          int64            `json:"lookup_bounces,omitempty"`
	LookupRequestsBounced  int64            `json:"lookup_requests_bounced,omitempty"`
	LookupScopesAsked      int64            `json:"lookup_scopes_asked,omitempty"`
	LookupAnsweredFound    int64            `json:"lookup_answered_found,omitempty"`
	LookupAnsweredNotFound int64            `json:"lookup_answered_not_found,omitempty"`
	LookupAnswersTruncated int64            `json:"lookup_answers_truncated,omitempty"`
	LookupCapFailures      int64            `json:"lookup_cap_failures,omitempty"`
	LookupBouncesByOp      map[string]int64 `json:"lookup_bounces_by_op,omitempty"`
}

func (s *SourceCacheStats) merge(delta SourceCacheStats) {
	s.ScopesReplayed += delta.ScopesReplayed
	s.ScopesStamped += delta.ScopesStamped
	for kind, n := range delta.RowsReplayed {
		if s.RowsReplayed == nil {
			s.RowsReplayed = make(map[string]int64, len(delta.RowsReplayed))
		}
		s.RowsReplayed[kind] += n
	}
	s.OverlayRows += delta.OverlayRows
	s.TombstoneIDs += delta.TombstoneIDs
	s.TombstonePrincipals += delta.TombstonePrincipals
	s.RowsDeleted += delta.RowsDeleted
	s.SpawnedCursors += delta.SpawnedCursors
	s.LookupBounces += delta.LookupBounces
	s.LookupRequestsBounced += delta.LookupRequestsBounced
	s.LookupScopesAsked += delta.LookupScopesAsked
	s.LookupAnsweredFound += delta.LookupAnsweredFound
	s.LookupAnsweredNotFound += delta.LookupAnsweredNotFound
	s.LookupAnswersTruncated += delta.LookupAnswersTruncated
	s.LookupCapFailures += delta.LookupCapFailures
	for op, n := range delta.LookupBouncesByOp {
		if s.LookupBouncesByOp == nil {
			s.LookupBouncesByOp = make(map[string]int64, len(delta.LookupBouncesByOp))
		}
		s.LookupBouncesByOp[op] += n
	}
}

// copy returns a deep copy (maps included).
func (s *SourceCacheStats) copy() *SourceCacheStats {
	out := *s
	if s.RowsReplayed != nil {
		out.RowsReplayed = make(map[string]int64, len(s.RowsReplayed))
		for k, v := range s.RowsReplayed {
			out.RowsReplayed[k] = v
		}
	}
	if s.LookupBouncesByOp != nil {
		out.LookupBouncesByOp = make(map[string]int64, len(s.LookupBouncesByOp))
		for k, v := range s.LookupBouncesByOp {
			out.LookupBouncesByOp[k] = v
		}
	}
	return &out
}

func (st *state) AddSourceCacheStats(delta SourceCacheStats) {
	st.mtx.Lock()
	defer st.mtx.Unlock()
	if st.sourceCacheStats == nil {
		st.sourceCacheStats = &SourceCacheStats{}
	}
	st.sourceCacheStats.merge(delta)
}

func (st *state) SourceCacheStatsSnapshot() *SourceCacheStats {
	st.mtx.RLock()
	defer st.mtx.RUnlock()
	if st.sourceCacheStats == nil {
		return nil
	}
	return st.sourceCacheStats.copy()
}

// logSourceCacheStats emits the source-cache summary line when the
// feature saw any use. Deliberately NOT gated on recordStats: replay is
// opt-in per connector and these counters are its rollout telemetry (the
// replay hit ratio, and the bounce counts the consumer briefs chart).
func (s *syncer) logSourceCacheStats(ctx context.Context) {
	if s.state == nil {
		return
	}
	stats := s.state.SourceCacheStatsSnapshot()
	if stats == nil {
		return
	}
	ctxzap.Extract(ctx).Info("source cache stats", zap.Any("source_cache_stats", stats))
}
