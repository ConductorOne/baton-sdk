package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"sync"
	"time"
)

// ConnectorCallStat contains cumulative latency statistics for one connector method.
type ConnectorCallStat struct {
	Count   int64 `json:"count"`
	TotalMs int64 `json:"total_ms"`
	MaxMs   int64 `json:"max_ms"`
}

// SessionStoreStat contains cumulative latency and outcome counters for one
// session-store operation. Timeouts is the deadline-exceeded subset of
// Errors; MaxMs pinned at a fixed value with Timeouts ≈ Count is the
// signature of a backend whose every request times out.
type SessionStoreStat struct {
	Count    int64 `json:"count"`
	Errors   int64 `json:"errors,omitempty"`
	Timeouts int64 `json:"timeouts,omitempty"`
	TotalMs  int64 `json:"total_ms"`
	MaxMs    int64 `json:"max_ms"`
}

// IngestQualityCheckpoint is the checkpointed connector-ingestion quality
// summary. A nil value means legacy/unknown provenance, not a clean sync.
type IngestQualityCheckpoint struct {
	SourceCacheReplayBlocked      bool   `json:"source_cache_replay_blocked,omitempty"`
	EntitlementsDropped           uint64 `json:"entitlements_dropped,omitempty"`
	GrantsDropped                 uint64 `json:"grants_dropped,omitempty"`
	GrantResourcesDropped         uint64 `json:"grant_resources_dropped,omitempty"`
	ExpansionResourceTypesDropped uint64 `json:"expansion_resource_types_dropped,omitempty"`
	ExpansionsDropped             uint64 `json:"expansions_dropped,omitempty"`
	InvalidResourceTypesObserved  uint64 `json:"invalid_resource_types_observed,omitempty"`
	InvalidResourcesObserved      uint64 `json:"invalid_resources_observed,omitempty"`
	InvalidEntitlementsObserved   uint64 `json:"invalid_entitlements_observed,omitempty"`
	ReasonFlags                   uint64 `json:"reason_flags,omitempty"`
}

// runStats is a sync run's best-effort accounting: where wall time went,
// what the connector and session store cost, how clean the ingestion was,
// and — for a compacted artifact — which inputs produced it.
//
// Parallel workers write these counters from their own goroutines, so it
// carries its own mutex. It is held apart from runState because none of it
// steers the action stack, not because it is less durable — it rides the same
// token. The field with reach past this type is ingest, which Sync restores
// into ingestFilterStats on resume, where SourceCacheReplayBlocked decides
// whether a later sync may replay from this artifact.
type runStats struct {
	mu              sync.RWMutex
	stepDurationsMs map[string]int64
	connectorCalls  map[string]*ConnectorCallStat
	sessionOps      map[string]*SessionStoreStat
	ingest          *IngestQualityCheckpoint
	// compaction is provenance written by the sync compactor via
	// BuildCompactedToken; the syncer itself never sets it. Held here so
	// decoding and re-encoding a token (e.g. an expansion replay token)
	// preserves it.
	compaction *CompactionTokenStats
}

func newRunStats() *runStats {
	return &runStats{
		stepDurationsMs: make(map[string]int64),
		connectorCalls:  make(map[string]*ConnectorCallStat),
		sessionOps:      make(map[string]*SessionStoreStat),
	}
}

func (s *runStats) addStepDuration(bucket string, duration time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.stepDurationsMs == nil {
		s.stepDurationsMs = make(map[string]int64)
	}
	s.stepDurationsMs[bucket] += duration.Milliseconds()
}

func (s *runStats) stepDurations() map[string]int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make(map[string]int64, len(s.stepDurationsMs))
	for bucket, duration := range s.stepDurationsMs {
		out[bucket] = duration
	}
	return out
}

func (s *runStats) recordConnectorCall(method string, duration time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.connectorCalls == nil {
		s.connectorCalls = make(map[string]*ConnectorCallStat)
	}
	stat := s.connectorCalls[method]
	if stat == nil {
		stat = &ConnectorCallStat{}
		s.connectorCalls[method] = stat
	}
	durationMs := duration.Milliseconds()
	stat.Count++
	stat.TotalMs += durationMs
	if durationMs > stat.MaxMs {
		stat.MaxMs = durationMs
	}
}

// mergeConnectorCallStat folds pre-aggregated connector-call stats (e.g. a
// compacted partial's totals) into method's cumulative counters.
func (s *runStats) mergeConnectorCallStat(method string, add ConnectorCallStat) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.connectorCalls == nil {
		s.connectorCalls = make(map[string]*ConnectorCallStat)
	}
	stat := s.connectorCalls[method]
	if stat == nil {
		stat = &ConnectorCallStat{}
		s.connectorCalls[method] = stat
	}
	stat.Count += add.Count
	stat.TotalMs += add.TotalMs
	if add.MaxMs > stat.MaxMs {
		stat.MaxMs = add.MaxMs
	}
}

func (s *runStats) connectorCallStats() map[string]ConnectorCallStat {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make(map[string]ConnectorCallStat, len(s.connectorCalls))
	for method, stat := range s.connectorCalls {
		if stat != nil {
			out[method] = *stat
		}
	}
	return out
}

func (s *runStats) recordSessionOp(op string, duration time.Duration, opErr error, timedOut bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.sessionOps == nil {
		s.sessionOps = make(map[string]*SessionStoreStat)
	}
	stat := s.sessionOps[op]
	if stat == nil {
		stat = &SessionStoreStat{}
		s.sessionOps[op] = stat
	}
	durationMs := duration.Milliseconds()
	stat.Count++
	stat.TotalMs += durationMs
	if durationMs > stat.MaxMs {
		stat.MaxMs = durationMs
	}
	if opErr != nil {
		stat.Errors++
		if timedOut {
			stat.Timeouts++
		}
	}
}

// mergeSessionStat folds pre-aggregated session stats (e.g. a connector's
// per-request usage report) into op's cumulative counters.
func (s *runStats) mergeSessionStat(op string, add SessionStoreStat) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.sessionOps == nil {
		s.sessionOps = make(map[string]*SessionStoreStat)
	}
	stat := s.sessionOps[op]
	if stat == nil {
		stat = &SessionStoreStat{}
		s.sessionOps[op] = stat
	}
	stat.Count += add.Count
	stat.Errors += add.Errors
	stat.Timeouts += add.Timeouts
	stat.TotalMs += add.TotalMs
	if add.MaxMs > stat.MaxMs {
		stat.MaxMs = add.MaxMs
	}
}

func (s *runStats) sessionStoreStats() map[string]SessionStoreStat {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make(map[string]SessionStoreStat, len(s.sessionOps))
	for op, stat := range s.sessionOps {
		if stat != nil {
			out[op] = *stat
		}
	}
	return out
}

func (s *runStats) setIngestQuality(quality *IngestQualityCheckpoint) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ingest = cloneIngestQualityCheckpoint(quality)
}

func (s *runStats) ingestQuality() *IngestQualityCheckpoint {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return cloneIngestQualityCheckpoint(s.ingest)
}

func (s *runStats) setCompaction(stats *CompactionTokenStats) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.compaction = stats
}

func (s *runStats) compactionStats() *CompactionTokenStats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.compaction
}

func cloneIngestQualityCheckpoint(in *IngestQualityCheckpoint) *IngestQualityCheckpoint {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}
