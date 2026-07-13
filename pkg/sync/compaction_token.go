package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import "encoding/json"

// maxCompactionPartialIDs caps how many partial sync ids a compacted token
// stores. PartialCount keeps the true total so a capped list is detectable.
const maxCompactionPartialIDs = 32

// CompactionRecordCounts describes one record type's provenance in a
// compacted output. Output is the record count in the compacted artifact.
// Added counts records admitted with no incumbent, Replaced counts records
// that overrode a strictly-older incumbent, and Carried counts incumbents
// that survived untouched. Added/Replaced/Carried are only populated by the
// fold strategy — rebuild merges lose per-source attribution in their
// run-file paths, so rebuild outputs carry Output only.
type CompactionRecordCounts struct {
	Output   int64 `json:"output"`
	Added    int64 `json:"added,omitempty"`
	Replaced int64 `json:"replaced,omitempty"`
	Carried  int64 `json:"carried,omitempty"`
}

// CompactionAggregate sums timing stats across the partial syncs merged into
// a compacted output. The values describe a population of Executions distinct
// sync runs — never one execution — and accumulate across chained folds.
type CompactionAggregate struct {
	Executions         int64                         `json:"executions"`
	StepDurationsMs    map[string]int64              `json:"step_durations_ms,omitempty"`
	ConnectorCallStats map[string]*ConnectorCallStat `json:"connector_call_stats,omitempty"`
}

// CompactionTokenStats is the compaction provenance section of a compacted
// sync token. Its presence re-attributes the token's top-level timing stats:
// they describe StatsSyncID's collection run, not the compacted artifact.
type CompactionTokenStats struct {
	// Mode is the compaction strategy that produced this artifact
	// (fold / overlay / kway).
	Mode string `json:"mode"`
	// StatsSyncID is the sync whose execution the token's top-level
	// step_durations_ms / connector_call_stats describe. It is carried
	// unchanged across chained folds so the attribution stays truthful.
	StatsSyncID string `json:"stats_sync_id,omitempty"`
	// BaseSyncID is the immediate base input of this compaction run (the id
	// the artifact carried before the rename).
	BaseSyncID string `json:"base_sync_id,omitempty"`
	// PartialSyncIDs lists merged partial sync ids, capped at
	// maxCompactionPartialIDs. PartialCount is the uncapped total and
	// accumulates across chained folds.
	PartialSyncIDs []string `json:"partial_sync_ids,omitempty"`
	PartialCount   int64    `json:"partial_count"`
	// RecordCounts is keyed by record type (resource_types, resources,
	// entitlements, grants).
	RecordCounts map[string]*CompactionRecordCounts `json:"record_counts,omitempty"`
	// PartialsAggregate sums the merged partials' timing stats; nil when no
	// partial carried stats.
	PartialsAggregate *CompactionAggregate `json:"partials_aggregate,omitempty"`
}

// CompactionTokenInput carries one compaction run's provenance into
// BuildCompactedToken.
type CompactionTokenInput struct {
	Mode           string
	BaseSyncID     string
	PartialSyncIDs []string
	// PartialTokens are the partials' marshalled sync tokens, used for the
	// timing aggregate. Entries may be empty or unparseable (e.g. converted
	// sqlite inputs carry no token); those contribute no stats.
	PartialTokens []string
	RecordCounts  map[string]CompactionRecordCounts
}

// BuildCompactedToken rewrites a compacted output's sync token with a
// compaction provenance section. baseToken is the base input's token ("" for
// rebuild outputs, which start empty); its resume state, skip flags, and
// timing stats are preserved. Chained compactions accumulate: the original
// StatsSyncID, the uncapped partial count, and the partials aggregate carry
// forward from an existing compaction section.
func BuildCompactedToken(baseToken string, in CompactionTokenInput) (string, error) {
	st := newState()
	if baseToken != "" {
		if err := st.Unmarshal(baseToken); err != nil {
			return "", err
		}
	} else {
		// Skip Unmarshal("") — it seeds an InitOp action to drive a fresh
		// sync, which a finished compacted output must not carry.
		st.actions = make(map[string]Action)
		st.actionOrder = []string{}
	}

	comp := &CompactionTokenStats{
		Mode:        in.Mode,
		StatsSyncID: in.BaseSyncID,
		BaseSyncID:  in.BaseSyncID,
	}
	if prior := st.compaction; prior != nil {
		if prior.StatsSyncID != "" {
			comp.StatsSyncID = prior.StatsSyncID
		}
		comp.PartialCount = prior.PartialCount
		comp.PartialSyncIDs = append(comp.PartialSyncIDs, prior.PartialSyncIDs...)
		if prior.PartialsAggregate != nil {
			comp.PartialsAggregate = prior.PartialsAggregate
		}
	}

	comp.PartialCount += int64(len(in.PartialSyncIDs))
	for _, id := range in.PartialSyncIDs {
		if len(comp.PartialSyncIDs) >= maxCompactionPartialIDs {
			break
		}
		comp.PartialSyncIDs = append(comp.PartialSyncIDs, id)
	}

	for _, token := range in.PartialTokens {
		aggregatePartialToken(comp, token)
	}

	if len(in.RecordCounts) > 0 {
		comp.RecordCounts = make(map[string]*CompactionRecordCounts, len(in.RecordCounts))
		for name, counts := range in.RecordCounts {
			c := counts
			comp.RecordCounts[name] = &c
		}
	}

	st.compaction = comp
	return st.Marshal()
}

// aggregatePartialToken folds one partial's timing stats into the aggregate.
// Unparseable or stat-less tokens contribute nothing; parse errors are not
// surfaced because provenance must never fail a compaction.
func aggregatePartialToken(comp *CompactionTokenStats, token string) {
	if token == "" {
		return
	}
	partial := newState()
	if err := partial.Unmarshal(token); err != nil {
		return
	}
	durations := partial.StepDurations()
	calls := partial.ConnectorCallStats()
	if len(durations) == 0 && len(calls) == 0 {
		return
	}

	if comp.PartialsAggregate == nil {
		comp.PartialsAggregate = &CompactionAggregate{}
	}
	agg := comp.PartialsAggregate
	agg.Executions++
	if len(durations) > 0 && agg.StepDurationsMs == nil {
		agg.StepDurationsMs = make(map[string]int64, len(durations))
	}
	for bucket, ms := range durations {
		agg.StepDurationsMs[bucket] += ms
	}
	if len(calls) > 0 && agg.ConnectorCallStats == nil {
		agg.ConnectorCallStats = make(map[string]*ConnectorCallStat, len(calls))
	}
	for method, stat := range calls {
		merged := agg.ConnectorCallStats[method]
		if merged == nil {
			merged = &ConnectorCallStat{}
			agg.ConnectorCallStats[method] = merged
		}
		merged.Count += stat.Count
		merged.TotalMs += stat.TotalMs
		if stat.MaxMs > merged.MaxMs {
			merged.MaxMs = stat.MaxMs
		}
	}
}

// CompactionStatsFromToken returns the compaction provenance section of a
// marshalled sync token, or nil when the token is empty or carries none.
func CompactionStatsFromToken(token string) (*CompactionTokenStats, error) {
	if token == "" {
		return nil, nil
	}
	st := newState()
	if err := st.Unmarshal(token); err != nil {
		return nil, err
	}
	if st.compaction == nil {
		return nil, nil
	}
	out, err := json.Marshal(st.compaction)
	if err != nil {
		return nil, err
	}
	copied := &CompactionTokenStats{}
	if err := json.Unmarshal(out, copied); err != nil {
		return nil, err
	}
	return copied, nil
}
