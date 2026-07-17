package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"encoding/json"
	"time"
)

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

// CompactionTokenStats is the compaction provenance section of a compacted
// sync token. Timing stats for merged partials are folded into the token's
// top-level step_durations_ms / connector_call_stats / session_store_stats
// (approximate combined view); this section carries identity / record-count
// provenance only.
type CompactionTokenStats struct {
	// Mode is the compaction strategy that produced this artifact
	// (fold / overlay / kway).
	Mode string `json:"mode"`
	// StatsSyncID is the original collection sync at the root of a fold
	// chain. Carried unchanged across chained folds. Top-level timings are
	// the approximate sum of that sync plus merged partials — not solely
	// this sync's execution.
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
}

// CompactionTokenInput carries one compaction run's provenance into
// BuildCompactedToken.
type CompactionTokenInput struct {
	Mode           string
	BaseSyncID     string
	PartialSyncIDs []string
	// PartialTokens are the partials' marshalled sync tokens. Their timing
	// stats are folded into the compacted token's top-level maps. Entries
	// may be empty or unparseable (e.g. converted sqlite inputs carry no
	// token); those contribute no stats.
	PartialTokens []string
	RecordCounts  map[string]CompactionRecordCounts
}

// BuildCompactedToken rewrites a compacted output's sync token with a
// compaction provenance section and folds each partial's timing stats into
// the top-level maps. baseToken is the base input's token ("" for rebuild
// outputs, which start empty); its resume state, skip flags, and timing
// stats are preserved as the starting point. Chained compactions accumulate:
// the original StatsSyncID, the uncapped partial count, and already-folded
// top-level timings carry forward; new partials are added on top.
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
	}

	comp.PartialCount += int64(len(in.PartialSyncIDs))
	for _, id := range in.PartialSyncIDs {
		if len(comp.PartialSyncIDs) >= maxCompactionPartialIDs {
			break
		}
		comp.PartialSyncIDs = append(comp.PartialSyncIDs, id)
	}

	for _, token := range in.PartialTokens {
		foldPartialTimings(st, token)
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

// foldPartialTimings adds one partial's timing stats into the compacted
// token's top-level maps. Unparseable or stat-less tokens contribute
// nothing; parse errors are not surfaced because provenance must never
// fail a compaction.
func foldPartialTimings(st *state, token string) {
	if token == "" {
		return
	}
	partial := newState()
	if err := partial.Unmarshal(token); err != nil {
		return
	}
	for bucket, ms := range partial.StepDurations() {
		if ms == 0 {
			continue
		}
		st.AddStepDuration(bucket, time.Duration(ms)*time.Millisecond)
	}
	for method, stat := range partial.ConnectorCallStats() {
		st.MergeConnectorCallStat(method, stat)
	}
	for op, stat := range partial.SessionStoreStats() {
		st.MergeSessionStat(op, stat)
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
