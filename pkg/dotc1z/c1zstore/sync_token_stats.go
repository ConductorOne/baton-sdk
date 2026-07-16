package c1zstore

import (
	"encoding/json"

	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
)

// syncTokenTiming is the subset of the syncer token JSON that EndSync
// lifts into SyncStats. Field names must stay aligned with
// pkg/sync.serializedTokenV1. Compacted syncs fold partial timings into
// these top-level maps at compaction time.
type syncTokenTiming struct {
	StepDurationsMs    map[string]int64          `json:"step_durations_ms,omitempty"`
	ConnectorCallStats map[string]*tokenCallStat `json:"connector_call_stats,omitempty"`
	SessionStoreStats  map[string]*tokenCallStat `json:"session_store_stats,omitempty"`
}

type tokenCallStat struct {
	Count    int64 `json:"count"`
	TotalMs  int64 `json:"total_ms"`
	MaxMs    int64 `json:"max_ms"`
	Errors   int64 `json:"errors,omitempty"`
	Timeouts int64 `json:"timeouts,omitempty"`
}

// ApplySyncTokenStats overlays timing / call stats from a syncer token
// onto a reader SyncStats. Intended for EndSync / stats-compute write
// paths that persist the result; cached stats are returned as stored.
// No-op when the token is empty or unparseable.
func ApplySyncTokenStats(stats *reader_v2.SyncStats, syncToken string) {
	if stats == nil || syncToken == "" {
		return
	}
	timing, ok := parseSyncTokenTiming(syncToken)
	if !ok {
		return
	}
	if len(timing.StepDurationsMs) > 0 {
		stats.SetStepDurationsMs(timing.StepDurationsMs)
	}
	if calls := toReaderCallStats(timing.ConnectorCallStats); len(calls) > 0 {
		stats.SetConnectorCallStats(calls)
	}
	if sessions := toReaderCallStats(timing.SessionStoreStats); len(sessions) > 0 {
		stats.SetSessionStoreStats(sessions)
	}
}

// ApplySyncTokenStatsRecord overlays timing / call stats from a syncer
// token onto a storage SyncStatsRecord before sidecar persist.
func ApplySyncTokenStatsRecord(rec *v3.SyncStatsRecord, syncToken string) {
	if rec == nil || syncToken == "" {
		return
	}
	timing, ok := parseSyncTokenTiming(syncToken)
	if !ok {
		return
	}
	if len(timing.StepDurationsMs) > 0 {
		rec.SetStepDurationsMs(timing.StepDurationsMs)
	}
	if calls := toStorageCallStats(timing.ConnectorCallStats); len(calls) > 0 {
		rec.SetConnectorCallStats(calls)
	}
	if sessions := toStorageCallStats(timing.SessionStoreStats); len(sessions) > 0 {
		rec.SetSessionStoreStats(sessions)
	}
}

func parseSyncTokenTiming(syncToken string) (syncTokenTiming, bool) {
	var timing syncTokenTiming
	if err := json.Unmarshal([]byte(syncToken), &timing); err != nil {
		return syncTokenTiming{}, false
	}
	if len(timing.StepDurationsMs) == 0 &&
		len(timing.ConnectorCallStats) == 0 &&
		len(timing.SessionStoreStats) == 0 {
		return syncTokenTiming{}, false
	}
	return timing, true
}

func toReaderCallStats(in map[string]*tokenCallStat) map[string]*reader_v2.CallStat {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]*reader_v2.CallStat, len(in))
	for k, v := range in {
		if v == nil {
			continue
		}
		out[k] = reader_v2.CallStat_builder{
			Count:    v.Count,
			TotalMs:  v.TotalMs,
			MaxMs:    v.MaxMs,
			Errors:   v.Errors,
			Timeouts: v.Timeouts,
		}.Build()
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func toStorageCallStats(in map[string]*tokenCallStat) map[string]*v3.CallStat {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]*v3.CallStat, len(in))
	for k, v := range in {
		if v == nil {
			continue
		}
		out[k] = v3.CallStat_builder{
			Count:    v.Count,
			TotalMs:  v.TotalMs,
			MaxMs:    v.MaxMs,
			Errors:   v.Errors,
			Timeouts: v.Timeouts,
		}.Build()
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
