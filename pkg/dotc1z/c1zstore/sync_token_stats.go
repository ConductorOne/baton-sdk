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
	IngestQuality      *tokenIngestQuality       `json:"ingest_quality,omitempty"`
}

type tokenCallStat struct {
	Count    int64 `json:"count"`
	TotalMs  int64 `json:"total_ms"`
	MaxMs    int64 `json:"max_ms"`
	Errors   int64 `json:"errors,omitempty"`
	Timeouts int64 `json:"timeouts,omitempty"`
}

type tokenIngestQuality struct {
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
	if quality := toStorageIngestQuality(timing.IngestQuality); quality != nil {
		rec.SetIngestQuality(quality)
	}
}

// SourceCacheReplayEligible fails closed for legacy and compacted artifacts:
// both omit ingest_quality. Only a quality-aware original sync that completed
// without a replay-blocking ingestion defect is eligible.
func SourceCacheReplayEligible(rec *v3.SyncStatsRecord) bool {
	quality := rec.GetIngestQuality()
	return quality != nil && !quality.GetSourceCacheReplayBlocked()
}

func parseSyncTokenTiming(syncToken string) (syncTokenTiming, bool) {
	var timing syncTokenTiming
	if err := json.Unmarshal([]byte(syncToken), &timing); err != nil {
		return syncTokenTiming{}, false
	}
	if len(timing.StepDurationsMs) == 0 &&
		len(timing.ConnectorCallStats) == 0 &&
		len(timing.SessionStoreStats) == 0 &&
		timing.IngestQuality == nil {
		return syncTokenTiming{}, false
	}
	return timing, true
}

func toStorageIngestQuality(in *tokenIngestQuality) *v3.IngestQualityStats {
	if in == nil {
		return nil
	}
	return v3.IngestQualityStats_builder{
		SourceCacheReplayBlocked:      in.SourceCacheReplayBlocked,
		EntitlementsDropped:           in.EntitlementsDropped,
		GrantsDropped:                 in.GrantsDropped,
		GrantResourcesDropped:         in.GrantResourcesDropped,
		ExpansionResourceTypesDropped: in.ExpansionResourceTypesDropped,
		ExpansionsDropped:             in.ExpansionsDropped,
		InvalidResourceTypesObserved:  in.InvalidResourceTypesObserved,
		InvalidResourcesObserved:      in.InvalidResourcesObserved,
		InvalidEntitlementsObserved:   in.InvalidEntitlementsObserved,
		ReasonFlags:                   in.ReasonFlags,
	}.Build()
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
