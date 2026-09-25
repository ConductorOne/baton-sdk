package c1zstore

import "maps"

const (
	LedgerFactIngestKnown   = "sync.ingest_known"
	LedgerFactIngestBlocked = "sync.ingest_blocked"
	LedgerCompletedActions  = "actions.completed"
)

func LedgerSyncStats(facts map[string]string, counters LedgerCounters) SyncStats {
	_, blocked := facts[LedgerFactIngestBlocked]
	stats := SyncStats{
		Run: RunStats{
			CompletedActions:   counters.Counters[LedgerCompletedActions],
			StepDurationsMs:    maps.Clone(counters.StepDurationsMs),
			ConnectorCallStats: maps.Clone(counters.ConnectorCalls),
			SessionStoreStats:  maps.Clone(counters.SessionCalls),
		},
		IngestQuality: &IngestQuality{
			SourceCacheReplayBlocked: blocked, ReasonFlags: counters.Flags,
			EntitlementsDropped:           counters.Counters["ingest.entitlements_dropped"],
			GrantsDropped:                 counters.Counters["ingest.grants_dropped"],
			GrantResourcesDropped:         counters.Counters["ingest.grant_resources_dropped"],
			ExpansionResourceTypesDropped: counters.Counters["ingest.expansion_resource_types_dropped"],
			ExpansionsDropped:             counters.Counters["ingest.expansions_dropped"],
			InvalidResourceTypesObserved:  counters.Counters["ingest.invalid_resource_types_observed"],
			InvalidResourcesObserved:      counters.Counters["ingest.invalid_resources_observed"],
			InvalidEntitlementsObserved:   counters.Counters["ingest.invalid_entitlements_observed"],
		},
	}
	if _, known := facts[LedgerFactIngestKnown]; !known && !blocked {
		stats.IngestQuality = nil
	}
	return stats
}
