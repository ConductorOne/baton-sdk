package synccompactor

import (
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	mergepkg "github.com/conductorone/baton-sdk/pkg/synccompactor/pebble"
)

// maxCompactionPartialIDs caps how many partial sync ids the provenance
// stores. PartialCount keeps the true total so a capped list is detectable.
const maxCompactionPartialIDs = 32

// buildCompactionProvenance describes one compaction run on the output's
// SyncStatsRecord. prior is the base input's provenance (nil unless the
// base was itself compacted): chained compactions carry the original
// StatsSyncID and the uncapped partial count forward and add the new
// partials on top.
func buildCompactionProvenance(
	prior *v3.CompactionProvenance,
	mode string,
	baseSyncID string,
	partialSyncIDs []string,
	counts map[string]*v3.CompactionRecordCounts,
) *v3.CompactionProvenance {
	comp := v3.CompactionProvenance_builder{
		Mode:        mode,
		StatsSyncId: baseSyncID,
		BaseSyncId:  baseSyncID,
	}.Build()
	if prior != nil {
		if prior.GetStatsSyncId() != "" {
			comp.SetStatsSyncId(prior.GetStatsSyncId())
		}
		comp.SetPartialCount(prior.GetPartialCount())
		comp.SetPartialSyncIds(append([]string(nil), prior.GetPartialSyncIds()...))
	}
	comp.SetPartialCount(comp.GetPartialCount() + int64(len(partialSyncIDs)))
	ids := comp.GetPartialSyncIds()
	for _, id := range partialSyncIDs {
		if len(ids) >= maxCompactionPartialIDs {
			break
		}
		ids = append(ids, id)
	}
	comp.SetPartialSyncIds(ids)
	if len(counts) > 0 {
		comp.SetRecordCounts(counts)
	}
	return comp
}

// overlayTimingStats copies from's timing / call stats onto into (the
// starting point of a compacted output's stats is its base's).
func overlayTimingStats(into, from *v3.SyncStatsRecord) {
	if into == nil || from == nil {
		return
	}
	if len(from.GetStepDurationsMs()) > 0 {
		into.SetStepDurationsMs(c1zstore.FoldDurations(nil, from.GetStepDurationsMs()))
	}
	if len(from.GetConnectorCallStats()) > 0 {
		into.SetConnectorCallStats(c1zstore.FoldCallStats(nil, from.GetConnectorCallStats()))
	}
	if len(from.GetSessionStoreStats()) > 0 {
		into.SetSessionStoreStats(c1zstore.FoldCallStats(nil, from.GetSessionStoreStats()))
	}
}

// foldPartialTimings adds one partial's timing / call stats into the
// compacted output's maps (approximate combined view). A nil partial —
// an input with no stats sidecar — contributes nothing.
func foldPartialTimings(into, partial *v3.SyncStatsRecord) {
	if into == nil || partial == nil {
		return
	}
	if src := partial.GetStepDurationsMs(); len(src) > 0 {
		into.SetStepDurationsMs(c1zstore.FoldDurations(into.GetStepDurationsMs(), src))
	}
	if src := partial.GetConnectorCallStats(); len(src) > 0 {
		into.SetConnectorCallStats(c1zstore.FoldCallStats(into.GetConnectorCallStats(), src))
	}
	if src := partial.GetSessionStoreStats(); len(src) > 0 {
		into.SetSessionStoreStats(c1zstore.FoldCallStats(into.GetSessionStoreStats(), src))
	}
}

// compactionRecordCounts renders per-type provenance counts. fold carries
// added/replaced attribution (fold mode only); rebuild modes pass nil and
// report output totals alone.
func compactionRecordCounts(output *v3.SyncStatsRecord, fold *mergepkg.FoldStats) map[string]*v3.CompactionRecordCounts {
	if output == nil {
		return nil
	}
	totals := map[string]int64{
		"resource_types": output.GetResourceTypes(),
		"resources":      output.GetResources(),
		"entitlements":   output.GetEntitlements(),
		"grants":         output.GetGrants(),
	}
	out := make(map[string]*v3.CompactionRecordCounts, len(totals))
	for bucket, total := range totals {
		counts := v3.CompactionRecordCounts_builder{Output: total}.Build()
		if fold != nil {
			added := fold.AddedByBucket[bucket]
			replaced := fold.ReplacedByBucket[bucket]
			carried := total - added - replaced
			if carried < 0 {
				carried = 0
			}
			counts.SetAdded(added)
			counts.SetReplaced(replaced)
			counts.SetCarried(carried)
		}
		out[bucket] = counts
	}
	return out
}
