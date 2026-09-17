package synccompactor

import (
	"context"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	sdksync "github.com/conductorone/baton-sdk/pkg/sync"
	mergepkg "github.com/conductorone/baton-sdk/pkg/synccompactor/pebble"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

// PartialCount keeps the true total, so a capped list is detectable.
const maxCompactionPartialIDs = 32

// Chained compactions carry the original StatsSyncID and the uncapped partial
// count forward.
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

func provenanceFromTokenSection(ctx context.Context, token string) *v3.CompactionProvenance {
	stats, err := sdksync.CompactionStatsFromToken(token) //nolint:staticcheck // reading the old artifact shape is the point
	if err != nil {
		ctxzap.Extract(ctx).Warn("compaction provenance: base token's compaction section unreadable; ancestry restarts here", zap.Error(err))
		return nil
	}
	if stats == nil {
		return nil
	}
	counts := make(map[string]*v3.CompactionRecordCounts, len(stats.RecordCounts))
	for k, c := range stats.RecordCounts {
		if c == nil {
			continue
		}
		counts[k] = v3.CompactionRecordCounts_builder{
			Output: c.Output, Added: c.Added, Replaced: c.Replaced, Carried: c.Carried,
		}.Build()
	}
	return v3.CompactionProvenance_builder{
		Mode:           stats.Mode,
		StatsSyncId:    stats.StatsSyncID,
		BaseSyncId:     stats.BaseSyncID,
		PartialSyncIds: append([]string(nil), stats.PartialSyncIDs...),
		PartialCount:   stats.PartialCount,
		RecordCounts:   counts,
	}.Build()
}
