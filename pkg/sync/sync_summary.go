package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"slices"
	"strings"
)

// topConnectorCallStatsByResourceType caps how many method:resource_type
// entries appear in the sync-complete log. The full set stays in the token.
const topConnectorCallStatsByResourceType = 8

// sessionStatsLogMinTotalMs is the aggregate session time below which idle
// session stats are omitted from logs (errors/timeouts always surface).
const sessionStatsLogMinTotalMs = 50

func isWaitStepBucket(bucket string) bool {
	switch {
	case bucket == "rate_limit_wait", bucket == "retry_wait":
		return true
	case strings.HasPrefix(bucket, "rate_limit_wait:"), strings.HasPrefix(bucket, "retry_wait:"):
		return true
	default:
		return false
	}
}

func timedSyncOpBuckets() map[string]struct{} {
	out := make(map[string]struct{}, len(timedSyncOps))
	for _, op := range timedSyncOps {
		out[op.String()] = struct{}{}
	}
	return out
}

// partitionStepDurationsForLog splits the token's step map into summable op
// buckets (matching sync_steps_total_ms), wait buckets (of-which retries),
// and everything else (checkpoint, connector-specific counters, …). Zero
// entries are dropped so logs stay compact.
func partitionStepDurationsForLog(all map[string]int64) (map[string]int64, map[string]int64, map[string]int64) {
	var ops, waits, other map[string]int64
	opSet := timedSyncOpBuckets()
	for bucket, ms := range all {
		if ms == 0 {
			continue
		}
		switch {
		case isWaitStepBucket(bucket):
			if waits == nil {
				waits = make(map[string]int64)
			}
			waits[bucket] = ms
		default:
			if _, ok := opSet[bucket]; ok {
				if ops == nil {
					ops = make(map[string]int64)
				}
				ops[bucket] = ms
				continue
			}
			if other == nil {
				other = make(map[string]int64)
			}
			other[bucket] = ms
		}
	}
	return ops, waits, other
}

// partitionConnectorCallStatsForLog returns flat method stats for the primary
// log field and the top-N method:resource_type entries by total_ms. Zero-count
// flats and zero-duration labeled entries are omitted.
func partitionConnectorCallStatsForLog(all map[string]ConnectorCallStat) (map[string]ConnectorCallStat, map[string]ConnectorCallStat) {
	type ranked struct {
		key  string
		stat ConnectorCallStat
	}
	var flat map[string]ConnectorCallStat
	var labeled []ranked
	for key, stat := range all {
		if strings.Contains(key, ":") {
			// Labeled noise like list-static-entitlements:user with 0ms.
			if stat.Count == 0 || (stat.TotalMs == 0 && stat.MaxMs == 0) {
				continue
			}
			labeled = append(labeled, ranked{key: key, stat: stat})
			continue
		}
		if stat.Count == 0 {
			continue
		}
		if flat == nil {
			flat = make(map[string]ConnectorCallStat)
		}
		flat[key] = stat
	}
	slices.SortFunc(labeled, func(a, b ranked) int {
		switch {
		case a.stat.TotalMs > b.stat.TotalMs:
			return -1
		case a.stat.TotalMs < b.stat.TotalMs:
			return 1
		case a.key < b.key:
			return -1
		case a.key > b.key:
			return 1
		default:
			return 0
		}
	})
	if n := topConnectorCallStatsByResourceType; len(labeled) > n {
		labeled = labeled[:n]
	}
	var topByRT map[string]ConnectorCallStat
	if len(labeled) > 0 {
		topByRT = make(map[string]ConnectorCallStat, len(labeled))
		for _, item := range labeled {
			topByRT[item.key] = item.stat
		}
	}
	return flat, topByRT
}

func sessionStatsDiverge(stats map[string]SessionStoreStat) bool {
	for op, cacheStat := range stats {
		suffix, ok := strings.CutPrefix(op, "connector.cache.")
		if !ok {
			continue
		}
		backend, ok := stats["connector."+suffix]
		if !ok {
			continue
		}
		delta := cacheStat.TotalMs - backend.TotalMs
		if delta < 0 {
			delta = -delta
		}
		threshold := cacheStat.TotalMs / 10
		if threshold < 5 {
			threshold = 5
		}
		if delta > threshold {
			return true
		}
	}
	return false
}

// filterSessionStatsForLog drops empty entries and reports whether the map is
// worth logging: errors/timeouts, material aggregate time, or cache-vs-backend
// divergence.
func filterSessionStatsForLog(all map[string]SessionStoreStat) (map[string]SessionStoreStat, bool) {
	if len(all) == 0 {
		return nil, false
	}
	var totalMs, errors, timeouts int64
	out := make(map[string]SessionStoreStat, len(all))
	for op, stat := range all {
		if stat.Count == 0 && stat.Errors == 0 && stat.Timeouts == 0 && stat.TotalMs == 0 {
			continue
		}
		out[op] = stat
		totalMs += stat.TotalMs
		errors += stat.Errors
		timeouts += stat.Timeouts
	}
	if len(out) == 0 {
		return nil, false
	}
	worth := errors > 0 || timeouts > 0 || totalMs >= sessionStatsLogMinTotalMs || sessionStatsDiverge(out)
	if !worth {
		return nil, false
	}
	return out, true
}

func aggregateSessionStats(all map[string]SessionStoreStat) (int64, int64, int64, int64, int64) {
	var count, errors, timeouts, totalMs, maxMs int64
	for _, stat := range all {
		count += stat.Count
		errors += stat.Errors
		timeouts += stat.Timeouts
		totalMs += stat.TotalMs
		if stat.MaxMs > maxMs {
			maxMs = stat.MaxMs
		}
	}
	return count, errors, timeouts, totalMs, maxMs
}
