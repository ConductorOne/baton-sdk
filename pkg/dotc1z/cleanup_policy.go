package dotc1z

import (
	"os"
	"strconv"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// defaultCleanupSyncLimit is the SDK-wide default for the number of
// full syncs to retain when no caller-provided override is in play.
// Matches the historical SQLite default in (*C1File).Cleanup.
const defaultCleanupSyncLimit = 2

// SelectSyncsToDelete applies the SDK retention policy to a snapshot
// of sync runs and returns the IDs whose data should be deleted.
//
// Policy (matches (*C1File).Cleanup, the SQLite implementation that
// has shipped since the file format's introduction):
//
//   - Candidates with no EndedAt are skipped (unfinished syncs are
//     considered "in flight" and must never be pruned).
//   - currentSyncID is skipped when non-empty (the actively-open sync
//     is also off-limits).
//   - Candidates are bucketed by Type into fullSyncs, partials, and
//     diff syncs. SyncTypeFull and any unrecognized type go into
//     fullSyncs (matches the SQLite default branch).
//   - syncLimit is the number of *additional* full syncs to retain
//     beyond the current one. The caller has already decremented for
//     a running sync (see resolveSyncLimit), so this function trusts
//     the value verbatim. When fullSyncs > syncLimit, the oldest
//     overflow is selected for deletion.
//   - Once the earliest-kept full sync is established, partials that
//     ended before that sync started are selected for deletion.
//   - When more than two diff syncs (partial_upserts / partial_deletions)
//     exist, only the most recent diff sync and its linked pair are
//     retained; everything else is selected.
//
// Order matters: callers must pass candidates in oldest-first order
// so "drop the oldest overflow" trims the right end. SQLite supplies
// them ordered by autoincrement id; Pebble's IterateAllSyncRuns walks
// by KSUID (timestamp-sortable) which produces the same ordering for
// syncs created on this machine.
func SelectSyncsToDelete(candidates []SyncRun, currentSyncID string, syncLimit int) []string {
	var fullSyncs []SyncRun
	var partials []SyncRun
	var diffSyncs []SyncRun

	for _, sr := range candidates {
		if sr.EndedAt == nil || sr.ID == currentSyncID {
			continue
		}
		switch sr.Type {
		case connectorstore.SyncTypePartial, connectorstore.SyncTypeResourcesOnly:
			partials = append(partials, sr)
		case connectorstore.SyncTypePartialUpserts, connectorstore.SyncTypePartialDeletions:
			diffSyncs = append(diffSyncs, sr)
		default:
			fullSyncs = append(fullSyncs, sr)
		}
	}

	currentSyncOpen := currentSyncID != ""
	var toDelete []string

	// Full syncs beyond the retention limit + partials that pre-date
	// the earliest-kept full sync.
	if len(fullSyncs) > syncLimit {
		overflow := len(fullSyncs) - syncLimit
		for i := 0; i < overflow; i++ {
			toDelete = append(toDelete, fullSyncs[i].ID)
		}
		cutoffIdx := overflow
		if currentSyncOpen {
			cutoffIdx--
		}
		if cutoffIdx >= 0 && cutoffIdx < len(fullSyncs) {
			cutoff := fullSyncs[cutoffIdx]
			if cutoff.StartedAt != nil {
				for _, partial := range partials {
					if partial.EndedAt != nil && partial.EndedAt.Before(*cutoff.StartedAt) {
						toDelete = append(toDelete, partial.ID)
					}
				}
			}
		}
	}

	// Diff syncs: keep latest + its linked partner; drop the rest.
	// Mirrors the SQLite branch at sync_runs.go:884-931. The
	// "diffSyncs > 2" guard preserves the historical no-op behavior
	// for small histories — we don't prune until there's enough to
	// be worth touching.
	if len(diffSyncs) > 2 {
		syncByID := make(map[string]SyncRun, len(diffSyncs))
		for _, ds := range diffSyncs {
			syncByID[ds.ID] = ds
		}
		latestDiff := diffSyncs[len(diffSyncs)-1]
		keepIDs := map[string]struct{}{latestDiff.ID: {}}
		if latestDiff.LinkedSyncID != "" {
			if _, ok := syncByID[latestDiff.LinkedSyncID]; ok {
				keepIDs[latestDiff.LinkedSyncID] = struct{}{}
			}
		}
		for _, ds := range diffSyncs {
			if _, keep := keepIDs[ds.ID]; keep {
				continue
			}
			toDelete = append(toDelete, ds.ID)
		}
	}

	return toDelete
}

func ResolveCleanupSyncLimit(callerLimit int, currentSyncOpen bool) int {
	limit := defaultCleanupSyncLimit
	if callerLimit > 0 {
		limit = callerLimit
	} else if env, err := strconv.ParseInt(os.Getenv("BATON_KEEP_SYNC_COUNT"), 10, 64); err == nil && env > 0 {
		limit = int(env)
	}
	if currentSyncOpen && limit > 0 {
		limit--
	}
	return limit
}

// CleanupSkippedByEnv reports whether BATON_SKIP_CLEANUP is set to a
// truthy value. Callers should also honor any explicit
// caller-supplied "skip cleanup" option in addition to this env
// fallback.
func CleanupSkippedByEnv() bool {
	skip, _ := strconv.ParseBool(os.Getenv("BATON_SKIP_CLEANUP"))
	return skip
}
