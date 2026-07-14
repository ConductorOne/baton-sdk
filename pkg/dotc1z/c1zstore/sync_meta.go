package c1zstore

import (
	"context"
	"time"

	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// SyncMeta is the sync-run-metadata sub-store of Store. It covers
// operations that read/write the sync_runs table without going through the
// gRPC Reader/Writer surfaces.
//
// All methods are callable without an active sync.
type SyncMeta interface {
	// MarkSyncSupportsDiff sets the supports_diff flag on the given sync.
	// Called by pkg/sync.parallelSyncer after graph construction to signal
	// that the sync run has SQL-layer grant metadata populated and diff
	// consumers may rely on it.
	MarkSyncSupportsDiff(ctx context.Context, syncID string) error

	// LatestFullSync returns the most-recently-finished SyncTypeFull sync
	// run, or nil if none exists.
	LatestFullSync(ctx context.Context) (*SyncRun, error)

	// LatestFinishedSyncOfAnyType returns the most-recently-finished sync
	// of any type (including diff types), or nil if none exists. Used by
	// tooling that wants to inspect whatever sync finished last regardless
	// of type.
	LatestFinishedSyncOfAnyType(ctx context.Context) (*SyncRun, error)

	// Stats returns a map of table-name to row-count for the given sync.
	// If syncID is empty, the latest sync of the given type is used.
	// Mirrors the existing *C1File.Stats signature exactly.
	Stats(ctx context.Context, syncType connectorstore.SyncType, syncID string) (map[string]int64, error)

	// RecalculateStats recomputes the cached stats for the given sync from
	// the underlying records and persists them, discarding any previously
	// cached value. The store must be writable. Used by tooling (e.g. the
	// `baton recalculate-stats` command) to refresh stale or missing stats.
	RecalculateStats(ctx context.Context, syncID string) error
}

// SyncRun is the exported shape of a sync run. The fields match the
// sync_runs schema.
//
// Callers typically only read ID, Type, and the timestamps; the rest is
// included for completeness and for use by tooling (e.g. sync-diff
// pipelines need ParentSyncID and LinkedSyncID).
type SyncRun struct {
	ID           string
	StartedAt    *time.Time
	EndedAt      *time.Time
	SyncToken    string
	Type         connectorstore.SyncType
	ParentSyncID string
	LinkedSyncID string
	SupportsDiff bool
	// Compacted marks a sync produced by compaction rather than a real
	// connector run. Compacted artifacts are keep-newer upsert merges, so
	// no input's source-cache validators (etags) describe their contents:
	// the syncer refuses to use a compacted sync as a replay source
	// (degrades to a cold sync), and orchestrators choosing which
	// artifact to materialize as the previous sync should read
	// UsableAsReplaySource instead of guessing from provenance.
	// Always false on the SQLite engine (v1 files cannot be replay
	// sources regardless).
	Compacted bool
	Stats     *reader_v2.SyncStats
}

// UsableAsReplaySource answers "can this sync serve source-cache replay?"
// — the question an orchestrator must ask before materializing an
// artifact as a syncer's previous sync. The predicate is deliberately
// narrow: only a FULL, non-compacted sync is a faithful snapshot that its
// own recorded validators describe.
//
//   - Partial/targeted, resources-only, and diff-typed syncs are subsets
//     or derivations — their rows do not cover the scopes any validator
//     would vouch for (they also never write manifest entries, but the
//     type gate makes the refusal explicit rather than incidental).
//   - Compacted syncs are keep-newer merges of multiple runs; no single
//     input's validators describe the merged row set.
//
// The syncer enforces this same predicate when a previous-sync c1z is
// supplied (see pkg/sync's configureSourceCache): a non-qualifying file
// degrades to a cold sync rather than replaying.
func (s *SyncRun) UsableAsReplaySource() bool {
	return s != nil && s.Type == connectorstore.SyncTypeFull && !s.Compacted
}
