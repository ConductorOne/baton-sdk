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
	// If syncID is empty, the latest finished sync of the given type is
	// used (NotFound when none exists). An explicit syncID may name an
	// in-progress sync; in that case the returned counts reflect whatever
	// has been written so far.
	// Mirrors the existing *C1File.Stats signature exactly.
	// Use StatsV2 for new code.
	Stats(ctx context.Context, syncType connectorstore.SyncType, syncID string) (map[string]int64, error)

	// StatsV2 returns structured stats for the given sync.
	// If syncID is empty, the latest finished sync of the given type is
	// used (NotFound when none exists). An explicit syncID may name an
	// in-progress sync; in that case the returned counts reflect whatever
	// has been written so far.
	StatsV2(ctx context.Context, syncType connectorstore.SyncType, syncID string) (*reader_v2.SyncStats, error)

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
	Stats        *reader_v2.SyncStats
}
