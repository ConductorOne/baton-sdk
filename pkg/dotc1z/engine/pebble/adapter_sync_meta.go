package pebble

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/cockroachdb/pebble/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// SyncMeta returns the SyncMeta sub-store backed by the Pebble
// adapter. Implements c1zstore.Store.SyncMeta(). The sync-run
// records live in Pebble under typeSyncRun; methods here walk
// IterateAllSyncRuns + GetSyncRunRecord + PutSyncRunRecord on the
// engine.
func (e *Engine) SyncMeta() c1zstore.SyncMeta {
	return pebbleSyncMeta{e: e}
}

type pebbleSyncMeta struct {
	e *Engine
}

// MarkSyncSupportsDiff sets supports_diff = true on the named sync's
// run record. Used by pkg/sync.parallelSyncer after graph
// construction to signal that the sync has SQL-layer grant metadata
// populated and diff consumers may rely on it.
//
// FileOps().GenerateSyncDiff and FileOps().CloneSync are implemented
// on the Pebble adapter (see adapter_diff.go and
// adapter_clone_sync.go); the bit stored here gates downstream
// consumers' willingness to call them, in parity with the SQLite
// sync_runs.supports_diff column.
func (s pebbleSyncMeta) MarkSyncSupportsDiff(ctx context.Context, syncID string) error {
	if syncID == "" {
		return errors.New("MarkSyncSupportsDiff: empty syncID")
	}
	r, err := s.e.GetSyncRunRecord(ctx, syncID)
	if err != nil {
		return c1zstore.AdaptNotFound(fmt.Errorf("MarkSyncSupportsDiff: get: %w", err), pebble.ErrNotFound)
	}
	r.SetSupportsDiff(true)
	if err := s.e.PutSyncRunRecord(ctx, r); err != nil {
		return fmt.Errorf("MarkSyncSupportsDiff: put: %w", err)
	}
	return nil
}

// LatestFullSync returns the most recently finished sync whose type
// is SYNC_TYPE_FULL, or nil if no such sync exists.
func (s pebbleSyncMeta) LatestFullSync(ctx context.Context) (*c1zstore.SyncRun, error) {
	return s.latestFinishedSync(ctx, func(t v3.SyncType) bool {
		return t == v3.SyncType_SYNC_TYPE_FULL
	})
}

// LatestFinishedSyncOfAnyType returns the most recently finished
// sync of any type, or nil if no sync has finished. Mirrors the
// SQLite implementation which uses SyncTypeAny.
func (s pebbleSyncMeta) LatestFinishedSyncOfAnyType(ctx context.Context) (*c1zstore.SyncRun, error) {
	return s.latestFinishedSync(ctx, func(v3.SyncType) bool { return true })
}

// latestFinishedSync delegates to Engine.LatestFinishedSyncRecord and
// translates the result into the exported c1zstore.SyncRun shape.
// typeOK is passed through verbatim; nil matches any sync type.
func (s pebbleSyncMeta) latestFinishedSync(ctx context.Context, typeOK func(v3.SyncType) bool) (*c1zstore.SyncRun, error) {
	best, err := s.e.LatestFinishedSyncRecord(ctx, typeOK)
	if err != nil {
		return nil, err
	}
	if best == nil {
		return nil, nil
	}
	return syncRunRecordToExported(best), nil
}

// Stats returns a map of record-type → row count for the named sync.
// Delegates to Adapter.Stats, which reads the EndFreshSync stats
// sidecar (O(1)) when present and falls back to iteration when not.
func (s pebbleSyncMeta) Stats(ctx context.Context, syncType connectorstore.SyncType, syncID string) (map[string]int64, error) {
	return s.e.Stats(ctx, syncType, syncID)
}

// StatsV2 implements SyncMeta. Signature matches *C1File.StatsV2 exactly.
func (s pebbleSyncMeta) StatsV2(ctx context.Context, syncType connectorstore.SyncType, syncID string) (*reader_v2.SyncStats, error) {
	return s.e.StatsV2(ctx, syncType, syncID)
}

// RecalculateStats recomputes the stats sidecar for syncID from the
// per-record-type keyspaces and persists it, overwriting any previously
// cached value. A v3 Pebble c1z holds a single sync, so the sidecar is a
// fixed key; the recompute scans the whole keyspace regardless of which
// sync is named.
func (s pebbleSyncMeta) RecalculateStats(ctx context.Context, syncID string) error {
	if syncID == "" {
		syncID = s.e.CurrentSyncID()
	}
	if syncID == "" {
		return errors.New("RecalculateStats: empty syncID and no current sync")
	}
	return s.e.PersistSyncStats(ctx, syncID)
}

// syncRunRecordToExported translates the Pebble v3.SyncRunRecord
// proto into the exported c1zstore.SyncRun shape. Mirrors
// syncRunToExported in pkg/dotc1z but adapted for the v3 proto.
func syncRunRecordToExported(r *v3.SyncRunRecord) *c1zstore.SyncRun {
	if r == nil {
		return nil
	}
	out := &c1zstore.SyncRun{
		ID:           r.GetSyncId(),
		Type:         syncTypeV3ToConnectorstore(r.GetType()),
		SyncToken:    r.GetSyncToken(),
		ParentSyncID: r.GetParentSyncId(),
		LinkedSyncID: r.GetLinkedSyncId(),
		SupportsDiff: r.GetSupportsDiff(),
	}
	if t := r.GetStartedAt(); t != nil {
		tt := t.AsTime()
		out.StartedAt = &tt
	}
	if t := r.GetEndedAt(); t != nil {
		tt := t.AsTime()
		out.EndedAt = &tt
	}
	return out
}

// sortedSyncRuns reads every sync_run record into the engine-neutral
// c1zstore.SyncRun shape, sorted oldest-first (started_at, sync_id
// tiebreaker). Shared by CleanupCandidates and ListSyncRuns so the
// projection and ordering live in one place.
//
// We sort explicitly rather than trust IterateAllSyncRuns' order:
// the iterator walks by sync_id (KSUID), and KSUIDs only encode the
// timestamp to second resolution. Two syncs created in the same
// second sort by the 16-byte random tail rather than chronologically,
// which would silently pick the wrong sync to prune. Sorting on
// started_at (with sync_id tiebreaker) matches the SQLite engine's
// ListSyncRuns ORDER BY id ASC semantics.
func (e *Engine) sortedSyncRuns(ctx context.Context) ([]c1zstore.SyncRun, error) {
	var out []c1zstore.SyncRun
	err := e.IterateAllSyncRuns(ctx, func(r *v3.SyncRunRecord) bool {
		cand := c1zstore.SyncRun{
			ID:           r.GetSyncId(),
			Type:         syncTypeV3ToConnectorstore(r.GetType()),
			SyncToken:    r.GetSyncToken(),
			ParentSyncID: r.GetParentSyncId(),
			SupportsDiff: r.GetSupportsDiff(),
			LinkedSyncID: r.GetLinkedSyncId(),
		}
		if t := r.GetStartedAt(); t != nil {
			tt := t.AsTime()
			cand.StartedAt = &tt
		}
		if t := r.GetEndedAt(); t != nil {
			tt := t.AsTime()
			cand.EndedAt = &tt
		}
		out = append(out, cand)
		return true
	})
	if err != nil {
		return nil, err
	}
	sort.SliceStable(out, func(i, j int) bool {
		ti, tj := out[i].StartedAt, out[j].StartedAt
		switch {
		case ti == nil && tj == nil:
			return out[i].ID < out[j].ID
		case ti == nil:
			return true
		case tj == nil:
			return false
		case ti.Equal(*tj):
			return out[i].ID < out[j].ID
		default:
			return ti.Before(*tj)
		}
	})
	return out, nil
}

// CleanupCandidates walks every sync_run record and projects it into
// the engine-neutral c1zstore.SyncRun shape, sorted oldest-first.
// c1zstore.SelectSyncsToDelete depends on this ordering so "drop the
// oldest overflow" trims the right end. Used by pkg/dotc1z's Pebble
// store to drive the retention policy at Cleanup.
func (e *Engine) CleanupCandidates(ctx context.Context) ([]c1zstore.SyncRun, error) {
	out, err := e.sortedSyncRuns(ctx)
	if err != nil {
		return nil, fmt.Errorf("pebble CleanupCandidates: IterateAllSyncRuns: %w", err)
	}
	return out, nil
}

// ListSyncRuns returns every sync-run record projected into the
// engine-neutral c1zstore.SyncRun shape, oldest-first (parent before
// child for a well-formed chain), in a single page. A v3 Pebble c1z
// holds exactly one sync, so pagination is trivial: the whole set is
// returned on the first call and the next-page token is always empty.
// pageToken and pageSize are accepted for parity with the SQLite
// ListSyncRuns and are not used. It backs the c1z sanitizer's
// source-side sync-graph-metadata read (linked_sync_id, supports_diff),
// which the gRPC reader surface does not carry.
func (e *Engine) ListSyncRuns(ctx context.Context, pageToken string, pageSize uint32) ([]*c1zstore.SyncRun, string, error) {
	runs, err := e.sortedSyncRuns(ctx)
	if err != nil {
		return nil, "", fmt.Errorf("pebble ListSyncRuns: %w", err)
	}
	// The returned pointers index into runs' backing array, which escapes via
	// the return value; runs must outlive this call and must not be reused.
	out := make([]*c1zstore.SyncRun, len(runs))
	for i := range runs {
		out[i] = &runs[i]
	}
	return out, "", nil
}

// syncTypeV3ToConnectorstore maps the v3-owned mirror SyncType enum
// back to the connectorstore.SyncType string-typed enum. Unknown
// values yield the empty (unspecified) string.
func syncTypeV3ToConnectorstore(t v3.SyncType) connectorstore.SyncType {
	switch t {
	case v3.SyncType_SYNC_TYPE_FULL:
		return connectorstore.SyncTypeFull
	case v3.SyncType_SYNC_TYPE_PARTIAL:
		return connectorstore.SyncTypePartial
	case v3.SyncType_SYNC_TYPE_RESOURCES_ONLY:
		return connectorstore.SyncTypeResourcesOnly
	case v3.SyncType_SYNC_TYPE_PARTIAL_UPSERTS:
		return connectorstore.SyncTypePartialUpserts
	case v3.SyncType_SYNC_TYPE_PARTIAL_DELETIONS:
		return connectorstore.SyncTypePartialDeletions
	default:
		return ""
	}
}
