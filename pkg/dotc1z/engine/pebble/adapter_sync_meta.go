package pebble

import (
	"context"
	"errors"
	"fmt"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
)

// SyncMeta returns the SyncMeta sub-store backed by the Pebble
// adapter. Implements dotc1z.C1ZStore.SyncMeta(). The sync-run
// records live in Pebble under typeSyncRun; methods here walk
// IterateAllSyncRuns + GetSyncRunRecord + PutSyncRunRecord on the
// engine.
func (a *Adapter) SyncMeta() dotc1z.SyncMeta {
	return pebbleSyncMeta{a: a}
}

type pebbleSyncMeta struct {
	a *Adapter
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
	r, err := s.a.engine.GetSyncRunRecord(ctx, syncID)
	if err != nil {
		return fmt.Errorf("MarkSyncSupportsDiff: get: %w", err)
	}
	r.SetSupportsDiff(true)
	if err := s.a.engine.PutSyncRunRecord(ctx, r); err != nil {
		return fmt.Errorf("MarkSyncSupportsDiff: put: %w", err)
	}
	return nil
}

// LatestFullSync returns the most recently finished sync whose type
// is SYNC_TYPE_FULL, or nil if no such sync exists.
func (s pebbleSyncMeta) LatestFullSync(ctx context.Context) (*dotc1z.SyncRun, error) {
	return s.latestFinishedSync(ctx, func(t v3.SyncType) bool {
		return t == v3.SyncType_SYNC_TYPE_FULL
	})
}

// LatestFinishedSyncOfAnyType returns the most recently finished
// sync of any type, or nil if no sync has finished. Mirrors the
// SQLite implementation which uses SyncTypeAny.
func (s pebbleSyncMeta) LatestFinishedSyncOfAnyType(ctx context.Context) (*dotc1z.SyncRun, error) {
	return s.latestFinishedSync(ctx, func(v3.SyncType) bool { return true })
}

// latestFinishedSync delegates to Engine.LatestFinishedSyncRecord and
// translates the result into the exported dotc1z.SyncRun shape.
// typeOK is passed through verbatim; nil matches any sync type.
func (s pebbleSyncMeta) latestFinishedSync(ctx context.Context, typeOK func(v3.SyncType) bool) (*dotc1z.SyncRun, error) {
	best, err := s.a.engine.LatestFinishedSyncRecord(ctx, typeOK)
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
	return s.a.Stats(ctx, syncType, syncID)
}

// syncRunRecordToExported translates the Pebble v3.SyncRunRecord
// proto into the exported dotc1z.SyncRun shape. Mirrors
// syncRunToExported in pkg/dotc1z but adapted for the v3 proto.
func syncRunRecordToExported(r *v3.SyncRunRecord) *dotc1z.SyncRun {
	if r == nil {
		return nil
	}
	out := &dotc1z.SyncRun{
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
