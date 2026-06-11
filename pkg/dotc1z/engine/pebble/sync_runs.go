package pebble

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cockroachdb/pebble/v2"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
)

// unfinishedSyncMaxAge bounds how old an in-progress sync may be and
// still be considered resumable/readable. Mirrors the one-week cutoff
// in SQLite's getLatestUnfinishedSync (pkg/dotc1z/sync_runs.go): a
// sync that started but never reached EndSync more than a week ago is
// treated as abandoned, not a live read target.
const unfinishedSyncMaxAge = 7 * 24 * time.Hour

// PutSyncRunRecord writes the file's single sync-run record at the
// fixed sync-run key. The sync_id lives in the record value (the only
// place a sync_id is stored — never in a key), and must be non-empty
// since this record is *defining* what counts as a valid sync.
func (e *Engine) PutSyncRunRecord(ctx context.Context, r *v3.SyncRunRecord) error {
	if r == nil {
		return errors.New("PutSyncRunRecord: nil record")
	}
	if r.GetSyncId() == "" {
		return errors.New("PutSyncRunRecord: empty sync_id")
	}
	return e.withWrite(func() error {
		val, err := marshalRecord(r)
		if err != nil {
			return err
		}
		return e.db.Set(encodeSyncRunKey(), val, writeOpts(e.opts.durability))
	})
}

func (e *Engine) GetSyncRunRecord(ctx context.Context, syncID string) (*v3.SyncRunRecord, error) {
	if syncID == "" {
		return nil, errors.New("GetSyncRunRecord: empty sync_id")
	}
	val, closer, err := e.db.Get(encodeSyncRunKey())
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	r := &v3.SyncRunRecord{}
	if err := unmarshalRecord(val, r); err != nil {
		return nil, fmt.Errorf("GetSyncRunRecord: unmarshal: %w", err)
	}
	// The sync-run record lives at a single fixed key (one sync per
	// file). A lookup for a sync_id other than the stored one must miss
	// rather than silently return the wrong sync — ResumeSync,
	// StartOrResumeSync, and CloneSync all rely on a not-found error to
	// distinguish "this sync exists" from "some sync exists".
	if r.GetSyncId() != syncID {
		return nil, pebble.ErrNotFound
	}
	return r, nil
}

func (e *Engine) DeleteSyncRunRecord(ctx context.Context, syncID string) error {
	return e.withWrite(func() error {
		if syncID == "" {
			return errors.New("DeleteSyncRunRecord: empty sync_id")
		}
		// Single fixed sync-run key: only delete when the stored record
		// is the requested sync. A mismatch (or absence) is a no-op, so
		// a stale id can't clobber a different sync's record.
		val, closer, getErr := e.db.Get(encodeSyncRunKey())
		if getErr != nil {
			if errors.Is(getErr, pebble.ErrNotFound) {
				return nil
			}
			return getErr
		}
		stored := &v3.SyncRunRecord{}
		uErr := unmarshalRecord(val, stored)
		closer.Close()
		if uErr != nil {
			return fmt.Errorf("DeleteSyncRunRecord: unmarshal: %w", uErr)
		}
		if stored.GetSyncId() != syncID {
			return nil
		}
		return e.db.Delete(encodeSyncRunKey(), writeOpts(e.opts.durability))
	})
}

// hasSyncRun reports whether the engine already holds a sync-run
// record (the file's one sync). StartNewSync uses it to decide whether
// a prior sync must be wiped before the replacement is bound.
func (e *Engine) hasSyncRun() (bool, error) {
	_, closer, err := e.db.Get(encodeSyncRunKey())
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	_ = closer.Close()
	return true, nil
}

// IterateAllSyncRuns iterates every sync_run record in the engine.
// Used by callers that want "what syncs do I have available?".
func (e *Engine) IterateAllSyncRuns(ctx context.Context, yield func(*v3.SyncRunRecord) bool) error {
	prefix := encodeSyncRunFullPrefix()
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upperBoundOf(prefix),
	})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		r := &v3.SyncRunRecord{}
		if err := unmarshalRecord(iter.Value(), r); err != nil {
			return fmt.Errorf("iterate sync_runs: %w", err)
		}
		if !yield(r) {
			return nil
		}
	}
	return iter.Error()
}

// LatestFinishedSyncRecord returns the SyncRunRecord with the latest
// ended_at that matches typeOK, or (nil, nil) when no finished sync
// matches. typeOK may be nil to accept any sync type.
//
// This is the single source of truth for "pick the latest finished
// sync" on the Pebble engine; all three external entry points
// (connectorstore.LatestFinishedSyncIDFetcher,
// dotc1z.SyncMeta.LatestFullSync / LatestFinishedSyncOfAnyType, and
// reader_v2.SyncsReaderService.GetLatestFinishedSync) call here so
// the tiebreaker and predicate semantics stay consistent.
//
// Ties on ended_at are broken by sync_id (KSUIDs sort by time, so
// the lexicographically greater id is the later sync). Matches the
// SQLite-side `ORDER BY ended_at DESC, sync_id DESC` in
// pkg/dotc1z/sync_runs.go:getFinishedSync (commit 1627b047) which
// closes a Windows coarse-time-resolution race where adjacent
// EndSync calls can produce identical ended_at timestamps.
//
// O(N) in the count of sync_runs (one record per sync; typically a
// few hundred over a c1z's lifetime, not millions).
func (e *Engine) LatestFinishedSyncRecord(ctx context.Context, typeOK func(v3.SyncType) bool) (*v3.SyncRunRecord, error) {
	var best *v3.SyncRunRecord
	err := e.IterateAllSyncRuns(ctx, func(r *v3.SyncRunRecord) bool {
		if r == nil || r.GetEndedAt() == nil {
			return true
		}
		if typeOK != nil && !typeOK(r.GetType()) {
			return true
		}
		if best == nil {
			best = r
			return true
		}
		curEnd := r.GetEndedAt().AsTime()
		bestEnd := best.GetEndedAt().AsTime()
		if curEnd.After(bestEnd) {
			best = r
			return true
		}
		// Equal ended_at: tiebreak on sync_id (KSUID > sort = later).
		if curEnd.Equal(bestEnd) && r.GetSyncId() > best.GetSyncId() {
			best = r
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	return best, nil
}

// LatestUnfinishedSyncRecord returns the most-recently-started
// sync_run that has NOT ended and was started within the last week,
// matching typeOK (nil = any type), or (nil, nil) when none match.
//
// Mirrors SQLite's getLatestUnfinishedSync (pkg/dotc1z/sync_runs.go):
// it selects ended_at-null records started after the
// unfinishedSyncMaxAge cutoff, ordered by started_at (sync_id
// tiebreaker for coarse clock resolution). It is the read-path
// fallback after LatestFinishedSyncRecord, so a c1z whose only sync
// was interrupted before EndSync still resolves to that in-progress
// sync instead of returning no sync at all — matching the SQLite
// resolveSyncIDForRead cascade.
//
// O(N) in the count of sync_runs (one record per sync).
func (e *Engine) LatestUnfinishedSyncRecord(ctx context.Context, typeOK func(v3.SyncType) bool) (*v3.SyncRunRecord, error) {
	cutoff := time.Now().Add(-unfinishedSyncMaxAge)
	var best *v3.SyncRunRecord
	err := e.IterateAllSyncRuns(ctx, func(r *v3.SyncRunRecord) bool {
		if r == nil || r.GetEndedAt() != nil {
			return true
		}
		started := r.GetStartedAt()
		if started == nil || started.AsTime().Before(cutoff) {
			return true
		}
		if typeOK != nil && !typeOK(r.GetType()) {
			return true
		}
		if best == nil {
			best = r
			return true
		}
		curStart := started.AsTime()
		bestStart := best.GetStartedAt().AsTime()
		if curStart.After(bestStart) {
			best = r
			return true
		}
		// Equal started_at: tiebreak on sync_id (KSUID > sort = later).
		if curStart.Equal(bestStart) && r.GetSyncId() > best.GetSyncId() {
			best = r
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	return best, nil
}
