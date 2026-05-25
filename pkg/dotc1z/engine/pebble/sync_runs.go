package pebble

import (
	"context"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"
	"google.golang.org/protobuf/proto"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
)

// PutSyncRunRecord — one row per sync. sync_id is the only PK
// component; the record reads/writes use only the sync_id.
//
// Unlike other writer methods this does NOT consult resolveSyncBytes
// fallback — a SyncRunRecord must always carry its own sync_id (since
// it's *defining* what's a valid sync, not operating under one).
func (e *Engine) PutSyncRunRecord(ctx context.Context, r *v3.SyncRunRecord) error {
	if r == nil {
		return errors.New("PutSyncRunRecord: nil record")
	}
	if r.GetSyncId() == "" {
		return errors.New("PutSyncRunRecord: empty sync_id")
	}
	return e.withWrite(func() error {
		idBytes, err := e.resolveSyncBytes(r.GetSyncId())
		if err != nil {
			return err
		}
		val, err := marshalRecord(r)
		if err != nil {
			return err
		}
		return e.db.Set(encodeSyncRunKey(idBytes), val, writeOpts(e.opts.durability))
	})
}

func (e *Engine) GetSyncRunRecord(ctx context.Context, syncID string) (*v3.SyncRunRecord, error) {
	if syncID == "" {
		return nil, errors.New("GetSyncRunRecord: empty sync_id")
	}
	idBytes, err := e.resolveSyncBytes(syncID)
	if err != nil {
		return nil, err
	}
	val, closer, err := e.db.Get(encodeSyncRunKey(idBytes))
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	r := &v3.SyncRunRecord{}
	if err := proto.Unmarshal(val, r); err != nil {
		return nil, fmt.Errorf("GetSyncRunRecord: unmarshal: %w", err)
	}
	return r, nil
}

func (e *Engine) DeleteSyncRunRecord(ctx context.Context, syncID string) error {
	return e.withWrite(func() error {
		if syncID == "" {
			return errors.New("DeleteSyncRunRecord: empty sync_id")
		}
		idBytes, err := e.resolveSyncBytes(syncID)
		if err != nil {
			return err
		}
		return e.db.Delete(encodeSyncRunKey(idBytes), writeOpts(e.opts.durability))
	})
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
		if err := proto.Unmarshal(iter.Value(), r); err != nil {
			return fmt.Errorf("iterate sync_runs: %w", err)
		}
		if !yield(r) {
			return nil
		}
	}
	return iter.Error()
}
