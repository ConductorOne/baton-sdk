package pebble

import (
	"context"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"
	"google.golang.org/protobuf/proto"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
)

// PutEntitlementRecord writes an entitlement + its by_resource index.
func (e *Engine) PutEntitlementRecord(ctx context.Context, r *v3.EntitlementRecord) error {
	if r == nil {
		return errors.New("PutEntitlementRecord: nil record")
	}
	return e.PutEntitlementRecords(ctx, r)
}

// PutEntitlementRecords writes N entitlements in one batch. Fresh-sync
// fast path mirrors PutGrantRecords.
func (e *Engine) PutEntitlementRecords(ctx context.Context, records ...*v3.EntitlementRecord) error {
	if len(records) == 0 {
		return nil
	}
	return e.withWrite(func() error {
		batch := e.db.NewBatch()
		defer batch.Close()
		fresh := e.IsFreshSync()
		for _, r := range records {
			if r == nil {
				continue
			}
			idBytes, err := e.resolveSyncBytes(r.GetSyncId())
			if err != nil {
				return err
			}
			key := encodeEntitlementKey(idBytes, r.GetExternalId())
			val, err := marshalRecord(r)
			if err != nil {
				return err
			}
			if !fresh {
				oldVal, closer, getErr := e.db.Get(key)
				switch {
				case getErr == nil:
					old := &v3.EntitlementRecord{}
					if err := proto.Unmarshal(oldVal, old); err != nil {
						closer.Close()
						return fmt.Errorf("PutEntitlementRecords: unmarshal old %q: %w", r.GetExternalId(), err)
					}
					if err := e.deleteEntitlementIndexes(batch, idBytes, old); err != nil {
						closer.Close()
						return err
					}
					closer.Close()
				case errors.Is(getErr, pebble.ErrNotFound):
				default:
					return fmt.Errorf("PutEntitlementRecords: get old: %w", getErr)
				}
			}
			if err := batch.Set(key, val, nil); err != nil {
				return err
			}
			if err := e.writeEntitlementIndexes(batch, idBytes, r); err != nil {
				return err
			}
		}
		opts := writeOpts(e.opts.durability)
		if fresh {
			opts = pebble.NoSync
		}
		return batch.Commit(opts)
	})
}

func (e *Engine) GetEntitlementRecord(ctx context.Context, syncID, externalID string) (*v3.EntitlementRecord, error) {
	idBytes, err := e.resolveSyncBytes(syncID)
	if err != nil {
		return nil, err
	}
	val, closer, err := e.db.Get(encodeEntitlementKey(idBytes, externalID))
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	r := &v3.EntitlementRecord{}
	if err := proto.Unmarshal(val, r); err != nil {
		return nil, fmt.Errorf("GetEntitlementRecord: unmarshal: %w", err)
	}
	return r, nil
}

func (e *Engine) DeleteEntitlementRecord(ctx context.Context, syncID, externalID string) error {
	return e.withWrite(func() error {
		idBytes, err := e.resolveSyncBytes(syncID)
		if err != nil {
			return err
		}
		key := encodeEntitlementKey(idBytes, externalID)
		batch := e.db.NewBatch()
		defer batch.Close()
		oldVal, closer, err := e.db.Get(key)
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				return nil
			}
			return err
		}
		old := &v3.EntitlementRecord{}
		if err := proto.Unmarshal(oldVal, old); err != nil {
			closer.Close()
			return fmt.Errorf("DeleteEntitlementRecord: unmarshal old %q: %w", externalID, err)
		}
		if err := e.deleteEntitlementIndexes(batch, idBytes, old); err != nil {
			closer.Close()
			return err
		}
		closer.Close()
		if err := batch.Delete(key, nil); err != nil {
			return err
		}
		return batch.Commit(writeOpts(e.opts.durability))
	})
}

func (e *Engine) writeEntitlementIndexes(batch *pebble.Batch, syncIDBytes []byte, r *v3.EntitlementRecord) error {
	res := r.GetResource()
	if res == nil || res.GetResourceId() == "" {
		return nil
	}
	k := encodeEntitlementByResourceIndexKey(
		syncIDBytes,
		res.GetResourceTypeId(), res.GetResourceId(),
		r.GetExternalId(),
	)
	return batch.Set(k, nil, nil)
}

func (e *Engine) deleteEntitlementIndexes(batch *pebble.Batch, syncIDBytes []byte, r *v3.EntitlementRecord) error {
	res := r.GetResource()
	if res == nil || res.GetResourceId() == "" {
		return nil
	}
	k := encodeEntitlementByResourceIndexKey(
		syncIDBytes,
		res.GetResourceTypeId(), res.GetResourceId(),
		r.GetExternalId(),
	)
	return batch.Delete(k, nil)
}

func (e *Engine) IterateEntitlementsBySync(ctx context.Context, syncID string, yield func(*v3.EntitlementRecord) bool) error {
	idBytes, err := e.resolveSyncBytes(syncID)
	if err != nil {
		return err
	}
	prefix := encodeEntitlementPrefix(idBytes)
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upperBoundOf(prefix),
	})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		r := &v3.EntitlementRecord{}
		if err := proto.Unmarshal(iter.Value(), r); err != nil {
			return fmt.Errorf("iterate entitlements: %w", err)
		}
		if !yield(r) {
			return nil
		}
	}
	return iter.Error()
}

func (e *Engine) IterateEntitlementsByResource(ctx context.Context, syncID, resourceTypeID, resourceID string, yield func(*v3.EntitlementRecord) bool) error {
	idBytes, err := e.resolveSyncBytes(syncID)
	if err != nil {
		return err
	}
	indexPrefix := encodeEntitlementByResourcePrefix(idBytes, resourceTypeID, resourceID)
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: indexPrefix,
		UpperBound: upperBoundOf(indexPrefix),
	})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		externalID := lastTupleComponent(iter.Key(), indexPrefix)
		if externalID == "" {
			continue
		}
		val, closer, err := e.db.Get(encodeEntitlementKey(idBytes, externalID))
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				continue
			}
			return err
		}
		r := &v3.EntitlementRecord{}
		err = proto.Unmarshal(val, r)
		closer.Close()
		if err != nil {
			return err
		}
		if !yield(r) {
			return nil
		}
	}
	return iter.Error()
}
