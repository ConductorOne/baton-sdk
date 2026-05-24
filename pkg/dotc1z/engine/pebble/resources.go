//go:build batonsdkv2

package pebble

import (
	"context"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble"
	"google.golang.org/protobuf/proto"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
)

// PutResourceRecord writes a resource record + its by_parent index
// entry (if the resource has a parent). Read-before-write index
// cleanup follows the canonical pattern from grants.go.
func (e *Engine) PutResourceRecord(ctx context.Context, r *v3.ResourceRecord) error {
	if r == nil {
		return errors.New("PutResourceRecord: nil record")
	}
	return e.withWrite(func() error {
		idBytes, err := e.resolveSyncBytes(r.GetSyncId())
		if err != nil {
			return err
		}
		key := encodeResourceKey(idBytes, r.GetResourceTypeId(), r.GetResourceId())
		val, err := marshalRecord(r)
		if err != nil {
			return err
		}

		batch := e.db.NewBatch()
		defer batch.Close()

		oldVal, closer, err := e.db.Get(key)
		switch {
		case err == nil:
			old := &v3.ResourceRecord{}
			if err := proto.Unmarshal(oldVal, old); err == nil {
				if err := e.deleteResourceIndexes(batch, idBytes, old); err != nil {
					closer.Close()
					return err
				}
			}
			closer.Close()
		case errors.Is(err, pebble.ErrNotFound):
			// no-op
		default:
			return fmt.Errorf("PutResourceRecord: get old: %w", err)
		}

		if err := batch.Set(key, val, nil); err != nil {
			return err
		}
		if err := e.writeResourceIndexes(batch, idBytes, r); err != nil {
			return err
		}
		return batch.Commit(writeOpts(e.opts.durability))
	})
}

func (e *Engine) GetResourceRecord(ctx context.Context, syncID, resourceTypeID, resourceID string) (*v3.ResourceRecord, error) {
	idBytes, err := e.resolveSyncBytes(syncID)
	if err != nil {
		return nil, err
	}
	key := encodeResourceKey(idBytes, resourceTypeID, resourceID)
	val, closer, err := e.db.Get(key)
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	r := &v3.ResourceRecord{}
	if err := proto.Unmarshal(val, r); err != nil {
		return nil, fmt.Errorf("GetResourceRecord: unmarshal: %w", err)
	}
	return r, nil
}

func (e *Engine) DeleteResourceRecord(ctx context.Context, syncID, resourceTypeID, resourceID string) error {
	return e.withWrite(func() error {
		idBytes, err := e.resolveSyncBytes(syncID)
		if err != nil {
			return err
		}
		key := encodeResourceKey(idBytes, resourceTypeID, resourceID)

		batch := e.db.NewBatch()
		defer batch.Close()

		oldVal, closer, err := e.db.Get(key)
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				return nil
			}
			return err
		}
		old := &v3.ResourceRecord{}
		if err := proto.Unmarshal(oldVal, old); err == nil {
			if err := e.deleteResourceIndexes(batch, idBytes, old); err != nil {
				closer.Close()
				return err
			}
		}
		closer.Close()
		if err := batch.Delete(key, nil); err != nil {
			return err
		}
		return batch.Commit(writeOpts(e.opts.durability))
	})
}

func (e *Engine) writeResourceIndexes(batch *pebble.Batch, syncIDBytes []byte, r *v3.ResourceRecord) error {
	parent := r.GetParent()
	if parent == nil || parent.GetResourceId() == "" {
		return nil
	}
	k := encodeResourceByParentIndexKey(
		syncIDBytes,
		parent.GetResourceTypeId(), parent.GetResourceId(),
		r.GetResourceTypeId(), r.GetResourceId(),
	)
	return batch.Set(k, nil, nil)
}

func (e *Engine) deleteResourceIndexes(batch *pebble.Batch, syncIDBytes []byte, r *v3.ResourceRecord) error {
	parent := r.GetParent()
	if parent == nil || parent.GetResourceId() == "" {
		return nil
	}
	k := encodeResourceByParentIndexKey(
		syncIDBytes,
		parent.GetResourceTypeId(), parent.GetResourceId(),
		r.GetResourceTypeId(), r.GetResourceId(),
	)
	return batch.Delete(k, nil)
}

func (e *Engine) IterateResourcesBySync(ctx context.Context, syncID string, yield func(*v3.ResourceRecord) bool) error {
	idBytes, err := e.resolveSyncBytes(syncID)
	if err != nil {
		return err
	}
	prefix := encodeResourcePrefix(idBytes)
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upperBoundOf(prefix),
	})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		r := &v3.ResourceRecord{}
		if err := proto.Unmarshal(iter.Value(), r); err != nil {
			return fmt.Errorf("iterate resources: %w", err)
		}
		if !yield(r) {
			return nil
		}
	}
	return iter.Error()
}

func (e *Engine) IterateResourcesByParent(ctx context.Context, syncID, parentRT, parentID string, yield func(*v3.ResourceRecord) bool) error {
	idBytes, err := e.resolveSyncBytes(syncID)
	if err != nil {
		return err
	}
	indexPrefix := encodeResourceByParentPrefix(idBytes, parentRT, parentID)
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: indexPrefix,
		UpperBound: upperBoundOf(indexPrefix),
	})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		// Decode (childRT, childID) from the tail.
		childRT, childID, ok := decodeTwoTupleComponents(iter.Key(), indexPrefix)
		if !ok {
			continue
		}
		val, closer, err := e.db.Get(encodeResourceKey(idBytes, childRT, childID))
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				continue
			}
			return err
		}
		r := &v3.ResourceRecord{}
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
