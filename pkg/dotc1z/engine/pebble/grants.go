package pebble

import (
	"context"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"
	"google.golang.org/protobuf/proto"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
)

// PutGrantRecord writes a grant record + its by_entitlement and
// by_principal index entries, atomically in a single pebble.Batch.
//
// This is the engine's canonical write path; other record types
// follow the same shape (read the previous primary if any → delete
// its index entries → write the new primary → write the new index
// entries → commit).
func (e *Engine) PutGrantRecord(ctx context.Context, r *v3.GrantRecord) error {
	if r == nil {
		return errors.New("PutGrantRecord: nil record")
	}
	return e.PutGrantRecords(ctx, r)
}

// PutGrantRecords writes N grants in a single pebble.Batch, fsyncing
// once at the end. This is the bulk path the adapter's PutGrants uses.
//
// Fresh-sync fast path: when IsFreshSync() is true, the read-before-
// write Get is skipped entirely (the sync_id has no prior records),
// and the batch commits with pebble.NoSync. EndFreshSync then does
// one Flush+fsync to harden the data.
//
// Mixed-sync / overwrite path (not fresh): per-grant Get for index
// cleanup, batch.Commit with pebble.Sync.
func (e *Engine) PutGrantRecords(ctx context.Context, records ...*v3.GrantRecord) error {
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
			key := encodeGrantKey(idBytes, r.GetExternalId())
			val, err := marshalRecord(r)
			if err != nil {
				return err
			}
			if !fresh {
				oldVal, closer, getErr := e.db.Get(key)
				switch {
				case getErr == nil:
					old := &v3.GrantRecord{}
					if err := proto.Unmarshal(oldVal, old); err != nil {
						closer.Close()
						// Stored record won't decode. Returning the
						// error here keeps index integrity (skipping
						// cleanup would orphan the old index entries
						// while the new write proceeds). Caller can
						// recover by deleting the corrupt key.
						return fmt.Errorf("PutGrantRecords: unmarshal old %q: %w", r.GetExternalId(), err)
					}
					if err := e.deleteGrantIndexes(batch, idBytes, old); err != nil {
						closer.Close()
						return err
					}
					closer.Close()
				case errors.Is(getErr, pebble.ErrNotFound):
					// no-op
				default:
					return fmt.Errorf("PutGrantRecords: get old: %w", getErr)
				}
			}
			if err := batch.Set(key, val, nil); err != nil {
				return err
			}
			if err := e.writeGrantIndexes(batch, idBytes, r); err != nil {
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

// GetGrantRecord fetches a grant record by sync_id + external_id.
// syncID may be empty to use the engine's currently-set sync.
func (e *Engine) GetGrantRecord(ctx context.Context, syncID, externalID string) (*v3.GrantRecord, error) {
	idBytes, err := e.resolveSyncBytes(syncID)
	if err != nil {
		return nil, err
	}
	key := encodeGrantKey(idBytes, externalID)
	val, closer, err := e.db.Get(key)
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	r := &v3.GrantRecord{}
	if err := proto.Unmarshal(val, r); err != nil {
		return nil, fmt.Errorf("GetGrantRecord: unmarshal: %w", err)
	}
	return r, nil
}

// DeleteGrantRecord removes a grant and its index entries.
func (e *Engine) DeleteGrantRecord(ctx context.Context, syncID, externalID string) error {
	return e.withWrite(func() error {
		idBytes, err := e.resolveSyncBytes(syncID)
		if err != nil {
			return err
		}
		key := encodeGrantKey(idBytes, externalID)

		batch := e.db.NewBatch()
		defer batch.Close()

		oldVal, closer, err := e.db.Get(key)
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				return nil // delete of non-existent is a no-op
			}
			return err
		}
		old := &v3.GrantRecord{}
		if err := proto.Unmarshal(oldVal, old); err != nil {
			closer.Close()
			return fmt.Errorf("DeleteGrantRecord: unmarshal old %q: %w", externalID, err)
		}
		{
			if err := e.deleteGrantIndexes(batch, idBytes, old); err != nil {
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

// writeGrantIndexes adds index entries for r to batch.
func (e *Engine) writeGrantIndexes(batch *pebble.Batch, syncIDBytes []byte, r *v3.GrantRecord) error {
	ent := r.GetEntitlement()
	princ := r.GetPrincipal()
	ext := r.GetExternalId()

	if ent != nil && princ != nil {
		k := encodeGrantByEntitlementIndexKey(
			syncIDBytes,
			ent.GetEntitlementId(),
			princ.GetResourceTypeId(),
			princ.GetResourceId(),
			ext,
		)
		if err := batch.Set(k, nil, nil); err != nil {
			return err
		}
	}
	if princ != nil {
		k := encodeGrantByPrincipalIndexKey(
			syncIDBytes,
			princ.GetResourceTypeId(),
			princ.GetResourceId(),
			ext,
		)
		if err := batch.Set(k, nil, nil); err != nil {
			return err
		}
	}
	return nil
}

// deleteGrantIndexes mirrors writeGrantIndexes but with batch.Delete.
func (e *Engine) deleteGrantIndexes(batch *pebble.Batch, syncIDBytes []byte, r *v3.GrantRecord) error {
	ent := r.GetEntitlement()
	princ := r.GetPrincipal()
	ext := r.GetExternalId()

	if ent != nil && princ != nil {
		k := encodeGrantByEntitlementIndexKey(
			syncIDBytes,
			ent.GetEntitlementId(),
			princ.GetResourceTypeId(),
			princ.GetResourceId(),
			ext,
		)
		if err := batch.Delete(k, nil); err != nil {
			return err
		}
	}
	if princ != nil {
		k := encodeGrantByPrincipalIndexKey(
			syncIDBytes,
			princ.GetResourceTypeId(),
			princ.GetResourceId(),
			ext,
		)
		if err := batch.Delete(k, nil); err != nil {
			return err
		}
	}
	return nil
}

// IterateGrantsBySync iterates all grants in a sync in primary-key
// order. yield returns false to stop iteration.
func (e *Engine) IterateGrantsBySync(ctx context.Context, syncID string, yield func(*v3.GrantRecord) bool) error {
	idBytes, err := e.resolveSyncBytes(syncID)
	if err != nil {
		return err
	}
	prefix := encodeGrantPrefix(idBytes)
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upperBoundOf(prefix),
	})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		r := &v3.GrantRecord{}
		if err := proto.Unmarshal(iter.Value(), r); err != nil {
			return fmt.Errorf("iterate grants: %w", err)
		}
		if !yield(r) {
			return nil
		}
	}
	return iter.Error()
}

// IterateGrantsByEntitlement iterates the by_entitlement index for
// the given entitlement_id, yielding each grant in encoded principal-
// key order. yield returns false to stop.
func (e *Engine) IterateGrantsByEntitlement(ctx context.Context, syncID, entitlementID string, yield func(*v3.GrantRecord) bool) error {
	idBytes, err := e.resolveSyncBytes(syncID)
	if err != nil {
		return err
	}
	indexPrefix := encodeGrantByEntitlementPrefix(idBytes, entitlementID)
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: indexPrefix,
		UpperBound: upperBoundOf(indexPrefix),
	})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		// Index key tail is the grant's external_id. Need to decode it
		// to look up the primary record. The tail is the last
		// tuple-encoded component in the index key.
		externalID := lastTupleComponent(iter.Key(), indexPrefix)
		if externalID == "" {
			continue
		}
		key := encodeGrantKey(idBytes, externalID)
		val, closer, err := e.db.Get(key)
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				// Index entry references a primary that's gone — a
				// transient orphan during overwrite, or a real
				// consistency issue. Skip; fsck reconciles.
				continue
			}
			return err
		}
		r := &v3.GrantRecord{}
		err = proto.Unmarshal(val, r)
		closer.Close()
		if err != nil {
			return fmt.Errorf("iterate by entitlement: %w", err)
		}
		if !yield(r) {
			return nil
		}
	}
	return iter.Error()
}

// IterateGrantsByPrincipal iterates the by_principal index.
func (e *Engine) IterateGrantsByPrincipal(ctx context.Context, syncID, principalRT, principalID string, yield func(*v3.GrantRecord) bool) error {
	idBytes, err := e.resolveSyncBytes(syncID)
	if err != nil {
		return err
	}
	indexPrefix := encodeGrantByPrincipalPrefix(idBytes, principalRT, principalID)
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
		key := encodeGrantKey(idBytes, externalID)
		val, closer, err := e.db.Get(key)
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				continue
			}
			return err
		}
		r := &v3.GrantRecord{}
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

// decodeTwoTupleComponents decodes the last two tuple-encoded string
// components from an index key relative to its prefix. The components
// are separated by a single 0x00 byte; embedded NULs in the components
// are escape-encoded per the tuple encoder. Returns (a, b, true) on
// success.
func decodeTwoTupleComponents(key, prefix []byte) (string, string, bool) {
	if len(key) <= len(prefix) {
		return "", "", false
	}
	tail := key[len(prefix):]
	// Find the separator between the two components. We can't just
	// scan for 0x00 because intra-component NULs are escaped — but in
	// the escape sequence 0x01 0x01 the 0x01 comes first, so a bare
	// 0x00 is always a separator.
	first := make([]byte, 0, len(tail)/2)
	i := 0
	for i < len(tail) {
		b := tail[i]
		if b == 0x00 {
			// Found separator.
			second := decodeOneTupleComponent(tail[i+1:])
			return string(first), second, true
		}
		if b == 0x01 && i+1 < len(tail) {
			switch tail[i+1] {
			case 0x01:
				first = append(first, 0x00)
			case 0x02:
				first = append(first, 0x01)
			default:
				return "", "", false
			}
			i += 2
			continue
		}
		first = append(first, b)
		i++
	}
	return "", "", false
}

func decodeOneTupleComponent(b []byte) string {
	out := make([]byte, 0, len(b))
	i := 0
	for i < len(b) {
		c := b[i]
		if c == 0x01 && i+1 < len(b) {
			switch b[i+1] {
			case 0x01:
				out = append(out, 0x00)
			case 0x02:
				out = append(out, 0x01)
			default:
				return string(out)
			}
			i += 2
			continue
		}
		if c == 0x00 {
			// Should not happen in a single component — return what we have.
			return string(out)
		}
		out = append(out, c)
		i++
	}
	return string(out)
}

// lastTupleComponent returns the decoded string of the last component
// in an index key relative to its prefix. Returns empty string if the
// key doesn't extend past the prefix.
func lastTupleComponent(key, prefix []byte) string {
	if len(key) <= len(prefix) {
		return ""
	}
	tail := key[len(prefix):]
	// The tail may have intermediate components; for our two indexes
	// the tail is exactly one tuple-encoded string (the external_id),
	// terminated by EOF (no trailing separator).
	out := make([]byte, 0, len(tail))
	i := 0
	for i < len(tail) {
		b := tail[i]
		if b == 0x00 {
			// Separator — shouldn't appear inside the last component;
			// indicates intermediate components remain. Skip past it
			// and continue with the next component.
			out = out[:0]
			i++
			continue
		}
		if b == 0x01 && i+1 < len(tail) {
			switch tail[i+1] {
			case 0x01:
				out = append(out, 0x00)
			case 0x02:
				out = append(out, 0x01)
			default:
				return string(out)
			}
			i += 2
			continue
		}
		out = append(out, b)
		i++
	}
	return string(out)
}
