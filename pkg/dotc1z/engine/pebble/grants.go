package pebble

import (
	"context"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"

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

// PutGrantRecords writes N grants in two pebble.Batches — one for
// primary-key writes, one for index-key writes — and commits them
// sequentially. This is the bulk path the adapter's PutGrants uses.
//
// Mutation safety. Connectors can legitimately emit the same
// external_id twice within a single sync (paginated sources,
// deduplication bugs in upstream APIs, etc.). To prevent orphan
// index entries from earlier duplicates, we pre-scan records to find
// the latest occurrence of each (sync_id, external_id) tuple and
// process only those — earlier duplicates are dropped before any
// batch byte is written. db.Get doesn't see in-batch writes either
// way, so this dedup pass is the load-bearing safety net that
// neither the old read-before-write path nor a pure skip-Get path
// provides.
//
// Read-before-write index cleanup. On a NON-fresh sync the engine
// must Get the prior primary value so it can delete the index keys
// the previous sync wrote (entitlement/principal can change between
// syncs). On a FRESH sync the keyspace under this sync is empty by
// construction (MarkFreshSync ranges-delete it) so the Get is
// guaranteed to return ErrNotFound and we skip it — saves an LSM
// lookup per record on the bulk path.
//
// Two batches. The primary keys arrive in roughly sorted order from
// the adapter (V2→V3 translator preserves the connector's record
// order, and most connectors emit grants clustered by entitlement
// which tends to cluster external_ids). The index keys are sorted on
// a totally different field (entitlement_id / principal), so
// interleaving them in one batch makes Pebble's flushable-batch
// promotion path quote sort. Splitting them lets each batch's
// natural order survive.
//
// Fresh-sync still uses pebble.NoSync — EndFreshSync does one
// Flush+fsync at sync end to harden the data.
func (e *Engine) PutGrantRecords(ctx context.Context, records ...*v3.GrantRecord) error {
	if len(records) == 0 {
		return nil
	}
	return e.withWrite(func() error {
		priBatch := e.db.NewBatch()
		defer priBatch.Close()
		idxBatch := e.db.NewBatch()
		defer idxBatch.Close()

		fresh := e.IsFreshSync()
		// skipGet fires exactly once per fresh sync — only the first
		// PutGrantRecords call sees the keyspace empty by construction.
		// Subsequent calls in the same fresh sync still need
		// read-before-write to clean up index entries that the prior
		// in-sync calls already committed (e.g. paginated sources
		// emitting an external_id on two pages).
		skipGet := e.takeFreshGrantsEmpty()

		// Dedup pre-pass: keep only the LAST occurrence of each
		// (sync_id, external_id). The map value is the records[]
		// index — when we re-iterate, we process record i only if
		// dedup[(sync,ext)] == i.
		type dedupKey struct {
			syncID string
			extID  string
		}
		var dedup map[dedupKey]int
		if len(records) > 1 {
			dedup = make(map[dedupKey]int, len(records))
			for i, r := range records {
				if r == nil {
					continue
				}
				dedup[dedupKey{r.GetSyncId(), r.GetExternalId()}] = i
			}
		}

		// Cache the resolved sync_id across records sharing the same
		// string. The adapter's V2→V3 translator stamps every record
		// with the engine's current sync, so the common case is one
		// distinct sync_id per call.
		var (
			lastSyncIDStr string
			lastIDBytes   []byte
			haveLast      bool
		)
		for i, r := range records {
			if r == nil {
				continue
			}
			if dedup != nil {
				if dedup[dedupKey{r.GetSyncId(), r.GetExternalId()}] != i {
					continue
				}
			}
			var (
				idBytes []byte
				err     error
			)
			if sid := r.GetSyncId(); haveLast && sid == lastSyncIDStr {
				idBytes = lastIDBytes
			} else {
				idBytes, err = e.resolveSyncBytes(sid)
				if err != nil {
					return err
				}
				lastSyncIDStr = sid
				lastIDBytes = idBytes
				haveLast = true
			}
			key := encodeGrantKey(idBytes, r.GetExternalId())
			val, err := marshalRecord(r)
			if err != nil {
				return err
			}
			if !skipGet {
				oldVal, closer, getErr := e.db.Get(key)
				switch {
				case getErr == nil:
					old := &v3.GrantRecord{}
					if err := unmarshalRecord(oldVal, old); err != nil {
						closer.Close()
						return fmt.Errorf("PutGrantRecords: unmarshal old %q: %w", r.GetExternalId(), err)
					}
					if err := e.deleteGrantIndexes(idxBatch, idBytes, old); err != nil {
						closer.Close()
						return err
					}
					closer.Close()
				case errors.Is(getErr, pebble.ErrNotFound):
					// no prior record — write unconditionally
				default:
					return fmt.Errorf("PutGrantRecords: get old: %w", getErr)
				}
			}
			if err := priBatch.Set(key, val, nil); err != nil {
				return err
			}
			if err := e.writeGrantIndexes(idxBatch, idBytes, r); err != nil {
				return err
			}
		}
		opts := writeOpts(e.opts.durability)
		if fresh {
			opts = pebble.NoSync
		}
		if err := priBatch.Commit(opts); err != nil {
			return err
		}
		return idxBatch.Commit(opts)
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
	if err := unmarshalRecord(val, r); err != nil {
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
		if err := unmarshalRecord(oldVal, old); err != nil {
			closer.Close()
			return fmt.Errorf("DeleteGrantRecord: unmarshal old %q: %w", externalID, err)
		}
		if err := e.deleteGrantIndexes(batch, idBytes, old); err != nil {
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
	// needs_expansion index — populated only when the grant
	// currently carries the flag. Mirrors the SQLite partial
	// index `WHERE needs_expansion = 1`. PendingExpansion scans
	// this keyspace; on overwrite (cross-call) the deleteGrantIndexes
	// pass removes the old key first, so a flag-flip is reflected
	// correctly.
	if r.GetNeedsExpansion() {
		k := encodeGrantByNeedsExpansionIndexKey(syncIDBytes, ext)
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
	// Always Delete the needs_expansion key, even when the old
	// record didn't carry the flag. Delete on a non-existent key
	// is a Pebble no-op, so this is cheaper than checking the
	// flag first and gives correct behavior when a connector
	// flips needs_expansion=false on a subsequent write of the
	// same external_id.
	if err := batch.Delete(encodeGrantByNeedsExpansionIndexKey(syncIDBytes, ext), nil); err != nil {
		return err
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
		if err := unmarshalRecord(iter.Value(), r); err != nil {
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
		err = unmarshalRecord(val, r)
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
		err = unmarshalRecord(val, r)
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

// IterateGrantsByNeedsExpansion iterates the needs_expansion index
// for a sync, yielding each grant whose NeedsExpansion flag is
// currently set. yield returns false to stop.
//
// Pebble-equivalent of the SQLite partial index
// `WHERE needs_expansion = 1`. Backs PendingExpansionPage on the
// grant store.
func (e *Engine) IterateGrantsByNeedsExpansion(ctx context.Context, syncID string, yield func(*v3.GrantRecord) bool) error {
	idBytes, err := e.resolveSyncBytes(syncID)
	if err != nil {
		return err
	}
	indexPrefix := encodeGrantByNeedsExpansionPrefix(idBytes)
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
				// Orphan: index entry without a primary. Skip;
				// fsck reconciles.
				continue
			}
			return err
		}
		r := &v3.GrantRecord{}
		err = unmarshalRecord(val, r)
		closer.Close()
		if err != nil {
			return fmt.Errorf("iterate needs_expansion: %w", err)
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
