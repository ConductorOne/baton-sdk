package pebble

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble/v2"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/codec"
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

// UnsafePutUniqueGrantRecords is the trusted-import write path: it writes
// records unconditionally, with NO read-before-write and NO dedup pass. Do not
// use it for live connector output. The engine must currently be in fresh-sync
// mode, and the caller must guarantee each external_id appears at most once
// across the whole sync (not just within this batch). Primary + index key/value
// encoding — including the proto marshal — runs in parallel across GOMAXPROCS
// workers; a single goroutine then Sets the pre-encoded bytes into two batches
// and commits them (NoSync during a fresh sync).
//
// Unlike PutGrantRecords this skips the per-record db.Get that PutGrantRecords
// performs on every batch after the first of a fresh sync. That read-before-
// write only exists to clean up stale index entries when an external_id is
// rewritten within a sync — impossible when the caller guarantees global
// uniqueness for the imported sync.
func (e *Engine) UnsafePutUniqueGrantRecords(ctx context.Context, records ...*v3.GrantRecord) error {
	if len(records) == 0 {
		return nil
	}
	return e.withWrite(func() error {
		if !e.IsFreshSync() {
			return errors.New("UnsafePutUniqueGrantRecords: sync is not fresh")
		}
		// Resolve sync_id bytes once per distinct sync_id (usually one) on the
		// calling goroutine so the parallel encoders only read the map.
		syncBytes := make(map[string][]byte)
		for _, r := range records {
			if r == nil {
				continue
			}
			sid := r.GetSyncId()
			if _, ok := syncBytes[sid]; ok {
				continue
			}
			idBytes, err := e.resolveSyncBytes(sid)
			if err != nil {
				return err
			}
			syncBytes[sid] = idBytes
		}

		type encoded struct {
			priKey  []byte
			priVal  []byte
			idxKeys [][]byte
		}
		enc := make([]encoded, len(records))

		workers := runtime.GOMAXPROCS(0)
		if workers < 1 {
			workers = 1
		}
		chunk := (len(records) + workers - 1) / workers
		var (
			wg     sync.WaitGroup
			encErr error
			errMu  sync.Mutex
			failed atomic.Bool
		)
		for w := 0; w < workers; w++ {
			lo := w * chunk
			hi := lo + chunk
			if hi > len(records) {
				hi = len(records)
			}
			if lo >= hi {
				break
			}
			wg.Add(1)
			go func(lo, hi int) {
				defer wg.Done()
				for i := lo; i < hi; i++ {
					if failed.Load() {
						return
					}
					r := records[i]
					if r == nil {
						continue
					}
					idBytes := syncBytes[r.GetSyncId()]
					val, err := marshalRecord(r)
					if err != nil {
						errMu.Lock()
						if encErr == nil {
							encErr = err
						}
						errMu.Unlock()
						failed.Store(true)
						return
					}
					enc[i] = encoded{
						priKey:  encodeGrantKey(idBytes, r.GetExternalId()),
						priVal:  val,
						idxKeys: grantIndexKeys(idBytes, r),
					}
				}
			}(lo, hi)
		}
		wg.Wait()
		if encErr != nil {
			return encErr
		}

		priBatch := e.db.NewBatch()
		defer priBatch.Close()
		idxBatch := e.db.NewBatch()
		defer idxBatch.Close()
		for i := range enc {
			if enc[i].priKey == nil {
				continue
			}
			if err := priBatch.Set(enc[i].priKey, enc[i].priVal, nil); err != nil {
				return err
			}
			for _, k := range enc[i].idxKeys {
				if err := idxBatch.Set(k, nil, nil); err != nil {
					return err
				}
			}
		}

		opts := writeOpts(e.opts.durability)
		if e.IsFreshSync() {
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

// grantIndexKeys returns the secondary-index keys for r. The set mirrors the
// index keyspaces documented on writeGrantIndexes:
//   - by_entitlement: (entitlement_id, principal) — needs both ent + principal.
//   - by_entitlement_resource: the resource side of the entitlement (drives
//     ListGrants with req.Resource set).
//   - by_principal and by_principal_resource_type: the principal side.
//   - needs_expansion: only when the grant currently carries the flag.
func grantIndexKeys(syncIDBytes []byte, r *v3.GrantRecord) [][]byte {
	ent := r.GetEntitlement()
	princ := r.GetPrincipal()
	ext := r.GetExternalId()

	keys := make([][]byte, 0, 4)
	if ent != nil && princ != nil {
		keys = append(keys, encodeGrantByEntitlementIndexKey(
			syncIDBytes, ent.GetEntitlementId(), princ.GetResourceTypeId(), princ.GetResourceId(), ext))
	}
	if ent != nil && ent.GetResourceId() != "" {
		keys = append(keys, encodeGrantByEntitlementResourceIndexKey(
			syncIDBytes, ent.GetResourceTypeId(), ent.GetResourceId(), ext))
	}
	if princ != nil {
		keys = append(keys, encodeGrantByPrincipalIndexKey(
			syncIDBytes, princ.GetResourceTypeId(), princ.GetResourceId(), ext))
		keys = append(keys, encodeGrantByPrincipalResourceTypeIndexKey(
			syncIDBytes, princ.GetResourceTypeId(), ext))
	}
	if r.GetNeedsExpansion() {
		keys = append(keys, encodeGrantByNeedsExpansionIndexKey(syncIDBytes, ext))
	}
	return keys
}

// writeGrantIndexes adds index entries for r to batch.
func (e *Engine) writeGrantIndexes(batch *pebble.Batch, syncIDBytes []byte, r *v3.GrantRecord) error {
	for _, k := range grantIndexKeys(syncIDBytes, r) {
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
	if ent != nil && ent.GetResourceId() != "" {
		k := encodeGrantByEntitlementResourceIndexKey(
			syncIDBytes,
			ent.GetResourceTypeId(),
			ent.GetResourceId(),
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
		kRT := encodeGrantByPrincipalResourceTypeIndexKey(
			syncIDBytes,
			princ.GetResourceTypeId(),
			ext,
		)
		if err := batch.Delete(kRT, nil); err != nil {
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

// IterateGrantsByPrincipalResourceType iterates the by-principal-RT
// index for a sync. Yields each grant whose principal carries the
// given resource_type, in encoded external_id order. Stops when
// yield returns false.
func (e *Engine) IterateGrantsByPrincipalResourceType(ctx context.Context, syncID, principalRT string, yield func(*v3.GrantRecord) bool) error {
	idBytes, err := e.resolveSyncBytes(syncID)
	if err != nil {
		return err
	}
	indexPrefix := encodeGrantByPrincipalResourceTypePrefix(idBytes, principalRT)
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
			return fmt.Errorf("iterate by principal_rt: %w", err)
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

// Tuple-component decoders for index-key tails. Thin wrappers over
// codec.DecodeTupleStringTo so the engine and the canonical codec
// share one implementation of the escape rules. The previous
// hand-rolled versions silently swallowed malformed escape sequences
// (returning truncated strings); these report decode failure by
// returning the zero value, which iter callers treat as "skip this
// entry" — same observable behavior on well-formed keys, fail-safe
// on corruption.

// decodeTwoTupleComponents decodes the first two tuple-encoded string
// components from an index key relative to its prefix. Components are
// separated by a single 0x00 byte; intra-component NUL/0x01 bytes are
// escape-encoded per codec.AppendTupleString. Returns (a, b, true) on
// success or ("", "", false) if the tail is empty, malformed, or has
// fewer than two components.
func decodeTwoTupleComponents(key, prefix []byte) (string, string, bool) {
	if len(key) <= len(prefix) {
		return "", "", false
	}
	tail := key[len(prefix):]
	first, next, err := codec.DecodeTupleStringTo(nil, tail, 0)
	if err != nil || next >= len(tail) {
		return "", "", false
	}
	// next points at the separator byte; skip it.
	second, _, err := codec.DecodeTupleStringTo(nil, tail, next+1)
	if err != nil {
		return "", "", false
	}
	return string(first), string(second), true
}

// lastTupleComponent returns the decoded string of the LAST tuple
// component in an index key relative to its prefix. Walks separator-
// delimited components and returns whichever one trails. Returns the
// empty string if the key doesn't extend past the prefix or the tail
// is malformed.
func lastTupleComponent(key, prefix []byte) string {
	if len(key) <= len(prefix) {
		return ""
	}
	tail := key[len(prefix):]
	var last []byte
	off := 0
	for off <= len(tail) {
		decoded, next, err := codec.DecodeTupleStringTo(nil, tail, off)
		if err != nil {
			return ""
		}
		last = decoded
		if next >= len(tail) {
			break
		}
		// next is the separator byte position; advance past it to
		// the next component.
		off = next + 1
	}
	return string(last)
}
