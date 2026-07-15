package pebble

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/cockroachdb/pebble/v2"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/types/known/timestamppb"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/codec"
)

// Source-cache replay, engine side.
//
// The typeSourceCache keyspace holds one SourceCacheEntryRecord per
// (row_kind, scope_key): the opaque upstream validator (etag / delta
// token) the sync recorded for that scope. Rows produced under a scope
// are stamped with source_scope_key and indexed under the
// by_source_scope families, whose tails are identity tuples — so a
// replay derives every primary key from the index key alone and copies
// raw values across files without a proto unmarshal.
//
// The previous sync lives in a separate read-only engine (a Pebble c1z
// holds exactly one sync); replay copies from prev into the receiver.

// replayBatchRows bounds how many rows accumulate in one pebble.Batch
// before an intermediate commit. Replay of a delta-query collection can
// be the whole previous row set, so the batch must not grow unbounded.
const replayBatchRows = 10_000

// ReplayedParentResource identifies one replayed resource whose
// annotations carry ChildResourceType — the syncer must schedule child
// SyncResources actions for it, since a replayed page returns no rows
// and the response-row path that normally schedules children never
// sees these parents.
type ReplayedParentResource struct {
	ResourceTypeID string
	ResourceID     string
	ChildTypeIDs   []string
}

// SourceCacheReplayResult reports what one scope's replay copied.
//
// NOTE on side effects: replayed rows contribute to the syncer's
// side-effect state through STORE-DERIVED mechanisms, not through fields
// here — the needs_expansion index and the external-match existence bit
// are maintained for replayed rows exactly as for fresh ones, and the
// syncer's ingestion invariants read the store (see
// docs/tasks/source-cache-ingestion-invariants.md). The two fields that
// remain (ChildResources, RelatedResourceRows) are MECHANISMS, not
// evidence: child scheduling and related-resource recreation cannot be
// derived from the store after the fact, so replay must carry them.
type SourceCacheReplayResult struct {
	Rows int64
	// StaleSkipped counts index entries under the scope that did NOT
	// yield a copied row: the primary was missing, or its value stamp
	// named a different scope. This is the discriminator between "scope
	// legitimately empty" (index prefix empty, StaleSkipped == 0) and
	// "scope's rows were clobbered without index cleanup" (index says
	// rows existed, none survived the stamp check). The syncer fails a
	// replay that copies zero rows while StaleSkipped > 0.
	StaleSkipped int64
	// ChildResources lists replayed parents carrying ChildResourceType
	// annotations (resources replay only). See ReplayedParentResource.
	ChildResources []ReplayedParentResource
	// RelatedResourceRows counts resource rows copied from prev because
	// a replayed grant carried InsertResourceGrants (grants replay
	// only). Those resources exist ONLY as grant-driven insertions —
	// no ListResources response ever returns them — so the replay must
	// recreate them or the current sync holds grants whose resources
	// do not exist.
	RelatedResourceRows int64
}

// SourceCacheManifestSnapshot returns every manifest entry as
// "row_kind\x00scope_key" -> etag. Inspection surface: the
// replay-equivalence harness compares a warm sync's manifest against a
// cold sync's (they must be identical — same scopes, same validators),
// catching phantom or missing entries that are invisible through the v2
// read API but poison FUTURE syncs' replays.
func (e *Engine) SourceCacheManifestSnapshot(ctx context.Context) (map[string]string, error) {
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: encodeSourceCachePrefix(),
		UpperBound: upperBoundOf(encodeSourceCachePrefix()),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	out := map[string]string{}
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		rec := &v3.SourceCacheEntryRecord{}
		if err := unmarshalRecord(iter.Value(), rec); err != nil {
			return nil, fmt.Errorf("SourceCacheManifestSnapshot: unmarshal: %w", err)
		}
		out[rec.GetRowKind()+"\x00"+rec.GetScopeKey()] = rec.GetCacheValidator()
	}
	return out, iter.Error()
}

// SourceScopeIndexSnapshot returns, per row kind, scope_key -> the
// number of by_source_scope index entries. Same inspection rationale as
// SourceCacheManifestSnapshot: stamp/index divergence between a warm and
// a cold sync is invisible to v2 reads until a later sync replays from
// the damaged file.
func (e *Engine) SourceScopeIndexSnapshot(ctx context.Context) (map[string]map[string]int, error) {
	out := map[string]map[string]int{}
	families := []struct {
		kind  string
		lower []byte
	}{
		{"resources", ResourceBySourceScopeLowerBound()},
		{"entitlements", EntitlementBySourceScopeLowerBound()},
		{"grants", GrantBySourceScopeLowerBound()},
	}
	for _, fam := range families {
		prefix := codec.AppendTupleSeparator(append([]byte{}, fam.lower...))
		iter, err := e.db.NewIter(&pebble.IterOptions{
			LowerBound: prefix,
			UpperBound: upperBoundOf(fam.lower),
		})
		if err != nil {
			return nil, err
		}
		counts := map[string]int{}
		for iter.First(); iter.Valid(); iter.Next() {
			if err := ctx.Err(); err != nil {
				iter.Close()
				return nil, err
			}
			tail := iter.Key()[len(prefix):]
			scope, _, ok := codec.DecodeTupleStringAlias(tail, 0)
			if !ok {
				iter.Close()
				return nil, fmt.Errorf("SourceScopeIndexSnapshot: malformed %s index key %x", fam.kind, iter.Key())
			}
			counts[string(scope)]++
		}
		if err := iter.Error(); err != nil {
			iter.Close()
			return nil, err
		}
		iter.Close()
		out[fam.kind] = counts
	}
	return out, nil
}

// countSourceScopeIndexRange counts the index entries under one scope's
// by_source_scope prefix — a contiguous key-only scan, no value reads.
func (e *Engine) countSourceScopeIndexRange(ctx context.Context, prefix []byte) (int64, error) {
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upperBoundOf(prefix),
	})
	if err != nil {
		return 0, err
	}
	defer iter.Close()
	var n int64
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		n++
	}
	return n, iter.Error()
}

// verifyReplayedScopeCount is the replay-time count check: immediately
// after a replay's final commit, the destination's scope-index range must
// hold exactly the rows the copy claims to have written. Sound because
// the copy is the scope's first writer in the sync (fresh rows for a
// replayed scope arrive only via the overlay, after this) and a resumed
// re-copy overwrites the identical identity set. A mismatch means a
// partial copy landed — the silent-bad-data class — so it fails the
// replay rather than letting the manifest entry seal it as complete.
func (e *Engine) verifyReplayedScopeCount(ctx context.Context, kind, scopeKey string, prefix []byte, copied int64) error {
	got, err := e.countSourceScopeIndexRange(ctx, prefix)
	if err != nil {
		return fmt.Errorf("source cache replay: recount %s scope %q: %w", kind, scopeKey, err)
	}
	if got != copied {
		return fmt.Errorf(
			"source cache replay: %s scope %q index holds %d entries after copy but %d rows were copied (partial replay copy)",
			kind, scopeKey, got, copied)
	}
	return nil
}

// SourceCacheOrphanScopes returns, per row kind, every scope key present
// in a by_source_scope index that has NO manifest entry. At a sealed
// sync's quiesce point this is an invariant violation: every stamped row
// was written under a scope whose completion writes the manifest entry,
// and post-processing (external match, expansion) only re-stamps rows
// with scopes that already completed. An orphan means a manifest write
// was lost or a stamp leaked — either would poison the NEXT sync's
// replay. One skip-scan per distinct scope; O(distinct scopes) seeks.
func (e *Engine) SourceCacheOrphanScopes(ctx context.Context) (map[string][]string, error) {
	out := map[string][]string{}
	families := []struct {
		kind   string
		lower  []byte
		prefix func(scope string) []byte
	}{
		{"resources", ResourceBySourceScopeLowerBound(), encodeResourceBySourceScopePrefix},
		{"entitlements", EntitlementBySourceScopeLowerBound(), encodeEntitlementBySourceScopePrefix},
		{"grants", GrantBySourceScopeLowerBound(), encodeGrantBySourceScopePrefix},
	}
	for _, fam := range families {
		famPrefix := codec.AppendTupleSeparator(append([]byte{}, fam.lower...))
		iter, err := e.db.NewIter(&pebble.IterOptions{
			LowerBound: famPrefix,
			UpperBound: upperBoundOf(fam.lower),
		})
		if err != nil {
			return nil, err
		}
		for valid := iter.First(); valid; {
			if err := ctx.Err(); err != nil {
				iter.Close()
				return nil, err
			}
			tail := iter.Key()[len(famPrefix):]
			scopeBytes, _, ok := codec.DecodeTupleStringAlias(tail, 0)
			if !ok {
				iter.Close()
				return nil, fmt.Errorf("SourceCacheOrphanScopes: malformed %s index key %x", fam.kind, iter.Key())
			}
			scope := string(scopeBytes)
			if _, closer, getErr := e.db.Get(encodeSourceCacheEntryKey(fam.kind, scope)); getErr == nil {
				closer.Close()
			} else if errors.Is(getErr, pebble.ErrNotFound) {
				out[fam.kind] = append(out[fam.kind], scope)
			} else {
				iter.Close()
				return nil, getErr
			}
			// Skip the rest of this scope's entries.
			valid = iter.SeekGE(upperBoundOf(fam.prefix(scope)))
		}
		if err := iter.Error(); err != nil {
			iter.Close()
			return nil, err
		}
		iter.Close()
	}
	if len(out) == 0 {
		return nil, nil
	}
	for _, scopes := range out {
		sort.Strings(scopes)
	}
	return out, nil
}

// ClearSourceCacheEntries removes every source-cache manifest entry.
// Compaction calls this on fold outputs: a keep-newer upsert merge is not
// a faithful snapshot of ANY input sync (base rows the newer sync deleted
// survive), so no input's validator may be allowed to 304-validate the
// merged row set. With the manifest empty, every lookup against the
// artifact misses and the next sync fetches fresh — cold-correct. The
// scope stamps and scope-index entries on rows are left in place: replay
// only reads them after a manifest hit, which can no longer happen.
func (e *Engine) ClearSourceCacheEntries(ctx context.Context) error {
	return e.withWrite(func() error {
		if err := e.requireCurrentSync(); err != nil {
			return err
		}
		return e.db.DeleteRange(SourceCacheEntryLowerBound(), SourceCacheEntryUpperBound(), writeOpts(e.opts.durability))
	})
}

// PutSourceCacheEntry writes the manifest entry for (rowKind, scopeKey).
// Zero-row scopes still get entries — the validator must survive to the
// next sync even when the scope produced no rows.
func (e *Engine) PutSourceCacheEntry(ctx context.Context, rowKind, scopeKey, cacheValidator string) error {
	return e.withWrite(func() error {
		if err := e.requireCurrentSync(); err != nil {
			return err
		}
		rec := &v3.SourceCacheEntryRecord{}
		rec.SetRowKind(rowKind)
		rec.SetScopeKey(scopeKey)
		rec.SetCacheValidator(cacheValidator)
		rec.SetDiscoveredAt(timestamppb.Now())
		val, err := marshalRecord(rec)
		if err != nil {
			return err
		}
		opts := writeOpts(e.opts.durability)
		if e.IsFreshSync() {
			opts = pebble.NoSync
		}
		return e.db.Set(encodeSourceCacheEntryKey(rowKind, scopeKey), val, opts)
	})
}

// GetSourceCacheEntry returns the manifest entry for (rowKind, scopeKey),
// or pebble.ErrNotFound.
func (e *Engine) GetSourceCacheEntry(ctx context.Context, rowKind, scopeKey string) (*v3.SourceCacheEntryRecord, error) {
	val, closer, err := e.db.Get(encodeSourceCacheEntryKey(rowKind, scopeKey))
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	rec := &v3.SourceCacheEntryRecord{}
	if err := unmarshalRecord(val, rec); err != nil {
		return nil, fmt.Errorf("GetSourceCacheEntry: unmarshal: %w", err)
	}
	return rec, nil
}

// DeleteGrantsByPrincipalsInScope deletes every grant row in the CURRENT
// store stamped with scopeKey whose principal id is in principalIDs —
// the engine side of principal-scoped delta tombstones
// (SourceCacheRecord.deleted_principal_ids).
//
// One prefix scan of the scope's by_source_scope index resolves
// everything: the index tail IS the grant identity, so the primary key
// and every secondary index key for a match are constructible from the
// index key alone — no value reads, no string resolution, no guessing.
// A principal with no rows in the scope is a no-op (providers tombstone
// objects the client never synced). Deleting a missing secondary index
// entry is a pebble no-op, which covers the mixed inline/deferred
// by_principal state mid-sync.
//
// Complexity: O(scope size) tuple-walks per call regardless of tombstone
// count — callers batch a page's tombstones into one call.
func (e *Engine) DeleteGrantsByPrincipalsInScope(ctx context.Context, scopeKey string, principalIDs map[string]struct{}) (int64, error) {
	if len(principalIDs) == 0 {
		return 0, nil
	}
	prefix := encodeGrantBySourceScopePrefix(scopeKey)
	var deleted int64

	err := e.withWrite(func() error {
		if err := e.requireCurrentSync(); err != nil {
			return err
		}
		iter, err := e.db.NewIter(&pebble.IterOptions{
			LowerBound: prefix,
			UpperBound: upperBoundOf(prefix),
		})
		if err != nil {
			return err
		}
		defer iter.Close()

		opts := writeOpts(e.opts.durability)
		if e.IsFreshSync() {
			opts = pebble.NoSync
		}
		batch := e.db.NewBatch()
		defer func() { _ = batch.Close() }()

		for iter.First(); iter.Valid(); iter.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			key := iter.Key()
			tail := key[len(prefix):]
			// Tail layout: ent_rt | ent_rid | flag | ent_tail | prin_rt | prin_id
			// (identical to the grant primary key tail; decoder shared with
			// the primary-prefix scan paths in grants.go).
			id, ok := decodeGrantIdentityTail(key, prefix)
			if !ok {
				continue // malformed index key — defensive skip
			}
			if _, hit := principalIDs[id.principalID]; !hit {
				continue
			}
			// Primary key = grant header + the identity tail verbatim.
			priKey := make([]byte, 0, 3+len(tail))
			priKey = append(priKey, versionV3, typeGrant)
			priKey = codec.AppendTupleSeparator(priKey)
			priKey = append(priKey, tail...)
			if err := batch.Delete(priKey, nil); err != nil {
				return err
			}
			if err := batch.Delete(encodeGrantByPrincipalIdentityIndexKey(id), nil); err != nil {
				return err
			}
			if err := batch.Delete(encodeGrantByNeedsExpansionIdentityIndexKey(id), nil); err != nil {
				return err
			}
			// The scope index entry itself (the key under the iterator —
			// safe: the iterator reads a snapshot).
			if err := batch.Delete(key, nil); err != nil {
				return err
			}
			deleted++
		}
		if err := iter.Error(); err != nil {
			return err
		}
		if err := batch.Commit(opts); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return deleted, nil
}

// DeleteGrantsByExternalIDsInScope deletes every grant row in the CURRENT
// store stamped with scopeKey whose STORED grant id (external id, which
// may be a connector-custom shape) is in ids. One scan of the scope's
// index, loading each candidate's primary row to compare the stored id —
// bounded by the scope's row count, never the whole keyspace. This is the
// tombstone path for connectors with custom grant ids whose scopes span
// multiple resources (so principal-scoped deletes would over-delete).
func (e *Engine) DeleteGrantsByExternalIDsInScope(ctx context.Context, scopeKey string, ids map[string]struct{}) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	prefix := encodeGrantBySourceScopePrefix(scopeKey)
	var deleted int64

	err := e.withWrite(func() error {
		if err := e.requireCurrentSync(); err != nil {
			return err
		}
		iter, err := e.db.NewIter(&pebble.IterOptions{
			LowerBound: prefix,
			UpperBound: upperBoundOf(prefix),
		})
		if err != nil {
			return err
		}
		defer iter.Close()

		opts := writeOpts(e.opts.durability)
		if e.IsFreshSync() {
			opts = pebble.NoSync
		}
		batch := e.db.NewBatch()
		defer func() { _ = batch.Close() }()

		for iter.First(); iter.Valid(); iter.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			key := iter.Key()
			tail := key[len(prefix):]
			id, ok := decodeGrantIdentityTail(key, prefix)
			if !ok {
				continue // malformed index key — defensive skip
			}
			// Primary key = grant header + the identity tail verbatim.
			priKey := make([]byte, 0, 3+len(tail))
			priKey = append(priKey, versionV3, typeGrant)
			priKey = codec.AppendTupleSeparator(priKey)
			priKey = append(priKey, tail...)

			val, closer, err := e.db.Get(priKey)
			if err != nil {
				if errors.Is(err, pebble.ErrNotFound) {
					continue // index ahead of primary — defensive skip
				}
				return err
			}
			rec := &v3.GrantRecord{}
			uerr := unmarshalRecord(val, rec)
			_ = closer.Close()
			if uerr != nil {
				return fmt.Errorf("DeleteGrantsByExternalIDsInScope: unmarshal: %w", uerr)
			}
			if _, hit := ids[rec.GetExternalId()]; !hit {
				continue
			}
			if err := batch.Delete(priKey, nil); err != nil {
				return err
			}
			if err := batch.Delete(encodeGrantByPrincipalIdentityIndexKey(id), nil); err != nil {
				return err
			}
			if err := batch.Delete(encodeGrantByNeedsExpansionIdentityIndexKey(id), nil); err != nil {
				return err
			}
			if err := batch.Delete(key, nil); err != nil {
				return err
			}
			deleted++
		}
		if err := iter.Error(); err != nil {
			return err
		}
		if err := batch.Commit(opts); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return deleted, nil
}

// DeleteResourcesByIDsInScope deletes every resource row in the CURRENT
// store stamped with scopeKey whose resource id is in resourceIDs (any
// resource type) — principal-scoped tombstones for RowKindResources.
func (e *Engine) DeleteResourcesByIDsInScope(ctx context.Context, scopeKey string, resourceIDs map[string]struct{}) (int64, error) {
	if len(resourceIDs) == 0 {
		return 0, nil
	}
	prefix := encodeResourceBySourceScopePrefix(scopeKey)
	var deleted int64

	err := e.withWrite(func() error {
		if err := e.requireCurrentSync(); err != nil {
			return err
		}
		iter, err := e.db.NewIter(&pebble.IterOptions{
			LowerBound: prefix,
			UpperBound: upperBoundOf(prefix),
		})
		if err != nil {
			return err
		}
		defer iter.Close()

		opts := writeOpts(e.opts.durability)
		if e.IsFreshSync() {
			opts = pebble.NoSync
		}
		batch := e.db.NewBatch()
		defer func() { _ = batch.Close() }()

		for iter.First(); iter.Valid(); iter.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			key := iter.Key()
			tail := key[len(prefix):]
			// Tail layout: resource_type_id | resource_id.
			rtBytes, next, ok := codec.DecodeTupleStringAlias(tail, 0)
			if !ok || next >= len(tail) {
				continue
			}
			ridBytes, _, ok := codec.DecodeTupleStringAlias(tail, next+1)
			if !ok {
				continue
			}
			if _, hit := resourceIDs[string(ridBytes)]; !hit {
				continue
			}
			rt, rid := string(rtBytes), string(ridBytes)
			priKey := make([]byte, 0, 3+len(tail))
			priKey = append(priKey, versionV3, typeResource)
			priKey = codec.AppendTupleSeparator(priKey)
			priKey = append(priKey, tail...)
			// by_parent cleanup needs the parent ref from the value.
			if val, closer, getErr := e.db.Get(priKey); getErr == nil {
				err := e.deleteResourceIndexesRaw(batch, rt, rid, val)
				closer.Close()
				if err != nil {
					return err
				}
			} else if !errors.Is(getErr, pebble.ErrNotFound) {
				return getErr
			}
			if err := batch.Delete(priKey, nil); err != nil {
				return err
			}
			// deleteResourceIndexesRaw already covered the scope entry
			// (source_scope_key is in the value), but delete the iterated
			// key too in case the value read missed (orphan entry).
			if err := batch.Delete(key, nil); err != nil {
				return err
			}
			deleted++
		}
		if err := iter.Error(); err != nil {
			return err
		}
		return batch.Commit(opts)
	})
	if err != nil {
		return 0, err
	}
	return deleted, nil
}

// grantValueHasSourcesRaw reports whether a marshaled GrantRecord carries
// at least one sources entry (field 9), without unmarshaling.
func grantValueHasSourcesRaw(value []byte) (bool, error) {
	for len(value) > 0 {
		num, typ, n := protowire.ConsumeTag(value)
		if n < 0 {
			return false, protowire.ParseError(n)
		}
		value = value[n:]
		if num == 9 {
			return true, nil
		}
		n = protowire.ConsumeFieldValue(num, typ, value)
		if n < 0 {
			return false, protowire.ParseError(n)
		}
		value = value[n:]
	}
	return false, nil
}

// stripExpanderSourcesRaw clears a replayed grant's Sources map when it is
// expander-written, so the current sync's expansion recomputes it from
// true state instead of inheriting contributions that may have been
// removed upstream. Classification mirrors RollbackExpansion: a Sources
// map containing a self-source entry (keyed by the grant's own entitlement
// id) was written by the expander; one without a self-source is
// connector-set public data and is preserved. Returns (newValue, true) when
// the record was rewritten, (nil, false) when the original bytes should be
// copied verbatim.
func stripExpanderSourcesRaw(value []byte, ownEntitlementID string) ([]byte, bool, error) {
	r := &v3.GrantRecord{}
	if err := unmarshalRecord(value, r); err != nil {
		return nil, false, fmt.Errorf("source cache replay: unmarshal grant for sources strip: %w", err)
	}
	sources := r.GetSources()
	if len(sources) == 0 {
		return nil, false, nil
	}
	if _, hasSelf := sources[ownEntitlementID]; !hasSelf {
		// No self-source: connector-set Sources. Preserve verbatim.
		return nil, false, nil
	}
	r.SetSources(nil)
	stripped, err := marshalRecord(r)
	if err != nil {
		return nil, false, fmt.Errorf("source cache replay: re-marshal grant after sources strip: %w", err)
	}
	return stripped, true, nil
}

// decodeResourcePrimaryTail decodes (resource_type_id, resource_id)
// from a resource primary key (v3 | typeResource | 0x00 | rt | 0x00 | rid).
func decodeResourcePrimaryTail(priKey []byte) (string, string, error) {
	const headerLen = 3 // versionV3, typeResource, separator
	if len(priKey) <= headerLen {
		return "", "", fmt.Errorf("source cache replay: malformed resource primary key %x", priKey)
	}
	tail := priKey[headerLen:]
	rtBytes, next, err := codec.DecodeTupleStringTo(nil, tail, 0)
	if err != nil {
		return "", "", err
	}
	if next >= len(tail) {
		return "", "", fmt.Errorf("source cache replay: resource primary key missing resource_id: %x", priKey)
	}
	ridBytes, _, err := codec.DecodeTupleStringTo(nil, tail, next+1)
	if err != nil {
		return "", "", err
	}
	return string(rtBytes), string(ridBytes), nil
}

// replayPrimaryFromIndexKey derives a record's primary key from its
// by_source_scope index key. The index prefix (header|0x00|scope|0x00)
// is followed by exactly the identity tuple that forms the primary
// key's tail, so the primary is header' + 0x00-separated remainder.
func replayPrimaryFromIndexKey(indexKey, indexPrefix []byte, primaryHeader [2]byte) ([]byte, error) {
	if len(indexKey) <= len(indexPrefix) {
		return nil, fmt.Errorf("source cache replay: malformed index key %x", indexKey)
	}
	tail := indexKey[len(indexPrefix):]
	key := make([]byte, 0, 3+len(tail))
	key = append(key, primaryHeader[0], primaryHeader[1], 0x00)
	return append(key, tail...), nil
}

// ReplaySourceCacheGrants copies every grant stamped with scopeKey from
// prev into the receiver: raw primary copy plus index synthesis from the
// raw value (principal, needs_expansion, source-scope families). Mirrors
// PutGrantRecords' read-before-write index cleanup when the receiver
// already holds a record at the same identity.
func (e *Engine) ReplaySourceCacheGrants(ctx context.Context, prev *Engine, scopeKey string) (SourceCacheReplayResult, error) {
	var res SourceCacheReplayResult
	prefix := encodeGrantBySourceScopePrefix(scopeKey)
	primaryHeader := [2]byte{versionV3, typeGrant}

	err := e.withWrite(func() error {
		if err := e.requireCurrentSync(); err != nil {
			return err
		}
		iter, err := prev.db.NewIter(&pebble.IterOptions{
			LowerBound: prefix,
			UpperBound: upperBoundOf(prefix),
		})
		if err != nil {
			return err
		}
		defer iter.Close()

		opts := writeOpts(e.opts.durability)
		if e.IsFreshSync() {
			opts = pebble.NoSync
		}
		batch := e.db.NewBatch()
		defer func() { _ = batch.Close() }()
		rowsInBatch := 0

		// Consumed in a defer keyed on rows whose commit LANDED, not on
		// success: a replay that fails after an intermediate commit
		// (large scope, error or cancellation mid-copy) has already
		// populated the keyspace, and while the failing action unwinds,
		// concurrently draining workers can still write — a first
		// PutGrantRecords taking the empty-keyspace fast path over
		// partially replayed identities would skip index cleanup.
		//
		// sawRelatedResource: InsertResourceGrants copies write resource
		// rows too, so the resources empty-keyspace fast path must also
		// be disarmed. Related copies always ride a batch that carries
		// at least their own grant row, so committedRows > 0 covers
		// "any batch that may hold a resource write landed".
		committedRows := 0
		sawRelatedResource := false
		defer func() {
			if committedRows > 0 {
				_ = e.takeFreshGrantsEmpty()
				if sawRelatedResource {
					_ = e.takeFreshResourcesEmpty()
				}
			}
		}()
		// relatedCopied dedupes InsertResourceGrants resource copies:
		// many grants in one scope typically reference few resources.
		var relatedCopied map[string]struct{}

		for iter.First(); iter.Valid(); iter.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			priKey, err := replayPrimaryFromIndexKey(iter.Key(), prefix, primaryHeader)
			if err != nil {
				return err
			}
			val, closer, getErr := prev.db.Get(priKey)
			if getErr != nil {
				if errors.Is(getErr, pebble.ErrNotFound) {
					// Orphan index entry in the previous file — skip,
					// matching the defensive-skip semantic of the other
					// index read paths.
					res.StaleSkipped++
					continue
				}
				return fmt.Errorf("source cache replay: get prev grant: %w", getErr)
			}

			entRT, entRID, entID, principalRT, principalID, needsExpansion, srcScope, scanErr := scanGrantIndexFieldsRaw(val)
			if scanErr != nil {
				closer.Close()
				return scanErr
			}
			// Stale-index defense: only copy rows whose VALUE stamp matches
			// the queried scope. An index entry pointing at a row stamped
			// differently (or not at all) is left over from a path that
			// replaced the row without cleaning the index — e.g. a fold
			// compaction predating the source-cache bucket plans, or an
			// in-sync same-identity rewrite under a different scope. Copying
			// it would inject rows upstream never returned for this scope.
			if srcScope != scopeKey {
				closer.Close()
				res.StaleSkipped++
				continue
			}

			// Clean up index entries for any record the current sync
			// already wrote at this identity (same discipline as
			// PutGrantRecords' read-before-write).
			if oldVal, oldCloser, oldErr := e.db.Get(priKey); oldErr == nil {
				if err := e.deleteGrantIndexesRaw(batch, "", oldVal); err != nil {
					oldCloser.Close()
					closer.Close()
					return err
				}
				oldCloser.Close()
			} else if !errors.Is(oldErr, pebble.ErrNotFound) {
				closer.Close()
				return fmt.Errorf("source cache replay: get current grant: %w", oldErr)
			}

			// Replay-equivalence: a cached sync must reproduce what a full
			// resync would produce. The one field where a verbatim copy
			// diverges is expander-written Sources — the previous sync's
			// expansion baked contributions into direct grants, and
			// re-expansion only ADDS, so a contribution removed this sync
			// (via a delta tombstone or a refetched page) would survive
			// forever. Strip expander-written Sources so the current sync's
			// expansion recomputes them from true state; connector-set
			// Sources (no self-source entry — same classification as
			// RollbackExpansion) are connector data and are preserved
			// verbatim. The probe is a cheap protowire scan; the vast
			// majority of rows carry no Sources and stay on the raw-copy
			// path.
			writeVal := val
			if hasSources, probeErr := grantValueHasSourcesRaw(val); probeErr != nil {
				closer.Close()
				return probeErr
			} else if hasSources {
				stripped, strippedOK, stripErr := stripExpanderSourcesRaw(val, entID)
				if stripErr != nil {
					closer.Close()
					return stripErr
				}
				if strippedOK {
					writeVal = stripped
				}
			}
			// Dense ingestion facts for replayed rows: same evidence the
			// put path maintains, so the syncer's store-derived seams
			// (I2) see replayed and fresh rows identically.
			flags, flagsErr := scanGrantSourceCacheFlagsRaw(val)
			if flagsErr != nil {
				closer.Close()
				return flagsErr
			}
			if flags.externalResourceMatch {
				if err := e.markExternalMatchFact(); err != nil {
					closer.Close()
					return err
				}
			}
			if flags.insertResourceGrants && entRT != "" && entRID != "" {
				resKey := encodeResourceKey(entRT, entRID)
				if _, done := relatedCopied[string(resKey)]; !done {
					if relatedCopied == nil {
						relatedCopied = make(map[string]struct{})
					}
					relatedCopied[string(resKey)] = struct{}{}
					copied, relErr := e.replayRelatedResourceRaw(batch, prev, resKey, entRT, entRID)
					if relErr != nil {
						closer.Close()
						return relErr
					}
					if copied {
						res.RelatedResourceRows++
						sawRelatedResource = true
					}
				}
			}

			if err := batch.Set(priKey, writeVal, nil); err != nil {
				closer.Close()
				return err
			}
			closer.Close()

			if entID != "" && entRT != "" && entRID != "" && principalRT != "" && principalID != "" {
				id := grantIdentity{
					entitlement:     entitlementIdentityFromParts(entRT, entRID, entID),
					principalTypeID: principalRT,
					principalID:     principalID,
				}
				if _, err := e.writeGrantIndexesForIdentityScratch(batch, id, needsExpansion, srcScope, nil); err != nil {
					return err
				}
			}
			res.Rows++
			rowsInBatch++
			if rowsInBatch >= replayBatchRows {
				if err := batch.Commit(opts); err != nil {
					return err
				}
				committedRows += rowsInBatch
				_ = batch.Close()
				batch = e.db.NewBatch()
				rowsInBatch = 0
			}
		}
		if err := iter.Error(); err != nil {
			return err
		}
		if err := batch.Commit(opts); err != nil {
			return err
		}
		// Replay populated the fresh sync's grant keyspace directly. The
		// first overlay PutGrantRecords must therefore perform its normal
		// read-before-write index cleanup, rather than claiming the
		// keyspace is still empty and leaving replayed index entries stale
		// (consumed by the defer above).
		committedRows += rowsInBatch
		return e.verifyReplayedScopeCount(ctx, "grants", scopeKey, prefix, res.Rows)
	})
	if err != nil {
		return SourceCacheReplayResult{}, err
	}
	return res, nil
}

// CopyResourceRowFrom copies one resource row (primary + secondary
// indexes) from prev into the receiver, byte-verbatim. The repair side of
// ingestion invariant I3: when a grant-inserted resource row was lost by
// a replay-path failure, the previous sync's row is the full-fidelity
// source of truth (it was cold-materialized from the wire grant's
// embedded resource, which grant STORAGE does not retain). Returns false
// when prev has no such row — repair impossible.
func (e *Engine) CopyResourceRowFrom(ctx context.Context, prev *Engine, resourceTypeID, resourceID string) (bool, error) {
	copied := false
	err := e.withWrite(func() error {
		if err := e.requireCurrentSync(); err != nil {
			return err
		}
		batch := e.db.NewBatch()
		defer batch.Close()
		ok, err := e.replayRelatedResourceRaw(batch, prev, encodeResourceKey(resourceTypeID, resourceID), resourceTypeID, resourceID)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		if err := batch.Commit(writeOpts(e.opts.durability)); err != nil {
			return err
		}
		_ = e.takeFreshResourcesEmpty()
		copied = true
		return nil
	})
	return copied, err
}

// replayRelatedResourceRaw copies one resource row from prev into batch
// (primary + secondary indexes), byte-verbatim — the engine side of
// recreating InsertResourceGrants-inserted resources when their grants
// page replays. Returns false when prev holds no such row: the previous
// sync's output didn't have it either, so there is nothing to preserve.
func (e *Engine) replayRelatedResourceRaw(batch *pebble.Batch, prev *Engine, priKey []byte, rt, rid string) (bool, error) {
	val, closer, err := prev.db.Get(priKey)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return false, nil
		}
		return false, fmt.Errorf("source cache replay: get prev related resource: %w", err)
	}
	defer closer.Close()

	parentRT, parentID, srcScope, err := scanResourceIndexFieldsRaw(val)
	if err != nil {
		return false, err
	}
	// Clean up index entries for any record the current sync already
	// wrote at this identity (same discipline as the resource replay).
	if oldVal, oldCloser, oldErr := e.db.Get(priKey); oldErr == nil {
		err := e.deleteResourceIndexesRaw(batch, rt, rid, oldVal)
		oldCloser.Close()
		if err != nil {
			return false, err
		}
	} else if !errors.Is(oldErr, pebble.ErrNotFound) {
		return false, fmt.Errorf("source cache replay: get current related resource: %w", oldErr)
	}
	if err := batch.Set(priKey, val, nil); err != nil {
		return false, err
	}
	if parentID != "" {
		if err := batch.Set(encodeResourceByParentIndexKey(parentRT, parentID, rt, rid), nil, nil); err != nil {
			return false, err
		}
	}
	if srcScope != "" {
		if err := batch.Set(encodeResourceBySourceScopeIndexKey(srcScope, rt, rid), nil, nil); err != nil {
			return false, err
		}
	}
	return true, nil
}

// ReplaySourceCacheEntitlements copies every entitlement stamped with
// scopeKey from prev into the receiver.
func (e *Engine) ReplaySourceCacheEntitlements(ctx context.Context, prev *Engine, scopeKey string) (SourceCacheReplayResult, error) {
	var res SourceCacheReplayResult
	prefix := encodeEntitlementBySourceScopePrefix(scopeKey)
	primaryHeader := [2]byte{versionV3, typeEntitlement}

	err := e.withWrite(func() error {
		if err := e.requireCurrentSync(); err != nil {
			return err
		}
		iter, err := prev.db.NewIter(&pebble.IterOptions{
			LowerBound: prefix,
			UpperBound: upperBoundOf(prefix),
		})
		if err != nil {
			return err
		}
		defer iter.Close()

		opts := writeOpts(e.opts.durability)
		if e.IsFreshSync() {
			opts = pebble.NoSync
		}
		batch := e.db.NewBatch()
		defer func() { _ = batch.Close() }()
		rowsInBatch := 0

		// Keyed on rows whose commit LANDED, not on success (see the
		// grants replay): a partial replay has already mutated the
		// entitlement keyspace, so the bare-id lookup map must be
		// invalidated and the empty-keyspace fast path disarmed even when
		// the replay itself fails — draining workers can still resolve
		// tombstones and write entitlements while the failure unwinds.
		committedRows := 0
		defer func() {
			if committedRows > 0 {
				e.noteEntitlementKeyspaceWrite()
				_ = e.takeFreshEntitlementsEmpty()
			}
		}()

		for iter.First(); iter.Valid(); iter.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			priKey, err := replayPrimaryFromIndexKey(iter.Key(), prefix, primaryHeader)
			if err != nil {
				return err
			}
			val, closer, getErr := prev.db.Get(priKey)
			if getErr != nil {
				if errors.Is(getErr, pebble.ErrNotFound) {
					res.StaleSkipped++
					continue
				}
				return fmt.Errorf("source cache replay: get prev entitlement: %w", getErr)
			}
			// Stale-index defense: only copy rows whose VALUE stamp matches
			// the queried scope (see the grants replay for rationale).
			prevScope, prevScanErr := scanEntitlementSourceScopeRaw(val)
			if prevScanErr != nil {
				closer.Close()
				return prevScanErr
			}
			if prevScope != scopeKey {
				closer.Close()
				res.StaleSkipped++
				continue
			}
			// The replayed row's source-scope index key is byte-identical
			// to prev's — the only entitlement secondary index. Clean up a
			// differing stamp on any record the current sync already wrote
			// at this identity.
			if oldVal, oldCloser, oldErr := e.db.Get(priKey); oldErr == nil {
				oldScope, scanErr := scanEntitlementSourceScopeRaw(oldVal)
				oldCloser.Close()
				if scanErr != nil {
					closer.Close()
					return scanErr
				}
				if oldScope != "" && oldScope != scopeKey {
					// The old index key's tail equals the primary key's
					// tail (identity tuple), so rebuild it byte-wise.
					oldIdxKey := make([]byte, 0, 8+len(oldScope)+len(priKey))
					oldIdxKey = append(oldIdxKey, versionV3, typeIndex, idxEntitlementBySourceScope)
					oldIdxKey = codec.AppendTupleSeparator(oldIdxKey)
					oldIdxKey = codec.AppendTupleStrings(oldIdxKey, oldScope)
					oldIdxKey = codec.AppendTupleSeparator(oldIdxKey)
					oldIdxKey = append(oldIdxKey, priKey[3:]...)
					if err := batch.Delete(oldIdxKey, nil); err != nil {
						closer.Close()
						return err
					}
				}
			} else if !errors.Is(oldErr, pebble.ErrNotFound) {
				closer.Close()
				return fmt.Errorf("source cache replay: get current entitlement: %w", oldErr)
			}
			if err := batch.Set(priKey, val, nil); err != nil {
				closer.Close()
				return err
			}
			closer.Close()
			if err := batch.Set(iter.Key(), nil, nil); err != nil {
				return err
			}
			res.Rows++
			rowsInBatch++
			if rowsInBatch >= replayBatchRows {
				if err := batch.Commit(opts); err != nil {
					return err
				}
				committedRows += rowsInBatch
				_ = batch.Close()
				batch = e.db.NewBatch()
				rowsInBatch = 0
			}
		}
		if err := iter.Error(); err != nil {
			return err
		}
		if err := batch.Commit(opts); err != nil {
			return err
		}
		// The defer above invalidates the lazy bare-id lookup map (see
		// lookup.go — later tombstones would silently miss replayed rows
		// otherwise) and disarms the empty-keyspace fast path for the
		// first overlay PutEntitlementRecords. The bump happens strictly
		// AFTER the last commit that landed: a map build racing the
		// replay records the pre-bump generation and is correctly
		// invalidated; bumping earlier would let such a build record the
		// fresh generation against partially-replayed state.
		committedRows += rowsInBatch
		return e.verifyReplayedScopeCount(ctx, "entitlements", scopeKey, prefix, res.Rows)
	})
	if err != nil {
		return SourceCacheReplayResult{}, err
	}
	return res, nil
}

// ReplaySourceCacheResources copies every resource stamped with scopeKey
// from prev into the receiver, synthesizing by_parent and by_source_scope
// index entries from the raw value.
func (e *Engine) ReplaySourceCacheResources(ctx context.Context, prev *Engine, scopeKey string) (SourceCacheReplayResult, error) {
	var res SourceCacheReplayResult
	prefix := encodeResourceBySourceScopePrefix(scopeKey)
	primaryHeader := [2]byte{versionV3, typeResource}

	err := e.withWrite(func() error {
		if err := e.requireCurrentSync(); err != nil {
			return err
		}
		iter, err := prev.db.NewIter(&pebble.IterOptions{
			LowerBound: prefix,
			UpperBound: upperBoundOf(prefix),
		})
		if err != nil {
			return err
		}
		defer iter.Close()

		opts := writeOpts(e.opts.durability)
		if e.IsFreshSync() {
			opts = pebble.NoSync
		}
		batch := e.db.NewBatch()
		defer func() { _ = batch.Close() }()
		rowsInBatch := 0

		// Keyed on rows whose commit LANDED, not on success — see the
		// grants replay for rationale.
		committedRows := 0
		defer func() {
			if committedRows > 0 {
				_ = e.takeFreshResourcesEmpty()
			}
		}()

		for iter.First(); iter.Valid(); iter.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			priKey, err := replayPrimaryFromIndexKey(iter.Key(), prefix, primaryHeader)
			if err != nil {
				return err
			}
			val, closer, getErr := prev.db.Get(priKey)
			if getErr != nil {
				if errors.Is(getErr, pebble.ErrNotFound) {
					res.StaleSkipped++
					continue
				}
				return fmt.Errorf("source cache replay: get prev resource: %w", getErr)
			}

			rt, rid, decodeErr := decodeResourcePrimaryTail(priKey)
			if decodeErr != nil {
				closer.Close()
				return decodeErr
			}
			parentRT, parentID, srcScope, scanErr := scanResourceIndexFieldsRaw(val)
			if scanErr != nil {
				closer.Close()
				return scanErr
			}
			// Stale-index defense: only copy rows whose VALUE stamp matches
			// the queried scope (see the grants replay for rationale).
			if srcScope != scopeKey {
				closer.Close()
				res.StaleSkipped++
				continue
			}
			// Child scheduling is response-row-driven in the syncer; a
			// replayed parent never reaches that path, so surface its
			// child types for the syncer to schedule.
			childTypeIDs, childErr := scanResourceChildTypeIDsRaw(val)
			if childErr != nil {
				closer.Close()
				return childErr
			}
			if len(childTypeIDs) > 0 {
				res.ChildResources = append(res.ChildResources, ReplayedParentResource{
					ResourceTypeID: rt,
					ResourceID:     rid,
					ChildTypeIDs:   childTypeIDs,
				})
			}
			if oldVal, oldCloser, oldErr := e.db.Get(priKey); oldErr == nil {
				if err := e.deleteResourceIndexesRaw(batch, rt, rid, oldVal); err != nil {
					oldCloser.Close()
					closer.Close()
					return err
				}
				oldCloser.Close()
			} else if !errors.Is(oldErr, pebble.ErrNotFound) {
				closer.Close()
				return fmt.Errorf("source cache replay: get current resource: %w", oldErr)
			}

			if err := batch.Set(priKey, val, nil); err != nil {
				closer.Close()
				return err
			}
			closer.Close()

			if parentID != "" {
				if err := batch.Set(encodeResourceByParentIndexKey(parentRT, parentID, rt, rid), nil, nil); err != nil {
					return err
				}
			}
			if srcScope != "" {
				if err := batch.Set(encodeResourceBySourceScopeIndexKey(srcScope, rt, rid), nil, nil); err != nil {
					return err
				}
			}
			res.Rows++
			rowsInBatch++
			if rowsInBatch >= replayBatchRows {
				if err := batch.Commit(opts); err != nil {
					return err
				}
				committedRows += rowsInBatch
				_ = batch.Close()
				batch = e.db.NewBatch()
				rowsInBatch = 0
			}
		}
		if err := iter.Error(); err != nil {
			return err
		}
		if err := batch.Commit(opts); err != nil {
			return err
		}
		// See grant replay above: direct replay writes mean the first
		// overlay PutResourceRecords must not use the empty-keyspace
		// fast-path, or old by_parent/by_source_scope entries survive
		// (consumed by the defer above).
		committedRows += rowsInBatch
		return e.verifyReplayedScopeCount(ctx, "resources", scopeKey, prefix, res.Rows)
	})
	if err != nil {
		return SourceCacheReplayResult{}, err
	}
	return res, nil
}
