package pebble

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/cockroachdb/pebble/v2"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/types/known/timestamppb"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/codec"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/internal/rawdb"
)

// Source-cache replay, engine side.
//
// The typeSourceCache keyspace holds one SourceCacheEntryRecord per
// (row_kind, scope_hash): the opaque upstream validator (etag / delta
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

func (e *Engine) sourceCacheReplayBatchLimit() int {
	if e.test.sourceCacheReplayBatchRows > 0 {
		return e.test.sourceCacheReplayBatchRows
	}
	return replayBatchRows
}

func (e *Engine) sourceCacheReplayIteratorError(kind string, iter *pebble.Iterator) error {
	if e.test.sourceCacheReplayIteratorErrorHook != nil {
		if err := e.test.sourceCacheReplayIteratorErrorHook(kind); err != nil {
			return err
		}
	}
	return iter.Error()
}

// SourceCacheReplayResult reports what one scope's replay copied.
type SourceCacheReplayResult struct {
	Rows int64
	// NeedsExpansion is true when at least one copied grant row carried
	// needs_expansion. Future syncer replay orchestration must consume this
	// signal to arm grant expansion: replayed pages never pass
	// GrantExpandable-annotated rows through the connector-response path.
	NeedsExpansion bool
	// StaleSkipped counts index entries under the scope that did NOT
	// yield a copied row: the primary was missing, or its value stamp
	// named a different scope.
	//
	// It is zero on every path that returns a result today, and replay
	// orchestration must NOT gate on it. validateReplaySourceScope runs
	// first over the same index range on the same immutable source and
	// hard-errors on exactly these two conditions, so a replay that
	// reaches the copy loop has already proven neither can occur. The
	// discriminator this field once provided — "scope legitimately empty"
	// versus "scope's rows were clobbered without index cleanup" — is the
	// preflight's error, not a counter the caller inspects.
	//
	// The increments below are kept as defense in depth, and remain
	// meaningful only if that preflight is ever relaxed: a non-zero value
	// means the preflight and the copy loop disagree, which is a bug in
	// one of them rather than a state the source can legitimately be in.
	StaleSkipped int64
}

// PutSourceCacheEntry writes the manifest entry for (rowKind, scopeKey).
// Zero-row scopes still get entries — the validator must survive to the
// next sync even when the scope produced no rows.
func (e *Engine) PutSourceCacheEntry(ctx context.Context, rowKind, scopeKey, cacheValidator string) error {
	if cacheValidator == "" {
		return errors.New("source cache manifest: cache validator is required")
	}
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
		if e.test.sourceCacheManifestWriteHook != nil {
			if err := e.test.sourceCacheManifestWriteHook(); err != nil {
				return err
			}
		}
		return e.db.SourceCacheSet(encodeSourceCacheEntryKey(rowKind, scopeKey), val, opts)
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

// InvalidateSourceCacheReplayState removes upstream validators from a
// compaction output. Rebuild compactions also drop the source-scope indexes
// they synthesized while materializing winner rows; fold keeps those indexes
// because rebuilding or deleting them record-by-record would defeat fold's
// bounded-write design. Range tombstones make both forms O(1) in row count.
func (e *Engine) InvalidateSourceCacheReplayState(ctx context.Context, dropScopeIndexes bool) error {
	return e.withWriteAllowSealed(func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		batch := e.db.NewRecordBatch()
		defer batch.Close()
		if err := batch.StageSourceCacheReplayInvalidation(dropScopeIndexes); err != nil {
			return err
		}
		// Compaction does not publish the artifact until Close checkpoints and
		// fsyncs the engine. Match the fold batches' NoSync policy so this
		// constant-size tombstone does not add a standalone fsync.
		return batch.Commit(pebble.NoSync)
	})
}

// DeleteGrantRecordBounded deletes a grant by canonical public id WITHOUT
// the O(all grants) stored-external-id scan fallback that the interactive
// DeleteGrantRecord path is allowed to take. Used by the source-cache
// tombstone path, where a mass-removal round would otherwise pay a full
// keyspace scan PER already-absent id.
//
// Consequence, by design: a grant stored under a connector-CUSTOM id (one
// that isn't the SDK concat shape) is unreachable here and the delete
// no-ops. Connectors with custom grant ids must use principal-scoped
// tombstones (SourceCacheRecord.deleted_principal_ids) instead — documented
// in the annotation proto.
func (e *Engine) DeleteGrantRecordBounded(ctx context.Context, externalID string) error {
	return e.withWrite(func() error {
		id, err := e.resolveGrantIdentityByCandidates(ctx, externalID)
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				return nil // absent (or custom-id) — tombstone no-op
			}
			return err
		}
		return e.deleteGrantByIdentityLocked(id)
	})
}

// DeleteGrantRecordsBounded validates every canonical public id before staging
// any tombstone, then commits the resolved deletes atomically. Resolution keeps
// DeleteGrantRecordBounded's candidate-only contract: missing or connector-custom
// ids are no-ops, while an ambiguous id rejects the entire request.
func (e *Engine) DeleteGrantRecordsBounded(ctx context.Context, externalIDs []string) error {
	return e.withWrite(func() error {
		identities := make([]grantIdentity, 0, len(externalIDs))
		seen := make(map[grantIdentity]struct{}, len(externalIDs))
		for _, externalID := range externalIDs {
			if err := ctx.Err(); err != nil {
				return err
			}
			id, err := e.resolveGrantIdentityByCandidates(ctx, externalID)
			if err != nil {
				if errors.Is(err, pebble.ErrNotFound) {
					continue
				}
				return err
			}
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			identities = append(identities, id)
		}
		if len(identities) == 0 {
			return nil
		}

		batch := e.db.NewRecordBatch()
		defer func() { _ = batch.Close() }()
		for _, id := range identities {
			key := encodeGrantIdentityKey(id)
			oldVal, closer, err := e.db.Get(key)
			if errors.Is(err, pebble.ErrNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			if err := batch.StageGrantDelete(key, oldVal); err != nil {
				_ = closer.Close()
				return err
			}
			_ = closer.Close()
		}
		return batch.Commit(writeOpts(e.opts.durability))
	})
}

type sourceCacheDeleteBatch struct {
	engine           *Engine
	batch            *rawdb.RecordBatch
	opts             *pebble.WriteOptions
	kind             string
	limit            int
	operations       int
	pendingDeleted   int64
	committedDeleted int64
}

func newSourceCacheDeleteBatch(e *Engine, kind string, opts *pebble.WriteOptions) *sourceCacheDeleteBatch {
	limit := replayBatchRows
	if e.test.sourceCacheDeleteBatchRows > 0 {
		limit = e.test.sourceCacheDeleteBatchRows
	}
	return &sourceCacheDeleteBatch{
		engine: e,
		batch:  e.db.NewRecordBatch(),
		opts:   opts,
		kind:   kind,
		limit:  limit,
	}
}

func (b *sourceCacheDeleteBatch) staged(rowDeleted bool) error {
	b.operations++
	if rowDeleted {
		b.pendingDeleted++
	}
	if b.operations < b.limit {
		return nil
	}
	return b.commit(false)
}

func (b *sourceCacheDeleteBatch) commit(final bool) error {
	if b.operations == 0 {
		return nil
	}
	if b.engine.test.sourceCacheDeleteCommitHook != nil {
		if err := b.engine.test.sourceCacheDeleteCommitHook(b.kind, b.operations, final); err != nil {
			return err
		}
	}
	if err := b.batch.Commit(b.opts); err != nil {
		return err
	}
	b.committedDeleted += b.pendingDeleted
	_ = b.batch.Close()
	b.batch = nil
	b.operations = 0
	b.pendingDeleted = 0
	if !final {
		b.batch = b.engine.db.NewRecordBatch()
	}
	return nil
}

func (b *sourceCacheDeleteBatch) close() {
	if b.batch == nil {
		return
	}
	_ = b.batch.Close()
	b.batch = nil
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
// count — callers batch a page's tombstones into one call. Deletes commit in
// bounded chunks; an error returns the count from chunks that already landed,
// and retry converges because deletion is idempotent.
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
		deletes := newSourceCacheDeleteBatch(e, "grant-principals", opts)
		defer deletes.close()
		defer func() { deleted = deletes.committedDeleted }()

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
			oldVal, closer, getErr := e.db.Get(priKey)
			if errors.Is(getErr, pebble.ErrNotFound) {
				if err := deletes.batch.StageSourceScopeOrphanIndexDelete(key); err != nil {
					return err
				}
				if err := deletes.staged(false); err != nil {
					return err
				}
				continue
			}
			if getErr != nil {
				return getErr
			}
			if err := deletes.batch.StageGrantDelete(priKey, oldVal); err != nil {
				closer.Close()
				return err
			}
			closer.Close()
			if err := deletes.staged(true); err != nil {
				return err
			}
		}
		if err := iter.Error(); err != nil {
			return err
		}
		return deletes.commit(true)
	})
	if err != nil {
		return deleted, err
	}
	return deleted, nil
}

// DeleteGrantsByExternalIDsInScope deletes every grant row in the CURRENT
// store stamped with scopeKey whose STORED grant id (external id, which
// may be a connector-custom shape) is in ids. One scan of the scope's
// index, loading each candidate's primary row to compare the stored id —
// bounded by the scope's row count, never the whole keyspace. This is the
// tombstone path for connectors with custom grant ids whose scopes span
// multiple resources (so principal-scoped deletes would over-delete). Deletes
// commit in bounded chunks and report committed progress on error.
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
		deletes := newSourceCacheDeleteBatch(e, "grant-external-ids", opts)
		defer deletes.close()
		defer func() { deleted = deletes.committedDeleted }()

		for iter.First(); iter.Valid(); iter.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			key := iter.Key()
			tail := key[len(prefix):]
			_, ok := decodeGrantIdentityTail(key, prefix)
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
					if err := deletes.batch.StageSourceScopeOrphanIndexDelete(key); err != nil {
						return err
					}
					if err := deletes.staged(false); err != nil {
						return err
					}
					continue
				}
				return err
			}
			rec := &v3.GrantRecord{}
			uerr := unmarshalRecord(val, rec)
			if uerr != nil {
				closer.Close()
				return fmt.Errorf("DeleteGrantsByExternalIDsInScope: unmarshal: %w", uerr)
			}
			if _, hit := ids[rec.GetExternalId()]; !hit {
				closer.Close()
				continue
			}
			if err := deletes.batch.StageGrantDelete(priKey, val); err != nil {
				closer.Close()
				return err
			}
			closer.Close()
			if err := deletes.staged(true); err != nil {
				return err
			}
		}
		if err := iter.Error(); err != nil {
			return err
		}
		return deletes.commit(true)
	})
	if err != nil {
		return deleted, err
	}
	return deleted, nil
}

// DeleteResourcesByIDsInScope deletes every resource row in the CURRENT
// store stamped with scopeKey whose resource id is in resourceIDs (any
// resource type) — principal-scoped tombstones for RowKindResources. Deletes
// commit in bounded chunks and report committed progress on error.
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
		deletes := newSourceCacheDeleteBatch(e, "resources", opts)
		defer deletes.close()
		defer func() { deleted = deletes.committedDeleted }()

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
			rowDeleted := false
			if val, closer, getErr := e.db.Get(priKey); getErr == nil {
				err := deletes.batch.StageResourceDelete(priKey, val, rt, rid)
				closer.Close()
				if err != nil {
					return err
				}
				rowDeleted = true
			} else if errors.Is(getErr, pebble.ErrNotFound) {
				if err := deletes.batch.StageSourceScopeOrphanIndexDelete(key); err != nil {
					return err
				}
			} else {
				return getErr
			}
			if err := deletes.staged(rowDeleted); err != nil {
				return err
			}
		}
		if err := iter.Error(); err != nil {
			return err
		}
		return deletes.commit(true)
	})
	if err != nil {
		return deleted, err
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

// validateReplaySourceScope proves the target scope's primary↔index
// biconditional before the destination is mutated. The index-to-primary pass is
// scope-bounded. Detecting a stamped primary whose index is absent requires a
// bounded-memory scan of the row-kind primary family because the manifest does
// not currently persist a row count or scope digest.
func validateReplaySourceScope(
	ctx context.Context,
	prev *Engine,
	rowKind string,
	recordType byte,
	scopeField protowire.Number,
	scopeKey string,
	indexPrefix []byte,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	primaryPrefix := []byte{versionV3, recordType}
	primaries, err := prev.db.NewIter(&pebble.IterOptions{
		LowerBound: primaryPrefix,
		UpperBound: upperBoundOf(primaryPrefix),
	})
	if err != nil {
		return fmt.Errorf("source cache replay: preflight %s primaries: %w", rowKind, err)
	}
	var scanned int
	for primaries.First(); primaries.Valid(); primaries.Next() {
		scanned++
		if scanned&0x3FF == 0 {
			if err := ctx.Err(); err != nil {
				primaries.Close()
				return err
			}
		}
		stamp, err := rawdb.ScanSourceScopeKeyRaw(primaries.Value(), scopeField)
		if err != nil {
			primaries.Close()
			return fmt.Errorf("source cache replay: preflight %s primary %x: %w", rowKind, primaries.Key(), err)
		}
		if stamp != scopeKey {
			continue
		}
		indexKey, ok := rawdb.AppendBySourceScopeKeyFromPrimary(nil, primaries.Key(), scopeKey)
		if !ok {
			primaries.Close()
			return fmt.Errorf("source cache replay: preflight %s primary %x cannot derive source index", rowKind, primaries.Key())
		}
		_, closer, err := prev.db.Get(indexKey)
		if err != nil {
			primaries.Close()
			return fmt.Errorf("source cache replay: preflight %s primary %x missing source index: %w", rowKind, primaries.Key(), err)
		}
		closer.Close()
	}
	if err := primaries.Error(); err != nil {
		primaries.Close()
		return fmt.Errorf("source cache replay: preflight %s primaries: %w", rowKind, err)
	}
	if err := primaries.Close(); err != nil {
		return err
	}

	indexes, err := prev.db.NewIter(&pebble.IterOptions{
		LowerBound: indexPrefix,
		UpperBound: upperBoundOf(indexPrefix),
	})
	if err != nil {
		return fmt.Errorf("source cache replay: preflight %s indexes: %w", rowKind, err)
	}
	defer indexes.Close()
	primaryHeader := [2]byte{versionV3, recordType}
	scanned = 0
	for indexes.First(); indexes.Valid(); indexes.Next() {
		scanned++
		if scanned&0x3FF == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		primaryKey, err := replayPrimaryFromIndexKey(indexes.Key(), indexPrefix, primaryHeader)
		if err != nil {
			return err
		}
		value, closer, err := prev.db.Get(primaryKey)
		if err != nil {
			return fmt.Errorf("source cache replay: preflight %s index %x has no primary: %w", rowKind, indexes.Key(), err)
		}
		stamp, scanErr := rawdb.ScanSourceScopeKeyRaw(value, scopeField)
		closer.Close()
		if scanErr != nil {
			return fmt.Errorf("source cache replay: preflight %s indexed primary %x: %w", rowKind, primaryKey, scanErr)
		}
		if stamp != scopeKey {
			return fmt.Errorf(
				"source cache replay: preflight %s index scope %q resolves to primary stamped %q",
				rowKind,
				scopeKey,
				stamp,
			)
		}
	}
	if err := indexes.Error(); err != nil {
		return fmt.Errorf("source cache replay: preflight %s indexes: %w", rowKind, err)
	}
	return nil
}

// clearReplayDestinationScopeLocked gives pure replay replacement semantics:
// the destination target partition is removed before the source partition is
// copied. The caller holds the engine write barrier. Deletes are committed in
// the same bounded row batches as replay, so interruption followed by retry
// converges without retaining destination-only rows.
func (e *Engine) clearReplayDestinationScopeLocked(
	ctx context.Context,
	rowKind string,
	recordType byte,
	indexPrefix []byte,
	opts *pebble.WriteOptions,
) (int, error) {
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: indexPrefix,
		UpperBound: upperBoundOf(indexPrefix),
	})
	if err != nil {
		return 0, err
	}
	defer iter.Close()
	primaryHeader := [2]byte{versionV3, recordType}
	batch := e.db.NewRecordBatch()
	defer func() { _ = batch.Close() }()
	rowsInBatch := 0
	deletedInBatch := 0
	deleted := 0
	commit := func(final bool) error {
		if rowsInBatch == 0 {
			return nil
		}
		if e.test.sourceCacheReplayClearCommitHook != nil {
			if err := e.test.sourceCacheReplayClearCommitHook(rowKind, rowsInBatch, final); err != nil {
				return err
			}
		}
		if err := batch.Commit(opts); err != nil {
			return err
		}
		deleted += deletedInBatch
		_ = batch.Close()
		batch = e.db.NewRecordBatch()
		rowsInBatch = 0
		deletedInBatch = 0
		return nil
	}

	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return deleted, err
		}
		indexKey := append([]byte(nil), iter.Key()...)
		primaryKey, err := replayPrimaryFromIndexKey(indexKey, indexPrefix, primaryHeader)
		if err != nil {
			return deleted, err
		}
		value, closer, err := e.db.Get(primaryKey)
		switch {
		case errors.Is(err, pebble.ErrNotFound):
			if err := batch.StageSourceScopeOrphanIndexDelete(indexKey); err != nil {
				return deleted, err
			}
		case err != nil:
			return deleted, err
		default:
			switch recordType {
			case typeGrant:
				err = batch.StageGrantDelete(primaryKey, value)
			case typeEntitlement:
				err = batch.StageEntitlementDelete(primaryKey, value)
			case typeResource:
				var resourceTypeID, resourceID string
				resourceTypeID, resourceID, err = decodeResourcePrimaryTail(primaryKey)
				if err == nil {
					err = batch.StageResourceDelete(primaryKey, value, resourceTypeID, resourceID)
				}
			default:
				err = fmt.Errorf("source cache replay: clear destination: invalid row kind %q", rowKind)
			}
			closer.Close()
			if err != nil {
				return deleted, err
			}
			deletedInBatch++
		}
		rowsInBatch++
		if rowsInBatch >= e.sourceCacheReplayBatchLimit() {
			if err := commit(false); err != nil {
				return deleted, err
			}
		}
	}
	if err := iter.Error(); err != nil {
		return deleted, err
	}
	if err := commit(true); err != nil {
		return deleted, err
	}
	return deleted, nil
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
	if err := validateReplaySourceScope(ctx, prev, "grants", typeGrant, 10, scopeKey, prefix); err != nil {
		return SourceCacheReplayResult{}, err
	}

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
		deleted, err := e.clearReplayDestinationScopeLocked(ctx, "grants", typeGrant, prefix, opts)
		if deleted > 0 {
			_ = e.takeFreshGrantsEmpty()
		}
		if err != nil {
			return err
		}
		batch := e.db.NewRecordBatch()
		defer func() { _ = batch.Close() }()
		rowsInBatch := 0

		// Consumed in a defer keyed on rows whose commit LANDED, not on
		// success: a replay that fails after an intermediate commit
		// (large scope, error or cancellation mid-copy) has already
		// populated the keyspace, and while the failing action unwinds,
		// concurrently draining workers can still write — a first
		// PutGrantRecords taking the empty-keyspace fast path over
		// partially replayed identities would skip index cleanup.
		committedRows := 0
		defer func() {
			if committedRows > 0 {
				_ = e.takeFreshGrantsEmpty()
			}
		}()

		sourceRow := 0
		for iter.First(); iter.Valid(); iter.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			if e.test.sourceCacheReplayReadHook != nil {
				if err := e.test.sourceCacheReplayReadHook("grants", sourceRow); err != nil {
					return err
				}
			}
			sourceRow++
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

			_, _, entID, _, _, needsExpansion, scanErr := scanGrantIndexFieldsRaw(val)
			if scanErr != nil {
				closer.Close()
				return scanErr
			}
			srcScope, scanErr := rawdb.ScanSourceScopeKeyRaw(val, 10)
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
			var oldVal []byte
			var oldCloser io.Closer
			currentVal, currentCloser, oldErr := e.db.Get(priKey)
			if oldErr == nil {
				oldVal, oldCloser = currentVal, currentCloser
			} else if !errors.Is(oldErr, pebble.ErrNotFound) {
				closer.Close()
				return fmt.Errorf("source cache replay: get current grant: %w", oldErr)
			}
			if err := batch.StageGrantPutInline(priKey, writeVal, oldVal, needsExpansion); err != nil {
				if oldCloser != nil {
					_ = oldCloser.Close()
				}
				closer.Close()
				return err
			}
			if oldCloser != nil {
				_ = oldCloser.Close()
			}
			closer.Close()
			if needsExpansion {
				res.NeedsExpansion = true
			}
			res.Rows++
			rowsInBatch++
			if rowsInBatch >= e.sourceCacheReplayBatchLimit() {
				if e.test.sourceCacheReplayCommitHook != nil {
					if err := e.test.sourceCacheReplayCommitHook("grants", rowsInBatch, false); err != nil {
						return err
					}
				}
				if err := batch.Commit(opts); err != nil {
					return err
				}
				committedRows += rowsInBatch
				_ = batch.Close()
				batch = e.db.NewRecordBatch()
				rowsInBatch = 0
			}
		}
		if err := e.sourceCacheReplayIteratorError("grants", iter); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if e.test.sourceCacheReplayCommitHook != nil {
			if err := e.test.sourceCacheReplayCommitHook("grants", rowsInBatch, true); err != nil {
				return err
			}
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
		return nil
	})
	if err != nil {
		return SourceCacheReplayResult{}, err
	}
	return res, nil
}

// ReplaySourceCacheEntitlements copies every entitlement stamped with
// scopeKey from prev into the receiver.
func (e *Engine) ReplaySourceCacheEntitlements(ctx context.Context, prev *Engine, scopeKey string) (SourceCacheReplayResult, error) {
	var res SourceCacheReplayResult
	prefix := encodeEntitlementBySourceScopePrefix(scopeKey)
	primaryHeader := [2]byte{versionV3, typeEntitlement}
	if err := validateReplaySourceScope(ctx, prev, "entitlements", typeEntitlement, 11, scopeKey, prefix); err != nil {
		return SourceCacheReplayResult{}, err
	}

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
		deleted, err := e.clearReplayDestinationScopeLocked(ctx, "entitlements", typeEntitlement, prefix, opts)
		if deleted > 0 {
			e.noteEntitlementKeyspaceWrite()
			_ = e.takeFreshEntitlementsEmpty()
		}
		if err != nil {
			return err
		}
		batch := e.db.NewRecordBatch()
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

		sourceRow := 0
		for iter.First(); iter.Valid(); iter.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			if e.test.sourceCacheReplayReadHook != nil {
				if err := e.test.sourceCacheReplayReadHook("entitlements", sourceRow); err != nil {
					return err
				}
			}
			sourceRow++
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
			prevScope, prevScanErr := rawdb.ScanSourceScopeKeyRaw(val, 11)
			if prevScanErr != nil {
				closer.Close()
				return prevScanErr
			}
			if prevScope != scopeKey {
				closer.Close()
				res.StaleSkipped++
				continue
			}
			var oldVal []byte
			var oldCloser io.Closer
			currentVal, currentCloser, oldErr := e.db.Get(priKey)
			if oldErr == nil {
				oldVal, oldCloser = currentVal, currentCloser
			} else if !errors.Is(oldErr, pebble.ErrNotFound) {
				closer.Close()
				return fmt.Errorf("source cache replay: get current entitlement: %w", oldErr)
			}
			if err := batch.StageEntitlementPut(priKey, val, oldVal); err != nil {
				if oldCloser != nil {
					_ = oldCloser.Close()
				}
				closer.Close()
				return err
			}
			if oldCloser != nil {
				_ = oldCloser.Close()
			}
			closer.Close()
			res.Rows++
			rowsInBatch++
			if rowsInBatch >= e.sourceCacheReplayBatchLimit() {
				if e.test.sourceCacheReplayCommitHook != nil {
					if err := e.test.sourceCacheReplayCommitHook("entitlements", rowsInBatch, false); err != nil {
						return err
					}
				}
				if err := batch.Commit(opts); err != nil {
					return err
				}
				committedRows += rowsInBatch
				_ = batch.Close()
				batch = e.db.NewRecordBatch()
				rowsInBatch = 0
			}
		}
		if err := e.sourceCacheReplayIteratorError("entitlements", iter); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if e.test.sourceCacheReplayCommitHook != nil {
			if err := e.test.sourceCacheReplayCommitHook("entitlements", rowsInBatch, true); err != nil {
				return err
			}
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
		return nil
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
	if err := validateReplaySourceScope(ctx, prev, "resources", typeResource, 12, scopeKey, prefix); err != nil {
		return SourceCacheReplayResult{}, err
	}

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
		deleted, err := e.clearReplayDestinationScopeLocked(ctx, "resources", typeResource, prefix, opts)
		if deleted > 0 {
			_ = e.takeFreshResourcesEmpty()
		}
		if err != nil {
			return err
		}
		batch := e.db.NewRecordBatch()
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

		sourceRow := 0
		for iter.First(); iter.Valid(); iter.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			if e.test.sourceCacheReplayReadHook != nil {
				if err := e.test.sourceCacheReplayReadHook("resources", sourceRow); err != nil {
					return err
				}
			}
			sourceRow++
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
			srcScope, scanErr := rawdb.ScanSourceScopeKeyRaw(val, 12)
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
			var oldVal []byte
			var oldCloser io.Closer
			currentVal, currentCloser, oldErr := e.db.Get(priKey)
			if oldErr == nil {
				oldVal, oldCloser = currentVal, currentCloser
			} else if !errors.Is(oldErr, pebble.ErrNotFound) {
				closer.Close()
				return fmt.Errorf("source cache replay: get current resource: %w", oldErr)
			}

			if err := batch.StageResourcePut(priKey, val, oldVal, rt, rid); err != nil {
				if oldCloser != nil {
					_ = oldCloser.Close()
				}
				closer.Close()
				return err
			}
			if oldCloser != nil {
				_ = oldCloser.Close()
			}
			closer.Close()
			res.Rows++
			rowsInBatch++
			if rowsInBatch >= e.sourceCacheReplayBatchLimit() {
				if e.test.sourceCacheReplayCommitHook != nil {
					if err := e.test.sourceCacheReplayCommitHook("resources", rowsInBatch, false); err != nil {
						return err
					}
				}
				if err := batch.Commit(opts); err != nil {
					return err
				}
				committedRows += rowsInBatch
				_ = batch.Close()
				batch = e.db.NewRecordBatch()
				rowsInBatch = 0
			}
		}
		if err := e.sourceCacheReplayIteratorError("resources", iter); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if e.test.sourceCacheReplayCommitHook != nil {
			if err := e.test.sourceCacheReplayCommitHook("resources", rowsInBatch, true); err != nil {
				return err
			}
		}
		if err := batch.Commit(opts); err != nil {
			return err
		}
		// See grant replay above: direct replay writes mean the first
		// overlay PutResourceRecords must not use the empty-keyspace
		// fast-path, or old by_parent/by_source_scope entries survive
		// (consumed by the defer above).
		committedRows += rowsInBatch
		return nil
	})
	if err != nil {
		return SourceCacheReplayResult{}, err
	}
	return res, nil
}
