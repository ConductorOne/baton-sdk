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
	"github.com/conductorone/baton-sdk/pkg/sourcecache"
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

// manifestRewritePageRows bounds one page of a seal/clear manifest
// rewrite. Manifest size is (scopes × row kinds) with connector-chosen
// scope granularity, so these passes page in the same dimension and at
// the same bound as every other scope-scale pass.
const manifestRewritePageRows = replayBatchRows

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
//
// The result is meaningful WITH a non-nil error too: bounded
// intermediate batches may have landed before the failure (that is the
// committed-prefix retry seam), so on error Rows reports only rows
// whose commit landed, matching the scoped-delete siblings' committed
// progress. NeedsExpansion accumulates at stage time and on error may
// overreport a row that never committed — the safe direction, since
// arming expansion is idempotent and add-only, while underreporting
// could leave a committed expandable grant unexpanded.
type SourceCacheReplayResult struct {
	Rows int64
	// NeedsExpansion is true when at least one copied grant row carried
	// needs_expansion. Future syncer replay orchestration must consume this
	// signal to arm grant expansion: replayed pages never pass
	// GrantExpandable-annotated rows through the connector-response path.
	NeedsExpansion bool
}

// sourceCacheRowKindSpecs maps each manifest row kind to its primary
// record family and the SourceScopeKey field number in that family's
// value proto. The field numbers are pinned by
// TestVerificationDescriptorClosedReplayAndDirectMaterialization.
var sourceCacheRowKindSpecs = map[string]struct {
	recordType byte
	scopeField protowire.Number
}{
	string(sourcecache.RowKindResources):    {typeResource, 12},
	string(sourcecache.RowKindEntitlements): {typeEntitlement, 11},
	string(sourcecache.RowKindGrants):       {typeGrant, 10},
}

// sealSourceCacheRowCounts stamps every manifest entry with the number
// of primary rows carrying its scope, recomputed from the primary
// keyspace at seal time (CO-004). Replay preflight compares the scope's
// index cardinality against this count instead of scanning every
// primary per scope, turning the preflight from O(scopes × rows) into
// O(scope size).
//
// Counting from the PRIMARIES (not the by_source_scope index) is the
// point: the count must be an independent witness of the same
// biconditional the old preflight proved, so index corruption cannot
// vouch for itself. Zero is written explicitly (field presence) — a
// proven-empty scope is a valid replay source, while an entry missing
// the count entirely means the artifact predates or skipped this seal
// step and replay must hard-fail.
//
// Runs SEALED, before the ended_at stamp: a crash between counting and
// the stamp leaves the sync unfinished, so no artifact can carry the
// finished verdict without also carrying the counts (they ride the
// same WAL the stamp fsyncs). Reseal after a SetCurrentSync rebind
// recounts and overwrites — rebound mutations never publish stale
// counts, and an unpublished working directory is never a replay
// source. Cost is zero for syncs with an empty manifest.
func (e *Engine) sealSourceCacheRowCounts(ctx context.Context) error {
	return e.withWriteAllowSealed(func() error {
		lo, hi := rawdb.SourceCacheEntryBounds()
		// Pass 1 — validate every entry's kind and learn which primary
		// families need counting. Entries are NOT retained: manifest
		// size is (scopes × row kinds) and scope granularity is
		// connector-chosen, so the only cross-pass state this function
		// holds is the counts map — per-scope totals, which are the
		// output and thus an irreducible O(scopes-with-stamped-rows).
		iter, err := e.db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
		if err != nil {
			return fmt.Errorf("seal source cache counts: manifest iter: %w", err)
		}
		kinds := make(map[string]struct{})
		hasEntries := false
		for iter.First(); iter.Valid(); iter.Next() {
			if err := ctx.Err(); err != nil {
				_ = iter.Close()
				return err
			}
			rec := &v3.SourceCacheEntryRecord{}
			if err := unmarshalRecord(iter.Value(), rec); err != nil {
				_ = iter.Close()
				return fmt.Errorf("seal source cache counts: manifest %x: %w", iter.Key(), err)
			}
			if _, ok := sourceCacheRowKindSpecs[rec.GetRowKind()]; !ok {
				_ = iter.Close()
				return fmt.Errorf("seal source cache counts: manifest %x has unknown row kind %q", iter.Key(), rec.GetRowKind())
			}
			hasEntries = true
			kinds[rec.GetRowKind()] = struct{}{}
		}
		if err := iter.Error(); err != nil {
			_ = iter.Close()
			return fmt.Errorf("seal source cache counts: manifest iter: %w", err)
		}
		if err := iter.Close(); err != nil {
			return err
		}
		if !hasEntries {
			return nil
		}

		// One pass per row kind that has manifest entries, regardless of
		// how many scopes partition it.
		counts := make(map[string]map[string]uint64, len(kinds))
		for kind := range kinds {
			spec := sourceCacheRowKindSpecs[kind]
			kindCounts := make(map[string]uint64)
			primaryPrefix := []byte{versionV3, spec.recordType}
			primaries, err := e.db.NewIter(&pebble.IterOptions{
				LowerBound: primaryPrefix,
				UpperBound: upperBoundOf(primaryPrefix),
			})
			if err != nil {
				return fmt.Errorf("seal source cache counts: %s primaries: %w", kind, err)
			}
			var scanned int
			for primaries.First(); primaries.Valid(); primaries.Next() {
				scanned++
				if scanned&0x3FF == 0 {
					if err := ctx.Err(); err != nil {
						_ = primaries.Close()
						return err
					}
				}
				stamp, err := rawdb.ScanSourceScopeKeyRaw(primaries.Value(), spec.scopeField)
				if err != nil {
					_ = primaries.Close()
					return fmt.Errorf("seal source cache counts: %s primary %x: %w", kind, primaries.Key(), err)
				}
				if stamp == "" {
					continue
				}
				kindCounts[stamp]++
			}
			if err := primaries.Error(); err != nil {
				_ = primaries.Close()
				return fmt.Errorf("seal source cache counts: %s primaries: %w", kind, err)
			}
			if err := primaries.Close(); err != nil {
				return err
			}
			counts[kind] = kindCounts
		}

		// Pass 3 — stream the counted entries back in bounded pages. The
		// iterator reads the pre-write snapshot, so rewriting keys it has
		// already visited cannot disturb the traversal, and only one page
		// is ever resident. NoSync: these writes ride the WAL ahead of
		// the ended_at stamp's fsync, so any crash image holding the
		// finished verdict also holds every count (same argument as
		// endSyncFinalize's page hardening).
		rewrite, err := e.db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
		if err != nil {
			return fmt.Errorf("seal source cache counts: manifest rewrite iter: %w", err)
		}
		var page []rawdb.SourceCacheKV
		flush := func() error {
			if err := e.db.SourceCacheSetMulti(page, pebble.NoSync); err != nil {
				return fmt.Errorf("seal source cache counts: write: %w", err)
			}
			page = page[:0]
			return nil
		}
		for rewrite.First(); rewrite.Valid(); rewrite.Next() {
			if err := ctx.Err(); err != nil {
				_ = rewrite.Close()
				return err
			}
			rec := &v3.SourceCacheEntryRecord{}
			if err := unmarshalRecord(rewrite.Value(), rec); err != nil {
				_ = rewrite.Close()
				return fmt.Errorf("seal source cache counts: manifest %x: %w", rewrite.Key(), err)
			}
			rec.SetRowCount(counts[rec.GetRowKind()][rec.GetScopeKey()])
			val, err := marshalRecord(rec)
			if err != nil {
				_ = rewrite.Close()
				return fmt.Errorf("seal source cache counts: marshal %x: %w", rewrite.Key(), err)
			}
			if len(page) >= manifestRewritePageRows {
				if err := flush(); err != nil {
					_ = rewrite.Close()
					return err
				}
			}
			page = append(page, rawdb.SourceCacheKV{Key: append([]byte(nil), rewrite.Key()...), Val: val})
		}
		if err := rewrite.Error(); err != nil {
			_ = rewrite.Close()
			return fmt.Errorf("seal source cache counts: manifest rewrite iter: %w", err)
		}
		if err := rewrite.Close(); err != nil {
			return err
		}
		return flush()
	})
}

// clearSourceCacheRowCounts strips sealed row counts from every manifest
// entry that carries one. Called on rebind (bindCurrentSync): a rebound
// sync admits new mutations, so counts sealed before those mutations no
// longer witness the primary keyspace. Published artifacts are unaffected
// (publication always passes through EndSync, which recounts); this keeps
// any UNPUBLISHED rebound store fail-closed — a replay-eligible entry
// without a count is a hard preflight error. No-op for unfinished syncs
// (counts exist only after a sealing EndSync) and empty manifests.
func (e *Engine) clearSourceCacheRowCounts() error {
	return e.withWrite(func() error {
		lo, hi := rawdb.SourceCacheEntryBounds()
		iter, err := e.db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
		if err != nil {
			return fmt.Errorf("clear source cache counts: manifest iter: %w", err)
		}
		// Paged rewrite over a snapshot iterator (writes to visited keys
		// cannot disturb the traversal): intermediate pages commit NoSync
		// and the FINAL page commits synced, which persists the whole WAL
		// prefix — every earlier page is durable before bind returns, so
		// no crash image holds a post-rebind mutation alongside any
		// still-counted entry. A crash MID-strip is safe in both
		// directions: bind never returned, so no mutation was admitted —
		// stripped entries are fail-closed (a missing count is a hard
		// preflight error) and still-counted entries remain accurate
		// witnesses of the untouched primary keyspace; the reopen's bind
		// strips the rest. Paging bounds memory and batch size by the
		// page, not the manifest.
		var page []rawdb.SourceCacheKV
		flush := func(o *pebble.WriteOptions) error {
			if err := e.db.SourceCacheSetMulti(page, o); err != nil {
				return fmt.Errorf("clear source cache counts: write: %w", err)
			}
			page = page[:0]
			return nil
		}
		cleared := false
		for iter.First(); iter.Valid(); iter.Next() {
			rec := &v3.SourceCacheEntryRecord{}
			if err := unmarshalRecord(iter.Value(), rec); err != nil {
				_ = iter.Close()
				return fmt.Errorf("clear source cache counts: manifest %x: %w", iter.Key(), err)
			}
			if !rec.HasRowCount() {
				continue
			}
			rec.ClearRowCount()
			val, err := marshalRecord(rec)
			if err != nil {
				_ = iter.Close()
				return fmt.Errorf("clear source cache counts: marshal %x: %w", iter.Key(), err)
			}
			if len(page) >= manifestRewritePageRows {
				if err := flush(pebble.NoSync); err != nil {
					_ = iter.Close()
					return err
				}
			}
			page = append(page, rawdb.SourceCacheKV{Key: append([]byte(nil), iter.Key()...), Val: val})
			cleared = true
		}
		if err := iter.Error(); err != nil {
			_ = iter.Close()
			return fmt.Errorf("clear source cache counts: manifest iter: %w", err)
		}
		if err := iter.Close(); err != nil {
			return err
		}
		if !cleared {
			return nil
		}
		// The final page is non-empty by construction (pages flush before
		// append, so the last append leaves at least one entry) and its
		// synced commit hardens the whole strip.
		return flush(pebble.Sync)
	})
}

// PutSourceCacheEntry writes the manifest entry for (rowKind, scopeKey).
// Zero-row scopes still get entries — the validator must survive to the
// next sync even when the scope produced no rows.
func (e *Engine) PutSourceCacheEntry(ctx context.Context, rowKind, scopeKey, cacheValidator string) error {
	// Reject unknown kinds at the write: the seal pass hard-errors on any
	// manifest entry whose kind it cannot count, manifest entries are
	// individually undeletable, and EndSync retries fail identically — an
	// unvalidated kind would turn a caller typo into an unsealable
	// artifact.
	if _, ok := sourceCacheRowKindSpecs[rowKind]; !ok {
		return fmt.Errorf("source cache manifest: unknown row kind %q", rowKind)
	}
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

// SourceCachePoisoned reports whether (rowKind, scopeKey) carries a
// poison marker (CO-015): a mutation in this store removed a row from
// the scope's stamped set on behalf of something other than the scope
// itself — a cross-scope restamp or an out-of-scope delete. A poisoned
// scope must be treated as a lookup miss by orchestration and is
// refused as a replay source by preflight; it re-fetches cold and the
// next sync's fresh artifact starts unpoisoned.
func (e *Engine) SourceCachePoisoned(ctx context.Context, rowKind, scopeKey string) (bool, error) {
	_, closer, err := e.db.Get(rawdb.SourceCachePoisonKey(rowKind, scopeKey))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	_ = closer.Close()
	return true, nil
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
//
// Acts unscoped: deleting a stamped row through this path poisons the
// row's scope (CO-015). Scope-acting tombstones use
// DeleteGrantRecordsBounded with an acting scope instead.
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
// any tombstone, then commits the resolved deletes in bounded chunks (same
// page mechanics as the scoped tombstone paths; deletion is idempotent, so an
// error mid-way retries convergently). Resolution keeps
// DeleteGrantRecordBounded's candidate-only contract: missing or
// connector-custom ids are no-ops, while an ambiguous id rejects the entire
// request. actingScope is the scope on whose behalf the tombstones act:
// deleting that scope's own rows stages no poison, while deleting a row
// stamped with any OTHER scope poisons it (CO-015).
func (e *Engine) DeleteGrantRecordsBounded(ctx context.Context, externalIDs []string, actingScope string) error {
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

		deletes := newSourceCacheDeleteBatch(e, "grants-canonical", actingScope, writeOpts(e.opts.durability))
		defer deletes.close()
		for _, id := range identities {
			key := encodeGrantIdentityKey(id)
			oldVal, closer, err := e.db.Get(key)
			if errors.Is(err, pebble.ErrNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			if err := deletes.batch.StageGrantDelete(key, oldVal); err != nil {
				_ = closer.Close()
				return err
			}
			_ = closer.Close()
			if err := deletes.staged(true); err != nil {
				return err
			}
		}
		return deletes.commit(true)
	})
}

// DeleteResourceRecordsBounded deletes resources by (resource_type_id,
// resource_id) in bounded chunks, acting for actingScope — the resources
// analog of DeleteGrantRecordsBounded, replacing a per-id single-commit
// loop for the canonical tombstone path. Absent ids are no-ops.
func (e *Engine) DeleteResourceRecordsBounded(ctx context.Context, refs []ResourceRef, actingScope string) error {
	if len(refs) == 0 {
		return nil
	}
	return e.withWrite(func() error {
		deletes := newSourceCacheDeleteBatch(e, "resources-canonical", actingScope, writeOpts(e.opts.durability))
		defer deletes.close()
		for _, ref := range refs {
			if err := ctx.Err(); err != nil {
				return err
			}
			key := encodeResourceKey(ref.ResourceTypeID, ref.ResourceID)
			oldVal, closer, err := e.db.Get(key)
			if errors.Is(err, pebble.ErrNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			if err := deletes.batch.StageResourceDelete(key, oldVal, ref.ResourceTypeID, ref.ResourceID); err != nil {
				_ = closer.Close()
				return err
			}
			_ = closer.Close()
			if err := deletes.staged(true); err != nil {
				return err
			}
		}
		return deletes.commit(true)
	})
}

// ResourceRef addresses one resource row for the bounded tombstone path.
type ResourceRef struct {
	ResourceTypeID string
	ResourceID     string
}

type sourceCacheDeleteBatch struct {
	engine *Engine
	batch  *rawdb.RecordBatch
	opts   *pebble.WriteOptions
	kind   string
	// actingScope is the scope on whose behalf the deletes act; applied
	// to every minted batch (chunked commits re-mint) so a scope's own
	// tombstones never stage poison against it (CO-015).
	actingScope string
	// onCommit runs after every commit that landed at least one delete
	// (intermediate chunks and the final one). Engine-state invalidation
	// keyed on keyspace mutation (the bare-id entitlement lookup) must
	// fire per landed chunk, not on function exit: readers of that state
	// synchronize on their own mutex, not the write barrier, so a bump
	// deferred to the end of a long id loop leaves a window where a
	// concurrent lookup serves rows a chunk already deleted.
	onCommit         func()
	limit            int
	operations       int
	pendingDeleted   int64
	committedDeleted int64
}

func newSourceCacheDeleteBatch(e *Engine, kind, actingScope string, opts *pebble.WriteOptions) *sourceCacheDeleteBatch {
	limit := replayBatchRows
	if e.test.sourceCacheDeleteBatchRows > 0 {
		limit = e.test.sourceCacheDeleteBatchRows
	}
	b := &sourceCacheDeleteBatch{
		engine:      e,
		batch:       e.db.NewRecordBatch(),
		opts:        opts,
		kind:        kind,
		actingScope: actingScope,
		limit:       limit,
	}
	b.batch.SetActingSourceScope(actingScope)
	return b
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
	landed := b.pendingDeleted
	b.committedDeleted += landed
	_ = b.batch.Close()
	b.batch = nil
	b.operations = 0
	b.pendingDeleted = 0
	if !final {
		b.batch = b.engine.db.NewRecordBatch()
		b.batch.SetActingSourceScope(b.actingScope)
	}
	if landed > 0 && b.onCommit != nil {
		b.onCommit()
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
		deletes := newSourceCacheDeleteBatch(e, "grant-principals", scopeKey, opts)
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
		deletes := newSourceCacheDeleteBatch(e, "grant-external-ids", scopeKey, opts)
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
		deletes := newSourceCacheDeleteBatch(e, "resources", scopeKey, opts)
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
// biconditional before the destination is mutated, in O(scope size):
//
//  1. Walk the scope's by_source_scope index. Every entry must resolve
//     to a primary whose value stamp names this scope (no orphans, no
//     stale entries). Index keys are unique and each derives a distinct
//     primary, so the walk's cardinality is |stamped primaries that are
//     indexed|.
//  2. Compare that cardinality against the manifest's sealed row_count —
//     the number of primaries stamped with this scope, counted from the
//     primary keyspace at EndSync (sealSourceCacheRowCounts, CO-004).
//     Equality makes the index→primary injection surjective: every
//     stamped primary is indexed. A stamped primary missing its index
//     entry shows up as a count shortfall, which is exactly what the
//     deleted O(all primaries) scan used to detect row-by-row.
//
// The equivalence holds because the source is immutable between seal
// and replay: a published c1z is opened read-only, and the sealed
// engine admits no record mutations between counting and the ended_at
// stamp. A manifest entry WITHOUT a sealed count is a hard error —
// replay-eligible artifacts are sealed by an SDK that counts, so
// absence means the seal step was bypassed, not an older format.
//
// Note what the count deliberately does NOT vouch for: mid-sync
// partition damage that happened BEFORE the seal (a cross-scope
// restamp, or an unscoped delete such as external-principal
// reconciliation removing a stamped row) leaves index, stamps, and
// count self-consistent with the damaged set. That is the per-scope
// poison marker's job, staged durably at mutation time — not the
// count's.
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
	entry, err := prev.GetSourceCacheEntry(ctx, rowKind, scopeKey)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return fmt.Errorf("source cache replay: preflight %s scope %q has no manifest entry", rowKind, scopeKey)
		}
		return fmt.Errorf("source cache replay: preflight %s scope %q manifest read: %w", rowKind, scopeKey, err)
	}
	if entry.GetInvalidated() {
		return fmt.Errorf("source cache replay: preflight %s scope %q manifest entry is invalidated", rowKind, scopeKey)
	}
	poisonedScope, err := prev.SourceCachePoisoned(ctx, rowKind, scopeKey)
	if err != nil {
		return fmt.Errorf("source cache replay: preflight %s scope %q poison read: %w", rowKind, scopeKey, err)
	}
	if poisonedScope {
		return fmt.Errorf(
			"source cache replay: preflight %s scope %q is poisoned: the source sync observed a row-partition violation "+
				"(cross-scope restamp or out-of-scope delete) against this scope; it must re-fetch cold",
			rowKind, scopeKey,
		)
	}
	if !entry.HasRowCount() {
		return fmt.Errorf(
			"source cache replay: preflight %s scope %q manifest entry has no sealed row count (source was not sealed by a counting EndSync)",
			rowKind, scopeKey,
		)
	}
	want := entry.GetRowCount()

	indexes, err := prev.db.NewIter(&pebble.IterOptions{
		LowerBound: indexPrefix,
		UpperBound: upperBoundOf(indexPrefix),
	})
	if err != nil {
		return fmt.Errorf("source cache replay: preflight %s indexes: %w", rowKind, err)
	}
	defer indexes.Close()
	primaryHeader := [2]byte{versionV3, recordType}
	var got uint64
	var scanned int
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
		got++
	}
	if err := indexes.Error(); err != nil {
		return fmt.Errorf("source cache replay: preflight %s indexes: %w", rowKind, err)
	}
	if got != want {
		return fmt.Errorf(
			"source cache replay: preflight %s scope %q index cardinality %d does not match sealed row count %d (stamped primary missing its index entry, or post-seal mutation)",
			rowKind, scopeKey, got, want,
		)
	}
	return nil
}

// clearReplayDestinationScopeLocked gives pure replay replacement semantics:
// the destination target partition is removed before the source partition is
// copied. The caller holds the engine write barrier. Deletes are committed in
// the same bounded row batches as replay, so interruption followed by retry
// converges without retaining destination-only rows. The deletes act FOR the
// scope being replayed (SetActingSourceScope): replacing your own partition
// is the replay flow itself, never a poison event.
func (e *Engine) clearReplayDestinationScopeLocked(
	ctx context.Context,
	rowKind string,
	recordType byte,
	scopeKey string,
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
	batch.SetActingSourceScope(scopeKey)
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
		// Same per-chunk contract as sourceCacheDeleteBatch.onCommit:
		// bare-id lookups synchronize on entIDLookupMu only, so the
		// entitlement keyspace must invalidate the cached map as each
		// chunk lands, not when the caller finishes.
		if deletedInBatch > 0 && recordType == typeEntitlement {
			e.noteEntitlementKeyspaceWrite()
		}
		_ = batch.Close()
		batch = e.db.NewRecordBatch()
		batch.SetActingSourceScope(scopeKey)
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

	committedRows := 0
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
		deleted, err := e.clearReplayDestinationScopeLocked(ctx, "grants", typeGrant, scopeKey, prefix, opts)
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
		// (committedRows itself is function-scoped so the error return
		// can report landed progress.)
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
				// An orphan index entry (no primary) was proven absent by
				// validateReplaySourceScope over this same range on the
				// same immutable source, so ErrNotFound here means the
				// preflight and the copy loop disagree — a bug in one of
				// them, never a legitimate source state. Fail loudly
				// rather than skip: silently dropping a row the index
				// promised is the silent-data-loss shape this subsystem
				// exists to refuse.
				return fmt.Errorf("source cache replay: get prev grant (index entry %x passed preflight): %w", iter.Key(), getErr)
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
			// Stamp re-check on the same preflight-proven invariant: an
			// index entry resolving to a row stamped for a different scope
			// would inject rows upstream never returned for this scope, so
			// disagreement with the preflight is a hard error, not a skip.
			if srcScope != scopeKey {
				closer.Close()
				return fmt.Errorf(
					"source cache replay: grants index scope %q resolves to primary stamped %q after passing preflight",
					scopeKey, srcScope,
				)
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
		// Bounded intermediate batches may already have landed (that is
		// the retry seam), so report committed progress with the error,
		// matching the scoped-delete siblings. Rows counts only rows
		// whose commit landed; the failing batch's staged rows are
		// dropped. NeedsExpansion accumulates at STAGE time and may
		// overreport a row that never committed — the safe direction
		// (arming expansion is idempotent and add-only), where
		// underreporting could leave a committed expandable grant
		// unexpanded.
		res.Rows = int64(committedRows)
		return res, err
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

	committedRows := 0
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
		deleted, err := e.clearReplayDestinationScopeLocked(ctx, "entitlements", typeEntitlement, scopeKey, prefix, opts)
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
		// (committedRows itself is function-scoped so the error return
		// can report landed progress.)
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
				// ErrNotFound means the copy loop disagrees with the
				// preflight over the same immutable range — a bug, never a
				// legitimate source state (see the grants replay).
				return fmt.Errorf("source cache replay: get prev entitlement (index entry %x passed preflight): %w", iter.Key(), getErr)
			}
			// Stamp re-check on the preflight-proven invariant (see the
			// grants replay for rationale): disagreement is a hard error.
			prevScope, prevScanErr := rawdb.ScanSourceScopeKeyRaw(val, 11)
			if prevScanErr != nil {
				closer.Close()
				return prevScanErr
			}
			if prevScope != scopeKey {
				closer.Close()
				return fmt.Errorf(
					"source cache replay: entitlements index scope %q resolves to primary stamped %q after passing preflight",
					scopeKey, prevScope,
				)
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
				// Same per-chunk contract as the clear half above and
				// sourceCacheDeleteBatch.onCommit: bare-id readers take
				// only entIDLookupMu, so each landed chunk must
				// invalidate the cached map — deferring to function exit
				// would let a reader build and keep serving a map
				// missing every entitlement this chunk just committed.
				e.noteEntitlementKeyspaceWrite()
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
		// The defer above covers the FINAL chunk's lookup invalidation
		// (see lookup.go — later tombstones would silently miss replayed
		// rows otherwise) and disarms the empty-keyspace fast path for
		// the first overlay PutEntitlementRecords; intermediate chunks
		// bumped as they landed above. Bump timing is conservative in
		// both directions: entitlementIdentitiesForExternalID loads the
		// generation BEFORE taking entIDLookupMu, so a map build racing
		// any bump records the older generation and the next lookup
		// rebuilds — a bump can never launder partially-replayed state
		// as fresh.
		committedRows += rowsInBatch
		return nil
	})
	if err != nil {
		// Committed progress rides the error — see the grants replay.
		res.Rows = int64(committedRows)
		return res, err
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

	committedRows := 0
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
		deleted, err := e.clearReplayDestinationScopeLocked(ctx, "resources", typeResource, scopeKey, prefix, opts)
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
		// grants replay for rationale. (committedRows itself is
		// function-scoped so the error return can report landed progress.)
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
				// ErrNotFound means the copy loop disagrees with the
				// preflight over the same immutable range — a bug, never a
				// legitimate source state (see the grants replay).
				return fmt.Errorf("source cache replay: get prev resource (index entry %x passed preflight): %w", iter.Key(), getErr)
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
			// Stamp re-check on the preflight-proven invariant (see the
			// grants replay for rationale): disagreement is a hard error.
			if srcScope != scopeKey {
				closer.Close()
				return fmt.Errorf(
					"source cache replay: resources index scope %q resolves to primary stamped %q after passing preflight",
					scopeKey, srcScope,
				)
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
		// Committed progress rides the error — see the grants replay.
		res.Rows = int64(committedRows)
		return res, err
	}
	return res, nil
}
