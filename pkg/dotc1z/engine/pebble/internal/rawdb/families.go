package rawdb

// The per-family write surface. Each write family gets its own batch
// type or purpose-named operations; the family is load-bearing — it
// ties every write to its registry entry, its obligations, and its
// crash contract, and it is the unit the meta-tests reason about.
//
// Two layers of enforcement: NOTHING outside internal/rawdb can mint
// a raw batch or touch the raw DB at all (unexported primitives +
// meta-tests), so every write arrives through a family-named
// constructor; and the RECORD family goes further — its batch exposes
// no generic staging at all, only typed Stage* operations
// (records.go) that derive their index/digest/marker obligations
// internally and assert key prefixes, so a record write physically
// cannot forget its side effects. The other families (session, meta,
// digest, fold) keep the uniform staged surface below.

import (
	"context"
	"sync/atomic"

	"github.com/cockroachdb/pebble/v2"
)

// Stager is the staged-write surface shared by every family batch.
// Helpers that legitimately serve two families take it instead of a
// concrete batch type — today that is digest invalidation, which is
// staged by RECORD mutations (post-seal partial syncs, via
// RecordBatch) and by the digest REPAIR pass (via DigestBatch).
type Stager interface {
	Set(key, val []byte) error
	Delete(key []byte) error
	DeleteRange(start, end []byte) error
}

// batch is the shared staged-write core the family types embed.
// open points at the owning DB's per-family outstanding-batch counter
// (batchAccounting): minting increments it, the first Close decrements
// it, and DB.Close reports any nonzero balance as a leak. Close also
// nils the pebble handle so a second Close is a no-op instead of a
// double release of a pooled object (§5.2 ownership transfer).
type batch struct {
	b    *pebble.Batch
	open *atomic.Int64
}

// Set stages key → val.
func (b *batch) Set(key, val []byte) error { return b.b.Set(key, val, nil) }

// Delete stages a point delete.
func (b *batch) Delete(key []byte) error { return b.b.Delete(key, nil) }

// DeleteRange stages a range delete [start, end).
func (b *batch) DeleteRange(start, end []byte) error { return b.b.DeleteRange(start, end, nil) }

// Empty reports whether nothing was staged.
func (b *batch) Empty() bool { return b.b.Empty() }

// Count returns the number of staged operations.
func (b *batch) Count() uint32 { return b.b.Count() }

// Len returns the encoded batch size in bytes (flush-threshold checks).
func (b *batch) Len() int { return len(b.b.Repr()) }

// Commit applies the staged writes with the given write options.
func (b *batch) Commit(o *pebble.WriteOptions) error { return b.b.Commit(o) }

// Close releases the batch. Safe after Commit, and idempotent: the
// first call transfers ownership back to pebble's pool and nils the
// handle; later calls return nil without touching the pooled object.
func (b *batch) Close() error {
	if b.b == nil {
		return nil
	}
	err := b.b.Close()
	b.b = nil
	if b.open != nil {
		b.open.Add(-1)
	}
	return err
}

// === record family: grants / entitlements / resources / resource types ===
//
// The primary record keyspaces plus their inline-maintained index
// families and the digest-invalidation markers a record mutation owes.
// Clients: the Put*Records paths, the expanded/synthesized grant
// writers, and delete paths.
//
// RecordBatch exposes NO generic staging. The only way to stage a
// record mutation is a typed Stage* operation (records.go) that
// derives and stages everything the row owes in the same call — the
// forgotten-obligation bug class (primary landed, index entry or
// digest invalidation forgotten) is unexpressible. The compactor's
// keep-newer fold, which legitimately stages raw keys it did not
// encode, uses FoldBatch instead (records.go) via the Engine merge
// surface (the engine package's merge_surface.go).
type RecordBatch struct {
	// core is deliberately NOT embedded: embedding would promote the
	// generic Set/Delete/DeleteRange onto the exported surface.
	core    batch
	db      *DB
	scratch []byte
	// actingSourceScope is the scope on whose behalf this batch's
	// deletes act (SetActingSourceScope); "" means unscoped. Consulted
	// by stageSourceScopeCleanup's poison decision (CO-015).
	actingSourceScope string
	// poisonStaged / poisonEvents: per-batch dedup set and ordered log
	// of staged poison markers; events are delivered to the DB's poison
	// observer after a successful Commit. Nil until the first poison —
	// the ordinary mutation path pays nothing.
	poisonStaged map[PoisonEvent]struct{}
	poisonEvents []PoisonEvent
}

// NewRecordBatch mints a batch for record-keyspace mutations.
func (d *DB) NewRecordBatch() *RecordBatch {
	d.acct.record.Add(1)
	return &RecordBatch{core: batch{b: d.newBatch(), open: &d.acct.record}, db: d}
}

// Commit applies the staged writes with the given write options. Poison
// events staged in this batch are delivered to the observer only after
// the commit lands — a failed commit stages nothing durable and logs
// nothing.
func (rb *RecordBatch) Commit(o *pebble.WriteOptions) error {
	if rb.db.testRecordCommitHook != nil {
		if err := rb.db.testRecordCommitHook(); err != nil {
			return err
		}
	}
	if err := rb.core.Commit(o); err != nil {
		return err
	}
	if rb.db.poisonObserver != nil {
		for _, ev := range rb.poisonEvents {
			rb.db.poisonObserver(ev)
		}
	}
	rb.poisonEvents = rb.poisonEvents[:0]
	return nil
}

// Close releases the batch. Safe after Commit.
func (rb *RecordBatch) Close() error { return rb.core.Close() }

// NOTE (2b): Absorb (the pri/idx two-batch fold) is gone — the typed
// Stage* ops put a row and its obligations into ONE batch, so the
// single-fsync atomicity the fold existed for is now structural.

// === session family ===
//
// Connector scratch state, written even after EndSync seals the engine
// (Cleanup clears sessions post-sync).

// SessionSet writes one session row.
func (d *DB) SessionSet(key, val []byte, o *pebble.WriteOptions) error { return d.set(key, val, o) }

// SessionDelete removes one session row.
func (d *DB) SessionDelete(key []byte, o *pebble.WriteOptions) error { return d.delete(key, o) }

// SessionClearRange removes a whole session key range (sync- or
// prefix-scoped clear).
func (d *DB) SessionClearRange(start, end []byte, o *pebble.WriteOptions) error {
	return d.deleteRange(start, end, o)
}

// SessionBatch stages multi-row session writes/deletes.
type SessionBatch struct {
	batch
}

// NewSessionBatch mints a batch for session-keyspace mutations.
func (d *DB) NewSessionBatch() *SessionBatch {
	d.acct.session.Add(1)
	return &SessionBatch{batch{b: d.newBatch(), open: &d.acct.session}}
}

// === engine-meta family ===
//
// Single fixed keys: the keyspace/format stamps, index-migration
// markers, the deferred-index and digest-build crash markers, the
// sync-run record, the stats sidecar, counters, and asset rows.
// Always single-key, never batched.

// MetaSet writes one engine-meta / fixed-key row.
func (d *DB) MetaSet(key, val []byte, o *pebble.WriteOptions) error { return d.set(key, val, o) }

// MetaDelete removes one engine-meta / fixed-key row.
func (d *DB) MetaDelete(key []byte, o *pebble.WriteOptions) error { return d.delete(key, o) }

// === source-cache manifest family ===

// SourceCacheSet writes one manifest or compatibility record. Row-copy
// mutations use RecordBatch because their scope-index obligations belong to
// the record family.
func (d *DB) SourceCacheSet(key, val []byte, o *pebble.WriteOptions) error {
	return d.set(key, val, o)
}

func (d *DB) SourceCacheDelete(key []byte, o *pebble.WriteOptions) error {
	return d.delete(key, o)
}

// SourceCacheKV is one manifest-entry write for SourceCacheSetMulti.
type SourceCacheKV struct {
	Key []byte
	Val []byte
}

// SourceCacheSetMulti rewrites one PAGE of manifest entries as a single
// atomic commit. The seal and rebind count-rewrites call it once per
// bounded page (manifestRewritePageRows in the engine's source_cache.go)
// so batch size and caller memory are bounded by the page, not the
// manifest (scopes × row kinds, connector-controlled). Durability is
// the caller's argument: the rebind clear commits intermediate pages
// NoSync and syncs the final page, which persists the WAL prefix
// covering every earlier one; the seal rides NoSync entirely, hardened
// by the ended_at stamp's fsync.
func (d *DB) SourceCacheSetMulti(kvs []SourceCacheKV, o *pebble.WriteOptions) error {
	if len(kvs) == 0 {
		return nil
	}
	b := d.newBatch()
	defer func() { _ = b.Close() }()
	for _, kv := range kvs {
		if err := b.Set(kv.Key, kv.Val, nil); err != nil {
			return err
		}
	}
	return b.Commit(o)
}

// === digest family: build / repair ===
//
// The seal-time digest build and the post-seal repair pass are the
// ONLY writers of digest nodes and roots (record mutations only stage
// invalidations, through RecordBatch). They commit node batches during
// their scans and stamp the global root as a single write.

// DigestBatch stages digest-node/root writes for the build/repair
// passes.
type DigestBatch struct {
	batch
}

// NewDigestBatch mints a batch for digest-keyspace writes.
func (d *DB) NewDigestBatch() *DigestBatch {
	d.acct.digest.Add(1)
	return &DigestBatch{batch{b: d.newBatch(), open: &d.acct.digest}}
}

// DigestSet writes one digest node/root row outside a batch (the
// global root stamp at the end of build/repair).
func (d *DB) DigestSet(key, val []byte, o *pebble.WriteOptions) error { return d.set(key, val, o) }

// DropKeyRange deletes a whole keyspace range. Purpose-named per
// caller in the registry: digest-state drops, hash-index drops,
// and migration range replacement with an empty source.
func (d *DB) DropKeyRange(start, end []byte, o *pebble.WriteOptions) error {
	return d.deleteRange(start, end, o)
}

// === ingest family: deferred index build, digest build, bulk import,
// synth-grant layer, id-index migration, compactor merges ===
//
// OBLIGATION: SST ingest bypasses the typed record ops, so it also
// bypasses the sourceScopeMayExist arming they perform. Any caller
// whose SSTs can contain by_source_scope index entries must re-probe
// or arm the gate after a successful ingest (bulk import's Finish
// does; the synth layer and rebuild compactors provably never emit
// scope keys; fold arms at NewFoldBatch). Ingesting scope entries
// behind an unarmed gate is the one unsound gate state — later
// overwrites/deletes would skip index cleanup and orphan entries.

// IngestSSTs ingests externally built SSTs (bulk import, synth-layer
// segments, compactor merge output). Paths must live on DB.FS().
func (d *DB) IngestSSTs(ctx context.Context, paths []string) error {
	return d.ingest(ctx, paths)
}

// ReplaceRangeWithSSTs atomically replaces span with the given SSTs
// (IngestAndExcise): the deferred index build's by_principal swap, the
// digest build's hash-index swap, the id-index migration's re-key, the
// compactor's bucket replacement.
func (d *DB) ReplaceRangeWithSSTs(ctx context.Context, paths []string, span pebble.KeyRange) error {
	return d.ingestAndExcise(ctx, paths, span)
}

// ExciseRange atomically drops span without ingesting anything
// (ResetForNewSync's wipe, the clone's engine-local scrub).
func (d *DB) ExciseRange(ctx context.Context, span pebble.KeyRange) error {
	return d.excise(ctx, span)
}
