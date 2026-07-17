// Package rawdb is the single owner of the engine's raw *pebble.DB —
// the compiler-enforced write choke point for the v3 storage engine.
//
// THE ENFORCEMENT MODEL: the pebble handle is an unexported field of
// DB, and the generic write primitives (set, delete, newBatch, ...)
// are unexported functions of this package. Code outside internal/rawdb
// physically cannot compose a raw write; it can only call the exported,
// purpose-named operations below, each of which states its keyspace
// family and carries that family's obligations (index maintenance,
// digest invalidation, crash markers) inside the operation instead of
// hoping every caller remembers them. Every recurring bug class this
// engine has shipped — the forgotten index write, the skipped digest
// invalidation, the fast-path flag that survived a failed mutation —
// is a caller that forgot an obligation; this package makes the
// obligation unforgettable by construction.
//
// What this package deliberately does NOT own: the engine's write
// BARRIER (writeMu / writeWG / closing / sealed / checkpointMu) stays
// in the pebble package. The barrier is lifecycle policy — who may
// write when — while rawdb is write mechanics — what a write must do.
// Callers arrive here already inside withWrite/withWriteAllowSealed;
// the known barrier bypasses (the synth-layer worker's background
// ingest, the compactor's DB() writes, cleanup's compaction driver)
// are enumerated on the operations that serve them.
//
// Reads are exposed liberally (Get / NewIter / Metrics): the bug class
// lives on the write side, and a read choke point would only add
// friction.
package rawdb

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/internal/keys"
)

// DB owns the raw pebble handle. Construct via Open; the pebble.DB is
// reachable outside this package only through the exported operations.
type DB struct {
	db *pebble.DB
	// fs is the filesystem the pebble.DB performs its IO through
	// (pebble.Options.FS resolved: the WithVFS override or vfs.Default).
	// Every SST this package stages for ingest MUST be created on it.
	fs vfs.FS
	// merge is the pre-built narrowed view MergeView() hands out.
	merge MergeView

	// deferredIdxPending mirrors the durable deferred-index marker
	// (keys.DeferredIdxPendingKey): grant writes that skipped the
	// inline by_principal index owe a rebuild at EndSync. The flag and
	// the durable key must never disagree — armed flag + absent key
	// means an in-process EndSync rebuilds while a crash+resume
	// silently skips the rebuild; cleared flag + present key forces a
	// spurious rebuild at the next open. ArmDeferredGrantIndex and
	// ClearDeferredGrantIndexMarker maintain the agreement on both
	// edges (durable half first; rollback/abort on failure), and
	// RestoreDeferredIdxPending re-arms from the key at Open.
	deferredIdxPending atomic.Bool

	// grantDigestsPresent reports whether the digest keyspace holds
	// any nodes — the gate for the record ops' digest-invalidation
	// obligation (present-means-exact: mutations on a digest-armed
	// file must tombstone their partition; on a digest-free file the
	// tombstones would be pure LSM bloat). Probed at Open
	// (ProbeGrantDigestsPresent), set true by the engine's seal-time
	// build, cleared by the engine's drop/reset paths.
	grantDigestsPresent atomic.Bool

	// testArmDeferredMarkerHook / testClearDeferredMarkerHook run
	// before the marker's durable commit / delete — the in-process
	// analogs of those writes failing. Installed only via
	// SetDeferredMarkerTestHooks (testing-gated).
	testArmDeferredMarkerHook   func() error
	testClearDeferredMarkerHook func() error
}

// Open opens the pebble database at dir. opts is consumed by
// pebble.Open exactly as given; fs must be the same filesystem the
// options carry (or vfs.Default when opts.FS is nil), because staged
// ingest files are created through it.
func Open(dir string, opts *pebble.Options, fs vfs.FS) (*DB, error) {
	db, err := pebble.Open(dir, opts)
	if err != nil {
		return nil, err
	}
	if fs == nil {
		fs = vfs.Default
	}
	d := &DB{db: db, fs: fs}
	d.merge = MergeView{db: d}
	return d, nil
}

// Close closes the underlying pebble.DB. The engine's teardown
// ordering (write barrier, worker drain) is the caller's job.
func (d *DB) Close() error { return d.db.Close() }

// FS returns the filesystem the DB's IO rides on. SSTs staged for
// Ingest/IngestAndExcise must be created through it.
func (d *DB) FS() vfs.FS { return d.fs }

// === read surface (exposed liberally) ===

// Get reads a key. Same contract as pebble.DB.Get, including
// pebble.ErrNotFound and the caller-owned closer.
func (d *DB) Get(key []byte) ([]byte, io.Closer, error) { return d.db.Get(key) }

// NewIter opens an iterator. Same contract as pebble.DB.NewIter.
func (d *DB) NewIter(o *pebble.IterOptions) (*pebble.Iterator, error) { return d.db.NewIter(o) }

// Metrics returns pebble's metrics snapshot.
func (d *DB) Metrics() *pebble.Metrics { return d.db.Metrics() }

// EstimateDiskUsage estimates on-disk size of the key range.
func (d *DB) EstimateDiskUsage(start, end []byte) (uint64, error) {
	return d.db.EstimateDiskUsage(start, end)
}

// UnsafeForTesting returns the raw *pebble.DB — the single test escape
// hatch through the choke point. Test fixtures legitimately construct
// states the production API cannot express by design: corruption
// planters (orphan index entries, tampered digests), crash fixtures,
// and marker manipulation. Each caller is asserting "this state is now
// unconstructible via the production API, and that is the point of my
// test". Panics outside `go test` (testing.Testing()); the os-IO/raw-
// write meta-tests additionally forbid it in non-test files.
func (d *DB) UnsafeForTesting() *pebble.DB {
	if !testing.Testing() {
		panic("rawdb.UnsafeForTesting: called outside a test binary")
	}
	return d.db
}

// === the compactor's narrowed view ===

// MergeView is the CONCRETE handle Engine.DB() hands the
// synccompactor: reads, LSM stats, the bulk range/ingest ops, and the
// fold-exempt batch — and nothing else. It must stay a concrete
// struct, not an interface over *DB: Go interfaces are structural, so
// an interface whose dynamic type is *DB would let any caller recover
// the omitted write families with a type assertion
// (`h.(interface{ MetaSet(...) error })`) — no internal-package import
// required (review finding, delta round). A struct with only these
// methods gives an assertion nothing to recover.
//
// NewFoldBatch is the one deliberate raw-write conduit here: the fold
// compactor rewrites record keyspaces wholesale, with its obligations
// handled by contract (digest state dropped, markers handled at the
// store layer) rather than derivation. Its use is fenced to
// pkg/synccompactor/pebble by meta-test.
type MergeView struct {
	db *DB
}

// MergeView returns the narrowed handle. Stable identity per DB (the
// engine returns it to external callers on every DB() call).
func (d *DB) MergeView() *MergeView { return &d.merge }

func (v *MergeView) Get(key []byte) ([]byte, io.Closer, error) { return v.db.Get(key) }

func (v *MergeView) NewIter(o *pebble.IterOptions) (*pebble.Iterator, error) {
	return v.db.NewIter(o)
}

func (v *MergeView) Metrics() *pebble.Metrics { return v.db.Metrics() }

func (v *MergeView) EstimateDiskUsage(start, end []byte) (uint64, error) {
	return v.db.EstimateDiskUsage(start, end)
}

func (v *MergeView) DropKeyRange(start, end []byte, o *pebble.WriteOptions) error {
	return v.db.DropKeyRange(start, end, o)
}

func (v *MergeView) IngestSSTs(ctx context.Context, paths []string) error {
	return v.db.IngestSSTs(ctx, paths)
}

func (v *MergeView) ReplaceRangeWithSSTs(ctx context.Context, paths []string, span pebble.KeyRange) error {
	return v.db.ReplaceRangeWithSSTs(ctx, paths, span)
}

func (v *MergeView) NewFoldBatch() *FoldBatch { return v.db.NewFoldBatch() }

// UnsafeForTesting delegates to DB.UnsafeForTesting — same
// testing.Testing() runtime gate, same meta-test source fence.
func (v *MergeView) UnsafeForTesting() *pebble.DB { return v.db.UnsafeForTesting() }

// === deferred-index marker + digest-presence state ===
//
// Write-side crash-contract state lives ON the choke point: the
// deferred marker's arm/clear are themselves durable writes with
// ordering obligations, and the digests-present flag gates an
// obligation the typed record ops stage. (Pre-2.5 these lived on the
// engine and arrived as injected closures.)

// DeferredIdxPending reports whether a deferred by_principal rebuild
// is owed (see the field doc).
func (d *DB) DeferredIdxPending() bool { return d.deferredIdxPending.Load() }

// ArmDeferredGrantIndex durably arms the deferred-index rebuild
// marker: CAS on the in-memory flag (repeat calls are one atomic
// load — the deferred write paths call this per record), then the
// fsync'd meta key. On a failed durable write the CAS rolls back so
// the flag and the key never disagree (armed flag + absent key = an
// in-process EndSync rebuilds while a crash+resume silently skips
// it). Called by StageGrantPutDeferred and by the engine's synth-layer
// ingest path.
func (d *DB) ArmDeferredGrantIndex() error {
	if !d.deferredIdxPending.CompareAndSwap(false, true) {
		return nil
	}
	if err := d.armDeferredMarkerDurably(); err != nil {
		d.deferredIdxPending.Store(false)
		return err
	}
	return nil
}

func (d *DB) armDeferredMarkerDurably() error {
	if d.testArmDeferredMarkerHook != nil {
		if err := d.testArmDeferredMarkerHook(); err != nil {
			return err
		}
	}
	return d.set(keys.DeferredIdxPendingKey(), nil, pebble.Sync)
}

// ClearDeferredGrantIndexMarker drops both halves of the marker after
// a successful rebuild. Durable delete FIRST, flag second — the same
// agreement contract as the arm side: a failed delete leaves BOTH
// armed, so the retried EndSync re-runs the (idempotent) rebuild and
// retries the clear, never the flag-cleared/key-present split that
// skipped the retry's rebuild and left a stale key forcing a spurious
// rebuild at the next open. The caller owns the write barrier (the
// engine runs this inside EndSync's sealed finalize window).
func (d *DB) ClearDeferredGrantIndexMarker() error {
	if d.testClearDeferredMarkerHook != nil {
		if err := d.testClearDeferredMarkerHook(); err != nil {
			return err
		}
	}
	if err := d.delete(keys.DeferredIdxPendingKey(), pebble.Sync); err != nil {
		return err
	}
	d.deferredIdxPending.Store(false)
	return nil
}

// RestoreDeferredIdxPending re-arms the in-memory flag from the
// durable marker — the Open-time half of the crash contract: a prior
// process may have deferred by_principal writes and been interrupted
// before the EndSync rebuild.
func (d *DB) RestoreDeferredIdxPending() error {
	_, closer, err := d.db.Get(keys.DeferredIdxPendingKey())
	switch {
	case err == nil:
		closer.Close()
		d.deferredIdxPending.Store(true)
		return nil
	case errors.Is(err, pebble.ErrNotFound):
		return nil
	default:
		return err
	}
}

// GrantDigestsPresent reports whether digest state exists (the record
// ops' invalidation gate).
func (d *DB) GrantDigestsPresent() bool { return d.grantDigestsPresent.Load() }

// SetGrantDigestsPresent flips the presence flag. The engine owns the
// transitions: true after a completed seal-time build, false on the
// drop/reset paths (whose durable deletes ride their own batches).
func (d *DB) SetGrantDigestsPresent(present bool) { d.grantDigestsPresent.Store(present) }

// ProbeGrantDigestsPresent initializes the presence flag with one
// bounded seek over the digest keyspace (the Open-time probe).
func (d *DB) ProbeGrantDigestsPresent() error {
	lo, hi := keys.DigestKeyspaceBounds()
	iter, err := d.db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return err
	}
	defer iter.Close()
	d.grantDigestsPresent.Store(iter.First())
	return iter.Error()
}

// SetDeferredMarkerTestHooks installs failure-injection hooks for the
// marker's durable arm/clear. Test-only, same runtime gate as
// UnsafeForTesting; pass nil to uninstall.
func (d *DB) SetDeferredMarkerTestHooks(armHook, clearHook func() error) {
	if !testing.Testing() {
		panic("rawdb.SetDeferredMarkerTestHooks: called outside a test binary")
	}
	d.testArmDeferredMarkerHook = armHook
	d.testClearDeferredMarkerHook = clearHook
}

// === lifecycle operations (write-class, engine-lifecycle-named) ===

// FlushMemtables forces the memtable out to L0 (pebble.DB.Flush,
// blocking). Used by the engine's durability boundaries (EndSync
// flush, pre-checkpoint flush, close).
func (d *DB) FlushMemtables() error { return d.db.Flush() }

// Checkpoint cuts a pebble checkpoint into destDir (created by pebble,
// must not exist). Caller holds the engine's checkpoint barrier.
func (d *DB) Checkpoint(destDir string) error { return d.db.Checkpoint(destDir) }

// Compact manually compacts the given key range. Serves cleanup's
// space-reclaim pass; deliberately barrier-free at the engine layer
// (see the checkpointMu inventory).
func (d *DB) Compact(ctx context.Context, start, end []byte, parallel bool) error {
	return d.db.Compact(ctx, start, end, parallel)
}

// WALSyncPoint commits an empty synced log record: a durability fence
// that guarantees every prior WAL entry is on disk without touching
// any keyspace. (pebble.DB.LogData with Sync.)
func (d *DB) WALSyncPoint() error {
	return d.db.LogData(nil, pebble.Sync)
}

// === generic primitives: UNEXPORTED, by design ===
//
// These are the only functions in the engine allowed to touch the raw
// pebble write API. Exported operations in this package's family files
// compose them; nothing outside the package can. If you are tempted to
// export one of these, you are about to reintroduce the bug class this
// package exists to kill — add a purpose-named operation instead.

func (d *DB) set(key, val []byte, o *pebble.WriteOptions) error { return d.db.Set(key, val, o) }

func (d *DB) delete(key []byte, o *pebble.WriteOptions) error { return d.db.Delete(key, o) }

func (d *DB) deleteRange(start, end []byte, o *pebble.WriteOptions) error {
	return d.db.DeleteRange(start, end, o)
}

func (d *DB) newBatch() *pebble.Batch { return d.db.NewBatch() }

func (d *DB) ingest(ctx context.Context, paths []string) error { return d.db.Ingest(ctx, paths) }

func (d *DB) ingestAndExcise(ctx context.Context, paths []string, span pebble.KeyRange) error {
	_, err := d.db.IngestAndExcise(ctx, paths, nil, nil, span)
	return err
}

func (d *DB) excise(ctx context.Context, span pebble.KeyRange) error {
	return d.db.Excise(ctx, span)
}
