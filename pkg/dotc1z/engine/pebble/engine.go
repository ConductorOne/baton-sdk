package pebble

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"google.golang.org/protobuf/proto"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/codec"
)

// Engine is the v3 Pebble-backed storage engine. Methods are
// goroutine-safe modulo the lifecycle rules in this file:
//
//   - Open the engine once with Open(...).
//   - Concurrent Reader/Writer calls are safe.
//   - Close() releases all resources. After Close, all methods return
//     ErrEngineClosing.
type Engine struct {
	db         *pebble.DB
	dbDir      string
	opts       *Options
	pebbleOpts *pebble.Options

	// currentSync is the engine's open sync_id (raw 20-byte KSUID).
	// Set by StartNewSync / ResumeSync / SetCurrentSync. Empty when
	// no sync is open. Reads under "" syncID consult this; if empty,
	// they return ErrNoCurrentSync.
	currentSyncMu sync.RWMutex
	currentSync   []byte
	// freshSync is true between MarkFreshSync (called by StartNewSync)
	// and EndSync. Indicates the engine can take perf shortcuts that
	// trade durability for throughput while the connector is the
	// source of truth (host crash → connector replays). Concretely:
	// writes skip per-batch fsync (use pebble.NoSync) and PutXRecord
	// can skip the read-before-write index-cleanup path because this
	// sync_id is guaranteed to be empty.
	freshSync bool
	// freshGrantsEmpty / freshResourcesEmpty
	// are one-shot bits guarded by currentSyncMu. MarkFreshSync sets
	// each to true; the first PutXxxRecords call of the fresh sync
	// reads the value via takeFreshXxxEmpty() which returns it and
	// clears it. Concrete use: gate the skip-Get fast path on "first
	// call only" — subsequent calls in the same fresh sync must
	// still read-before-write to clean up cross-call duplicate index
	// entries.
	freshGrantsEmpty    bool
	freshResourcesEmpty bool

	// writeWG tracks in-flight writes. Incremented at the start of
	// every Writer method, decremented in defer.
	writeWG sync.WaitGroup
	writeMu sync.Mutex
	closing atomic.Bool // strict write-barrier flag, read on every Writer call
	closeMu sync.Mutex

	// computedStats holds caller-computed stats records stashed via
	// StashComputedSyncStats, keyed by sync_id. PersistSyncStats pops
	// and persists the stashed record instead of re-scanning the
	// keyspaces — used by bulk imports that already counted every
	// record they wrote.
	computedStatsMu sync.Mutex
	computedStats   map[string]*v3.SyncStatsRecord

	// deferredGrantStats holds the grant counts BuildDeferredGrantIndexes
	// accumulated while scanning the whole grant primary keyspace, so
	// computeSyncStats can skip its own O(grants) scan at EndSync. Consumed
	// once, guarded by sync_id.
	deferredGrantStatsMu sync.Mutex
	deferredGrantStats   *deferredGrantStats

	// deferredIdxPending is set by grant writes that skipped the inline
	// by_principal index write (all of them: the index family is scattered
	// relative to the entitlement-first write order, so it is always built
	// as one sorted SST at EndSync — see BuildDeferredGrantIndexes).
	deferredIdxPending atomic.Bool

	// grantDigestsPresent reports whether the digest keyspace holds any
	// nodes — i.e. whether a grant mutation must invalidate the touched
	// entitlement's digest + hash-index ranges
	// (stageGrantDigestInvalidation). Probed once at Open, set by the
	// seal-time build, cleared by ResetForNewSync and the Drop* paths.
	grantDigestsPresent atomic.Bool

	// grantDigestBuildPending mirrors the durable digest-build marker
	// (encodeGrantDigestBuildPendingKey): true between a digest build's
	// arm and its completion (or the drop that cleans up after it). A
	// writable Open that finds the durable marker drops all digest state
	// immediately, so on a writable engine this is only ever true while
	// a build owns the write barrier or after an in-process build
	// failure whose cleanup drop itself failed. A READ-ONLY Open cannot
	// drop, so the flag stays set and the digest root getters
	// (getPartitionDigestRoot, GetGrantDigestGlobalRoot) report "never
	// built" instead of trusting nodes a crashed build half-committed.
	grantDigestBuildPending atomic.Bool

	// Test seams for the digest build/repair tests: a hook fired at
	// named points inside buildGrantDigestsFromSpill
	// (grant_digest_build_crash_test.go) and a batch flush-threshold
	// override — shared by the build's fold and the streaming partition
	// repair (repairOneGrantDigestPartitionLocked) — so a small test
	// dataset exercises the mid-stream commit paths. Nil/zero in
	// production.
	testDigestBuildHook      func(stage string) error
	testDigestNodeFlushBytes int

	// synthLayer is the open wave-scoped layer session, if any (see
	// BeginSynthesizedGrantLayer). Single producer: the expansion driver
	// opens/adds/finishes sessions strictly sequentially. synthLayerMu
	// guards the pointer itself — Abort and Close read/nil it without the
	// engine write barrier, so pointer access needs its own lock even
	// though the session contents are only ever touched by one goroutine.
	synthLayerMu sync.Mutex
	synthLayer   *synthGrantLayerSession

	// checkpointMu is the barrier between CheckpointTo's Flush→Checkpoint→
	// WAL-truncate window (write lock) and DB mutations that bypass writeMu
	// (read lock). The full bypass inventory:
	//   - the synth-layer worker's background SST ingest (takes the read
	//     lock). It cannot take writeMu: an Add holding writeMu blocks on
	//     the worker's bounded segment channel, so worker-needs-writeMu
	//     would deadlock. A dedicated RWMutex gives CheckpointTo exclusion
	//     without that cycle.
	//   - CompactAllRanges/Flush (cleanup.go): deliberately writeMu-free
	//     long operations; safe against checkpoints because pebble
	//     compactions/flushes are internally consistent with Checkpoint
	//     and LogData(nil) carries no keys.
	//   - the compactor's raw DB() writes: fenced by call ordering (the
	//     merge completes before the store's save/CheckpointTo runs).
	// Everything else (record writes, sessions, the stats sidecar, the
	// deferred index build, bulk-import ingest) holds writeMu via
	// withWrite*, which CheckpointTo also takes.
	checkpointMu sync.RWMutex

	// sealed is the explicit post-EndSync lifecycle state. A successful
	// EndSync seals the engine: record writes fail with ErrEngineSealed and
	// the compaction scheduler is paused, because the only work left before
	// save/close (checkpoint + envelope encode) never benefits from either.
	// Binding a sync again (SetCurrentSync / MarkFreshSync) unseals and
	// resumes compactions. Sync-run metadata writes (PutSyncRunRecord and
	// friends) are exempt — callers legitimately stamp ended_at overrides,
	// diff links, and supports_diff markers on a finished sync. Without this
	// state the "no writes while compactions are paused" invariant was
	// convention only, and a caller that kept writing after EndSync would
	// silently accumulate L0 until pebble stalled writes at
	// L0StopWritesThreshold with nothing left to resume the scheduler.
	sealed atomic.Bool
	// sealMu makes the (sealed, compactions-paused) pair transition
	// atomically: an interleaved seal/unseal could otherwise end at
	// sealed=false with the scheduler paused — writes allowed with nothing
	// draining L0, the silent stall state sealing exists to eliminate.
	sealMu sync.Mutex

	// compactionScheduler is the engine's pausable compaction scheduler,
	// installed by newPebbleOptions. Pause/resume via pauseCompactions /
	// resumeCompactions (package-private; see those funcs for why).
	compactionScheduler *pausableCompactionScheduler

	// Lazy bare-id entitlement lookup (see lookup.go). entIDLookupGen is
	// bumped by every entitlement-keyspace mutation; the map rebuilds on the
	// next lookup when its built generation is stale.
	entIDLookupGen      atomic.Uint64
	entIDLookupMu       sync.Mutex
	entIDLookup         map[string][]entitlementIdentity
	entIDLookupBuiltGen uint64

	// migratedOnOpen reports that this Open ran the in-place id-index
	// migration. The store layer uses it to mark a writable store dirty so
	// the migrated layout is saved back into the c1z once, instead of
	// re-running the O(rows) migration on every subsequent open.
	migratedOnOpen bool

	expandedWriteCalls    atomic.Int64
	expandedWriteRows     atomic.Int64
	synthesizedWriteCalls atomic.Int64
	synthesizedWriteRows  atomic.Int64
}

// Open creates or opens a Pebble engine rooted at dir. If dir does
// not exist, Pebble creates it. The caller is responsible for
// providing a directory that won't be shared with another Pebble
// instance.
func Open(ctx context.Context, dir string, opts ...Option) (*Engine, error) {
	o := defaultOptions()
	for _, opt := range opts {
		opt(o)
	}

	pebbleOpts := newPebbleOptions(o)

	db, err := pebble.Open(dir, pebbleOpts)
	if err != nil {
		// pebble.Open failure path: we minted a Cache (when no shared
		// cache was supplied) and won't reach Engine.Close. Unref it
		// here so the cache memory is released. If the caller supplied
		// the cache, they own its lifecycle and we leave it alone.
		if o.sharedCache == nil && pebbleOpts.Cache != nil {
			pebbleOpts.Cache.Unref()
		}
		return nil, fmt.Errorf("pebble.Open: %w", err)
	}

	e := &Engine{
		db:         db,
		dbDir:      dir,
		opts:       o,
		pebbleOpts: pebbleOpts,
	}
	if s, ok := pebbleOpts.Experimental.CompactionScheduler.(*pausableCompactionScheduler); ok {
		e.compactionScheduler = s
	}
	// Enforce the single-sync key-layout contract before touching any
	// keys: reject an old multi-sync-layout file (which the current
	// encoders would silently mis-decode) and stamp a fresh writable
	// file. Runs before migrations so we never try to backfill indexes
	// on a file we can't read.
	if err := e.verifyOrStampKeyspaceVersion(ctx); err != nil {
		_ = e.Close()
		return nil, err
	}
	if err := e.verifyOrStampIDIndexFormat(ctx); err != nil {
		_ = e.Close()
		return nil, err
	}
	// Restore the durable deferred-index marker (see
	// encodeDeferredIdxPendingKey): a prior process may have deferred
	// by_principal writes and been interrupted before the EndSync rebuild.
	if _, closer, err := e.db.Get(encodeDeferredIdxPendingKey()); err == nil {
		closer.Close()
		e.deferredIdxPending.Store(true)
	} else if !errors.Is(err, pebble.ErrNotFound) {
		_ = e.Close()
		return nil, err
	}
	// Honor the durable digest-build marker (see
	// encodeGrantDigestBuildPendingKey): a prior process was killed
	// mid-digest-build, after some digest-node commits were durable but
	// before the hash-index ingest completed. Those nodes LOOK present
	// while the index beneath them is empty or stale, so nothing stored
	// may be trusted: drop it all before probing presence — absent
	// digests are always safe (present-means-exact, digest.go). A
	// read-only open cannot drop; it keeps the flag set instead, which
	// makes the digest root getters report "never built".
	if _, closer, err := e.db.Get(encodeGrantDigestBuildPendingKey()); err == nil {
		closer.Close()
		e.grantDigestBuildPending.Store(true)
		if !o.readOnly {
			ctxzap.Extract(ctx).Warn("pebble: interrupted grant digest build detected at open; dropping all digest state — the next EndSync rebuilds it from scratch")
			if err := e.dropAllGrantDigestStateLocked(); err != nil {
				_ = e.Close()
				return nil, fmt.Errorf("pebble: drop digest state left by an interrupted build: %w", err)
			}
		}
	} else if !errors.Is(err, pebble.ErrNotFound) {
		_ = e.Close()
		return nil, err
	}
	// Arm the mutation-path digest invalidation iff the file actually
	// holds digest nodes (one bounded seek; see grant_digest.go).
	if err := e.probeGrantDigestsPresent(); err != nil {
		_ = e.Close()
		return nil, err
	}
	// Run secondary-index migrations before returning. Migrations
	// are skipped for read-only opens (the on-disk file is
	// immutable, so we'd error out trying to backfill).
	if err := e.applyIndexMigrations(ctx); err != nil {
		_ = e.Close()
		return nil, fmt.Errorf("pebble: apply index migrations: %w", err)
	}
	return e, nil
}

// Close shuts down the engine. After Close, all methods return
// ErrEngineClosing. Close blocks until all in-flight writes complete.
func (e *Engine) Close() error {
	e.closeMu.Lock()
	defer e.closeMu.Unlock()
	if e.db == nil {
		return nil
	}
	e.closing.Store(true)
	e.writeWG.Wait()
	// A leaked synthesized-grant layer session (possible only if a panic
	// unwound past the expansion driver's Abort) has a background worker
	// ingesting through e.db; drain it before tearing the DB down. This
	// runs AFTER the closing/writeWG barrier so no in-flight Add/Finish
	// (which run under withWrite) can be touching the session concurrently
	// — Abort itself takes no write barrier, only synthLayerMu for the
	// pointer handoff, and is a no-op when no session is open.
	_ = e.AbortSynthesizedGrantLayer(context.Background())
	// Hold writeMu for the teardown: writeWG only covers withWrite users,
	// while CheckpointTo takes writeMu directly (no WG participation). A
	// CheckpointTo that passed its closing check but hasn't locked yet must
	// find either the mutex held or db nil'd under the lock — never a db
	// torn down mid-checkpoint.
	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	// Invariant: flush before close on any write path. This drives the
	// memtable out to an SST so a Close is never the step that leaves
	// un-materialized writes behind — independent of whether EndSync or
	// CheckpointTo (which flush for their own reasons) ran first. Skipped
	// in read-only mode, where Flush is illegal and there is nothing to
	// harden. A no-op when the memtable is already empty.
	var err error
	if !e.opts.readOnly {
		if ferr := e.db.Flush(); ferr != nil {
			err = fmt.Errorf("flush during close: %w", ferr)
		}
	}
	err = errors.Join(err, e.db.Close())
	e.db = nil
	// Release the cache if we minted it (no shared cache).
	if e.opts.sharedCache == nil && e.pebbleOpts != nil && e.pebbleOpts.Cache != nil {
		e.pebbleOpts.Cache.Unref()
	}
	return err
}

// SetCurrentSync sets the engine's tracked current sync_id from a
// string KSUID. Subsequent Put*/List* calls with an empty syncID
// use this value. Clears the freshSync flag — a bare SetCurrentSync
// is conservative (treats the sync as resumable, so writes keep
// fsync + read-before-write).
func (e *Engine) SetCurrentSync(syncID string) error {
	idBytes, err := codec.EncodeSyncID(syncID)
	if err != nil {
		return err
	}
	e.currentSyncMu.Lock()
	e.currentSync = idBytes
	e.freshSync = false
	e.freshGrantsEmpty = false
	e.freshResourcesEmpty = false
	e.currentSyncMu.Unlock()
	// Binding a sync means more writes are coming; leave the sealed state
	// and resume compactions so L0 keeps draining (see seal).
	e.unseal()
	return nil
}

// pauseCompactions stops the engine from granting new automatic compactions.
// In-flight compactions finish; flushes are unaffected. Intended for the
// EndSync-to-close window, where compaction output never survives to the
// saved artifact but competes with the deferred index build and envelope
// encode.
//
// Deliberately unexported, as is resumeCompactions: the only way for a
// caller outside this package to restart compactions is to bind a sync
// (StartNewSync / ResumeSync / SetCurrentSync), which also unseals the
// engine. Pause without seal (or resume without a bound sync) is how the
// "writes on a paused scheduler stall at L0StopWritesThreshold" hang
// happens, so the two transitions are only available as a pair.
func (e *Engine) pauseCompactions() {
	if e.compactionScheduler != nil {
		e.compactionScheduler.pause()
	}
}

// resumeCompactions re-enables automatic compaction granting. See
// pauseCompactions for why this is unexported.
func (e *Engine) resumeCompactions() {
	if e.compactionScheduler != nil {
		e.compactionScheduler.resume()
	}
}

// seal moves the engine into the explicit post-EndSync state: record
// writes fail with ErrEngineSealed and automatic compactions stop. Called
// by Adapter.EndSync after a successful finalize; undone by binding a sync
// (SetCurrentSync / MarkFreshSync → unseal). See the sealed field doc for
// why this is a hard state rather than a convention.
func (e *Engine) seal() {
	e.sealMu.Lock()
	defer e.sealMu.Unlock()
	e.sealed.Store(true)
	e.pauseCompactions()
}

// unseal leaves the sealed state and resumes automatic compactions.
func (e *Engine) unseal() {
	e.sealMu.Lock()
	defer e.sealMu.Unlock()
	e.sealed.Store(false)
	e.resumeCompactions()
}

// IsSealed reports whether the engine is in the post-EndSync sealed state.
func (e *Engine) IsSealed() bool {
	return e.sealed.Load()
}

// MarkFreshSync sets currentSync AND flags the sync as freshly
// started (no prior records under this sync_id). The engine then
// takes the perf-fast write path: pebble.NoSync per commit and skip
// read-before-write index cleanup. The host crash semantics match
// SQLite's PRAGMA synchronous=NORMAL — the connector is the source
// of truth during the sync; a crash forces re-sync rather than
// silent data loss.
//
// Callers should call EndFreshSync (via Flush) at sync end to harden
// the data with a single fsync.
func (e *Engine) MarkFreshSync(syncID string) error {
	idBytes, err := codec.EncodeSyncID(syncID)
	if err != nil {
		return err
	}
	e.currentSyncMu.Lock()
	e.currentSync = idBytes
	e.freshSync = true
	e.freshGrantsEmpty = true
	e.freshResourcesEmpty = true
	e.currentSyncMu.Unlock()
	// A fresh sync writes heavily; leave the sealed state and resume
	// compactions so L0 keeps draining (see seal).
	e.unseal()
	return nil
}

// clearCurrentSync detaches the engine from its current sync and disables
// fresh-sync write shortcuts. After this, operations that resolve an empty
// sync_id fail with ErrNoCurrentSync until StartNewSync, ResumeSync, or
// SetCurrentSync binds a sync again.
func (e *Engine) clearCurrentSync() {
	e.currentSyncMu.Lock()
	e.currentSync = nil
	e.freshSync = false
	e.freshGrantsEmpty = false
	e.freshResourcesEmpty = false
	e.currentSyncMu.Unlock()
}

// IsFreshSync reports whether the engine is in the fresh-sync write
// path (set by MarkFreshSync).
func (e *Engine) IsFreshSync() bool {
	e.currentSyncMu.RLock()
	defer e.currentSyncMu.RUnlock()
	return e.freshSync
}

// GrantDigestIndexEnabled reports whether the seal-time deferred pass
// builds the by_entitlement_principal_hash index and grant digests.
// See WithGrantDigestIndex.
func (e *Engine) GrantDigestIndexEnabled() bool { return e.opts.grantDigestIndex }

// takeFreshGrantsEmpty / takeFreshResourcesEmpty return true
// exactly once per fresh sync, for the first PutXxxRecords call
// of that type after
// MarkFreshSync. Subsequent calls (and any call after EndSync) see
// false. PutXxxRecords uses these to safely skip the
// read-before-write Get on the first bulk write of each type:
// the keyspace under the freshly-minted sync_id is provably empty
// by construction.
func (e *Engine) takeFreshGrantsEmpty() bool {
	e.currentSyncMu.Lock()
	defer e.currentSyncMu.Unlock()
	if !e.freshGrantsEmpty {
		return false
	}
	e.freshGrantsEmpty = false
	return true
}

func (e *Engine) takeFreshResourcesEmpty() bool {
	e.currentSyncMu.Lock()
	defer e.currentSyncMu.Unlock()
	if !e.freshResourcesEmpty {
		return false
	}
	e.freshResourcesEmpty = false
	return true
}

// EndFreshSync clears the fresh-sync flag and flushes the memtable
// + fsyncs the WAL so the data written during the sync is on disk
// before the caller returns. Called by Adapter.EndSync.
//
// Uses withWrite (not a bare writeMu) so the flush participates in the
// closing check and writeWG: Close tears e.db down after writeWG.Wait,
// and a bare-mutex EndFreshSync racing Close would flush a nil db.
func (e *Engine) EndFreshSync(ctx context.Context) error {
	// AllowSealed: this is the last step of EndSync's sealed finalize
	// window (see Adapter.EndSync).
	return e.withWriteAllowSealed(func() error {
		e.currentSyncMu.RLock()
		wasFresh := e.freshSync
		e.currentSyncMu.RUnlock()
		if !wasFresh {
			e.clearCurrentSync()
			return nil
		}
		// Flush the memtable (turns NoSync-buffered writes into on-disk
		// SSTs) and let pebble.LogData with Sync force-fsync the WAL tail.
		if err := e.db.Flush(); err != nil {
			return fmt.Errorf("EndFreshSync: flush: %w", err)
		}
		if err := e.db.LogData(nil, pebble.Sync); err != nil {
			return fmt.Errorf("EndFreshSync: fsync WAL: %w", err)
		}
		e.clearCurrentSync()
		return nil
	})
}

// currentSyncBytes returns the engine's tracked sync_id (raw bytes)
// or nil if none is set. The sync_id is never part of a key; this is
// used only to validate that a caller's sync_id matches the engine's
// one bound sync (see StartBulkSyncImport).
func (e *Engine) currentSyncBytes() []byte {
	e.currentSyncMu.RLock()
	defer e.currentSyncMu.RUnlock()
	out := make([]byte, len(e.currentSync))
	copy(out, e.currentSync)
	return out
}

// requireCurrentSync returns ErrNoCurrentSync unless a sync is bound
// (StartNewSync/SetCurrentSync, cleared by EndSync). Record writes
// gate on this so data never lands without a sync-run record — the
// sync_id is NOT encoded in keys, but a write still has to happen
// inside an open sync. Reads do not gate: a finished sync's data
// persists and stays readable after EndSync clears the binding.
func (e *Engine) requireCurrentSync() error {
	e.currentSyncMu.RLock()
	defer e.currentSyncMu.RUnlock()
	if len(e.currentSync) == 0 {
		return ErrNoCurrentSync
	}
	return nil
}

// checkWritable returns ErrEngineClosing if the engine has been closed,
// and ErrEngineSealed after a successful EndSync until a sync is bound
// again. Called at the start of every Writer method.
func (e *Engine) checkWritable() error {
	if err := e.checkWritableAllowSealed(); err != nil {
		return err
	}
	if e.sealed.Load() {
		return ErrEngineSealed
	}
	return nil
}

// checkWritableAllowSealed is checkWritable without the sealed check, for
// the few write paths that legitimately run on a finished sync (sync-run
// metadata updates and the pre-StartNewSync wipe).
func (e *Engine) checkWritableAllowSealed() error {
	if e.closing.Load() {
		return ErrEngineClosing
	}
	if e.db == nil {
		return ErrEngineClosing
	}
	if e.opts.readOnly {
		return errors.New("pebble engine: opened read-only")
	}
	return nil
}

// withWrite wraps a writer function with WaitGroup tracking + the
// closing and sealed checks. The closure runs only if the engine is open
// and a sync is bound (not sealed).
func (e *Engine) withWrite(fn func() error) error {
	if e.sealed.Load() {
		return ErrEngineSealed
	}
	return e.withWriteAllowSealed(func() error {
		// Re-check under writeMu: the first check is lock-free, so a writer
		// that passed it and then blocked on writeMu (e.g. behind the
		// EndSync finalize steps) must not commit once the engine sealed in
		// the meantime — a grant landing in that window would be
		// permanently missing from by_principal (the deferred rebuild
		// already ran and the pending marker was cleared).
		if e.sealed.Load() {
			return ErrEngineSealed
		}
		return fn()
	})
}

// withWriteAllowSealed is withWrite without the sealed check. Reserved for
// writes that are part of the sealed lifecycle itself: sync-run metadata
// stamps on a finished sync (ended_at overrides, diff links, supports_diff)
// and ResetForNewSync's wipe on the way into a new sync. Record-data writes
// must use withWrite.
func (e *Engine) withWriteAllowSealed(fn func() error) error {
	if err := e.checkWritableAllowSealed(); err != nil {
		return err
	}
	e.writeWG.Add(1)
	defer e.writeWG.Done()
	// Re-check after Add because closing could have flipped between
	// our first check and our Add.
	if e.closing.Load() {
		return ErrEngineClosing
	}
	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	return fn()
}

func (e *Engine) Save(ctx context.Context, dest string) error {
	return errors.New("pebble engine: Save requires the dotc1z.Save shim (envelope write); use CheckpointTo for direct directory access")
}

// DBDir returns the on-disk path the engine writes to. Exported so
// the Adapter can implement OutputFilepath / CurrentDBSizeBytes.
func (e *Engine) DBDir() string {
	return e.dbDir
}

// CurrentDBSizeBytes returns the total size of regular files in the Pebble
// database directory. This is the Pebble equivalent of C1File's SQLite
// DBSizeProvider capability: it reports the uncompressed working set on disk,
// including WAL/log, MANIFEST, OPTIONS, and SST files currently present.
func (e *Engine) CurrentDBSizeBytes() (int64, error) {
	if e.dbDir == "" {
		return 0, errors.New("pebble engine: db dir is empty")
	}
	var total int64
	if err := filepath.WalkDir(e.dbDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return fmt.Errorf("stat %s: %w", path, err)
		}
		if info.Mode().IsRegular() {
			total += info.Size()
		}
		return nil
	}); err != nil {
		if os.IsNotExist(err) {
			return 0, fmt.Errorf("pebble engine: db dir missing: %w", err)
		}
		return 0, err
	}
	return total, nil
}

// DB returns the underlying *pebble.DB. Exported for the
// synccompactor/pebble package; callers must not Close it directly
// (use Engine.Close) and must respect the engine's lifecycle. Returns
// nil after Close. Callers that write the entitlement keyspace through
// this handle (ingests, excises) must call InvalidateBareIDLookups
// afterwards.
func (e *Engine) DB() *pebble.DB { return e.db }

// InvalidateBareIDLookups invalidates the lazily built bare-id lookup
// state (see lookup.go). Engine write paths call this internally; it is
// exported for callers that mutate the keyspace through DB() directly.
func (e *Engine) InvalidateBareIDLookups() { e.noteEntitlementKeyspaceWrite() }

// MigratedOnOpen reports whether this Open ran the in-place id-index
// migration (see migratedOnOpen).
func (e *Engine) MigratedOnOpen() bool { return e.migratedOnOpen }

// CheckpointTo writes a self-contained Pebble directory snapshot to
// destDir. destDir must not exist yet. Pebble creates it and
// hard-links SSTs where possible.
//
// The source engine stays writable after CheckpointTo returns; writes
// are only blocked while the checkpoint is cut. This is the building
// block dotc1z's higher-level Save wraps with the v3 envelope format.
//
// Read-only engines cannot call pebble.DB.Checkpoint (it copies
// OPTIONS via d.optionsFileNum, which Pebble never populates on
// read-only open). Those engines clone the on-disk tree with
// vfs.Clone instead.
//
// The explicit Flush is what makes the snapshot WAL-independent:
// every committed write lands in SSTs before the checkpoint is cut.
// We deliberately do NOT pass pebble.WithFlushedWAL() — it would be
// redundant after the flush, and it appends a WAL record, guaranteeing
// the checkpoint carries a WAL file.
//
// CheckpointTo takes the engine write barrier for the whole
// Flush→Checkpoint→truncate window. That prevents a write from
// committing between the Flush and Checkpoint — such a write would
// otherwise exist only in the WAL, which truncateCheckpointWALs discards.
// It also takes checkpointMu exclusively for the same window: the
// synth-layer session's background worker ingests SSTs outside writeMu
// (see ingestSynthLayerSegment), and a flushable ingest landing mid-window
// would be a WAL-only record the truncate discards.
func (e *Engine) CheckpointTo(ctx context.Context, destDir string) error {
	// Wait for all in-flight writes to complete.
	e.writeWG.Wait()

	if e.closing.Load() {
		return ErrEngineClosing
	}
	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	e.checkpointMu.Lock()
	defer e.checkpointMu.Unlock()
	// Re-check under the lock: Close (which also takes writeMu for its
	// teardown) may have won the race and nil'd e.db.
	if e.closing.Load() || e.db == nil {
		return ErrEngineClosing
	}

	if e.opts.readOnly {
		return copyReadOnlyDBDir(e.dbDir, destDir)
	}

	if err := e.db.Flush(); err != nil {
		return fmt.Errorf("checkpoint flush: %w", err)
	}
	if err := e.db.Checkpoint(destDir); err != nil {
		return fmt.Errorf("checkpoint db %s: %w", destDir, err)
	}
	if err := truncateCheckpointWALs(destDir); err != nil {
		return fmt.Errorf("checkpoint truncate WALs: %w", err)
	}

	return nil
}

// copyReadOnlyDBDir clones a read-only Pebble directory tree into
// destDir. destDir must not exist yet, matching db.Checkpoint's
// contract.
func copyReadOnlyDBDir(srcDir, destDir string) error {
	if _, err := os.Stat(destDir); err == nil {
		return &os.PathError{Op: "checkpoint", Path: destDir, Err: fs.ErrExist}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	pfs := vfs.Default
	// Skip LOCK: the source engine holds an exclusive lock on it (on
	// Windows the same process cannot reopen it for read). The clone
	// gets a fresh LOCK when Pebble opens destDir.
	ok, err := vfs.Clone(pfs, pfs, srcDir, destDir,
		vfs.CloneSync,
		vfs.CloneSkip(func(path string) bool {
			return filepath.Base(path) == "LOCK"
		}),
	)
	if err != nil {
		return fmt.Errorf("checkpoint copy: %w", err)
	}
	if !ok {
		return fmt.Errorf("checkpoint copy: source dir %q missing", srcDir)
	}
	return nil
}

// truncateCheckpointWALs truncates every WAL segment in a freshly cut
// checkpoint directory to zero bytes.
//
// Why: pebble copies WAL files into checkpoints wholesale
// (checkpoint.go: recycling makes hard-links unsafe), and WAL
// recycling means the copied file's physical content is mostly stale
// records from the file's previous life. Replaying that on every
// subsequent open is expensive — stale chunks fail their CRC and
// trigger pebble's per-bit bit-flip corruption diagnostic
// (record.Reader.nextChunk → bitflip.CheckSliceForBitFlip). Profiling
// a 500-source compaction showed WAL replay at ~23% of total CPU.
//
// After CheckpointTo's flush there is no unflushed data, so the WAL
// carries nothing the checkpoint needs. We truncate rather than delete:
// a zero-length WAL is indistinguishable from a freshly created one
// (replay reads a clean EOF), whereas deleting the file would change
// the file set pebble's open sequence discovers and validates against
// the manifest's minUnflushedLogNum.
func truncateCheckpointWALs(destDir string) error {
	entries, err := os.ReadDir(destDir)
	if err != nil {
		return err
	}
	for _, ent := range entries {
		if ent.IsDir() || filepath.Ext(ent.Name()) != ".log" {
			continue
		}
		if err := os.Truncate(filepath.Join(destDir, ent.Name()), 0); err != nil {
			return err
		}
	}
	return nil
}

// internal: marshal a record value deterministically.
func marshalRecord(m proto.Message) ([]byte, error) {
	return proto.MarshalOptions{Deterministic: true}.Marshal(m)
}

// marshalRecordAppend is marshalRecord into a caller-owned buffer, for
// hot paths that immediately copy the bytes onward (e.g. the bulk
// import's SST appends) and can reuse one scratch across records.
func marshalRecordAppend(dst []byte, m proto.Message) ([]byte, error) {
	return proto.MarshalOptions{Deterministic: true}.MarshalAppend(dst, m)
}

// NOTE: formerly used vtprotobuf, but it is unmaintained and doesn't support deterministic serialization.
func unmarshalRecord(b []byte, m proto.Message) error {
	return proto.Unmarshal(b, m)
}
