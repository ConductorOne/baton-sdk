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
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	v2pb "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/codec"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/internal/rawdb"
)

// Engine is the v3 Pebble-backed storage engine. Methods are
// goroutine-safe modulo the lifecycle rules in this file:
//
//   - Open the engine once with Open(...).
//   - Concurrent Reader/Writer calls are safe.
//   - Close() releases all resources. After Close, all methods return
//     ErrEngineClosing.
type Engine struct {
	// db is the write choke point: the raw *pebble.DB lives inside
	// internal/rawdb and is reachable only through rawdb's exported,
	// purpose-named operations (reads are passed through liberally).
	// See the rawdb package doc for the enforcement model.
	db         *rawdb.DB
	dbDir      string
	opts       *Options
	pebbleOpts *pebble.Options

	// Embedded Unimplemented stubs for the gRPC service surfaces the
	// engine's connectorstore face (adapter.go / adapter_reader.go)
	// implements partially. Each implemented method overrides the
	// stub. GrantsReaderServiceServer is deliberately NOT stubbed: the
	// engine implements it in full, and adapter_reader.go asserts the
	// complete contract — re-adding the stub would make that assertion
	// vacuous.
	v2pb.UnimplementedResourceTypesServiceServer
	reader_v2.UnimplementedResourceTypesReaderServiceServer
	v2pb.UnimplementedResourcesServiceServer
	reader_v2.UnimplementedResourcesReaderServiceServer
	v2pb.UnimplementedEntitlementsServiceServer
	reader_v2.UnimplementedEntitlementsReaderServiceServer
	v2pb.UnimplementedGrantsServiceServer
	reader_v2.UnimplementedSyncsReaderServiceServer

	// lifecycleMu serializes the sync-lifecycle transitions
	// (StartNewSync/ResumeSync/SetCurrentSync/CheckpointSync/EndSync),
	// whose bodies are read-check-write sequences over the sync-run
	// record + the currentSync binding. Formerly the Adapter layer's
	// mutex; the record writes themselves ride the write barrier.
	lifecycleMu sync.Mutex
	// resolvedFS is rawdb's Open-time FS resolution (WithVFS override
	// or vfs.Default), snapshotted so fs() stays valid after Close
	// nils db — see fs().
	resolvedFS vfs.FS

	// writeMu is the engine's one write lock. Every DB mutation the
	// engine's own goroutines make runs under it (withWrite*, Close,
	// CheckpointTo, CompactAllRanges, Flush), and every lifecycle
	// transition that writers must not straddle takes it: binding or
	// clearing the sync (binding), seal/unseal, opening or aborting the
	// synth-layer session, and Close's teardown. Fields below marked
	// "under writeMu" are read and written only while holding it.
	// TestWriteMuHolders checks the mutation side statically.
	writeMu sync.Mutex

	// binding is the sync lifecycle state (bound sync_id, fresh, sealed)
	// as one immutable snapshot. Transitions replace the pointer under
	// writeMu; readers on any goroutine Load it without a lock and see a
	// consistent triple. Never nil after Open.
	//
	// fresh is true between MarkFreshSync (called by StartNewSync) and
	// EndSync. It lets PutXRecord skip the read-before-write index
	// cleanup, because ResetForNewSync excised the record keyspace and
	// this sync is therefore empty by construction. It does not affect
	// durability (recordWriteOpts).
	//
	// sealed is the post-EndSync state: record writes fail with
	// ErrEngineSealed and the compaction scheduler is paused, because
	// the only work left before save/close (checkpoint + envelope encode)
	// benefits from neither. Binding a sync again unseals and resumes.
	// Sync-run metadata writes (PutSyncRunRecord and friends) are exempt
	// via withWriteAllowSealed. Without this state the "no writes while
	// compactions are paused" invariant was convention only, and a caller
	// that kept writing after EndSync would accumulate L0 until pebble
	// stalled at L0StopWritesThreshold with nothing left to resume.
	binding atomic.Pointer[syncBinding]

	// freshGrantsEmpty / freshEntitlementsEmpty / freshResourcesEmpty are
	// one-shot bits, under writeMu. MarkFreshSync sets each; the first
	// PutXxxRecords call of the fresh sync takes it via
	// takeFreshXxxEmpty. Later calls in the same sync read-before-write
	// to clean up cross-call duplicate index entries.
	freshGrantsEmpty       bool
	freshEntitlementsEmpty bool
	freshResourcesEmpty    bool

	// computedStats holds caller-computed stats records stashed via
	// StashComputedSyncStats, keyed by sync_id. PersistSyncStats pops
	// and persists the stashed record instead of re-scanning the
	// keyspaces — used by bulk imports that already counted every
	// record they wrote.
	computedStatsMu  sync.Mutex
	computedStats    map[string]*v3.SyncStatsRecord
	syncStatsOverlay map[string]*v3.SyncStatsRecord

	// deferredGrantStats holds the grant counts BuildDeferredGrantIndexes
	// accumulated while scanning the whole grant primary keyspace, so
	// computeSyncStats can skip its own O(grants) scan at EndSync. Consumed
	// once, guarded by sync_id.
	deferredGrantStatsMu sync.Mutex
	deferredGrantStats   *deferredGrantStats

	// The deferred-index rebuild flag and the digests-present flag live
	// ON rawdb (e.db.DeferredIdxPending / e.db.GrantDigestsPresent):
	// they are write-side crash-contract state the choke point's typed
	// record ops consume directly (the deferred regime arms the marker;
	// the digest-invalidation obligation gates on presence).

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

	// grantDigestAbiStale is the read-only-open counterpart of the ABI
	// check in verifyGrantDigestABI: true when the file holds digest
	// nodes whose stamp (rawdb.GrantDigestABIStampKey) does not name
	// the current GrantDigestABIVersion — state built by different hash
	// code, e.g. a file sealed by an older SDK. A writable Open drops
	// such state instead of setting this, so on a writable engine it is
	// always false; on a read-only engine it makes the digest root
	// getters report "never built" (the same fail-safe shape as
	// grantDigestBuildPending above), and consumers recalculate.
	grantDigestAbiStale atomic.Bool

	// test holds every test-only injection seam, sequestered on one
	// field so hooks don't accumulate on the production struct. All
	// zero in production; see testSeams (test_seams.go).
	test testSeams

	// synthLayer is the open wave-scoped layer session, if any (see
	// BeginSynthesizedGrantLayer). Under writeMu. Single producer: the
	// expansion driver opens/adds/finishes sessions sequentially; the
	// session's worker merges spill chunks to SSTs but never touches the
	// DB, so the ingest stays on the producer under writeMu.
	synthLayer *synthGrantLayerSession

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

	ledger Ledger

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

	// DELIBERATE ASYMMETRY (do not "fix"): rawdb gets the UNWRAPPED FS
	// (o.vfs, nil → vfs.Default), while pebble.Open internally wraps
	// its clone of pebbleOpts.FS with disk-health middleware. So the
	// DB's own IO is health-monitored but engine-managed IO through
	// fs() (staged SSTs, checkpoint WAL cleanup) is not — exactly
	// main's split, where staging was plain os.* (pinned in PR-1
	// review: "only pebble's own IO gets the disk-health wrap").
	// Passing the wrapped FS here would put staging writes under
	// pebble's stall escalation (Logger.Fatalf on slow disks) — a
	// production behavior change, not a cleanup. Both objects sit on
	// the same underlying filesystem, so files interoperate; the
	// MemFS lifecycle test pins that.
	db, err := rawdb.Open(dir, pebbleOpts, o.vfs)
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
		resolvedFS: db.FS(),
	}
	e.binding.Store(&syncBinding{})
	e.ledger.e = e
	if s, ok := pebbleOpts.Experimental.CompactionScheduler.(*pausableCompactionScheduler); ok {
		e.compactionScheduler = s
	}
	// Poison events (CO-015) are always actionable — the scope re-fetches
	// cold next sync, and persistent overlap means the connector's
	// partitioning is wrong — but NOT rare per sync in the mis-partitioned
	// case: batch-level staging dedups only within one RecordBatch, and
	// batches re-mint per chunk, so a persistently overlapping scope (the
	// external-principal reconciliation shape included) would otherwise
	// warn once per 10k-row chunk. Dedup here, per (kind, scope) per open:
	// one warning per poisoned scope per artifact is the diagnostic
	// signal; the durable marker itself stays idempotent per batch.
	// The dedup set is a pure log cache in the connector-controlled
	// scope dimension, so it is bounded like every other scope-scale
	// allocation: past the cap, one notice and further UNSEEN scopes go
	// unlogged (already-seen scopes stay deduplicated) — thousands of
	// distinct poisoned scopes is a partitioning pathology where
	// per-scope lines stop adding signal, and the durable markers still
	// record every scope for direct inspection. Logger captured at open
	// — staging batches have no ctx at commit time. Mutex because
	// batches from different callers may commit concurrently.
	const poisonLogSetCap = 4096
	poisonLogger := ctxzap.Extract(ctx)
	var poisonLogMu sync.Mutex
	poisonLogged := make(map[[2]string]struct{})
	poisonLogCapped := false
	e.db.SetPoisonObserver(func(ev rawdb.PoisonEvent) {
		// Bound resolved at event time so the test seam (set after open,
		// before any mutation commits) can shrink it; events deliver on
		// the committing goroutine, so the read is ordered after the
		// test's write.
		bound := poisonLogSetCap
		if e.test.poisonLogSetCap > 0 {
			bound = e.test.poisonLogSetCap
		}
		seen := [2]string{ev.RowKind, ev.ScopeKey}
		poisonLogMu.Lock()
		if _, dup := poisonLogged[seen]; dup {
			poisonLogMu.Unlock()
			return
		}
		if len(poisonLogged) >= bound {
			notice := !poisonLogCapped
			poisonLogCapped = true
			poisonLogMu.Unlock()
			if notice {
				poisonLogger.Warn("pebble: further source-cache poison warnings suppressed — distinct poisoned scopes exceeded the log-dedup bound; durable poison markers still record every scope",
					zap.Int("bound", bound),
				)
			}
			return
		}
		poisonLogged[seen] = struct{}{}
		poisonLogMu.Unlock()
		poisonLogger.Warn("pebble: source-cache scope poisoned — refused as a replay source for this artifact",
			zap.String("row_kind", ev.RowKind),
			zap.String("scope_key", ev.ScopeKey),
			zap.String("cause", ev.Cause),
		)
	})
	// Under writeMu: TestWriteMuHolders reasons about call sites, and
	// ResetForNewSync runs the same method on a published engine.
	initKeyspaceState := func() error {
		return e.withWriteMu(func() error { return e.initKeyspaceStateLocked(ctx) })
	}
	err = initKeyspaceState()
	if errors.Is(err, errLegacyIDIndexLayout) {
		// The migration takes writeMu itself, so it runs between two init passes.
		if err := e.migrateIDIndexFormatToStructuredV1(ctx); err != nil {
			_ = e.Close()
			return nil, err
		}
		err = initKeyspaceState()
	}
	if err != nil {
		_ = e.Close()
		return nil, err
	}
	return e, nil
}

// Open and ResetForNewSync both run this, so a reset engine reaches the
// fresh-Open state by the same code. Every branch assigns its flag
// (TestResetForNewSyncRederivesKeyspaceFlags).
func (e *Engine) initKeyspaceStateLocked(ctx context.Context) error {
	// Enforce the single-sync key-layout contract before touching any
	// keys: reject an old multi-sync-layout file (which the current
	// encoders would silently mis-decode) and stamp a fresh writable
	// file. Runs before migrations so we never try to backfill indexes
	// on a file we can't read.
	if err := e.verifyOrStampKeyspaceVersion(ctx); err != nil {
		return err
	}
	if err := e.verifyOrStampIDIndexFormat(ctx); err != nil {
		return err
	}
	// Restore the durable deferred-index marker (see
	// rawdb.DeferredIdxPendingKey): a prior process may have deferred
	// by_principal writes and been interrupted before the EndSync
	// rebuild (rawdb owns the marker's crash contract).
	if err := e.db.RestoreDeferredIdxPending(); err != nil {
		return err
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
	_, closer, err := e.db.Get(encodeGrantDigestBuildPendingKey())
	switch {
	case err == nil:
		closer.Close()
		e.grantDigestBuildPending.Store(true)
		if !e.opts.readOnly {
			ctxzap.Extract(ctx).Warn("pebble: interrupted grant digest build detected at open; dropping all digest state — the next EndSync rebuilds it from scratch")
			if err := e.dropAllGrantDigestStateLocked(); err != nil {
				return fmt.Errorf("pebble: drop digest state left by an interrupted build: %w", err)
			}
		}
	case errors.Is(err, pebble.ErrNotFound):
		e.grantDigestBuildPending.Store(false)
	default:
		return err
	}
	// Arm the mutation-path digest invalidation iff the file actually
	// holds digest nodes (one bounded seek; rawdb owns the flag its
	// record ops gate on).
	if err := e.db.ProbeGrantDigestsPresent(); err != nil {
		return err
	}
	// Enforce the digest ABI contract: digest nodes not certified by a
	// stamp naming the CURRENT GrantDigestABIVersion were computed by
	// different hash code and must never be trusted or extended — a
	// writable open drops them wholesale (the next EndSync's existing
	// digests-absent path rebuilds everything at the current ABI); a
	// read-only open flags them so the root getters report "never
	// built". Runs after the probe so it sees post-marker-recovery
	// presence, and its own drop re-falses the flag.
	if err := e.verifyGrantDigestABI(ctx, e.opts.readOnly); err != nil {
		return err
	}
	// Arm the mutation-path source-scope index obligations iff the file
	// actually holds by_source_scope entries (bounded seeks, same
	// contract as the digest probe): scope-free stores keep the exact
	// pre-scope write cost. Runs BEFORE migrations so any migration
	// staging typed record ops sees a derived gate, not the false
	// default; a migration that backfills by_source_scope entries must
	// itself re-probe or arm (see the indexMigrations registry doc).
	if err := e.db.ProbeSourceScopeMayExist(); err != nil {
		return err
	}
	// Migrations are skipped for read-only opens (the on-disk file is
	// immutable, so we'd error out trying to backfill).
	if err := e.applyIndexMigrations(ctx); err != nil {
		return fmt.Errorf("pebble: apply index migrations: %w", err)
	}
	return nil
}

// Close shuts down the engine. After Close, write methods return
// ErrEngineClosing. Close blocks until the in-flight write, if any,
// completes; a second Close is a no-op.
func (e *Engine) Close() error {
	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	if e.db == nil {
		return nil
	}
	// A leaked synthesized-grant layer session (possible only if a panic
	// unwound past the expansion driver's Abort) has a worker merging into
	// its staging dir; drain it before tearing the DB down.
	_ = e.abortSynthesizedGrantLayerLocked(context.Background())
	// Invariant: flush before close on any write path. This drives the
	// memtable out to an SST so a Close is never the step that leaves
	// un-materialized writes behind — independent of whether EndSync or
	// CheckpointTo (which flush for their own reasons) ran first. Skipped
	// in read-only mode, where Flush is illegal and there is nothing to
	// harden. A no-op when the memtable is already empty.
	var err error
	if !e.opts.readOnly {
		if ferr := e.db.FlushMemtables(); ferr != nil {
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

// bindCurrentSync sets the engine's tracked current sync_id from a
// string KSUID. Subsequent Put*/List* calls with an empty syncID
// use this value. Clears the freshSync flag: a bound sync is treated
// as resumable, so writes keep read-before-write.
func (e *Engine) bindCurrentSync(syncID string) error {
	idBytes, err := codec.EncodeSyncID(syncID)
	if err != nil {
		return err
	}
	// Binding a sync means more writes are coming; leave the sealed state
	// and resume compactions so L0 keeps draining (see seal).
	e.transition(&syncBinding{id: idBytes}, false)
	// Rebinding admits mutations that sealed manifest row counts no longer
	// witness; strip them so an unpublished rebound store stays fail-closed
	// for replay (CO-014). Reseal recounts. Must follow unseal — the clear
	// uses the normal write path. Read-only engines skip it: they admit no
	// mutations, so sealed counts remain valid witnesses (and the write
	// would be illegal anyway).
	if e.opts.readOnly {
		return nil
	}
	if err := e.clearSourceCacheRowCounts(); err != nil {
		return err
	}
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

// syncBinding is one snapshot of the sync lifecycle state; see
// Engine.binding.
type syncBinding struct {
	id     []byte
	fresh  bool
	sealed bool
}

// transitionLocked publishes next as the lifecycle snapshot and moves the
// compaction scheduler to match next.sealed. Requires writeMu, which is
// what makes a transition exclusive with writers: a writer blocked on
// writeMu observes the new state once it gets the lock.
func (e *Engine) transitionLocked(next *syncBinding) {
	e.binding.Store(next)
	if next.sealed {
		e.pauseCompactions()
	} else {
		e.resumeCompactions()
	}
}

func (e *Engine) transition(next *syncBinding, freshBits bool) {
	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	e.transitionLocked(next)
	e.freshGrantsEmpty = freshBits
	e.freshEntitlementsEmpty = freshBits
	e.freshResourcesEmpty = freshBits
}

// setSealed replaces only the sealed bit of the current snapshot.
func (e *Engine) setSealed(sealed bool) {
	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	cur := e.binding.Load()
	e.transitionLocked(&syncBinding{id: cur.id, fresh: cur.fresh, sealed: sealed})
}

// seal moves the engine into the post-EndSync state: record writes fail
// with ErrEngineSealed and automatic compactions stop. Called by
// Adapter.EndSync before finalize; undone by binding a sync
// (SetCurrentSync / MarkFreshSync) or by unseal when finalize fails. Waits
// for an in-flight write, so no write straddles the seal.
func (e *Engine) seal() { e.setSealed(true) }

// unseal leaves the sealed state and resumes automatic compactions.
func (e *Engine) unseal() { e.setSealed(false) }

// IsSealed reports whether the engine is in the post-EndSync sealed state.
func (e *Engine) IsSealed() bool {
	return e.binding.Load().sealed
}

// MarkFreshSync sets currentSync AND flags the sync as freshly
// started (no prior records under this sync_id), so Put*Records skip
// the read-before-write index cleanup. FinishSync clears it.
func (e *Engine) MarkFreshSync(syncID string) error {
	idBytes, err := codec.EncodeSyncID(syncID)
	if err != nil {
		return err
	}
	// A fresh sync writes heavily; leave the sealed state and resume
	// compactions so L0 keeps draining (see seal).
	e.transition(&syncBinding{id: idBytes, fresh: true}, true)
	return nil
}

// clearCurrentSync detaches the engine from its current sync and disables
// fresh-sync write shortcuts. After this, operations that resolve an empty
// sync_id fail with ErrNoCurrentSync until StartNewSync, ResumeSync, or
// SetCurrentSync binds a sync again. The sealed bit is kept.
func (e *Engine) clearCurrentSync() {
	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	e.clearCurrentSyncLocked()
}

func (e *Engine) clearCurrentSyncLocked() {
	e.transitionLocked(&syncBinding{sealed: e.binding.Load().sealed})
	e.freshGrantsEmpty = false
	e.freshEntitlementsEmpty = false
	e.freshResourcesEmpty = false
}

// IsFreshSync reports whether the engine is in the fresh-sync write
// path (set by MarkFreshSync).
func (e *Engine) IsFreshSync() bool {
	return e.binding.Load().fresh
}

// GrantDigestIndexEnabled reports whether the seal-time deferred pass
// builds the by_entitlement_principal_hash index and grant digests.
// See WithGrantDigestIndex.
func (e *Engine) GrantDigestIndexEnabled() bool { return e.opts.grantDigestIndex }

// GrantDigestsPresent reports whether this engine currently holds ANY
// grant-digest state (nodes + the by_entitlement_principal_hash index
// beneath them) — the same Open-probed flag the record write paths
// gate their per-write invalidation obligation on. Exported for
// callers outside this package that need to tell "no digest state at
// all" apart from "digest state present but stale/invalidated" (e.g.
// the compactor's fold, deciding whether a byte-copied base needs a
// one-time digest build).
func (e *Engine) GrantDigestsPresent() bool { return e.db.GrantDigestsPresent() }

// grantDigestStateUntrusted reports whether NO stored grant-digest
// state — digest nodes, the whole-file root, or the
// by_entitlement_principal_hash index beneath them — may be trusted
// right now. Both flags it OR's together mean this by construction:
// grantDigestBuildPending means an interrupted build may have left
// digest nodes durable while the hash index under them never finished
// ingesting; grantDigestAbiStale means the nodes and hash-index
// content hashes were computed by a different hash ABI (a read-only
// open of a file whose stamp doesn't name the current
// GrantDigestABIVersion). Either way, every getter and on-demand fold
// over that state must report "not built" rather than trust or
// recompute from it — see getPartitionDigestRoot,
// GetGrantDigestGlobalRoot, and ComputeEntitlementBucketDigest.
func (e *Engine) grantDigestStateUntrusted() bool {
	return e.grantDigestBuildPending.Load() || e.grantDigestAbiStale.Load()
}

// takeFreshGrantsEmpty / takeFreshResourcesEmpty return true
// exactly once per fresh sync, for the first PutXxxRecords call
// of that type after
// MarkFreshSync. Subsequent calls (and any call after EndSync) see
// false. PutXxxRecords uses these to safely skip the
// read-before-write Get on the first bulk write of each type:
// the keyspace under the freshly-minted sync_id is provably empty
// by construction. Callers hold writeMu (they run inside withWrite).
func (e *Engine) takeFreshGrantsEmpty() bool {
	was := e.freshGrantsEmpty
	e.freshGrantsEmpty = false
	return was
}

func (e *Engine) takeFreshResourcesEmpty() bool {
	was := e.freshResourcesEmpty
	e.freshResourcesEmpty = false
	return was
}

func (e *Engine) takeFreshEntitlementsEmpty() bool {
	was := e.freshEntitlementsEmpty
	e.freshEntitlementsEmpty = false
	return was
}

// FinishSync flushes the memtable, fsyncs the WAL, and clears the
// current sync and the fresh-sync flag. Last step of Adapter.EndSync,
// fresh or bound.
//
// Uses withWriteAllowSealed (not a bare writeMu) so the flush goes
// through checkWritableAllowSealed: Close sets e.db to nil under writeMu,
// and a bare-mutex FinishSync racing Close would flush a nil db.
func (e *Engine) FinishSync(ctx context.Context) error {
	// AllowSealed: this is the last step of EndSync's sealed finalize
	// window (see Adapter.EndSync).
	return e.withWriteAllowSealed(func() error {
		if err := e.db.FlushMemtables(); err != nil {
			return fmt.Errorf("FinishSync: flush: %w", err)
		}
		// SIDE EFFECT ONLY. WALSyncPoint writes no key; it commits an
		// empty pebble.Sync record so that the WAL gets fsynced, which
		// puts every earlier NoSync commit (recordWriteOpts) on disk.
		if err := e.db.WALSyncPoint(); err != nil {
			return fmt.Errorf("FinishSync: fsync WAL: %w", err)
		}
		e.clearCurrentSyncLocked()
		return nil
	})
}

// currentSyncBytes returns the engine's tracked sync_id (raw bytes)
// or nil if none is set. The sync_id is never part of a key; this is
// used only to validate that a caller's sync_id matches the engine's
// one bound sync (see StartBulkSyncImport).
func (e *Engine) currentSyncBytes() []byte {
	id := e.binding.Load().id
	out := make([]byte, len(id))
	copy(out, id)
	return out
}

// CurrentSyncID returns the bound sync's id string, or "" when no sync
// is bound. THE single source of truth for "which sync is open" — the
// old Adapter-level syncRunState cache that shadowed it was deleted
// (PR 2.6): lifecycle readers decode this binding, and everything else
// about the open sync (step token, type, parent) is read from the
// durable SyncRunRecord on demand, exactly like the SQLite engine's
// row-backed reads.
func (e *Engine) CurrentSyncID() string {
	return codec.DecodeSyncID(e.binding.Load().id)
}

// requireCurrentSync returns ErrNoCurrentSync unless a sync is bound
// (StartNewSync/SetCurrentSync, cleared by EndSync). Record writes
// gate on this so data never lands without a sync-run record — the
// sync_id is NOT encoded in keys, but a write still has to happen
// inside an open sync. Reads do not gate: a finished sync's data
// persists and stays readable after EndSync clears the binding.
func (e *Engine) requireCurrentSync() error {
	if len(e.binding.Load().id) == 0 {
		return ErrNoCurrentSync
	}
	return nil
}

// checkWritableLocked returns ErrEngineClosing after Close, and
// ErrEngineSealed after a successful EndSync until a sync is bound again.
// Requires writeMu: Close nils db and seal flips the snapshot under it, so
// a check made under the lock holds for the rest of the critical section.
func (e *Engine) checkWritableLocked() error {
	if err := e.checkWritableAllowSealedLocked(); err != nil {
		return err
	}
	if e.binding.Load().sealed {
		return ErrEngineSealed
	}
	return nil
}

// checkWritableAllowSealedLocked is checkWritableLocked without the sealed
// check, for the few write paths that legitimately run on a finished sync
// (sync-run metadata updates and the pre-StartNewSync wipe).
func (e *Engine) checkWritableAllowSealedLocked() error {
	if e.db == nil {
		return ErrEngineClosing
	}
	if e.opts.readOnly {
		return errors.New("pebble engine: opened read-only")
	}
	return nil
}

// withWrite runs fn under writeMu if the engine is open and a sync is
// bound (not sealed). A writer that blocked on writeMu behind EndSync's
// seal sees the sealed snapshot once it gets the lock and is refused: a
// grant landing after the deferred rebuild would be permanently missing
// from by_principal.
func (e *Engine) withWrite(fn func() error) error {
	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	if err := e.checkWritableLocked(); err != nil {
		return err
	}
	return fn()
}

// withWriteAllowSealed is withWrite without the sealed check. Reserved for
// writes that are part of the sealed lifecycle itself: sync-run metadata
// stamps on a finished sync (ended_at overrides, supports_diff),
// compactor source-cache invalidation, and ResetForNewSync's wipe on the way
// into a new sync. Record-data writes must use withWrite.
func (e *Engine) withWriteAllowSealed(fn func() error) error {
	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	if err := e.checkWritableAllowSealedLocked(); err != nil {
		return err
	}
	return fn()
}

// Open-time only: the init runs on read-only engines too, which
// withWriteAllowSealed refuses.
func (e *Engine) withWriteMu(fn func() error) error {
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
//
// Walks through the engine FS, not the host FS (review follow-up): a
// WithVFS engine's DB dir lives on the injected filesystem, where a
// host filepath.WalkDir either errors or measures an unrelated
// directory. On the default FS this is the same walk it always was.
func (e *Engine) CurrentDBSizeBytes() (int64, error) {
	if e.dbDir == "" {
		return 0, errors.New("pebble engine: db dir is empty")
	}
	pfs := e.fs()
	// No-follow stat, matching main's filepath.WalkDir semantics: a
	// symlink inside the DB dir must be skipped, not traversed (a link
	// to a foreign directory would count files outside the DB; a
	// self-link would loop). vfs.FS has no Lstat, so the default FS
	// uses os.Lstat directly (allowlisted); MemFS cannot represent
	// symlinks, so Stat is equivalent there (review finding, 2.5
	// round).
	stat := func(path string) (os.FileInfo, error) {
		if pfs == vfs.Default {
			return os.Lstat(path)
		}
		return pfs.Stat(path)
	}
	var walk func(dir string) (int64, error)
	walk = func(dir string) (int64, error) {
		names, err := pfs.List(dir)
		if err != nil {
			return 0, err
		}
		var total int64
		for _, name := range names {
			path := pfs.PathJoin(dir, name)
			info, err := stat(path)
			if err != nil {
				if errors.Is(err, fs.ErrNotExist) {
					// Compaction can remove files mid-walk; skip.
					continue
				}
				return 0, fmt.Errorf("stat %s: %w", path, err)
			}
			switch {
			case info.IsDir():
				sub, err := walk(path)
				if err != nil {
					return 0, err
				}
				total += sub
			case info.Mode().IsRegular():
				total += info.Size()
			}
		}
		return total, nil
	}
	total, err := walk(e.dbDir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return 0, fmt.Errorf("pebble engine: db dir missing: %w", err)
		}
		return 0, err
	}
	return total, nil
}

// InvalidateBareIDLookups invalidates the lazily built bare-id lookup
// state (see lookup.go). Engine write paths call this internally; it
// is exported for callers that mutate the entitlement keyspace through
// the merge surface (merge_surface.go) directly.
func (e *Engine) InvalidateBareIDLookups() { e.noteEntitlementKeyspaceWrite() }

// MigratedOnOpen reports whether this Open ran the in-place id-index
// migration (see migratedOnOpen).
func (e *Engine) MigratedOnOpen() bool { return e.migratedOnOpen }

// fs returns the filesystem the engine performs its own IO through:
// the WithVFS override when set, vfs.Default otherwise. This must be
// the same FS the pebble.DB reads from — the staged SSTs the engine
// hands to Ingest/IngestAndExcise are resolved through the DB's FS.
//
// Snapshotted from the rawdb handle at Open (resolvedFS) rather than
// read through e.db on every call: Close nils e.db, but cleanup paths
// legitimately outlive it — a retained BulkSyncImport's Abort/Finish
// tears down its staging dirs through fs() after the engine closed
// (review finding, external parity round: the e.db.FS() form
// nil-panicked there, where main's opts-based fs() was safe). The
// snapshot is a copy of rawdb's one resolution, not a second decision.
func (e *Engine) fs() vfs.FS {
	return e.resolvedFS
}

// prepareStagingDir mints a unique staging directory via os.MkdirTemp
// and mirrors it onto the engine FS. SST files staged for ingest are
// created through e.fs() (the pebble.DB resolves ingest paths through
// that FS), while spill-chunk scratch is plain OS IO — so the directory
// must exist on both. On the default FS the MkdirAll is a no-op.
//
// Portability note: mirroring a host temp path onto a MemFS assumes
// "/" separators (MemFS only splits on "/"). WithVFS with a MemFS is a
// test-only configuration and the tests are unix-only; a Windows port
// would need to stage under fs.PathJoin'd relative paths instead.
func (e *Engine) prepareStagingDir(tmpDir, pattern string) (string, error) {
	dir, err := os.MkdirTemp(tmpDir, pattern)
	if err != nil {
		return "", err
	}
	if err := e.fs().MkdirAll(dir, 0o755); err != nil {
		_ = os.RemoveAll(dir)
		return "", err
	}
	return dir, nil
}

// removeStagingDir removes a staging directory from both filesystems
// it exists on (see prepareStagingDir). Cleanup-path best effort.
func (e *Engine) removeStagingDir(dir string) {
	_ = e.fs().RemoveAll(dir)
	_ = os.RemoveAll(dir)
}

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
// CheckpointTo holds writeMu for the whole Flush→Checkpoint→truncate
// window. That prevents a write from committing between the Flush and
// Checkpoint — such a write would otherwise exist only in the WAL, which
// truncateCheckpointWALs discards.
func (e *Engine) CheckpointTo(ctx context.Context, destDir string) error {
	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	if e.db == nil {
		return ErrEngineClosing
	}

	if e.opts.readOnly {
		return copyReadOnlyDBDir(e.fs(), e.dbDir, destDir)
	}

	if err := e.db.FlushMemtables(); err != nil {
		return fmt.Errorf("checkpoint flush: %w", err)
	}
	if err := e.db.Checkpoint(destDir); err != nil {
		return fmt.Errorf("checkpoint db %s: %w", destDir, err)
	}
	if err := truncateCheckpointWALs(e.fs(), destDir); err != nil {
		// The checkpoint itself succeeded, so destDir exists; a caller
		// that retries CheckpointTo with the same path would otherwise
		// hard-fail pebble Checkpoint's dest-must-not-exist contract.
		// Removal keeps the failure retryable (the clone-sync caller
		// uses a fresh temp dir either way). If the removal ALSO fails
		// — plausible, since whatever broke the truncate may still be
		// broken — same-path retryability is NOT restored, so the
		// cleanup failure rides along in the returned error instead of
		// being swallowed.
		if rmErr := e.fs().RemoveAll(destDir); rmErr != nil {
			return errors.Join(
				fmt.Errorf("checkpoint truncate WALs: %w", err),
				fmt.Errorf("cleanup of %s also failed (retry needs a fresh dest or manual removal): %w", destDir, rmErr),
			)
		}
		return fmt.Errorf("checkpoint truncate WALs: %w", err)
	}

	return nil
}

// copyReadOnlyDBDir clones a read-only Pebble directory tree into
// destDir. destDir must not exist yet, matching db.Checkpoint's
// contract. pfs is the engine FS (Engine.fs()) — the source tree was
// written through it and the clone must land where the destination
// open will look.
func copyReadOnlyDBDir(pfs vfs.FS, srcDir, destDir string) error {
	if _, err := pfs.Stat(destDir); err == nil {
		return &os.PathError{Op: "checkpoint", Path: destDir, Err: fs.ErrExist}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
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
//
// pfs is the engine FS (Engine.fs()) — db.Checkpoint wrote the
// checkpoint through it, so the WALs to truncate live there, not
// necessarily on the host filesystem. vfs has no Truncate, and
// vfs.Default.Create is REMOVE-then-recreate (not O_TRUNC — see its
// hard-link rationale), so "Create the WAL path directly" could
// unlink a WAL and then fail the recreate, which is exactly the
// delete-changes-the-discovered-file-set hazard the truncate-not-
// delete policy above exists to avoid. Instead the zero-byte
// replacement is built at a side name and Rename'd over the WAL:
// the original survives every failure before the rename, and the
// rename replaces the path atomically on both the default FS and
// MemFS.
func truncateCheckpointWALs(pfs vfs.FS, destDir string) error {
	names, err := pfs.List(destDir)
	if err != nil {
		return err
	}
	for _, name := range names {
		if filepath.Ext(name) != ".log" {
			continue
		}
		path := pfs.PathJoin(destDir, name)
		if info, err := pfs.Stat(path); err != nil || info.IsDir() {
			if err != nil {
				return err
			}
			continue
		}
		tmp := path + ".trunc"
		f, err := pfs.Create(tmp, vfs.WriteCategoryUnspecified)
		if err != nil {
			return err
		}
		if err := f.Close(); err != nil {
			_ = pfs.Remove(tmp)
			return err
		}
		if err := pfs.Rename(tmp, path); err != nil {
			_ = pfs.Remove(tmp)
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
