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
	"google.golang.org/protobuf/proto"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/codec"
)

// Engine is the v3 Pebble-backed storage engine. Methods are
// goroutine-safe modulo the lifecycle rules in this file:
//
//   - Open the engine once with Open(...).
//   - Concurrent Reader/Writer calls are safe.
//   - Quiesce() flips the engine into a terminal state; subsequent
//     Writer calls return ErrEngineQuiesced.
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
	// freshGrantsEmpty / freshResourcesEmpty / freshEntitlementsEmpty
	// are one-shot bits guarded by currentSyncMu. MarkFreshSync sets
	// each to true; the first PutXxxRecords call of the fresh sync
	// reads the value via takeFreshXxxEmpty() which returns it and
	// clears it. Concrete use: gate the skip-Get fast path on "first
	// call only" — subsequent calls in the same fresh sync must
	// still read-before-write to clean up cross-call duplicate index
	// entries.
	freshGrantsEmpty       bool
	freshResourcesEmpty    bool
	freshEntitlementsEmpty bool

	// writeWG tracks in-flight writes for the strict quiesce
	// protocol. Incremented at the start of every Writer method,
	// decremented in defer. Quiesce flips closing=true then waits
	// for writeWG to drain.
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
// ErrEngineClosing. If the engine was Quiesce'd first, the
// in-flight writes have already drained; otherwise Close blocks
// until they complete.
func (e *Engine) Close() error {
	e.closeMu.Lock()
	defer e.closeMu.Unlock()
	if e.db == nil {
		return nil
	}
	e.closing.Store(true)
	e.writeWG.Wait()
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

// Quiesce is the strict write-barrier from RFC v4 §3.7. After Quiesce
// returns, all subsequent Writer method calls return ErrEngineQuiesced.
// In-flight writes drain via the engine's WaitGroup. Concurrent
// callers serialize on closeMu; the second caller observes a
// fully-quiesced engine.
//
// Idempotent: calling Quiesce twice is a no-op the second time.
//
// There is no Unquiesce. Callers that want to keep writing open a
// new engine instance via Open.
func (e *Engine) Quiesce(ctx context.Context) error {
	e.closeMu.Lock()
	defer e.closeMu.Unlock()
	if e.closing.Load() {
		return nil
	}
	e.closing.Store(true)
	e.writeWG.Wait()
	// Force memtable to L0 so a subsequent Save sees a clean state.
	if err := e.db.Flush(); err != nil {
		return fmt.Errorf("flush during quiesce: %w", err)
	}
	return nil
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
	e.freshEntitlementsEmpty = false
	e.currentSyncMu.Unlock()
	return nil
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
	e.freshEntitlementsEmpty = true
	e.currentSyncMu.Unlock()
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
	e.freshEntitlementsEmpty = false
	e.currentSyncMu.Unlock()
}

// IsFreshSync reports whether the engine is in the fresh-sync write
// path (set by MarkFreshSync).
func (e *Engine) IsFreshSync() bool {
	e.currentSyncMu.RLock()
	defer e.currentSyncMu.RUnlock()
	return e.freshSync
}

// takeFreshGrantsEmpty / takeFreshResourcesEmpty /
// takeFreshEntitlementsEmpty return true exactly once per fresh
// sync, for the first PutXxxRecords call of that type after
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

func (e *Engine) takeFreshEntitlementsEmpty() bool {
	e.currentSyncMu.Lock()
	defer e.currentSyncMu.Unlock()
	if !e.freshEntitlementsEmpty {
		return false
	}
	e.freshEntitlementsEmpty = false
	return true
}

// EndFreshSync clears the fresh-sync flag and flushes the memtable
// + fsyncs the WAL so the data written during the sync is on disk
// before the caller returns. Called by Adapter.EndSync.
func (e *Engine) EndFreshSync(ctx context.Context) error {
	e.writeMu.Lock()
	defer e.writeMu.Unlock()

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
}

// currentSyncBytes returns the engine's tracked sync_id (raw bytes)
// or nil if none is set.
func (e *Engine) currentSyncBytes() []byte {
	e.currentSyncMu.RLock()
	defer e.currentSyncMu.RUnlock()
	out := make([]byte, len(e.currentSync))
	copy(out, e.currentSync)
	return out
}

// resolveSyncBytes returns the raw sync_id bytes from a string syncID,
// falling back to the engine's currently-set sync if the string is
// empty. Returns ErrNoCurrentSync if no sync is set and the string is
// empty.
func (e *Engine) resolveSyncBytes(syncID string) ([]byte, error) {
	if syncID == "" {
		b := e.currentSyncBytes()
		if len(b) == 0 {
			return nil, ErrNoCurrentSync
		}
		return b, nil
	}
	return codec.EncodeSyncID(syncID)
}

// checkWritable returns ErrEngineClosing if the engine has been
// Quiesce'd or closed. Called at the start of every Writer method.
func (e *Engine) checkWritable() error {
	if e.closing.Load() {
		return ErrEngineQuiesced
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
// closing check. The closure runs only if the engine is open and
// not quiesced.
func (e *Engine) withWrite(fn func() error) error {
	if err := e.checkWritable(); err != nil {
		return err
	}
	e.writeWG.Add(1)
	defer e.writeWG.Done()
	// Re-check after Add because Quiesce could have flipped between
	// our first check and our Add.
	if e.closing.Load() {
		return ErrEngineQuiesced
	}
	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	return fn()
}

// withWriteBarrier serializes a short critical section against all
// engine writes without permanently quiescing the engine. It is used by
// snapshotting code that must observe "all writes before the barrier,
// no writes during the barrier, writes may resume after".
func (e *Engine) withWriteBarrier(fn func() error) error {
	if err := e.checkWritable(); err != nil {
		return err
	}
	e.writeWG.Add(1)
	defer e.writeWG.Done()
	if e.closing.Load() {
		return ErrEngineQuiesced
	}
	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	if err := e.checkWritable(); err != nil {
		return err
	}
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
// nil after Close.
func (e *Engine) DB() *pebble.DB { return e.db }

// CheckpointTo writes a self-contained Pebble directory snapshot to
// destDir. destDir must not exist yet. Pebble creates it and
// hard-links SSTs where possible.
//
// The source engine stays writable after CheckpointTo returns; writes
// are only blocked while the checkpoint is cut. This is the building
// block dotc1z's higher-level Save wraps with the v3 envelope format.
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
func (e *Engine) CheckpointTo(ctx context.Context, destDir string) error {
	return e.withWriteBarrier(func() error {
		if err := e.db.Flush(); err != nil {
			return fmt.Errorf("checkpoint flush: %w", err)
		}
		if err := e.db.Checkpoint(destDir); err != nil {
			return fmt.Errorf("checkpoint: %w", err)
		}
		if err := truncateCheckpointWALs(destDir); err != nil {
			return fmt.Errorf("checkpoint truncate WALs: %w", err)
		}
		return nil
	})
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
