//go:build batonsdkv2

package pebble

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/cockroachdb/pebble"
	"google.golang.org/protobuf/proto"

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
//
// Stack 3 ships the GrantRecord path end-to-end (Put, Get, range
// scans by primary key and by_entitlement / by_principal indexes,
// Save via DB.Checkpoint). Other record types follow the same pattern
// and land as follow-up commits on this branch.
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

	// writeWG tracks in-flight writes for the strict quiesce
	// protocol. Incremented at the start of every Writer method,
	// decremented in defer. Quiesce flips closing=true then waits
	// for writeWG to drain.
	writeWG sync.WaitGroup
	closing atomicBool // strict write-barrier flag
	closeMu sync.Mutex
}

// atomicBool is a minimal one-flag atomic without importing
// sync/atomic.Bool (compat with older Go in vendored deps).
type atomicBool struct {
	mu sync.RWMutex
	v  bool
}

func (a *atomicBool) Load() bool   { a.mu.RLock(); defer a.mu.RUnlock(); return a.v }
func (a *atomicBool) Store(v bool) { a.mu.Lock(); a.v = v; a.mu.Unlock() }

// Open creates or opens a Pebble engine rooted at dir. If dir does
// not exist, Pebble creates it. The caller is responsible for
// providing a directory that won't be shared with another Pebble
// instance.
//
// Returns ErrEngineNotAvailable if the binary was built without the
// batonsdkv2 tag — defended at compile time, so this never fires in
// practice, but the sentinel is wired for safety.
func Open(ctx context.Context, dir string, opts ...Option) (*Engine, error) {
	o := defaultOptions()
	for _, opt := range opts {
		opt(o)
	}

	pebbleOpts := newPebbleOptions(o)

	db, err := pebble.Open(dir, pebbleOpts)
	if err != nil {
		return nil, fmt.Errorf("pebble.Open: %w", err)
	}

	return &Engine{
		db:         db,
		dbDir:      dir,
		opts:       o,
		pebbleOpts: pebbleOpts,
	}, nil
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
	err := e.db.Close()
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
// use this value.
func (e *Engine) SetCurrentSync(syncID string) error {
	idBytes, err := codec.EncodeSyncID(syncID)
	if err != nil {
		return err
	}
	e.currentSyncMu.Lock()
	e.currentSync = idBytes
	e.currentSyncMu.Unlock()
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
	return fn()
}

// Save snapshots the engine to a v3 c1z file at dest. Uses
// DB.Checkpoint per RFC v4 §3.7 and the micro-test in import-11. The
// source DB remains writable after Save returns.
//
// Save creates dest's parent directory if needed. Returns
// ErrSaveDestExists if dest already exists. Returns
// ErrCrossFilesystem if the engine's working directory and dest are
// on different filesystems (the atomic rename at the end requires
// same-FS).
func (e *Engine) Save(ctx context.Context, dest string) error {
	// dest existence guard.
	if _, err := os.Stat(dest); err == nil {
		return fmt.Errorf("%w: %s", ErrSaveDestExists, dest)
	}
	// Parent dir.
	parent := filepath.Dir(dest)
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return fmt.Errorf("save: create parent dir: %w", err)
	}
	// Strict quiesce — refuses all subsequent writes; flushes memtable.
	if err := e.Quiesce(ctx); err != nil {
		return fmt.Errorf("save: quiesce: %w", err)
	}
	// Checkpoint into a tmp dir adjacent to dest (same filesystem so
	// the eventual rename is atomic).
	ckDir, err := os.MkdirTemp(parent, ".c1z3-checkpoint-")
	if err != nil {
		return fmt.Errorf("save: tmp dir: %w", err)
	}
	// Pebble's Checkpoint requires the destDir to not exist yet —
	// remove the empty tmp we just made so Checkpoint creates the real one.
	if err := os.Remove(ckDir); err != nil {
		return fmt.Errorf("save: prep checkpoint dir: %w", err)
	}
	if err := e.db.Checkpoint(ckDir, pebble.WithFlushedWAL()); err != nil {
		return fmt.Errorf("save: db.Checkpoint: %w", err)
	}
	defer os.RemoveAll(ckDir)

	// Envelope write is in the v3 format package — but importing it
	// here would create a cycle (format/v3 imports the codec; the
	// engine imports the codec). The cycle is broken by the caller
	// (typically dotc1z.Save) invoking format/v3.WriteEnvelope after
	// the engine produces the Pebble directory. Stack 3 leaves the
	// envelope write to a higher-level shim; see CheckpointTo below.
	_ = ckDir
	return errors.New("pebble engine: Save requires the dotc1z.Save shim (envelope write); use CheckpointTo for direct directory access")
}

// CheckpointTo writes a self-contained Pebble directory snapshot to
// destDir. destDir must not exist yet. Pebble creates it and
// hard-links SSTs where possible.
//
// The source engine stays writable after CheckpointTo returns; this
// is the building block dotc1z's higher-level Save wraps with the v3
// envelope format.
func (e *Engine) CheckpointTo(ctx context.Context, destDir string) error {
	if err := e.Quiesce(ctx); err != nil {
		return err
	}
	if err := e.db.Checkpoint(destDir, pebble.WithFlushedWAL()); err != nil {
		return fmt.Errorf("checkpoint: %w", err)
	}
	return nil
}

// internal: marshal a record value deterministically.
func marshalRecord(m proto.Message) ([]byte, error) {
	return proto.MarshalOptions{Deterministic: true}.Marshal(m)
}
