package pebble

// The write choke point's EXPLICIT EXEMPTION SURFACE, promoted onto
// Engine (this replaced the Engine.DB() handle — one less wrapper to
// hold, and every call nil-checks the engine's lifecycle instead of
// trusting a retained handle). Exported for the synccompactor/pebble
// package, whose merge/fold/overlay machinery reads source files and
// writes the record keyspaces outside the engine's writeMu barrier —
// fenced by call ordering: the merge completes before the store's
// save/CheckpointTo runs (see the checkpointMu inventory).
//
// The surface carries only what the compactor needs — reads, LSM
// stats, the bulk range/ingest ops, and the fold-exempt batch. Typed
// record staging (NewRecordBatch) and the session/meta/digest write
// families are NOT reachable from outside the engine package; the raw
// *pebble.DB stays unreachable (UnsafeForTesting is runtime-gated to
// tests). The choke-point meta-tests fence production use of
// NewFoldBatch and the bulk write ops to the fold compactor; this
// file's delegating definitions are the fence's sanctioned exception.
//
// Callers that write the entitlement keyspace through this surface
// (ingests, excises) must call InvalidateBareIDLookups afterwards.

import (
	"context"
	"io"

	"github.com/cockroachdb/pebble/v2"
)

// Get reads a key. Same contract as pebble.DB.Get, including
// pebble.ErrNotFound and the caller-owned closer.
func (e *Engine) Get(key []byte) ([]byte, io.Closer, error) {
	if e.db == nil {
		return nil, nil, ErrEngineClosing
	}
	return e.db.Get(key)
}

// NewIter opens an iterator. Same contract as pebble.DB.NewIter.
func (e *Engine) NewIter(o *pebble.IterOptions) (*pebble.Iterator, error) {
	if e.db == nil {
		return nil, ErrEngineClosing
	}
	return e.db.NewIter(o)
}

// Metrics returns pebble's metrics snapshot, or nil after Close.
func (e *Engine) Metrics() *pebble.Metrics {
	if e.db == nil {
		return nil
	}
	return e.db.Metrics()
}

// EstimateDiskUsage estimates on-disk size of the key range.
func (e *Engine) EstimateDiskUsage(start, end []byte) (uint64, error) {
	if e.db == nil {
		return 0, ErrEngineClosing
	}
	return e.db.EstimateDiskUsage(start, end)
}

// DropKeyRange stages and commits a range tombstone over [start, end).
func (e *Engine) DropKeyRange(start, end []byte, o *pebble.WriteOptions) error {
	if e.db == nil {
		return ErrEngineClosing
	}
	return e.db.DropKeyRange(start, end, o)
}

// IngestSSTs ingests externally built SSTs (created on the engine FS).
func (e *Engine) IngestSSTs(ctx context.Context, paths []string) error {
	if e.db == nil {
		return ErrEngineClosing
	}
	return e.db.IngestSSTs(ctx, paths)
}

// ReplaceRangeWithSSTs atomically replaces span with the given SSTs
// (pebble IngestAndExcise).
func (e *Engine) ReplaceRangeWithSSTs(ctx context.Context, paths []string, span pebble.KeyRange) error {
	if e.db == nil {
		return ErrEngineClosing
	}
	return e.db.ReplaceRangeWithSSTs(ctx, paths, span)
}

// NewFoldBatch mints the compactor's generic staged batch (the one
// sanctioned raw record-write conduit; see the fold-family doc in
// rawdb and the meta-test fence). Panics with an explicit message
// after Close: unlike the error-returning ops above, callers stage
// into the batch without a per-call error path, so a nil return would
// only defer the crash to an anonymous nil deref at the first Set —
// and a closed engine here means the compactor's call-ordering fence
// (merge completes before save/Close) was violated, which is a
// programming error, not a runtime condition to handle.
func (e *Engine) NewFoldBatch() *FoldBatch {
	if e.db == nil {
		panic("pebble engine: NewFoldBatch on a closed engine — the fold must complete before the engine closes")
	}
	return e.db.NewFoldBatch()
}

// UnsafeForTesting returns the raw *pebble.DB — the single raw escape
// hatch through the choke point, for test fixtures constructing states
// the production API cannot express. Panics outside `go test`
// (testing.Testing(), enforced inside rawdb) and, explicitly, after
// Close.
func (e *Engine) UnsafeForTesting() *pebble.DB {
	if e.db == nil {
		panic("pebble engine: UnsafeForTesting on a closed engine")
	}
	return e.db.UnsafeForTesting()
}
