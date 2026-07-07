package pebble

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/codec"
)

// deferredIndexSpillChunkBytes is the spill-chunk arena size for the deferred
// by_principal index build. The shared bulkSpillKeyChunkBytes (8MiB) is sized
// for the bulk import, where lanes × index-families sorters are alive at once
// and small arenas bound aggregate memory. The deferred build is the opposite
// shape — one producer, one sorter, nothing else running — and with 8MiB
// chunks a whale (57M+ index keys ≈ 6.4GB) produced an 801-way final merge:
// ~10 heap comparisons per entry plus 801 open chunk files with 1MiB readers
// (~800MB of buffers). 128MiB chunks cut that to ~50 runs. Peak memory is the
// active arena plus up to `sorters` chunks being sorted in background
// (~640MB), in the same ballpark as what the wide merge's read buffers used.
const deferredIndexSpillChunkBytes = 128 << 20

// appendGrantByPrincipalKeyFromPrimary builds the by_principal index key
// directly from a primary grant key by permuting its tuple segments, into
// dst. The primary tail is ent_rt|ent_rid|ent_kind|ent_name|p_rt|p_id and the
// index tail is p_rt|p_id|ent_rt|ent_rid|ent_kind|ent_name — the segments are
// already escaped, and the tuple escaping is canonical, so splicing the raw
// bytes is byte-identical to decode + re-encode
// (encodeGrantByPrincipalIdentityIndexKey ∘ decodeGrantIdentityKey) while
// skipping six string allocations and a fresh key buffer per row. That
// decode/re-encode pair was ~7GB of allocations per whale deferred build.
// Returns ok=false for keys that do not have exactly six segments.
//
// Pinned against the decode+re-encode path by
// TestAppendGrantByPrincipalKeyFromPrimary.
func appendGrantByPrincipalKeyFromPrimary(dst, primaryKey []byte) ([]byte, bool) {
	const prefixLen = 3 // versionV3 | typeGrant | separator
	if len(primaryKey) < prefixLen || primaryKey[0] != versionV3 || primaryKey[1] != typeGrant || primaryKey[2] != 0 {
		return dst, false
	}
	tail := primaryKey[prefixLen:]
	// Split into exactly six escaped segments on bare separator bytes (the
	// escape rules guarantee segments contain no bare 0x00).
	var segs [6][]byte
	rest := tail
	for i := 0; i < 5; i++ {
		sep := bytes.IndexByte(rest, 0)
		if sep < 0 {
			return dst, false
		}
		segs[i] = rest[:sep]
		rest = rest[sep+1:]
	}
	if bytes.IndexByte(rest, 0) >= 0 {
		return dst, false
	}
	segs[5] = rest

	dst = append(dst, versionV3, typeIndex, idxGrantByPrincipal, 0)
	for i, idx := range [6]int{4, 5, 0, 1, 2, 3} { // p_rt, p_id, ent_rt, ent_rid, ent_kind, ent_name
		if i > 0 {
			dst = append(dst, 0)
		}
		dst = append(dst, segs[idx]...)
	}
	return dst, true
}

// deferredGrantStats carries the grant-keyspace stats accumulated during the
// BuildDeferredGrantIndexes scan: the same numbers computeSyncStats derives
// from its own full grant scan. Fusing them into the index scan removes a
// second O(grants) pass at EndSync.
type deferredGrantStats struct {
	syncID                string
	grants                int64
	grantsByEntitlementRT map[string]int64
}

// deferredRebuildCutBytes bounds the raw (key+value) bytes per rebuilt
// primary-grant SST. Cuts are strictly increasing in key order, so the files
// are disjoint and one IngestAndExcise call accepts them all.
const deferredRebuildCutBytes = 256 << 20

// rebuildBatch is one arena of raw rows handed from the scan thread to the
// rebuild writer goroutine. views index into arena (see kvView).
type rebuildBatch struct {
	arena []byte
	views []kvView
}

// grantRebuildTee streams raw primary-grant rows from the deferred index scan
// to a background goroutine that builds the rolling rebuild SSTs, so SST
// block compression and file IO overlap the scan instead of adding to it (an
// inline tee inflated the whale scan from ~28s to ~63s). Rows are copied into
// pooled arenas (the iterator's key/value bytes are only valid until Next)
// and flushed in ~8MiB batches over a small bounded channel: the scan blocks
// when the writer falls more than a few batches behind. Single producer.
type grantRebuildTee struct {
	dir string

	// Scan-thread state: the arena being filled.
	arena    []byte
	views    []kvView
	rawBytes int64

	ch   chan rebuildBatch
	done chan struct{}

	// Writer-goroutine state; read by finish()/abort() only after done.
	writer      *bulkSSTWriter
	chunkBytes  int64
	seq         int
	files       []string
	rowsWritten int64

	mu  sync.Mutex
	err error
}

func newGrantRebuildTee(dir string) *grantRebuildTee {
	t := &grantRebuildTee{
		dir:  dir,
		ch:   make(chan rebuildBatch, 4),
		done: make(chan struct{}),
	}
	go t.run()
	return t
}

func (t *grantRebuildTee) setErr(err error) {
	t.mu.Lock()
	if t.err == nil {
		t.err = err
	}
	t.mu.Unlock()
}

func (t *grantRebuildTee) takeErr() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.err
}

func (t *grantRebuildTee) run() {
	defer close(t.done)
	for batch := range t.ch {
		// After a failure, keep draining (recycling arenas) so the producer
		// never blocks; the stored error surfaces at the next add/finish.
		if t.takeErr() == nil {
			if err := t.writeBatch(batch); err != nil {
				t.setErr(err)
			}
		}
		putSpillArena(batch.arena)
		putSpillViews(batch.views)
	}
	if t.writer != nil {
		if err := t.writer.finish(); err != nil {
			t.setErr(err)
		} else if t.takeErr() == nil && t.writer.count > 0 {
			t.files = append(t.files, t.writer.path)
		}
		t.writer = nil
	}
}

func (t *grantRebuildTee) writeBatch(b rebuildBatch) error {
	for _, v := range b.views {
		if t.writer == nil {
			w, err := newBulkSSTWriter(t.dir, fmt.Sprintf("grant-rebuild-%03d", t.seq))
			if err != nil {
				return err
			}
			t.seq++
			t.writer = w
			t.chunkBytes = 0
		}
		if err := t.writer.add(b.arena[v.keyOff:v.keyEnd], b.arena[v.keyEnd:v.valEnd]); err != nil {
			return err
		}
		t.rowsWritten++
		t.chunkBytes += int64(v.valEnd - v.keyOff)
		if t.chunkBytes >= deferredRebuildCutBytes {
			if err := t.writer.finish(); err != nil {
				return err
			}
			t.files = append(t.files, t.writer.path)
			t.writer = nil
		}
	}
	return nil
}

// add copies one raw row into the current arena, flushing a batch to the
// writer when the arena fills.
func (t *grantRebuildTee) add(key, val []byte) error {
	if t.arena == nil {
		t.arena = getSpillArena()
		t.views = getSpillViews()
	}
	off := len(t.arena)
	t.arena = append(t.arena, key...)
	t.arena = append(t.arena, val...)
	t.views = append(t.views, kvView{keyOff: off, keyEnd: off + len(key), valEnd: off + len(key) + len(val)})
	t.rawBytes += int64(len(key) + len(val))
	if len(t.arena) >= bulkSpillKeyChunkBytes {
		return t.flushBatch()
	}
	return nil
}

func (t *grantRebuildTee) flushBatch() error {
	if err := t.takeErr(); err != nil {
		return err
	}
	if len(t.views) == 0 {
		return nil
	}
	t.ch <- rebuildBatch{arena: t.arena, views: t.views}
	t.arena, t.views = nil, nil
	return nil
}

// rebuildResult is what a finished tee hands back: the rolling SST paths,
// the number of rows written into them (compared against the scan's row
// count before the excise is allowed to fire), and the total raw key+value
// bytes teed (logging only).
type rebuildResult struct {
	files    []string
	rows     int64
	rawBytes int64
}

// finish flushes the tail batch, waits the writer goroutine out, and returns
// the finished result. Call exactly once (use abort on early-error paths).
func (t *grantRebuildTee) finish() (rebuildResult, error) {
	flushErr := t.flushBatch()
	t.closeAndWait()
	if err := t.takeErr(); err != nil {
		return rebuildResult{}, err
	}
	if flushErr != nil {
		return rebuildResult{}, flushErr
	}
	return rebuildResult{files: t.files, rows: t.rowsWritten, rawBytes: t.rawBytes}, nil
}

// abort discards the tee: the writer goroutine drains and exits, partial SSTs
// are left for the caller's temp-dir cleanup. Safe after a failed flush; must
// not be called after finish.
func (t *grantRebuildTee) abort() {
	t.setErr(errors.New("pebble: grant rebuild tee aborted"))
	t.closeAndWait()
}

func (t *grantRebuildTee) closeAndWait() {
	close(t.ch)
	<-t.done
	if t.arena != nil {
		putSpillArena(t.arena)
		putSpillViews(t.views)
		t.arena, t.views = nil, nil
	}
}

// BuildDeferredGrantIndexes rebuilds the remaining scattered expansion index
// family, by_principal, from entitlement-first primary grant keys. The expansion
// write path can skip by_principal inline because expansion reads by entitlement
// only; this method rewrites the whole by_principal range as one sorted SST at
// EndSync.
//
// The same scan also rebuilds the primary grant keyspace itself: every raw
// (key, value) is teed into rolling SSTs which replace the whole grant range
// via IngestAndExcise. Expansion's layer ingests are SSTs whose spans
// interleave, so they stack in the LSM's upper levels; consolidating them via
// compaction rewrites the data once per level hop (a whale measured ~60s /
// 431 compactions), while this tee rewrites each byte exactly once on top of
// a scan that is already being paid for the index build. After the excise the
// grant range is one flat sorted run in the bottom level: compaction debt ~0
// without running the compactor at all. Requires no concurrent grant writers,
// which EndSync guarantees.
//
// The scan also accumulates the grant portion of the sync-stats sidecar
// (total count + per-entitlement-resource-type grouping) and stashes it so
// the subsequent PersistSyncStats call can skip its own full grant scan. The
// stats accumulation is best-effort: a value that fails the shallow field
// scan just disables the stash and PersistSyncStats falls back to scanning.
//
// The whole build runs under the engine write barrier (withWrite): the
// IngestAndExcise calls destroy everything in their key ranges, so a grant
// row committed by a concurrent writer after the scan's iterator snapshot
// would be silently erased. EndSync's callers are expected to have quiesced
// writers already — holding writeMu for the duration converts that
// convention into an enforced invariant (a straggler write blocks until the
// build finishes instead of racing the excise), and writeWG participation
// means Close waits the build out instead of tearing down e.db under it.
func (e *Engine) BuildDeferredGrantIndexes(ctx context.Context) error {
	// AllowSealed: EndSync seals BEFORE running this build so no straggler
	// record writer can slip a row in behind the scan (see Adapter.EndSync);
	// the build itself is one of the sealed window's own steps.
	return e.withWriteAllowSealed(func() error {
		return e.buildDeferredGrantIndexesLocked(ctx)
	})
}

func (e *Engine) buildDeferredGrantIndexesLocked(ctx context.Context) error {
	// The scan below only polls ctx periodically; don't start an O(grants)
	// pass (or its destructive IngestAndExcise) on an already-dead context.
	if err := ctx.Err(); err != nil {
		return err
	}
	start := time.Now()
	l := ctxzap.Extract(ctx)
	dir, err := os.MkdirTemp("", "pebble-deferred-idx-")
	if err != nil {
		return fmt.Errorf("BuildDeferredGrantIndexes: mkdir temp: %w", err)
	}
	defer os.RemoveAll(dir)

	sorters := min(4, max(2, runtime.GOMAXPROCS(0)/2))
	sem := make(chan struct{}, sorters)
	principal := newSpillSorter(dir, fmt.Sprintf("index-%02x", idxGrantByPrincipal), sem, deferredIndexSpillChunkBytes)
	principal.free = newSpillArenaFreeList(deferredIndexSpillChunkBytes, sorters+2)
	// Every early return must wait out principal's background chunk sorts
	// before the deferred RemoveAll(dir) deletes the directory they write
	// into (mirrors the rebuild tee's guard below). Declared AFTER the
	// RemoveAll defer so LIFO ordering runs the wait first; abort is a
	// no-op once finalize has drained the sorter.
	defer principal.abort()

	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: GrantLowerBound(),
		UpperBound: GrantUpperBound(),
	})
	if err != nil {
		return fmt.Errorf("BuildDeferredGrantIndexes: iter: %w", err)
	}
	iterClosed := false
	defer func() {
		if !iterClosed {
			_ = iter.Close()
		}
	}()

	statsOK := true
	var totalKeys int64
	grantsByEntRTPtr := map[string]*int64{}

	rebuild := newGrantRebuildTee(dir)
	rebuildFinished := false
	defer func() {
		if !rebuildFinished {
			rebuild.abort()
		}
	}()
	// A tee failure surfacing mid-scan (via add→flushBatch→takeErr) is the
	// same class of failure finish() reports post-scan, and the GUARD below
	// downgrades that to a loud skip-the-excise no-op. Honor the same
	// contract here: remember the error, stop teeing, and keep scanning —
	// by_principal and the stats stash don't depend on the rebuild.
	var rebuildScanErr error

	var scanned, droppedMalformedKeys int64
	var idxKeyScratch []byte
	lastLog := start
	for iter.First(); iter.Valid(); iter.Next() {
		totalKeys++
		if statsOK {
			// Same shallow scan + grouping computeSyncStats performs; see
			// sync_stats_sidecar.go for the map[string]*int64 rationale.
			if entRT, serr := scanGrantEntitlementResourceTypeRaw(iter.Value()); serr != nil {
				statsOK = false
			} else if p, ok := grantsByEntRTPtr[string(entRT)]; ok {
				*p++
			} else {
				n := int64(1)
				grantsByEntRTPtr[string(entRT)] = &n
			}
		}
		// Tee the raw row into the primary-keyspace rebuild pipeline. The
		// iterator yields globally sorted keys, so each SST is sorted and
		// the cut files are mutually disjoint.
		if rebuildScanErr == nil {
			if err := rebuild.add(iter.Key(), iter.Value()); err != nil {
				rebuildScanErr = err
			}
		}
		idxKey, ok := appendGrantByPrincipalKeyFromPrimary(idxKeyScratch[:0], iter.Key())
		idxKeyScratch = idxKey
		if !ok {
			// Only possible on key-layout drift or corruption: every grant
			// write path derives its key from the same 6-segment encoder.
			// The row still rides the primary rebuild (totalKeys counts it),
			// but it cannot be represented in by_principal — count and warn
			// below so a principal silently losing grants is observable.
			droppedMalformedKeys++
			continue
		}
		if err := principal.add(idxKey, nil); err != nil {
			return err
		}
		scanned++
		// Throttle per-row bookkeeping; ctx.Err and time.Now are measurable
		// at 57M+ rows.
		if scanned&0xFFFF == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
			if now := time.Now(); now.Sub(lastLog) >= 15*time.Second {
				l.Info("deferred grant index build: scanning primary grants",
					zap.Int64("index_keys_added", scanned),
					zap.Duration("elapsed", now.Sub(start)),
				)
				lastLog = now
			}
		}
	}
	if err := iter.Error(); err != nil {
		return fmt.Errorf("BuildDeferredGrantIndexes: iter error: %w", err)
	}
	if droppedMalformedKeys > 0 {
		l.Error("deferred grant index build: grant primary keys did not decode as 6-segment identities; their rows are NOT represented in by_principal",
			zap.Int64("dropped", droppedMalformedKeys),
		)
	}
	rebuildFinished = true
	// The rebuild is an optimization: any failure — scan-time tee error or
	// finish-time writer error — skips the excise and leaves the (correct,
	// just unconsolidated) LSM alone rather than failing the sync.
	var rebuilt rebuildResult
	rebuildErr := rebuildScanErr
	if rebuildErr == nil {
		rebuilt, rebuildErr = rebuild.finish()
	} else {
		rebuild.abort()
	}
	if statsOK {
		if syncID := codec.DecodeSyncID(e.currentSyncBytes()); syncID != "" {
			grantsByEntitlementRT := make(map[string]int64, len(grantsByEntRTPtr))
			for rt, p := range grantsByEntRTPtr {
				grantsByEntitlementRT[rt] = *p
			}
			e.stashDeferredGrantStats(&deferredGrantStats{
				syncID:                syncID,
				grants:                totalKeys,
				grantsByEntitlementRT: grantsByEntitlementRT,
			})
		}
	}
	scanDone := time.Now()
	l.Info("deferred grant index build: scan complete",
		zap.Int64("index_keys_added", scanned),
		zap.Duration("elapsed", scanDone.Sub(start)),
	)

	// Release the scan's pinned version before excising the range it read.
	iterClosed = true
	if err := iter.Close(); err != nil {
		return fmt.Errorf("BuildDeferredGrantIndexes: close iter: %w", err)
	}

	// Atomically replace the whole grant primary range with the rebuilt flat
	// run. The excised span drops the stacked expansion-layer SSTs (and any
	// batch-written grant rows in L0) in one version edit; the replacement
	// files are disjoint and land in the bottom level.
	//
	// GUARD: the excise destroys everything in the range, so it only runs
	// when the rebuilt SSTs verifiably contain every scanned row. A tee
	// failure or a row-count mismatch downgrades to a loud no-op — the LSM
	// stays correct, just unconsolidated.
	switch {
	case rebuildErr != nil:
		l.Error("deferred grant index build: primary keyspace rebuild failed; skipping consolidation",
			zap.Error(rebuildErr),
		)
	case rebuilt.rows != totalKeys:
		l.Error("deferred grant index build: rebuild row count mismatch; skipping consolidation",
			zap.Int64("rows_scanned", totalKeys),
			zap.Int64("rows_rebuilt", rebuilt.rows),
		)
	case len(rebuilt.files) > 0:
		if _, err := e.db.IngestAndExcise(ctx, rebuilt.files, nil, nil, pebble.KeyRange{
			Start: GrantLowerBound(),
			End:   GrantUpperBound(),
		}); err != nil {
			// Ingest is atomic: a failure applies nothing. Keep the sync alive.
			l.Error("deferred grant index build: rebuild ingest/excise failed; skipping consolidation",
				zap.Error(err),
			)
			break
		}
		l.Info("deferred grant index build: primary grant keyspace rebuilt",
			zap.Int("ssts", len(rebuilt.files)),
			zap.Int64("rows", rebuilt.rows),
			zap.Int64("raw_bytes", rebuilt.rawBytes),
			zap.Duration("elapsed", time.Since(scanDone)),
		)
	}

	chunks, err := principal.finalize()
	if err != nil {
		return err
	}
	if len(chunks) == 0 {
		l.Info("deferred grant index build: no index chunks to ingest",
			zap.Int64("index_keys_added", scanned),
			zap.Duration("total", time.Since(start)),
		)
		return nil
	}
	l.Info("deferred grant index build: merging sorted chunks",
		zap.Int("chunks", len(chunks)),
		zap.Int64("index_keys_added", scanned),
		zap.Duration("scan", scanDone.Sub(start)),
	)
	sstPath := filepath.Join(dir, fmt.Sprintf("index-%02x.sst", idxGrantByPrincipal))
	if err := mergeSortedSpillChunksToSST(ctx, sstPath, fmt.Sprintf("index-%02x", idxGrantByPrincipal), chunks); err != nil {
		return err
	}
	mergeDone := time.Now()

	_, err = e.db.IngestAndExcise(ctx, []string{sstPath}, nil, nil, pebble.KeyRange{
		Start: GrantByPrincipalLowerBound(),
		End:   GrantByPrincipalUpperBound(),
	})
	if err != nil {
		return fmt.Errorf("BuildDeferredGrantIndexes: ingest/excise: %w", err)
	}

	ctxzap.Extract(ctx).Info("deferred grant index build complete",
		zap.Int64("index_keys_scanned", scanned),
		zap.Duration("scan", scanDone.Sub(start)),
		zap.Duration("merge", mergeDone.Sub(scanDone)),
		zap.Duration("ingest", time.Since(mergeDone)),
		zap.Duration("total", time.Since(start)),
	)
	return nil
}
