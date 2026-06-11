package pebble

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/v2/sstable"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/codec"
)

// ErrBulkImportOutOfOrder is returned by BulkSyncImport's ordered add
// methods when a primary key arrives that does not sort strictly after
// the previous key in the same bucket. It means the caller's
// sorted-source contract was violated; the import cannot continue and
// must be Abort()ed.
var ErrBulkImportOutOfOrder = errors.New("bulk sync import: keys are not strictly increasing")

// errBulkImportDuplicateKey is the spill-merge guard against duplicate
// keys. Duplicates are impossible for well-formed sources (the importer
// requires global uniqueness); hitting this means corrupt input.
var errBulkImportDuplicateKey = errors.New("bulk sync import: duplicate key in spill merge")

// bulkSpillKeyChunkBytes bounds the in-memory arena of one spill chunk.
// Chunks are sorted and written as sorted runs in the background while
// records keep streaming in, then k-way merged into SSTs at Finish.
const bulkSpillKeyChunkBytes = 8 << 20

// bulkSpillBufferSize is the bufio size for spill-chunk IO.
const bulkSpillBufferSize = 1 << 20

// grantIndexFamilies are the index-discriminator bytes AddGrants can
// emit (see grantIndexKeys).
var grantIndexFamilies = []byte{
	idxGrantByEntitlement,
	idxGrantByPrincipal,
	idxGrantByNeedsExpansion,
	idxGrantByPrincipalResourceType,
	idxGrantByEntitlementResource,
}

// bulkSSTWriter builds one SST file for a single disjoint key bucket.
// Keys must arrive in strictly increasing order (enforced). Mirrors the
// compactor's sstBuilder: the table format is pinned to SDKPebbleFormat
// so the SST is ingestible without a format upgrade, and 32 KiB blocks
// trade a little space for less per-block framing CPU on write-once
// sequentially-read tables.
type bulkSSTWriter struct {
	name  string
	path  string
	w     *sstable.Writer
	last  []byte
	count int
}

func newBulkSSTWriter(dir, name string) (*bulkSSTWriter, error) {
	path := filepath.Join(dir, name+".sst")
	file, err := vfs.Default.Create(path, vfs.WriteCategoryUnspecified)
	if err != nil {
		return nil, fmt.Errorf("bulk sync import: create %q: %w", path, err)
	}
	w := sstable.NewWriter(objstorageprovider.NewFileWritable(file), sstable.WriterOptions{
		TableFormat: SDKPebbleFormat.MaxTableFormat(),
		BlockSize:   32 << 10,
	})
	return &bulkSSTWriter{name: name, path: path, w: w}, nil
}

func (w *bulkSSTWriter) add(key, val []byte) error {
	if len(w.last) > 0 && bytes.Compare(key, w.last) <= 0 {
		return fmt.Errorf("%w: bucket %s", ErrBulkImportOutOfOrder, w.name)
	}
	if err := w.w.Set(key, val); err != nil {
		return fmt.Errorf("bulk sync import: set %s: %w", w.name, err)
	}
	w.last = append(w.last[:0], key...)
	w.count++
	return nil
}

// finish closes the sstable writer (flushing the table footer). Safe to
// call more than once. The underlying vfs.File is closed by the writer.
func (w *bulkSSTWriter) finish() error {
	if w.w == nil {
		return nil
	}
	err := w.w.Close()
	w.w = nil
	if err != nil {
		return fmt.Errorf("bulk sync import: close %s: %w", w.name, err)
	}
	return nil
}

// BulkSyncImport is the trusted one-shot import fast path: instead of
// committing records through the memtable/WAL, it builds sorted SSTs on
// disk and ingests them all in a single pebble Ingest at Finish. Each key
// is written once into its final table — no WAL append, no L0 flush, no
// background compaction debt.
//
// Resource types, resources, and entitlements stream straight into one
// SST per bucket and must arrive in strictly increasing encoded-key
// order (SQLite BINARY collation order on the key's tuple columns —
// the tuple key codec is order-preserving; violations fail with
// ErrBulkImportOutOfOrder). These add methods are single-threaded.
//
// Grants scale across goroutines: each scanning goroutine takes its own
// shard (NewGrantShard) covering a disjoint ascending external-id range,
// so the grant hot path acquires no shared lock at all. Shard primaries
// stream straight into one final SST per shard (ordered by the shard
// contract — no spill, no sort, no merge); the five secondary index
// families (derived internally from the translated records — the same
// nil-guards and key shapes as the engine's canonical writeXxxIndexes
// paths) are key-only spill-sorted. Spill chunks sort and flush to disk
// in the background while the scans keep streaming; Finish k-way merges
// each family's sorted runs (across all shards) into one SST per
// family, in parallel, and ingests everything in a single call.
//
// Every record/index tuple must appear at most once (no dedup, no
// read-before-write); SQLite's UNIQUE(external_id, sync_id) gives
// converters that guarantee. The destination sync must be freshly
// started (MarkFreshSync via StartNewSync) and empty; nothing else may
// write to the engine between Start and Finish. Used by C1File.ToPebble
// for sqlite→pebble conversion.
type BulkSyncImport struct {
	e      *Engine
	syncID string
	dir    string
	done   bool

	resourceTypes *bulkSSTWriter
	resources     *bulkSSTWriter
	entitlements  *bulkSSTWriter

	// sortSem bounds concurrently running background chunk sorts
	// across all sorters and shards.
	sortSem chan struct{}

	// Parent-level sorters for the (single-threaded) resource and
	// entitlement index keys.
	idxResourceByParent      *spillSorter
	idxEntitlementByResource *spillSorter

	// mu guards shard registration/aggregation. The grant hot path
	// never takes it.
	mu       sync.Mutex
	shards   []*BulkGrantShard
	shardSeq int

	resourcesByRT map[string]int64
}

// StartBulkSyncImport opens a bulk import targeting syncID, which must be
// the engine's current FRESH sync (see BulkSyncImport contract). Working
// files are staged in a fresh directory under tmpDir ("" = system temp
// dir) and removed by Finish/Abort.
func (e *Engine) StartBulkSyncImport(ctx context.Context, syncID string, tmpDir string) (*BulkSyncImport, error) {
	if !e.IsFreshSync() {
		return nil, errors.New("StartBulkSyncImport: sync is not fresh")
	}
	idBytes, err := codec.EncodeSyncID(syncID)
	if err != nil {
		return nil, fmt.Errorf("StartBulkSyncImport: %w", err)
	}
	if cur := e.currentSyncBytes(); !bytes.Equal(cur, idBytes) {
		return nil, fmt.Errorf("StartBulkSyncImport: sync %s is not the engine's current sync", syncID)
	}
	dir, err := os.MkdirTemp(tmpDir, "pebble-bulk-import-")
	if err != nil {
		return nil, fmt.Errorf("StartBulkSyncImport: mkdir temp: %w", err)
	}
	// Background chunk-sort fan-out: enough to keep sorts off the scan
	// goroutines' critical path, with an absolute cap so a large shared
	// host doesn't see a conversion grab dozens of cores (each slot also
	// pins one chunk arena, so this bounds sort memory too).
	sorters := min(4, max(2, runtime.GOMAXPROCS(0)/2))
	b := &BulkSyncImport{
		e:             e,
		syncID:        syncID,
		dir:           dir,
		sortSem:       make(chan struct{}, sorters),
		resourcesByRT: map[string]int64{},
	}
	for _, w := range []struct {
		slot **bulkSSTWriter
		name string
	}{
		{&b.resourceTypes, "resource-types"},
		{&b.resources, "resources"},
		{&b.entitlements, "entitlements"},
	} {
		sw, err := newBulkSSTWriter(dir, w.name)
		if err != nil {
			b.Abort()
			return nil, err
		}
		*w.slot = sw
	}
	b.idxResourceByParent = newSpillSorter(dir, fmt.Sprintf("index-%02x-p", idxResourceByParent), b.sortSem, bulkSpillKeyChunkBytes)
	b.idxEntitlementByResource = newSpillSorter(dir, fmt.Sprintf("index-%02x-p", idxEntitlementByResource), b.sortSem, bulkSpillKeyChunkBytes)
	return b, nil
}

// BulkGrantShard is one goroutine's private view of the grant import:
// an ordered primary SST writer and per-family index sorters. Create
// one per scanning goroutine via NewGrantShard; AddGrants on a shard
// takes no shared locks. Close the shard when its scan completes.
//
// Grants within a shard MUST arrive sorted by external id, and shards
// must cover pairwise-disjoint external-id ranges — exactly what a
// sharded `ORDER BY external_id` scan over partitioned ranges of
// SQLite's UNIQUE(external_id, sync_id) index produces. That lets each
// shard stream its primaries straight into a final SST with no spill,
// no sort, and no merge; pebble's Ingest rejects overlapping tables, so
// a boundary bug cannot corrupt the store. The index keys sort on other
// fields and go through the shard's spill sorters.
type BulkGrantShard struct {
	b      *BulkSyncImport
	grants *bulkSSTWriter
	idx    map[byte]*spillSorter
	entRT  map[string]int64
	closed bool
	err    error
	// valBuf is the reusable marshal scratch for primary record values.
	// Safe because both the sstable writer and the spill sorters copy
	// bytes out before add() returns.
	valBuf []byte
}

// NewGrantShard registers a new grant import shard. Safe to call
// concurrently.
func (b *BulkSyncImport) NewGrantShard() (*BulkGrantShard, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	id := b.shardSeq
	b.shardSeq++
	w, err := newBulkSSTWriter(b.dir, fmt.Sprintf("grants-s%03d", id))
	if err != nil {
		return nil, err
	}
	s := &BulkGrantShard{
		b:      b,
		grants: w,
		idx:    map[byte]*spillSorter{},
		entRT:  map[string]int64{},
	}
	for _, idx := range grantIndexFamilies {
		s.idx[idx] = newSpillSorter(b.dir, fmt.Sprintf("index-%02x-s%03d", idx, id), b.sortSem, bulkSpillKeyChunkBytes)
	}
	b.shards = append(b.shards, s)
	return s, nil
}

// AddGrants translates and appends grants to this shard, in ascending
// external-id order (see BulkGrantShard). Translation, the v3 marshal,
// and key encoding run in the caller's goroutine; all writers are
// shard-private, so concurrent shards never contend.
func (s *BulkGrantShard) AddGrants(ctx context.Context, grants ...*v2.Grant) error {
	if s.closed {
		return errors.New("bulk sync import: AddGrants on closed shard")
	}
	// Serial translate: the caller's lanes are the parallelism; the
	// fan-out inside translateGrants would only oversubscribe shared
	// hosts (lanes × translate shards goroutine bursts).
	records := translateGrantsSerial(s.b.syncID, grants, timestamppb.Now())
	for _, r := range records {
		if r == nil {
			continue
		}
		val, err := marshalRecordAppend(s.valBuf[:0], r)
		if err != nil {
			return s.fail(err)
		}
		s.valBuf = val
		if err := s.grants.add(encodeGrantKey(r.GetExternalId()), val); err != nil {
			return s.fail(err)
		}
		s.entRT[r.GetEntitlement().GetResourceTypeId()]++
		for _, k := range grantIndexKeys(r) {
			if len(k) < 3 || k[1] != typeIndex {
				return s.fail(fmt.Errorf("bulk sync import: malformed index key %x", k))
			}
			w := s.idx[k[2]]
			if w == nil {
				return s.fail(fmt.Errorf("bulk sync import: unknown index family %#02x", k[2]))
			}
			if err := w.add(k, nil); err != nil {
				return s.fail(err)
			}
		}
	}
	return nil
}

func (s *BulkGrantShard) fail(err error) error {
	if s.err == nil {
		s.err = err
	}
	return err
}

// Close finalizes the shard's primary SST and flushes its index tail
// chunks into the background sorters. Must be called once per shard
// before Finish; AddGrants is invalid afterwards.
func (s *BulkGrantShard) Close() {
	if s.closed {
		return
	}
	s.closed = true
	if err := s.grants.finish(); err != nil {
		_ = s.fail(err)
	}
	for _, w := range s.idx {
		w.cutAndDispatch()
	}
}

// AddResourceTypes translates and appends resource types, which must
// arrive sorted by external id.
func (b *BulkSyncImport) AddResourceTypes(ctx context.Context, rts ...*v2.ResourceType) error {
	now := timestamppb.Now()
	for _, rt := range rts {
		if rt == nil {
			continue
		}
		rec := V2ResourceTypeToV3(b.syncID, rt)
		if rec == nil {
			continue
		}
		if rec.GetDiscoveredAt() == nil {
			rec.SetDiscoveredAt(now)
		}
		val, err := marshalRecord(rec)
		if err != nil {
			return err
		}
		if err := b.resourceTypes.add(encodeResourceTypeKey(rec.GetExternalId()), val); err != nil {
			return err
		}
	}
	return nil
}

// AddResources translates and appends resources, which must arrive
// sorted by (resource_type_id, resource_id). by_parent index keys are
// derived and spilled with the same parent guard as writeResourceIndexes.
func (b *BulkSyncImport) AddResources(ctx context.Context, resources ...*v2.Resource) error {
	now := timestamppb.Now()
	for _, r := range resources {
		if r == nil {
			continue
		}
		rec := V2ResourceToV3(b.syncID, r)
		if rec == nil {
			continue
		}
		if rec.GetDiscoveredAt() == nil {
			rec.SetDiscoveredAt(now)
		}
		val, err := marshalRecord(rec)
		if err != nil {
			return err
		}
		if err := b.resources.add(encodeResourceKey(rec.GetResourceTypeId(), rec.GetResourceId()), val); err != nil {
			return err
		}
		b.resourcesByRT[rec.GetResourceTypeId()]++
		if parent := rec.GetParent(); parent != nil && parent.GetResourceId() != "" {
			k := encodeResourceByParentIndexKey(
				parent.GetResourceTypeId(), parent.GetResourceId(),
				rec.GetResourceTypeId(), rec.GetResourceId(),
			)
			if err := b.idxResourceByParent.add(k, nil); err != nil {
				return err
			}
		}
	}
	return nil
}

// AddEntitlements translates and appends entitlements, which must arrive
// sorted by external id. by_resource index keys are derived and spilled
// with the same resource guard as writeEntitlementIndexes.
func (b *BulkSyncImport) AddEntitlements(ctx context.Context, ents ...*v2.Entitlement) error {
	now := timestamppb.Now()
	for _, e := range ents {
		if e == nil {
			continue
		}
		rec := V2EntitlementToV3(b.syncID, e)
		if rec == nil {
			continue
		}
		if rec.GetDiscoveredAt() == nil {
			rec.SetDiscoveredAt(now)
		}
		val, err := marshalRecord(rec)
		if err != nil {
			return err
		}
		if err := b.entitlements.add(encodeEntitlementKey(rec.GetExternalId()), val); err != nil {
			return err
		}
		if res := rec.GetResource(); res != nil && res.GetResourceId() != "" {
			k := encodeEntitlementByResourceIndexKey(
				res.GetResourceTypeId(), res.GetResourceId(),
				rec.GetExternalId(),
			)
			if err := b.idxEntitlementByResource.add(k, nil); err != nil {
				return err
			}
		}
	}
	return nil
}

// ComputedStats returns a SyncStatsRecord built from the counts the
// import accumulated while streaming (primary record counts, resources
// by resource type, grants by entitlement resource type). Valid after
// all grant shards are Closed. Assets do not flow through the importer
// — the caller sets that count itself before stashing the record via
// Engine.StashComputedSyncStats.
func (b *BulkSyncImport) ComputedStats() *v3.SyncStatsRecord {
	b.mu.Lock()
	defer b.mu.Unlock()
	var grants int64
	grantsByEntRT := map[string]int64{}
	for _, s := range b.shards {
		grants += int64(s.grants.count)
		for rt, n := range s.entRT {
			grantsByEntRT[rt] += n
		}
	}
	rec := &v3.SyncStatsRecord{}
	rec.SetSyncId(b.syncID)
	rec.SetResourceTypes(int64(b.resourceTypes.count))
	rec.SetResources(int64(b.resources.count))
	rec.SetEntitlements(int64(b.entitlements.count))
	rec.SetGrants(grants)
	rec.SetResourcesByResourceType(b.resourcesByRT)
	rec.SetGrantsByEntitlementResourceType(grantsByEntRT)
	return rec
}

// Finish finalizes the import: every spill sorter drains its background
// sorts, each index family k-way merges its sorted runs (combined
// across all shards) into one SST in parallel, the ordered primary SSTs
// (including each shard's grant SST) are collected, and everything is
// ingested in one pebble Ingest call (the key ranges are pairwise
// disjoint, so a single call is legal and costs one manifest rotation).
// The staging directory is removed on the way out regardless of outcome.
func (b *BulkSyncImport) Finish(ctx context.Context) error {
	if b.done {
		return errors.New("bulk sync import: already finished or aborted")
	}
	b.done = true
	defer func() { _ = os.RemoveAll(b.dir) }()
	start := time.Now()

	paths := make([]string, 0, 4+len(grantIndexFamilies))
	for _, w := range []*bulkSSTWriter{b.resourceTypes, b.resources, b.entitlements} {
		if err := w.finish(); err != nil {
			return err
		}
		if w.count > 0 {
			paths = append(paths, w.path)
		}
	}

	// Per-shard grant primary SSTs: already final, already sorted, and
	// pairwise disjoint by the shard range contract (Ingest verifies).
	b.mu.Lock()
	shards := b.shards
	b.mu.Unlock()
	for _, s := range shards {
		if !s.closed {
			return errors.New("bulk sync import: Finish called with an unclosed grant shard")
		}
		if s.err != nil {
			return s.err
		}
		if s.grants.count > 0 {
			paths = append(paths, s.grants.path)
		}
	}

	// Build the merge units: each combines the sorted runs of one key
	// family across every sorter that produced them.
	type mergeUnit struct {
		name    string
		sorters []*spillSorter
	}
	units := []mergeUnit{
		{name: fmt.Sprintf("index-%02x", idxResourceByParent), sorters: []*spillSorter{b.idxResourceByParent}},
		{name: fmt.Sprintf("index-%02x", idxEntitlementByResource), sorters: []*spillSorter{b.idxEntitlementByResource}},
	}
	for _, idx := range grantIndexFamilies {
		u := mergeUnit{name: fmt.Sprintf("index-%02x", idx)}
		for _, s := range shards {
			u.sorters = append(u.sorters, s.idx[idx])
		}
		units = append(units, u)
	}

	results := make([]struct {
		path string
		err  error
	}, len(units))
	var wg sync.WaitGroup
	for i, u := range units {
		wg.Add(1)
		go func(slot int, u mergeUnit) {
			defer wg.Done()
			var chunks []string
			for _, s := range u.sorters {
				c, err := s.finalize()
				if err != nil {
					results[slot].err = err
					return
				}
				chunks = append(chunks, c...)
			}
			if len(chunks) == 0 {
				return
			}
			sstPath := filepath.Join(b.dir, u.name+".sst")
			if err := mergeSortedSpillChunksToSST(ctx, sstPath, u.name, chunks); err != nil {
				results[slot].err = err
				return
			}
			results[slot].path = sstPath
		}(i, u)
	}
	wg.Wait()
	for _, r := range results {
		if r.err != nil {
			return r.err
		}
		if r.path != "" {
			paths = append(paths, r.path)
		}
	}
	mergesDone := time.Now()

	if len(paths) == 0 {
		return nil
	}
	err := b.e.withWrite(func() error {
		if err := b.e.db.Ingest(ctx, paths); err != nil {
			return fmt.Errorf("bulk sync import: ingest: %w", err)
		}
		return nil
	})
	if err != nil {
		return err
	}
	ctxzap.Extract(ctx).Debug("bulk sync import: finish timings",
		zap.Duration("spill_merge", mergesDone.Sub(start)),
		zap.Duration("ingest", time.Since(mergesDone)),
		zap.Int("ssts", len(paths)),
	)
	return nil
}

// Abort closes and removes all staged files without ingesting. Safe to
// call after Finish (no-op) or on a partially-failed import.
func (b *BulkSyncImport) Abort() {
	if b.done {
		return
	}
	b.done = true
	for _, w := range []*bulkSSTWriter{b.resourceTypes, b.resources, b.entitlements} {
		if w != nil {
			_ = w.finish()
		}
	}
	b.mu.Lock()
	shards := b.shards
	b.mu.Unlock()
	for _, s := range shards {
		s.closed = true
		_ = s.grants.finish()
		for _, w := range s.idx {
			w.abort()
		}
	}
	for _, w := range []*spillSorter{b.idxResourceByParent, b.idxEntitlementByResource} {
		if w != nil {
			w.abort()
		}
	}
	_ = os.RemoveAll(b.dir)
}

// --- spill sorter: unordered (key[,value]) → background chunk sort →
//     k-way merge into one SST ---
//
// add() appends entries to an in-memory arena; when the arena reaches
// its chunk threshold it is handed to a background goroutine (bounded
// by the import-wide sortSem) that sorts it by key and writes a sorted
// run file, overlapping sort+IO with the caller's scan. finalize()
// flushes the tail and waits; the caller then merges the runs.

// kvView is an (offset, end, end) view into a sort arena: the key at
// [keyOff:keyEnd] is immediately followed by its value at [keyEnd:valEnd].
// Views over one contiguous buffer avoid a heap copy per entry and give
// the GC a pointer-free chunk to skip.
type kvView struct {
	keyOff int
	keyEnd int
	valEnd int
}

type spillSorter struct {
	name       string
	dir        string
	sem        chan struct{}
	chunkBytes int

	arena []byte
	views []kvView
	count int64

	wg       sync.WaitGroup
	chunkMu  sync.Mutex
	chunks   []string
	firstErr error
}

func newSpillSorter(dir, name string, sem chan struct{}, chunkBytes int) *spillSorter {
	return &spillSorter{name: name, dir: dir, sem: sem, chunkBytes: chunkBytes}
}

// add appends one entry. val may be nil (key-only spills). NOT
// goroutine-safe — each sorter has exactly one producer. May block on
// the sort semaphore when the arena fills (backpressure on this
// producer only).
func (s *spillSorter) add(key, val []byte) error {
	if len(key) > math.MaxUint32 || len(val) > math.MaxUint32 {
		return fmt.Errorf("bulk sync import: spill entry too large (%d/%d bytes)", len(key), len(val))
	}
	if err := s.takeErr(); err != nil {
		return err
	}
	keyOff := len(s.arena)
	s.arena = append(s.arena, key...)
	s.arena = append(s.arena, val...)
	s.views = append(s.views, kvView{keyOff: keyOff, keyEnd: keyOff + len(key), valEnd: keyOff + len(key) + len(val)})
	s.count++
	if len(s.arena) >= s.chunkBytes {
		s.cutAndDispatch()
	}
	return nil
}

// cutAndDispatch detaches the active arena and hands it to a background
// sorter, blocking while all sort slots are busy.
func (s *spillSorter) cutAndDispatch() {
	arena, views := s.arena, s.views
	s.arena, s.views = nil, nil
	if len(views) == 0 {
		return
	}
	s.wg.Add(1)
	s.sem <- struct{}{}
	go func() {
		defer func() {
			<-s.sem
			s.wg.Done()
		}()
		s.sortAndWriteChunk(arena, views)
	}()
}

func (s *spillSorter) sortAndWriteChunk(arena []byte, views []kvView) {
	sort.Slice(views, func(i, j int) bool {
		return bytes.Compare(arena[views[i].keyOff:views[i].keyEnd], arena[views[j].keyOff:views[j].keyEnd]) < 0
	})
	s.chunkMu.Lock()
	chunkPath := filepath.Join(s.dir, fmt.Sprintf("%s-chunk-%04d.bin", s.name, len(s.chunks)))
	s.chunks = append(s.chunks, chunkPath)
	s.chunkMu.Unlock()
	if err := writeSortedSpillChunk(chunkPath, arena, views); err != nil {
		s.setErr(err)
	}
}

func (s *spillSorter) setErr(err error) {
	s.chunkMu.Lock()
	if s.firstErr == nil {
		s.firstErr = err
	}
	s.chunkMu.Unlock()
}

func (s *spillSorter) takeErr() error {
	s.chunkMu.Lock()
	defer s.chunkMu.Unlock()
	return s.firstErr
}

// abort waits out in-flight sorts so the staging dir can be removed.
func (s *spillSorter) abort() {
	s.arena, s.views = nil, nil
	s.wg.Wait()
}

// finalize flushes the tail chunk, waits for background sorts, and
// returns the sorted run paths.
func (s *spillSorter) finalize() ([]string, error) {
	s.cutAndDispatch()
	s.wg.Wait()
	if err := s.takeErr(); err != nil {
		return nil, err
	}
	return s.chunks, nil
}

func writeSortedSpillChunk(path string, arena []byte, views []kvView) error {
	f, err := os.Create(path) // #nosec G304 - staged under the import's MkdirTemp dir.
	if err != nil {
		return err
	}
	writer := bufio.NewWriterSize(f, bulkSpillBufferSize)
	success := false
	defer func() {
		_ = f.Close()
		if !success {
			_ = os.Remove(path)
		}
	}()
	var lenBuf [4]byte
	for _, v := range views {
		// Lengths are bounded by add()'s MaxUint32 checks.
		binary.BigEndian.PutUint32(lenBuf[:], uint32(v.keyEnd-v.keyOff)) // #nosec G115
		if _, err := writer.Write(lenBuf[:]); err != nil {
			return err
		}
		if _, err := writer.Write(arena[v.keyOff:v.keyEnd]); err != nil {
			return err
		}
		binary.BigEndian.PutUint32(lenBuf[:], uint32(v.valEnd-v.keyEnd)) // #nosec G115
		if _, err := writer.Write(lenBuf[:]); err != nil {
			return err
		}
		if _, err := writer.Write(arena[v.keyEnd:v.valEnd]); err != nil {
			return err
		}
	}
	if err := writer.Flush(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	success = true
	return nil
}

type spillChunkItem struct {
	chunkIdx int
	key      []byte
	val      []byte
}

type spillChunkHeap []spillChunkItem

func (h spillChunkHeap) less(i, j int) bool {
	cmp := bytes.Compare(h[i].key, h[j].key)
	if cmp == 0 {
		return h[i].chunkIdx < h[j].chunkIdx
	}
	return cmp < 0
}
func (h spillChunkHeap) swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *spillChunkHeap) push(item spillChunkItem) {
	*h = append(*h, item)
	for child := len(*h) - 1; child > 0; {
		parent := (child - 1) / 2
		if !h.less(child, parent) {
			break
		}
		h.swap(child, parent)
		child = parent
	}
}
func (h *spillChunkHeap) pop() spillChunkItem {
	old := *h
	item := old[0]
	last := old[len(old)-1]
	*h = old[:len(old)-1]
	if len(*h) > 0 {
		(*h)[0] = last
		h.down(0)
	}
	return item
}
func (h *spillChunkHeap) down(parent int) {
	for {
		left := 2*parent + 1
		if left >= len(*h) {
			return
		}
		child := left
		if right := left + 1; right < len(*h) && h.less(right, left) {
			child = right
		}
		if !h.less(child, parent) {
			return
		}
		h.swap(parent, child)
		parent = child
	}
}

// readSpillEntry reads one length-prefixed (key, value) pair. Buffers
// are reused across calls per chunk.
func readSpillEntry(r io.Reader, key, val *[]byte, lenBuf *[4]byte) (bool, error) {
	readBlob := func(dst *[]byte) error {
		if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
			return err
		}
		n := int(binary.BigEndian.Uint32(lenBuf[:]))
		if cap(*dst) < n {
			*dst = make([]byte, n)
		} else {
			*dst = (*dst)[:n]
		}
		_, err := io.ReadFull(r, *dst)
		return err
	}
	if err := readBlob(key); err != nil {
		if errors.Is(err, io.EOF) {
			return false, nil
		}
		return false, err
	}
	if err := readBlob(val); err != nil {
		return false, fmt.Errorf("bulk sync import: truncated spill entry: %w", err)
	}
	return true, nil
}

// mergeSortedSpillChunksToSST heap-merges the sorted chunk files into a
// single SST. Duplicate keys are corruption (the importer requires
// globally unique tuples) and fail the merge.
func mergeSortedSpillChunksToSST(ctx context.Context, sstPath, name string, chunks []string) error {
	readers := make([]*os.File, 0, len(chunks))
	defer func() {
		for _, r := range readers {
			_ = r.Close()
		}
	}()
	bufReaders := make([]*bufio.Reader, len(chunks))
	keyBufs := make([][]byte, len(chunks))
	valBufs := make([][]byte, len(chunks))
	h := &spillChunkHeap{}
	var lenBuf [4]byte
	for i, chunk := range chunks {
		f, err := os.Open(chunk) // #nosec G304 - staged under the import's MkdirTemp dir.
		if err != nil {
			return err
		}
		readers = append(readers, f)
		bufReaders[i] = bufio.NewReaderSize(f, bulkSpillBufferSize)
		ok, err := readSpillEntry(bufReaders[i], &keyBufs[i], &valBufs[i], &lenBuf)
		if err != nil {
			return err
		}
		if ok {
			h.push(spillChunkItem{chunkIdx: i, key: keyBufs[i], val: valBufs[i]})
		}
	}

	writer, err := newBulkSSTWriter(filepath.Dir(sstPath), name)
	if err != nil {
		return err
	}
	success := false
	defer func() {
		_ = writer.finish()
		if !success {
			_ = os.Remove(sstPath)
		}
	}()
	var last []byte
	for len(*h) > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		item := h.pop()
		if bytes.Equal(item.key, last) {
			return fmt.Errorf("%w: bucket %s key %x", errBulkImportDuplicateKey, name, item.key)
		}
		var v []byte
		if len(item.val) > 0 {
			v = item.val
		}
		if err := writer.add(item.key, v); err != nil {
			return err
		}
		last = append(last[:0], item.key...)
		ok, err := readSpillEntry(bufReaders[item.chunkIdx], &keyBufs[item.chunkIdx], &valBufs[item.chunkIdx], &lenBuf)
		if err != nil {
			return err
		}
		if ok {
			h.push(spillChunkItem{chunkIdx: item.chunkIdx, key: keyBufs[item.chunkIdx], val: valBufs[item.chunkIdx]})
		}
	}
	if err := writer.finish(); err != nil {
		return err
	}
	success = true
	return nil
}
