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
	"sort"
	"sync"
	"time"

	cpebble "github.com/cockroachdb/pebble/v2"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	enginepkg "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
)

const (
	defaultKWayFanIn       = 50
	runFileBufferSize      = 1 << 20
	indexSortChunkKeys     = 100000
	runHeaderSize          = 20
	runBucketResourceTypes = 0
	runBucketResources     = 1
	runBucketEntitlements  = 2
	runBucketGrants        = 3
	runBucketCount         = 4
)

// SourceFile identifies a compactable Pebble c1z input and the sync within it.
type SourceFile struct {
	Path   string
	SyncID string
	Stats  *reader_v2.SyncStats
	Engine *enginepkg.Engine
	DBDir  string
	// DecoderPool, when set, supplies the v3 payload decoder for every
	// open of this source (the compactor scopes one pool to the whole
	// merge). Nil is fine: each open constructs a one-shot decoder.
	DecoderPool *dotc1z.EnvelopeDecoderPool
}

// MergeFilesInto folds source syncs into destSyncID using a bounded K-way
// semantic merge and final SST ingestion. It materializes resource_types,
// resources, entitlements, and grants plus all derived indexes. Assets are not
// copied, matching SQLite compaction behavior.
//
// Algorithm (external merge sort with fan-in cfg.fanIn, default 50):
//
//   - ≤ fanIn sources merge DIRECTLY into the dest Pebble — one pass,
//     no run files (mergeSourceChunkToPebble).
//   - Otherwise sources are processed in fanIn-sized chunks, each
//     chunk's buckets streamed into one sorted run file
//     (buildChunkRunFileFromSources); run files are then merged fanIn
//     at a time per round (mergeRunFileGroup) until ≤ fanIn remain,
//     and the final generation is materialized into SSTs and ingested
//     (materializeRunFilesToPebble). Each extra round re-reads and
//     re-writes the full dataset, so fan-in should comfortably exceed
//     the expected source count.
//   - Winner rule: newest discovered_at per primary key, ties keep the
//     newer source (runRecordIsNewer) — same rule as the overlay and
//     sqlite compactors. Sources are unpacked per chunk and their
//     directories removed asynchronously when the chunk closes, so
//     peak disk is O(fanIn) unpacked sources, not O(len(sources)).
//
// Memory stays bounded regardless of input count or size — there is no
// in-memory dedup state — at the cost of writing and re-reading every
// record through run files. The overlay merge routes its oversized
// buckets here; see the package doc for how the strategies divide up.
//
// On success it returns the dest sync's stats record, accumulated at the
// final winner-dedupe sites (never at run-file build time — run files keep
// duplicate keys across chunks until the last merge round), so the caller
// can persist the stats sidecar without re-scanning the output.
func MergeFilesInto(ctx context.Context, dest *enginepkg.Engine, sources []SourceFile, destSyncID string, tmpDir string, opts ...KWayOption) (*v3.SyncStatsRecord, error) {
	return mergeBucketsInto(ctx, dest, sources, destSyncID, tmpDir, allBuckets(), opts...)
}

func mergeBucketsInto(ctx context.Context, dest *enginepkg.Engine, sources []SourceFile, destSyncID string, tmpDir string, buckets []bucketSpec, opts ...KWayOption) (*v3.SyncStatsRecord, error) {
	if dest == nil {
		return nil, fmt.Errorf("kway merge: dest engine is nil")
	}
	if destSyncID == "" {
		return nil, fmt.Errorf("kway merge: destSyncID is required")
	}
	cfg := kWayConfig{fanIn: defaultKWayFanIn}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.fanIn < 2 {
		cfg.fanIn = defaultKWayFanIn
	}
	l := ctxzap.Extract(ctx)
	mergeStart := time.Now()
	direct := len(sources) <= cfg.fanIn
	l.Info("pebble kway merge: start",
		zap.Int("sources", len(sources)),
		zap.Int("fan_in", cfg.fanIn),
		zap.Bool("direct", direct),
		zap.Strings("buckets", bucketNames(buckets)),
	)
	stats := newMergeStatsAccumulator()
	rm := &asyncRemover{}
	defer rm.wait()
	if direct {
		if err := mergeSourceChunkToPebble(ctx, dest, tmpDir, sources, 0, buckets, stats, rm); err != nil {
			return nil, err
		}
		l.Info("pebble kway merge: done",
			zap.Duration("elapsed", time.Since(mergeStart)),
			zap.Int("merge_rounds", 0),
		)
		return stats.record(), nil
	}
	roundFiles := make([]runFile, 0, (len(sources)+cfg.fanIn-1)/cfg.fanIn)
	for start := 0; start < len(sources); start += cfg.fanIn {
		end := min(start+cfg.fanIn, len(sources))
		run, err := buildChunkRunFileFromSources(ctx, tmpDir, sources[start:end], start, fmt.Sprintf("initial-%04d", len(roundFiles)), buckets, rm)
		if err != nil {
			removeRunFiles(roundFiles)
			return nil, err
		}
		roundFiles = append(roundFiles, run)
	}
	defer func() {
		removeRunFiles(roundFiles)
	}()

	round := 1
	for len(roundFiles) > cfg.fanIn {
		next := make([]runFile, 0, (len(roundFiles)+cfg.fanIn-1)/cfg.fanIn)
		for start := 0; start < len(roundFiles); start += cfg.fanIn {
			end := min(start+cfg.fanIn, len(roundFiles))
			merged, err := mergeRunFileGroup(ctx, tmpDir, roundFiles[start:end], fmt.Sprintf("%02d-%04d", round, len(next)), buckets)
			if err != nil {
				removeRunFiles(next)
				return nil, err
			}
			next = append(next, merged)
			// A group's inputs are fully consumed by its merge; delete
			// them now so run-file disk overhead is bounded by one
			// group, not one whole round. The deferred removeRunFiles
			// backstop tolerates the already-deleted paths.
			removeRunFiles(roundFiles[start:end])
		}
		roundFiles = next
		round++
	}
	if err := materializeRunFilesToPebble(ctx, dest, tmpDir, roundFiles, buckets, stats); err != nil {
		return nil, err
	}
	// merge_rounds counts intermediate run-file generations beyond the
	// initial chunk round — nonzero values mean fan_in is small relative
	// to the source count and each extra round re-reads + re-writes the
	// full dataset through run files.
	l.Info("pebble kway merge: done",
		zap.Duration("elapsed", time.Since(mergeStart)),
		zap.Int("merge_rounds", round-1),
	)
	return stats.record(), nil
}

type kWayConfig struct {
	fanIn int
}

type KWayOption func(*kWayConfig)

func WithFanIn(fanIn int) KWayOption {
	return func(c *kWayConfig) {
		c.fanIn = fanIn
	}
}

type runSection struct {
	offset int64
	length int64
}

type runFile struct {
	path     string
	sections [runBucketCount]runSection
}

func removeRunFiles(files []runFile) {
	for _, file := range files {
		_ = os.Remove(file.path)
	}
}

type sourceHandle struct {
	rank   int
	syncID string
	engine *enginepkg.Engine
	close  func() error
	// stats carries the SourceFile's cached per-bucket counts (from the
	// envelope manifest / stats sidecar) when available. Consulted by
	// the overlay's whole-source size gate.
	stats *reader_v2.SyncStats
}

// sourceChunk owns one fan-in window of open sources. Path-based
// sources are unpacked under a single chunk-scoped parent directory;
// close() closes every engine and removes that directory in one
// recursive delete. Both merge algorithms are chunk-major over sources
// (a source is never revisited after its chunk closes), so this bounds
// peak disk at O(fan-in) unpacked sources instead of O(total). The
// compactor's final tmp-root cleanup remains the backstop for anything
// a failed chunk leaves behind.
type sourceChunk struct {
	handles []sourceHandle
	dir     string // chunk-scoped unpack dir; empty when no Path sources
}

func (c *sourceChunk) close() {
	closeSourceHandles(c.handles)
	if c.dir != "" {
		_ = os.RemoveAll(c.dir)
	}
}

// closeAsync closes the chunk's engines synchronously (file locks and
// descriptors must release before deletion) and hands the chunk
// directory to rm, so the unlink storm of an unpacked Pebble tree
// happens off the merge's critical path. Benchmarks on APFS showed
// in-loop chunk deletion costing a measurable slice of merge time.
func (c *sourceChunk) closeAsync(rm *asyncRemover) {
	closeSourceHandles(c.handles)
	rm.remove(c.dir)
}

// asyncRemover deletes directory trees on background goroutines.
// Owners must call wait() before returning so no deletion is still in
// flight when the compactor's tmp-root cleanup (the crash backstop)
// sweeps the same parent. Deletion errors are ignored — the tmp-root
// sweep catches anything left behind.
type asyncRemover struct {
	wg sync.WaitGroup
}

func (d *asyncRemover) remove(path string) {
	if path == "" {
		return
	}
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		_ = os.RemoveAll(path)
	}()
}

func (d *asyncRemover) wait() { d.wg.Wait() }

func openSourceChunk(ctx context.Context, tmpDir string, sources []SourceFile, baseRank int) (*sourceChunk, error) {
	chunk := &sourceChunk{handles: make([]sourceHandle, 0, len(sources))}
	cleanupOnError := func(err error) error {
		chunk.close()
		return err
	}
	for i, source := range sources {
		if source.Engine != nil {
			chunk.handles = append(chunk.handles, sourceHandle{
				rank:   baseRank + i,
				syncID: source.SyncID,
				engine: source.Engine,
				stats:  source.Stats,
			})
			continue
		}
		if source.DBDir != "" {
			// Caller-owned directory: reopen in place, never delete.
			eng, err := enginepkg.Open(ctx, source.DBDir, enginepkg.WithReadOnly(true))
			if err != nil {
				return nil, cleanupOnError(err)
			}
			chunk.handles = append(chunk.handles, sourceHandle{
				rank:   baseRank + i,
				syncID: source.SyncID,
				engine: eng,
				close:  eng.Close,
				stats:  source.Stats,
			})
			continue
		}
		if chunk.dir == "" {
			dir, err := os.MkdirTemp(tmpDir, "kway-chunk-")
			if err != nil {
				return nil, cleanupOnError(err)
			}
			chunk.dir = dir
		}
		w, err := dotc1z.NewStore(ctx, source.Path, dotc1z.WithReadOnly(true), dotc1z.WithTmpDir(chunk.dir), dotc1z.WithDecoderPool(source.DecoderPool))
		if err != nil {
			return nil, cleanupOnError(err)
		}
		eng, ok := enginepkg.AsEngine(w)
		if !ok {
			_ = w.Close(ctx)
			return nil, cleanupOnError(fmt.Errorf("kway merge: input is not pebble: %s", source.Path))
		}
		store := w
		chunk.handles = append(chunk.handles, sourceHandle{
			rank:   baseRank + i,
			syncID: source.SyncID,
			engine: eng,
			// CloseEngineOnly: the unpacked directory lives under
			// chunk.dir and is removed wholesale by sourceChunk.close().
			close: func() error { return enginepkg.CloseEngineOnly(store) },
			stats: source.Stats,
		})
	}
	return chunk, nil
}

func closeSourceHandles(handles []sourceHandle) {
	for _, handle := range handles {
		if handle.close != nil {
			_ = handle.close()
		}
	}
}

func buildChunkRunFileFromSources(
	ctx context.Context,
	tmpDir string,
	sources []SourceFile,
	baseRank int,
	name string,
	buckets []bucketSpec,
	rm *asyncRemover,
) (runFile, error) {
	chunk, err := openSourceChunk(ctx, tmpDir, sources, baseRank)
	if err != nil {
		return runFile{}, err
	}
	defer chunk.closeAsync(rm)
	return buildChunkRunFileFromHandles(ctx, tmpDir, chunk.handles, name, buckets)
}

func buildChunkRunFileFromHandles(
	ctx context.Context,
	tmpDir string,
	handles []sourceHandle,
	name string,
	buckets []bucketSpec,
) (runFile, error) {
	path := filepath.Join(tmpDir, "kway-run-"+name+".bin")
	f, err := os.Create(path)
	if err != nil {
		return runFile{}, err
	}
	cw := &countingWriter{w: bufio.NewWriterSize(f, runFileBufferSize)}
	success := false
	defer func() {
		_ = f.Close()
		if !success {
			_ = os.Remove(path)
		}
	}()
	run := runFile{path: path}
	for _, bucket := range buckets {
		if err := mergeSourceBucketToRunSection(ctx, cw, handles, bucket, &run); err != nil {
			return runFile{}, err
		}
	}
	if err := cw.Flush(); err != nil {
		return runFile{}, err
	}
	if err := f.Close(); err != nil {
		return runFile{}, err
	}
	success = true
	return run, nil
}

func mergeSourceChunkToPebble(
	ctx context.Context,
	dest *enginepkg.Engine,
	tmpDir string,
	sources []SourceFile,
	baseRank int,
	buckets []bucketSpec,
	stats *mergeStatsAccumulator,
	rm *asyncRemover,
) error {
	chunk, err := openSourceChunk(ctx, tmpDir, sources, baseRank)
	if err != nil {
		return err
	}
	defer chunk.closeAsync(rm)
	for _, bucket := range buckets {
		if err := materializeSourceBucketToPebble(ctx, dest, tmpDir, chunk.handles, bucket, stats); err != nil {
			return err
		}
	}
	return nil
}

type countingWriter struct {
	w *bufio.Writer
	n int64
	// hdr is the run-record header scratch. It lives here (one heap
	// allocation per run file) instead of on writeRunRecord's frame
	// because a stack array passed to an io.Writer interface call
	// escapes — that was one allocation per record written.
	hdr [runHeaderSize]byte
}

func (w *countingWriter) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	w.n += int64(n)
	return n, err
}

func (w *countingWriter) Flush() error {
	return w.w.Flush()
}

type bucketSpec struct {
	id        int
	name      string
	newRecord func() proto.Message
	// syncRange returns the half-open Pebble key range containing every
	// primary record for this bucket. The file holds one sync and keys
	// carry no sync_id, so this is the bucket's whole range.
	syncRange       func() ([]byte, []byte)
	key             func(proto.Message) string
	ts              func(proto.Message) *timestamppb.Timestamp
	indexKeys       func(proto.Message) [][]byte
	forEachIndexKey func(proto.Message, func([]byte) error) error
}

func allBuckets() []bucketSpec {
	return []bucketSpec{resourceTypeBucket(), resourceBucket(), entitlementBucket(), grantBucket()}
}

func resourceTypeBucket() bucketSpec {
	return bucketSpec{
		id:        runBucketResourceTypes,
		name:      "resource_types",
		newRecord: func() proto.Message { return &v3.ResourceTypeRecord{} },
		syncRange: func() ([]byte, []byte) {
			return enginepkg.ResourceTypeLowerBound(), enginepkg.ResourceTypeUpperBound()
		},
		key: func(m proto.Message) string { return m.(*v3.ResourceTypeRecord).GetExternalId() },
		ts:  func(m proto.Message) *timestamppb.Timestamp { return m.(*v3.ResourceTypeRecord).GetDiscoveredAt() },
	}
}

func resourceBucket() bucketSpec {
	return bucketSpec{
		id:        runBucketResources,
		name:      "resources",
		newRecord: func() proto.Message { return &v3.ResourceRecord{} },
		syncRange: func() ([]byte, []byte) {
			return enginepkg.ResourceLowerBound(), enginepkg.ResourceUpperBound()
		},
		key: func(m proto.Message) string {
			r := m.(*v3.ResourceRecord)
			return r.GetResourceTypeId() + "\x00" + r.GetResourceId()
		},
		ts: func(m proto.Message) *timestamppb.Timestamp { return m.(*v3.ResourceRecord).GetDiscoveredAt() },
		indexKeys: func(m proto.Message) [][]byte {
			return enginepkg.ResourceIndexKeys(m.(*v3.ResourceRecord))
		},
		forEachIndexKey: func(m proto.Message, yield func([]byte) error) error {
			return enginepkg.ForEachResourceIndexKey(m.(*v3.ResourceRecord), yield)
		},
	}
}

func entitlementBucket() bucketSpec {
	return bucketSpec{
		id:        runBucketEntitlements,
		name:      "entitlements",
		newRecord: func() proto.Message { return &v3.EntitlementRecord{} },
		syncRange: func() ([]byte, []byte) {
			return enginepkg.EntitlementLowerBound(), enginepkg.EntitlementUpperBound()
		},
		key: func(m proto.Message) string { return m.(*v3.EntitlementRecord).GetExternalId() },
		ts:  func(m proto.Message) *timestamppb.Timestamp { return m.(*v3.EntitlementRecord).GetDiscoveredAt() },
		indexKeys: func(m proto.Message) [][]byte {
			return enginepkg.EntitlementIndexKeys(m.(*v3.EntitlementRecord))
		},
		forEachIndexKey: func(m proto.Message, yield func([]byte) error) error {
			return enginepkg.ForEachEntitlementIndexKey(m.(*v3.EntitlementRecord), yield)
		},
	}
}

func grantBucket() bucketSpec {
	return bucketSpec{
		id:        runBucketGrants,
		name:      "grants",
		newRecord: func() proto.Message { return &v3.GrantRecord{} },
		syncRange: func() ([]byte, []byte) {
			return enginepkg.GrantLowerBound(), enginepkg.GrantUpperBound()
		},
		key: func(m proto.Message) string { return m.(*v3.GrantRecord).GetExternalId() },
		ts:  func(m proto.Message) *timestamppb.Timestamp { return m.(*v3.GrantRecord).GetDiscoveredAt() },
		indexKeys: func(m proto.Message) [][]byte {
			return enginepkg.GrantIndexKeys(m.(*v3.GrantRecord))
		},
		forEachIndexKey: func(m proto.Message, yield func([]byte) error) error {
			return enginepkg.ForEachGrantIndexKey(m.(*v3.GrantRecord), yield)
		},
	}
}

type directStream struct {
	sourceRank int
	iter       *cpebble.Iterator
	bucket     bucketSpec

	// keyBuf/valBuf back the runRecord returned by next(). The merge
	// loops hold at most one live record per stream (in the heap or
	// the current dedupe group), and a stream's buffers are only
	// overwritten by that stream's own next(), which callers invoke
	// only after the record has been consumed (written to the run
	// file / SST and index-emitted). Reusing them removes the two
	// per-record allocations the old copy-out version paid.
	keyBuf []byte
	valBuf []byte
}

func (s *directStream) close() {
	if s.iter != nil {
		_ = s.iter.Close()
	}
}

func (s *directStream) next() (runRecord, bool, error) {
	if !s.iter.Valid() {
		if err := s.iter.Error(); err != nil {
			return runRecord{}, false, err
		}
		return runRecord{}, false, nil
	}
	// K-way's source stream is raw: source keys and values are already
	// in final dest form (v3 keys and values carry no sync_id), so the
	// stream copies both verbatim and shallow-scans only discovered_at.
	// Indexes are generated once for final winners instead of being
	// carried through every run-file round; on non-skewed syncs=500,
	// carrying index blobs accounted for tens of millions of allocations.
	rank, err := checkedUint32(s.sourceRank, "source rank")
	if err != nil {
		return runRecord{}, false, err
	}
	tsNanos, err := discoveredAtNanosFromRaw(s.bucket, s.iter.Value())
	if err != nil {
		return runRecord{}, false, err
	}
	s.keyBuf = append(s.keyBuf[:0], s.iter.Key()...)
	s.valBuf = append(s.valBuf[:0], s.iter.Value()...)
	item := runRecord{
		key:        s.keyBuf,
		value:      s.valBuf,
		tsNanos:    tsNanos,
		sourceRank: rank,
	}
	s.iter.Next()
	return item, true, nil
}

func mergeSourceBucketToRunSection(
	ctx context.Context,
	w *countingWriter,
	sources []sourceHandle,
	bucket bucketSpec,
	run *runFile,
) error {
	start := w.n
	streams := make([]*directStream, 0, len(sources))
	defer func() {
		for _, stream := range streams {
			stream.close()
		}
	}()
	lower, upper := bucket.syncRange()
	h := &directHeap{}
	for i, source := range sources {
		iter, err := source.engine.DB().NewIter(&cpebble.IterOptions{LowerBound: lower, UpperBound: upper})
		if err != nil {
			return err
		}
		iter.First()
		stream := &directStream{
			sourceRank: source.rank,
			iter:       iter,
			bucket:     bucket,
		}
		streams = append(streams, stream)
		rec, ok, err := stream.next()
		if err != nil {
			return err
		}
		if ok {
			h.push(directItem{streamIdx: i, rec: rec})
		}
	}
	for h.Len() > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		first := h.pop()
		winner := first.rec
		group := []directItem{first}
		for h.Len() > 0 && bytes.Equal((*h)[0].rec.key, first.rec.key) {
			group = append(group, h.pop())
		}
		for _, item := range group[1:] {
			if runRecordIsNewer(item.rec, winner) {
				winner = item.rec
			}
		}
		if err := writeRunRecord(w, winner); err != nil {
			return err
		}
		for _, item := range group {
			next, ok, err := streams[item.streamIdx].next()
			if err != nil {
				return err
			}
			if ok {
				h.push(directItem{streamIdx: item.streamIdx, rec: next})
			}
		}
	}
	run.sections[bucket.id] = runSection{offset: start, length: w.n - start}
	return nil
}

func materializeSourceBucketToPebble(
	ctx context.Context,
	dest *enginepkg.Engine,
	tmpDir string,
	sources []sourceHandle,
	bucket bucketSpec,
	stats *mergeStatsAccumulator,
) error {
	primaryPath := filepath.Join(tmpDir, fmt.Sprintf("kway-direct-%s-primary.sst", bucket.name))
	primaryWriter, err := newSSTBuilder(primaryPath)
	if err != nil {
		return err
	}
	primarySuccess := false
	defer func() {
		if !primarySuccess {
			_ = primaryWriter.Close()
			_ = os.Remove(primaryPath)
		}
	}()
	indexWriters, err := newIndexWriterSet(tmpDir, bucket.name+"-direct-index")
	if err != nil {
		return err
	}
	defer indexWriters.cleanup()

	streams := make([]*directStream, 0, len(sources))
	defer func() {
		for _, stream := range streams {
			stream.close()
		}
	}()
	lower, upper := bucket.syncRange()
	h := &directHeap{}
	for i, source := range sources {
		iter, err := source.engine.DB().NewIter(&cpebble.IterOptions{LowerBound: lower, UpperBound: upper})
		if err != nil {
			return err
		}
		iter.First()
		stream := &directStream{
			sourceRank: source.rank,
			iter:       iter,
			bucket:     bucket,
		}
		streams = append(streams, stream)
		rec, ok, err := stream.next()
		if err != nil {
			return err
		}
		if ok {
			h.push(directItem{streamIdx: i, rec: rec})
		}
	}

	var lastPrimaryKey []byte
	var scratch rawIndexScratch
	// Hoisted bound-method value: creating it inside the loop would
	// allocate a closure per record.
	emitIndexKey := indexWriters.writeKey
	for h.Len() > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		first := h.pop()
		winner := first.rec
		group := []directItem{first}
		for h.Len() > 0 && bytes.Equal((*h)[0].rec.key, first.rec.key) {
			group = append(group, h.pop())
		}
		for _, item := range group[1:] {
			if runRecordIsNewer(item.rec, winner) {
				winner = item.rec
			}
		}
		if !bytes.Equal(winner.key, lastPrimaryKey) {
			if err := primaryWriter.Set(winner.key, winner.value); err != nil {
				return err
			}
			if err := stats.countWinner(bucket, winner.key, lower, winner.value); err != nil {
				return err
			}
			lastPrimaryKey = append(lastPrimaryKey[:0], winner.key...)
			// stats is nil here: counting already happened via countWinner
			// above (the winner-only contract on forEachIndexKeyFromRaw's
			// stats parameter is satisfied either way, but counting twice
			// would double the totals).
			if err := forEachIndexKeyFromRaw(bucket, winner.key, lower, winner.value, &scratch, nil, emitIndexKey); err != nil {
				return err
			}
		}
		for _, item := range group {
			next, ok, err := streams[item.streamIdx].next()
			if err != nil {
				return err
			}
			if ok {
				h.push(directItem{streamIdx: item.streamIdx, rec: next})
			}
		}
	}
	if err := primaryWriter.Close(); err != nil {
		return err
	}
	primarySuccess = true
	defer func() { _ = os.Remove(primaryPath) }()
	if err := dest.DB().Ingest(ctx, []string{primaryPath}); err != nil {
		return fmt.Errorf("ingest direct primary %s: %w", bucket.name, err)
	}
	return indexWriters.closeSortAndIngest(ctx, dest)
}

type directItem struct {
	streamIdx int
	rec       runRecord
}

type directHeap []directItem

func (h directHeap) Len() int { return len(h) }
func (h directHeap) less(i, j int) bool {
	cmp := bytes.Compare(h[i].rec.key, h[j].rec.key)
	if cmp == 0 {
		return h[i].rec.sourceRank < h[j].rec.sourceRank
	}
	return cmp < 0
}
func (h directHeap) swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *directHeap) push(item directItem) {
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
func (h *directHeap) pop() directItem {
	old := *h
	item := old[0]
	last := old[len(old)-1]
	*h = old[:len(old)-1]
	if len(*h) == 0 {
		return item
	}
	(*h)[0] = last
	h.down(0)
	return item
}
func (h *directHeap) down(parent int) {
	for {
		left := 2*parent + 1
		if left >= len(*h) {
			return
		}
		child := left
		right := left + 1
		if right < len(*h) && h.less(right, left) {
			child = right
		}
		if !h.less(child, parent) {
			return
		}
		h.swap(parent, child)
		parent = child
	}
}

type runRecord struct {
	key        []byte
	value      []byte
	tsNanos    int64
	sourceRank uint32
}

func writeRunRecord(w *countingWriter, rec runRecord) error {
	ts, err := checkedUint64FromInt64(rec.tsNanos, "discovered_at nanos")
	if err != nil {
		return err
	}
	keyLen, err := checkedUint32(len(rec.key), "run record key length")
	if err != nil {
		return err
	}
	valueLen, err := checkedUint32(len(rec.value), "run record value length")
	if err != nil {
		return err
	}
	binary.BigEndian.PutUint64(w.hdr[0:8], ts)
	binary.BigEndian.PutUint32(w.hdr[8:12], rec.sourceRank)
	binary.BigEndian.PutUint32(w.hdr[12:16], keyLen)
	binary.BigEndian.PutUint32(w.hdr[16:20], valueLen)
	if _, err := w.Write(w.hdr[:]); err != nil {
		return err
	}
	if _, err := w.Write(rec.key); err != nil {
		return err
	}
	if _, err := w.Write(rec.value); err != nil {
		return err
	}
	return nil
}

// readRunRecordInto reads one run record. hdr is caller-owned scratch:
// a function-local array would escape through the io.Reader interface
// call and cost one heap allocation per record read.
func readRunRecordInto(r io.Reader, rec *runRecord, hdr *[runHeaderSize]byte) (bool, error) {
	_, err := io.ReadFull(r, hdr[:])
	if err != nil {
		if errors.Is(err, io.EOF) {
			return false, nil
		}
		return false, err
	}
	keyLen := int(binary.BigEndian.Uint32(hdr[12:16]))
	valueLen := int(binary.BigEndian.Uint32(hdr[16:20]))
	ts, err := checkedInt64FromUint64(binary.BigEndian.Uint64(hdr[0:8]), "discovered_at nanos")
	if err != nil {
		return false, err
	}
	rec.tsNanos = ts
	rec.sourceRank = binary.BigEndian.Uint32(hdr[8:12])
	rec.key = growBytes(rec.key, keyLen)
	rec.value = growBytes(rec.value, valueLen)
	if _, err := io.ReadFull(r, rec.key); err != nil {
		return false, err
	}
	if _, err := io.ReadFull(r, rec.value); err != nil {
		return false, err
	}
	return true, nil
}

func growBytes(b []byte, n int) []byte {
	if cap(b) < n {
		return make([]byte, n)
	}
	return b[:n]
}

func checkedUint32(n int, label string) (uint32, error) {
	if n < 0 || n > math.MaxUint32 {
		return 0, fmt.Errorf("%s exceeds uint32: %d", label, n)
	}
	return uint32(n), nil
}

func checkedUint64FromInt64(n int64, label string) (uint64, error) {
	if n < 0 {
		return 0, fmt.Errorf("%s must not be negative: %d", label, n)
	}
	return uint64(n), nil
}

func checkedInt64FromUint64(n uint64, label string) (int64, error) {
	if n > math.MaxInt64 {
		return 0, fmt.Errorf("%s exceeds int64: %d", label, n)
	}
	return int64(n), nil
}

func runRecordIsNewer(incoming, existing runRecord) bool {
	if incoming.tsNanos != existing.tsNanos {
		return incoming.tsNanos > existing.tsNanos
	}
	return incoming.sourceRank < existing.sourceRank
}

func discoveredAtNanosFromRaw(bucket bucketSpec, value []byte) (int64, error) {
	field := discoveredAtFieldNumber(bucket)
	if field == 0 {
		return 0, nil
	}
	for len(value) > 0 {
		num, typ, n := protowire.ConsumeTag(value)
		if n < 0 {
			return 0, protowire.ParseError(n)
		}
		value = value[n:]
		if num != field {
			n = protowire.ConsumeFieldValue(num, typ, value)
			if n < 0 {
				return 0, protowire.ParseError(n)
			}
			value = value[n:]
			continue
		}
		if typ != protowire.BytesType {
			return 0, fmt.Errorf("kway merge: discovered_at has wire type %v", typ)
		}
		ts, n := protowire.ConsumeBytes(value)
		if n < 0 {
			return 0, protowire.ParseError(n)
		}
		return timestampBytesNanosFromRaw(ts)
	}
	return 0, nil
}

func discoveredAtFieldNumber(bucket bucketSpec) protowire.Number {
	switch bucket.id {
	case runBucketResourceTypes:
		return 6
	case runBucketResources, runBucketEntitlements:
		return 8
	case runBucketGrants:
		return 5
	default:
		return 0
	}
}

func timestampBytesNanosFromRaw(value []byte) (int64, error) {
	var seconds int64
	var nanos int32
	for len(value) > 0 {
		num, typ, n := protowire.ConsumeTag(value)
		if n < 0 {
			return 0, protowire.ParseError(n)
		}
		value = value[n:]
		switch num {
		case 1:
			if typ != protowire.VarintType {
				return 0, fmt.Errorf("kway merge: timestamp seconds has wire type %v", typ)
			}
			v, n := protowire.ConsumeVarint(value)
			if n < 0 {
				return 0, protowire.ParseError(n)
			}
			checked, err := checkedInt64FromUint64(v, "timestamp seconds")
			if err != nil {
				return 0, err
			}
			seconds = checked
			value = value[n:]
		case 2:
			if typ != protowire.VarintType {
				return 0, fmt.Errorf("kway merge: timestamp nanos has wire type %v", typ)
			}
			v, n := protowire.ConsumeVarint(value)
			if n < 0 {
				return 0, protowire.ParseError(n)
			}
			if v > math.MaxInt32 {
				return 0, fmt.Errorf("timestamp nanos exceeds int32: %d", v)
			}
			nanos = int32(v)
			value = value[n:]
		default:
			n = protowire.ConsumeFieldValue(num, typ, value)
			if n < 0 {
				return 0, protowire.ParseError(n)
			}
			value = value[n:]
		}
	}
	if seconds > math.MaxInt64/int64(time.Second) || seconds < math.MinInt64/int64(time.Second) {
		return 0, fmt.Errorf("timestamp seconds overflow: %d", seconds)
	}
	return seconds*int64(time.Second) + int64(nanos), nil
}

func indexID(key []byte) (byte, bool) {
	if len(key) < 3 {
		return 0, false
	}
	return key[2], true
}

func mergeRunFileGroup(ctx context.Context, tmpDir string, inputs []runFile, name string, buckets []bucketSpec) (runFile, error) {
	outPath := filepath.Join(tmpDir, "kway-run-merge-"+name+".bin")
	out, err := os.Create(outPath)
	if err != nil {
		return runFile{}, err
	}
	cw := &countingWriter{w: bufio.NewWriterSize(out, runFileBufferSize)}
	success := false
	defer func() {
		_ = out.Close()
		if !success {
			_ = os.Remove(outPath)
		}
	}()
	run := runFile{path: outPath}
	for _, bucket := range buckets {
		if err := mergeRunBucketSection(ctx, cw, inputs, bucket, &run); err != nil {
			return runFile{}, err
		}
	}
	if err := cw.Flush(); err != nil {
		return runFile{}, err
	}
	if err := out.Close(); err != nil {
		return runFile{}, err
	}
	success = true
	return run, nil
}

type runHeapItem struct {
	readerIdx int
	rec       runRecord
}

type runRecordHeap []runHeapItem

func (h runRecordHeap) Len() int { return len(h) }
func (h runRecordHeap) less(i, j int) bool {
	cmp := bytes.Compare(h[i].rec.key, h[j].rec.key)
	if cmp == 0 {
		return h[i].readerIdx < h[j].readerIdx
	}
	return cmp < 0
}
func (h runRecordHeap) swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *runRecordHeap) push(item runHeapItem) {
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
func (h *runRecordHeap) pop() runHeapItem {
	old := *h
	item := old[0]
	last := old[len(old)-1]
	*h = old[:len(old)-1]
	if len(*h) == 0 {
		return item
	}
	(*h)[0] = last
	h.down(0)
	return item
}
func (h *runRecordHeap) down(parent int) {
	for {
		left := 2*parent + 1
		if left >= len(*h) {
			return
		}
		child := left
		right := left + 1
		if right < len(*h) && h.less(right, left) {
			child = right
		}
		if !h.less(child, parent) {
			return
		}
		h.swap(parent, child)
		parent = child
	}
}

func mergeRunBucketSection(ctx context.Context, w *countingWriter, inputs []runFile, bucket bucketSpec, run *runFile) error {
	start := w.n
	readers := make([]*os.File, 0, len(inputs))
	bufReaders := make([]*bufio.Reader, 0, len(inputs))
	readBufs := make([]runRecord, 0, len(inputs))
	defer func() {
		for _, r := range readers {
			_ = r.Close()
		}
	}()
	h := &runRecordHeap{}
	var hdr [runHeaderSize]byte
	for i, input := range inputs {
		f, err := os.Open(input.path)
		if err != nil {
			return err
		}
		section := input.sections[bucket.id]
		if _, err := f.Seek(section.offset, io.SeekStart); err != nil {
			_ = f.Close()
			return err
		}
		readers = append(readers, f)
		br := bufio.NewReaderSize(io.LimitReader(f, section.length), runFileBufferSize)
		bufReaders = append(bufReaders, br)
		readBufs = append(readBufs, runRecord{})
		ok, err := readRunRecordInto(br, &readBufs[len(readBufs)-1], &hdr)
		if err != nil {
			return err
		}
		if ok {
			h.push(runHeapItem{readerIdx: i, rec: readBufs[len(readBufs)-1]})
		}
	}
	for h.Len() > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		first := h.pop()
		winner := first.rec
		group := []runHeapItem{first}
		for h.Len() > 0 && bytes.Equal((*h)[0].rec.key, first.rec.key) {
			group = append(group, h.pop())
		}
		for _, item := range group[1:] {
			if runRecordIsNewer(item.rec, winner) {
				winner = item.rec
			}
		}
		if err := writeRunRecord(w, winner); err != nil {
			return err
		}
		for _, item := range group {
			ok, err := readRunRecordInto(bufReaders[item.readerIdx], &readBufs[item.readerIdx], &hdr)
			if err != nil {
				return err
			}
			if ok {
				h.push(runHeapItem{readerIdx: item.readerIdx, rec: readBufs[item.readerIdx]})
			}
		}
	}
	run.sections[bucket.id] = runSection{offset: start, length: w.n - start}
	return nil
}

func materializeRunFilesToPebble(
	ctx context.Context,
	dest *enginepkg.Engine,
	tmpDir string,
	inputs []runFile,
	buckets []bucketSpec,
	stats *mergeStatsAccumulator,
) error {
	merged, err := mergeRunFileGroup(ctx, tmpDir, inputs, "final", buckets)
	if err != nil {
		return err
	}
	defer removeRunFiles([]runFile{merged})
	for _, bucket := range buckets {
		if err := materializeRunFileBucket(ctx, dest, tmpDir, merged, bucket, stats); err != nil {
			return err
		}
	}
	return nil
}

func materializeRunFileBucket(ctx context.Context, dest *enginepkg.Engine, tmpDir string, input runFile, bucket bucketSpec, stats *mergeStatsAccumulator) error {
	f, err := os.Open(input.path)
	if err != nil {
		return err
	}
	defer f.Close()
	section := input.sections[bucket.id]
	if _, err := f.Seek(section.offset, io.SeekStart); err != nil {
		return err
	}
	br := bufio.NewReaderSize(io.LimitReader(f, section.length), runFileBufferSize)
	indexWriters, err := newIndexWriterSet(tmpDir, bucket.name+"-index")
	if err != nil {
		return err
	}
	defer indexWriters.cleanup()

	primaryPath := filepath.Join(tmpDir, fmt.Sprintf("kway-%s-primary.sst", bucket.name))
	primaryWriter, err := newSSTBuilder(primaryPath)
	if err != nil {
		return err
	}
	primarySuccess := false
	defer func() {
		if !primarySuccess {
			_ = primaryWriter.Close()
			_ = os.Remove(primaryPath)
		}
	}()
	destLower, _ := bucket.syncRange()
	var lastPrimaryKey []byte
	var rec runRecord
	var hdr [runHeaderSize]byte
	var scratch rawIndexScratch
	emitIndexKey := indexWriters.writeKey
	for {
		ok, err := readRunRecordInto(br, &rec, &hdr)
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if !bytes.Equal(rec.key, lastPrimaryKey) {
			if err := primaryWriter.Set(rec.key, rec.value); err != nil {
				return err
			}
			if err := stats.countWinner(bucket, rec.key, destLower, rec.value); err != nil {
				return err
			}
			lastPrimaryKey = append(lastPrimaryKey[:0], rec.key...)
			// stats is nil: counted via countWinner above.
			if err := forEachIndexKeyFromRaw(bucket, rec.key, destLower, rec.value, &scratch, nil, emitIndexKey); err != nil {
				return err
			}
		}
	}
	if err := primaryWriter.Close(); err != nil {
		return err
	}
	primarySuccess = true
	defer func() { _ = os.Remove(primaryPath) }()
	if err := dest.DB().Ingest(ctx, []string{primaryPath}); err != nil {
		return fmt.Errorf("ingest primary %s: %w", bucket.name, err)
	}
	return indexWriters.closeSortAndIngest(ctx, dest)
}

type indexWriterSet struct {
	writers map[byte]*indexRunWriter
	all     []*indexRunWriter
}

func newIndexWriterSet(tmpDir string, name string) (*indexWriterSet, error) {
	set := &indexWriterSet{writers: map[byte]*indexRunWriter{}}
	for _, idx := range []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07} {
		w, err := newIndexRunWriter(tmpDir, fmt.Sprintf("%s-%02x", name, idx))
		if err != nil {
			set.cleanup()
			return nil, err
		}
		set.writers[idx] = w
		set.all = append(set.all, w)
	}
	return set, nil
}

// writeKey routes one index key to its family writer. It is the emit
// callback for forEachIndexKeyFromRaw — callers hoist the bound-method
// value (emit := set.writeKey) outside their record loops so the
// closure is allocated once, and keys flow straight to the per-family
// buffered writer with no intermediate per-record blob buffers.
func (s *indexWriterSet) writeKey(key []byte) error {
	idx, ok := indexID(key)
	if !ok {
		return nil
	}
	w := s.writers[idx]
	if w == nil {
		return nil
	}
	return w.writeKey(key)
}

func (s *indexWriterSet) closeSortAndIngest(ctx context.Context, dest *enginepkg.Engine) error {
	for _, w := range s.all {
		if err := w.closeSortAndIngest(ctx, dest); err != nil {
			return err
		}
	}
	return nil
}

func (s *indexWriterSet) cleanup() {
	for _, w := range s.all {
		w.cleanup()
	}
}

type indexRunWriter struct {
	path   string
	tmpDir string
	name   string
	file   *os.File
	writer *bufio.Writer
	count  int
	closed bool
	lenBuf [4]byte
}

func newIndexRunWriter(tmpDir string, name string) (*indexRunWriter, error) {
	path := filepath.Join(tmpDir, "kway-index-run-"+name+".bin")
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &indexRunWriter{path: path, tmpDir: tmpDir, name: name, file: f, writer: bufio.NewWriterSize(f, runFileBufferSize)}, nil
}

func (w *indexRunWriter) writeKey(key []byte) error {
	if err := writeLengthPrefixedBytes(w.writer, key, &w.lenBuf); err != nil {
		return err
	}
	w.count++
	return nil
}

func (w *indexRunWriter) close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	if err := w.writer.Flush(); err != nil {
		_ = w.file.Close()
		return err
	}
	return w.file.Close()
}

func (w *indexRunWriter) cleanup() {
	_ = w.close()
	_ = os.Remove(w.path)
}

func (w *indexRunWriter) closeSortAndIngest(ctx context.Context, dest *enginepkg.Engine) error {
	if err := w.close(); err != nil {
		return err
	}
	if w.count == 0 {
		return nil
	}
	chunks, err := sortIndexRunChunks(ctx, w.tmpDir, w.name, w.path)
	if err != nil {
		removePaths(chunks)
		return err
	}
	defer removePaths(chunks)
	sstPath := filepath.Join(w.tmpDir, "kway-index-"+w.name+".sst")
	if err := mergeSortedIndexChunksToSST(ctx, sstPath, chunks); err != nil {
		return err
	}
	defer func() { _ = os.Remove(sstPath) }()
	if err := dest.DB().Ingest(ctx, []string{sstPath}); err != nil {
		return fmt.Errorf("ingest index %s: %w", w.name, err)
	}
	return nil
}

func removePaths(paths []string) {
	for _, p := range paths {
		_ = os.Remove(p)
	}
}

// writeLengthPrefixedBytes / readLengthPrefixedBytes take caller-owned
// lenBuf scratch for the same escape-analysis reason as
// readRunRecordInto: a function-local array passed to an io interface
// call heap-allocates per call.
func writeLengthPrefixedBytes(w io.Writer, b []byte, lenBuf *[4]byte) error {
	n, err := checkedUint32(len(b), "length-prefixed bytes length")
	if err != nil {
		return err
	}
	binary.BigEndian.PutUint32(lenBuf[:], n)
	if _, err := w.Write(lenBuf[:]); err != nil {
		return err
	}
	_, err = w.Write(b)
	return err
}

func readLengthPrefixedBytes(r io.Reader, dst *[]byte, lenBuf *[4]byte) (bool, error) {
	_, err := io.ReadFull(r, lenBuf[:])
	if err != nil {
		if errors.Is(err, io.EOF) {
			return false, nil
		}
		return false, err
	}
	n := int(binary.BigEndian.Uint32(lenBuf[:]))
	*dst = growBytes(*dst, n)
	if _, err := io.ReadFull(r, *dst); err != nil {
		return false, err
	}
	return true, nil
}

// indexKeyView is an (offset, end) view into a sort arena. Sorting
// views over one contiguous buffer instead of [][]byte avoids one heap
// copy per index key and gives the GC a pointer-free chunk to skip.
type indexKeyView struct {
	off int
	end int
}

func sortIndexRunChunks(ctx context.Context, tmpDir string, name string, path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	reader := bufio.NewReaderSize(f, runFileBufferSize)
	var chunks []string
	var arena []byte
	views := make([]indexKeyView, 0, indexSortChunkKeys)
	var lenBuf [4]byte
	for {
		arena = arena[:0]
		views = views[:0]
		for len(views) < indexSortChunkKeys {
			if err := ctx.Err(); err != nil {
				return chunks, err
			}
			ok, err := readLengthPrefixedIntoArena(reader, &arena, &views, &lenBuf)
			if err != nil {
				return chunks, err
			}
			if !ok {
				break
			}
		}
		if len(views) == 0 {
			break
		}
		sort.Slice(views, func(i, j int) bool {
			return bytes.Compare(arena[views[i].off:views[i].end], arena[views[j].off:views[j].end]) < 0
		})
		chunkPath := filepath.Join(tmpDir, fmt.Sprintf("kway-index-%s-chunk-%04d.bin", name, len(chunks)))
		if err := writeSortedIndexChunk(chunkPath, arena, views); err != nil {
			return chunks, err
		}
		chunks = append(chunks, chunkPath)
		if len(views) < indexSortChunkKeys {
			break
		}
	}
	return chunks, nil
}

// readLengthPrefixedIntoArena appends one length-prefixed key from r
// onto arena and records its view. The arena's capacity is retained
// across chunks by the caller, so steady-state reads are
// allocation-free.
func readLengthPrefixedIntoArena(r io.Reader, arena *[]byte, views *[]indexKeyView, lenBuf *[4]byte) (bool, error) {
	_, err := io.ReadFull(r, lenBuf[:])
	if err != nil {
		if errors.Is(err, io.EOF) {
			return false, nil
		}
		return false, err
	}
	n := int(binary.BigEndian.Uint32(lenBuf[:]))
	start := len(*arena)
	*arena = growArenaTail(*arena, n)
	if _, err := io.ReadFull(r, (*arena)[start:start+n]); err != nil {
		return false, err
	}
	*views = append(*views, indexKeyView{off: start, end: start + n})
	return true, nil
}

// growArenaTail extends arena by n bytes, growing capacity
// geometrically so repeated appends amortize to O(1) allocations.
func growArenaTail(arena []byte, n int) []byte {
	cur := len(arena)
	if cap(arena)-cur >= n {
		return arena[:cur+n]
	}
	newCap := max(2*cap(arena), cur+n)
	newCap = max(newCap, runFileBufferSize)
	out := make([]byte, cur+n, newCap)
	copy(out, arena)
	return out
}

func writeSortedIndexChunk(path string, arena []byte, views []indexKeyView) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	writer := bufio.NewWriterSize(f, runFileBufferSize)
	success := false
	defer func() {
		_ = f.Close()
		if !success {
			_ = os.Remove(path)
		}
	}()
	var lenBuf [4]byte
	var last []byte
	for _, v := range views {
		key := arena[v.off:v.end]
		if bytes.Equal(key, last) {
			continue
		}
		if err := writeLengthPrefixedBytes(writer, key, &lenBuf); err != nil {
			return err
		}
		last = key
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

type indexChunkItem struct {
	chunkIdx int
	key      []byte
}

type indexChunkHeap []indexChunkItem

func (h indexChunkHeap) Len() int { return len(h) }
func (h indexChunkHeap) less(i, j int) bool {
	cmp := bytes.Compare(h[i].key, h[j].key)
	if cmp == 0 {
		return h[i].chunkIdx < h[j].chunkIdx
	}
	return cmp < 0
}
func (h indexChunkHeap) swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *indexChunkHeap) push(item indexChunkItem) {
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
func (h *indexChunkHeap) pop() indexChunkItem {
	old := *h
	item := old[0]
	last := old[len(old)-1]
	*h = old[:len(old)-1]
	if len(*h) == 0 {
		return item
	}
	(*h)[0] = last
	h.down(0)
	return item
}
func (h *indexChunkHeap) down(parent int) {
	for {
		left := 2*parent + 1
		if left >= len(*h) {
			return
		}
		child := left
		right := left + 1
		if right < len(*h) && h.less(right, left) {
			child = right
		}
		if !h.less(child, parent) {
			return
		}
		h.swap(parent, child)
		parent = child
	}
}

func mergeSortedIndexChunksToSST(ctx context.Context, sstPath string, chunks []string) error {
	readers := make([]*os.File, 0, len(chunks))
	bufReaders := make([]*bufio.Reader, 0, len(chunks))
	readBufs := make([][]byte, len(chunks))
	defer func() {
		for _, r := range readers {
			_ = r.Close()
		}
	}()
	h := &indexChunkHeap{}
	var lenBuf [4]byte
	for i, chunk := range chunks {
		f, err := os.Open(chunk)
		if err != nil {
			return err
		}
		readers = append(readers, f)
		br := bufio.NewReaderSize(f, runFileBufferSize)
		bufReaders = append(bufReaders, br)
		ok, err := readLengthPrefixedBytes(br, &readBufs[i], &lenBuf)
		if err != nil {
			return err
		}
		if ok {
			h.push(indexChunkItem{chunkIdx: i, key: readBufs[i]})
		}
	}
	writer, err := newSSTBuilder(sstPath)
	if err != nil {
		return err
	}
	success := false
	defer func() {
		if !success {
			_ = writer.Close()
			_ = os.Remove(sstPath)
		}
	}()
	var last []byte
	for h.Len() > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		item := h.pop()
		if !bytes.Equal(item.key, last) {
			if err := writer.Set(item.key, nil); err != nil {
				return err
			}
			last = append(last[:0], item.key...)
		}
		ok, err := readLengthPrefixedBytes(bufReaders[item.chunkIdx], &readBufs[item.chunkIdx], &lenBuf)
		if err != nil {
			return err
		}
		if ok {
			h.push(indexChunkItem{chunkIdx: item.chunkIdx, key: readBufs[item.chunkIdx]})
		}
	}
	if err := writer.Close(); err != nil {
		return err
	}
	success = true
	return nil
}
