package pebble

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/maphash"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"

	cpebble "github.com/cockroachdb/pebble/v2"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protowire"

	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	enginepkg "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/codec"
)

// defaultOverlayRecordChunkSize is how many records are buffered per
// raw write batch while streaming winners into the dest. Tunable via
// BATON_EXPERIMENTAL_OVERLAY_CHUNK_SIZE.
const defaultOverlayRecordChunkSize = 32768

// defaultOverlaySeenKeyLimit caps the per-bucket seen-set size (the
// soft limit) for the overlay path; buckets that would exceed it
// degrade to the kway run-file path mid-merge (see the bucket state
// machine in MergeFilesIntoOverlay). Each seen-set entry is a
// 16-byte hash key + 8-byte timestamp (~40B with map overhead), so
// this bounds seen-set memory to ~15MB per bucket. Raised from 250k
// when the hashed set replaced the retained-string set (~3-4x smaller
// per key). Tunable via BATON_EXPERIMENTAL_OVERLAY_SEEN_LIMIT.
const defaultOverlaySeenKeyLimit = 375000

// defaultOverlayBufferFactor scales the soft seen-key limit into the
// hard limit (hard = soft × factor). A source that crosses the soft
// limit mid-scan is allowed to finish inside this buffer (then the
// bucket resumes via kway at the source boundary); crossing the HARD
// limit mid-source restarts the bucket through the blind kway path.
// Overridable via WithOverlayBufferFactor (synccompactor callers:
// WithOverlayBufferFactor on the Compactor).
const defaultOverlayBufferFactor = 1.25

// defaultOverlayGateFraction sets the statless pre-source gate as a
// fraction of the soft limit: when a source has no cached stats and
// the seen set is already at gateFraction × soft, the bucket resumes
// via kway before scanning the source (avoiding a mid-source restart).
// Sources WITH stats use the exact prediction seen+count > hard
// instead. Overridable via WithOverlayGateFraction (synccompactor
// callers: WithOverlayGateFraction on the Compactor).
const defaultOverlayGateFraction = 0.9

// overlayConfig carries the overlay merge tunables. The defaults are
// the production values; explicit options exist for benchmarks and
// tests.
type overlayConfig struct {
	seenKeyLimit    int64
	recordChunkSize int
	bufferFactor    float64
	gateFraction    float64
	fanIn           int
}

func defaultOverlayConfig() overlayConfig {
	return overlayConfig{
		seenKeyLimit:    defaultOverlaySeenKeyLimit,
		recordChunkSize: defaultOverlayRecordChunkSize,
		bufferFactor:    defaultOverlayBufferFactor,
		gateFraction:    defaultOverlayGateFraction,
		fanIn:           defaultKWayFanIn,
	}
}

// hardLimit is the seen-set size that triggers a mid-source restart.
// Clamped to at least the soft limit so a buffer factor below 1 cannot
// invert the two thresholds.
func (c overlayConfig) hardLimit() int64 {
	hl := int64(float64(c.seenKeyLimit) * c.bufferFactor)
	if hl < c.seenKeyLimit {
		return c.seenKeyLimit
	}
	return hl
}

// gateThreshold is the statless pre-source gate. Clamped to at least 1
// so a tiny soft limit cannot make the gate fire on an empty seen set
// (the first source must always get a chance to scan).
func (c overlayConfig) gateThreshold() int64 {
	g := int64(c.gateFraction * float64(c.seenKeyLimit))
	if g < 1 {
		return 1
	}
	return g
}

// OverlayOption tunes the overlay merge.
type OverlayOption func(*overlayConfig)

// WithOverlaySeenKeyLimit caps the estimated per-bucket key count
// admitted to the overlay path; buckets above the cap route to the
// K-way run-file fallback (overlayPlanBuckets). Each seen-set entry
// costs ~40B with map overhead, so the cap bounds per-bucket memory.
// Non-positive values are ignored.
func WithOverlaySeenKeyLimit(n int64) OverlayOption {
	return func(c *overlayConfig) {
		if n > 0 {
			c.seenKeyLimit = n
		}
	}
}

// WithOverlayRecordChunkSize sets how many winner records are buffered
// per raw write batch while streaming into the dest. Non-positive
// values are ignored.
func WithOverlayRecordChunkSize(n int) OverlayOption {
	return func(c *overlayConfig) {
		if n > 0 {
			c.recordChunkSize = n
		}
	}
}

// WithOverlayBufferFactor overrides the soft→hard limit multiplier
// (default 1.25). Non-positive values are ignored.
func WithOverlayBufferFactor(f float64) OverlayOption {
	return func(c *overlayConfig) {
		if f > 0 {
			c.bufferFactor = f
		}
	}
}

// WithOverlayGateFraction overrides the statless pre-source gate
// fraction (default 0.9). Non-positive values are ignored.
func WithOverlayGateFraction(f float64) OverlayOption {
	return func(c *overlayConfig) {
		if f > 0 {
			c.gateFraction = f
		}
	}
}

// WithOverlayFanIn overrides how many sources are opened per chunk
// (default matches the kway fan-in). Intended for tests that need
// multi-chunk behavior without dozens of sources. Values below 1 are
// ignored.
func WithOverlayFanIn(n int) OverlayOption {
	return func(c *overlayConfig) {
		if n >= 1 {
			c.fanIn = n
		}
	}
}

// seenSuffixSet tracks, per admitted primary-key suffix, the
// discovered_at (UnixNano) of the record currently admitted for that
// key. Keys are 128-bit hashes of the suffix instead of the suffix
// bytes themselves: lookups are allocation-free (a map[string] set
// materialized one string per scanned record, the single biggest
// overlay-specific allocator), and the fixed 16-byte pointer-free keys
// are cheaper for the GC to scan and smaller in memory than retained
// suffix strings.
//
// Trade-off: a hash collision makes a never-seen key look seen, so a
// record that should have won is dropped (or wrongly compared) in the
// dest sync. The 128-bit key is built from two independently-seeded
// 64-bit maphash values; at the 375k-key seen limit the collision
// probability per merge is ~(n^2/2)/2^128 ≈ 2e-28 — negligible against
// the hardware error floor. Seeds are per-process random, which is
// fine: the set never persists and never crosses processes.
type seenSuffixSet struct {
	m     map[[16]byte]int64
	seed1 maphash.Seed
	seed2 maphash.Seed
}

func newSeenSuffixSet() *seenSuffixSet {
	return &seenSuffixSet{
		m:     map[[16]byte]int64{},
		seed1: maphash.MakeSeed(),
		seed2: maphash.MakeSeed(),
	}
}

// keyOf hashes a primary-key suffix into the set's key space.
func (s *seenSuffixSet) keyOf(suffix []byte) [16]byte {
	var k [16]byte
	binary.BigEndian.PutUint64(k[0:8], maphash.Bytes(s.seed1, suffix))
	binary.BigEndian.PutUint64(k[8:16], maphash.Bytes(s.seed2, suffix))
	return k
}

// get returns the discovered_at nanos recorded for k, if any.
func (s *seenSuffixSet) get(k [16]byte) (int64, bool) {
	ts, ok := s.m[k]
	return ts, ok
}

// put records (or updates) the discovered_at nanos for k.
func (s *seenSuffixSet) put(k [16]byte, ts int64) {
	s.m[k] = ts
}

func (s *seenSuffixSet) size() int { return len(s.m) }

// errOverlayHardLimit is the sentinel overlaySinglePassBucket returns
// when admitting one more key would push the seen set past the hard
// limit. The caller restarts the bucket through the blind kway path.
var errOverlayHardLimit = errors.New("overlay merge: seen-set hard limit exceeded")

// overlayBucketMode is the per-bucket degradation state. Transitions
// are one-way: active → resumed (seen map frozen, remaining sources
// flow to run files and a map-aware materialization) or active →
// restarted (dest keyspace range-deleted, ALL sources flow to run
// files and the blind kway materialization).
type overlayBucketMode int

const (
	overlayBucketActive overlayBucketMode = iota
	overlayBucketResumed
	overlayBucketRestarted
)

func (m overlayBucketMode) String() string {
	switch m {
	case overlayBucketResumed:
		return "resumed"
	case overlayBucketRestarted:
		return "restarted"
	default:
		return "active"
	}
}

type overlayBucketState struct {
	mode overlayBucketMode
	// cutRank is the global source rank where kway coverage begins for
	// a resumed bucket: sources[cutRank:] were never overlay-scanned.
	cutRank int
	// restartChunk is the chunk index where a restart fired. Chunks
	// before it closed without building run files for this bucket and
	// are re-opened by the backfill pass.
	restartChunk int
}

// overlayPreGateFires reports whether the bucket should resume via
// kway BEFORE scanning this source. With per-source stats the gate is
// the exact overshoot prediction (current seen size plus the source's
// bucket count exceeding the hard limit); without stats it is the
// conservative gateThreshold on the current seen size.
func overlayPreGateFires(seen *seenSuffixSet, srcStats *reader_v2.SyncStats, bucket bucketSpec, hardLimit, gateThreshold int64) bool {
	if srcStats != nil {
		if n, ok := syncStatsBucketKeys(srcStats, bucket); ok {
			return int64(seen.size())+n > hardLimit
		}
	}
	return int64(seen.size()) >= gateThreshold
}

// bucketIndexRanges enumerates the bucket's secondary index keyspaces
// — everything the overlay writer's forEachIndexKeyFromRaw can have
// emitted for this bucket. Paired with bucket.syncRange (the primary
// range) these are exactly the ranges a restart must delete. The file
// holds one sync and keys carry no sync_id, so each is one contiguous
// range.
func bucketIndexRanges(bucket bucketSpec) [][2][]byte {
	ranges := make([][2][]byte, 0, len(bucketIndexCopySpecs(bucket)))
	for _, spec := range bucketIndexCopySpecs(bucket) {
		ranges = append(ranges, spec.bounds)
	}
	return ranges
}

// indexFamilyCopySpec describes how one secondary-index family participates in
// the whole-source copy: its key range, whether copied entries must be
// filtered against the primary seen set, and — when filtering — how many
// trailing tuple elements of the index key reconstruct the record's
// primary-key suffix (the exact bytes the seen set hashed at admission).
type indexFamilyCopySpec struct {
	bounds [2][]byte
	// filter=false families are copied verbatim: their index key is a pure
	// function of the record's structural identity, so a "seen" identity
	// implies the admitted winner produces the byte-identical index key —
	// the copy is an exact duplicate Set, never a stale entry.
	filter bool
	elems  int
}

func bucketIndexCopySpecs(bucket bucketSpec) []indexFamilyCopySpec {
	switch bucket.id {
	case runBucketResources:
		// by_parent tail is (parent_rt, parent_id, child_rt, child_id);
		// the resource primary suffix is the trailing (child_rt, child_id).
		return []indexFamilyCopySpec{{
			bounds: [2][]byte{enginepkg.ResourceByParentLowerBound(), enginepkg.ResourceByParentUpperBound()},
			filter: true,
			elems:  2,
		}}
	case runBucketEntitlements:
		// Retired family: the structured-identity migration deletes this
		// range, so it is empty on every source this copy can see. Copied
		// verbatim (i.e. not at all) for symmetry.
		return []indexFamilyCopySpec{{
			bounds: [2][]byte{enginepkg.EntitlementByResourceLowerBound(), enginepkg.EntitlementByResourceUpperBound()},
			filter: false,
		}}
	case runBucketGrants:
		return []indexFamilyCopySpec{
			{
				// by_principal permutes the identity components
				// (prt, pid, rt, rid, flag, tail): a trailing-element walk
				// CANNOT reconstruct the primary suffix. It doesn't need
				// to — the key is a pure function of the grant identity,
				// so filtered (seen) identities would produce the same
				// bytes the winner's own index entry carries. Copy
				// verbatim.
				bounds: [2][]byte{enginepkg.GrantByPrincipalLowerBound(), enginepkg.GrantByPrincipalUpperBound()},
				filter: false,
			},
			{
				// by_needs_expansion tail IS the grant primary tail
				// (rt, rid, flag, tail, prt, pid): the whole 6-element
				// suffix reconstructs the primary suffix byte-for-byte.
				// This one MUST filter: the flag is record state, not
				// identity — a newer source can admit the same identity
				// with needs_expansion=false, and copying the older
				// source's entry verbatim would bake a phantom
				// pending-expansion row into the artifact.
				bounds: [2][]byte{enginepkg.GrantByNeedsExpansionLowerBound(), enginepkg.GrantByNeedsExpansionUpperBound()},
				filter: true,
				elems:  6,
			},
		}
	default:
		return nil
	}
}

// overlayRestartBucket undoes every overlay-era write for one bucket:
// pending (uncommitted) batch writes are discarded — the writer is
// bucket-scoped, so nothing else is in them — and the committed
// primary + index keyspaces are range-deleted, including anything the
// whole-source SST path ingested. Stats for the bucket are zeroed; the
// blind kway materialization recounts from scratch.
func overlayRestartBucket(ctx context.Context, dest *enginepkg.Engine, bucket bucketSpec, writer *overlayBucketRawWriter, stats *mergeStatsAccumulator) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	writer.discard()
	b := dest.DB().NewBatch()
	defer func() { _ = b.Close() }()
	lo, hi := bucket.syncRange()
	if err := b.DeleteRange(lo, hi, nil); err != nil {
		return err
	}
	for _, r := range bucketIndexRanges(bucket) {
		if err := b.DeleteRange(r[0], r[1], nil); err != nil {
			return err
		}
	}
	if err := b.Commit(cpebble.NoSync); err != nil {
		return err
	}
	stats.resetBucket(bucket.id)
	return nil
}

// MergeFilesIntoOverlay is a sqlite-shaped compactor:
//
//   - keep a bounded in-memory seen map (suffix hash → discovered_at)
//     for each bucket while scanning sources newest-to-oldest;
//   - per key, admit the record with the strictly newest discovered_at;
//     ties keep the earliest admission (the newest source). This is the
//     same winner rule as the K-way merge (runRecordIsNewer) and the
//     sqlite attached compactor (`a.discovered_at > m.discovered_at`),
//     enforced via a shallow discovered_at scan of each candidate value;
//   - write winners through raw Pebble batches so secondary indexes are
//     materialized once;
//   - let the last source scanned (the oldest — in the production
//     skewed shape, the large base) skip the record write path when
//     its bucket is big enough (overlayWholeSourceWorthIt): the bucket
//     is materialized as SST files filtered against the seen set and
//     ingested wholesale, making the base's cost proportional to bytes
//     copied rather than records decoded; and
//   - degrade buckets whose seen set would outgrow memory to the K-way
//     run-file path ADAPTIVELY (per-bucket state machine):
//     a pre-source gate cuts at a source boundary and RESUMES via kway
//     with the seen map frozen as the conflict oracle; a source that
//     crosses the soft limit mid-scan may finish inside the buffer
//     (hard limit = soft × bufferFactor) and resume at the boundary;
//     crossing the hard limit mid-source RESTARTS the bucket — its
//     dest keyspace is range-deleted and the whole bucket flows
//     through the blind kway path (keeping SST ingest, total work ≤2×).
//     Planning routes only the provably-safe (stats sum ≤ soft) and
//     provably-doomed (single-source count > hard) buckets up front.
//
// On success it returns the dest sync's stats record, accumulated as
// winners were written, so the caller can persist the stats sidecar
// without re-scanning the output.
func MergeFilesIntoOverlay(ctx context.Context, dest *enginepkg.Engine, sources []SourceFile, destSyncID string, tmpDir string, opts ...OverlayOption) (*v3.SyncStatsRecord, error) {
	if dest == nil {
		return nil, fmt.Errorf("overlay merge: dest engine is nil")
	}
	if destSyncID == "" {
		return nil, fmt.Errorf("overlay merge: destSyncID is required")
	}
	cfg := defaultOverlayConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	// Overlay writers commit batches through the raw DB handle; invalidate
	// the dest engine's bare-id lookup state on the way out (even on error).
	defer dest.InvalidateBareIDLookups()
	hardLimit := cfg.hardLimit()
	gateThreshold := cfg.gateThreshold()
	buckets := allBuckets()
	overlayBuckets, kwayBuckets := overlayPlanBuckets(ctx, sources, buckets, cfg.seenKeyLimit, hardLimit)

	l := ctxzap.Extract(ctx)
	mergeStart := time.Now()
	l.Info("pebble overlay merge: bucket plan",
		zap.Int("sources", len(sources)),
		zap.Strings("overlay_buckets", bucketNames(overlayBuckets)),
		zap.Strings("kway_upfront_buckets", bucketNames(kwayBuckets)),
		zap.Int64("seen_key_limit", cfg.seenKeyLimit),
		zap.Int64("seen_key_hard_limit", hardLimit),
		zap.Int64("pre_gate_threshold", gateThreshold),
	)

	stats := newMergeStatsAccumulator()
	rm := &asyncRemover{}
	defer rm.wait()
	var kwayRunFiles []runFile
	defer func() {
		removeRunFiles(kwayRunFiles)
	}()
	writers := make([]*overlayBucketRawWriter, len(overlayBuckets))
	seenByBucket := make([]*seenSuffixSet, len(overlayBuckets))
	states := make([]overlayBucketState, len(overlayBuckets))
	for i, bucket := range overlayBuckets {
		writers[i] = newOverlayBucketRawWriter(dest, bucket, stats, cfg.recordChunkSize)
		seenByBucket[i] = newSeenSuffixSet()
	}
	defer func() {
		for _, writer := range writers {
			writer.cleanup()
		}
	}()

	// Process a bounded chunk of source engines through all buckets before
	// closing it. This preserves the K-way FD bound while avoiding the old
	// bucket-major reopen pattern; same-size syncs=50 overlay dropped from
	// ~5.4s to ~2.9s when source opens stopped multiplying by bucket count.
	chunkIdx := 0
	for start := 0; start < len(sources); start += cfg.fanIn {
		end := min(start+cfg.fanIn, len(sources))
		chunk, err := openSourceChunk(ctx, tmpDir, sources[start:end], start)
		if err != nil {
			return nil, err
		}
		if err := func() error {
			defer chunk.closeAsync(rm)
			for _, source := range chunk.handles {
				for bucketIdx, bucket := range overlayBuckets {
					st := &states[bucketIdx]
					if st.mode != overlayBucketActive {
						// Degraded buckets are covered by run files
						// (built per chunk below); nothing to scan.
						continue
					}
					seen := seenByBucket[bucketIdx]
					if source.rank == len(sources)-1 {
						useSST, err := overlayWholeSourceWorthIt(ctx, source, bucket, seen)
						if err != nil {
							return err
						}
						// The gate decision is the main prod tunable
						// (overlayWholeSourceMinKeys): sst_path=false
						// on a huge base means the threshold is too
						// high; sst_path=true on small merges means
						// it's too low.
						l.Debug("pebble overlay merge: whole-source gate",
							zap.String("bucket", bucket.name),
							zap.Bool("sst_path", useSST),
							zap.Int("seen_keys", seen.size()),
						)
						if useSST {
							// Last source (the base sync in the
							// skewed/prod shape): filtered SST-ingest.
							// Unseen keys — the vast majority for a
							// large base — stream straight into an
							// ingested SST instead of through the
							// memtable; seen keys get the same
							// discovered_at comparison as the
							// single-pass path. Filtering keeps the
							// ingested SST key-disjoint from earlier
							// admissions, so ingest seqnum ordering
							// can't shadow them. The SST path never
							// grows the seen set, so no degradation
							// checks apply here.
							if err := overlayMaterializeWholeSourceBucketSST(ctx, dest, source.engine, bucket, writers[bucketIdx], seen, tmpDir, stats); err != nil {
								return err
							}
							continue
						}
					}
					if overlayPreGateFires(seen, source.stats, bucket, hardLimit, gateThreshold) {
						if seen.size() == 0 {
							// Nothing admitted yet: restarting is free
							// (discard/range-delete/reset are all
							// no-ops) and routes the whole bucket
							// through the blind kway path, keeping
							// SST ingest instead of paying the
							// map-aware batch path with an empty map.
							if err := overlayRestartBucket(ctx, dest, bucket, writers[bucketIdx], stats); err != nil {
								return err
							}
							st.mode = overlayBucketRestarted
							st.restartChunk = chunkIdx
						} else {
							st.mode = overlayBucketResumed
							st.cutRank = source.rank
						}
						l.Info("pebble overlay merge: bucket degraded at pre-source gate",
							zap.String("bucket", bucket.name),
							zap.String("mode", st.mode.String()),
							zap.Int("source_rank", source.rank),
							zap.Int("seen_keys", seen.size()),
						)
						continue
					}
					err := overlaySinglePassBucket(ctx, source.engine, bucket, writers[bucketIdx], seen, hardLimit)
					switch {
					case errors.Is(err, errOverlayHardLimit):
						if err := overlayRestartBucket(ctx, dest, bucket, writers[bucketIdx], stats); err != nil {
							return err
						}
						// Release the (large) seen map; the blind kway
						// materialization needs no conflict oracle.
						seenByBucket[bucketIdx] = newSeenSuffixSet()
						st.mode = overlayBucketRestarted
						st.restartChunk = chunkIdx
						l.Info("pebble overlay merge: bucket restarted at hard limit",
							zap.String("bucket", bucket.name),
							zap.Int("source_rank", source.rank),
							zap.Int64("hard_limit", hardLimit),
						)
					case err != nil:
						return err
					default:
						if int64(seen.size()) > cfg.seenKeyLimit {
							// The source finished inside the buffer;
							// cut at the boundary. The frozen map is
							// the conflict oracle for the resumed
							// materialization.
							st.mode = overlayBucketResumed
							st.cutRank = source.rank + 1
							l.Info("pebble overlay merge: bucket resumed at source boundary",
								zap.String("bucket", bucket.name),
								zap.Int("source_rank", source.rank),
								zap.Int("seen_keys", seen.size()),
								zap.Int64("soft_limit", cfg.seenKeyLimit),
							)
						}
					}
				}
			}
			// Build this chunk's run files. Buckets need different
			// handle ranges: up-front kway and restarted buckets need
			// the full chunk; a bucket resumed mid-chunk needs only the
			// handles from its cut boundary onward. Group by start
			// index so the common case (everything from 0) stays one
			// run file per chunk.
			needs := map[int][]bucketSpec{}
			if len(kwayBuckets) > 0 {
				needs[0] = append(needs[0], kwayBuckets...)
			}
			for bucketIdx, bucket := range overlayBuckets {
				switch states[bucketIdx].mode {
				case overlayBucketResumed:
					from := states[bucketIdx].cutRank - start
					if from < 0 {
						from = 0
					}
					if from < len(chunk.handles) {
						needs[from] = append(needs[from], bucket)
					}
				case overlayBucketRestarted:
					// Chunks before restartChunk are re-opened by the
					// backfill pass; this chunk and later ones are
					// covered here in full.
					needs[0] = append(needs[0], bucket)
				case overlayBucketActive:
				}
			}
			froms := make([]int, 0, len(needs))
			for from := range needs {
				froms = append(froms, from)
			}
			sort.Ints(froms)
			for _, from := range froms {
				run, err := buildChunkRunFileFromHandles(
					ctx,
					tmpDir,
					chunk.handles[from:],
					fmt.Sprintf("overlay-fallback-%04d", len(kwayRunFiles)),
					needs[from],
				)
				if err != nil {
					return err
				}
				kwayRunFiles = append(kwayRunFiles, run)
			}
			return nil
		}(); err != nil {
			return nil, err
		}
		chunkIdx++
	}

	// Backfill: a restarted bucket needs run files for the chunks that
	// closed BEFORE its restart point (their overlay writes were
	// range-deleted). Re-open only those chunks, only for the buckets
	// that need them. Records carry global source ranks, so backfill
	// run files merge correctly regardless of build order.
	if err := overlayBackfillRestartedChunks(ctx, tmpDir, sources, cfg.fanIn, overlayBuckets, states, &kwayRunFiles, rm); err != nil {
		return nil, err
	}

	for _, writer := range writers {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := writer.flush(ctx); err != nil {
			return nil, err
		}
	}

	// Materialize the kway-routed data. Up-front kway buckets and
	// restarted buckets take the blind path (SST ingest, no conflict
	// checks — their dest keyspace holds nothing from overlay). Resumed
	// buckets take the map-aware path: the frozen seen map arbitrates
	// against overlay-admitted winners via addRaw/replaceRaw.
	blindBuckets := append([]bucketSpec{}, kwayBuckets...)
	resumedIdx := make([]int, 0, len(overlayBuckets))
	for i, bucket := range overlayBuckets {
		switch states[i].mode {
		case overlayBucketRestarted:
			blindBuckets = append(blindBuckets, bucket)
		case overlayBucketResumed:
			resumedIdx = append(resumedIdx, i)
		case overlayBucketActive:
		}
	}
	if len(blindBuckets) > 0 && len(kwayRunFiles) > 0 {
		kwayStats := newMergeStatsAccumulator()
		if err := materializeRunFilesToPebble(ctx, dest, tmpDir, kwayRunFiles, blindBuckets, kwayStats); err != nil {
			return nil, err
		}
		stats.addRecord(kwayStats.record())
	}
	for _, i := range resumedIdx {
		if err := materializeResumedRunFilesToPebble(ctx, tmpDir, kwayRunFiles, overlayBuckets[i], seenByBucket[i], writers[i]); err != nil {
			return nil, err
		}
	}

	// Per-bucket seen sizes calibrate the seen-key limit (memory);
	// per-bucket modes report how often degradation fires in prod.
	doneFields := []zap.Field{
		zap.Duration("elapsed", time.Since(mergeStart)),
		zap.Int("kway_fallback_run_files", len(kwayRunFiles)),
	}
	for i, bucket := range overlayBuckets {
		doneFields = append(doneFields,
			zap.Int("seen_keys_"+bucket.name, seenByBucket[i].size()),
			zap.String("mode_"+bucket.name, states[i].mode.String()),
		)
	}
	l.Info("pebble overlay merge: done", doneFields...)
	return stats.record(), nil
}

// overlayBackfillRestartedChunks re-opens the chunks that closed
// before each restarted bucket's restart point and builds run files
// covering those buckets. Chunks at or after a bucket's restartChunk
// were already covered in the main loop.
func overlayBackfillRestartedChunks(
	ctx context.Context,
	tmpDir string,
	sources []SourceFile,
	fanIn int,
	overlayBuckets []bucketSpec,
	states []overlayBucketState,
	kwayRunFiles *[]runFile,
	rm *asyncRemover,
) error {
	chunkIdx := 0
	for start := 0; start < len(sources); start += fanIn {
		var needed []bucketSpec
		for i, bucket := range overlayBuckets {
			if states[i].mode == overlayBucketRestarted && states[i].restartChunk > chunkIdx {
				needed = append(needed, bucket)
			}
		}
		if len(needed) == 0 {
			chunkIdx++
			continue
		}
		end := min(start+fanIn, len(sources))
		chunk, err := openSourceChunk(ctx, tmpDir, sources[start:end], start)
		if err != nil {
			return err
		}
		run, err := func() (runFile, error) {
			defer chunk.closeAsync(rm)
			return buildChunkRunFileFromHandles(
				ctx,
				tmpDir,
				chunk.handles,
				fmt.Sprintf("overlay-backfill-%04d", len(*kwayRunFiles)),
				needed,
			)
		}()
		if err != nil {
			return err
		}
		*kwayRunFiles = append(*kwayRunFiles, run)
		chunkIdx++
	}
	return nil
}

// materializeResumedRunFilesToPebble is the map-aware counterpart to
// materializeRunFilesToPebble for one resumed bucket. The merged run
// stream holds the kway winner per key among the remaining (older)
// sources — duplicates are adjacent and deduped positionally, so the
// frozen seen map needs no insertions and memory stays exactly capped.
// Each stream winner is arbitrated against the overlay-admitted record
// via the map: a miss is a fresh admission (addRaw), a hit replaces
// only on a strictly newer discovered_at (replaceRaw — ties keep the
// overlay record, which came from a newer source, matching
// runRecordIsNewer's ordering), otherwise it is dropped.
//
// Writes go through the bucket's overlayBucketRawWriter (batch path)
// rather than SST ingest: replacements must delete the superseded
// value's index keys, and fresh admissions must not shadow batch
// writes via ingest seqnums. addRaw counts winners and replaceRaw
// regroups; the blind path's countWinner is deliberately not used, so
// nothing double-counts.
func materializeResumedRunFilesToPebble(
	ctx context.Context,
	tmpDir string,
	inputs []runFile,
	bucket bucketSpec,
	seen *seenSuffixSet,
	writer *overlayBucketRawWriter,
) error {
	if len(inputs) == 0 {
		return nil
	}
	merged, err := mergeRunFileGroup(ctx, tmpDir, inputs, "overlay-resumed-"+bucket.name, []bucketSpec{bucket})
	if err != nil {
		return err
	}
	defer removeRunFiles([]runFile{merged})
	f, err := os.Open(merged.path)
	if err != nil {
		return err
	}
	defer f.Close()
	section := merged.sections[bucket.id]
	if _, err := f.Seek(section.offset, io.SeekStart); err != nil {
		return err
	}
	br := bufio.NewReaderSize(io.LimitReader(f, section.length), runFileBufferSize)
	destLower, _ := bucket.syncRange()
	var rec runRecord
	var hdr [runHeaderSize]byte
	var lastPrimaryKey []byte
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
		if bytes.Equal(rec.key, lastPrimaryKey) {
			continue
		}
		lastPrimaryKey = append(lastPrimaryKey[:0], rec.key...)
		if len(rec.key) <= len(destLower) {
			return fmt.Errorf("overlay resumed merge: run key shorter than dest prefix: key=%d prefix=%d", len(rec.key), len(destLower))
		}
		suffix := rec.key[len(destLower):]
		if prevTs, hit := seen.get(seen.keyOf(suffix)); hit {
			if rec.tsNanos <= prevTs {
				continue
			}
			if err := writer.replaceRaw(ctx, bucket, rec.key, destLower, rec.value); err != nil {
				return err
			}
			continue
		}
		if err := writer.addRaw(ctx, bucket, rec.key, destLower, rec.value); err != nil {
			return err
		}
	}
	return writer.flush(ctx)
}

// overlayPlanBuckets routes each bucket up front using only cached
// stats — no counting pre-scan. The stats give two bounds on the
// distinct-key count the seen set will hold:
//
//   - sum of per-source counts is an UPPER bound (duplicates counted
//     once per source) → sum ≤ softLimit guarantees overlay is safe;
//   - max single-source count is a LOWER bound (one source's keys are
//     all distinct within it) → max > hardLimit guarantees overlay
//     would restart, so route straight to kway and keep blind ingest.
//
// Everything in between — and every bucket where any source lacks
// stats — starts in overlay and relies on the adaptive degradation
// state machine in MergeFilesIntoOverlay. Sources whose keys never
// feed the seen set (the last source when it will take the
// whole-source SST path) are excluded from both bounds.
func overlayPlanBuckets(ctx context.Context, sources []SourceFile, buckets []bucketSpec, softLimit, hardLimit int64) ([]bucketSpec, []bucketSpec) {
	l := ctxzap.Extract(ctx)
	overlayBuckets := make([]bucketSpec, 0, len(buckets))
	kwayBuckets := make([]bucketSpec, 0, len(buckets))
	for _, bucket := range buckets {
		sum, maxSingle, ok := overlayEstimatedBucketBounds(sources, bucket)
		if !ok {
			// At least one source lacks a stats projection
			// (pre-projection file or missing sidecar) — a
			// fleet-health signal that inputs should be regenerated
			// by a current SDK. Start in overlay; degradation covers
			// the misprediction.
			l.Debug("pebble overlay merge: bucket routing",
				zap.String("bucket", bucket.name),
				zap.Bool("from_cached_stats", false),
				zap.Bool("routed_to_kway", false),
			)
			overlayBuckets = append(overlayBuckets, bucket)
			continue
		}
		routeToKway := sum > softLimit && maxSingle > hardLimit
		l.Debug("pebble overlay merge: bucket routing",
			zap.String("bucket", bucket.name),
			zap.Int64("estimated_seen_load", sum),
			zap.Int64("max_single_source", maxSingle),
			zap.Bool("from_cached_stats", true),
			zap.Bool("routed_to_kway", routeToKway),
		)
		if routeToKway {
			kwayBuckets = append(kwayBuckets, bucket)
		} else {
			overlayBuckets = append(overlayBuckets, bucket)
		}
	}
	return overlayBuckets, kwayBuckets
}

// bucketNames renders a bucket list for log fields.
func bucketNames(buckets []bucketSpec) []string {
	out := make([]string, 0, len(buckets))
	for _, b := range buckets {
		out = append(out, b.name)
	}
	return out
}

// overlayEstimatedBucketBounds returns the sum (upper bound on
// distinct keys) and max single-source count (lower bound) over the
// sources whose keys feed the seen set. ok is false when any source
// lacks cached stats.
//
// The last source's keys never enter the seen set when it takes the
// whole-source SST path (they're filtered against the set, not added
// to it). Exclude that contribution from both bounds — otherwise a
// large base sync (the production skewed shape) routes its bucket to
// the kway fallback and the SST path never runs for exactly the data
// it exists for.
func overlayEstimatedBucketBounds(sources []SourceFile, bucket bucketSpec) (int64, int64, bool) {
	counts := make([]int64, 0, len(sources))
	for _, source := range sources {
		if source.Stats == nil {
			return 0, 0, false
		}
		n, ok := syncStatsBucketKeys(source.Stats, bucket)
		if !ok {
			return 0, 0, false
		}
		counts = append(counts, n)
	}
	var sum, maxSingle int64
	for i, n := range counts {
		if i == len(counts)-1 && n >= overlayWholeSourceMinKeys {
			// Will take the whole-source SST path; never feeds the map.
			continue
		}
		sum += n
		if n > maxSingle {
			maxSingle = n
		}
	}
	return sum, maxSingle, true
}

func syncStatsBucketKeys(stats *reader_v2.SyncStats, bucket bucketSpec) (int64, bool) {
	switch bucket.id {
	case runBucketResourceTypes:
		return stats.GetResourceTypes(), true
	case runBucketResources:
		return stats.GetResources(), true
	case runBucketEntitlements:
		return stats.GetEntitlements(), true
	case runBucketGrants:
		return stats.GetGrants(), true
	default:
		return 0, false
	}
}

func overlaySinglePassBucket(
	ctx context.Context,
	source *enginepkg.Engine,
	bucket bucketSpec,
	writer *overlayBucketRawWriter,
	seen *seenSuffixSet,
	hardLimit int64,
) error {
	lower, upper := bucket.syncRange()
	iter, err := source.DB().NewIter(&cpebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		// Dedupe on the primary-key suffix. Source keys are already in
		// final dest form (no sync_id), so admitted records are written
		// under iter.Key() verbatim. Admission is hash-based and
		// allocation-free (see seenSuffixSet); the shallow
		// discovered_at scan keeps the winner rule identical to
		// K-way / sqlite.
		suffix := iter.Key()[len(lower):]
		k := seen.keyOf(suffix)
		ts, err := discoveredAtNanosFromRaw(bucket, iter.Value())
		if err != nil {
			return err
		}
		if prevTs, ok := seen.get(k); ok {
			// Key already admitted by an earlier (newer) source. Replace
			// only on a strictly newer discovered_at; ties keep the
			// earlier admission — matching runRecordIsNewer's
			// (ts, then source rank) ordering. Updates never grow the
			// map, so no hard-limit check applies.
			if ts <= prevTs {
				continue
			}
			if err := writer.replaceRaw(ctx, bucket, iter.Key(), lower, iter.Value()); err != nil {
				return err
			}
			seen.put(k, ts)
			continue
		}
		// New key: admitting it grows the seen set. Crossing the soft
		// limit mid-source is allowed (the buffer); crossing the HARD
		// limit aborts the scan and the caller restarts the bucket
		// through the blind kway path.
		if int64(seen.size()) >= hardLimit {
			return errOverlayHardLimit
		}
		seen.put(k, ts)
		if err := writer.addRaw(ctx, bucket, iter.Key(), lower, iter.Value()); err != nil {
			return err
		}
	}
	return iter.Error()
}

// overlayWholeSourceMinKeys gates the filtered whole-source SST path
// by bucket size. The SST route carries fixed overhead — an index
// writer set (7 run files), chunk sorts, and one ingest per family,
// each ingest potentially forcing a memtable flush where it overlaps
// earlier batch writes — which only pays off when the bucket is large
// enough that memtable insertion would dominate. Benchmarks showed the
// unconditional SST route doubling small-merge time. Var (not const)
// so tests can force the path.
var overlayWholeSourceMinKeys int64 = 100_000

// overlayWholeSourceWorthIt decides whether the last source's bucket
// takes the filtered SST-ingest path. An empty seen set always
// qualifies (pure whole-source adoption, the original fast path);
// otherwise the bucket must be large enough to amortize the SST
// machinery. Bucket size comes from the source's cached stats (free,
// via the manifest projection) or a bounded key count capped at the
// threshold.
func overlayWholeSourceWorthIt(ctx context.Context, source sourceHandle, bucket bucketSpec, seen *seenSuffixSet) (bool, error) {
	if seen.size() == 0 {
		return true, nil
	}
	if source.stats != nil {
		if n, ok := syncStatsBucketKeys(source.stats, bucket); ok {
			return n >= overlayWholeSourceMinKeys, nil
		}
	}
	lower, upper := bucket.syncRange()
	iter, err := source.engine.DB().NewIter(&cpebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return false, err
	}
	defer iter.Close()
	var n int64
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		n++
		if n >= overlayWholeSourceMinKeys {
			return true, nil
		}
	}
	return false, iter.Error()
}

// overlayMaterializeWholeSourceBucketSST streams the last source's
// bucket into a pre-sorted SST and ingests it, bypassing the memtable
// entirely. Keys already admitted by earlier (newer) sources are
// filtered out via the seen set — that keeps the ingested file
// key-disjoint from every batch write, so ingest sequence numbers
// cannot shadow earlier winners. A filtered-out key whose value is
// strictly newer (by discovered_at) is replaced through the writer's
// batch path instead, preserving the merge-wide winner rule.
//
// Secondary indexes are NOT derived here: the source's index
// keyspaces are copied verbatim by overlayCopySourceIndexesSST, which
// applies the same seen-set filter at byte level. Re-deriving and
// re-sorting index keys per record was the whole-source path's
// dominant allocation and CPU cost.
//
// This is the bulk path for the production skewed shape: a large base
// sync with a small partial-derived seen set streams through here at
// SST-build speed instead of paying memtable insertion, flush, and
// background compaction per record.
func overlayMaterializeWholeSourceBucketSST(
	ctx context.Context,
	dest *enginepkg.Engine,
	source *enginepkg.Engine,
	bucket bucketSpec,
	writer *overlayBucketRawWriter,
	seen *seenSuffixSet,
	tmpDir string,
	stats *mergeStatsAccumulator,
) error {
	lower, upper := bucket.syncRange()
	iter, err := source.DB().NewIter(&cpebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return err
	}
	defer iter.Close()

	primaryPath := filepath.Join(tmpDir, fmt.Sprintf("overlay-whole-%s-primary.sst", bucket.name))
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

	wrotePrimary := false
	checkSeen := seen.size() > 0
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		// Source keys are already in final dest form (no sync_id):
		// winners are written under iter.Key() verbatim.
		sourceKey := iter.Key()
		if len(sourceKey) < len(lower) {
			return fmt.Errorf("overlay merge: source key shorter than lower bound prefix: key=%d prefix=%d", len(sourceKey), len(lower))
		}
		suffix := sourceKey[len(lower):]
		if checkSeen {
			if prevTs, ok := seen.get(seen.keyOf(suffix)); ok {
				// Admitted by an earlier (newer) source. Same rule as
				// overlaySinglePassBucket: replace only on a strictly
				// newer discovered_at, through the batch path (rare).
				// No seen.put: this is the last source, nothing follows.
				ts, err := discoveredAtNanosFromRaw(bucket, iter.Value())
				if err != nil {
					return err
				}
				if ts <= prevTs {
					continue
				}
				if err := writer.replaceRaw(ctx, bucket, sourceKey, lower, iter.Value()); err != nil {
					return err
				}
				continue
			}
		}
		if err := primaryWriter.Set(sourceKey, iter.Value()); err != nil {
			return err
		}
		wrotePrimary = true
		// Every unseen key in this path is a winner. countWinner does
		// total + grouping (resources: from the key tail; grants: a
		// shallow value scan) — the grouping the removed index-key
		// derivation used to piggyback.
		if err := stats.countWinner(bucket, sourceKey, lower, iter.Value()); err != nil {
			return err
		}
	}
	if err := iter.Error(); err != nil {
		return err
	}
	if err := primaryWriter.Close(); err != nil {
		return err
	}
	primarySuccess = true
	defer func() { _ = os.Remove(primaryPath) }()
	if wrotePrimary {
		if err := dest.DB().Ingest(ctx, []string{primaryPath}); err != nil {
			return fmt.Errorf("overlay merge: ingest whole primary %s: %w", bucket.name, err)
		}
		dest.InvalidateBareIDLookups()
	}
	return overlayCopySourceIndexesSST(ctx, dest, source, bucket, seen, tmpDir)
}

// indexKeyPrimarySuffix extracts the trailing tuple elements of an
// index key that form the corresponding record's primary-key suffix —
// the exact bytes the seen set hashed when the record was admitted:
//
//   - grants / entitlements: every index family's last element is the
//     record's external_id, and the grant/entitlement primary suffix
//     is sep | esc(external_id) → elems = 1;
//   - resources (by_parent): the tail is (parent_rt, parent_id,
//     child_rt, child_id) and the resource primary suffix is
//     sep | esc(rt) | sep | esc(id) → elems = 2.
//
// The returned slice INCLUDES the leading separator, matching
// key[len(lower):] of the primary key byte-for-byte: tuple escaping
// guarantees element bytes never contain a bare separator (0x00), so
// the separators found by LastIndexByte are exactly the element
// boundaries, and the escaped element bytes are identical in primary
// and index keys.
func indexKeyPrimarySuffix(key []byte, elems int) ([]byte, error) {
	i := len(key)
	for n := 0; n < elems; n++ {
		i = bytes.LastIndexByte(key[:i], 0x00)
		if i < 3 {
			// Position 3 is the first separator (after the 3-byte
			// v3|typeIndex|idx header); anything earlier means the key
			// has fewer tuple elements than the family guarantees.
			return nil, fmt.Errorf("overlay merge: index key too short for %d-element suffix: %x", elems, key)
		}
	}
	return key[i:], nil
}

// overlayCopySourceIndexesSST streams one bucket's secondary-index
// keyspaces from the source verbatim into a single SST and ingests it.
// Source index keys are already in final dest byte form (v3 keys carry
// no sync_id) and each family range is already sorted, so no per-record
// derivation, run files, or external sort are needed.
//
// Index entries belonging to records the primary copy filtered out
// (admitted earlier by a newer source) are dropped by probing the seen
// set with the index key's primary suffix (indexKeyPrimarySuffix).
// Replacements that went through the batch path re-emit their own
// index keys there, so dropping every seen entry here is exact.
func overlayCopySourceIndexesSST(
	ctx context.Context,
	dest *enginepkg.Engine,
	source *enginepkg.Engine,
	bucket bucketSpec,
	seen *seenSuffixSet,
	tmpDir string,
) error {
	specs := bucketIndexCopySpecs(bucket)
	if len(specs) == 0 {
		return nil
	}
	// SST keys must be appended in ascending order; the family ranges
	// are disjoint, so ordering them by lower bound suffices.
	sort.Slice(specs, func(i, j int) bool {
		return bytes.Compare(specs[i].bounds[0], specs[j].bounds[0]) < 0
	})
	sstPath := filepath.Join(tmpDir, fmt.Sprintf("overlay-whole-%s-index.sst", bucket.name))
	w, err := newSSTBuilder(sstPath)
	if err != nil {
		return err
	}
	success := false
	defer func() {
		if !success {
			_ = w.Close()
		}
		_ = os.Remove(sstPath)
	}()
	wrote := false
	for _, spec := range specs {
		checkSeen := spec.filter && seen.size() > 0
		if err := copyIndexRangeFiltered(ctx, source, spec.bounds[0], spec.bounds[1], checkSeen, seen, spec.elems, w, &wrote); err != nil {
			return err
		}
	}
	if err := w.Close(); err != nil {
		return err
	}
	success = true
	if !wrote {
		return nil
	}
	if err := dest.DB().Ingest(ctx, []string{sstPath}); err != nil {
		return fmt.Errorf("overlay merge: ingest whole index %s: %w", bucket.name, err)
	}
	dest.InvalidateBareIDLookups()
	return nil
}

func copyIndexRangeFiltered(
	ctx context.Context,
	source *enginepkg.Engine,
	lower, upper []byte,
	checkSeen bool,
	seen *seenSuffixSet,
	suffixElems int,
	w *sstBuilder,
	wrote *bool,
) error {
	iter, err := source.DB().NewIter(&cpebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		key := iter.Key()
		if checkSeen {
			suffix, err := indexKeyPrimarySuffix(key, suffixElems)
			if err != nil {
				return err
			}
			if _, ok := seen.get(seen.keyOf(suffix)); ok {
				continue
			}
		}
		if err := w.Set(key, nil); err != nil {
			return err
		}
		*wrote = true
	}
	return iter.Error()
}

type rawIndexScratch struct {
	key   []byte
	tail1 []byte
	tail2 []byte
}

// Build secondary index keys from borrowed protobuf string bytes and reusable
// tuple-decode scratch instead of materializing Go strings. Together with the
// raw batch writer, this moved the bad same-size syncs=50 overlay case from
// ~2.69s / 2.4M allocs to ~2.08s / 0.79M allocs.
//
// When stats is non-nil, per-resource-type stats grouping piggybacks on the
// field extraction this scan already does for winners (resources: the key
// tail decode; grants: the entitlement ref scan), so the accumulator costs
// no additional value parsing. Callers must only pass stats when every
// record passed in is a merge winner.
func forEachIndexKeyFromRaw(
	bucket bucketSpec,
	destKey []byte,
	destLower []byte,
	value []byte,
	scratch *rawIndexScratch,
	stats *mergeStatsAccumulator,
	emit func([]byte) error,
) error {
	if bucket.forEachIndexKey == nil {
		return nil
	}
	switch bucket.id {
	case runBucketResources:
		rt, id, err := decodePrimaryTailBytes2(destKey, destLower, scratch)
		if err != nil {
			return err
		}
		stats.groupResource(rt)
		parentRT, parentID, err := scanResourceParentBytes(value)
		if err != nil {
			return err
		}
		if len(parentID) == 0 {
			return nil
		}
		scratch.key = enginepkg.AppendResourceIndexKeyRawBytes(scratch.key[:0], parentRT, parentID, rt, id)
		return emit(scratch.key)
	case runBucketEntitlements:
		resourceRT, _, err := scanEntitlementResourceBytes(value)
		if err != nil {
			return err
		}
		stats.groupEntitlement(resourceRT)
		return nil
	case runBucketGrants:
		entRT, entRID, entID, principalRT, principalID, needsExpansion, err := scanGrantIndexFieldsBytes(value)
		if err != nil {
			return err
		}
		stats.groupGrant(entRT)
		return enginepkg.ForEachGrantIndexKeyRaw(
			string(entRT),
			string(entRID),
			string(entID),
			string(principalRT),
			string(principalID),
			"",
			needsExpansion,
			emit,
		)
	default:
		return nil
	}
}

func decodePrimaryTailBytes2(key []byte, lower []byte, scratch *rawIndexScratch) ([]byte, []byte, error) {
	tail, err := primaryTail(key, lower)
	if err != nil {
		return nil, nil, err
	}
	var next int
	scratch.tail1, next, err = codec.DecodeTupleStringTo(scratch.tail1[:0], tail, 0)
	if err != nil {
		return nil, nil, err
	}
	if next >= len(tail) {
		return scratch.tail1, nil, nil
	}
	scratch.tail2, _, err = codec.DecodeTupleStringTo(scratch.tail2[:0], tail, next+1)
	if err != nil {
		return nil, nil, err
	}
	return scratch.tail1, scratch.tail2, nil
}

func primaryTail(key []byte, lower []byte) ([]byte, error) {
	if len(key) <= len(lower) {
		return nil, fmt.Errorf("overlay raw index: key shorter than expected lower bound")
	}
	return key[len(lower)+1:], nil
}

// The shallow message-field scanners below keep the LAST occurrence of
// the target field, approximating proto merge semantics (a shallow scan
// cannot field-merge repeated submessage occurrences, but values written
// by this SDK carry at most one occurrence). All shallow scanners in
// this package and in the engine's raw_records.go follow this rule.
func scanResourceParentBytes(value []byte) ([]byte, []byte, error) {
	var rt, id []byte
	for len(value) > 0 {
		num, typ, n := protowire.ConsumeTag(value)
		if n < 0 {
			return nil, nil, protowire.ParseError(n)
		}
		value = value[n:]
		if num != 6 {
			n = protowire.ConsumeFieldValue(num, typ, value)
			if n < 0 {
				return nil, nil, protowire.ParseError(n)
			}
			value = value[n:]
			continue
		}
		if typ != protowire.BytesType {
			return nil, nil, fmt.Errorf("overlay raw index: resource parent wire type %v", typ)
		}
		msg, n := protowire.ConsumeBytes(value)
		if n < 0 {
			return nil, nil, protowire.ParseError(n)
		}
		var err error
		rt, id, err = scanResourceRefBytes(msg)
		if err != nil {
			return nil, nil, err
		}
		value = value[n:]
	}
	return rt, id, nil
}

func scanEntitlementResourceBytes(value []byte) ([]byte, []byte, error) {
	var rt, id []byte
	for len(value) > 0 {
		num, typ, n := protowire.ConsumeTag(value)
		if n < 0 {
			return nil, nil, protowire.ParseError(n)
		}
		value = value[n:]
		if num != 3 {
			n = protowire.ConsumeFieldValue(num, typ, value)
			if n < 0 {
				return nil, nil, protowire.ParseError(n)
			}
			value = value[n:]
			continue
		}
		if typ != protowire.BytesType {
			return nil, nil, fmt.Errorf("overlay raw index: entitlement resource wire type %v", typ)
		}
		msg, n := protowire.ConsumeBytes(value)
		if n < 0 {
			return nil, nil, protowire.ParseError(n)
		}
		var err error
		rt, id, err = scanResourceRefBytes(msg)
		if err != nil {
			return nil, nil, err
		}
		value = value[n:]
	}
	return rt, id, nil
}

func scanGrantIndexFieldsBytes(value []byte) ([]byte, []byte, []byte, []byte, []byte, bool, error) {
	var entRT, entRID, entID, principalRT, principalID []byte
	var needsExpansion bool
	for len(value) > 0 {
		num, typ, n := protowire.ConsumeTag(value)
		if n < 0 {
			return nil, nil, nil, nil, nil, false, protowire.ParseError(n)
		}
		value = value[n:]
		switch num {
		case 3:
			if typ != protowire.BytesType {
				return nil, nil, nil, nil, nil, false, fmt.Errorf("overlay raw index: grant entitlement wire type %v", typ)
			}
			msg, n := protowire.ConsumeBytes(value)
			if n < 0 {
				return nil, nil, nil, nil, nil, false, protowire.ParseError(n)
			}
			var err error
			entRT, entRID, entID, err = scanEntitlementRefBytes(msg)
			if err != nil {
				return nil, nil, nil, nil, nil, false, err
			}
			value = value[n:]
		case 4:
			if typ != protowire.BytesType {
				return nil, nil, nil, nil, nil, false, fmt.Errorf("overlay raw index: grant principal wire type %v", typ)
			}
			msg, n := protowire.ConsumeBytes(value)
			if n < 0 {
				return nil, nil, nil, nil, nil, false, protowire.ParseError(n)
			}
			var err error
			principalRT, principalID, err = scanPrincipalRefBytes(msg)
			if err != nil {
				return nil, nil, nil, nil, nil, false, err
			}
			value = value[n:]
		case 7:
			if typ != protowire.VarintType {
				return nil, nil, nil, nil, nil, false, fmt.Errorf("overlay raw index: grant needs_expansion wire type %v", typ)
			}
			v, n := protowire.ConsumeVarint(value)
			if n < 0 {
				return nil, nil, nil, nil, nil, false, protowire.ParseError(n)
			}
			needsExpansion = v != 0
			value = value[n:]
		default:
			n = protowire.ConsumeFieldValue(num, typ, value)
			if n < 0 {
				return nil, nil, nil, nil, nil, false, protowire.ParseError(n)
			}
			value = value[n:]
		}
	}
	return entRT, entRID, entID, principalRT, principalID, needsExpansion, nil
}

// The byte scanners borrow protobuf string field slices directly from the
// source value. They do not validate UTF-8: these values were written by our
// protobuf encoder on the trusted c1z path, and the scanner's job is only to
// avoid allocations while deriving secondary index keys.
func scanResourceRefBytes(value []byte) ([]byte, []byte, error) {
	var rt, id []byte
	for len(value) > 0 {
		num, typ, n := protowire.ConsumeTag(value)
		if n < 0 {
			return nil, nil, protowire.ParseError(n)
		}
		value = value[n:]
		if typ != protowire.BytesType {
			n = protowire.ConsumeFieldValue(num, typ, value)
			if n < 0 {
				return nil, nil, protowire.ParseError(n)
			}
			value = value[n:]
			continue
		}
		b, n := protowire.ConsumeBytes(value)
		if n < 0 {
			return nil, nil, protowire.ParseError(n)
		}
		switch num {
		case 1:
			rt = b
		case 2:
			id = b
		default:
		}
		value = value[n:]
	}
	return rt, id, nil
}

func scanEntitlementRefBytes(value []byte) ([]byte, []byte, []byte, error) {
	var rt, rid, eid []byte
	for len(value) > 0 {
		num, typ, n := protowire.ConsumeTag(value)
		if n < 0 {
			return nil, nil, nil, protowire.ParseError(n)
		}
		value = value[n:]
		if typ != protowire.BytesType {
			n = protowire.ConsumeFieldValue(num, typ, value)
			if n < 0 {
				return nil, nil, nil, protowire.ParseError(n)
			}
			value = value[n:]
			continue
		}
		b, n := protowire.ConsumeBytes(value)
		if n < 0 {
			return nil, nil, nil, protowire.ParseError(n)
		}
		switch num {
		case 1:
			rt = b
		case 2:
			rid = b
		case 3:
			eid = b
		default:
		}
		value = value[n:]
	}
	return rt, rid, eid, nil
}

func scanPrincipalRefBytes(value []byte) ([]byte, []byte, error) {
	var rt, id []byte
	for len(value) > 0 {
		num, typ, n := protowire.ConsumeTag(value)
		if n < 0 {
			return nil, nil, protowire.ParseError(n)
		}
		value = value[n:]
		if typ != protowire.BytesType {
			n = protowire.ConsumeFieldValue(num, typ, value)
			if n < 0 {
				return nil, nil, protowire.ParseError(n)
			}
			value = value[n:]
			continue
		}
		b, n := protowire.ConsumeBytes(value)
		if n < 0 {
			return nil, nil, protowire.ParseError(n)
		}
		switch num {
		case 1:
			rt = b
		case 2:
			id = b
		default:
		}
		value = value[n:]
	}
	return rt, id, nil
}

type overlayBucketRawWriter struct {
	dest      *enginepkg.Engine
	bucket    bucketSpec
	chunkSize int
	primary   *cpebble.Batch
	index     *cpebble.Batch
	count     int
	scratch   rawIndexScratch
	stats     *mergeStatsAccumulator
	// replacements counts replaceRaw invocations (an older source
	// carrying a strictly newer discovered_at). Logged at merge end —
	// the replace slow path assumes this is rare; a high count in prod
	// invalidates that assumption.
	replacements int64
}

// The normal overlay path writes raw primary values and raw secondary index
// keys directly into Pebble batches. This avoids proto.Unmarshal + Put*Records
// remarshal/index generation; same-size syncs=50 improved from ~2.9s / 3.8M
// allocs to ~2.25s / 3.1M allocs before the later byte-slice reductions.
func newOverlayBucketRawWriter(dest *enginepkg.Engine, bucket bucketSpec, stats *mergeStatsAccumulator, chunkSize int) *overlayBucketRawWriter {
	return &overlayBucketRawWriter{
		dest:      dest,
		bucket:    bucket,
		chunkSize: chunkSize,
		primary:   dest.DB().NewBatch(),
		index:     dest.DB().NewBatch(),
		stats:     stats,
	}
}

func (w *overlayBucketRawWriter) addRaw(ctx context.Context, bucket bucketSpec, destKey []byte, destLower []byte, value []byte) error {
	if err := w.primary.Set(destKey, value, nil); err != nil {
		return err
	}
	// Every addRaw call is a winner (the seen-set dedupe happens before
	// admission), so totals count here; per-RT grouping piggybacks on
	// the index-key field scan below.
	w.stats.countWinnerTotal(bucket.id)
	if bucket.forEachIndexKey == nil {
		w.count++
		if w.count >= w.chunkSize {
			return w.flush(ctx)
		}
		return nil
	}
	if err := forEachIndexKeyFromRaw(bucket, destKey, destLower, value, &w.scratch, w.stats, func(key []byte) error {
		return w.index.Set(key, nil, nil)
	}); err != nil {
		return err
	}
	w.count++
	if w.count >= w.chunkSize {
		return w.flush(ctx)
	}
	return nil
}

// replaceRaw overwrites a previously admitted record with a strictly
// newer one — an older source carried a newer discovered_at than the
// admitted record. Rare in practice (normal syncs stamp discovered_at
// in sync order, so the newest source wins at first admission), which
// is why this path can afford to be expensive: the old value's derived
// index keys must be deleted, and the old value may still sit in an
// uncommitted batch, so we flush first and read it back from the DB.
//
// Totals are unchanged (same logical key, already counted at first
// admission); only the per-RT grouping can move, because the grant's
// entitlement resource type and the entitlement's resource type both
// live in the value.
func (w *overlayBucketRawWriter) replaceRaw(ctx context.Context, bucket bucketSpec, destKey []byte, destLower []byte, value []byte) error {
	w.replacements++
	if err := w.flush(ctx); err != nil {
		return err
	}
	oldVal, closer, err := w.dest.DB().Get(destKey)
	if err != nil {
		if errors.Is(err, cpebble.ErrNotFound) {
			// Seen-set hash collision: the admitted record was a different
			// key (probability ~1e-28, see seenSuffixSet). Treat the
			// candidate as a fresh admission.
			return w.addRaw(ctx, bucket, destKey, destLower, value)
		}
		return err
	}
	// Drop the index keys derived from the old value and move the grant
	// stats grouping before the closer invalidates oldVal. Deleting and
	// re-setting an identical index key within the same batch is fine —
	// batch ops apply in order, so the Set below wins.
	if bucket.forEachIndexKey != nil {
		if err := forEachIndexKeyFromRaw(bucket, destKey, destLower, oldVal, &w.scratch, nil, func(key []byte) error {
			return w.index.Delete(key, nil)
		}); err != nil {
			closer.Close()
			return err
		}
	}
	switch bucket.id {
	case runBucketGrants:
		oldEntRT, _, _, _, _, _, err := scanGrantIndexFieldsBytes(oldVal)
		if err != nil {
			closer.Close()
			return err
		}
		newEntRT, _, _, _, _, _, err := scanGrantIndexFieldsBytes(value)
		if err != nil {
			closer.Close()
			return err
		}
		w.stats.regroupGrant(oldEntRT, newEntRT)
	case runBucketEntitlements:
		oldRT, _, err := scanEntitlementResourceBytes(oldVal)
		if err != nil {
			closer.Close()
			return err
		}
		newRT, _, err := scanEntitlementResourceBytes(value)
		if err != nil {
			closer.Close()
			return err
		}
		w.stats.regroupEntitlement(oldRT, newRT)
	}
	closer.Close()
	if err := w.primary.Set(destKey, value, nil); err != nil {
		return err
	}
	if bucket.forEachIndexKey != nil {
		if err := forEachIndexKeyFromRaw(bucket, destKey, destLower, value, &w.scratch, nil, func(key []byte) error {
			return w.index.Set(key, nil, nil)
		}); err != nil {
			return err
		}
	}
	w.count++
	if w.count >= w.chunkSize {
		return w.flush(ctx)
	}
	return nil
}

func (w *overlayBucketRawWriter) flush(ctx context.Context) error {
	if w == nil || w.count == 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	opts := cpebble.NoSync
	if err := w.primary.Commit(opts); err != nil {
		return err
	}
	if err := w.index.Commit(opts); err != nil {
		return err
	}
	w.primary = w.dest.DB().NewBatch()
	w.index = w.dest.DB().NewBatch()
	w.count = 0
	return nil
}

func (w *overlayBucketRawWriter) cleanup() {
	if w == nil {
		return
	}
	if w.primary != nil {
		_ = w.primary.Close()
	}
	if w.index != nil {
		_ = w.index.Close()
	}
}

// discard drops the writer's uncommitted batches and starts fresh
// ones. Used by the restart path: the writer is bucket-scoped, so the
// pending writes are exactly the data the restart's range-delete would
// tombstone — dropping them before they commit is strictly cheaper and
// prevents a later flush from resurrecting deleted keys.
func (w *overlayBucketRawWriter) discard() {
	if w == nil {
		return
	}
	if w.primary != nil {
		_ = w.primary.Close()
	}
	if w.index != nil {
		_ = w.index.Close()
	}
	w.primary = w.dest.DB().NewBatch()
	w.index = w.dest.DB().NewBatch()
	w.count = 0
}
