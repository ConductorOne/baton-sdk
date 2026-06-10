package pebble

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/maphash"
	"os"
	"path/filepath"

	cpebble "github.com/cockroachdb/pebble/v2"
	"google.golang.org/protobuf/encoding/protowire"

	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	enginepkg "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/codec"
)

const defaultOverlayRecordChunkSize = 32768
const defaultOverlaySeenKeyLimit = 375000

// overlayConfig carries the overlay merge tunables. The defaults are
// the production values; explicit options exist for benchmarks and
// tests.
type overlayConfig struct {
	seenKeyLimit    int64
	recordChunkSize int
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
// 64-bit maphash values; at the 250k-key seen limit the collision
// probability per merge is ~(n^2/2)/2^128 ≈ 1e-28 — negligible against
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
//     materialized once; and
//   - route buckets whose estimated key count exceeds the in-memory
//     limit through the K-way run-file path instead (overlayPlanBuckets).
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
	cfg := overlayConfig{
		seenKeyLimit:    defaultOverlaySeenKeyLimit,
		recordChunkSize: defaultOverlayRecordChunkSize,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	destSyncBytes, err := codec.EncodeSyncID(destSyncID)
	if err != nil {
		return nil, err
	}
	buckets := allBuckets()
	overlayBuckets, kwayBuckets, err := overlayPlanBuckets(ctx, sources, buckets, tmpDir, cfg.seenKeyLimit)
	if err != nil {
		return nil, err
	}

	stats := newMergeStatsAccumulator()
	rm := &asyncRemover{}
	defer rm.wait()
	var kwayRunFiles []runFile
	defer func() {
		removeRunFiles(kwayRunFiles)
	}()
	writers := make([]*overlayBucketRawWriter, len(overlayBuckets))
	seenByBucket := make([]*seenSuffixSet, len(overlayBuckets))
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
	for start := 0; start < len(sources); start += defaultKWayFanIn {
		end := min(start+defaultKWayFanIn, len(sources))
		chunk, err := openSourceChunk(ctx, tmpDir, sources[start:end], start)
		if err != nil {
			return nil, err
		}
		if err := func() error {
			defer chunk.closeAsync(rm)
			for _, source := range chunk.handles {
				sourceSyncBytes, err := codec.EncodeSyncID(source.syncID)
				if err != nil {
					return err
				}
				for bucketIdx, bucket := range overlayBuckets {
					seen := seenByBucket[bucketIdx]
					if source.rank == len(sources)-1 && seen.size() == 0 {
						if err := overlayMaterializeWholeSourceBucketSST(ctx, dest, source.engine, bucket, sourceSyncBytes, destSyncBytes, tmpDir, stats); err != nil {
							return err
						}
						continue
					}
					if err := overlaySinglePassBucket(ctx, source.engine, bucket, writers[bucketIdx], seen, sourceSyncBytes, destSyncBytes); err != nil {
						return err
					}
				}
			}
			if len(kwayBuckets) > 0 {
				run, err := buildChunkRunFileFromHandles(
					ctx,
					tmpDir,
					chunk.handles,
					fmt.Sprintf("overlay-fallback-%04d", len(kwayRunFiles)),
					destSyncID,
					destSyncBytes,
					kwayBuckets,
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
	}

	for _, writer := range writers {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := writer.flush(ctx); err != nil {
			return nil, err
		}
	}
	if len(kwayBuckets) > 0 {
		kwayStats := newMergeStatsAccumulator()
		if err := materializeRunFilesToPebble(ctx, dest, tmpDir, kwayRunFiles, destSyncBytes, kwayBuckets, kwayStats); err != nil {
			return nil, err
		}
		stats.addRecord(kwayStats.record())
	}
	return stats.record(), nil
}

func overlayPlanBuckets(ctx context.Context, sources []SourceFile, buckets []bucketSpec, tmpDir string, limit int64) ([]bucketSpec, []bucketSpec, error) {
	overlayBuckets := make([]bucketSpec, 0, len(buckets))
	kwayBuckets := make([]bucketSpec, 0, len(buckets))
	for _, bucket := range buckets {
		if total, ok := overlayEstimatedBucketKeys(sources, bucket); ok {
			if total > limit {
				kwayBuckets = append(kwayBuckets, bucket)
			} else {
				overlayBuckets = append(overlayBuckets, bucket)
			}
			continue
		}
		total, err := overlayCountBucketKeysUpTo(ctx, sources, bucket, tmpDir, limit)
		if err != nil {
			return nil, nil, err
		}
		if total > limit {
			kwayBuckets = append(kwayBuckets, bucket)
		} else {
			overlayBuckets = append(overlayBuckets, bucket)
		}
	}
	return overlayBuckets, kwayBuckets, nil
}

func overlayCountBucketKeysUpTo(ctx context.Context, sources []SourceFile, bucket bucketSpec, tmpDir string, limit int64) (int64, error) {
	var total int64
	for _, source := range sources {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		err := withSourceEngine(ctx, tmpDir, source, func(eng *enginepkg.Engine) error {
			sourceSyncBytes, err := codec.EncodeSyncID(source.SyncID)
			if err != nil {
				return err
			}
			sourceLower, sourceUpper := bucket.syncRange(sourceSyncBytes)
			iter, err := eng.DB().NewIter(&cpebble.IterOptions{LowerBound: sourceLower, UpperBound: sourceUpper})
			if err != nil {
				return err
			}
			defer iter.Close()
			for iter.First(); iter.Valid(); iter.Next() {
				total++
				if total > limit {
					return nil
				}
			}
			return iter.Error()
		})
		if err != nil {
			return 0, err
		}
		if total > limit {
			return total, nil
		}
	}
	return total, nil
}

func overlayEstimatedBucketKeys(sources []SourceFile, bucket bucketSpec) (int64, bool) {
	var total int64
	for _, source := range sources {
		if source.Stats == nil {
			return 0, false
		}
		n, ok := syncStatsBucketKeys(source.Stats, bucket)
		if !ok {
			return 0, false
		}
		total += n
	}
	return total, true
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
	sourceSyncBytes []byte,
	destSyncBytes []byte,
) error {
	destLower, _ := bucket.syncRange(destSyncBytes)
	sourceLower, sourceUpper := bucket.syncRange(sourceSyncBytes)
	iter, err := source.DB().NewIter(&cpebble.IterOptions{LowerBound: sourceLower, UpperBound: sourceUpper})
	if err != nil {
		return err
	}
	defer iter.Close()
	var destKey []byte
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		// Dedupe on the primary-key suffix before constructing the
		// destination key. Admission is hash-based and allocation-free
		// (see seenSuffixSet); the shallow discovered_at scan keeps the
		// winner rule identical to K-way / sqlite.
		sourceSuffix := iter.Key()[len(sourceLower):]
		k := seen.keyOf(sourceSuffix)
		ts, err := discoveredAtNanosFromRaw(bucket, iter.Value())
		if err != nil {
			return err
		}
		if prevTs, ok := seen.get(k); ok {
			// Key already admitted by an earlier (newer) source. Replace
			// only on a strictly newer discovered_at; ties keep the
			// earlier admission — matching runRecordIsNewer's
			// (ts, then source rank) ordering.
			if ts <= prevTs {
				continue
			}
			destKey = rewritePrimaryKeyForDestInto(destKey[:0], sourceSuffix, destLower)
			if err := writer.replaceRaw(ctx, bucket, destSyncBytes, destKey, destLower, iter.Value()); err != nil {
				return err
			}
			seen.put(k, ts)
			continue
		}
		seen.put(k, ts)
		destKey = rewritePrimaryKeyForDestInto(destKey[:0], sourceSuffix, destLower)
		if err := writer.addRaw(ctx, bucket, destSyncBytes, destKey, destLower, iter.Value()); err != nil {
			return err
		}
	}
	return iter.Error()
}

func overlayMaterializeWholeSourceBucketSST(
	ctx context.Context,
	dest *enginepkg.Engine,
	source *enginepkg.Engine,
	bucket bucketSpec,
	sourceSyncBytes []byte,
	destSyncBytes []byte,
	tmpDir string,
	stats *mergeStatsAccumulator,
) error {
	sourceLower, sourceUpper := bucket.syncRange(sourceSyncBytes)
	destLower, _ := bucket.syncRange(destSyncBytes)
	iter, err := source.DB().NewIter(&cpebble.IterOptions{LowerBound: sourceLower, UpperBound: sourceUpper})
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
	indexWriters, err := newIndexWriterSet(tmpDir, bucket.name+"-overlay-whole-index")
	if err != nil {
		return err
	}
	defer indexWriters.cleanup()

	wrotePrimary := false
	var destKey []byte
	var scratch rawIndexScratch
	emitIndexKey := indexWriters.writeKey
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		sourceKey := iter.Key()
		if len(sourceKey) < len(sourceLower) {
			return fmt.Errorf("overlay merge: source key shorter than lower bound prefix: key=%d prefix=%d", len(sourceKey), len(sourceLower))
		}
		destKey = rewritePrimaryKeyForDestInto(destKey[:0], sourceKey[len(sourceLower):], destLower)
		if err := primaryWriter.Set(destKey, iter.Value()); err != nil {
			return err
		}
		wrotePrimary = true
		// Every key in this path is a winner: this fast path only runs
		// for the last source when nothing was admitted before it.
		stats.countWinnerTotal(bucket.id)
		if bucket.forEachIndexKey != nil {
			if err := forEachIndexKeyFromRaw(bucket, destSyncBytes, destKey, destLower, iter.Value(), &scratch, stats, emitIndexKey); err != nil {
				return err
			}
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
	}
	return indexWriters.closeSortAndIngest(ctx, dest)
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
	syncBytes []byte,
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
		scratch.key = enginepkg.AppendResourceIndexKeyRawBytes(scratch.key[:0], syncBytes, parentRT, parentID, rt, id)
		return emit(scratch.key)
	case runBucketEntitlements:
		ext, err := decodePrimaryTailBytes1(destKey, destLower, scratch)
		if err != nil {
			return err
		}
		resourceRT, resourceID, err := scanEntitlementResourceBytes(value)
		if err != nil {
			return err
		}
		if len(resourceID) == 0 {
			return nil
		}
		scratch.key = enginepkg.AppendEntitlementIndexKeyRawBytes(scratch.key[:0], syncBytes, resourceRT, resourceID, ext)
		return emit(scratch.key)
	case runBucketGrants:
		ext, err := decodePrimaryTailBytes1(destKey, destLower, scratch)
		if err != nil {
			return err
		}
		entRT, entRID, entID, principalRT, principalID, needsExpansion, err := scanGrantIndexFieldsBytes(value)
		if err != nil {
			return err
		}
		stats.groupGrant(entRT)
		if len(entID) != 0 && len(principalRT) != 0 && len(principalID) != 0 {
			scratch.key = enginepkg.AppendGrantByEntitlementIndexKeyRawBytes(scratch.key[:0], syncBytes, entID, principalRT, principalID, ext)
			if err := emit(scratch.key); err != nil {
				return err
			}
		}
		if len(entRID) != 0 {
			scratch.key = enginepkg.AppendGrantByEntitlementResourceIndexKeyRawBytes(scratch.key[:0], syncBytes, entRT, entRID, ext)
			if err := emit(scratch.key); err != nil {
				return err
			}
		}
		if len(principalRT) != 0 && len(principalID) != 0 {
			scratch.key = enginepkg.AppendGrantByPrincipalIndexKeyRawBytes(scratch.key[:0], syncBytes, principalRT, principalID, ext)
			if err := emit(scratch.key); err != nil {
				return err
			}
			scratch.key = enginepkg.AppendGrantByPrincipalResourceTypeIndexKeyRawBytes(scratch.key[:0], syncBytes, principalRT, ext)
			if err := emit(scratch.key); err != nil {
				return err
			}
		}
		if needsExpansion {
			scratch.key = enginepkg.AppendGrantByNeedsExpansionIndexKeyRawBytes(scratch.key[:0], syncBytes, ext)
			if err := emit(scratch.key); err != nil {
				return err
			}
		}
		return nil
	default:
		return nil
	}
}

func decodePrimaryTailBytes1(key []byte, lower []byte, scratch *rawIndexScratch) ([]byte, error) {
	tail, err := primaryTail(key, lower)
	if err != nil {
		return nil, err
	}
	scratch.tail1, _, err = codec.DecodeTupleStringTo(scratch.tail1[:0], tail, 0)
	if err != nil {
		return nil, err
	}
	return scratch.tail1, nil
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

func withSourceEngine(ctx context.Context, tmpDir string, source SourceFile, fn func(*enginepkg.Engine) error) error {
	if source.Engine != nil {
		return fn(source.Engine)
	}
	if source.DBDir != "" {
		eng, err := enginepkg.Open(ctx, source.DBDir, enginepkg.WithReadOnly(true))
		if err != nil {
			return err
		}
		defer eng.Close()
		return fn(eng)
	}
	w, err := dotc1z.NewStore(ctx, source.Path, dotc1z.WithReadOnly(true), dotc1z.WithTmpDir(tmpDir), dotc1z.WithDecoderPool(source.DecoderPool))
	if err != nil {
		return err
	}
	// Full Close removes the read-only store's unpacked temp directory;
	// callers iterate sources one at a time, so planning never holds
	// more than one unpacked source on disk.
	defer func() { _ = w.Close(ctx) }()
	eng, ok := enginepkg.AsEngine(w)
	if !ok {
		return fmt.Errorf("overlay merge: input is not pebble: %s", source.Path)
	}
	return fn(eng)
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

func (w *overlayBucketRawWriter) addRaw(ctx context.Context, bucket bucketSpec, syncBytes []byte, destKey []byte, destLower []byte, value []byte) error {
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
	if err := forEachIndexKeyFromRaw(bucket, syncBytes, destKey, destLower, value, &w.scratch, w.stats, func(key []byte) error {
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
// admission); only the grant per-RT grouping can move, because the
// entitlement resource type lives in the value.
func (w *overlayBucketRawWriter) replaceRaw(ctx context.Context, bucket bucketSpec, syncBytes []byte, destKey []byte, destLower []byte, value []byte) error {
	if err := w.flush(ctx); err != nil {
		return err
	}
	oldVal, closer, err := w.dest.DB().Get(destKey)
	if err != nil {
		if errors.Is(err, cpebble.ErrNotFound) {
			// Seen-set hash collision: the admitted record was a different
			// key (probability ~1e-28, see seenSuffixSet). Treat the
			// candidate as a fresh admission.
			return w.addRaw(ctx, bucket, syncBytes, destKey, destLower, value)
		}
		return err
	}
	// Drop the index keys derived from the old value and move the grant
	// stats grouping before the closer invalidates oldVal. Deleting and
	// re-setting an identical index key within the same batch is fine —
	// batch ops apply in order, so the Set below wins.
	if bucket.forEachIndexKey != nil {
		if err := forEachIndexKeyFromRaw(bucket, syncBytes, destKey, destLower, oldVal, &w.scratch, nil, func(key []byte) error {
			return w.index.Delete(key, nil)
		}); err != nil {
			closer.Close()
			return err
		}
	}
	if bucket.id == runBucketGrants {
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
	}
	closer.Close()
	if err := w.primary.Set(destKey, value, nil); err != nil {
		return err
	}
	if bucket.forEachIndexKey != nil {
		if err := forEachIndexKeyFromRaw(bucket, syncBytes, destKey, destLower, value, &w.scratch, nil, func(key []byte) error {
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
