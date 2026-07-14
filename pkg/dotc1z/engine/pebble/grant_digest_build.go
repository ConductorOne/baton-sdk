package pebble

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

// Seal-time construction of the by_entitlement_principal_hash index and
// the per-entitlement grant digests, fused into the deferred EndSync
// pass (BuildDeferredGrantIndexes). This file is the ONLY writer of
// either keyspace.
//
// The deferred pass's primary-grant scan emits one hash-index row per
// grant into a second spill-sorter family (appendGrantHashIndexRow).
// The sorted rows come back in whole-key byte order, which by the index
// layout is (entitlement partition, bucket hash, principal) — exactly
// digest fold order. The final k-way merge therefore does double duty:
// it streams the rows into the index SST (ingested over the excised
// index range, like by_principal) AND folds the digests on the fly
// (grantDigestFold), accumulating each partition's buckets at max width
// and folding down to chooseDigestWidth's pick when the partition
// closes — the total count isn't known until then, and XOR
// split-independence makes the fold-down exact.
//
// Digest nodes do NOT ride the index SST: typeDigest is separated from
// the index keyspace by typeCounter/typeSession, and an excise span
// covering all three would wipe counters and sessions. The nodes are
// tiny (one root + sparse leaves per entitlement), so they go through
// ordinary batch Sets, committed in bounded chunks during the merge.

// grantHashRowScratch is the reusable per-row scratch for
// appendGrantHashIndexRow: the deferred scan calls it once per grant
// and none of it may allocate per row.
type grantHashRowScratch struct {
	keyBuf   []byte
	tupleBuf []byte
	srcKeys  [][]byte
}

// appendGrantHashIndexRow derives one hash-index row from a raw
// primary-grant (key, value) and adds it to the sorter:
//
//   - the index key is spliced out of the primary key
//     (appendGrantHashIndexKeyFromPrimary — no decode);
//   - the bucket hash is xxHash64 of the primary key's principal
//     region (grantPrincipalBucketHash64 — a raw sub-slice);
//   - the content hash covers the primary-key tail plus the grant's
//     sorted source-entitlement ids, pulled from the value with a raw
//     protobuf field scan (scanGrantSourceKeysRawBytes — no proto
//     unmarshal anywhere on this path).
//
// key/value are only borrowed (the sorter copies before returning).
func appendGrantHashIndexRow(sorter *spillSorter, primaryKey, value []byte, s *grantHashRowScratch) error {
	sep4, ok := splitGrantPrimaryKey(primaryKey)
	if !ok {
		// The caller skips rows that already failed the by_principal
		// splice; reaching here means the two splitters disagree.
		return fmt.Errorf("grant hash index: primary key %x did not split as a 6-segment identity", primaryKey)
	}
	srcs, err := scanGrantSourceKeysRawBytes(value, s.srcKeys[:0])
	if err != nil {
		return fmt.Errorf("grant hash index: scan sources: %w", err)
	}
	s.srcKeys = srcs
	if len(srcs) > 1 {
		sortByteSlices(srcs)
	}
	ch64, tuple := grantContentHash64(s.tupleBuf, primaryKey[grantPrimaryKeyPrefixLen:], srcs)
	s.tupleBuf = tuple
	bh64 := grantPrincipalBucketHash64(primaryKey[sep4+1:])
	s.keyBuf = appendGrantHashIndexKeyFromPrimary(s.keyBuf[:0], primaryKey, sep4, bh64)
	var chb [hashLen]byte
	binary.BigEndian.PutUint64(chb[:], ch64)
	return sorter.add(s.keyBuf, chb[:])
}

// digestNodeBatchFlushBytes bounds how much digest-node data a single
// batch accumulates before committing mid-merge.
const digestNodeBatchFlushBytes = 4 << 20

// markGrantDigestBuildPending durably arms the digest-build marker
// (encodeGrantDigestBuildPendingKey). MUST be fsync'd (pebble.Sync,
// even during a fresh sync) and MUST precede the build's first digest
// write: the build's own node commits may ride NoSync, and a node
// batch becoming durable ahead of the marker would reopen exactly the
// trust-a-crashed-build window the marker closes.
func (e *Engine) markGrantDigestBuildPending() error {
	if err := e.db.Set(encodeGrantDigestBuildPendingKey(), nil, pebble.Sync); err != nil {
		return fmt.Errorf("arm grant digest build marker: %w", err)
	}
	e.grantDigestBuildPending.Store(true)
	return nil
}

// clearGrantDigestBuildPending drops both forms of the marker. Called
// only once the digest state is a complete, self-consistent whole: a
// finished build (buildGrantDigestsFromSpill) or a full drop
// (dropAllGrantDigestStateLocked). The delete is fsync'd deliberately —
// the WAL is prefix-durable, so a durable clear implies every
// (possibly NoSync) write that preceded it is durable too. The
// marker's absence therefore certifies COMPLETE digest state on disk,
// never a lucky partial one.
func (e *Engine) clearGrantDigestBuildPending() error {
	if err := e.db.Delete(encodeGrantDigestBuildPendingKey(), pebble.Sync); err != nil {
		return fmt.Errorf("clear grant digest build marker: %w", err)
	}
	e.grantDigestBuildPending.Store(false)
	return nil
}

// grantDigestFold streams the merge's (index key, content hash) rows
// into digest nodes. Rows arrive in (partition, bucket hash) order, so
// each partition is a contiguous run and its buckets are touched in
// ascending order; the fold keeps a max-width (2^16) scratch — ~1.5MiB
// of counts+xors, reused across all partitions — plus the list of
// touched buckets, so per-partition close and reset are O(touched),
// never O(2^16).
type grantDigestFold struct {
	e          *Engine
	opts       *pebble.WriteOptions
	flushBytes int // digestNodeBatchFlushBytes, or the engine's test override

	counts  []int64         // 1<<digestMaxWidthBits, index = max-width bucket
	xors    [][hashLen]byte // 1<<digestMaxWidthBits
	touched []uint16        // ascending within the open partition

	partition     []byte // copy of the open partition's bytes
	havePartition bool
	rootXor       [hashLen]byte
	total         int64

	// globalXor/globalTotal accumulate across EVERY partition the fold
	// sees (never reset by closePartition, unlike rootXor/total): the
	// whole-file grant digest root written once at finish(). XOR is
	// associative/commutative, so folding it here in partition-close
	// order is exact regardless of how partitions or their buckets were
	// subdivided — same split-independence property the per-partition
	// digest relies on (digest.go).
	globalXor   [hashLen]byte
	globalTotal int64

	batch      *pebble.Batch
	partitions int64
	nodes      int64
}

func newGrantDigestFold(e *Engine) (*grantDigestFold, error) {
	opts := writeOpts(e.opts.durability)
	if e.IsFreshSync() {
		// EndSync's EndFreshSync flush is the durability boundary for
		// seal-time writes; matches the deferred pass's other writes.
		opts = pebble.NoSync
	}
	flushBytes := digestNodeBatchFlushBytes
	if e.testDigestNodeFlushBytes > 0 {
		flushBytes = e.testDigestNodeFlushBytes
	}
	f := &grantDigestFold{
		e:          e,
		opts:       opts,
		flushBytes: flushBytes,
		counts:     make([]int64, 1<<digestMaxWidthBits),
		xors:       make([][hashLen]byte, 1<<digestMaxWidthBits),
		batch:      e.db.NewBatch(),
	}
	// The build only Sets nodes: clear every prior digest first so a
	// reseal can't leave stale partitions (dropped entitlements, width
	// changes) for the comparison merge scan to read. Leads the first
	// batch; in-batch ordering keeps the new nodes on top.
	if err := f.batch.DeleteRange(DigestLowerBound(), DigestUpperBound(), nil); err != nil {
		f.abort()
		return nil, err
	}
	return f, nil
}

// add folds one merged index row. partitionBytes is the raw partition
// region of the index key (borrowed; copied on partition change),
// bucket is the max-width bucket from the key's raw hash region, and
// contentHash is the row's value.
func (f *grantDigestFold) add(partitionBytes []byte, bucket uint16, contentHash []byte) error {
	if len(contentHash) != hashLen {
		return fmt.Errorf("grant digest fold: content hash is %d bytes, want %d", len(contentHash), hashLen)
	}
	if !f.havePartition || !bytes.Equal(partitionBytes, f.partition) {
		if err := f.closePartition(); err != nil {
			return err
		}
		f.partition = append(f.partition[:0], partitionBytes...)
		f.havePartition = true
	}
	if f.counts[bucket] == 0 {
		f.touched = append(f.touched, bucket)
	}
	f.counts[bucket]++
	xorInto(f.xors[bucket][:], contentHash)
	xorInto(f.rootXor[:], contentHash)
	f.total++
	xorInto(f.globalXor[:], contentHash)
	f.globalTotal++
	return nil
}

// closePartition emits the open partition's root + leaves and resets
// the scratch. No-op when no partition is open.
func (f *grantDigestFold) closePartition() error {
	if !f.havePartition {
		return nil
	}
	partition := string(f.partition)
	width := chooseDigestWidth(f.total)
	rootKey := encodeDigestNodeKey(grantDigestSpec.indexID, partition, digestLevelRoot, nil)
	if err := f.batch.Set(rootKey, packDigestRoot(width, f.total, f.rootXor[:]), nil); err != nil {
		return err
	}
	f.nodes++
	if width > 0 {
		// Fold the touched max-width buckets (ascending) down to the
		// chosen width: consecutive buckets sharing the top `width`
		// bits merge into one leaf. Exact by XOR split-independence.
		shift := digestMaxWidthBits - width
		for i := 0; i < len(f.touched); {
			leafIdx := f.touched[i] >> shift
			var digest [hashLen]byte
			var count int64
			for i < len(f.touched) && f.touched[i]>>shift == leafIdx {
				b := f.touched[i]
				xorInto(digest[:], f.xors[b][:])
				count += f.counts[b]
				i++
			}
			var prefix [digestLeafPrefixLen]byte
			binary.BigEndian.PutUint16(prefix[:], leafIdx<<shift)
			leafKey := encodeDigestNodeKey(grantDigestSpec.indexID, partition, digestLevelLeaf, prefix[:])
			if err := f.batch.Set(leafKey, packDigestLeaf(count, digest[:]), nil); err != nil {
				return err
			}
			f.nodes++
		}
	}
	// O(touched) reset — the scratch arrays are shared across every
	// partition of the build.
	for _, b := range f.touched {
		f.counts[b] = 0
		f.xors[b] = [hashLen]byte{}
	}
	f.touched = f.touched[:0]
	f.rootXor = [hashLen]byte{}
	f.total = 0
	f.havePartition = false
	f.partitions++

	if f.batch.Len() >= f.flushBytes {
		if err := f.batch.Commit(f.opts); err != nil {
			return err
		}
		f.batch.Close()
		f.batch = f.e.db.NewBatch()
		if f.e.testDigestBuildHook != nil {
			// Crash-window seam: digest-node batches are now committed
			// (durable under WAL replay) while the hash-index ingest has
			// not run. See grant_digest_build_crash_test.go.
			if err := f.e.testDigestBuildHook("node-batch-committed"); err != nil {
				return err
			}
		}
	}
	return nil
}

// finish closes the last partition, writes the whole-file global root
// (the fold of every partition this build touched — see globalXor/
// globalTotal), and commits the tail batch. The global root lands in
// the same final batch as the last partition's nodes, so it is never
// visible without them: a crash between batches can only leave the
// global root ABSENT, never present ahead of a partition it should
// have folded in.
func (f *grantDigestFold) finish() error {
	if err := f.closePartition(); err != nil {
		return err
	}
	if err := f.batch.Set(globalGrantDigestNodeKey(), packDigestLeaf(f.globalTotal, f.globalXor[:]), nil); err != nil {
		return err
	}
	f.nodes++
	err := f.batch.Commit(f.opts)
	f.batch.Close()
	f.batch = nil
	return err
}

// abort discards the un-committed tail batch. Safe after finish (no-op).
func (f *grantDigestFold) abort() {
	if f.batch != nil {
		f.batch.Close()
		f.batch = nil
	}
}

// splitGrantHashIndexKey locates the partition and raw-hash regions of
// a hash-index key: partition = key[grantHashIndexKeyPrefixLen:sepEnd],
// bucket hash at [sepEnd+1, sepEnd+1+digestBucketHashLen). Counts the
// four partition separators from the LEFT so the walk never crosses
// the raw hash region (whose bytes may be 0x00).
func splitGrantHashIndexKey(key []byte) ([]byte, uint16, bool) {
	if len(key) < grantHashIndexKeyPrefixLen ||
		key[0] != versionV3 || key[1] != typeIndex ||
		key[2] != idxGrantByEntitlementPrincipalHash || key[3] != 0 {
		return nil, 0, false
	}
	off := grantHashIndexKeyPrefixLen
	for range 4 {
		sep := bytes.IndexByte(key[off:], 0)
		if sep < 0 {
			return nil, 0, false
		}
		off += sep + 1
	}
	if len(key) < off+digestBucketHashLen {
		return nil, 0, false
	}
	return key[grantHashIndexKeyPrefixLen : off-1], binary.BigEndian.Uint16(key[off : off+digestBucketHashLen]), true
}

// mergeGrantHashChunksToSST heap-merges the hash-index sorter's chunk
// files into one SST while streaming every row through the digest
// fold. Same merge shape as mergeSortedSpillChunksToSST; duplicate
// keys are corruption here too — (entitlement, principal) is the grant
// primary identity, so the sorter sees exactly one row per grant.
func mergeGrantHashChunksToSST(ctx context.Context, sstPath, name string, chunks []string, fold *grantDigestFold) error {
	start := time.Now()
	l := ctxzap.Extract(ctx)
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
		f, err := os.Open(chunk) // #nosec G304 - staged under the build's MkdirTemp dir.
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
	var written int64
	lastLog := start
	for len(*h) > 0 {
		item := h.pop()
		if bytes.Equal(item.key, last) {
			return fmt.Errorf("%w: bucket %s key %x", errBulkImportDuplicateKey, name, item.key)
		}
		partition, bucket, ok := splitGrantHashIndexKey(item.key)
		if !ok {
			// Impossible for keys this build emitted; corruption.
			return fmt.Errorf("grant hash index merge: malformed index key %x", item.key)
		}
		if err := fold.add(partition, bucket, item.val); err != nil {
			return err
		}
		if err := writer.add(item.key, item.val); err != nil {
			return err
		}
		written++
		// Throttled bookkeeping, same rationale as the primary merge.
		if written&0xFFFF == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
			if now := time.Now(); now.Sub(lastLog) >= 15*time.Second {
				l.Info("grant digest build: merging hash-index chunks",
					zap.Int("chunks", len(chunks)),
					zap.Int64("entries_written", written),
					zap.Int64("partitions_folded", fold.partitions),
					zap.Duration("elapsed", now.Sub(start)),
				)
				lastLog = now
			}
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
	l.Info("grant digest build: hash-index merge complete",
		zap.Int("chunks", len(chunks)),
		zap.Int64("entries_written", written),
		zap.Int64("partitions_folded", fold.partitions),
		zap.Duration("elapsed", time.Since(start)),
	)
	success = true
	return nil
}

// buildGrantDigestsFromSpill finalizes the deferred pass's hash-index
// sorter into the ingested index SST and the digest nodes, then
// backfills zero-grant entitlement roots. Runs under the engine write
// barrier (the deferred pass holds it). Any error leaves partially
// written digest nodes behind — the caller MUST drop the digest state
// on failure (dropAllGrantDigestStateLocked) so no half-built digest
// survives looking present.
//
// An in-process error the caller can drop on is only half the failure
// surface: node batches commit DURING the merge and at fold.finish(),
// before the index ingest, and committed WAL writes survive a process
// kill. The durable build-pending marker armed here (and cleared only
// once everything below has completed, or by the drop) is what keeps a
// crash inside that window from resurrecting correct-looking digest
// roots over a never-ingested hash index — see
// encodeGrantDigestBuildPendingKey.
func (e *Engine) buildGrantDigestsFromSpill(ctx context.Context, dir string, hashIdx *spillSorter) error {
	start := time.Now()
	l := ctxzap.Extract(ctx)
	chunks, err := hashIdx.finalize()
	if err != nil {
		return err
	}
	opts := writeOpts(e.opts.durability)
	if e.IsFreshSync() {
		opts = pebble.NoSync
	}
	// Arm the durable crash marker before the first digest write on
	// EITHER branch below.
	if err := e.markGrantDigestBuildPending(); err != nil {
		return err
	}
	if len(chunks) == 0 {
		// No grants at all — pebble rejects an empty SST, so clear any
		// stale state directly, then give every entitlement its
		// {count: 0} root (the "empty vs. never built" distinction).
		if err := e.db.DeleteRange(DigestLowerBound(), DigestUpperBound(), opts); err != nil {
			return err
		}
		if err := e.db.DeleteRange(GrantByEntPrincHashLowerBound(), GrantByEntPrincHashUpperBound(), opts); err != nil {
			return err
		}
		if err := e.writeMissingEntitlementDigestRoots(ctx, opts); err != nil {
			return err
		}
		// Zero grants still means the digest WAS built (present-means-
		// exact — an absent global root would tell a manifest reader to
		// recalculate instead of trusting "nothing to diff").
		if err := e.db.Set(globalGrantDigestNodeKey(), packDigestLeaf(0, zeroDigest[:]), opts); err != nil {
			return err
		}
		e.grantDigestsPresent.Store(true)
		return e.clearGrantDigestBuildPending()
	}

	fold, err := newGrantDigestFold(e)
	if err != nil {
		return err
	}
	defer fold.abort()
	sstPath := filepath.Join(dir, fmt.Sprintf("index-%02x.sst", idxGrantByEntitlementPrincipalHash))
	if err := mergeGrantHashChunksToSST(ctx, sstPath, fmt.Sprintf("index-%02x", idxGrantByEntitlementPrincipalHash), chunks, fold); err != nil {
		return err
	}
	if err := fold.finish(); err != nil {
		return err
	}
	mergeDone := time.Now()
	if e.testDigestBuildHook != nil {
		// Crash-window seam: every digest node INCLUDING the global root
		// is committed, the ingest has not run. See
		// grant_digest_build_crash_test.go.
		if err := e.testDigestBuildHook("post-finish"); err != nil {
			return err
		}
	}

	// Atomically replace the whole hash-index range with the merged SST.
	if _, err := e.db.IngestAndExcise(ctx, []string{sstPath}, nil, nil, pebble.KeyRange{
		Start: GrantByEntPrincHashLowerBound(),
		End:   GrantByEntPrincHashUpperBound(),
	}); err != nil {
		return fmt.Errorf("grant digest build: ingest/excise: %w", err)
	}

	// Grant-bearing partitions got their digests from the fold;
	// entitlements with zero grants still need a root.
	if err := e.writeMissingEntitlementDigestRoots(ctx, opts); err != nil {
		return err
	}
	e.grantDigestsPresent.Store(true)
	// The digest state is complete: consume the crash marker. Its
	// fsync'd delete also makes every NoSync node batch above durable
	// (WAL prefix ordering), so "marker absent" always means "complete
	// state on disk".
	if err := e.clearGrantDigestBuildPending(); err != nil {
		return err
	}

	l.Info("grant digest build complete",
		zap.Int64("index_rows", hashIdx.count),
		zap.Int64("partitions", fold.partitions),
		zap.Int64("digest_nodes", fold.nodes),
		zap.Duration("merge", mergeDone.Sub(start)),
		zap.Duration("total", time.Since(start)),
	)
	return nil
}

// writeMissingEntitlementDigestRoots gives every entitlement RECORD
// with no stored digest root a {width: 0, count: 0} root. Grant-bearing
// partitions were covered by the fold (including orphans that have
// grants but no entitlement record), so a missing root here means zero
// grants by construction; this pass covers entitlements the sync saw
// but granted nothing under. One pass over the entitlement primary
// keyspace — by far the smallest record type — with a point Get per
// entitlement; deliberately not fused into the grant scan.
func (e *Engine) writeMissingEntitlementDigestRoots(ctx context.Context, opts *pebble.WriteOptions) error {
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: EntitlementLowerBound(),
		UpperBound: EntitlementUpperBound(),
	})
	if err != nil {
		return err
	}
	defer iter.Close()
	batch := e.db.NewBatch()
	// Closure, not a bare defer: the loop rotates `batch` on flush and a
	// receiver-bound defer would double-Close the first batch.
	defer func() { batch.Close() }()
	emptyRoot := packDigestRoot(0, 0, zeroDigest[:])
	var scanned int64
	for iter.First(); iter.Valid(); iter.Next() {
		scanned++
		if scanned&0x3FF == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		key := iter.Key()
		if len(key) <= grantPrimaryKeyPrefixLen {
			continue
		}
		// The entitlement primary tail IS the digest partition (see the
		// partition convention in keys.go) — a raw splice, no decode.
		partition := string(key[grantPrimaryKeyPrefixLen:])
		rootKey := encodeDigestNodeKey(grantDigestSpec.indexID, partition, digestLevelRoot, nil)
		if _, closer, err := e.db.Get(rootKey); err == nil {
			closer.Close()
			continue
		} else if !errors.Is(err, pebble.ErrNotFound) {
			return err
		}
		if err := batch.Set(rootKey, emptyRoot, nil); err != nil {
			return err
		}
		if batch.Len() >= digestNodeBatchFlushBytes {
			if err := batch.Commit(opts); err != nil {
				return err
			}
			batch.Close()
			batch = e.db.NewBatch()
		}
	}
	if err := iter.Error(); err != nil {
		return err
	}
	return batch.Commit(opts)
}

// BuildGrantDigests is the full-file standalone digest build: grants
// written through PutGrantRecords / UnsafePutUniqueGrantRecords / the
// bulk importer maintain by_principal inline and never arm the
// deferred-index marker, so a sync without expansion-path writes
// reaches EndSync with deferredIdxPending false — and possibly
// millions of grants. This runs its own primary-grant scan feeding the
// same spill-sorter → merge+fold → ingest machinery the fused pass
// uses (buildGrantDigestsFromSpill). When the deferred pass DOES run,
// the fused build supersedes this (same output, one shared scan).
//
// Callers: Adapter.endSyncFinalize calls RepairMissingGrantDigests
// rather than this directly — RepairMissingGrantDigests falls back to
// this exact function whenever no digest exists yet at all
// (grantDigestsPresent false), which is always true for a brand-new
// sync (ResetForNewSync excises the digest keyspace at StartNewSync),
// so the common case is unchanged. This function stays exported and
// self-sufficient for direct callers (tests, explicit repair) that
// want an unconditional from-scratch rebuild regardless of what
// already exists.
//
// Failure semantics match the fused pass: any build error (except
// context cancellation, which stays fatal) downgrades to a loud drop of
// the digest state — absent digests are safe, half-built ones are not.
func (e *Engine) BuildGrantDigests(ctx context.Context) error {
	return e.withWriteAllowSealed(func() error {
		err := e.buildGrantDigestsStandaloneLocked(ctx)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return err
		}
		ctxzap.Extract(ctx).Error("grant digest build failed; dropping digest state — grant-diff callers must re-read grants until the next successful seal",
			zap.Error(err),
		)
		if dropErr := e.dropAllGrantDigestStateLocked(); dropErr != nil {
			return fmt.Errorf("BuildGrantDigests: drop grant digest state after failed build: %w", dropErr)
		}
		return nil
	})
}

func (e *Engine) buildGrantDigestsStandaloneLocked(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	dir, err := os.MkdirTemp("", "pebble-grant-digest-")
	if err != nil {
		return fmt.Errorf("BuildGrantDigests: mkdir temp: %w", err)
	}
	defer os.RemoveAll(dir)

	sorters := min(4, max(2, runtime.GOMAXPROCS(0)/2))
	sem := make(chan struct{}, sorters)
	hashIdx := newSpillSorter(dir, fmt.Sprintf("index-%02x", idxGrantByEntitlementPrincipalHash), sem, deferredIndexSpillChunkBytes)
	hashIdx.free = newSpillArenaFreeList(deferredIndexSpillChunkBytes, sorters+1)
	// Wait out background chunk sorts before the deferred RemoveAll
	// deletes the directory they write into; no-op after finalize.
	defer hashIdx.abort()

	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: GrantLowerBound(),
		UpperBound: GrantUpperBound(),
	})
	if err != nil {
		return fmt.Errorf("BuildGrantDigests: iter: %w", err)
	}
	iterClosed := false
	defer func() {
		if !iterClosed {
			_ = iter.Close()
		}
	}()
	var scanned, droppedMalformedKeys int64
	var scratch grantHashRowScratch
	for iter.First(); iter.Valid(); iter.Next() {
		if _, ok := splitGrantPrimaryKey(iter.Key()); !ok {
			// Same key-layout-drift/corruption case the deferred pass
			// counts; such rows cannot be represented in the digests.
			droppedMalformedKeys++
			continue
		}
		if err := appendGrantHashIndexRow(hashIdx, iter.Key(), iter.Value(), &scratch); err != nil {
			return err
		}
		scanned++
		if scanned&0xFFFF == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
	}
	if err := iter.Error(); err != nil {
		return fmt.Errorf("BuildGrantDigests: iter error: %w", err)
	}
	if droppedMalformedKeys > 0 {
		ctxzap.Extract(ctx).Error("grant digest build: grant primary keys did not decode as 6-segment identities; their rows are NOT represented in the digests",
			zap.Int64("dropped", droppedMalformedKeys),
		)
	}
	// Release the iterator's pinned version before the build's
	// IngestAndExcise replaces the hash-index range.
	iterClosed = true
	if err := iter.Close(); err != nil {
		return fmt.Errorf("BuildGrantDigests: close iter: %w", err)
	}
	return e.buildGrantDigestsFromSpill(ctx, dir, hashIdx)
}

// dropAllGrantDigestStateLocked is DropAllGrantDigestState for callers
// already holding the engine write barrier (the failed-build handlers,
// Open's interrupted-build recovery), plus the marker consumption those
// callers need: the drop restores the always-safe "digests absent"
// state, which is exactly what the build-pending marker demands, so it
// is cleared here in the same stroke. The clear's fsync'd delete runs
// LAST — WAL prefix ordering makes the (possibly NoSync) tombstones
// above durable before the marker's absence is.
func (e *Engine) dropAllGrantDigestStateLocked() error {
	e.grantDigestsPresent.Store(false)
	opts := writeOpts(e.opts.durability)
	if e.IsFreshSync() {
		opts = pebble.NoSync
	}
	if err := e.db.DeleteRange(DigestLowerBound(), DigestUpperBound(), opts); err != nil {
		return err
	}
	if err := e.db.DeleteRange(GrantByEntPrincHashLowerBound(), GrantByEntPrincHashUpperBound(), opts); err != nil {
		return err
	}
	return e.clearGrantDigestBuildPending()
}
