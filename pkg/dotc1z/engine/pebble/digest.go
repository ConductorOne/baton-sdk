package pebble

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"
)

// Bucketed XOR set digests over bucket-hash indexes.
//
// Goal: answer "does this partition hold exactly the same records as
// some other sync/file?" with a single key read, and when the answer is
// no, identify which hash buckets differ so a caller can load only
// those records instead of re-reading the whole partition.
//
// The digest is generic over any secondary index matching the shape
// described on digestIndexSpec — a partition prefix followed by a raw
// fixed-width bucket hash, with a per-record content hash as the
// value. Two properties make such an index the right substrate:
//
//   - hash-major order is identical across two files that hold the same
//     records, so a streamed fold produces the same digest; and
//   - a bucket is a contiguous bit-range of the hash, which is a
//     contiguous range of the index key, so "all records in bucket P"
//     is a single range scan.
//
// The grant instantiation (partition = entitlement, bucket hash =
// hash(principal identity)) lives in grant_digest.go.
//
// Combiner. A node's digest is the XOR of every content hash beneath
// it (the content hashes themselves are whatever the index writer
// chose — only the combiner is XOR). XOR is homomorphic (parent = XOR
// of children), order-independent, and invertible, which buys three
// things:
//
//   - split-independence: a bucket's digest depends only on the records
//     in its hash range, never on how the range is subdivided, so
//     buckets from digests of different widths compare directly (a
//     width-w bucket is the XOR of its two width-(w+1) halves);
//   - O(1) incremental maintenance: post-seal insert/overwrite/delete
//     XOR the record's content hash into/out of the root and the one
//     leaf on its bucket path (see digestMutator);
//   - the empty digest is all-zero (the XOR identity), so an absent
//     leaf reads as {count: 0, digest: 0}.
//
// Every node also stores its record COUNT. Comparison always checks the
// (count, digest) pair, so a non-empty node whose hashes happened to XOR
// to zero can never be conflated with an empty/absent one. XOR
// set-hashing is not adversarially collision-resistant
// (Bellare–Micciancio); a digest is an optimization, not a trust
// boundary — see RFC 0003 §9. Index writers should fold every
// key-distinguishing field into the content hash so duplicate leaves
// (and thus in-partition self-cancellation) are impossible by
// construction.
//
// Shape. A flat hash table, not a multi-level radix tree: one root plus
// a single leaf level of 2^width buckets, where width — in BITS of the
// bucket hash, 0..digestMaxWidthBits — is chosen per partition so the
// average bucket holds at most digestTargetBucketSize records
// (chooseDigestWidth). Width 0 means root-only (small and empty
// partitions cost exactly one stored node). Growing capacity one bit at
// a time keeps realized bucket occupancy within a 2x band of the
// target; a byte-per-level radix could only pick capacities of 256^k,
// so a partition just past a boundary would store up to ~256x more
// nodes than needed.
//
// Interior levels are deliberately absent: hierarchical pruning only
// pays when the leaf level is too large to scan, and at <= 2^16 leaves
// a contiguous range scan beats a level-by-level descent. Comparison is
// a single merge scan of both sides' leaf levels (see
// dirtyPartitionBuckets); cross-width comparison folds the finer side's
// leaves down to the coarser width on the fly, which split-independence
// makes exact. For the same reason digestTargetBucketSize is a soft
// tuning knob, not an ABI constant: digests built with different
// targets still compare correctly.
//
// Leaves are stored sparsely — a leaf is materialized iff its bucket
// holds >=1 record. The root is always materialized (it is the "digest
// was built" marker — absence of the root means "never built", never
// "empty").
//
// On-disk ABI. The bucket-hash width, each index's content-hash
// definition, the combiner, the leaf-prefix encoding, and the node
// value framing are all part of the stored format: changing
// digestBucketHashLen, an index's content-hash field set, the combiner,
// or the framing requires an index-migration version bump (see
// index_migrations.go). digestTargetBucketSize is NOT part of the ABI
// (see above).

const (
	// digestBucketHashLen is the width, in bytes, of the raw bucket
	// hash embedded in a digested index's key. It is exactly the bytes
	// digestMaxWidthBits can address (16 bits = 2 bytes); storing more
	// would be dead weight on every index row, since only the top
	// digestMaxWidthBits ever select a bucket. Collisions in the bucket
	// address are harmless (they only co-locate records — the index
	// key's tail still distinguishes rows). ABI.
	digestBucketHashLen = 2

	// digestTargetBucketSize is the record count a single leaf bucket
	// aims to hold. The width is grown until 2^width buckets bring the
	// average bucket under this. Tunable without a migration: the width
	// is read from the stored root and cross-width comparison folds to
	// the coarser side.
	digestTargetBucketSize = 512

	// digestMaxWidthBits caps the leaf-level width. 2^16 buckets keeps
	// the comparison's full leaf scan trivially cheap; past the cap
	// (digestTargetBucketSize << 16 records) buckets simply grow beyond
	// the target.
	digestMaxWidthBits = 16

	// digestLeafPrefixLen is the stored byte width of a leaf key's
	// bucket prefix: the bucket index LEFT-ALIGNED in 16 bits, so leaf
	// keys sort in bucket-hash order at every width and folding to a
	// coarser width is "take the top bits". ABI.
	digestLeafPrefixLen = 2
)

// Node-key levels: the root is level 0 (empty prefix); the single leaf
// level is 1 (digestLeafPrefixLen-byte prefix). See encodeDigestNodeKey.
const (
	digestLevelRoot byte = 0
	digestLevelLeaf byte = 1
)

// hashLen is the width of a content hash (index value) and of a node
// digest.
const hashLen = 8

// zeroDigest is the XOR identity — the digest of an empty/absent node.
var zeroDigest [hashLen]byte

// xorInto XORs src into dst in place, over min(len(dst), len(src)).
func xorInto(dst, src []byte) {
	for i := range min(len(dst), len(src)) {
		dst[i] ^= src[i]
	}
}

// digestIndexSpec describes one bucket-hash index the digest core can
// fold. The index must have the shape
//
//	index key   = partitionPrefix(partition) | <raw bucket hash: digestBucketHashLen bytes> | <anything>
//	index value = content hash (hashLen bytes)
//
// The content hash defines record identity for the diff and must fold
// every key-distinguishing field (see the package comment); the bucket
// hash must be derived from fields that are stable across syncs.
type digestIndexSpec struct {
	// indexID discriminates this index's nodes inside the typeDigest
	// keyspace. Conventionally the digested index's own idx* byte. ABI.
	indexID byte

	// partitionPrefix returns the index-key prefix covering one
	// partition's entries, ending immediately before the raw bucket
	// hash (trailing separator included).
	partitionPrefix func(partition string) []byte
}

// DigestBucket addresses one hash bucket of a partition: the records
// whose bucket hash starts with the top Bits bits of Index. The zero
// value (Bits 0) addresses the whole partition.
type DigestBucket struct {
	Index uint32
	Bits  int
}

// leafKeyPrefix returns the stored digestLeafPrefixLen-byte node-key
// prefix for a leaf bucket: the index left-aligned in 16 bits.
// Requires 1 <= Bits <= digestMaxWidthBits.
func (b DigestBucket) leafKeyPrefix() []byte {
	out := make([]byte, digestLeafPrefixLen)
	binary.BigEndian.PutUint16(out, uint16(b.Index)<<(16-b.Bits)) //nolint:gosec // Index < 2^Bits <= 2^16 by construction
	return out
}

// bucketOfHash returns the width-`bits` bucket holding bucket hash bh.
// Requires 1 <= bits <= digestMaxWidthBits.
func bucketOfHash(bh []byte, bits int) DigestBucket {
	u := binary.BigEndian.Uint16(bh[:digestLeafPrefixLen])
	return DigestBucket{Index: uint32(u >> (16 - bits)), Bits: bits}
}

// bucketBounds returns the index key range [lower, upper) covering a
// bucket's records in one partition. The raw bucket hash is a clean
// byte region of the index key, so a bit-granular bucket maps to plain
// uint64 arithmetic on that region.
func (s digestIndexSpec) bucketBounds(partition string, b DigestBucket) ([]byte, []byte) {
	prefix := s.partitionPrefix(partition)
	if b.Bits == 0 {
		return prefix, upperBoundOf(prefix)
	}
	boundAt := func(hash uint64) []byte {
		// hash carries its bucket bits in the top digestMaxWidthBits, so
		// the bound is the top digestBucketHashLen bytes of the big-endian
		// uint64 appended to the partition prefix.
		var full [8]byte
		binary.BigEndian.PutUint64(full[:], hash)
		out := make([]byte, 0, len(prefix)+digestBucketHashLen)
		out = append(out, prefix...)
		return append(out, full[:digestBucketHashLen]...)
	}
	// Signed shift count (Go >= 1.13): b.Bits is in [1, digestMaxWidthBits]
	// here (the Bits==0 case returned above), so shift is in [48, 63] —
	// no uint conversion, so no overflow-checker warning to suppress.
	shift := 64 - b.Bits
	lower := boundAt(uint64(b.Index) << shift)
	if uint64(b.Index)+1 == uint64(1)<<b.Bits {
		// Last bucket: the range runs to the end of the hash space.
		return lower, upperBoundOf(prefix)
	}
	return lower, boundAt((uint64(b.Index) + 1) << shift)
}

// chooseDigestWidth picks the leaf-level width (in bits) for a
// partition with the given record count: 0 when the count fits one
// target-sized bucket, else the smallest width whose 2^width buckets
// bring the average bucket under digestTargetBucketSize, capped at
// digestMaxWidthBits.
func chooseDigestWidth(count int64) int {
	width := 0
	capacity := int64(digestTargetBucketSize)
	for capacity < count && width < digestMaxWidthBits {
		capacity *= 2
		width++
	}
	return width
}

// Node value framing. The root prepends the chosen width so a reader
// knows the leaf granularity without scanning.
//
//	leaf body:  count(8 BE) | digest(hashLen)
//	root body:  width(1)    | count(8 BE) | digest(hashLen)
//
// An ABSENT leaf is {count: 0, digest: 0} by definition — readers
// substitute that for any missing key. An absent ROOT means "digest
// never built" (never "empty"); readers must fall back to the on-demand
// fold, not assume zero.

func packDigestRoot(widthBits int, count int64, digest []byte) []byte {
	buf := make([]byte, 0, 1+8+len(digest))
	// Mask to a byte: widthBits is in [0, digestMaxWidthBits], and the
	// mask makes that provable to the overflow checker (no nolint, which
	// would be flagged unused by gosec versions that don't warn here).
	buf = append(buf, byte(widthBits&0xFF))
	var n [8]byte
	binary.BigEndian.PutUint64(n[:], uint64(count)) //nolint:gosec // non-negative row count
	buf = append(buf, n[:]...)
	return append(buf, digest...)
}

// unpackDigestRoot returns (widthBits, count, digest, ok). ok is false
// when the value is not a well-formed root blob.
func unpackDigestRoot(val []byte) (int, int64, []byte, bool) {
	if len(val) != 1+8+hashLen || int(val[0]) > digestMaxWidthBits {
		return 0, 0, nil, false
	}
	widthBits := int(val[0])
	count := int64(binary.BigEndian.Uint64(val[1:9])) //nolint:gosec // count is a non-negative row count
	return widthBits, count, val[9:], true
}

func packDigestLeaf(count int64, digest []byte) []byte {
	buf := make([]byte, 0, 8+len(digest))
	var n [8]byte
	binary.BigEndian.PutUint64(n[:], uint64(count)) //nolint:gosec // non-negative row count
	buf = append(buf, n[:]...)
	return append(buf, digest...)
}

// unpackDigestLeaf returns (count, digest, ok) for a leaf node body.
func unpackDigestLeaf(val []byte) (int64, []byte, bool) {
	if len(val) != 8+hashLen {
		return 0, nil, false
	}
	count := int64(binary.BigEndian.Uint64(val[:8])) //nolint:gosec // non-negative count
	return count, val[8:], true
}

// buildPartitionDigest counts a partition's index entries (pass 1),
// picks the width from that count, and delegates the fold to
// buildPartitionDigestAtWidth (pass 2).
func (e *Engine) buildPartitionDigest(ctx context.Context, spec digestIndexSpec, partition string) error {
	prefix := spec.partitionPrefix(partition)
	count, err := e.countKeysInRange(prefix, upperBoundOf(prefix))
	if err != nil {
		return err
	}
	return e.buildPartitionDigestAtWidth(ctx, spec, partition, chooseDigestWidth(count))
}

// buildPartitionDigestAtWidth folds the index for one partition into a
// root plus every non-empty leaf, in a single streaming pass — O(1)
// memory regardless of partition size.
//
// The pass starts by range-deleting the partition's whole node
// keyspace: the build only ever Sets nodes, so without the clear a
// rebuild that changes width or empties a bucket would leave stale
// leaves that the comparison merge scan (which enumerates leaves from
// the node keyspace) would read — and a stale digest that happens to
// match the peer prunes a real diff. Old and new framings are
// byte-length identical, so stale nodes are not detectable by
// inspection.
//
// Sorted index order means each bucket's entries are contiguous, so the
// single "open" leaf closes exactly when its prefix changes. Only
// non-empty leaves are ever opened, so sparsity is automatic, not a
// prune pass.
//
// The width is taken as a parameter rather than derived so the
// width-selection seam can be exercised directly: tests force a width
// that the natural count→width mapping would only produce at a very
// large record count, which is how the cross-width comparison path gets
// covered without seeding hundreds of thousands of records.
func (e *Engine) buildPartitionDigestAtWidth(ctx context.Context, spec digestIndexSpec, partition string, widthBits int) error {
	prefix := spec.partitionPrefix(partition)
	upper := upperBoundOf(prefix)
	nodeLower := encodeDigestPartitionPrefix(spec.indexID, partition)
	nodeUpper := upperBoundOf(nodeLower)

	return e.withWrite(func() error {
		batch := e.db.NewBatch()
		defer batch.Close()

		// Clear any prior build (see function comment). In-batch
		// ordering makes this safe: the Sets below land after the
		// tombstone and survive it.
		if err := batch.DeleteRange(nodeLower, nodeUpper, nil); err != nil {
			return err
		}

		iter, err := e.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: upper})
		if err != nil {
			return err
		}
		defer iter.Close()

		// lowMask clears the sub-bucket bits of a left-aligned 16-bit
		// prefix, leaving the leaf's stored key prefix value.
		var lowMask uint16
		if widthBits > 0 {
			lowMask = ^uint16(0) >> widthBits
		}

		var (
			leafOpen   bool
			leafLV     uint16 // left-aligned stored prefix of the open leaf
			leafDigest [hashLen]byte
			leafCount  int64
			rootDigest [hashLen]byte
			total      int64
		)
		flushLeaf := func() error {
			if !leafOpen {
				return nil
			}
			var lp [digestLeafPrefixLen]byte
			binary.BigEndian.PutUint16(lp[:], leafLV)
			key := encodeDigestNodeKey(spec.indexID, partition, digestLevelLeaf, lp[:])
			return batch.Set(key, packDigestLeaf(leafCount, leafDigest[:]), nil)
		}

		for iter.First(); iter.Valid(); iter.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			key := iter.Key()
			if len(key) < len(prefix)+digestBucketHashLen {
				continue // malformed; skip defensively
			}
			val := iter.Value() // per-record content hash
			if len(val) != hashLen {
				// Index writers always emit exactly hashLen bytes
				// (grantContentHash); a wrong length is a corrupt or
				// mis-encoded entry. xorInto would fold only a prefix and
				// quietly corrupt the digest, so reject it. At seal the
				// caller downgrades a build error to "no digest" (readers
				// fall back to the on-demand fold), so this fails safe.
				return fmt.Errorf("buildPartitionDigestAtWidth: content hash for %q is %d bytes, want %d", partition, len(val), hashLen)
			}
			if widthBits > 0 {
				lv := binary.BigEndian.Uint16(key[len(prefix):]) &^ lowMask
				if !leafOpen || lv != leafLV {
					if err := flushLeaf(); err != nil {
						return err
					}
					leafLV = lv
					leafDigest = [hashLen]byte{}
					leafCount = 0
					leafOpen = true
				}
				xorInto(leafDigest[:], val)
				leafCount++
			}
			xorInto(rootDigest[:], val)
			total++
		}
		if err := iter.Error(); err != nil {
			return err
		}
		if err := flushLeaf(); err != nil {
			return err
		}

		// Root is written unconditionally — even at count 0 — as the
		// "digest was built" marker.
		rootKey := encodeDigestNodeKey(spec.indexID, partition, digestLevelRoot, nil)
		if err := batch.Set(rootKey, packDigestRoot(widthBits, total, rootDigest[:]), nil); err != nil {
			return err
		}

		opts := writeOpts(e.opts.durability)
		if e.IsFreshSync() {
			opts = pebble.NoSync
		}
		return batch.Commit(opts)
	})
}

// countKeysInRange counts keys in [lower, upper) without materializing
// values. Used by the build's count→width pass and by the diff driver's
// primary-vs-index coverage guard.
func (e *Engine) countKeysInRange(lower, upper []byte) (int64, error) {
	iter, err := e.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return 0, err
	}
	defer iter.Close()
	var n int64
	for iter.First(); iter.Valid(); iter.Next() {
		n++
	}
	return n, iter.Error()
}

// DigestRoot is a partition's stored root digest.
type DigestRoot struct {
	Hash  []byte
	Bits  int // leaf-level width in bits; 0 = root-only digest
	Count int64
}

// getPartitionDigestRoot returns the stored root for a partition. ok is
// false when no digest has been built for it (the caller can fall back
// to computeBucketDigest, which derives the same digest from the index
// on demand).
func (e *Engine) getPartitionDigestRoot(spec digestIndexSpec, partition string) (DigestRoot, bool, error) {
	val, closer, err := e.db.Get(encodeDigestNodeKey(spec.indexID, partition, digestLevelRoot, nil))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return DigestRoot{}, false, nil
		}
		return DigestRoot{}, false, err
	}
	defer closer.Close()
	widthBits, count, h, valid := unpackDigestRoot(val)
	if !valid {
		return DigestRoot{}, false, fmt.Errorf("getPartitionDigestRoot: malformed root for %q", partition)
	}
	out := make([]byte, len(h))
	copy(out, h)
	return DigestRoot{Hash: out, Bits: widthBits, Count: count}, true, nil
}

// getDigestLeaf reads one stored leaf by its key prefix. An absent leaf
// returns (0, zero digest, present=false, nil) — the XOR identity.
func (e *Engine) getDigestLeaf(spec digestIndexSpec, partition string, leafPrefix []byte) (int64, []byte, bool, error) {
	val, closer, err := e.db.Get(encodeDigestNodeKey(spec.indexID, partition, digestLevelLeaf, leafPrefix))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return 0, zeroDigest[:], false, nil
		}
		return 0, nil, false, err
	}
	defer closer.Close()
	count, digest, ok := unpackDigestLeaf(val)
	if !ok {
		return 0, nil, false, fmt.Errorf("getDigestLeaf: malformed leaf for %q", partition)
	}
	out := make([]byte, hashLen)
	copy(out, digest)
	return count, out, true, nil
}

// foldedBucket is one entry of a folded leaf scan: the (XOR, count)
// aggregate of the consecutive stored leaves sharing the top `bits`
// bits of their bucket index.
type foldedBucket struct {
	idx    uint32
	count  int64
	digest [hashLen]byte
}

// foldedLeafBuckets scans a partition's stored leaf level and folds it
// to width foldBits (which must be <= the width the digest was built
// at), returning the non-empty buckets in index order. One contiguous
// range scan; folding is exact because leaf prefixes are left-aligned
// (so keys sort in bucket-hash order at any width) and XOR digests are
// split-independent.
func (e *Engine) foldedLeafBuckets(ctx context.Context, spec digestIndexSpec, partition string, foldBits int) ([]foldedBucket, error) {
	stem := encodeDigestNodeKey(spec.indexID, partition, digestLevelLeaf, nil)
	iter, err := e.db.NewIter(&pebble.IterOptions{LowerBound: stem, UpperBound: upperBoundOf(stem)})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	var out []foldedBucket
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		key := iter.Key()
		if len(key) != len(stem)+digestLeafPrefixLen {
			continue // malformed; skip defensively
		}
		lv := binary.BigEndian.Uint16(key[len(stem):])
		idx := uint32(lv >> (16 - foldBits))
		count, digest, ok := unpackDigestLeaf(iter.Value())
		if !ok {
			return nil, fmt.Errorf("foldedLeafBuckets: malformed leaf for %q", partition)
		}
		if n := len(out); n > 0 && out[n-1].idx == idx {
			out[n-1].count += count
			xorInto(out[n-1].digest[:], digest)
			continue
		}
		fb := foldedBucket{idx: idx, count: count}
		copy(fb.digest[:], digest)
		out = append(out, fb)
	}
	return out, iter.Error()
}

// computeBucketDigest folds the index over a single bucket (the zero
// bucket = whole partition = the root) and returns the content-defined
// XOR digest plus the record count. This is the authoritative
// definition of a node; stored nodes are a cache of it.
// Split-independent: the digest depends only on the records in the
// bucket's hash range, not on any digest's width.
func (e *Engine) computeBucketDigest(ctx context.Context, spec digestIndexSpec, partition string, bucket DigestBucket) ([]byte, int64, error) {
	lower, upper := spec.bucketBounds(partition, bucket)
	iter, err := e.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return nil, 0, err
	}
	defer iter.Close()
	digest := make([]byte, hashLen)
	var count int64
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return nil, 0, err
		}
		val := iter.Value()
		if len(val) != hashLen {
			// See buildPartitionDigestAtWidth: a mis-length index value is
			// corruption; reject it rather than silently fold a prefix.
			return nil, 0, fmt.Errorf("computeBucketDigest: content hash for %q is %d bytes, want %d", partition, len(val), hashLen)
		}
		xorInto(digest, val)
		count++
	}
	if err := iter.Error(); err != nil {
		return nil, 0, err
	}
	return digest, count, nil
}

// computeBucketsAtWidth scans the index for one partition and rolls it
// up into width-`bits` buckets directly — O(records), one contiguous
// index scan, O(1) memory. It is the fallback for
// GetEntitlementGrantDigestNodes when the requested level is finer than
// the stored digest's width (foldedLeafBuckets only goes as fine as what
// was built). bits must be in [1, digestMaxWidthBits]; the caller clamps
// to the bucket-hash resolution. Returns the non-empty buckets in index
// order — index entries are bucket-hash-major, so each bucket's records
// are contiguous and close when the top-`bits` prefix changes.
func (e *Engine) computeBucketsAtWidth(ctx context.Context, spec digestIndexSpec, partition string, bits int) ([]foldedBucket, error) {
	prefix := spec.partitionPrefix(partition)
	iter, err := e.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: upperBoundOf(prefix)})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	var out []foldedBucket
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		key := iter.Key()
		if len(key) < len(prefix)+digestBucketHashLen {
			continue // malformed; skip defensively
		}
		// The bucket hash is the digestBucketHashLen raw bytes right after
		// the partition prefix; its top `bits` bits select the bucket.
		idx := uint32(binary.BigEndian.Uint16(key[len(prefix):]) >> (16 - bits))
		val := iter.Value()
		if len(val) != hashLen {
			// See buildPartitionDigestAtWidth: a mis-length index value is
			// corruption; reject it rather than fold a prefix.
			return nil, fmt.Errorf("computeBucketsAtWidth: content hash for %q is %d bytes, want %d", partition, len(val), hashLen)
		}
		if len(out) == 0 || out[len(out)-1].idx != idx {
			out = append(out, foldedBucket{idx: idx})
		}
		i := len(out) - 1
		xorInto(out[i].digest[:], val)
		out[i].count++
	}
	return out, iter.Error()
}

// dirtyPartitionBuckets compares this engine's partition against
// other's and returns the buckets whose records differ. A single zero
// bucket (Bits 0) means "the whole partition differs" (used when the
// comparison granularity is the root, e.g. small partitions). A nil
// (empty) result means the two are identical.
//
// The fast path is a single root read per side. On mismatch both sides'
// leaf levels are folded to compareBits — the narrower digest's width,
// where both sides have directly comparable buckets (XOR digests are
// split-independent) — and merge-compared in one pass. Each side's fold
// is a single contiguous range scan of its stored leaves, so cost is
// O(leaves), bounded by count/digestTargetBucketSize per side.
//
// A missing root means the digest was never built on that side — NOT
// that the partition is empty — so both sides are compared via the
// authoritative on-demand fold instead.
func (e *Engine) dirtyPartitionBuckets(ctx context.Context, spec digestIndexSpec, other *Engine, partition string) ([]DigestBucket, error) {
	rootA, okA, err := e.getPartitionDigestRoot(spec, partition)
	if err != nil {
		return nil, err
	}
	rootB, okB, err := other.getPartitionDigestRoot(spec, partition)
	if err != nil {
		return nil, err
	}

	if !okA || !okB {
		ha, ca, err := e.computeBucketDigest(ctx, spec, partition, DigestBucket{})
		if err != nil {
			return nil, err
		}
		hb, cb, err := other.computeBucketDigest(ctx, spec, partition, DigestBucket{})
		if err != nil {
			return nil, err
		}
		if ca == cb && bytes.Equal(ha, hb) {
			return nil, nil
		}
		return []DigestBucket{{}}, nil
	}

	if rootA.Count == rootB.Count && bytes.Equal(rootA.Hash, rootB.Hash) {
		return nil, nil
	}

	// Roots differ. The comparison granularity is the narrower digest's
	// width; at width 0 there is nothing below the root.
	compareBits := min(rootA.Bits, rootB.Bits)
	if compareBits == 0 {
		return []DigestBucket{{}}, nil
	}

	fa, err := e.foldedLeafBuckets(ctx, spec, partition, compareBits)
	if err != nil {
		return nil, err
	}
	fb, err := other.foldedLeafBuckets(ctx, spec, partition, compareBits)
	if err != nil {
		return nil, err
	}

	// Merge the two sorted folded-bucket streams. A bucket present on
	// only one side is dirty by construction (stored leaves are never
	// empty); a shared bucket is dirty iff its (count, digest) differs.
	var dirty []DigestBucket
	i, j := 0, 0
	for i < len(fa) || j < len(fb) {
		switch {
		case j == len(fb) || (i < len(fa) && fa[i].idx < fb[j].idx):
			dirty = append(dirty, DigestBucket{Index: fa[i].idx, Bits: compareBits})
			i++
		case i == len(fa) || fb[j].idx < fa[i].idx:
			dirty = append(dirty, DigestBucket{Index: fb[j].idx, Bits: compareBits})
			j++
		default:
			if fa[i].count != fb[j].count || fa[i].digest != fb[j].digest {
				dirty = append(dirty, DigestBucket{Index: fa[i].idx, Bits: compareBits})
			}
			i++
			j++
		}
	}

	// Roots differed but the merge found nothing: with consistent
	// digests that's impossible (the root is the XOR of the leaves), so
	// a stored node is stale or corrupt. Fail safe — whole partition
	// dirty; the next rebuild heals the digest.
	if len(dirty) == 0 {
		return []DigestBucket{{}}, nil
	}
	return dirty, nil
}

// --- Incremental maintenance (post-seal) ---

// digestMutator accumulates per-node (XOR, count) deltas for a batch of
// post-seal record mutations against one digested index, and applies
// each touched node exactly once.
//
// Why an accumulator instead of read-modify-write per mutation: the
// updates target a plain pebble.Batch, which does NOT read through its
// own writes — and every mutation in a batch touches the root node, so
// naive per-mutation RMW would lose deltas. Accumulating also collapses
// N writes per node into one. All record writers run under withWrite's
// mutex, so reading current node values from the DB inside apply is
// race-free.
//
// Lifecycle: one mutator per write batch. Callers feed removeHash(old)
// / addHash(new) as they process records (an overwrite that moves a
// record to a different bucket is exactly remove+add), then call
// apply(batch) once before commit.
//
// Partitions whose digest was never built (no stored root) are skipped:
// the seal-time build or the on-Open backfill will construct them from
// the index. This also makes the mutator free during a fresh sync — but
// callers on the fresh-sync bulk path should skip constructing one
// anyway to avoid the per-partition root probe.
type digestMutator struct {
	e               *Engine
	spec            digestIndexSpec
	parts           map[string]*mutatorPartition // keyed by string(root node key)
	createIfAbsent  bool                         // treat missing root as zero instead of dropping
}

type mutatorPartition struct {
	partition  string
	rootKey    []byte
	present    bool          // stored root exists; if false all deltas are dropped
	bits       int           // leaf-level width read from the stored root
	rootCount  int64         // count read from the stored root
	rootDigest [hashLen]byte // digest read from the stored root
	xor        [hashLen]byte // accumulated root delta
	countDelta int64
	nodes      map[string]*mutatorNode // leaves, keyed by string(node key)
}

type mutatorNode struct {
	key        []byte
	xor        [hashLen]byte
	countDelta int64
}

func newDigestMutator(e *Engine, spec digestIndexSpec) *digestMutator {
	return &digestMutator{e: e, spec: spec, parts: make(map[string]*mutatorPartition)}
}

// partFor returns the (cached) per-partition state, probing the stored
// root on first touch. The cached root snapshot stays valid for the
// mutator's lifetime because all writers serialize through withWrite.
func (m *digestMutator) partFor(partition string) (*mutatorPartition, error) {
	rootKey := encodeDigestNodeKey(m.spec.indexID, partition, digestLevelRoot, nil)
	k := string(rootKey)
	if mp, ok := m.parts[k]; ok {
		return mp, nil
	}
	mp := &mutatorPartition{partition: partition, rootKey: rootKey, nodes: make(map[string]*mutatorNode)}
	val, closer, err := m.e.db.Get(rootKey)
	switch {
	case err == nil:
		widthBits, count, digest, ok := unpackDigestRoot(val)
		closer.Close()
		if ok {
			mp.present = true
			mp.bits = widthBits
			mp.rootCount = count
			copy(mp.rootDigest[:], digest)
		}
		// Malformed root: leave present=false so mutations are dropped;
		// the stale root heals at the next rebuild.
	case errors.Is(err, pebble.ErrNotFound):
		// No digest — treat as zero identity if createIfAbsent, else no-ops.
		if m.createIfAbsent {
			mp.present = true
		}
	default:
		return nil, err
	}
	m.parts[k] = mp
	return mp, nil
}

// addHash records the insertion of a record with the given bucket and
// content hashes into its partition's digest.
func (m *digestMutator) addHash(partition string, bucketHash, contentHash []byte) error {
	return m.delta(partition, bucketHash, contentHash, 1)
}

// removeHash records the removal of a record from its partition's
// digest.
func (m *digestMutator) removeHash(partition string, bucketHash, contentHash []byte) error {
	return m.delta(partition, bucketHash, contentHash, -1)
}

func (m *digestMutator) delta(partition string, bucketHash, contentHash []byte, sign int64) error {
	mp, err := m.partFor(partition)
	if err != nil {
		return err
	}
	if !mp.present {
		return nil
	}
	xorInto(mp.xor[:], contentHash)
	mp.countDelta += sign
	if mp.bits == 0 {
		return nil
	}
	key := encodeDigestNodeKey(m.spec.indexID, partition, digestLevelLeaf, bucketOfHash(bucketHash, mp.bits).leafKeyPrefix())
	nk := string(key)
	n, ok := mp.nodes[nk]
	if !ok {
		n = &mutatorNode{key: key}
		mp.nodes[nk] = n
	}
	xorInto(n.xor[:], contentHash)
	n.countDelta += sign
	return nil
}

// apply folds the accumulated deltas into the stored nodes via batch.
// Nodes whose delta cancelled to zero (e.g. an overwrite that changed
// only excluded fields) are skipped; a leaf whose count reaches zero is
// deleted (restoring sparsity); the root is rewritten in place. A count
// that would go negative means the stored digest disagrees with the
// mutation stream — the digest is dropped wholesale (DeleteRange), so
// readers fall back to the on-demand fold until the next rebuild.
func (m *digestMutator) apply(batch *pebble.Batch) error {
	for _, mp := range m.parts {
		if !mp.present {
			continue
		}
		if err := m.applyPartition(batch, mp); err != nil {
			return err
		}
	}
	return nil
}

func (m *digestMutator) applyPartition(batch *pebble.Batch, mp *mutatorPartition) error {
	if mp.countDelta == 0 && mp.xor == zeroDigest && len(mp.nodes) == 0 {
		return nil
	}
	dropDigest := func() error {
		// In-batch ordering: this tombstone lands after any node Sets
		// already staged for this partition and removes them too.
		lo := encodeDigestPartitionPrefix(m.spec.indexID, mp.partition)
		return batch.DeleteRange(lo, upperBoundOf(lo), nil)
	}
	if mp.rootCount+mp.countDelta < 0 {
		return dropDigest()
	}
	for _, n := range mp.nodes {
		if n.countDelta == 0 && n.xor == zeroDigest {
			continue
		}
		var (
			curCount  int64
			curDigest [hashLen]byte
		)
		val, closer, err := m.e.db.Get(n.key)
		switch {
		case err == nil:
			c, d, ok := unpackDigestLeaf(val)
			closer.Close()
			if !ok {
				return dropDigest()
			}
			curCount = c
			copy(curDigest[:], d)
		case errors.Is(err, pebble.ErrNotFound):
			// absent leaf = {0, zero}
		default:
			return err
		}
		newCount := curCount + n.countDelta
		if newCount < 0 {
			return dropDigest()
		}
		xorInto(curDigest[:], n.xor[:])
		if newCount == 0 {
			// An emptied leaf's digest must cancel to exactly zero
			// (count 0 ⇒ digest 0); anything else means the stored
			// digest disagrees with the mutation stream.
			if curDigest != zeroDigest {
				return dropDigest()
			}
			if err := batch.Delete(n.key, nil); err != nil {
				return err
			}
			continue
		}
		if err := batch.Set(n.key, packDigestLeaf(newCount, curDigest[:]), nil); err != nil {
			return err
		}
	}
	if mp.countDelta == 0 && mp.xor == zeroDigest {
		return nil
	}
	newDigest := mp.rootDigest
	xorInto(newDigest[:], mp.xor[:])
	return batch.Set(mp.rootKey, packDigestRoot(mp.bits, mp.rootCount+mp.countDelta, newDigest[:]), nil)
}
