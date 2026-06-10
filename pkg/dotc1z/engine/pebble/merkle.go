package pebble

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"sort"

	"github.com/cockroachdb/pebble/v2"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
)

// Per-entitlement grant merkle tree.
//
// Goal: answer "does this entitlement have exactly the same grants as
// some other sync/file?" with a single key read, and when the answer is
// no, identify which principal-hash buckets differ so a caller can load
// only those grants instead of re-reading the whole entitlement.
//
// The tree folds over the by_entitlement_principal_hash index
// (idxGrantByEntitlementPrincipalHash), whose entries are sorted by
// (entitlement_id, hash(principal)) and whose VALUE is the per-grant
// content hash. Two properties make the index the right substrate:
//
//   - hash-major order is identical across two files that hold the same
//     grants, so a streamed fold produces the same digest; and
//   - a hash prefix is a clean byte-prefix of the key, so "all grants in
//     bucket P" is a contiguous range scan.
//
// Variable height. The tree depth is chosen from the grant count:
// depth 0 is a single root node (used for empty and small entitlements —
// this is why an entitlement with no grants costs exactly one stored
// node); each additional level consumes one more byte of the bucket
// hash, multiplying the bucket count by 256. Node hashes are
// CONTENT-defined — a node's hash is the fold of every grant content
// hash beneath it, independent of how the subtree is split — so a node
// at one depth is directly comparable to the equivalent prefix-range in
// a tree of a different depth.
//
// On-disk ABI. Both the principal bucket hash and the grant content
// hash are part of the stored format: changing merkleBucketHashLen, the
// content-hash field set (grantContentHash), or the node value framing
// requires an index-migration version bump (see index_migrations.go).

const (
	// merkleBucketHashLen is the width, in bytes, of the principal
	// bucket hash embedded in the index key. 8 bytes (64 bits) bounds
	// the maximum tree depth at 8 byte-levels; collisions in the bucket
	// address are harmless (they only co-locate principals — the full
	// principal identity and external_id still distinguish index rows).
	merkleBucketHashLen = 8

	// merkleTargetBucketSize is the grant count a single leaf bucket
	// aims to hold. Depth is grown until buckets are roughly this size.
	merkleTargetBucketSize = 256

	// merkleMaxDepth caps tree depth at the bucket-hash width: one byte
	// of hash is consumed per level.
	merkleMaxDepth = merkleBucketHashLen
)

// hashLen is the width of a sha256 digest, used for both the grant
// content hash (index value) and node hashes.
const hashLen = sha256.Size

// writeLenPrefixed writes an 8-byte big-endian length followed by b.
// Length-prefixing every field makes the canonical encoding injective:
// no concatenation of fields can be confused with a different split.
func writeLenPrefixed(h hash.Hash, b []byte) {
	var n [8]byte
	binary.BigEndian.PutUint64(n[:], uint64(len(b)))
	_, _ = h.Write(n[:])
	_, _ = h.Write(b)
}

// principalBucketHash is the bucket address for a principal: the first
// merkleBucketHashLen bytes of sha256(rt, id). Identity only — never the
// principal's full object — so the address is stable across syncs even
// when the principal's attributes change. Returns a fresh slice.
func principalBucketHash(rt, id string) []byte {
	h := sha256.New()
	writeLenPrefixed(h, []byte(rt))
	writeLenPrefixed(h, []byte(id))
	sum := h.Sum(nil)
	out := make([]byte, merkleBucketHashLen)
	copy(out, sum[:merkleBucketHashLen])
	return out
}

// grantContentHash is the canonical content hash of a grant — the value
// stored in the hash index and the unit the merkle tree folds.
//
// ABI: the field set below defines what "the same grant" means for the
// diff. It deliberately covers the membership EDGE (entitlement id,
// principal identity, external_id) plus the grant's source-entitlement
// set (expansion provenance), and deliberately EXCLUDES sync-relative
// and transient processing state — sync_id, discovered_at,
// needs_expansion, expansion, and annotations — none of which change
// "which principal holds which entitlement". This is a hand-rolled
// framing, NOT proto marshal: deterministic-proto output is not
// canonical across protobuf library versions, which would make two
// files written by different SDK builds hash identical grants
// differently. Changing this set requires an index-migration bump.
func grantContentHash(r *v3.GrantRecord) []byte {
	h := sha256.New()
	ent := r.GetEntitlement()
	princ := r.GetPrincipal()
	writeLenPrefixed(h, []byte(ent.GetEntitlementId()))
	writeLenPrefixed(h, []byte(princ.GetResourceTypeId()))
	writeLenPrefixed(h, []byte(princ.GetResourceId()))
	writeLenPrefixed(h, []byte(r.GetExternalId()))

	// Source-entitlement ids, sorted for order-independence. The map
	// values (GrantSourceRecord) are not folded in v1 — only the set of
	// source ids, which is the membership-composition signal.
	sources := r.GetSources()
	ids := make([]string, 0, len(sources))
	for k := range sources {
		ids = append(ids, k)
	}
	sort.Strings(ids)
	var nbuf [8]byte
	binary.BigEndian.PutUint64(nbuf[:], uint64(len(ids)))
	_, _ = h.Write(nbuf[:])
	for _, id := range ids {
		writeLenPrefixed(h, []byte(id))
	}
	return h.Sum(nil)
}

// grantHashIndexKey returns the by_entitlement_principal_hash index key
// for r, or nil when the grant lacks the entitlement/principal needed to
// place it (mirrors the by_entitlement index's nil-guard).
func grantHashIndexKey(syncIDBytes []byte, r *v3.GrantRecord) []byte {
	ent := r.GetEntitlement()
	princ := r.GetPrincipal()
	if ent == nil || princ == nil {
		return nil
	}
	bh := principalBucketHash(princ.GetResourceTypeId(), princ.GetResourceId())
	return encodeGrantByEntPrincHashIndexKey(
		syncIDBytes, ent.GetEntitlementId(), bh,
		princ.GetResourceTypeId(), princ.GetResourceId(), r.GetExternalId(),
	)
}

// chooseMerkleDepth picks the tree depth for an entitlement with the
// given grant count: 0 when the count fits one target-sized bucket, else
// the smallest depth whose 256^depth buckets bring the average bucket
// under merkleTargetBucketSize, capped at merkleMaxDepth.
func chooseMerkleDepth(count int64) int {
	if count <= merkleTargetBucketSize {
		return 0
	}
	needed := (count + merkleTargetBucketSize - 1) / merkleTargetBucketSize
	depth := 0
	capacity := int64(1)
	for capacity < needed && depth < merkleMaxDepth {
		capacity *= 256
		depth++
	}
	return depth
}

// Node value framing.
//
//	root  (level 0):  depth(1) | count(8 BE) | hash(hashLen)
//	leaf  (level d):  count(8 BE) | hash(hashLen)
//
// The root carries the chosen depth so a reader knows the leaf level
// without scanning. Leaves omit it (their level is in the key).

func packMerkleRoot(depth int, count int64, h []byte) []byte {
	buf := make([]byte, 0, 1+8+len(h))
	buf = append(buf, byte(depth))
	var n [8]byte
	binary.BigEndian.PutUint64(n[:], uint64(count)) //nolint:gosec // non-negative row count
	buf = append(buf, n[:]...)
	return append(buf, h...)
}

// unpackMerkleRoot returns (depth, count, hash, ok). ok is false when
// the value is not a well-formed root blob.
func unpackMerkleRoot(val []byte) (int, int64, []byte, bool) {
	if len(val) != 1+8+hashLen {
		return 0, 0, nil, false
	}
	depth := int(val[0])
	count := int64(binary.BigEndian.Uint64(val[1:9])) //nolint:gosec // count is a non-negative row count
	return depth, count, val[9:], true
}

func packMerkleLeaf(count int64, h []byte) []byte {
	buf := make([]byte, 0, 8+len(h))
	var n [8]byte
	binary.BigEndian.PutUint64(n[:], uint64(count)) //nolint:gosec // non-negative row count
	buf = append(buf, n[:]...)
	return append(buf, h...)
}

// BuildAllMerkleTrees rebuilds the per-entitlement merkle tree for every
// entitlement in syncID. Called at seal time (Adapter.EndSync) after all
// grants are written, and by the on-Open migration backfill. Every
// entitlement gets a tree — including those with zero grants, which
// store a single root node — so a reader can always distinguish "empty"
// from "never built".
func (e *Engine) BuildAllMerkleTrees(ctx context.Context, syncID string) error {
	idBytes, err := e.resolveSyncBytes(syncID)
	if err != nil {
		return err
	}
	// Collect entitlement ids first: BuildEntitlementMerkle writes into
	// the typeMerkle keyspace while we'd otherwise be mid-iteration over
	// typeEntitlement. Different keyspaces, but snapshotting the ids
	// keeps the iterator and the writes cleanly separated.
	var ents []string
	if err := e.IterateEntitlementsBySync(ctx, syncID, func(r *v3.EntitlementRecord) bool {
		ents = append(ents, r.GetExternalId())
		return true
	}); err != nil {
		return fmt.Errorf("BuildAllMerkleTrees: list entitlements: %w", err)
	}
	for _, ent := range ents {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := e.buildEntitlementMerkle(ctx, idBytes, ent); err != nil {
			return fmt.Errorf("BuildAllMerkleTrees: entitlement %q: %w", ent, err)
		}
	}
	return nil
}

// buildEntitlementMerkle counts an entitlement's grants (pass 1), picks
// the depth from that count, and delegates the fold to
// buildEntitlementMerkleAtDepth (pass 2).
func (e *Engine) buildEntitlementMerkle(ctx context.Context, idBytes []byte, entitlementID string) error {
	entPrefix := encodeGrantByEntPrincHashEntPrefix(idBytes, entitlementID)
	count, err := e.countHashIndexRange(entPrefix, upperBoundOf(entPrefix))
	if err != nil {
		return err
	}
	return e.buildEntitlementMerkleAtDepth(ctx, idBytes, entitlementID, chooseMerkleDepth(count))
}

// buildEntitlementMerkleAtDepth folds the hash index for one entitlement
// into a root (and, when depth > 0, one leaf per non-empty bucket) and
// writes the nodes in a single streaming pass — O(1) memory regardless
// of entitlement size. The depth is taken as a parameter rather than
// derived so the depth-selection seam can be exercised directly: tests
// force a depth that the natural count→depth mapping would only produce
// at a very large grant count, which is how the cross-depth comparison
// path gets covered without seeding tens of thousands of grants.
func (e *Engine) buildEntitlementMerkleAtDepth(ctx context.Context, idBytes []byte, entitlementID string, depth int) error {
	entPrefix := encodeGrantByEntPrincHashEntPrefix(idBytes, entitlementID)
	upper := upperBoundOf(entPrefix)

	return e.withWrite(func() error {
		batch := e.db.NewBatch()
		defer batch.Close()

		iter, err := e.db.NewIter(&pebble.IterOptions{LowerBound: entPrefix, UpperBound: upper})
		if err != nil {
			return err
		}
		defer iter.Close()

		rootH := sha256.New()
		var (
			leafH     hash.Hash
			leafCount int64
			curPrefix []byte
			haveLeaf  bool
			total     int64
		)
		flushLeaf := func() error {
			if depth == 0 || !haveLeaf {
				return nil
			}
			key := encodeMerkleNodeKey(idBytes, entitlementID, byte(depth), curPrefix)
			return batch.Set(key, packMerkleLeaf(leafCount, leafH.Sum(nil)), nil)
		}

		for iter.First(); iter.Valid(); iter.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			key := iter.Key()
			if len(key) < len(entPrefix)+merkleBucketHashLen {
				continue // malformed; skip defensively
			}
			bucketHash := key[len(entPrefix) : len(entPrefix)+merkleBucketHashLen]
			val := iter.Value() // 32-byte content hash

			if depth > 0 {
				prefix := bucketHash[:depth]
				if !haveLeaf || !bytes.Equal(prefix, curPrefix) {
					if err := flushLeaf(); err != nil {
						return err
					}
					curPrefix = append(curPrefix[:0], prefix...)
					leafH = sha256.New()
					leafCount = 0
					haveLeaf = true
				}
				_, _ = leafH.Write(val)
				leafCount++
			}
			_, _ = rootH.Write(val)
			total++
		}
		if err := iter.Error(); err != nil {
			return err
		}
		if err := flushLeaf(); err != nil {
			return err
		}

		rootKey := encodeMerkleNodeKey(idBytes, entitlementID, 0, nil)
		if err := batch.Set(rootKey, packMerkleRoot(depth, total, rootH.Sum(nil)), nil); err != nil {
			return err
		}

		opts := writeOpts(e.opts.durability)
		if e.IsFreshSync() {
			opts = pebble.NoSync
		}
		return batch.Commit(opts)
	})
}

// countHashIndexRange counts index entries in [lower, upper) without
// materializing values.
func (e *Engine) countHashIndexRange(lower, upper []byte) (int64, error) {
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

// MerkleRoot is the result of GetEntitlementMerkleRoot.
type MerkleRoot struct {
	Hash  []byte
	Depth int
	Count int64
}

// GetEntitlementMerkleRoot returns the stored root for an entitlement.
// ok is false when no tree has been built for it (the caller can fall
// back to ComputeEntitlementRoot, which derives the same digest from the
// index on demand).
func (e *Engine) GetEntitlementMerkleRoot(ctx context.Context, syncID, entitlementID string) (MerkleRoot, bool, error) {
	idBytes, err := e.resolveSyncBytes(syncID)
	if err != nil {
		return MerkleRoot{}, false, err
	}
	val, closer, err := e.db.Get(encodeMerkleNodeKey(idBytes, entitlementID, 0, nil))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return MerkleRoot{}, false, nil
		}
		return MerkleRoot{}, false, err
	}
	defer closer.Close()
	depth, count, h, valid := unpackMerkleRoot(val)
	if !valid {
		return MerkleRoot{}, false, fmt.Errorf("GetEntitlementMerkleRoot: malformed root for %q", entitlementID)
	}
	out := make([]byte, len(h))
	copy(out, h)
	return MerkleRoot{Hash: out, Depth: depth, Count: count}, true, nil
}

// ComputeBucketHash folds the hash index over a single principal-hash
// bucket (identified by a raw hash prefix; empty prefix = whole
// entitlement = the root) and returns the content-defined digest plus
// the grant count. This is the authoritative definition of a node hash;
// stored nodes are a cache of it. Depth-independent: the digest depends
// only on the grants in the prefix range, not on any tree's shape.
func (e *Engine) ComputeBucketHash(ctx context.Context, syncID, entitlementID string, hashPrefix []byte) ([]byte, int64, error) {
	idBytes, err := e.resolveSyncBytes(syncID)
	if err != nil {
		return nil, 0, err
	}
	lower := encodeGrantByEntPrincHashBucketPrefix(idBytes, entitlementID, hashPrefix)
	upper := upperBoundOf(lower)
	iter, err := e.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return nil, 0, err
	}
	defer iter.Close()
	h := sha256.New()
	var count int64
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return nil, 0, err
		}
		_, _ = h.Write(iter.Value())
		count++
	}
	if err := iter.Error(); err != nil {
		return nil, 0, err
	}
	return h.Sum(nil), count, nil
}

// IterateGrantsByEntitlementBucket yields the grants in one principal-hash
// bucket of an entitlement (empty hashPrefix = the whole entitlement).
// This is the dirty-bucket loader: after a merkle comparison flags a
// bucket prefix, the caller materializes only those grants. Like the
// other index iterators it does a point Get per entry to fetch the
// primary; orphan index entries are skipped.
func (e *Engine) IterateGrantsByEntitlementBucket(ctx context.Context, syncID, entitlementID string, hashPrefix []byte, yield func(*v3.GrantRecord) bool) error {
	idBytes, err := e.resolveSyncBytes(syncID)
	if err != nil {
		return err
	}
	entPrefix := encodeGrantByEntPrincHashEntPrefix(idBytes, entitlementID)
	lower := append(append([]byte(nil), entPrefix...), hashPrefix...)
	upper := upperBoundOf(lower)
	iter, err := e.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		_, _, _, externalID, ok := decodeEntPrincHashTail(iter.Key(), entPrefix)
		if !ok {
			continue
		}
		val, closer, getErr := e.db.Get(encodeGrantKey(idBytes, externalID))
		if getErr != nil {
			if errors.Is(getErr, pebble.ErrNotFound) {
				continue
			}
			return getErr
		}
		r := &v3.GrantRecord{}
		uErr := unmarshalRecord(val, r)
		closer.Close()
		if uErr != nil {
			return fmt.Errorf("IterateGrantsByEntitlementBucket: unmarshal: %w", uErr)
		}
		if !yield(r) {
			return nil
		}
	}
	return iter.Error()
}

// DirtyEntitlementBuckets compares this engine's entitlement against
// other's and returns the raw hash-bucket prefixes whose grants differ.
// A single empty prefix means "the whole entitlement differs" (used when
// the comparison granularity is the root, e.g. tiny entitlements). A nil
// (empty) result means the two are identical.
//
// The fast path is a root-hash equality check. On mismatch it descends
// to the shallower of the two trees' depths and compares each bucket at
// that granularity, preferring stored leaf hashes and falling back to an
// on-demand index fold when a side's tree is shallower or absent.
func (e *Engine) DirtyEntitlementBuckets(ctx context.Context, syncID string, other *Engine, otherSyncID, entitlementID string) ([][]byte, error) {
	rootA, okA, err := e.GetEntitlementMerkleRoot(ctx, syncID, entitlementID)
	if err != nil {
		return nil, err
	}
	rootB, okB, err := other.GetEntitlementMerkleRoot(ctx, otherSyncID, entitlementID)
	if err != nil {
		return nil, err
	}

	// Fast equality via stored roots when both exist.
	if okA && okB && bytes.Equal(rootA.Hash, rootB.Hash) {
		return nil, nil
	}

	// Comparison granularity: the shallower available depth. A missing
	// tree is treated as depth 0 (compare at the root → whole-entitlement
	// dirty if the computed roots differ).
	compareDepth := 0
	if okA && okB {
		compareDepth = min(rootA.Depth, rootB.Depth)
	}

	if compareDepth == 0 {
		// Confirm via the authoritative fold (covers the missing-tree
		// case and guards against a stale stored root).
		ha, _, err := e.ComputeBucketHash(ctx, syncID, entitlementID, nil)
		if err != nil {
			return nil, err
		}
		hb, _, err := other.ComputeBucketHash(ctx, otherSyncID, entitlementID, nil)
		if err != nil {
			return nil, err
		}
		if bytes.Equal(ha, hb) {
			return nil, nil
		}
		return [][]byte{{}}, nil
	}

	// Union of non-empty bucket prefixes at compareDepth from both sides.
	prefixes, err := e.distinctBucketPrefixes(ctx, syncID, entitlementID, compareDepth)
	if err != nil {
		return nil, err
	}
	otherPrefixes, err := other.distinctBucketPrefixes(ctx, otherSyncID, entitlementID, compareDepth)
	if err != nil {
		return nil, err
	}
	union := mergeSortedPrefixes(prefixes, otherPrefixes)

	var dirty [][]byte
	for _, p := range union {
		ha, _, err := e.bucketHashPreferStored(ctx, syncID, entitlementID, p, rootA, okA)
		if err != nil {
			return nil, err
		}
		hb, _, err := other.bucketHashPreferStored(ctx, otherSyncID, entitlementID, p, rootB, okB)
		if err != nil {
			return nil, err
		}
		if !bytes.Equal(ha, hb) {
			dirty = append(dirty, p)
		}
	}
	return dirty, nil
}

// bucketHashPreferStored returns a bucket's hash, using the stored leaf
// node when the tree's depth matches the prefix length (the cheap path),
// otherwise folding the index. root/ok describe the entitlement's stored
// tree for this engine.
func (e *Engine) bucketHashPreferStored(ctx context.Context, syncID, entitlementID string, prefix []byte, root MerkleRoot, ok bool) ([]byte, int64, error) {
	if ok && root.Depth == len(prefix) {
		idBytes, err := e.resolveSyncBytes(syncID)
		if err != nil {
			return nil, 0, err
		}
		val, closer, err := e.db.Get(encodeMerkleNodeKey(idBytes, entitlementID, byte(len(prefix)), prefix))
		if err == nil {
			defer closer.Close()
			if len(val) == 8+hashLen {
				count := int64(binary.BigEndian.Uint64(val[:8])) //nolint:gosec // non-negative count
				h := make([]byte, hashLen)
				copy(h, val[8:])
				return h, count, nil
			}
		} else if !errors.Is(err, pebble.ErrNotFound) {
			return nil, 0, err
		}
		// fall through to compute on miss/malformed
	}
	return e.ComputeBucketHash(ctx, syncID, entitlementID, prefix)
}

// distinctBucketPrefixes returns the sorted, distinct depth-byte hash
// prefixes present in an entitlement's hash index. It seeks past each
// bucket once found, so the cost is O(distinct buckets) seeks rather than
// O(grants).
func (e *Engine) distinctBucketPrefixes(ctx context.Context, syncID, entitlementID string, depth int) ([][]byte, error) {
	idBytes, err := e.resolveSyncBytes(syncID)
	if err != nil {
		return nil, err
	}
	entPrefix := encodeGrantByEntPrincHashEntPrefix(idBytes, entitlementID)
	upper := upperBoundOf(entPrefix)
	iter, err := e.db.NewIter(&pebble.IterOptions{LowerBound: entPrefix, UpperBound: upper})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var out [][]byte
	for iter.First(); iter.Valid(); {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		key := iter.Key()
		if len(key) < len(entPrefix)+depth {
			iter.Next()
			continue
		}
		prefix := make([]byte, depth)
		copy(prefix, key[len(entPrefix):len(entPrefix)+depth])
		out = append(out, prefix)
		// Seek past this whole bucket to the next distinct prefix.
		seekTo := upperBoundOf(append(append([]byte(nil), entPrefix...), prefix...))
		if seekTo == nil {
			break
		}
		iter.SeekGE(seekTo)
	}
	return out, iter.Error()
}

// mergeSortedPrefixes returns the sorted union of two sorted, de-duped
// prefix lists.
func mergeSortedPrefixes(a, b [][]byte) [][]byte {
	out := make([][]byte, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch bytes.Compare(a[i], b[j]) {
		case 0:
			out = append(out, a[i])
			i++
			j++
		case -1:
			out = append(out, a[i])
			i++
		default:
			out = append(out, b[j])
			j++
		}
	}
	out = append(out, a[i:]...)
	out = append(out, b[j:]...)
	return out
}
