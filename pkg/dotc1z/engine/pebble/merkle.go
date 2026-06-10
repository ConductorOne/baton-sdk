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

// Per-entitlement grant merkle tree (XOR combiner).
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
// Combiner. A node's digest is the XOR of every grant content hash
// beneath it (leaves of the fold stay sha256 — only the combiner is
// XOR). XOR is homomorphic (parent = XOR of children), order-independent,
// and invertible, which buys three things:
//
//   - depth-independence: a node's digest depends only on the grants in
//     its prefix range, never on how the subtree is split, so nodes from
//     trees of different heights compare directly;
//   - O(1) incremental maintenance: post-seal insert/overwrite/delete
//     XOR the grant's content hash into/out of the O(depth) nodes on its
//     bucket path (see merkleMutator);
//   - the empty digest is all-zero (the XOR identity), so an absent node
//     reads as {count: 0, digest: 0}.
//
// Every node also stores its grant COUNT. Comparison always checks the
// (count, digest) pair, so a non-empty node whose hashes happened to XOR
// to zero can never be conflated with an empty/absent one. Within one
// tree that cancellation cannot even occur: every key-distinguishing
// field (principal rt/id, external_id) is folded into the content hash,
// so duplicate leaves are impossible by construction. XOR set-hashing is
// not adversarially collision-resistant (Bellare–Micciancio); this tree
// is an optimization, not a trust boundary — see RFC 0003 §9.
//
// Shape. 256-ary radix, variable height: depth 0 is a single root node
// (used for empty and small entitlements — this is why an entitlement
// with no grants costs exactly one stored node); each additional level
// consumes one more byte of the bucket hash. ALL levels are stored,
// sparsely: the root is always materialized (it is the "tree was built"
// marker — absence of the root means "never built", never "empty"), and
// every non-root node is materialized iff its subtree holds ≥1 grant.
//
// On-disk ABI. The principal bucket hash, the grant content hash, the
// combiner, and the node value framing are all part of the stored
// format: changing merkleBucketHashLen, the content-hash field set
// (grantContentHash), the combiner, or the framing requires an
// index-migration version bump (see index_migrations.go).

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
// content hash (index value) and node digests.
const hashLen = sha256.Size

// zeroDigest is the XOR identity — the digest of an empty/absent node.
var zeroDigest [hashLen]byte

// xorInto XORs src into dst in place, over min(len(dst), len(src)).
func xorInto(dst, src []byte) {
	for i := range min(len(dst), len(src)) {
		dst[i] ^= src[i]
	}
}

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

// Node value framing. All non-root nodes share one body so interior and
// leaf nodes are read uniformly; the root prepends the chosen depth so a
// reader knows the leaf level without scanning.
//
//	node body:  count(8 BE) | digest(hashLen)
//	root body:  depth(1)    | count(8 BE) | digest(hashLen)
//
// An ABSENT non-root node is {count: 0, digest: 0} by definition —
// readers substitute that for any missing key. An absent ROOT means
// "tree never built" (never "empty"); readers must fall back to the
// on-demand fold, not assume zero.

func packMerkleRoot(depth int, count int64, digest []byte) []byte {
	buf := make([]byte, 0, 1+8+len(digest))
	buf = append(buf, byte(depth))
	var n [8]byte
	binary.BigEndian.PutUint64(n[:], uint64(count)) //nolint:gosec // non-negative row count
	buf = append(buf, n[:]...)
	return append(buf, digest...)
}

// unpackMerkleRoot returns (depth, count, digest, ok). ok is false when
// the value is not a well-formed root blob.
func unpackMerkleRoot(val []byte) (int, int64, []byte, bool) {
	if len(val) != 1+8+hashLen {
		return 0, 0, nil, false
	}
	depth := int(val[0])
	count := int64(binary.BigEndian.Uint64(val[1:9])) //nolint:gosec // count is a non-negative row count
	return depth, count, val[9:], true
}

func packMerkleNode(count int64, digest []byte) []byte {
	buf := make([]byte, 0, 8+len(digest))
	var n [8]byte
	binary.BigEndian.PutUint64(n[:], uint64(count)) //nolint:gosec // non-negative row count
	buf = append(buf, n[:]...)
	return append(buf, digest...)
}

// unpackMerkleNode returns (count, digest, ok) for a non-root node body.
func unpackMerkleNode(val []byte) (int64, []byte, bool) {
	if len(val) != 8+hashLen {
		return 0, nil, false
	}
	count := int64(binary.BigEndian.Uint64(val[:8])) //nolint:gosec // non-negative count
	return count, val[8:], true
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
// into a root plus every non-empty node at every level, in a single
// streaming pass — O(depth) memory regardless of entitlement size.
//
// The pass starts by range-deleting the entitlement's whole typeMerkle
// keyspace: the build only ever Sets nodes, so without the clear a
// rebuild that changes depth or empties a bucket would leave stale nodes
// that the comparison descent (which enumerates children from the node
// keyspace) would read — and a stale digest that happens to match the
// peer prunes a real diff. Old and new framings are byte-length
// identical, so stale nodes are not detectable by inspection.
//
// Sorted index order means each node's grants are contiguous, so a
// level's "open" node closes exactly when its prefix changes. Only
// non-empty nodes are ever opened, so sparsity is automatic, not a
// prune pass.
//
// The depth is taken as a parameter rather than derived so the
// depth-selection seam can be exercised directly: tests force a depth
// that the natural count→depth mapping would only produce at a very
// large grant count, which is how the cross-depth comparison path gets
// covered without seeding tens of thousands of grants.
func (e *Engine) buildEntitlementMerkleAtDepth(ctx context.Context, idBytes []byte, entitlementID string, depth int) error {
	entPrefix := encodeGrantByEntPrincHashEntPrefix(idBytes, entitlementID)
	upper := upperBoundOf(entPrefix)
	nodeLower := encodeMerkleEntPrefix(idBytes, entitlementID)
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

		iter, err := e.db.NewIter(&pebble.IterOptions{LowerBound: entPrefix, UpperBound: upper})
		if err != nil {
			return err
		}
		defer iter.Close()

		// One running node per level 1..depth; the root accumulates
		// separately (its prefix is always empty, so it never closes
		// mid-stream).
		type openNode struct {
			active bool
			prefix []byte
			digest [hashLen]byte
			count  int64
		}
		open := make([]openNode, depth+1)
		flush := func(level int) error {
			n := &open[level]
			if !n.active {
				return nil
			}
			key := encodeMerkleNodeKey(idBytes, entitlementID, byte(level), n.prefix)
			return batch.Set(key, packMerkleNode(n.count, n.digest[:]), nil)
		}

		var (
			rootDigest [hashLen]byte
			total      int64
		)
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

			for level := 1; level <= depth; level++ {
				prefix := bucketHash[:level]
				n := &open[level]
				if !n.active || !bytes.Equal(n.prefix, prefix) {
					if err := flush(level); err != nil {
						return err
					}
					n.prefix = append(n.prefix[:0], prefix...)
					n.digest = [hashLen]byte{}
					n.count = 0
					n.active = true
				}
				xorInto(n.digest[:], val)
				n.count++
			}
			xorInto(rootDigest[:], val)
			total++
		}
		if err := iter.Error(); err != nil {
			return err
		}
		for level := 1; level <= depth; level++ {
			if err := flush(level); err != nil {
				return err
			}
		}

		// Root is written unconditionally — even at count 0 — as the
		// "tree was built" marker.
		rootKey := encodeMerkleNodeKey(idBytes, entitlementID, 0, nil)
		if err := batch.Set(rootKey, packMerkleRoot(depth, total, rootDigest[:]), nil); err != nil {
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
// back to ComputeBucketHash, which derives the same digest from the
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

// getMerkleNode reads one stored non-root node. An absent node returns
// (0, zero digest, present=false, nil) — the XOR identity.
func (e *Engine) getMerkleNode(idBytes []byte, entitlementID string, level int, prefix []byte) (int64, []byte, bool, error) {
	val, closer, err := e.db.Get(encodeMerkleNodeKey(idBytes, entitlementID, byte(level), prefix))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return 0, zeroDigest[:], false, nil
		}
		return 0, nil, false, err
	}
	defer closer.Close()
	count, digest, ok := unpackMerkleNode(val)
	if !ok {
		return 0, nil, false, fmt.Errorf("getMerkleNode: malformed node for %q level %d", entitlementID, level)
	}
	out := make([]byte, hashLen)
	copy(out, digest)
	return count, out, true, nil
}

// merkleChildPrefixes returns the sorted full prefixes (length = level)
// of the stored nodes at `level` under parentPrefix. Because the node
// key embeds the level byte before the prefix bytes, the children of one
// parent are a contiguous key range — cost is O(children present), and
// only non-empty children are ever stored.
func (e *Engine) merkleChildPrefixes(ctx context.Context, idBytes []byte, entitlementID string, level int, parentPrefix []byte) ([][]byte, error) {
	stem := encodeMerkleNodeKey(idBytes, entitlementID, byte(level), nil)
	lower := append(append([]byte(nil), stem...), parentPrefix...)
	upper := upperBoundOf(lower)
	iter, err := e.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	var out [][]byte
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		key := iter.Key()
		if len(key) != len(stem)+level {
			continue // malformed; skip defensively
		}
		prefix := make([]byte, level)
		copy(prefix, key[len(stem):])
		out = append(out, prefix)
	}
	return out, iter.Error()
}

// ComputeBucketHash folds the hash index over a single principal-hash
// bucket (identified by a raw hash prefix; empty prefix = whole
// entitlement = the root) and returns the content-defined XOR digest
// plus the grant count. This is the authoritative definition of a node;
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
	digest := make([]byte, hashLen)
	var count int64
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return nil, 0, err
		}
		xorInto(digest, iter.Value())
		count++
	}
	if err := iter.Error(); err != nil {
		return nil, 0, err
	}
	return digest, count, nil
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
// The fast path is a single root read per side. On mismatch it descends
// level by level, pruning every subtree whose (count, digest) pair
// matches and emitting the differing prefixes at compareDepth — the
// shallower tree's leaf level, where both sides still have directly
// comparable nodes (XOR digests are depth-independent). Children are
// enumerated from the stored node keyspace (union of both sides), so
// descent cost is proportional to the symmetric difference, not the
// fan-out.
//
// A missing root means the tree was never built on that side — NOT that
// the entitlement is empty — so both sides are compared via the
// authoritative on-demand fold instead.
func (e *Engine) DirtyEntitlementBuckets(ctx context.Context, syncID string, other *Engine, otherSyncID, entitlementID string) ([][]byte, error) {
	rootA, okA, err := e.GetEntitlementMerkleRoot(ctx, syncID, entitlementID)
	if err != nil {
		return nil, err
	}
	rootB, okB, err := other.GetEntitlementMerkleRoot(ctx, otherSyncID, entitlementID)
	if err != nil {
		return nil, err
	}

	if !okA || !okB {
		ha, ca, err := e.ComputeBucketHash(ctx, syncID, entitlementID, nil)
		if err != nil {
			return nil, err
		}
		hb, cb, err := other.ComputeBucketHash(ctx, otherSyncID, entitlementID, nil)
		if err != nil {
			return nil, err
		}
		if ca == cb && bytes.Equal(ha, hb) {
			return nil, nil
		}
		return [][]byte{{}}, nil
	}

	if rootA.Count == rootB.Count && bytes.Equal(rootA.Hash, rootB.Hash) {
		return nil, nil
	}

	// Roots differ. The descent granularity is the shallower tree's
	// leaf level; at depth 0 there is nothing below the root.
	compareDepth := min(rootA.Depth, rootB.Depth)
	if compareDepth == 0 {
		return [][]byte{{}}, nil
	}

	idBytesA, err := e.resolveSyncBytes(syncID)
	if err != nil {
		return nil, err
	}
	idBytesB, err := other.resolveSyncBytes(otherSyncID)
	if err != nil {
		return nil, err
	}

	var dirty [][]byte
	var walk func(prefix []byte, level int) error
	walk = func(prefix []byte, level int) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		ca, da, _, err := e.getMerkleNode(idBytesA, entitlementID, level, prefix)
		if err != nil {
			return err
		}
		cb, db, _, err := other.getMerkleNode(idBytesB, entitlementID, level, prefix)
		if err != nil {
			return err
		}
		if ca == cb && bytes.Equal(da, db) {
			return nil // identical subtree (or absent on both sides) → prune
		}
		if level == compareDepth {
			dirty = append(dirty, prefix)
			return nil
		}
		kidsA, err := e.merkleChildPrefixes(ctx, idBytesA, entitlementID, level+1, prefix)
		if err != nil {
			return err
		}
		kidsB, err := other.merkleChildPrefixes(ctx, idBytesB, entitlementID, level+1, prefix)
		if err != nil {
			return err
		}
		for _, k := range mergeSortedPrefixes(kidsA, kidsB) {
			if err := walk(k, level+1); err != nil {
				return err
			}
		}
		return nil
	}

	kidsA, err := e.merkleChildPrefixes(ctx, idBytesA, entitlementID, 1, nil)
	if err != nil {
		return nil, err
	}
	kidsB, err := other.merkleChildPrefixes(ctx, idBytesB, entitlementID, 1, nil)
	if err != nil {
		return nil, err
	}
	for _, k := range mergeSortedPrefixes(kidsA, kidsB) {
		if err := walk(k, 1); err != nil {
			return nil, err
		}
	}

	// Roots differed but the descent found nothing: with consistent
	// trees that's impossible (the root is the XOR of level 1), so a
	// stored node is stale or corrupt. Fail safe — whole entitlement
	// dirty; the next rebuild heals the tree.
	if len(dirty) == 0 {
		return [][]byte{{}}, nil
	}
	return dirty, nil
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

// --- Incremental maintenance (post-seal) ---

// merkleMutator accumulates per-node (XOR, count) deltas for a batch of
// post-seal grant mutations and applies each touched node exactly once.
//
// Why an accumulator instead of read-modify-write per mutation: the
// updates target a plain pebble.Batch, which does NOT read through its
// own writes — and every mutation in a batch touches the root node, so
// naive per-mutation RMW would lose deltas. Accumulating also collapses
// N writes per node into one. All grant writers run under withWrite's
// mutex, so reading current node values from the DB inside apply is
// race-free.
//
// Lifecycle: one mutator per write batch. Callers feed remove(old) /
// add(new) as they process records (an overwrite that moves the grant to
// a different bucket — principal changed — is exactly remove+add), then
// call apply(batch) once before commit.
//
// Entitlements whose tree was never built (no stored root) are skipped:
// the seal-time build or the on-Open backfill will construct them from
// the index. This also makes the mutator free during a fresh sync — but
// callers on the fresh-sync bulk path should skip constructing one
// anyway to avoid the per-entitlement root probe.
type merkleMutator struct {
	e    *Engine
	ents map[string]*mutatorEnt // keyed by string(root node key)
}

type mutatorEnt struct {
	idBytes    []byte
	entID      string
	rootKey    []byte
	present    bool // stored root exists; if false all deltas are dropped
	depth      int
	rootCount  int64         // count read from the stored root
	rootDigest [hashLen]byte // digest read from the stored root
	xor        [hashLen]byte // accumulated root delta
	countDelta int64
	nodes      map[string]*mutatorNode // levels 1..depth, keyed by string(node key)
}

type mutatorNode struct {
	key        []byte
	xor        [hashLen]byte
	countDelta int64
}

func newMerkleMutator(e *Engine) *merkleMutator {
	return &merkleMutator{e: e, ents: make(map[string]*mutatorEnt)}
}

// entFor returns the (cached) per-entitlement state, probing the stored
// root on first touch. The cached root snapshot stays valid for the
// mutator's lifetime because all writers serialize through withWrite.
func (m *merkleMutator) entFor(idBytes []byte, entitlementID string) (*mutatorEnt, error) {
	rootKey := encodeMerkleNodeKey(idBytes, entitlementID, 0, nil)
	k := string(rootKey)
	if me, ok := m.ents[k]; ok {
		return me, nil
	}
	me := &mutatorEnt{idBytes: idBytes, entID: entitlementID, rootKey: rootKey, nodes: make(map[string]*mutatorNode)}
	val, closer, err := m.e.db.Get(rootKey)
	switch {
	case err == nil:
		depth, count, digest, ok := unpackMerkleRoot(val)
		closer.Close()
		if ok {
			me.present = true
			me.depth = depth
			me.rootCount = count
			copy(me.rootDigest[:], digest)
		}
		// Malformed root: leave present=false so mutations are dropped;
		// the stale root heals at the next rebuild.
	case errors.Is(err, pebble.ErrNotFound):
		// No tree — deltas for this entitlement are no-ops.
	default:
		return nil, err
	}
	m.ents[k] = me
	return me, nil
}

// add records r's insertion into its entitlement's tree.
func (m *merkleMutator) add(idBytes []byte, r *v3.GrantRecord) error {
	return m.delta(idBytes, r, 1)
}

// remove records r's removal from its entitlement's tree.
func (m *merkleMutator) remove(idBytes []byte, r *v3.GrantRecord) error {
	return m.delta(idBytes, r, -1)
}

func (m *merkleMutator) delta(idBytes []byte, r *v3.GrantRecord, sign int64) error {
	ent := r.GetEntitlement()
	princ := r.GetPrincipal()
	if ent == nil || princ == nil {
		return nil // not in the hash index → not in the tree
	}
	me, err := m.entFor(idBytes, ent.GetEntitlementId())
	if err != nil {
		return err
	}
	if !me.present {
		return nil
	}
	h := grantContentHash(r)
	xorInto(me.xor[:], h)
	me.countDelta += sign
	if me.depth == 0 {
		return nil
	}
	bh := principalBucketHash(princ.GetResourceTypeId(), princ.GetResourceId())
	for level := 1; level <= me.depth; level++ {
		key := encodeMerkleNodeKey(idBytes, ent.GetEntitlementId(), byte(level), bh[:level])
		nk := string(key)
		n, ok := me.nodes[nk]
		if !ok {
			n = &mutatorNode{key: key}
			me.nodes[nk] = n
		}
		xorInto(n.xor[:], h)
		n.countDelta += sign
	}
	return nil
}

// apply folds the accumulated deltas into the stored nodes via batch.
// Nodes whose delta cancelled to zero (e.g. an overwrite that changed
// only excluded fields) are skipped; a non-root node whose count reaches
// zero is deleted (restoring sparsity); the root is rewritten in place.
// A count that would go negative means the stored tree disagrees with
// the mutation stream — the tree is dropped wholesale (DeleteRange), so
// readers fall back to the on-demand fold until the next rebuild.
func (m *merkleMutator) apply(batch *pebble.Batch) error {
	for _, me := range m.ents {
		if !me.present {
			continue
		}
		if err := m.applyEnt(batch, me); err != nil {
			return err
		}
	}
	return nil
}

func (m *merkleMutator) applyEnt(batch *pebble.Batch, me *mutatorEnt) error {
	if me.countDelta == 0 && me.xor == zeroDigest && len(me.nodes) == 0 {
		return nil
	}
	dropTree := func() error {
		// In-batch ordering: this tombstone lands after any node Sets
		// already staged for this entitlement and removes them too.
		lo := encodeMerkleEntPrefix(me.idBytes, me.entID)
		return batch.DeleteRange(lo, upperBoundOf(lo), nil)
	}
	if me.rootCount+me.countDelta < 0 {
		return dropTree()
	}
	for _, n := range me.nodes {
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
			c, d, ok := unpackMerkleNode(val)
			closer.Close()
			if !ok {
				return dropTree()
			}
			curCount = c
			copy(curDigest[:], d)
		case errors.Is(err, pebble.ErrNotFound):
			// absent node = {0, zero}
		default:
			return err
		}
		newCount := curCount + n.countDelta
		if newCount < 0 {
			return dropTree()
		}
		xorInto(curDigest[:], n.xor[:])
		if newCount == 0 {
			// An emptied node's digest must cancel to exactly zero
			// (count 0 ⇒ digest 0); anything else means the stored
			// tree disagrees with the mutation stream.
			if curDigest != zeroDigest {
				return dropTree()
			}
			if err := batch.Delete(n.key, nil); err != nil {
				return err
			}
			continue
		}
		if err := batch.Set(n.key, packMerkleNode(newCount, curDigest[:]), nil); err != nil {
			return err
		}
	}
	if me.countDelta == 0 && me.xor == zeroDigest {
		return nil
	}
	newDigest := me.rootDigest
	xorInto(newDigest[:], me.xor[:])
	return batch.Set(me.rootKey, packMerkleRoot(me.depth, me.rootCount+me.countDelta, newDigest[:]), nil)
}
