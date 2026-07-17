package pebble

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cespare/xxhash/v2"
	"github.com/cockroachdb/pebble/v2"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/codec"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/internal/rawdb"
)

// Grant instantiation of the digest core (digest.go): the per-
// entitlement grant digest, folded over the
// by_entitlement_principal_hash index.
//
//	partition    = the entitlement's encoded primary-key tail
//	               (see the partition convention in keys.go)
//	bucket hash  = grantPrincipalBucketHash (identity of the principal)
//	content hash = grantContentHash64 (the membership edge)
//
// This answers "does this entitlement have exactly the same grants as
// some other sync/file?" with a single root read, and localizes any
// difference to principal-hash buckets so a diff driver loads only
// those grants.
//
// Everything on this path works on the ENCODED key bytes: the index
// key is spliced out of the grant primary key, and both hashes are
// computed over encoded segment bytes — no decode, no re-encode, no
// proto unmarshal. The encoded tuple form is injective and its escape
// is order-preserving, so hashing encoded bytes is exactly as
// collision-free as hashing the decoded fields with a canonical
// framing, while letting the seal-time build run allocation-free.

// grantDigestSpec wires the grant hash index into the digest core. The
// index's key layout (see rawdb.GrantHashIndexEntitlementPrefix) satisfies
// the digestIndexSpec shape contract: partition prefix, then the raw
// digestBucketHashLen-byte bucket hash, then the principal tail; the
// value is the grant content hash.
var grantDigestSpec = digestIndexSpec{
	indexID:         idxGrantByEntitlementPrincipalHash,
	partitionPrefix: rawdb.GrantHashIndexEntitlementPrefix,
}

// GrantDigestABIVersion is the version of the content-hash / bucket-hash
// definitions below (grantContentHash64, grantPrincipalBucketHash64).
// Stamped into the manifest's GrantDigestRoot.abi_version so a future
// ABI bump (a change to either hash's input framing) makes stored
// manifest roots computed under different versions incomparable by
// construction, rather than silently comparing unrelated hash schemes.
// Bump alongside any index-migration version bump that touches these
// hashes (see index_migrations.go).
//
// Exported so consumers of GrantContentHash / GrantDigestAccumulator
// can check a stored root's abi_version before comparing.
const GrantDigestABIVersion uint32 = 1

// The whole-file grant digest root's node-key level lives in
// internal/keys (rawdb.DigestLevelGlobalRoot, consumed by
// rawdb.GlobalGrantDigestNodeKey): the XOR fold of every
// per-entitlement root's digest, plus the total grant count, across
// the whole sync. It is NOT part of the generic digest.go core (which
// only knows about per-partition roots/leaves at digestLevelRoot /
// digestLevelLeaf) — the "whole index" summary is a grant-digest-
// specific manifest need, so a level value the core never produces
// keeps the global node's key disjoint from every per-partition node
// regardless of what any entitlement's partition bytes happen to be.

// digestPartitionForEntitlement returns the digest partition for an
// entitlement identity: its encoded primary-key tail as a string (see
// the partition convention in keys.go).
func digestPartitionForEntitlement(id entitlementIdentity) string {
	return string(appendEntitlementIdentityTail(make([]byte, 0, 96), id))
}

// --- Hashes (on-disk ABI) ---
//
// ABI: the two hash definitions below are part of the stored format.
// Two SDK builds must hash identical grants identically or the digest
// comparison reads "everything differs"; changing either input framing
// requires an index-migration bump (index_migrations.go).

// grantPrincipalBucketHash64 is the bucket address for a principal:
// xxHash64 over the ENCODED principal segments
//
//	esc(principal_rt) | 0x00 | esc(principal_id)
//
// — byte-identical to the principal region of the grant's primary key,
// which is what the seal-time build actually hashes (a raw sub-slice,
// no re-encode). Identity only — never the principal's full object —
// so the address is stable across syncs even when the principal's
// attributes change. Only the top digestBucketHashLen bytes of the
// big-endian hash are stored in index keys (the bucket-selecting
// bits). ABI.
func grantPrincipalBucketHash64(encodedPrincipalSegments []byte) uint64 {
	return xxhash.Sum64(encodedPrincipalSegments)
}

// principalBucketHash is the from-identity form of the bucket hash:
// the stored digestBucketHashLen key bytes for a principal given its
// decoded identity. Encodes the segments exactly as the primary grant
// key does, then hashes — so it MUST agree with hashing the spliced
// key region (pinned by TestGrantDigestSpliceMatchesEncode). Returns a
// fresh slice.
func principalBucketHash(principalRT, principalID string) []byte {
	enc := codec.AppendTupleStrings(make([]byte, 0, 64), principalRT, principalID)
	var full [8]byte
	binary.BigEndian.PutUint64(full[:], grantPrincipalBucketHash64(enc))
	out := make([]byte, digestBucketHashLen)
	copy(out, full[:])
	return out
}

// grantContentHash64 is the canonical content hash of a grant — the
// value stored in the hash index and the unit the grant digest folds.
//
// ABI: "the same grant" is defined as
//
//	xxHash64( primaryKeyTail ‖ ( 0x00 ‖ esc(source_id) )* )
//
// where primaryKeyTail is the grant's encoded primary-key tail (the
// 6-segment identity tuple ent_rt|ent_rid|ent_flag|ent_tail|p_rt|p_id,
// escaped and separator-delimited exactly as stored) and the source
// ids — the keys of the grant's sources map, its expansion
// provenance — are appended as additional tuple segments in ascending
// byte order (sortedSourceKeys must already be sorted; the escape is
// order-preserving so raw order == encoded order).
//
// The field set deliberately covers the membership EDGE (the identity
// tuple) plus the grant's source-entitlement set, and deliberately
// EXCLUDES everything sync-relative or transient — external_id (not
// identity under the injective-key scheme; the same edge keeps its
// hash when a connector changes its id grammar), discovered_at,
// needs_expansion, expansion state, and annotations — none of which
// change "which principal holds which entitlement". The source map
// VALUES (GrantSourceRecord) are not folded in v1 — only the set of
// source ids, which is the membership-composition signal.
//
// This is a hand-rolled framing, NOT proto marshal: deterministic-proto
// output is not canonical across protobuf library versions, which
// would make two files written by different SDK builds hash identical
// grants differently. The tuple framing is injective: every segment is
// escaped and separator-delimited, and the identity is a fixed six
// segments, so no source list can alias a different identity split (a
// naive 0x00-joined concatenation would collide e.g. sources
// ["a","b"] vs ["a\x00b"]).
//
// tuple is a caller-reused scratch buffer, returned grown for reuse.
func grantContentHash64(tuple, primaryKeyTail []byte, sortedSourceKeys [][]byte) (uint64, []byte) {
	if len(sortedSourceKeys) == 0 {
		// Common case: no sources — hash the tail bytes in place.
		return xxhash.Sum64(primaryKeyTail), tuple
	}
	tuple = append(tuple[:0], primaryKeyTail...)
	for _, k := range sortedSourceKeys {
		tuple = codec.AppendTupleSeparator(tuple)
		tuple = codec.AppendTupleBytes(tuple, k)
	}
	return xxhash.Sum64(tuple), tuple
}

// grantContentHashForRecord is the from-record form of the content
// hash: encodes the grant's identity tuple and sorts its source keys,
// then delegates to grantContentHash64. The seal-time build never uses
// this (it splices key bytes and raw-scans the value); it exists for
// readers, tests, and any future repair path, and is pinned against
// the splice form by TestGrantDigestSpliceMatchesEncode.
func grantContentHashForRecord(r *v3.GrantRecord) ([]byte, error) {
	id, err := grantIdentityFromRecord(r)
	if err != nil {
		return nil, err
	}
	key := encodeGrantIdentityKey(id)
	srcs := make([][]byte, 0, len(r.GetSources()))
	for k := range r.GetSources() {
		srcs = append(srcs, []byte(k))
	}
	sortByteSlices(srcs)
	h, _ := grantContentHash64(nil, key[grantPrimaryKeyPrefixLen:], srcs)
	out := make([]byte, hashLen)
	binary.BigEndian.PutUint64(out, h)
	return out, nil
}

// GrantContentHash is the public form of the grant content hash
// (grantContentHash64) computed from a v2 proto — the uint64 the stored
// 8-byte big-endian hash encodes. Use it (or GrantDigestAccumulator,
// its fold) to digest grants not held in a pebble file — e.g. a legacy
// SQLite c1z — for comparison against engine-served digests.
//
// Hash grants as stored, not as emitted: expansion populates the
// sources map after ingest, so raw connector output hashes differently
// from what a sealed file digested. A grant missing its structural
// identity errors — such a grant cannot exist in a pebble file.
// Duplicates of one identity tuple drift from the disk fold (pebble
// stores the tuple once), but the counts differ too, so the comparison
// just reports a mismatch — never a false match.
//
// ABI: pinned to GrantDigestABIVersion alongside the internal forms.
func GrantContentHash(g *v2.Grant) (uint64, error) {
	ent := g.GetEntitlement()
	entRes := ent.GetResource().GetId()
	if entRes.GetResourceType() == "" || entRes.GetResource() == "" || ent.GetId() == "" {
		return 0, fmt.Errorf("grant content hash: missing entitlement identity")
	}
	princ := g.GetPrincipal().GetId()
	if princ.GetResourceType() == "" || princ.GetResource() == "" {
		return 0, fmt.Errorf("grant content hash: missing principal identity")
	}
	id := grantIdentity{
		entitlement:     entitlementIdentityFromParts(entRes.GetResourceType(), entRes.GetResource(), ent.GetId()),
		principalTypeID: princ.GetResourceType(),
		principalID:     princ.GetResource(),
	}
	key := encodeGrantIdentityKey(id)
	sources := g.GetSources().GetSources()
	srcs := make([][]byte, 0, len(sources))
	for k := range sources {
		srcs = append(srcs, []byte(k))
	}
	sortByteSlices(srcs)
	h, _ := grantContentHash64(nil, key[grantPrimaryKeyPrefixLen:], srcs)
	return h, nil
}

// GrantDigestAccumulator XOR/count-folds GrantContentHash over a set of
// grants. Fold one entitlement's grants to reproduce its stored digest
// root (GetEntitlementDigestRoot), or every grant in a sync to
// reproduce the whole-file root (GetGrantDigestGlobalRoot / the
// manifest's GrantDigestRoot.xor_digest — per-entitlement roots
// XOR-combine, so the flat fold equals the fold of roots). Compare only
// digests computed under the same GrantDigestABIVersion; see
// GrantContentHash for the input contract.
//
// The zero value is ready to use. Not safe for concurrent use.
type GrantDigestAccumulator struct {
	xor   uint64
	count int64
}

// Add folds one grant into the accumulator; a grant GrantContentHash
// rejects leaves it unchanged.
func (a *GrantDigestAccumulator) Add(g *v2.Grant) error {
	h, err := GrantContentHash(g)
	if err != nil {
		return err
	}
	a.xor ^= h
	a.count++
	return nil
}

// Root returns the fold so far as a DigestRoot: Hash is the 8-byte
// big-endian XOR fold, Count the number of grants added. Bits is 0
// (root-only); compare Hash and Count.
func (a *GrantDigestAccumulator) Root() DigestRoot {
	return DigestRoot{
		Hash:  binary.BigEndian.AppendUint64(nil, a.xor),
		Count: a.count,
	}
}

// sortByteSlices sorts byte slices ascending (bytes.Compare order).
func sortByteSlices(s [][]byte) {
	// Small-n insertion sort: source sets are tiny (usually 0–4).
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && bytes.Compare(s[j], s[j-1]) < 0; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

// --- Key splices ---

// grantPrimaryKeyPrefixLen is the byte length of the grant primary-key
// header: versionV3 | typeGrant | separator.
const grantPrimaryKeyPrefixLen = rawdb.GrantPrimaryKeyPrefixLen

// grantHashIndexKeyPrefixLen is the byte length of the hash-index key
// header: versionV3 | typeIndex | idxGrantByEntitlementPrincipalHash |
// separator.
const grantHashIndexKeyPrefixLen = 4

// appendGrantHashIndexKeyFromPrimary builds the
// by_entitlement_principal_hash index key by SPLICING a grant primary
// key around the raw bucket hash, into dst:
//
//	primary = v3|typeGrant|0x00| ent_rt|0|ent_rid|0|ent_flag|0|ent_tail |0| p_rt|0|p_id
//	index   = v3|typeIndex|idx |0x00| ent_rt|0|ent_rid|0|ent_flag|0|ent_tail |0| bh[0:2] | p_rt|0|p_id
//
// The segments are already escaped and the tuple encoding is
// canonical, so the raw byte splice is byte-identical to
// decode + re-encode (same trick as rawdb.AppendGrantByPrincipalKeyFromPrimary,
// pinned by TestGrantDigestSpliceMatchesEncode). sep4 must come from
// rawdb.SplitGrantPrimaryKey on the same key.
func appendGrantHashIndexKeyFromPrimary(dst, primaryKey []byte, sep4 int, bucketHash64 uint64) []byte {
	var bh [8]byte
	binary.BigEndian.PutUint64(bh[:], bucketHash64)
	dst = append(dst, versionV3, typeIndex, idxGrantByEntitlementPrincipalHash, 0)
	dst = append(dst, primaryKey[grantPrimaryKeyPrefixLen:sep4+1]...) // partition + its trailing separator
	dst = append(dst, bh[:digestBucketHashLen]...)
	return append(dst, primaryKey[sep4+1:]...) // principal segments
}

// grantPrimaryKeyFromHashIndexKey reconstructs the grant primary key
// from a hash-index key by removing the raw hash region — a byte
// splice, no decode. Returns ok=false for keys that don't parse as
// hash-index entries. The hash bytes may contain 0x00, so the 4th
// separator is found by counting separators from the LEFT (the walk
// never crosses the hash region — see the positional-decoding note on
// rawdb.GrantHashIndexEntitlementPrefix).
func grantPrimaryKeyFromHashIndexKey(dst, idxKey []byte) ([]byte, bool) {
	if len(idxKey) < grantHashIndexKeyPrefixLen ||
		idxKey[0] != versionV3 || idxKey[1] != typeIndex ||
		idxKey[2] != idxGrantByEntitlementPrincipalHash || idxKey[3] != 0 {
		return dst, false
	}
	off := grantHashIndexKeyPrefixLen
	for range 4 {
		sep := bytes.IndexByte(idxKey[off:], 0)
		if sep < 0 {
			return dst, false
		}
		off += sep + 1
	}
	// off now points at the raw bucket hash.
	if len(idxKey) < off+digestBucketHashLen {
		return dst, false
	}
	dst = append(dst, versionV3, typeGrant, 0)
	dst = append(dst, idxKey[grantHashIndexKeyPrefixLen:off]...) // partition + trailing separator
	return append(dst, idxKey[off+digestBucketHashLen:]...), true
}

// --- Engine API ---

// GetEntitlementDigestRoot returns the stored grant-digest root for an
// entitlement. ok is false when no digest has been built for it (or it
// was invalidated) — which means the caller must re-read the
// entitlement's grants (or treat the whole entitlement as dirty), NOT
// fall back to ComputeEntitlementBucketDigest: the hash index that
// fold reads is only ever written and dropped alongside the digest
// nodes, so with no root it is absent too and the fold would report
// "zero grants" for an entitlement that may have millions — the
// false-clean trap dirtyPartitionBuckets' doc comment describes.
func (e *Engine) GetEntitlementDigestRoot(ctx context.Context, id entitlementIdentity) (DigestRoot, bool, error) {
	return e.getPartitionDigestRoot(grantDigestSpec, digestPartitionForEntitlement(id))
}

// GetGrantDigestGlobalRoot returns the whole-file grant digest root —
// the XOR fold of every entitlement's grant-digest root's content-hash
// plus the total grant count, across the sync's single sync. ok is
// false when no digest has been built for this file (present-means-
// exact: absence means "recalculate", never "zero grants"). Written
// only alongside a full grant-digest build (the seal-time build, or a
// compaction that runs BuildGrantDigests) and dropped by the same
// invalidation paths that drop any per-entitlement root — see
// stageGrantDigestInvalidation and the Drop* functions below.
func (e *Engine) GetGrantDigestGlobalRoot(ctx context.Context) (DigestRoot, bool, error) {
	if e.grantDigestBuildPending.Load() {
		// Same guard as getPartitionDigestRoot: a global root committed
		// by an interrupted build must read as absent, not certify a
		// hash index that was never ingested.
		return DigestRoot{}, false, nil
	}
	val, closer, err := e.db.Get(rawdb.GlobalGrantDigestNodeKey())
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return DigestRoot{}, false, nil
		}
		return DigestRoot{}, false, err
	}
	defer closer.Close()
	count, digest, ok := unpackDigestLeaf(val)
	if !ok {
		return DigestRoot{}, false, fmt.Errorf("GetGrantDigestGlobalRoot: malformed global root")
	}
	out := make([]byte, len(digest))
	copy(out, digest)
	return DigestRoot{Hash: out, Count: count}, true, nil
}

// ComputeEntitlementBucketDigest folds the grant hash index over a
// single bucket of an entitlement (the zero bucket = the whole
// entitlement) — the authoritative on-demand counterpart of the stored
// digest nodes, for verifying or subdividing a digest that EXISTS.
// Only meaningful while the entitlement's digest is built (its root is
// stored): the hash index lives and dies with the digest nodes, so
// against a never-built or invalidated entitlement this folds an
// absent index range and returns {0, 0} — "zero grants", not "unknown".
// Never use it as a fallback for a missing root; see
// GetEntitlementDigestRoot and computeBucketDigest's precondition.
func (e *Engine) ComputeEntitlementBucketDigest(ctx context.Context, id entitlementIdentity, bucket DigestBucket) ([]byte, int64, error) {
	return e.computeBucketDigest(ctx, grantDigestSpec, digestPartitionForEntitlement(id), bucket)
}

// DirtyEntitlementBuckets compares this engine's entitlement against
// other's and returns the buckets whose grants differ — see
// dirtyPartitionBuckets for the comparison contract (zero bucket =
// whole entitlement; nil = identical).
func (e *Engine) DirtyEntitlementBuckets(ctx context.Context, other *Engine, id entitlementIdentity) ([]DigestBucket, error) {
	return e.dirtyPartitionBuckets(ctx, grantDigestSpec, other, digestPartitionForEntitlement(id))
}

// IterateGrantsByEntitlementBucket yields the grants in one
// principal-hash bucket of an entitlement (the zero bucket = the whole
// entitlement). This is the dirty-bucket loader: after a digest
// comparison flags a bucket, the caller materializes only those grants.
// The primary key is reconstructed from each index key by byte splice
// (no decode); the point Get per entry is the cost of MATERIALIZING a
// changed grant, not of finding it. Orphan index entries are skipped.
func (e *Engine) IterateGrantsByEntitlementBucket(ctx context.Context, id entitlementIdentity, bucket DigestBucket, yield func(*v3.GrantRecord) bool) error {
	lower, upper := grantDigestSpec.bucketBounds(digestPartitionForEntitlement(id), bucket)
	iter, err := e.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return err
	}
	defer iter.Close()
	var keyScratch []byte
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		priKey, ok := grantPrimaryKeyFromHashIndexKey(keyScratch[:0], iter.Key())
		keyScratch = priKey
		if !ok {
			continue
		}
		val, closer, getErr := e.db.Get(priKey)
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

// DropAllGrantDigests removes every stored digest node. Called when a
// seal-time build fails partway: a partially built digest that LOOKS
// present would violate the present-means-exact contract, whereas
// absent digests just make readers re-read the grants until the next
// successful seal recalculates them.
func (e *Engine) DropAllGrantDigests(ctx context.Context) error {
	return e.withWrite(func() error {
		e.db.SetGrantDigestsPresent(false)
		return e.db.DropKeyRange(DigestLowerBound(), DigestUpperBound(), writeOpts(e.opts.durability))
	})
}

// DropAllGrantDigestState removes every stored digest node AND the
// whole by_entitlement_principal_hash index, and clears the engine's
// digests-present flag. For callers that are about to mutate grants
// through paths that bypass the engine's index maintenance entirely
// (the synccompactor's fold merge): after this, digests read as
// "missing — recalculate", never as stale-but-present, and subsequent
// engine-path writes skip per-entitlement invalidation tombstones.
// Two range tombstones: the digest keyspace and the hash-index
// keyspace are NOT adjacent (typeCounter/typeSession sit between
// them), so this must never be collapsed into one span.
func (e *Engine) DropAllGrantDigestState(ctx context.Context) error {
	return e.withWrite(func() error {
		e.db.SetGrantDigestsPresent(false)
		opts := writeOpts(e.opts.durability)
		if err := e.db.DropKeyRange(DigestLowerBound(), DigestUpperBound(), opts); err != nil {
			return err
		}
		return e.db.DropKeyRange(GrantByEntPrincHashLowerBound(), GrantByEntPrincHashUpperBound(), opts)
	})
}

// The per-record digest-invalidation obligation (partition nodes +
// whole-file root + hash-index range, present-means-exact) lives in
// rawdb itself (RecordBatch.stageGrantDigestInvalidation), KEY-DERIVED
// from the grant primary key and gated on the digests-present flag.
// The engine-side batch form for callers that invalidate MANY named
// partitions at once is InvalidateGrantDigestPartitions (repair /
// compaction), which hoists the global-root delete out of the loop.
