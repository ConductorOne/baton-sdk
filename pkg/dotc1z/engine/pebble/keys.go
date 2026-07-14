package pebble

import (
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/codec"
)

// Key layout convention.
//
// A v3 Pebble c1z holds exactly ONE sync by contract. The sync_id is
// NOT part of any key (nor any stored protobuf value) — it lives only
// in the single sync-run metadata record. Every key has the shape:
//
//	[ fixed header ][ 0x00 ][ tuple-encoded tail ]
//
// where "fixed header" is one of:
//
//   - primary key:  versionV3 | typeXxx                         (2 bytes)
//   - index key:    versionV3 | typeIndex | idxXxx              (3 bytes)
//
// The single leading 0x00 starts the variable tuple-encoded tail.
//
// Encoders take no sync_id: it must never be written per-row (that was
// the multi-sync layout, which made single-sync a cleanup convention
// rather than a contract). The engine-meta keyspace-version stamp
// (engine.go Open) rejects any old-layout file so the two shapes can
// never be confused.
//
// Each primary/index key shape is paired with TWO prefix shapes used
// for range scans:
//
//   - by-type prefix:    header
//     ← matches every key in this bucket. No trailing separator; the
//     iterator runs to upperBoundOf(prefix).
//
//   - by-value prefix:   header | 0x00 | elem(s) | 0x00
//     ← matches every key whose leading tail elements equal the given
//     values. The trailing separator is LOAD-BEARING: without it the
//     prefix "ent" would falsely match "entitlement-1" entries. With
//     it, the prefix is "ent\x00" and matches only keys where the
//     element ended exactly at "ent".
//
// When adding a new index, write encodeXxxIndexKey + encodeXxxPrefix
// as a pair. Build the tuple-encoded tail with codec.AppendTupleStrings
// so the inter-element separators stay consistent — manually
// interleaving AppendTupleString and AppendTupleSeparator works but is
// the failure mode this helper exists to prevent.
//
// On-disk wire format. The codec's escape rules and integer encodings
// are an ABI: every v3 c1z file depends on them. Changing them
// requires a keyspace-version bump (see the stamp in engine.go).

// Type-discriminator bytes for the v3 keyspace. Each top-level
// keyspace occupies one byte.
const (
	versionV3 byte = 0x03

	typeResourceType byte = 0x01
	typeResource     byte = 0x02
	typeEntitlement  byte = 0x03
	typeGrant        byte = 0x04
	typeAsset        byte = 0x05
	typeSyncRun      byte = 0x06
	typeIndex        byte = 0x07
	typeCounter      byte = 0x08
	typeSession      byte = 0x09
	typeDigest       byte = 0x0A
	typeEngineMeta   byte = 0xFF
)

// Index-discriminator bytes (second byte after typeIndex). One byte
// per declared index across all record types; reuse across record
// types is fine because the index key is scoped by the type byte that
// preceded it.
const (
	idxResourceByParent             byte = 0x01
	idxEntitlementByResource        byte = 0x02 // retired: served by entitlement primary key prefixes.
	idxGrantByEntitlement           byte = 0x03 // retired: grant primary keys are entitlement-first.
	idxGrantByPrincipal             byte = 0x04
	idxGrantByNeedsExpansion        byte = 0x05
	idxGrantByPrincipalResourceType byte = 0x06 // retired: served by idxGrantByPrincipal prefix scans.
	idxGrantByEntitlementResource   byte = 0x07 // retired: served by grant primary entitlement-resource prefix scans.
	// idxGrantByEntitlementPrincipalHash sorts grants by
	// (entitlement identity, hash(principal identity)). Unlike every
	// other grant index its entries carry a VALUE (the grant content
	// hash). It is the substrate the per-entitlement grant digest
	// (typeDigest) folds over; built ONLY by the seal-time deferred
	// pass, never maintained inline. See digest.go and grant_digest.go.
	idxGrantByEntitlementPrincipalHash byte = 0x08
)

// --- Grant ---

// encodeGrantKey returns the primary key for a grant.
//
//	v3 | typeGrant | 0x00 | external_id
//
// Paired with encodeGrantPrefix (by-type prefix, no trailing sep).
func encodeGrantKey(externalID string) []byte {
	return appendGrantKey(make([]byte, 0, 3+len(externalID)), externalID)
}

// appendGrantKey encodes the grant primary key into dst (truncated to
// dst[:0] is the caller's responsibility) and returns the extended
// slice. Lets hot write paths reuse one scratch buffer across records
// instead of allocating a fresh key per Set — pebble.Batch.Set copies
// the key, so the scratch is safe to overwrite on the next record.
func appendGrantKey(dst []byte, externalID string) []byte {
	dst = append(dst, versionV3, typeGrant)
	dst = codec.AppendTupleSeparator(dst)
	return codec.AppendTupleStrings(dst, externalID)
}

func encodeGrantIdentityKey(id grantIdentity) []byte {
	return appendGrantIdentityKey(make([]byte, 0, 128), id)
}

func appendGrantIdentityKey(dst []byte, id grantIdentity) []byte {
	dst = append(dst, versionV3, typeGrant)
	dst = codec.AppendTupleSeparator(dst)
	return codec.AppendTupleStrings(
		dst,
		id.entitlement.resourceTypeID,
		id.entitlement.resourceID,
		id.entitlement.flagComponent(),
		id.entitlement.tail,
		id.principalTypeID,
		id.principalID,
	)
}

// encodeGrantPrefix returns the by-type prefix for iterating all
// grants. Paired with encodeGrantKey.
func encodeGrantPrefix() []byte {
	return []byte{versionV3, typeGrant}
}

// encodeGrantByEntitlementIndexKey is the retired by_entitlement secondary
// index on GrantRecord. New writes use entitlement-first primary grant keys
// instead; this helper remains for old debug/test tooling.
//
//	v3 | typeIndex | idxGrantByEntitlement | 0x00 |
//	    entitlement_id | 0x00 |
//	    principal_resource_type | 0x00 |
//	    principal_resource_id | 0x00 |
//	    external_id   (tail element for index-row uniqueness)
//
// Paired with encodeGrantByEntitlementPrefix (by-value prefix, with
// trailing sep).
func encodeGrantByEntitlementIndexKey(entitlementID, principalRT, principalID, externalID string) []byte {
	return appendGrantByEntitlementIndexKey(make([]byte, 0, 64), entitlementID, principalRT, principalID, externalID)
}

func appendGrantByEntitlementIndexKey(dst []byte, entitlementID, principalRT, principalID, externalID string) []byte {
	dst = append(dst, versionV3, typeIndex, idxGrantByEntitlement)
	dst = codec.AppendTupleSeparator(dst)
	return codec.AppendTupleStrings(dst, entitlementID, principalRT, principalID, externalID)
}

// retired: served by entitlement primary key prefixes.
// func encodeGrantByEntitlementIdentityIndexKey(id grantIdentity) []byte {
// 	return appendGrantByEntitlementIdentityIndexKey(make([]byte, 0, 128), id)
// }

// retired
// appendGrantByEntitlementIdentityIndexKey encodes the retired by_entitlement
// identity index. Do not use for new writes; primary grant keys are already
// entitlement-first.
// func appendGrantByEntitlementIdentityIndexKey(dst []byte, id grantIdentity) []byte {
// 	dst = append(dst, versionV3, typeIndex, idxGrantByEntitlement)
// 	dst = codec.AppendTupleSeparator(dst)
// 	return codec.AppendTupleStrings(
// 		dst,
// 		id.entitlement.resourceTypeID,
// 		id.entitlement.resourceID,
// 		id.entitlement.kind,
// 		id.entitlement.name,
// 		id.principalTypeID,
// 		id.principalID,
// 	)
// }

func encodeGrantByPrincipalIdentityIndexKey(id grantIdentity) []byte {
	return appendGrantByPrincipalIdentityIndexKey(make([]byte, 0, 128), id)
}

func appendGrantByPrincipalIdentityIndexKey(dst []byte, id grantIdentity) []byte {
	dst = append(dst, versionV3, typeIndex, idxGrantByPrincipal)
	dst = codec.AppendTupleSeparator(dst)
	return codec.AppendTupleStrings(
		dst,
		id.principalTypeID,
		id.principalID,
		id.entitlement.resourceTypeID,
		id.entitlement.resourceID,
		id.entitlement.flagComponent(),
		id.entitlement.tail,
	)
}

func encodeGrantPrimaryEntitlementPrefix(id entitlementIdentity) []byte {
	buf := make([]byte, 0, 128)
	buf = append(buf, versionV3, typeGrant)
	buf = codec.AppendTupleSeparator(buf)
	buf = codec.AppendTupleStrings(buf, id.resourceTypeID, id.resourceID, id.flagComponent(), id.tail)
	return codec.AppendTupleSeparator(buf)
}

func encodeGrantPrimaryEntitlementResourcePrefix(resourceTypeID, resourceID string) []byte {
	buf := make([]byte, 0, 64)
	buf = append(buf, versionV3, typeGrant)
	buf = codec.AppendTupleSeparator(buf)
	buf = codec.AppendTupleStrings(buf, resourceTypeID, resourceID)
	return codec.AppendTupleSeparator(buf)
}

// encodeGrantByNeedsExpansionIndexKey: index of grants whose
// NeedsExpansion flag is true. Pebble equivalent of the SQLite
// partial index `WHERE needs_expansion = 1`. The grant is added to
// this keyspace on Put when NeedsExpansion=true and removed when
// NeedsExpansion=false (or when the grant is deleted).
//
//	v3 | typeIndex | idxGrantByNeedsExpansion | 0x00 | external_id
//
// Paired with encodeGrantByNeedsExpansionPrefix (by-type prefix —
// no by-value scan is needed because the only filter is "is this
// flag set?", which is captured by the index's existence).
func encodeGrantByNeedsExpansionIndexKey(externalID string) []byte {
	return appendGrantByNeedsExpansionIndexKey(make([]byte, 0, 5+len(externalID)), externalID)
}

func appendGrantByNeedsExpansionIndexKey(dst []byte, externalID string) []byte {
	dst = append(dst, versionV3, typeIndex, idxGrantByNeedsExpansion)
	dst = codec.AppendTupleSeparator(dst)
	return codec.AppendTupleStrings(dst, externalID)
}

func encodeGrantByNeedsExpansionIdentityIndexKey(id grantIdentity) []byte {
	return appendGrantByNeedsExpansionIdentityIndexKey(make([]byte, 0, 128), id)
}

func appendGrantByNeedsExpansionIdentityIndexKey(dst []byte, id grantIdentity) []byte {
	dst = append(dst, versionV3, typeIndex, idxGrantByNeedsExpansion)
	dst = codec.AppendTupleSeparator(dst)
	return codec.AppendTupleStrings(
		dst,
		id.entitlement.resourceTypeID,
		id.entitlement.resourceID,
		id.entitlement.flagComponent(),
		id.entitlement.tail,
		id.principalTypeID,
		id.principalID,
	)
}

// encodeGrantByNeedsExpansionPrefix is the by-type prefix for all
// grants that still need expansion processing.
func encodeGrantByNeedsExpansionPrefix() []byte {
	buf := []byte{versionV3, typeIndex, idxGrantByNeedsExpansion}
	return codec.AppendTupleSeparator(buf)
}

// encodeGrantByPrincipalPrefix is the by-value prefix for "all
// grants for this principal". Trailing sep is load-bearing — see
// keys.go convention.
func encodeGrantByPrincipalPrefix(principalRT, principalID string) []byte {
	buf := make([]byte, 0, 32+len(principalRT)+len(principalID))
	buf = append(buf, versionV3, typeIndex, idxGrantByPrincipal)
	buf = codec.AppendTupleSeparator(buf)
	buf = codec.AppendTupleStrings(buf, principalRT, principalID)
	return codec.AppendTupleSeparator(buf)
}

func encodeGrantByPrincipalResourceTypeIdentityPrefix(principalRT string) []byte {
	buf := make([]byte, 0, 32+len(principalRT))
	buf = append(buf, versionV3, typeIndex, idxGrantByPrincipal)
	buf = codec.AppendTupleSeparator(buf)
	buf = codec.AppendTupleStrings(buf, principalRT)
	return codec.AppendTupleSeparator(buf)
}

// --- Grant by (entitlement, principal-hash) + digest nodes ---
//
// PARTITION CONVENTION. Both keyspaces below are addressed by a digest
// "partition": the raw encoded tail of the entitlement's PRIMARY key —
// the already-escaped, separator-delimited 4-segment tuple
//
//	ent_rt | 0x00 | ent_rid | 0x00 | ent_flag | 0x00 | ent_tail
//
// exactly as appendEntitlementIdentityKey produces it (minus the 3-byte
// header), and exactly as it appears inside every grant primary key.
// Using the encoded bytes (spliced, never decode+re-encoded) keeps the
// seal-time build allocation-free and makes index/digest partition
// order byte-identical to primary entitlement key order. Segments are
// escaped and the segment COUNT is fixed at four, so partitions are
// mutually prefix-free even though they contain bare 0x00 separators.
// appendEntitlementIdentityTail is the from-identity constructor.

// appendEntitlementIdentityTail appends the digest-partition bytes for
// an entitlement identity: the 4-segment encoded tuple tail of its
// primary key (no header, no trailing separator). MUST stay in
// byte-lockstep with appendEntitlementIdentityKey's tail.
func appendEntitlementIdentityTail(dst []byte, id entitlementIdentity) []byte {
	return codec.AppendTupleStrings(dst, id.resourceTypeID, id.resourceID, id.flagComponent(), id.tail)
}

// encodeGrantByEntPrincHashEntPrefix is the by-value prefix for "all
// hash-index rows under this entitlement partition", in principal-hash
// order. partition is the raw encoded entitlement tail (see the
// partition convention above) and is appended RAW — it is already
// tuple-encoded. The trailing separator is load-bearing (see the
// keys.go convention doc); the raw bucket hash follows it. Its output
// length is also the offset decoders use to locate the raw hash region.
//
// The full index key shape (see grant_digest.go for the hash and value
// definitions):
//
//	v3 | typeIndex | idxGrantByEntitlementPrincipalHash | 0x00 |
//	    ent_rt | 0x00 | ent_rid | 0x00 | ent_flag | 0x00 | ent_tail | 0x00 |
//	    <raw bucketHash: digestBucketHashLen bytes> |
//	    principal_rt | 0x00 | principal_id
//	  -> value: grant content hash (xxHash64, 8 bytes)
//
// (entitlement identity, principal identity) is the grant PRIMARY
// identity, so the index holds exactly one row per grant by
// construction. Because the bucket hash is raw it can contain 0x00, so
// generic tuple walkers must NOT be pointed past the partition prefix —
// their walk would derail on hash bytes. Decode positionally: the hash
// occupies exactly digestBucketHashLen bytes after the prefix.
func encodeGrantByEntPrincHashEntPrefix(partition string) []byte {
	buf := make([]byte, 0, 6+len(partition))
	buf = append(buf, versionV3, typeIndex, idxGrantByEntitlementPrincipalHash)
	buf = codec.AppendTupleSeparator(buf)
	buf = append(buf, partition...)
	return codec.AppendTupleSeparator(buf)
}

// GrantByEntPrincHashLowerBound / UpperBound bound the entire
// by_entitlement_principal_hash index. Exported for the
// cleanup/clone/compaction keyspace plans.
func GrantByEntPrincHashLowerBound() []byte {
	return []byte{versionV3, typeIndex, idxGrantByEntitlementPrincipalHash}
}

func GrantByEntPrincHashUpperBound() []byte {
	return upperBoundOf(GrantByEntPrincHashLowerBound())
}

// Digest node keys.
//
//	v3 | typeDigest | index_id(1 byte) | 0x00 | esc(partition) | 0x00 | level(1 byte) | bucket_prefix
//
// index_id discriminates WHICH digested index the node belongs to (the
// digested index's own idx* byte, see digestIndexSpec) — it leads the
// tail so all of a file's digests for every index are still a single
// contiguous range for the cleanup/clone plans. The partition (which
// for the grant digest contains bare 0x00 separators — see the
// partition convention above) is tuple-ESCAPED here, unlike in the
// index key: node keys carry a level byte and a raw bucket prefix after
// it, so the partition must parse as a single ordinary tuple element.
// The tuple escape is order-preserving, so node keys still sort in raw
// partition order. level 0 is the root (bucket_prefix empty); level 1
// is the single leaf level, one node per non-empty bucket, whose
// bucket_prefix is the bucket index LEFT-ALIGNED in 2 raw bytes
// (digestLeafPrefixLen). The left alignment makes leaf keys sort in
// bucket-hash order at every digest width, so the comparison's
// fold-to-coarser-width merge is a single contiguous scan of this
// range. See digest.go for the node value framing.
func encodeDigestNodeKey(indexID byte, partition string, level byte, bucketPrefix []byte) []byte {
	buf := encodeDigestPartitionPrefix(indexID, partition)
	buf = append(buf, level)
	return append(buf, bucketPrefix...)
}

// encodeDigestPartitionPrefix is the prefix of every digest node key
// for one (index, partition) — the range a rebuild clears before
// writing (the build only Sets nodes; without the leading DeleteRange a
// width change or an emptied bucket would leave stale nodes for the
// comparison merge scan to read) and the range the invalidation paths
// remove when a record mutation invalidates the partition.
func encodeDigestPartitionPrefix(indexID byte, partition string) []byte {
	buf := make([]byte, 0, 7+len(partition))
	buf = append(buf, versionV3, typeDigest)
	buf = append(buf, indexID)
	buf = codec.AppendTupleSeparator(buf)
	buf = codec.AppendTupleStrings(buf, partition)
	return codec.AppendTupleSeparator(buf)
}

// DigestLowerBound / UpperBound bound the entire digest keyspace (all
// digested indexes). Exported for the cleanup/clone/compaction keyspace
// plans. typeDigest is NOT adjacent to typeIndex — typeCounter and
// typeSession sit between them — so an excise span must never be
// widened to cover both the hash index and the digests in one range.
func DigestLowerBound() []byte {
	return []byte{versionV3, typeDigest}
}

func DigestUpperBound() []byte {
	return upperBoundOf(DigestLowerBound())
}

// --- ResourceType ---

// encodeResourceTypeKey returns the primary key for a resource_type:
//
//	v3 | typeResourceType | 0x00 | external_id
//
// Paired with encodeResourceTypePrefix (by-type prefix).
func encodeResourceTypeKey(externalID string) []byte {
	buf := make([]byte, 0, 3+len(externalID))
	buf = append(buf, versionV3, typeResourceType)
	buf = codec.AppendTupleSeparator(buf)
	return codec.AppendTupleStrings(buf, externalID)
}

// encodeResourceTypePrefix is the by-type prefix for resource_types.
func encodeResourceTypePrefix() []byte {
	return []byte{versionV3, typeResourceType}
}

// --- Resource ---

// encodeResourceKey returns the primary key for a resource:
//
//	v3 | typeResource | 0x00 | resource_type_id | 0x00 | resource_id
//
// Paired with encodeResourcePrefix (by-type prefix).
func encodeResourceKey(resourceTypeID, resourceID string) []byte {
	buf := make([]byte, 0, 32)
	buf = append(buf, versionV3, typeResource)
	buf = codec.AppendTupleSeparator(buf)
	return codec.AppendTupleStrings(buf, resourceTypeID, resourceID)
}

// encodeResourcePrefix is the by-type prefix for resources.
func encodeResourcePrefix() []byte {
	return []byte{versionV3, typeResource}
}

// encodeResourceByParentIndexKey: index of children-by-parent:
//
//	v3 | typeIndex | idxResourceByParent | 0x00 |
//	    parent_rt | 0x00 | parent_id | 0x00 | child_rt | 0x00 | child_id
//
// Paired with encodeResourceByParentPrefix (by-value prefix, with
// trailing sep).
func encodeResourceByParentIndexKey(parentRT, parentID, childRT, childID string) []byte {
	buf := make([]byte, 0, 64)
	buf = append(buf, versionV3, typeIndex, idxResourceByParent)
	buf = codec.AppendTupleSeparator(buf)
	return codec.AppendTupleStrings(buf, parentRT, parentID, childRT, childID)
}

// encodeResourceByParentPrefix is the by-value prefix for "all
// children of (parent_rt, parent_id)". Trailing sep is load-bearing.
func encodeResourceByParentPrefix(parentRT, parentID string) []byte {
	buf := make([]byte, 0, 32+len(parentRT)+len(parentID))
	buf = append(buf, versionV3, typeIndex, idxResourceByParent)
	buf = codec.AppendTupleSeparator(buf)
	buf = codec.AppendTupleStrings(buf, parentRT, parentID)
	return codec.AppendTupleSeparator(buf)
}

// --- Entitlement ---

// encodeEntitlementKey returns the primary key for an entitlement:
//
//	v3 | typeEntitlement | 0x00 | external_id
//
// Paired with encodeEntitlementPrefix (by-type prefix).
func encodeEntitlementKey(externalID string) []byte {
	buf := make([]byte, 0, 3+len(externalID))
	buf = append(buf, versionV3, typeEntitlement)
	buf = codec.AppendTupleSeparator(buf)
	return codec.AppendTupleStrings(buf, externalID)
}

func encodeEntitlementIdentityKey(id entitlementIdentity) []byte {
	return appendEntitlementIdentityKey(make([]byte, 0, 96), id)
}

func appendEntitlementIdentityKey(dst []byte, id entitlementIdentity) []byte {
	dst = append(dst, versionV3, typeEntitlement)
	dst = codec.AppendTupleSeparator(dst)
	return codec.AppendTupleStrings(dst, id.resourceTypeID, id.resourceID, id.flagComponent(), id.tail)
}

// encodeEntitlementPrefix is the by-type prefix for entitlements.
func encodeEntitlementPrefix() []byte {
	return []byte{versionV3, typeEntitlement}
}

// encodeEntitlementByResourceIndexKey:
//
//	v3 | typeIndex | idxEntitlementByResource | 0x00 |
//	    resource_type_id | 0x00 | resource_id | 0x00 | external_id
//
// Paired with encodeEntitlementByResourcePrefix (by-value prefix,
// with trailing sep).
// func encodeEntitlementByResourceIndexKey(resourceTypeID, resourceID, externalID string) []byte {
// 	buf := make([]byte, 0, 64)
// 	buf = append(buf, versionV3, typeIndex, idxEntitlementByResource)
// 	buf = codec.AppendTupleSeparator(buf)
// 	return codec.AppendTupleStrings(buf, resourceTypeID, resourceID, externalID)
// }

// encodeEntitlementByResourcePrefix is the by-value prefix for "all
// entitlements on (resource_type_id, resource_id)". Trailing sep is
// load-bearing.
// func encodeEntitlementByResourcePrefix(resourceTypeID, resourceID string) []byte {
// 	buf := make([]byte, 0, 32+len(resourceTypeID)+len(resourceID))
// 	buf = append(buf, versionV3, typeIndex, idxEntitlementByResource)
// 	buf = codec.AppendTupleSeparator(buf)
// 	buf = codec.AppendTupleStrings(buf, resourceTypeID, resourceID)
// 	return codec.AppendTupleSeparator(buf)
// }

func encodeEntitlementPrimaryResourcePrefix(resourceTypeID, resourceID string) []byte {
	buf := make([]byte, 0, 64)
	buf = append(buf, versionV3, typeEntitlement)
	buf = codec.AppendTupleSeparator(buf)
	buf = codec.AppendTupleStrings(buf, resourceTypeID, resourceID)
	return codec.AppendTupleSeparator(buf)
}

// --- Asset ---

// encodeAssetKey returns the primary key for an asset:
//
//	v3 | typeAsset | 0x00 | external_id
//
// Paired with encodeAssetPrefix (by-type prefix).
func encodeAssetKey(externalID string) []byte {
	buf := make([]byte, 0, 3+len(externalID))
	buf = append(buf, versionV3, typeAsset)
	buf = codec.AppendTupleSeparator(buf)
	return codec.AppendTupleStrings(buf, externalID)
}

// encodeAssetPrefix is the by-type prefix for assets.
func encodeAssetPrefix() []byte {
	return []byte{versionV3, typeAsset}
}

// --- SyncRun ---
//
// SyncRun is a single fixed key — the file holds exactly one sync, so
// the sync-run record lives at one well-known location:
//
//	v3 | typeSyncRun        (no sync_id, no tail)
//
// A second StartNewSync overwrites this record (and ResetForNewSync
// wipes the data keyspace), so two sync-run records can never coexist.
// encodeSyncRunKey and encodeSyncRunFullPrefix are therefore identical;
// both are retained for call-site clarity (point write/read vs.
// IterateAllSyncRuns range scan).
func encodeSyncRunKey() []byte {
	return []byte{versionV3, typeSyncRun}
}

func encodeSyncRunFullPrefix() []byte {
	return []byte{versionV3, typeSyncRun}
}

// --- Exported bound helpers for synccompactor/pebble ---
//
// Each returns one end of the half-open [lo, hi) range covering an
// entire record-type or index bucket. No sync_id: a v3 c1z holds one
// sync, so a bucket's whole key range belongs to it.

func ResourceTypeLowerBound() []byte { return encodeResourceTypePrefix() }
func ResourceTypeUpperBound() []byte { return upperBoundOf(encodeResourceTypePrefix()) }

func ResourceLowerBound() []byte { return encodeResourcePrefix() }
func ResourceUpperBound() []byte { return upperBoundOf(encodeResourcePrefix()) }

func ResourceByParentLowerBound() []byte {
	return []byte{versionV3, typeIndex, idxResourceByParent}
}
func ResourceByParentUpperBound() []byte { return upperBoundOf(ResourceByParentLowerBound()) }

func EntitlementLowerBound() []byte { return encodeEntitlementPrefix() }
func EntitlementUpperBound() []byte { return upperBoundOf(encodeEntitlementPrefix()) }

func EntitlementByResourceLowerBound() []byte {
	return []byte{versionV3, typeIndex, idxEntitlementByResource}
}
func EntitlementByResourceUpperBound() []byte { return upperBoundOf(EntitlementByResourceLowerBound()) }

// GrantLowerBound / GrantUpperBound give the half-open [lo, hi) range
// covering every grant primary key. Exported for synccompactor/pebble.
func GrantLowerBound() []byte { return encodeGrantPrefix() }
func GrantUpperBound() []byte { return upperBoundOf(encodeGrantPrefix()) }

// GrantByEntitlementLowerBound returns the retired by_entitlement index range.
// Kept so cleanup/migration can delete old files' index entries.
func GrantByEntitlementLowerBound() []byte {
	return []byte{versionV3, typeIndex, idxGrantByEntitlement}
}
func GrantByEntitlementUpperBound() []byte { return upperBoundOf(GrantByEntitlementLowerBound()) }

func GrantByPrincipalLowerBound() []byte {
	return []byte{versionV3, typeIndex, idxGrantByPrincipal}
}
func GrantByPrincipalUpperBound() []byte { return upperBoundOf(GrantByPrincipalLowerBound()) }

func GrantByNeedsExpansionLowerBound() []byte { return encodeGrantByNeedsExpansionPrefix() }
func GrantByNeedsExpansionUpperBound() []byte {
	return upperBoundOf(GrantByNeedsExpansionLowerBound())
}

// GrantByPrincipalResourceTypeLowerBound returns a retired folded index range.
// Principal-resource-type scans are served by idxGrantByPrincipal prefixes.
func GrantByPrincipalResourceTypeLowerBound() []byte {
	return []byte{versionV3, typeIndex, idxGrantByPrincipalResourceType}
}
func GrantByPrincipalResourceTypeUpperBound() []byte {
	return upperBoundOf(GrantByPrincipalResourceTypeLowerBound())
}

// GrantByEntitlementResourceLowerBound returns a retired folded index range.
// Entitlement-resource scans are served by primary grant key prefixes.
func GrantByEntitlementResourceLowerBound() []byte {
	return []byte{versionV3, typeIndex, idxGrantByEntitlementResource}
}
func GrantByEntitlementResourceUpperBound() []byte {
	return upperBoundOf(GrantByEntitlementResourceLowerBound())
}

func AssetLowerBound() []byte { return encodeAssetPrefix() }
func AssetUpperBound() []byte { return upperBoundOf(encodeAssetPrefix()) }

func SyncRunLowerBound() []byte { return encodeSyncRunKey() }
func SyncRunUpperBound() []byte { return upperBoundOf(encodeSyncRunKey()) }

// upperBoundOf returns the smallest key strictly greater than every
// key with the given prefix. Used as the UpperBound in pebble.IterOptions
// for range scans. Delegates to codec.KeyUpperBound so the increment/
// all-0xff semantics live in one place shared with the tuple codec.
func upperBoundOf(prefix []byte) []byte {
	return codec.KeyUpperBound(prefix)
}
