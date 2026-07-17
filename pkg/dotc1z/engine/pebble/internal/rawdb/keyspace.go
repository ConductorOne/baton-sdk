// The v3 keyspace ABI the choke point enforces: the type/index
// discriminator bytes, the grant primary-key splices, the resource
// value scanners, and the digest invalidation key builders. The typed
// record ops derive their obligations from these directly; the engine
// package imports them for its own encoders and read paths. Everything
// here is a pure function over bytes/strings: no engine state, no
// pebble handles. The full key-layout convention (header shapes,
// tuple encoding, prefix pairing) is documented in the engine
// package's keys.go; the byte formats are an on-disk ABI shared with
// every v3 c1z file. See the codec package for the escape rules.
package rawdb

import (
	"bytes"
	"fmt"

	"google.golang.org/protobuf/encoding/protowire"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/codec"
)

// Type-discriminator bytes for the v3 keyspace. Each top-level
// keyspace occupies one byte.
const (
	VersionV3 byte = 0x03

	TypeResourceType byte = 0x01
	TypeResource     byte = 0x02
	TypeEntitlement  byte = 0x03
	TypeGrant        byte = 0x04
	TypeAsset        byte = 0x05
	TypeSyncRun      byte = 0x06
	TypeIndex        byte = 0x07
	TypeCounter      byte = 0x08
	TypeSession      byte = 0x09
	TypeDigest       byte = 0x0A
	TypeEngineMeta   byte = 0xFF
)

// Index-discriminator bytes (second byte after TypeIndex). One byte
// per declared index across all record types; reuse across record
// types is fine because the index key is scoped by the type byte that
// preceded it.
const (
	IdxResourceByParent             byte = 0x01
	IdxEntitlementByResource        byte = 0x02 // retired: served by entitlement primary key prefixes.
	IdxGrantByEntitlement           byte = 0x03 // retired: grant primary keys are entitlement-first.
	IdxGrantByPrincipal             byte = 0x04
	IdxGrantByNeedsExpansion        byte = 0x05
	IdxGrantByPrincipalResourceType byte = 0x06 // retired: served by IdxGrantByPrincipal prefix scans.
	IdxGrantByEntitlementResource   byte = 0x07 // retired: served by grant primary entitlement-resource prefix scans.
	// IdxGrantByEntitlementPrincipalHash sorts grants by
	// (entitlement identity, hash(principal identity)). Unlike every
	// other grant index its entries carry a VALUE (the grant content
	// hash). It is the substrate the per-entitlement grant digest
	// (TypeDigest) folds over; built ONLY by the seal-time deferred
	// pass, never maintained inline. See the engine's digest.go and
	// grant_digest.go.
	IdxGrantByEntitlementPrincipalHash byte = 0x08
)

// GrantPrimaryKeyPrefixLen is the byte length of the grant primary-key
// header: VersionV3 | TypeGrant | separator.
const GrantPrimaryKeyPrefixLen = 3

// DigestLevelGlobalRoot is the digest node-key level for the
// whole-file grant digest root (see GlobalGrantDigestNodeKey). It is
// deliberately a level value the generic digest core never produces
// (root=0, leaf=1), keeping the global node's key disjoint from every
// per-partition node regardless of partition bytes.
const DigestLevelGlobalRoot byte = 2

// === grant primary-key splices ===

// SplitGrantPrimaryKey locates the partition/principal boundary of a
// grant primary key: the byte offset of the 4th tuple separator in the
// key (the one that ends the entitlement's 4 identity segments). It
// also validates the overall 6-segment shape, exactly like
// AppendGrantByPrincipalKeyFromPrimary. Returns ok=false for keys that
// are not well-formed 6-segment grant identities.
//
// With the returned sep4 in hand every region is a plain sub-slice:
//
//	partition          = key[GrantPrimaryKeyPrefixLen:sep4]
//	principal segments = key[sep4+1:]
func SplitGrantPrimaryKey(primaryKey []byte) (int, bool) {
	if len(primaryKey) < GrantPrimaryKeyPrefixLen ||
		primaryKey[0] != VersionV3 || primaryKey[1] != TypeGrant || primaryKey[2] != 0 {
		return 0, false
	}
	sep4 := 0
	off := GrantPrimaryKeyPrefixLen
	for i := range 5 {
		sep := bytes.IndexByte(primaryKey[off:], 0)
		if sep < 0 {
			return 0, false
		}
		off += sep
		if i == 3 {
			sep4 = off
		}
		off++
	}
	if bytes.IndexByte(primaryKey[off:], 0) >= 0 {
		return 0, false
	}
	return sep4, true
}

// AppendGrantByPrincipalKeyFromPrimary builds the by_principal index key
// directly from a primary grant key by permuting its tuple segments, into
// dst. The primary tail is ent_rt|ent_rid|ent_kind|ent_name|p_rt|p_id and the
// index tail is p_rt|p_id|ent_rt|ent_rid|ent_kind|ent_name — the segments are
// already escaped, and the tuple escaping is canonical, so splicing the raw
// bytes is byte-identical to decode + re-encode while skipping six string
// allocations and a fresh key buffer per row (that decode/re-encode pair was
// ~7GB of allocations per whale deferred build). Returns ok=false for keys
// that do not have exactly six segments.
//
// Pinned against the decode+re-encode path by the engine's
// TestAppendGrantByPrincipalKeyFromPrimary.
func AppendGrantByPrincipalKeyFromPrimary(dst, primaryKey []byte) ([]byte, bool) {
	if len(primaryKey) < GrantPrimaryKeyPrefixLen ||
		primaryKey[0] != VersionV3 || primaryKey[1] != TypeGrant || primaryKey[2] != 0 {
		return dst, false
	}
	tail := primaryKey[GrantPrimaryKeyPrefixLen:]
	// Split into exactly six escaped segments on bare separator bytes (the
	// escape rules guarantee segments contain no bare 0x00).
	var segs [6][]byte
	rest := tail
	for i := 0; i < 5; i++ {
		sep := bytes.IndexByte(rest, 0)
		if sep < 0 {
			return dst, false
		}
		segs[i] = rest[:sep] // #nosec G602 -- false positive: IndexByte returned >= 0 above, so sep < len(rest).
		rest = rest[sep+1:]
	}
	if bytes.IndexByte(rest, 0) >= 0 {
		return dst, false
	}
	segs[5] = rest

	dst = append(dst, VersionV3, TypeIndex, IdxGrantByPrincipal, 0)
	for i, idx := range [6]int{4, 5, 0, 1, 2, 3} { // p_rt, p_id, ent_rt, ent_rid, ent_kind, ent_name
		if i > 0 {
			dst = append(dst, 0)
		}
		dst = append(dst, segs[idx]...)
	}
	return dst, true
}

// AppendGrantByNeedsExpansionKeyFromPrimary builds the
// by_needs_expansion index key directly from a primary grant key: the
// primary's separator+tail IS the index key's tail, byte-identical
// (pinned by the engine's TestNeedsExpansionKeyHeaderSpliceFromPrimary),
// so the splice is a header swap. Validates the same 6-segment shape as
// the by_principal splice so a malformed key cannot silently produce a
// malformed index entry.
func AppendGrantByNeedsExpansionKeyFromPrimary(dst, primaryKey []byte) ([]byte, bool) {
	if _, ok := SplitGrantPrimaryKey(primaryKey); !ok {
		return dst, false
	}
	dst = append(dst, VersionV3, TypeIndex, IdxGrantByNeedsExpansion)
	return append(dst, primaryKey[2:]...), true
}

// === resource index key + value scanners ===

// EncodeResourceByParentIndexKey: index of children-by-parent:
//
//	v3 | TypeIndex | IdxResourceByParent | 0x00 |
//	    parent_rt | 0x00 | parent_id | 0x00 | child_rt | 0x00 | child_id
func EncodeResourceByParentIndexKey(parentRT, parentID, childRT, childID string) []byte {
	buf := make([]byte, 0, 64)
	buf = append(buf, VersionV3, TypeIndex, IdxResourceByParent)
	buf = codec.AppendTupleSeparator(buf)
	return codec.AppendTupleStrings(buf, parentRT, parentID, childRT, childID)
}

// ScanResourceParentRaw scans a marshaled v3 ResourceRecord value for
// its parent ref (field 6) without a full unmarshal. ("", "") = no
// parent. Keeps the LAST occurrence of the field, approximating proto
// merge semantics; values written by this SDK carry at most one
// occurrence, so this only matters for foreign writers.
func ScanResourceParentRaw(value []byte) (string, string, error) {
	var rt, id string
	for len(value) > 0 {
		num, typ, n := protowire.ConsumeTag(value)
		if n < 0 {
			return "", "", protowire.ParseError(n)
		}
		value = value[n:]
		if num != 6 {
			n = protowire.ConsumeFieldValue(num, typ, value)
			if n < 0 {
				return "", "", protowire.ParseError(n)
			}
			value = value[n:]
			continue
		}
		if typ != protowire.BytesType {
			return "", "", fmt.Errorf("raw record: resource parent has wire type %v", typ)
		}
		msg, n := protowire.ConsumeBytes(value)
		if n < 0 {
			return "", "", protowire.ParseError(n)
		}
		var err error
		rt, id, err = ScanResourceRefRaw(msg)
		if err != nil {
			return "", "", err
		}
		value = value[n:]
	}
	return rt, id, nil
}

// ScanResourceRefRaw extracts (resource_type_id, resource_id) from a
// marshaled ResourceRef/ResourceId-shaped message (fields 1 and 2).
func ScanResourceRefRaw(value []byte) (string, string, error) {
	var rt, id []byte
	err := ScanResourceRefRawBytes(value, func(num protowire.Number, val []byte) {
		switch num {
		case 1:
			rt = val
		case 2:
			id = val
		default:
		}
	})
	return string(rt), string(id), err
}

// ScanResourceRefRawBytes walks a marshaled ref-shaped message and
// hands the raw bytes of fields 1-3 to set. Borrowed bytes: valid only
// during the callback.
func ScanResourceRefRawBytes(value []byte, set func(protowire.Number, []byte)) error {
	for len(value) > 0 {
		num, typ, n := protowire.ConsumeTag(value)
		if n < 0 {
			return protowire.ParseError(n)
		}
		value = value[n:]
		if typ != protowire.BytesType {
			n = protowire.ConsumeFieldValue(num, typ, value)
			if n < 0 {
				return protowire.ParseError(n)
			}
			value = value[n:]
			continue
		}
		b, n := protowire.ConsumeBytes(value)
		if n < 0 {
			return protowire.ParseError(n)
		}
		switch num {
		case 1, 2, 3:
			set(num, b)
		default:
		}
		value = value[n:]
	}
	return nil
}

// === grant digest invalidation keys ===
//
// A grant mutation on a digest-armed file owes three tombstone sets
// (present-means-exact — a mutated partition's digest must read as
// missing, never stale-but-present): the entitlement partition's
// digest node range, the whole-file global root (the fold of every
// partition root is stale once any partition is), and the partition's
// hash-index range. The builders below give rawdb (and the engine's
// digest repair paths) those ranges from raw partition bytes.

// DigestPartitionPrefix is the prefix of every digest node key for one
// (index, partition): v3 | TypeDigest | indexID | 0x00 | esc(partition) | 0x00.
// The partition is tuple-ESCAPED here, unlike in the hash-index key:
// node keys carry a level byte and a raw bucket prefix after it, so the
// partition must parse as a single ordinary tuple element.
func DigestPartitionPrefix(indexID byte, partition string) []byte {
	buf := make([]byte, 0, 7+len(partition))
	buf = append(buf, VersionV3, TypeDigest)
	buf = append(buf, indexID)
	buf = codec.AppendTupleSeparator(buf)
	buf = codec.AppendTupleStrings(buf, partition)
	return codec.AppendTupleSeparator(buf)
}

// GlobalGrantDigestNodeKey returns the storage key for the whole-file
// grant digest root: a level-DigestLevelGlobalRoot node under an empty
// partition of the grant digest index.
func GlobalGrantDigestNodeKey() []byte {
	buf := DigestPartitionPrefix(IdxGrantByEntitlementPrincipalHash, "")
	return append(buf, DigestLevelGlobalRoot)
}

// GrantHashIndexEntitlementPrefix is the by-value prefix bounding one
// entitlement partition's rows in the by_entitlement_principal_hash
// index: v3 | TypeIndex | idx | 0x00 | partition | 0x00. The partition
// bytes are already escaped (they are a raw sub-slice of the grant
// primary key), so unlike DigestPartitionPrefix there is no re-escape.
func GrantHashIndexEntitlementPrefix(partition string) []byte {
	buf := make([]byte, 0, 6+len(partition))
	buf = append(buf, VersionV3, TypeIndex, IdxGrantByEntitlementPrincipalHash)
	buf = codec.AppendTupleSeparator(buf)
	buf = append(buf, partition...)
	return codec.AppendTupleSeparator(buf)
}

// UpperBound returns the exclusive upper bound for a key prefix
// (codec.KeyUpperBound re-exported for range builders here and in
// rawdb).
func UpperBound(prefix []byte) []byte { return codec.KeyUpperBound(prefix) }

// === engine-meta markers owned by rawdb ===

// DeferredIdxPendingKey is the durable marker that grant writes have
// skipped the inline by_principal index and a deferred rebuild is
// owed. The in-memory flag alone cannot survive a process restart: a
// sync interrupted after its expansion writes and resumed in a fresh
// process would otherwise write nothing (idempotent re-run), never
// re-arm the flag, and EndSync would skip the rebuild — saving a
// "finished" c1z whose by_principal index misses every
// deferred-written grant.
func DeferredIdxPendingKey() []byte {
	buf := make([]byte, 0, 2+len("deferred_grant_idx_pending"))
	buf = append(buf, VersionV3, TypeEngineMeta)
	return codec.AppendTupleStrings(buf, "deferred_grant_idx_pending")
}

// DigestKeyspaceBounds bounds the entire digest keyspace (all digested
// indexes) — the presence-probe range for the digests-present flag.
func DigestKeyspaceBounds() ([]byte, []byte) {
	lo := []byte{VersionV3, TypeDigest}
	return lo, UpperBound(lo)
}
