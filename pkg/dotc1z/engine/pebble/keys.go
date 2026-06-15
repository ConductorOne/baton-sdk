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
	typeEngineMeta   byte = 0xFF
)

// Index-discriminator bytes (second byte after typeIndex). One byte
// per declared index across all record types; reuse across record
// types is fine because the index key is scoped by the type byte that
// preceded it.
const (
	idxResourceByParent             byte = 0x01
	idxEntitlementByResource        byte = 0x02
	idxGrantByEntitlement           byte = 0x03
	idxGrantByPrincipal             byte = 0x04
	idxGrantByNeedsExpansion        byte = 0x05
	idxGrantByPrincipalResourceType byte = 0x06
	idxGrantByEntitlementResource   byte = 0x07
)

// --- Grant ---

// encodeGrantKey returns the primary key for a grant.
//
//	v3 | typeGrant | 0x00 | external_id
//
// Paired with encodeGrantPrefix (by-type prefix, no trailing sep).
func encodeGrantKey(externalID string) []byte {
	buf := make([]byte, 0, 3+len(externalID))
	buf = append(buf, versionV3, typeGrant)
	buf = codec.AppendTupleSeparator(buf)
	return codec.AppendTupleStrings(buf, externalID)
}

// encodeGrantPrefix returns the by-type prefix for iterating all
// grants. Paired with encodeGrantKey.
func encodeGrantPrefix() []byte {
	return []byte{versionV3, typeGrant}
}

// encodeGrantByEntitlementIndexKey is the by_entitlement secondary
// index on GrantRecord:
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
	buf := make([]byte, 0, 64)
	buf = append(buf, versionV3, typeIndex, idxGrantByEntitlement)
	buf = codec.AppendTupleSeparator(buf)
	return codec.AppendTupleStrings(buf, entitlementID, principalRT, principalID, externalID)
}

// encodeGrantByPrincipalIndexKey:
//
//	v3 | typeIndex | idxGrantByPrincipal | 0x00 |
//	    principal_resource_type | 0x00 |
//	    principal_resource_id | 0x00 |
//	    external_id
//
// Paired with encodeGrantByPrincipalPrefix (by-value prefix, with
// trailing sep).
func encodeGrantByPrincipalIndexKey(principalRT, principalID, externalID string) []byte {
	buf := make([]byte, 0, 64)
	buf = append(buf, versionV3, typeIndex, idxGrantByPrincipal)
	buf = codec.AppendTupleSeparator(buf)
	return codec.AppendTupleStrings(buf, principalRT, principalID, externalID)
}

// encodeGrantByEntitlementPrefix is the by-value prefix for "all
// grants with this entitlement_id". Trailing separator is
// load-bearing — see the keys.go convention doc.
func encodeGrantByEntitlementPrefix(entitlementID string) []byte {
	buf := make([]byte, 0, 32+len(entitlementID))
	buf = append(buf, versionV3, typeIndex, idxGrantByEntitlement)
	buf = codec.AppendTupleSeparator(buf)
	buf = codec.AppendTupleStrings(buf, entitlementID)
	return codec.AppendTupleSeparator(buf)
}

// encodeGrantByEntitlementPrincipalPrefix is the by-value prefix for
// "all grants with this entitlement_id and principal". It reuses the
// existing by_entitlement index tail:
//
//	entitlement_id | principal_resource_type | principal_resource_id | external_id
func encodeGrantByEntitlementPrincipalPrefix(entitlementID, principalRT, principalID string) []byte {
	buf := make([]byte, 0, 32+len(entitlementID)+len(principalRT)+len(principalID))
	buf = append(buf, versionV3, typeIndex, idxGrantByEntitlement)
	buf = codec.AppendTupleSeparator(buf)
	buf = codec.AppendTupleStrings(buf, entitlementID, principalRT, principalID)
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
	buf := make([]byte, 0, 5+len(externalID))
	buf = append(buf, versionV3, typeIndex, idxGrantByNeedsExpansion)
	buf = codec.AppendTupleSeparator(buf)
	return codec.AppendTupleStrings(buf, externalID)
}

// encodeGrantByPrincipalResourceTypeIndexKey: by-principal-RT
// index. Closes the only O(G) full-scan path in the Reader
// (ListGrantsForResourceType, which previously walked the entire
// grant primary range and post-filtered).
//
//	v3 | typeIndex | idxGrantByPrincipalResourceType | 0x00 |
//	    principal_resource_type | 0x00 |
//	    external_id
//
// Paired with encodeGrantByPrincipalResourceTypePrefix (by-value
// prefix, with trailing sep).
func encodeGrantByPrincipalResourceTypeIndexKey(principalRT, externalID string) []byte {
	buf := make([]byte, 0, 64)
	buf = append(buf, versionV3, typeIndex, idxGrantByPrincipalResourceType)
	buf = codec.AppendTupleSeparator(buf)
	return codec.AppendTupleStrings(buf, principalRT, externalID)
}

// encodeGrantByPrincipalResourceTypePrefix is the by-value prefix
// for "all grants whose principal has the given resource_type".
// Trailing sep is load-bearing — see keys.go convention.
func encodeGrantByPrincipalResourceTypePrefix(principalRT string) []byte {
	buf := make([]byte, 0, 32+len(principalRT))
	buf = append(buf, versionV3, typeIndex, idxGrantByPrincipalResourceType)
	buf = codec.AppendTupleSeparator(buf)
	buf = codec.AppendTupleStrings(buf, principalRT)
	return codec.AppendTupleSeparator(buf)
}

// encodeGrantByNeedsExpansionPrefix is the by-type prefix for all
// grants that still need expansion processing.
func encodeGrantByNeedsExpansionPrefix() []byte {
	return []byte{versionV3, typeIndex, idxGrantByNeedsExpansion}
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

// encodeGrantByEntitlementResourceIndexKey is the by_entitlement_resource
// secondary index on GrantRecord. Indexes grants by the
// resource side of their entitlement (i.e. the resource the
// entitlement is on — the group/role/app/etc., NOT the principal).
//
//	v3 | typeIndex | idxGrantByEntitlementResource | 0x00 |
//	    ent_resource_type | 0x00 |
//	    ent_resource_id   | 0x00 |
//	    external_id  (tail element for index-row uniqueness)
//
// Drives Adapter.ListGrants / ListWithAnnotationsForResourcePage when
// req.Resource is set — matches SQLite's `listGrantsGeneric` which
// filters on grants.resource_id / resource_type_id (the entitlement-
// side resource columns). The pre-existing by_principal index served
// the wrong semantic and produced silently-empty reads for callers
// that wanted "grants on this group" rather than "grants where this
// group is a principal".
//
// Paired with encodeGrantByEntitlementResourcePrefix (by-value prefix,
// with trailing sep).
func encodeGrantByEntitlementResourceIndexKey(entRT, entRID, externalID string) []byte {
	buf := make([]byte, 0, 64)
	buf = append(buf, versionV3, typeIndex, idxGrantByEntitlementResource)
	buf = codec.AppendTupleSeparator(buf)
	return codec.AppendTupleStrings(buf, entRT, entRID, externalID)
}

// encodeGrantByEntitlementResourcePrefix is the by-value prefix for
// "all grants whose entitlement is on this resource". Trailing sep is
// load-bearing — see keys.go convention.
func encodeGrantByEntitlementResourcePrefix(entRT, entRID string) []byte {
	buf := make([]byte, 0, 32+len(entRT)+len(entRID))
	buf = append(buf, versionV3, typeIndex, idxGrantByEntitlementResource)
	buf = codec.AppendTupleSeparator(buf)
	buf = codec.AppendTupleStrings(buf, entRT, entRID)
	return codec.AppendTupleSeparator(buf)
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
func encodeEntitlementByResourceIndexKey(resourceTypeID, resourceID, externalID string) []byte {
	buf := make([]byte, 0, 64)
	buf = append(buf, versionV3, typeIndex, idxEntitlementByResource)
	buf = codec.AppendTupleSeparator(buf)
	return codec.AppendTupleStrings(buf, resourceTypeID, resourceID, externalID)
}

// encodeEntitlementByResourcePrefix is the by-value prefix for "all
// entitlements on (resource_type_id, resource_id)". Trailing sep is
// load-bearing.
func encodeEntitlementByResourcePrefix(resourceTypeID, resourceID string) []byte {
	buf := make([]byte, 0, 32+len(resourceTypeID)+len(resourceID))
	buf = append(buf, versionV3, typeIndex, idxEntitlementByResource)
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

func GrantByPrincipalResourceTypeLowerBound() []byte {
	return []byte{versionV3, typeIndex, idxGrantByPrincipalResourceType}
}
func GrantByPrincipalResourceTypeUpperBound() []byte {
	return upperBoundOf(GrantByPrincipalResourceTypeLowerBound())
}

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
// for range scans. Increments the last byte; if the prefix is all
// 0xff, no finite exclusive upper bound exists and nil leaves the
// iterator unbounded above.
func upperBoundOf(prefix []byte) []byte {
	end := make([]byte, len(prefix))
	copy(end, prefix)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}
