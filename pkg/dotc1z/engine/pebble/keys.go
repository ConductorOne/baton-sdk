package pebble

import (
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/codec"
)

// Key layout convention.
//
// Every v3 Pebble key has the same three-region shape:
//
//	[ fixed header ][ raw sync_id_bytes (20) ][ 0x00 ][ tuple-encoded tail ]
//
// where "fixed header" is one of:
//
//   - primary key:  versionV3 | typeXxx                         (2 bytes)
//   - index key:    versionV3 | typeIndex | idxXxx              (3 bytes)
//
// The sync_id is appended raw (not tuple-encoded) because KSUIDs are
// always exactly 20 bytes — the boundary is implicit by position, not
// by scanning for a separator. The 0x00 byte immediately after the
// sync_id starts the variable tuple-encoded tail; no decoder ever
// scans into the sync_id bytes.
//
// Each primary/index key shape is paired with TWO prefix shapes used
// for range scans:
//
//   - by-sync prefix:    header | sync_id
//     ← matches every key in this bucket under one sync. No trailing
//     separator; the iterator runs to upperBoundOf(prefix).
//
//   - by-value prefix:   header | sync_id | 0x00 | elem(s) | 0x00
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
// are an ABI: every existing v3 c1z file depends on them. Changing
// them requires a schema migration. See
// pkg/dotc1z/engine/pebble/index_migrations.go for the upgrade
// framework.

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
//	v3 | typeGrant | sync_id_bytes | 0x00 | external_id
//
// Paired with encodeGrantPrefix (by-sync prefix, no trailing sep).
// sync_id is the 20-byte canonical KSUID binary form (see
// codec.EncodeSyncID).
func encodeGrantKey(syncIDBytes []byte, externalID string) []byte {
	buf := make([]byte, 0, 5+len(syncIDBytes)+len(externalID))
	buf = append(buf, versionV3, typeGrant)
	buf = append(buf, syncIDBytes...)
	buf = codec.AppendTupleSeparator(buf)
	return codec.AppendTupleStrings(buf, externalID)
}

// encodeGrantPrefix returns the by-sync prefix for iterating all
// grants in a sync. Paired with encodeGrantKey.
func encodeGrantPrefix(syncIDBytes []byte) []byte {
	buf := make([]byte, 0, 2+len(syncIDBytes))
	buf = append(buf, versionV3, typeGrant)
	return append(buf, syncIDBytes...)
}

// encodeGrantByEntitlementIndexKey is the by_entitlement secondary
// index on GrantRecord:
//
//	v3 | typeIndex | idxGrantByEntitlement | sync_id_bytes | 0x00 |
//	    entitlement_id | 0x00 |
//	    principal_resource_type | 0x00 |
//	    principal_resource_id | 0x00 |
//	    external_id   (tail element for index-row uniqueness)
//
// Paired with encodeGrantByEntitlementPrefix (by-value prefix, with
// trailing sep).
func encodeGrantByEntitlementIndexKey(syncIDBytes []byte, entitlementID, principalRT, principalID, externalID string) []byte {
	buf := make([]byte, 0, 64)
	buf = append(buf, versionV3, typeIndex, idxGrantByEntitlement)
	buf = append(buf, syncIDBytes...)
	buf = codec.AppendTupleSeparator(buf)
	return codec.AppendTupleStrings(buf, entitlementID, principalRT, principalID, externalID)
}

// encodeGrantByPrincipalIndexKey:
//
//	v3 | typeIndex | idxGrantByPrincipal | sync_id_bytes | 0x00 |
//	    principal_resource_type | 0x00 |
//	    principal_resource_id | 0x00 |
//	    external_id
//
// Paired with encodeGrantByPrincipalPrefix (by-value prefix, with
// trailing sep).
func encodeGrantByPrincipalIndexKey(syncIDBytes []byte, principalRT, principalID, externalID string) []byte {
	buf := make([]byte, 0, 64)
	buf = append(buf, versionV3, typeIndex, idxGrantByPrincipal)
	buf = append(buf, syncIDBytes...)
	buf = codec.AppendTupleSeparator(buf)
	return codec.AppendTupleStrings(buf, principalRT, principalID, externalID)
}

// encodeGrantByEntitlementPrefix is the by-value prefix for "all
// grants in this sync with this entitlement_id". Trailing separator
// is load-bearing — see the keys.go convention doc.
func encodeGrantByEntitlementPrefix(syncIDBytes []byte, entitlementID string) []byte {
	buf := make([]byte, 0, 32+len(entitlementID))
	buf = append(buf, versionV3, typeIndex, idxGrantByEntitlement)
	buf = append(buf, syncIDBytes...)
	buf = codec.AppendTupleSeparator(buf)
	buf = codec.AppendTupleStrings(buf, entitlementID)
	return codec.AppendTupleSeparator(buf)
}

// encodeGrantByEntitlementPrincipalPrefix is the by-value prefix for
// "all grants in this sync with this entitlement_id and principal".
// It reuses the existing by_entitlement index tail:
//
//	entitlement_id | principal_resource_type | principal_resource_id | external_id
func encodeGrantByEntitlementPrincipalPrefix(syncIDBytes []byte, entitlementID, principalRT, principalID string) []byte {
	buf := make([]byte, 0, 32+len(entitlementID)+len(principalRT)+len(principalID))
	buf = append(buf, versionV3, typeIndex, idxGrantByEntitlement)
	buf = append(buf, syncIDBytes...)
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
//	v3 | typeIndex | idxGrantByNeedsExpansion | sync_id_bytes | 0x00 | external_id
//
// Paired with encodeGrantByNeedsExpansionPrefix (by-sync prefix —
// no by-value scan is needed because the only filter is "is this
// flag set?", which is captured by the index's existence).
func encodeGrantByNeedsExpansionIndexKey(syncIDBytes []byte, externalID string) []byte {
	buf := make([]byte, 0, 5+len(syncIDBytes)+len(externalID))
	buf = append(buf, versionV3, typeIndex, idxGrantByNeedsExpansion)
	buf = append(buf, syncIDBytes...)
	buf = codec.AppendTupleSeparator(buf)
	return codec.AppendTupleStrings(buf, externalID)
}

// encodeGrantByPrincipalResourceTypeIndexKey: by-principal-RT
// index. Closes the only O(G) full-scan path in the Reader
// (ListGrantsForResourceType, which previously walked the entire
// grant primary range and post-filtered).
//
//	v3 | typeIndex | idxGrantByPrincipalResourceType | sync_id_bytes | 0x00 |
//	    principal_resource_type | 0x00 |
//	    external_id
//
// Paired with encodeGrantByPrincipalResourceTypePrefix (by-value
// prefix, with trailing sep).
func encodeGrantByPrincipalResourceTypeIndexKey(syncIDBytes []byte, principalRT, externalID string) []byte {
	buf := make([]byte, 0, 64)
	buf = append(buf, versionV3, typeIndex, idxGrantByPrincipalResourceType)
	buf = append(buf, syncIDBytes...)
	buf = codec.AppendTupleSeparator(buf)
	return codec.AppendTupleStrings(buf, principalRT, externalID)
}

// encodeGrantByPrincipalResourceTypePrefix is the by-value prefix
// for "all grants in this sync whose principal has the given
// resource_type". Trailing sep is load-bearing — see keys.go convention.
func encodeGrantByPrincipalResourceTypePrefix(syncIDBytes []byte, principalRT string) []byte {
	buf := make([]byte, 0, 32+len(principalRT))
	buf = append(buf, versionV3, typeIndex, idxGrantByPrincipalResourceType)
	buf = append(buf, syncIDBytes...)
	buf = codec.AppendTupleSeparator(buf)
	buf = codec.AppendTupleStrings(buf, principalRT)
	return codec.AppendTupleSeparator(buf)
}

// encodeGrantByNeedsExpansionPrefix is the by-sync prefix for all
// grants in this sync that still need expansion processing.
func encodeGrantByNeedsExpansionPrefix(syncIDBytes []byte) []byte {
	buf := make([]byte, 0, 3+len(syncIDBytes))
	buf = append(buf, versionV3, typeIndex, idxGrantByNeedsExpansion)
	return append(buf, syncIDBytes...)
}

// encodeGrantByPrincipalPrefix is the by-value prefix for "all
// grants in this sync for this principal". Trailing sep is
// load-bearing — see keys.go convention.
func encodeGrantByPrincipalPrefix(syncIDBytes []byte, principalRT, principalID string) []byte {
	buf := make([]byte, 0, 32+len(principalRT)+len(principalID))
	buf = append(buf, versionV3, typeIndex, idxGrantByPrincipal)
	buf = append(buf, syncIDBytes...)
	buf = codec.AppendTupleSeparator(buf)
	buf = codec.AppendTupleStrings(buf, principalRT, principalID)
	return codec.AppendTupleSeparator(buf)
}

// encodeGrantByEntitlementResourceIndexKey is the by_entitlement_resource
// secondary index on GrantRecord. Indexes grants by the
// resource side of their entitlement (i.e. the resource the
// entitlement is on — the group/role/app/etc., NOT the principal).
//
//	v3 | typeIndex | idxGrantByEntitlementResource | sync_id_bytes | 0x00 |
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
func encodeGrantByEntitlementResourceIndexKey(syncIDBytes []byte, entRT, entRID, externalID string) []byte {
	buf := make([]byte, 0, 64)
	buf = append(buf, versionV3, typeIndex, idxGrantByEntitlementResource)
	buf = append(buf, syncIDBytes...)
	buf = codec.AppendTupleSeparator(buf)
	return codec.AppendTupleStrings(buf, entRT, entRID, externalID)
}

// encodeGrantByEntitlementResourcePrefix is the by-value prefix for
// "all grants in this sync whose entitlement is on this resource".
// Trailing sep is load-bearing — see keys.go convention.
func encodeGrantByEntitlementResourcePrefix(syncIDBytes []byte, entRT, entRID string) []byte {
	buf := make([]byte, 0, 32+len(entRT)+len(entRID))
	buf = append(buf, versionV3, typeIndex, idxGrantByEntitlementResource)
	buf = append(buf, syncIDBytes...)
	buf = codec.AppendTupleSeparator(buf)
	buf = codec.AppendTupleStrings(buf, entRT, entRID)
	return codec.AppendTupleSeparator(buf)
}

// --- ResourceType ---

// encodeResourceTypeKey returns the primary key for a resource_type:
//
//	v3 | typeResourceType | sync_id_bytes | 0x00 | external_id
//
// Paired with encodeResourceTypePrefix (by-sync prefix).
func encodeResourceTypeKey(syncIDBytes []byte, externalID string) []byte {
	buf := make([]byte, 0, 5+len(syncIDBytes)+len(externalID))
	buf = append(buf, versionV3, typeResourceType)
	buf = append(buf, syncIDBytes...)
	buf = codec.AppendTupleSeparator(buf)
	return codec.AppendTupleStrings(buf, externalID)
}

// encodeResourceTypePrefix is the by-sync prefix for resource_types.
func encodeResourceTypePrefix(syncIDBytes []byte) []byte {
	buf := make([]byte, 0, 2+len(syncIDBytes))
	buf = append(buf, versionV3, typeResourceType)
	return append(buf, syncIDBytes...)
}

// --- Resource ---

// encodeResourceKey returns the primary key for a resource:
//
//	v3 | typeResource | sync_id_bytes | 0x00 | resource_type_id | 0x00 | resource_id
//
// Paired with encodeResourcePrefix (by-sync prefix).
func encodeResourceKey(syncIDBytes []byte, resourceTypeID, resourceID string) []byte {
	buf := make([]byte, 0, 32)
	buf = append(buf, versionV3, typeResource)
	buf = append(buf, syncIDBytes...)
	buf = codec.AppendTupleSeparator(buf)
	return codec.AppendTupleStrings(buf, resourceTypeID, resourceID)
}

// encodeResourcePrefix is the by-sync prefix for resources.
func encodeResourcePrefix(syncIDBytes []byte) []byte {
	buf := make([]byte, 0, 2+len(syncIDBytes))
	buf = append(buf, versionV3, typeResource)
	return append(buf, syncIDBytes...)
}

// encodeResourceByParentIndexKey: index of children-by-parent:
//
//	v3 | typeIndex | idxResourceByParent | sync_id_bytes | 0x00 |
//	    parent_rt | 0x00 | parent_id | 0x00 | child_rt | 0x00 | child_id
//
// Paired with encodeResourceByParentPrefix (by-value prefix, with
// trailing sep).
func encodeResourceByParentIndexKey(syncIDBytes []byte, parentRT, parentID, childRT, childID string) []byte {
	buf := make([]byte, 0, 64)
	buf = append(buf, versionV3, typeIndex, idxResourceByParent)
	buf = append(buf, syncIDBytes...)
	buf = codec.AppendTupleSeparator(buf)
	return codec.AppendTupleStrings(buf, parentRT, parentID, childRT, childID)
}

// encodeResourceByParentPrefix is the by-value prefix for "all
// children of (parent_rt, parent_id) in this sync". Trailing sep
// is load-bearing.
func encodeResourceByParentPrefix(syncIDBytes []byte, parentRT, parentID string) []byte {
	buf := make([]byte, 0, 32+len(parentRT)+len(parentID))
	buf = append(buf, versionV3, typeIndex, idxResourceByParent)
	buf = append(buf, syncIDBytes...)
	buf = codec.AppendTupleSeparator(buf)
	buf = codec.AppendTupleStrings(buf, parentRT, parentID)
	return codec.AppendTupleSeparator(buf)
}

// --- Entitlement ---

// encodeEntitlementKey returns the primary key for an entitlement:
//
//	v3 | typeEntitlement | sync_id_bytes | 0x00 | external_id
//
// Paired with encodeEntitlementPrefix (by-sync prefix).
func encodeEntitlementKey(syncIDBytes []byte, externalID string) []byte {
	buf := make([]byte, 0, 5+len(syncIDBytes)+len(externalID))
	buf = append(buf, versionV3, typeEntitlement)
	buf = append(buf, syncIDBytes...)
	buf = codec.AppendTupleSeparator(buf)
	return codec.AppendTupleStrings(buf, externalID)
}

// encodeEntitlementPrefix is the by-sync prefix for entitlements.
func encodeEntitlementPrefix(syncIDBytes []byte) []byte {
	buf := make([]byte, 0, 2+len(syncIDBytes))
	buf = append(buf, versionV3, typeEntitlement)
	return append(buf, syncIDBytes...)
}

// encodeEntitlementByResourceIndexKey:
//
//	v3 | typeIndex | idxEntitlementByResource | sync_id_bytes | 0x00 |
//	    resource_type_id | 0x00 | resource_id | 0x00 | external_id
//
// Paired with encodeEntitlementByResourcePrefix (by-value prefix,
// with trailing sep).
func encodeEntitlementByResourceIndexKey(syncIDBytes []byte, resourceTypeID, resourceID, externalID string) []byte {
	buf := make([]byte, 0, 64)
	buf = append(buf, versionV3, typeIndex, idxEntitlementByResource)
	buf = append(buf, syncIDBytes...)
	buf = codec.AppendTupleSeparator(buf)
	return codec.AppendTupleStrings(buf, resourceTypeID, resourceID, externalID)
}

// encodeEntitlementByResourcePrefix is the by-value prefix for "all
// entitlements on (resource_type_id, resource_id) in this sync".
// Trailing sep is load-bearing.
func encodeEntitlementByResourcePrefix(syncIDBytes []byte, resourceTypeID, resourceID string) []byte {
	buf := make([]byte, 0, 32+len(resourceTypeID)+len(resourceID))
	buf = append(buf, versionV3, typeIndex, idxEntitlementByResource)
	buf = append(buf, syncIDBytes...)
	buf = codec.AppendTupleSeparator(buf)
	buf = codec.AppendTupleStrings(buf, resourceTypeID, resourceID)
	return codec.AppendTupleSeparator(buf)
}

// --- Asset ---

// encodeAssetKey returns the primary key for an asset:
//
//	v3 | typeAsset | sync_id_bytes | 0x00 | external_id
//
// Paired with encodeAssetPrefix (by-sync prefix).
func encodeAssetKey(syncIDBytes []byte, externalID string) []byte {
	buf := make([]byte, 0, 5+len(syncIDBytes)+len(externalID))
	buf = append(buf, versionV3, typeAsset)
	buf = append(buf, syncIDBytes...)
	buf = codec.AppendTupleSeparator(buf)
	return codec.AppendTupleStrings(buf, externalID)
}

// encodeAssetPrefix is the by-sync prefix for assets.
func encodeAssetPrefix(syncIDBytes []byte) []byte {
	buf := make([]byte, 0, 2+len(syncIDBytes))
	buf = append(buf, versionV3, typeAsset)
	return append(buf, syncIDBytes...)
}

// --- SyncRun ---
//
// SyncRun is the one shape that breaks the standard layout:
//
//	v3 | typeSyncRun | sync_id_bytes        (no separator, no tail)
//
// The sync_id is the entire primary key — there's nothing variable
// to delimit. Two iteration patterns are paired:
//
//   - encodeSyncRunKey:        the full primary key for one sync.
//   - encodeSyncRunFullPrefix: covers ALL sync_runs across every
//     sync_id (used by IterateAllSyncRuns).

func encodeSyncRunKey(syncIDBytes []byte) []byte {
	buf := make([]byte, 0, 2+len(syncIDBytes))
	buf = append(buf, versionV3, typeSyncRun)
	return append(buf, syncIDBytes...)
}

func encodeSyncRunFullPrefix() []byte {
	return []byte{versionV3, typeSyncRun}
}

// --- Exported bound helpers for synccompactor/pebble ---

func ResourceTypeSyncLowerBound(syncIDBytes []byte) []byte {
	return encodeResourceTypePrefix(syncIDBytes)
}

func ResourceTypeSyncUpperBound(syncIDBytes []byte) []byte {
	return upperBoundOf(ResourceTypeSyncLowerBound(syncIDBytes))
}

func ResourceSyncLowerBound(syncIDBytes []byte) []byte {
	return encodeResourcePrefix(syncIDBytes)
}

func ResourceSyncUpperBound(syncIDBytes []byte) []byte {
	return upperBoundOf(ResourceSyncLowerBound(syncIDBytes))
}

func ResourceByParentSyncLowerBound(syncIDBytes []byte) []byte {
	buf := make([]byte, 0, 3+len(syncIDBytes))
	buf = append(buf, versionV3, typeIndex, idxResourceByParent)
	buf = append(buf, syncIDBytes...)
	return buf
}

func ResourceByParentSyncUpperBound(syncIDBytes []byte) []byte {
	return upperBoundOf(ResourceByParentSyncLowerBound(syncIDBytes))
}

func EntitlementSyncLowerBound(syncIDBytes []byte) []byte {
	return encodeEntitlementPrefix(syncIDBytes)
}

func EntitlementSyncUpperBound(syncIDBytes []byte) []byte {
	return upperBoundOf(EntitlementSyncLowerBound(syncIDBytes))
}

func EntitlementByResourceSyncLowerBound(syncIDBytes []byte) []byte {
	buf := make([]byte, 0, 3+len(syncIDBytes))
	buf = append(buf, versionV3, typeIndex, idxEntitlementByResource)
	buf = append(buf, syncIDBytes...)
	return buf
}

func EntitlementByResourceSyncUpperBound(syncIDBytes []byte) []byte {
	return upperBoundOf(EntitlementByResourceSyncLowerBound(syncIDBytes))
}

// GrantSyncLowerBound returns the lowest key in the grant primary
// bucket for a given sync. Together with GrantSyncUpperBound it gives
// the half-open [lo, hi) range covering every grant under that sync.
// Exported for the synccompactor/pebble package; not part of the
// stable public API of the engine.
func GrantSyncLowerBound(syncIDBytes []byte) []byte {
	return encodeGrantPrefix(syncIDBytes)
}

// GrantSyncUpperBound returns the exclusive upper bound for the
// grant primary bucket under a sync.
func GrantSyncUpperBound(syncIDBytes []byte) []byte {
	return upperBoundOf(encodeGrantPrefix(syncIDBytes))
}

// GrantByEntitlementSyncLowerBound returns the lowest key in the
// by_entitlement index bucket for a given sync.
func GrantByEntitlementSyncLowerBound(syncIDBytes []byte) []byte {
	buf := make([]byte, 0, 3+len(syncIDBytes))
	buf = append(buf, versionV3, typeIndex, idxGrantByEntitlement)
	buf = append(buf, syncIDBytes...)
	return buf
}

// GrantByEntitlementSyncUpperBound is the exclusive upper bound.
func GrantByEntitlementSyncUpperBound(syncIDBytes []byte) []byte {
	return upperBoundOf(GrantByEntitlementSyncLowerBound(syncIDBytes))
}

// GrantByPrincipalSyncLowerBound returns the lowest key in the
// by_principal index bucket for a given sync.
func GrantByPrincipalSyncLowerBound(syncIDBytes []byte) []byte {
	buf := make([]byte, 0, 3+len(syncIDBytes))
	buf = append(buf, versionV3, typeIndex, idxGrantByPrincipal)
	buf = append(buf, syncIDBytes...)
	return buf
}

// GrantByPrincipalSyncUpperBound is the exclusive upper bound.
func GrantByPrincipalSyncUpperBound(syncIDBytes []byte) []byte {
	return upperBoundOf(GrantByPrincipalSyncLowerBound(syncIDBytes))
}

// GrantByNeedsExpansionSyncLowerBound returns the lowest key in the
// needs_expansion index bucket for a given sync.
func GrantByNeedsExpansionSyncLowerBound(syncIDBytes []byte) []byte {
	return encodeGrantByNeedsExpansionPrefix(syncIDBytes)
}

// GrantByNeedsExpansionSyncUpperBound is the exclusive upper bound.
func GrantByNeedsExpansionSyncUpperBound(syncIDBytes []byte) []byte {
	return upperBoundOf(GrantByNeedsExpansionSyncLowerBound(syncIDBytes))
}

// GrantByPrincipalResourceTypeSyncLowerBound returns the lowest key
// in the by-principal-resource-type index bucket for a given sync.
func GrantByPrincipalResourceTypeSyncLowerBound(syncIDBytes []byte) []byte {
	buf := make([]byte, 0, 3+len(syncIDBytes))
	buf = append(buf, versionV3, typeIndex, idxGrantByPrincipalResourceType)
	buf = append(buf, syncIDBytes...)
	return buf
}

// GrantByPrincipalResourceTypeSyncUpperBound is the exclusive upper bound.
func GrantByPrincipalResourceTypeSyncUpperBound(syncIDBytes []byte) []byte {
	return upperBoundOf(GrantByPrincipalResourceTypeSyncLowerBound(syncIDBytes))
}

// GrantByEntitlementResourceSyncLowerBound returns the lowest key in
// the by_entitlement_resource index bucket for a given sync.
func GrantByEntitlementResourceSyncLowerBound(syncIDBytes []byte) []byte {
	buf := make([]byte, 0, 3+len(syncIDBytes))
	buf = append(buf, versionV3, typeIndex, idxGrantByEntitlementResource)
	buf = append(buf, syncIDBytes...)
	return buf
}

// GrantByEntitlementResourceSyncUpperBound is the exclusive upper bound.
func GrantByEntitlementResourceSyncUpperBound(syncIDBytes []byte) []byte {
	return upperBoundOf(GrantByEntitlementResourceSyncLowerBound(syncIDBytes))
}

func AssetSyncLowerBound(syncIDBytes []byte) []byte {
	return encodeAssetPrefix(syncIDBytes)
}

func AssetSyncUpperBound(syncIDBytes []byte) []byte {
	return upperBoundOf(AssetSyncLowerBound(syncIDBytes))
}

func SyncRunLowerBound(syncIDBytes []byte) []byte {
	return encodeSyncRunKey(syncIDBytes)
}

func SyncRunUpperBound(syncIDBytes []byte) []byte {
	return upperBoundOf(SyncRunLowerBound(syncIDBytes))
}

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
