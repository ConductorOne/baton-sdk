package pebble

import (
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/codec"
)

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
	idxResourceByParent      byte = 0x01
	idxEntitlementByResource byte = 0x02
	idxGrantByEntitlement    byte = 0x03
	idxGrantByPrincipal      byte = 0x04
	idxGrantByNeedsExpansion byte = 0x05
)

// --- Grant ---

// encodeGrantKey returns the primary key for a grant.
//
//	v3 | typeGrant | sync_id_bytes | external_id
//
// sync_id is the 20-byte canonical KSUID binary form (see codec.EncodeSyncID).
func encodeGrantKey(syncIDBytes []byte, externalID string) []byte {
	buf := make([]byte, 0, 5+len(syncIDBytes)+len(externalID))
	buf = append(buf, versionV3, typeGrant)
	buf = append(buf, syncIDBytes...)
	buf = codec.AppendTupleSeparator(buf)
	buf = codec.AppendTupleString(buf, externalID)
	return buf
}

// encodeGrantPrefix returns the prefix key for iterating all grants
// in a sync. Used for full-sync range scans.
func encodeGrantPrefix(syncIDBytes []byte) []byte {
	buf := make([]byte, 0, 2+len(syncIDBytes))
	buf = append(buf, versionV3, typeGrant)
	buf = append(buf, syncIDBytes...)
	return buf
}

// encodeGrantByEntitlementIndexKey returns the index key for the
// by_entitlement secondary index on GrantRecord:
//
//	v3 | typeIndex | idxGrantByEntitlement | sync_id_bytes |
//	    entitlement_id | principal_resource_type | principal_resource_id |
//	    external_id  (tail for uniqueness)
func encodeGrantByEntitlementIndexKey(syncIDBytes []byte, entitlementID, principalRT, principalID, externalID string) []byte {
	buf := make([]byte, 0, 64)
	buf = append(buf, versionV3, typeIndex, idxGrantByEntitlement)
	buf = append(buf, syncIDBytes...)
	buf = codec.AppendTupleSeparator(buf)
	buf = codec.AppendTupleString(buf, entitlementID)
	buf = codec.AppendTupleSeparator(buf)
	buf = codec.AppendTupleString(buf, principalRT)
	buf = codec.AppendTupleSeparator(buf)
	buf = codec.AppendTupleString(buf, principalID)
	buf = codec.AppendTupleSeparator(buf)
	buf = codec.AppendTupleString(buf, externalID)
	return buf
}

// encodeGrantByPrincipalIndexKey:
//
//	v3 | typeIndex | idxGrantByPrincipal | sync_id_bytes |
//	    principal_resource_type | principal_resource_id | external_id
func encodeGrantByPrincipalIndexKey(syncIDBytes []byte, principalRT, principalID, externalID string) []byte {
	buf := make([]byte, 0, 64)
	buf = append(buf, versionV3, typeIndex, idxGrantByPrincipal)
	buf = append(buf, syncIDBytes...)
	buf = codec.AppendTupleSeparator(buf)
	buf = codec.AppendTupleString(buf, principalRT)
	buf = codec.AppendTupleSeparator(buf)
	buf = codec.AppendTupleString(buf, principalID)
	buf = codec.AppendTupleSeparator(buf)
	buf = codec.AppendTupleString(buf, externalID)
	return buf
}

// encodeGrantByEntitlementPrefix returns the prefix for "all grants
// with this entitlement_id" range scan.
func encodeGrantByEntitlementPrefix(syncIDBytes []byte, entitlementID string) []byte {
	buf := make([]byte, 0, 32+len(entitlementID))
	buf = append(buf, versionV3, typeIndex, idxGrantByEntitlement)
	buf = append(buf, syncIDBytes...)
	buf = codec.AppendTupleSeparator(buf)
	buf = codec.AppendTupleString(buf, entitlementID)
	buf = codec.AppendTupleSeparator(buf)
	return buf
}

// encodeGrantByNeedsExpansionIndexKey: index of grants whose
// NeedsExpansion flag is true. Pebble equivalent of the SQLite
// partial index `WHERE needs_expansion = 1`. The grant is added to
// this keyspace on Put when NeedsExpansion=true and removed when
// NeedsExpansion=false (or when the grant is deleted).
//
//	v3 | typeIndex | idxGrantByNeedsExpansion | sync_id_bytes | external_id
func encodeGrantByNeedsExpansionIndexKey(syncIDBytes []byte, externalID string) []byte {
	buf := make([]byte, 0, 5+len(syncIDBytes)+len(externalID))
	buf = append(buf, versionV3, typeIndex, idxGrantByNeedsExpansion)
	buf = append(buf, syncIDBytes...)
	buf = codec.AppendTupleSeparator(buf)
	buf = codec.AppendTupleString(buf, externalID)
	return buf
}

// encodeGrantByNeedsExpansionPrefix returns the prefix for "all
// grants in this sync that still need expansion processing."
func encodeGrantByNeedsExpansionPrefix(syncIDBytes []byte) []byte {
	buf := make([]byte, 0, 3+len(syncIDBytes))
	buf = append(buf, versionV3, typeIndex, idxGrantByNeedsExpansion)
	buf = append(buf, syncIDBytes...)
	return buf
}

// encodeGrantByPrincipalPrefix returns the prefix for "all grants for
// this principal" range scan.
func encodeGrantByPrincipalPrefix(syncIDBytes []byte, principalRT, principalID string) []byte {
	buf := make([]byte, 0, 32+len(principalRT)+len(principalID))
	buf = append(buf, versionV3, typeIndex, idxGrantByPrincipal)
	buf = append(buf, syncIDBytes...)
	buf = codec.AppendTupleSeparator(buf)
	buf = codec.AppendTupleString(buf, principalRT)
	buf = codec.AppendTupleSeparator(buf)
	buf = codec.AppendTupleString(buf, principalID)
	buf = codec.AppendTupleSeparator(buf)
	return buf
}

// --- ResourceType ---

// encodeResourceTypeKey returns the primary key for a resource_type:
//
//	v3 | typeResourceType | sync_id_bytes | external_id
func encodeResourceTypeKey(syncIDBytes []byte, externalID string) []byte {
	buf := make([]byte, 0, 5+len(syncIDBytes)+len(externalID))
	buf = append(buf, versionV3, typeResourceType)
	buf = append(buf, syncIDBytes...)
	buf = codec.AppendTupleSeparator(buf)
	buf = codec.AppendTupleString(buf, externalID)
	return buf
}

func encodeResourceTypePrefix(syncIDBytes []byte) []byte {
	buf := make([]byte, 0, 2+len(syncIDBytes))
	buf = append(buf, versionV3, typeResourceType)
	buf = append(buf, syncIDBytes...)
	return buf
}

// --- Resource ---

// encodeResourceKey returns the primary key for a resource:
//
//	v3 | typeResource | sync_id_bytes | resource_type_id | resource_id
func encodeResourceKey(syncIDBytes []byte, resourceTypeID, resourceID string) []byte {
	buf := make([]byte, 0, 32)
	buf = append(buf, versionV3, typeResource)
	buf = append(buf, syncIDBytes...)
	buf = codec.AppendTupleSeparator(buf)
	buf = codec.AppendTupleString(buf, resourceTypeID)
	buf = codec.AppendTupleSeparator(buf)
	buf = codec.AppendTupleString(buf, resourceID)
	return buf
}

func encodeResourcePrefix(syncIDBytes []byte) []byte {
	buf := make([]byte, 0, 2+len(syncIDBytes))
	buf = append(buf, versionV3, typeResource)
	buf = append(buf, syncIDBytes...)
	return buf
}

// encodeResourceByParentIndexKey: index of children-by-parent.
func encodeResourceByParentIndexKey(syncIDBytes []byte, parentRT, parentID, childRT, childID string) []byte {
	buf := make([]byte, 0, 64)
	buf = append(buf, versionV3, typeIndex, idxResourceByParent)
	buf = append(buf, syncIDBytes...)
	buf = codec.AppendTupleSeparator(buf)
	buf = codec.AppendTupleString(buf, parentRT)
	buf = codec.AppendTupleSeparator(buf)
	buf = codec.AppendTupleString(buf, parentID)
	buf = codec.AppendTupleSeparator(buf)
	buf = codec.AppendTupleString(buf, childRT)
	buf = codec.AppendTupleSeparator(buf)
	buf = codec.AppendTupleString(buf, childID)
	return buf
}

func encodeResourceByParentPrefix(syncIDBytes []byte, parentRT, parentID string) []byte {
	buf := make([]byte, 0, 32+len(parentRT)+len(parentID))
	buf = append(buf, versionV3, typeIndex, idxResourceByParent)
	buf = append(buf, syncIDBytes...)
	buf = codec.AppendTupleSeparator(buf)
	buf = codec.AppendTupleString(buf, parentRT)
	buf = codec.AppendTupleSeparator(buf)
	buf = codec.AppendTupleString(buf, parentID)
	buf = codec.AppendTupleSeparator(buf)
	return buf
}

// --- Entitlement ---

func encodeEntitlementKey(syncIDBytes []byte, externalID string) []byte {
	buf := make([]byte, 0, 5+len(syncIDBytes)+len(externalID))
	buf = append(buf, versionV3, typeEntitlement)
	buf = append(buf, syncIDBytes...)
	buf = codec.AppendTupleSeparator(buf)
	buf = codec.AppendTupleString(buf, externalID)
	return buf
}

func encodeEntitlementPrefix(syncIDBytes []byte) []byte {
	buf := make([]byte, 0, 2+len(syncIDBytes))
	buf = append(buf, versionV3, typeEntitlement)
	buf = append(buf, syncIDBytes...)
	return buf
}

func encodeEntitlementByResourceIndexKey(syncIDBytes []byte, resourceTypeID, resourceID, externalID string) []byte {
	buf := make([]byte, 0, 64)
	buf = append(buf, versionV3, typeIndex, idxEntitlementByResource)
	buf = append(buf, syncIDBytes...)
	buf = codec.AppendTupleSeparator(buf)
	buf = codec.AppendTupleString(buf, resourceTypeID)
	buf = codec.AppendTupleSeparator(buf)
	buf = codec.AppendTupleString(buf, resourceID)
	buf = codec.AppendTupleSeparator(buf)
	buf = codec.AppendTupleString(buf, externalID)
	return buf
}

func encodeEntitlementByResourcePrefix(syncIDBytes []byte, resourceTypeID, resourceID string) []byte {
	buf := make([]byte, 0, 32+len(resourceTypeID)+len(resourceID))
	buf = append(buf, versionV3, typeIndex, idxEntitlementByResource)
	buf = append(buf, syncIDBytes...)
	buf = codec.AppendTupleSeparator(buf)
	buf = codec.AppendTupleString(buf, resourceTypeID)
	buf = codec.AppendTupleSeparator(buf)
	buf = codec.AppendTupleString(buf, resourceID)
	buf = codec.AppendTupleSeparator(buf)
	return buf
}

// --- Asset ---

func encodeAssetKey(syncIDBytes []byte, externalID string) []byte {
	buf := make([]byte, 0, 5+len(syncIDBytes)+len(externalID))
	buf = append(buf, versionV3, typeAsset)
	buf = append(buf, syncIDBytes...)
	buf = codec.AppendTupleSeparator(buf)
	buf = codec.AppendTupleString(buf, externalID)
	return buf
}

func encodeAssetPrefix(syncIDBytes []byte) []byte {
	buf := make([]byte, 0, 2+len(syncIDBytes))
	buf = append(buf, versionV3, typeAsset)
	buf = append(buf, syncIDBytes...)
	return buf
}

// --- SyncRun ---

func encodeSyncRunKey(syncIDBytes []byte) []byte {
	buf := make([]byte, 0, 2+len(syncIDBytes))
	buf = append(buf, versionV3, typeSyncRun)
	buf = append(buf, syncIDBytes...)
	return buf
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
