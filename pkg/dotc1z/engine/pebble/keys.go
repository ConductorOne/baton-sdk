//go:build batonsdkv2

package pebble

import (
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/codec"
)

// Type-discriminator bytes for the v3 keyspace. Central table; the
// RFC v4 Appendix C.1 has the full list. Each top-level keyspace
// occupies one byte. Reserved bytes (0xCC, 0xEA-0xEC) are noted in
// the RFC for the deferred-expansion future work (Appendix H); not
// emitted by Stack 3.
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

// upperBoundOf returns the smallest key strictly greater than every
// key with the given prefix. Used as the UpperBound in pebble.IterOptions
// for range scans. Increments the last byte; if the prefix is all
// 0xff, appends a 0x00 to mark the boundary.
func upperBoundOf(prefix []byte) []byte {
	end := make([]byte, len(prefix))
	copy(end, prefix)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	// All 0xff — append 0x00 to end the range.
	return append(end, 0x00)
}
