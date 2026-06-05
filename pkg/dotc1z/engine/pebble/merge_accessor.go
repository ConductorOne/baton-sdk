package pebble

import (
	"github.com/conductorone/baton-sdk/pkg/connectorstore"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
)

// AsEngine recovers the underlying *Engine from a connectorstore.Writer
// produced by dotc1z.NewStore for the Pebble engine. NewStore returns a
// *registeredStore wrapper (which embeds *Adapter); a bare *Adapter is
// also accepted for callers that construct one directly. Returns
// (nil, false) for any non-Pebble store, so a caller can branch on the
// engine without importing internal types.
func AsEngine(w connectorstore.Writer) (*Engine, bool) {
	switch s := w.(type) {
	case *registeredStore:
		if s == nil || s.engine == nil {
			return nil, false
		}
		return s.engine, true
	case *Adapter:
		if s == nil || s.engine == nil {
			return nil, false
		}
		return s.engine, true
	default:
		return nil, false
	}
}

// ResourceTypeRecordKey returns the primary Pebble key for a resource_type
// record under syncIDBytes. It is exported for synccompactor's SST materializer.
func ResourceTypeRecordKey(syncIDBytes []byte, externalID string) []byte {
	return encodeResourceTypeKey(syncIDBytes, externalID)
}

// ResourceRecordKey returns the primary Pebble key for a resource record under
// syncIDBytes. It is exported for synccompactor's SST materializer.
func ResourceRecordKey(syncIDBytes []byte, resourceTypeID string, resourceID string) []byte {
	return encodeResourceKey(syncIDBytes, resourceTypeID, resourceID)
}

// EntitlementRecordKey returns the primary Pebble key for an entitlement record
// under syncIDBytes. It is exported for synccompactor's SST materializer.
func EntitlementRecordKey(syncIDBytes []byte, externalID string) []byte {
	return encodeEntitlementKey(syncIDBytes, externalID)
}

// GrantRecordKey returns the primary Pebble key for a grant record under
// syncIDBytes. It is exported for synccompactor's SST materializer.
func GrantRecordKey(syncIDBytes []byte, externalID string) []byte {
	return encodeGrantKey(syncIDBytes, externalID)
}

// ResourceIndexKeys returns the secondary index keys for a resource record.
// It is exported for synccompactor's SST materializer.
func ResourceIndexKeys(syncIDBytes []byte, r *v3.ResourceRecord) [][]byte {
	parent := r.GetParent()
	if parent == nil || parent.GetResourceId() == "" {
		return nil
	}
	return [][]byte{encodeResourceByParentIndexKey(
		syncIDBytes,
		parent.GetResourceTypeId(),
		parent.GetResourceId(),
		r.GetResourceTypeId(),
		r.GetResourceId(),
	)}
}

// EntitlementIndexKeys returns the secondary index keys for an entitlement
// record. It is exported for synccompactor's SST materializer.
func EntitlementIndexKeys(syncIDBytes []byte, r *v3.EntitlementRecord) [][]byte {
	res := r.GetResource()
	if res == nil || res.GetResourceId() == "" {
		return nil
	}
	return [][]byte{encodeEntitlementByResourceIndexKey(
		syncIDBytes,
		res.GetResourceTypeId(),
		res.GetResourceId(),
		r.GetExternalId(),
	)}
}

// GrantIndexKeys returns the secondary index keys for a grant record. It is
// exported for synccompactor's SST materializer.
func GrantIndexKeys(syncIDBytes []byte, r *v3.GrantRecord) [][]byte {
	return grantIndexKeys(syncIDBytes, r)
}
