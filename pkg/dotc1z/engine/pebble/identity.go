package pebble

import (
	"fmt"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
)

// Structural identity.
//
// External ids are an external-consumer contract: baton emits them
// byte-identical to what connectors produced and never interprets their
// grammar. Internally, identity is derived ONLY from the structured fields
// records already carry (resource refs, entitlement refs, principal refs) —
// the lossy ":"-joined id string is never parsed to decide identity.
//
// The one place identity touches the id bytes is pure compression, not
// interpretation: when an entitlement's external id literally begins with
// the exact bytes `resource_type_id + ":" + resource_id + ":"` (the shape
// SDK-generated ids always have), the redundant prefix is stripped from the
// stored key tail and a one-byte flag records that it was. The mapping
// (external_id) ↔ (stripped, tail) is bijective given (rt, rid): stripped
// identities reconstruct as `rt:rid:tail`, opaque identities are exactly
// the ids that do NOT start with that prefix, so the two ranges cannot
// collide and reconstruction is byte-exact.

// entitlementIdentity is the structural identity of an entitlement:
// (resource_type_id, resource_id, flag, tail). See the package comment
// above for the flag/tail compression contract.
type entitlementIdentity struct {
	resourceTypeID string
	resourceID     string
	// stripped reports that the external id begins with the exact bytes
	// resourceTypeID + ":" + resourceID + ":" and tail holds the remainder;
	// otherwise tail holds the entire raw external id.
	stripped bool
	tail     string
}

// Key-component encodings of the stripped flag. Single printable bytes so
// the tuple codec never needs to escape them.
const (
	idFlagStripped = "1"
	idFlagOpaque   = "0"
)

func (id entitlementIdentity) flagComponent() string {
	if id.stripped {
		return idFlagStripped
	}
	return idFlagOpaque
}

// externalID reconstructs the exact raw external id this identity was
// derived from.
func (id entitlementIdentity) externalID() string {
	if !id.stripped {
		return id.tail
	}
	return id.resourceTypeID + ":" + id.resourceID + ":" + id.tail
}

type grantIdentity struct {
	entitlement     entitlementIdentity
	principalTypeID string
	principalID     string
}

// externalID reconstructs the legacy public grant id shape
// (entitlement_id + ":" + principal_rt + ":" + principal_id) for this
// identity. This matches what the SDK's NewGrantID emits; connector-custom
// grant ids are stored on the record and take precedence when present.
func (id grantIdentity) externalID() string {
	return id.entitlement.externalID() + ":" + id.principalTypeID + ":" + id.principalID
}

// entitlementIdentityFromParts derives an entitlement identity from the
// structured resource components and the raw external id. Pure byte-prefix
// check — no grammar, no escaping, byte-exact round trip via externalID().
func entitlementIdentityFromParts(resourceTypeID, resourceID, entitlementID string) entitlementIdentity {
	prefixLen := len(resourceTypeID) + len(resourceID) + 2
	if len(entitlementID) >= prefixLen &&
		entitlementID[len(resourceTypeID)] == ':' &&
		entitlementID[prefixLen-1] == ':' &&
		entitlementID[:len(resourceTypeID)] == resourceTypeID &&
		entitlementID[len(resourceTypeID)+1:prefixLen-1] == resourceID {
		return entitlementIdentity{
			resourceTypeID: resourceTypeID,
			resourceID:     resourceID,
			stripped:       true,
			tail:           entitlementID[prefixLen:],
		}
	}
	return entitlementIdentity{
		resourceTypeID: resourceTypeID,
		resourceID:     resourceID,
		tail:           entitlementID,
	}
}

func entitlementIdentityFromRecord(r *v3.EntitlementRecord) (entitlementIdentity, error) {
	if r == nil {
		return entitlementIdentity{}, fmt.Errorf("entitlement identity: nil record")
	}
	res := r.GetResource()
	if res == nil || res.GetResourceTypeId() == "" || res.GetResourceId() == "" {
		return entitlementIdentity{}, fmt.Errorf("entitlement identity: missing resource")
	}
	return entitlementIdentityFromParts(res.GetResourceTypeId(), res.GetResourceId(), r.GetExternalId()), nil
}

func grantIdentityFromRecord(r *v3.GrantRecord) (grantIdentity, error) {
	if r == nil {
		return grantIdentity{}, fmt.Errorf("grant identity: nil record")
	}
	ent := r.GetEntitlement()
	if ent == nil || ent.GetResourceTypeId() == "" || ent.GetResourceId() == "" || ent.GetEntitlementId() == "" {
		return grantIdentity{}, fmt.Errorf("grant identity: missing entitlement")
	}
	princ := r.GetPrincipal()
	if princ == nil || princ.GetResourceTypeId() == "" || princ.GetResourceId() == "" {
		return grantIdentity{}, fmt.Errorf("grant identity: missing principal")
	}
	return grantIdentity{
		entitlement:     entitlementIdentityFromParts(ent.GetResourceTypeId(), ent.GetResourceId(), ent.GetEntitlementId()),
		principalTypeID: princ.GetResourceTypeId(),
		principalID:     princ.GetResourceId(),
	}, nil
}

// publicGrantRecordID returns the id V3GrantToV2 emits for a record: the
// stored external id when the connector provided one, otherwise the
// reconstructed legacy concat (what the SDK would have emitted).
func publicGrantRecordID(r *v3.GrantRecord) string {
	if ext := r.GetExternalId(); ext != "" {
		return ext
	}
	id, err := grantIdentityFromRecord(r)
	if err != nil {
		return ""
	}
	return id.externalID()
}
