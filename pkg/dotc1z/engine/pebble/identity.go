package pebble

import (
	"fmt"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	entitlement "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	grant "github.com/conductorone/baton-sdk/pkg/types/grant"
)

type entitlementIdentity struct {
	resourceTypeID string
	resourceID     string
	kind           string
	name           string
}

type grantIdentity struct {
	entitlement     entitlementIdentity
	principalTypeID string
	principalID     string
}

func entitlementIdentityFromParts(resourceTypeID, resourceID, entitlementID string) entitlementIdentity {
	parts := entitlement.DeriveEntitlementIDParts(resourceTypeID, resourceID, entitlementID)
	return entitlementIdentity{
		resourceTypeID: parts.ResourceTypeID,
		resourceID:     parts.ResourceID,
		kind:           parts.Kind,
		name:           parts.Name,
	}
}

func entitlementIdentityFromLegacyParts(resourceTypeID, resourceID, entitlementID string) entitlementIdentity {
	parts := entitlement.DeriveLegacyEntitlementIDParts(resourceTypeID, resourceID, entitlementID)
	return entitlementIdentity{
		resourceTypeID: parts.ResourceTypeID,
		resourceID:     parts.ResourceID,
		kind:           parts.Kind,
		name:           parts.Name,
	}
}

func entitlementIdentityFromID(id string) (entitlementIdentity, error) {
	parts, err := entitlement.DecodeEntitlementID(id)
	if err != nil {
		return entitlementIdentity{}, err
	}
	return entitlementIdentity{
		resourceTypeID: parts.ResourceTypeID,
		resourceID:     parts.ResourceID,
		kind:           parts.Kind,
		name:           parts.Name,
	}, nil
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

func (id entitlementIdentity) toPublicParts() entitlement.EntitlementIDParts {
	return entitlement.EntitlementIDParts{
		ResourceTypeID: id.resourceTypeID,
		ResourceID:     id.resourceID,
		Kind:           id.kind,
		Name:           id.name,
	}
}

func (id entitlementIdentity) toPublicID() string {
	return id.toPublicParts().Encode()
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

func grantIdentityFromID(id string) (grantIdentity, error) {
	parts, err := grant.DecodeGrantID(id)
	if err != nil {
		return grantIdentity{}, err
	}
	return grantIdentity{
		entitlement: entitlementIdentity{
			resourceTypeID: parts.Entitlement.ResourceTypeID,
			resourceID:     parts.Entitlement.ResourceID,
			kind:           parts.Entitlement.Kind,
			name:           parts.Entitlement.Name,
		},
		principalTypeID: parts.PrincipalTypeID,
		principalID:     parts.PrincipalID,
	}, nil
}
