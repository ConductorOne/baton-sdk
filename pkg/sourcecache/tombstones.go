package sourcecache

import (
	"errors"
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

// ErrIncompleteTombstone is returned for a reference missing a component.
// The store never guesses a delete from a partial identity.
var ErrIncompleteTombstone = errors.New("source cache tombstone: incomplete reference")

type ResourceRef struct {
	ResourceTypeID string
	ResourceID     string
}

type EntitlementRef struct {
	Resource      ResourceRef
	EntitlementID string
}

type GrantRef struct {
	Entitlement EntitlementRef
	Principal   ResourceRef
}

// Tombstones are the rows a page deletes from the current sync, by
// structured identity. Principals delete every grant in the scope whose
// principal matches.
type Tombstones struct {
	Resources    []ResourceRef
	Entitlements []EntitlementRef
	Grants       []GrantRef
	Principals   []ResourceRef
}

func (t Tombstones) Empty() bool {
	return len(t.Resources) == 0 && len(t.Entitlements) == 0 && len(t.Grants) == 0 && len(t.Principals) == 0
}

// TombstonesFromProto validates every reference and that only the fields
// for kind are set.
func TombstonesFromProto(kind RowKind, p *v2.SourceCacheTombstones) (Tombstones, error) {
	var t Tombstones
	if p == nil {
		return t, nil
	}
	for i, r := range p.GetResources() {
		ref, err := resourceRefFromProto(r)
		if err != nil {
			return Tombstones{}, fmt.Errorf("resources[%d]: %w", i, err)
		}
		t.Resources = append(t.Resources, ref)
	}
	for i, e := range p.GetEntitlements() {
		ref, err := entitlementRefFromProto(e)
		if err != nil {
			return Tombstones{}, fmt.Errorf("entitlements[%d]: %w", i, err)
		}
		t.Entitlements = append(t.Entitlements, ref)
	}
	for i, g := range p.GetGrants() {
		ent, err := entitlementRefFromProto(g.GetEntitlement())
		if err != nil {
			return Tombstones{}, fmt.Errorf("grants[%d].entitlement: %w", i, err)
		}
		principal, err := resourceRefFromProto(g.GetPrincipal())
		if err != nil {
			return Tombstones{}, fmt.Errorf("grants[%d].principal: %w", i, err)
		}
		t.Grants = append(t.Grants, GrantRef{Entitlement: ent, Principal: principal})
	}
	for i, r := range p.GetPrincipals() {
		ref, err := resourceRefFromProto(r)
		if err != nil {
			return Tombstones{}, fmt.Errorf("principals[%d]: %w", i, err)
		}
		t.Principals = append(t.Principals, ref)
	}
	if err := t.ValidateKind(kind); err != nil {
		return Tombstones{}, err
	}
	return t, nil
}

// ValidateKind rejects fields that do not belong to kind's pages.
func (t Tombstones) ValidateKind(kind RowKind) error {
	if err := ValidateRowKind(kind); err != nil {
		return err
	}
	switch kind {
	case RowKindResources:
		if len(t.Entitlements) > 0 || len(t.Grants) > 0 || len(t.Principals) > 0 {
			return fmt.Errorf("source cache tombstone: only resources may be deleted from a %s page", kind)
		}
	case RowKindEntitlements:
		if len(t.Resources) > 0 || len(t.Grants) > 0 || len(t.Principals) > 0 {
			return fmt.Errorf("source cache tombstone: only entitlements may be deleted from a %s page", kind)
		}
	case RowKindGrants:
		if len(t.Resources) > 0 || len(t.Entitlements) > 0 {
			return fmt.Errorf("source cache tombstone: only grants and principals may be deleted from a %s page", kind)
		}
	}
	return nil
}

func resourceRefFromProto(r *v2.ResourceId) (ResourceRef, error) {
	if r.GetResourceType() == "" || r.GetResource() == "" {
		return ResourceRef{}, fmt.Errorf("%w: resource_type=%q resource=%q", ErrIncompleteTombstone, r.GetResourceType(), r.GetResource())
	}
	return ResourceRef{ResourceTypeID: r.GetResourceType(), ResourceID: r.GetResource()}, nil
}

func entitlementRefFromProto(e *v2.SourceCacheEntitlementRef) (EntitlementRef, error) {
	res, err := resourceRefFromProto(e.GetResource())
	if err != nil {
		return EntitlementRef{}, err
	}
	if e.GetEntitlementId() == "" {
		return EntitlementRef{}, fmt.Errorf("%w: empty entitlement_id", ErrIncompleteTombstone)
	}
	return EntitlementRef{Resource: res, EntitlementID: e.GetEntitlementId()}, nil
}
