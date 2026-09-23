package sourcecache

import (
	"errors"
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

// ErrIncompleteTombstone is returned for a reference missing a component.
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

func TombstonesFromProto(kind RowKind, p *v2.SourceCacheTombstones) (Tombstones, error) {
	var t Tombstones
	for _, r := range p.GetResources() {
		t.Resources = append(t.Resources, resourceRefFromProto(r))
	}
	for _, e := range p.GetEntitlements() {
		t.Entitlements = append(t.Entitlements, entitlementRefFromProto(e))
	}
	for _, g := range p.GetGrants() {
		t.Grants = append(t.Grants, GrantRef{
			Entitlement: entitlementRefFromProto(g.GetEntitlement()),
			Principal:   resourceRefFromProto(g.GetPrincipal()),
		})
	}
	for _, r := range p.GetPrincipals() {
		t.Principals = append(t.Principals, resourceRefFromProto(r))
	}
	if err := t.Validate(kind); err != nil {
		return Tombstones{}, err
	}
	return t, nil
}

// Validate rejects fields that do not belong to kind's pages and any
// reference missing a component. Every delete path calls it, so an
// incomplete ref never reaches the engine as a no-op.
func (t Tombstones) Validate(kind RowKind) error {
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
	for i, r := range t.Resources {
		if err := r.validate(); err != nil {
			return fmt.Errorf("resources[%d]: %w", i, err)
		}
	}
	for i, e := range t.Entitlements {
		if err := e.validate(); err != nil {
			return fmt.Errorf("entitlements[%d]: %w", i, err)
		}
	}
	for i, g := range t.Grants {
		if err := g.Entitlement.validate(); err != nil {
			return fmt.Errorf("grants[%d].entitlement: %w", i, err)
		}
		if err := g.Principal.validate(); err != nil {
			return fmt.Errorf("grants[%d].principal: %w", i, err)
		}
	}
	for i, p := range t.Principals {
		if err := p.validate(); err != nil {
			return fmt.Errorf("principals[%d]: %w", i, err)
		}
	}
	return nil
}

func (r ResourceRef) validate() error {
	if r.ResourceTypeID == "" || r.ResourceID == "" {
		return fmt.Errorf("%w: resource_type=%q resource=%q", ErrIncompleteTombstone, r.ResourceTypeID, r.ResourceID)
	}
	return nil
}

func (e EntitlementRef) validate() error {
	if err := e.Resource.validate(); err != nil {
		return err
	}
	if e.EntitlementID == "" {
		return fmt.Errorf("%w: empty entitlement_id", ErrIncompleteTombstone)
	}
	return nil
}

func resourceRefFromProto(r *v2.ResourceId) ResourceRef {
	return ResourceRef{ResourceTypeID: r.GetResourceType(), ResourceID: r.GetResource()}
}

func entitlementRefFromProto(e *v2.SourceCacheEntitlementRef) EntitlementRef {
	return EntitlementRef{Resource: resourceRefFromProto(e.GetResource()), EntitlementID: e.GetEntitlementId()}
}
