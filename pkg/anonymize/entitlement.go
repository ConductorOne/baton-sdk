package anonymize

import (
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

// AnonymizeEntitlement anonymizes an Entitlement in place.
func (a *Anonymizer) AnonymizeEntitlement(e *v2.Entitlement) error {
	if e == nil {
		return nil
	}

	// Anonymize display name
	if e.GetDisplayName() != "" {
		e.SetDisplayName(a.hasher.AnonymizeDisplayName(e.GetDisplayName()))
	}

	// Anonymize description
	if e.GetDescription() != "" {
		e.SetDescription("[ANONYMIZED]")
	}

	// Anonymize slug
	if e.GetSlug() != "" {
		e.SetSlug(a.hasher.HashN(e.GetSlug(), 12))
	}

	// Anonymize ID
	if e.GetId() != "" {
		e.SetId(a.hasher.AnonymizeExternalID(e.GetId()))
	}

	// Anonymize embedded resource
	if e.HasResource() {
		if err := a.AnonymizeResource(e.GetResource()); err != nil {
			return err
		}
	}

	// Anonymize grantable_to ResourceTypes
	for _, rt := range e.GetGrantableTo() {
		if err := a.AnonymizeResourceType(rt); err != nil {
			return err
		}
	}

	// Anonymize entitlement annotations (EntitlementImmutable, etc.)
	if err := a.anonymizeEntitlementAnnotations(e); err != nil {
		return err
	}

	// Note: Purpose is an enum value, not PII

	return nil
}
