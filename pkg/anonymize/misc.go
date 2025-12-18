package anonymize

import (
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

// AnonymizeResourceType anonymizes a ResourceType in place.
func (a *Anonymizer) AnonymizeResourceType(rt *v2.ResourceType) error {
	if rt == nil {
		return nil
	}

	// Anonymize ID - uses same hashing as ResourceId.resource_type for consistency
	if rt.GetId() != "" {
		rt.SetId(a.hasher.AnonymizeResourceType(rt.GetId()))
	}

	// Anonymize display name
	if rt.GetDisplayName() != "" {
		rt.SetDisplayName(a.hasher.AnonymizeDisplayName(rt.GetDisplayName()))
	}

	// Anonymize description
	if rt.GetDescription() != "" {
		rt.SetDescription("[ANONYMIZED]")
	}

	// Anonymize annotations (ChildResourceType, ExternalLink, etc.)
	if err := a.anonymizeResourceTypeAnnotations(rt); err != nil {
		return err
	}

	// Note: Traits are enum values, not PII
	// Note: SourcedExternally is a boolean flag, not PII

	return nil
}

// ShouldDeleteAssets returns true because all assets are deleted during anonymization.
// Assets (icons, logos, etc.) can contain identifying information.
func (a *Anonymizer) ShouldDeleteAssets() bool {
	return true
}

// ShouldClearSessionStore returns whether the session store should be cleared.
func (a *Anonymizer) ShouldClearSessionStore() bool {
	// Session store always contains potentially sensitive cached data
	// and should be cleared during anonymization.
	return true
}
