package anonymize

import (
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

// AnonymizeGrant anonymizes a Grant in place.
func (a *Anonymizer) AnonymizeGrant(g *v2.Grant) error {
	if g == nil {
		return nil
	}

	// Anonymize ID
	if g.GetId() != "" {
		g.SetId(a.hasher.AnonymizeExternalID(g.GetId()))
	}

	// Anonymize embedded entitlement
	if g.HasEntitlement() {
		if err := a.AnonymizeEntitlement(g.GetEntitlement()); err != nil {
			return err
		}
	}

	// Anonymize principal (which is a Resource)
	if g.HasPrincipal() {
		if err := a.AnonymizeResource(g.GetPrincipal()); err != nil {
			return err
		}
	}

	// Anonymize grant sources
	if g.HasSources() {
		a.anonymizeGrantSources(g.GetSources())
	}

	// Anonymize grant annotations (GrantExpandable, GrantMetadata, etc.)
	if err := a.anonymizeGrantAnnotations(g); err != nil {
		return err
	}

	return nil
}

// anonymizeGrantSources anonymizes GrantSources in place.
func (a *Anonymizer) anonymizeGrantSources(gs *v2.GrantSources) {
	if gs == nil {
		return
	}

	// Grant sources contain a map of source identifiers to GrantSource objects
	// The keys might be resource IDs or other identifiers that should be anonymized
	sources := gs.GetSources()
	if sources == nil {
		return
	}

	// Create a new map with anonymized keys
	newSources := make(map[string]*v2.GrantSources_GrantSource)
	for key, source := range sources {
		// Anonymize the key (which is typically a resource identifier)
		newKey := a.hasher.AnonymizeResourceID(key)
		newSources[newKey] = source
	}
	gs.SetSources(newSources)
}
