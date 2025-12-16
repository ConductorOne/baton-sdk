package anonymize

import (
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// anonymizeResourceTypeAnnotations processes all annotations on a ResourceType,
// anonymizing known types and dropping unknown types.
func (a *Anonymizer) anonymizeResourceTypeAnnotations(rt *v2.ResourceType) error {
	if rt == nil {
		return nil
	}

	var result []*anypb.Any
	for _, ann := range rt.GetAnnotations() {
		if processed, err := a.processResourceTypeAnnotation(ann); err != nil {
			return err
		} else if processed != nil {
			result = append(result, processed)
		}
		// Unknown types are dropped
	}

	rt.SetAnnotations(result)
	return nil
}

// processResourceTypeAnnotation processes a single annotation from a ResourceType.
// Returns nil if the annotation should be dropped.
func (a *Anonymizer) processResourceTypeAnnotation(ann *anypb.Any) (*anypb.Any, error) {
	// ChildResourceType - anonymize resource type ID
	childRT := &v2.ChildResourceType{}
	if ann.MessageIs(childRT) {
		if err := ann.UnmarshalTo(childRT); err != nil {
			return nil, err
		}
		if childRT.GetResourceTypeId() != "" {
			childRT.SetResourceTypeId(a.hasher.AnonymizeResourceType(childRT.GetResourceTypeId()))
		}
		return anypb.New(childRT)
	}

	// ExternalLink - anonymize URL
	externalLink := &v2.ExternalLink{}
	if ann.MessageIs(externalLink) {
		if err := ann.UnmarshalTo(externalLink); err != nil {
			return nil, err
		}
		if externalLink.GetUrl() != "" {
			externalLink.SetUrl(a.hasher.AnonymizeURL(externalLink.GetUrl()))
		}
		return anypb.New(externalLink)
	}

	// Empty marker types - preserve as-is (no PII)
	for _, marker := range []proto.Message{
		&v2.SkipEntitlementsAndGrants{},
		&v2.SkipGrants{},
		&v2.SkipEntitlements{},
	} {
		if ann.MessageIs(marker) {
			return ann, nil
		}
	}

	// Unknown type - drop it
	return nil, nil
}

// anonymizeGrantAnnotations processes all annotations on a Grant,
// anonymizing known types and dropping unknown types.
func (a *Anonymizer) anonymizeGrantAnnotations(g *v2.Grant) error {
	if g == nil {
		return nil
	}

	var result []*anypb.Any
	for _, ann := range g.GetAnnotations() {
		if processed, err := a.processGrantAnnotation(ann); err != nil {
			return err
		} else if processed != nil {
			result = append(result, processed)
		}
	}

	g.SetAnnotations(result)
	return nil
}

// processGrantAnnotation processes a single annotation from a Grant.
// Returns nil if the annotation should be dropped.
func (a *Anonymizer) processGrantAnnotation(ann *anypb.Any) (*anypb.Any, error) {
	// GrantExpandable - anonymize entitlement and resource type IDs
	expandable := &v2.GrantExpandable{}
	if ann.MessageIs(expandable) {
		if err := ann.UnmarshalTo(expandable); err != nil {
			return nil, err
		}
		entitlementIDs := expandable.GetEntitlementIds()
		for i, eid := range entitlementIDs {
			if eid != "" {
				entitlementIDs[i] = a.hasher.AnonymizeExternalID(eid)
			}
		}
		expandable.SetEntitlementIds(entitlementIDs)

		resourceTypeIDs := expandable.GetResourceTypeIds()
		for i, rtid := range resourceTypeIDs {
			if rtid != "" {
				resourceTypeIDs[i] = a.hasher.AnonymizeResourceType(rtid)
			}
		}
		expandable.SetResourceTypeIds(resourceTypeIDs)
		return anypb.New(expandable)
	}

	// GrantMetadata - clear metadata
	grantMetadata := &v2.GrantMetadata{}
	if ann.MessageIs(grantMetadata) {
		if err := ann.UnmarshalTo(grantMetadata); err != nil {
			return nil, err
		}
		grantMetadata.ClearMetadata()
		return anypb.New(grantMetadata)
	}

	// GrantImmutable - anonymize source_id, clear metadata
	grantImmutable := &v2.GrantImmutable{}
	if ann.MessageIs(grantImmutable) {
		if err := ann.UnmarshalTo(grantImmutable); err != nil {
			return nil, err
		}
		if grantImmutable.GetSourceId() != "" {
			grantImmutable.SetSourceId(a.hasher.Hash(grantImmutable.GetSourceId()))
		}
		grantImmutable.ClearMetadata()
		return anypb.New(grantImmutable)
	}

	// ExternalLink - anonymize URL
	externalLink := &v2.ExternalLink{}
	if ann.MessageIs(externalLink) {
		if err := ann.UnmarshalTo(externalLink); err != nil {
			return nil, err
		}
		if externalLink.GetUrl() != "" {
			externalLink.SetUrl(a.hasher.AnonymizeURL(externalLink.GetUrl()))
		}
		return anypb.New(externalLink)
	}

	// Unknown type - drop it
	return nil, nil
}

// anonymizeEntitlementAnnotations processes all annotations on an Entitlement,
// anonymizing known types and dropping unknown types.
func (a *Anonymizer) anonymizeEntitlementAnnotations(e *v2.Entitlement) error {
	if e == nil {
		return nil
	}

	var result []*anypb.Any
	for _, ann := range e.GetAnnotations() {
		if processed, err := a.processEntitlementAnnotation(ann); err != nil {
			return err
		} else if processed != nil {
			result = append(result, processed)
		}
	}

	e.SetAnnotations(result)
	return nil
}

// processEntitlementAnnotation processes a single annotation from an Entitlement.
// Returns nil if the annotation should be dropped.
func (a *Anonymizer) processEntitlementAnnotation(ann *anypb.Any) (*anypb.Any, error) {
	// EntitlementImmutable - anonymize source_id, clear metadata
	entitlementImmutable := &v2.EntitlementImmutable{}
	if ann.MessageIs(entitlementImmutable) {
		if err := ann.UnmarshalTo(entitlementImmutable); err != nil {
			return nil, err
		}
		if entitlementImmutable.GetSourceId() != "" {
			entitlementImmutable.SetSourceId(a.hasher.Hash(entitlementImmutable.GetSourceId()))
		}
		entitlementImmutable.ClearMetadata()
		return anypb.New(entitlementImmutable)
	}

	// ExternalLink - anonymize URL
	externalLink := &v2.ExternalLink{}
	if ann.MessageIs(externalLink) {
		if err := ann.UnmarshalTo(externalLink); err != nil {
			return nil, err
		}
		if externalLink.GetUrl() != "" {
			externalLink.SetUrl(a.hasher.AnonymizeURL(externalLink.GetUrl()))
		}
		return anypb.New(externalLink)
	}

	// Unknown type - drop it
	return nil, nil
}
