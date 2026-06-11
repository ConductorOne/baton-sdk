package pebble

import (
	"context"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
)

// resourceTypeIDsWithTrait scans every resource_type in the sync
// and returns those whose Traits list includes the requested trait.
// resource_types is small (O(10–100)) so a primary-range scan is
// cheap; no need for a denormalized index here. v3.ResourceTypeRecord
// stores traits as their string names (TRAIT_USER → "USER") — see
// translate_v2.go; the comparison goes through stringToTrait so a
// future trait can be added to the enum without touching this code.
func (a *Adapter) resourceTypeIDsWithTrait(
	ctx context.Context,
	trait v2.ResourceType_Trait,
) ([]string, error) {
	out := []string{}
	if err := a.engine.IterateResourceTypes(ctx, func(r *v3.ResourceTypeRecord) bool {
		for _, s := range r.GetTraits() {
			if stringToTrait(s) == trait {
				out = append(out, r.GetExternalId())
				break
			}
		}
		return true
	}); err != nil {
		return nil, err
	}
	return out, nil
}
