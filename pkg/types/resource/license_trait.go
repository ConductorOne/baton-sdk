package resource

import (
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
)

// defaultLicenseCurrency is applied to LicenseProfileTrait.currency when the
// connector does not provide one explicitly.
const defaultLicenseCurrency = "USD"

// LicenseProfileTraitOption is a functional option for configuring a LicenseProfileTrait.
type LicenseProfileTraitOption func(*v2.LicenseProfileTrait) error

// WithLicenseName sets the human-readable license label
// (e.g. "Enterprise", "Business Plus", "E3").
func WithLicenseName(name string) LicenseProfileTraitOption {
	return func(t *v2.LicenseProfileTrait) error {
		t.SetLicenseName(name)
		return nil
	}
}

// WithLicenseSeats sets purchased and consumed seat counts from the vendor
// API. Pass 0 for either value when the source does not report it.
func WithLicenseSeats(purchased, consumed int64) LicenseProfileTraitOption {
	return func(t *v2.LicenseProfileTrait) error {
		t.SetPurchasedSeats(purchased)
		t.SetConsumedSeats(consumed)
		return nil
	}
}

// WithLicenseCost sets the per-seat cost in cents and the ISO 4217 currency.
// An empty currency defaults to USD.
func WithLicenseCost(costPerUnitInCents int64, currency string) LicenseProfileTraitOption {
	return func(t *v2.LicenseProfileTrait) error {
		t.SetCostPerUnitInCents(costPerUnitInCents)
		if currency == "" {
			currency = defaultLicenseCurrency
		}
		t.SetCurrency(currency)
		return nil
	}
}

// WithLicenseEntitlementIDs sets the entitlements on this connector that
// indicate a user holds this license seat. Replaces any previous list.
func WithLicenseEntitlementIDs(ids ...string) LicenseProfileTraitOption {
	return func(t *v2.LicenseProfileTrait) error {
		t.SetEntitlementIds(ids)
		return nil
	}
}

// NewLicenseProfileTrait builds a LicenseProfileTrait. All fields are
// optional; currency defaults to USD when at least one cost-related option
// is applied.
func NewLicenseProfileTrait(opts ...LicenseProfileTraitOption) (*v2.LicenseProfileTrait, error) {
	trait := &v2.LicenseProfileTrait{}

	for _, opt := range opts {
		if err := opt(trait); err != nil {
			return nil, err
		}
	}

	return trait, nil
}

// GetLicenseProfileTrait returns the LicenseProfileTrait from a resource's annotations.
func GetLicenseProfileTrait(resource *v2.Resource) (*v2.LicenseProfileTrait, error) {
	ret := &v2.LicenseProfileTrait{}
	annos := annotations.Annotations(resource.GetAnnotations())
	ok, err := annos.Pick(ret)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("license profile trait was not found on resource")
	}
	return ret, nil
}

// WithLicenseProfileTrait adds or updates a LicenseProfileTrait annotation on a resource.
//
// Merge semantics: if the resource already carries a LicenseProfileTrait, each
// option overwrites only its field. Repeated fields (entitlement_ids) are
// replaced wholesale, not appended — pass the full intended list each time.
func WithLicenseProfileTrait(opts ...LicenseProfileTraitOption) ResourceOption {
	return func(r *v2.Resource) error {
		t := &v2.LicenseProfileTrait{}
		annos := annotations.Annotations(r.GetAnnotations())
		_, err := annos.Pick(t)
		if err != nil {
			return err
		}

		for _, o := range opts {
			if err := o(t); err != nil {
				return err
			}
		}

		annos.Update(t)
		r.SetAnnotations(annos)
		return nil
	}
}

// IsLicenseProfileResource returns true if the resource type declares TRAIT_LICENSE_PROFILE.
func IsLicenseProfileResource(resourceType *v2.ResourceType) bool {
	for _, trait := range resourceType.GetTraits() {
		if trait == v2.ResourceType_TRAIT_LICENSE_PROFILE {
			return true
		}
	}
	return false
}
