package resource

import (
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
)

// defaultLicenseCurrency is applied to LicenseProfile.currency when the
// connector does not provide one explicitly.
const defaultLicenseCurrency = "USD"

// LicenseTraitOption is a functional option for configuring a LicenseTrait.
type LicenseTraitOption func(*v2.LicenseTrait) error

// WithLicenseName sets the human-readable license label
// (e.g. "Enterprise", "Business Plus", "E3").
func WithLicenseName(name string) LicenseTraitOption {
	return func(t *v2.LicenseTrait) error {
		licenseProfile(t).SetLicenseName(name)
		return nil
	}
}

// WithLicenseSeats sets purchased and consumed seat counts from the vendor
// API. Pass 0 for either value when the source does not report it.
func WithLicenseSeats(purchased, consumed int64) LicenseTraitOption {
	return func(t *v2.LicenseTrait) error {
		p := licenseProfile(t)
		p.SetPurchasedSeats(purchased)
		p.SetConsumedSeats(consumed)
		return nil
	}
}

// WithLicenseCost sets the per-seat cost in cents and the ISO 4217 currency.
// An empty currency defaults to USD.
func WithLicenseCost(costPerUnitInCents int64, currency string) LicenseTraitOption {
	return func(t *v2.LicenseTrait) error {
		p := licenseProfile(t)
		p.SetCostPerUnitInCents(costPerUnitInCents)
		if currency == "" {
			currency = defaultLicenseCurrency
		}
		p.SetCurrency(currency)
		return nil
	}
}

// WithLicenseEntitlementIDs sets the entitlements on this connector that
// indicate a user holds this license seat. Replaces any previous list.
func WithLicenseEntitlementIDs(ids ...string) LicenseTraitOption {
	return func(t *v2.LicenseTrait) error {
		licenseProfile(t).SetEntitlementIds(ids)
		return nil
	}
}

// NewLicenseTrait builds a LicenseTrait. All fields are optional; currency
// defaults to USD when at least one cost-related option is applied.
func NewLicenseTrait(opts ...LicenseTraitOption) (*v2.LicenseTrait, error) {
	trait := &v2.LicenseTrait{}

	for _, opt := range opts {
		if err := opt(trait); err != nil {
			return nil, err
		}
	}

	if p := trait.GetProfile(); p != nil && p.GetCurrency() == "" {
		p.SetCurrency(defaultLicenseCurrency)
	}

	return trait, nil
}

// GetLicenseTrait returns the LicenseTrait from a resource's annotations.
func GetLicenseTrait(resource *v2.Resource) (*v2.LicenseTrait, error) {
	ret := &v2.LicenseTrait{}
	annos := annotations.Annotations(resource.GetAnnotations())
	ok, err := annos.Pick(ret)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("license trait was not found on resource")
	}
	return ret, nil
}

// WithLicenseTrait adds or updates a LicenseTrait annotation on a resource.
//
// Merge semantics: if the resource already carries a LicenseTrait, each
// option overwrites only its field. Repeated fields (entitlement_ids) are
// replaced wholesale, not appended — pass the full intended list each time.
func WithLicenseTrait(opts ...LicenseTraitOption) ResourceOption {
	return func(r *v2.Resource) error {
		t := &v2.LicenseTrait{}
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

		if p := t.GetProfile(); p != nil && p.GetCurrency() == "" {
			p.SetCurrency(defaultLicenseCurrency)
		}

		annos.Update(t)
		r.SetAnnotations(annos)
		return nil
	}
}

// IsLicenseResource returns true if the resource type declares TRAIT_LICENSE.
func IsLicenseResource(resourceType *v2.ResourceType) bool {
	for _, trait := range resourceType.GetTraits() {
		if trait == v2.ResourceType_TRAIT_LICENSE {
			return true
		}
	}
	return false
}

// licenseProfile returns the trait's profile, lazily initializing it on
// first access so options can write fields without each having to check.
func licenseProfile(t *v2.LicenseTrait) *v2.LicenseProfile {
	if p := t.GetProfile(); p != nil {
		return p
	}
	p := &v2.LicenseProfile{}
	t.SetProfile(p)
	return p
}
