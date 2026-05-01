package resource

import (
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
)

// VendorTraitOption is a functional option for configuring a VendorTrait.
//
// VendorTrait carries cross-system identity for a vendor: who it is in the
// source, where to view it, and the IDs by which it can be cross-referenced
// against other sources. It is intentionally narrow.
//
// Two related concerns live elsewhere:
//
//   - Agreement, spend, and contract data go on VendorAgreementTrait.
//     A resource representing an agreement typically carries both traits.
//   - Internal owners (security owner, business owner, etc.) are modeled
//     as Baton entitlements + grants on the resource, granted to user
//     resources synced from the same source.
type VendorTraitOption func(*v2.VendorTrait) error

// WithVendorIdentity sets the required identity fields. dbaName may be
// empty when legal and trade names are the same.
func WithVendorIdentity(vendorID string, vendorName string, dbaName string) VendorTraitOption {
	return func(t *v2.VendorTrait) error {
		if vendorID == "" {
			return fmt.Errorf("vendor_id cannot be empty")
		}
		if vendorName == "" {
			return fmt.Errorf("vendor_name cannot be empty")
		}
		t.SetVendorId(vendorID)
		t.SetVendorName(vendorName)
		t.SetVendorDbaName(dbaName)
		return nil
	}
}

// WithVendorWebsite sets the inferred website domain (e.g. "example.com").
// A primary join key for cross-source vendor identity. No-op when empty.
func WithVendorWebsite(domain string) VendorTraitOption {
	return func(t *v2.VendorTrait) error {
		t.SetWebsiteDomain(domain)
		return nil
	}
}

// WithExternalVendorID sets the customer-defined external ID for the vendor
// in the source system, stable across renames. The strongest cross-source
// join key when set.
func WithExternalVendorID(id string) VendorTraitOption {
	return func(t *v2.VendorTrait) error {
		t.SetExternalVendorId(id)
		return nil
	}
}

// WithVendorDeepLinkURL sets the source-system URL for the vendor. Must be
// https://.
func WithVendorDeepLinkURL(url string) VendorTraitOption {
	return func(t *v2.VendorTrait) error {
		t.SetDeepLinkUrl(url)
		return nil
	}
}

// WithVendorSourceScoping sets the source-system business and entity IDs.
// Either may be empty when the source is single-business or single-entity.
func WithVendorSourceScoping(businessID string, entityID string) VendorTraitOption {
	return func(t *v2.VendorTrait) error {
		t.SetSourceBusinessId(businessID)
		t.SetSourceEntityId(entityID)
		return nil
	}
}

// WithTrailing30DaySpend sets the trailing-30-day spend aggregate. Use
// only when the source pre-aggregates and returns it. Spend is always a
// vendor-level (not agreement-level) property.
func WithTrailing30DaySpend(m *v2.Money) VendorTraitOption {
	return func(t *v2.VendorTrait) error {
		t.SetTrailing_30DSpend(m)
		return nil
	}
}

// WithTrailing365DaySpend sets the trailing-365-day spend aggregate.
func WithTrailing365DaySpend(m *v2.Money) VendorTraitOption {
	return func(t *v2.VendorTrait) error {
		t.SetTrailing_365DSpend(m)
		return nil
	}
}

// WithYTDSpend sets the year-to-date spend aggregate.
func WithYTDSpend(m *v2.Money) VendorTraitOption {
	return func(t *v2.VendorTrait) error {
		t.SetYtdSpend(m)
		return nil
	}
}

// NewVendorTrait builds a VendorTrait. WithVendorIdentity is required.
//
// Example:
//
//	trait, err := NewVendorTrait(
//	    WithVendorIdentity("vendor-uuid", "Acme Software, Inc.", "Acme"),
//	    WithVendorWebsite("example.com"),
//	    WithExternalVendorID("acme-prod"),
//	    WithVendorDeepLinkURL("https://example.com/vendors/v123"),
//	    WithTrailing30DaySpend(NewMoney(400_000, "USD")),
//	    WithYTDSpend(NewMoney(400_000, "USD")))
func NewVendorTrait(opts ...VendorTraitOption) (*v2.VendorTrait, error) {
	trait := &v2.VendorTrait{}

	for _, opt := range opts {
		if err := opt(trait); err != nil {
			return nil, err
		}
	}

	if trait.GetVendorId() == "" {
		return nil, fmt.Errorf("vendor_id must be set (use WithVendorIdentity)")
	}
	if trait.GetVendorName() == "" {
		return nil, fmt.Errorf("vendor_name must be set (use WithVendorIdentity)")
	}

	return trait, nil
}

// GetVendorTrait returns the VendorTrait from a resource's annotations.
func GetVendorTrait(resource *v2.Resource) (*v2.VendorTrait, error) {
	ret := &v2.VendorTrait{}
	annos := annotations.Annotations(resource.GetAnnotations())
	ok, err := annos.Pick(ret)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("vendor trait was not found on resource")
	}
	return ret, nil
}

// WithVendorTrait adds or updates a VendorTrait annotation on a resource.
// WithVendorIdentity is required.
//
// Merge semantics: if the resource already carries a VendorTrait, each
// option overwrites only its field. To clear a singular field, construct
// the trait fresh and re-attach.
func WithVendorTrait(opts ...VendorTraitOption) ResourceOption {
	return func(r *v2.Resource) error {
		t := &v2.VendorTrait{}
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

		if t.GetVendorId() == "" {
			return fmt.Errorf("vendor_id must be set (use WithVendorIdentity)")
		}
		if t.GetVendorName() == "" {
			return fmt.Errorf("vendor_name must be set (use WithVendorIdentity)")
		}

		annos.Update(t)
		r.SetAnnotations(annos)
		return nil
	}
}

// IsVendorResource returns true if the resource type declares TRAIT_VENDOR.
func IsVendorResource(resourceType *v2.ResourceType) bool {
	for _, trait := range resourceType.GetTraits() {
		if trait == v2.ResourceType_TRAIT_VENDOR {
			return true
		}
	}
	return false
}
