package resource

import (
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
)

// VendorAgreementTraitOption is a functional option for configuring a
// VendorAgreementTrait. The trait carries agreement-specific data: term,
// renewal status, total value, line items, pricing model, trailing-window
// spend, and the vendor-side account manager.
//
// Vendor identity (id, name, website, external IDs, deep link, scoping)
// lives on VendorTrait, a separate trait. A resource representing an
// agreement typically carries both traits; attach VendorTrait via
// WithVendorTrait and VendorAgreementTrait via WithVendorAgreementTrait.
//
// Two related concerns live elsewhere:
//
//   - Internal owners (security owner, business owner, etc.) are modeled
//     as Baton entitlements + grants on the resource, granted to user
//     resources synced from the same source.
//   - Write-back of computed metadata (e.g. seat or utilization counts
//     pushed from a consuming platform back to the source) is exposed via
//     ResourceActionProvider; register an action and let consumers
//     discover the capability via ListActionSchemas.
type VendorAgreementTraitOption func(*v2.VendorAgreementTrait) error

// NewMoney constructs a Money. amountMinor is the signed amount in the
// currency's minor unit (USD: 12345 means $123.45; JPY: 12345 means ¥12,345).
func NewMoney(amountMinor int64, currencyCode string) *v2.Money {
	return v2.Money_builder{
		AmountMinor:  amountMinor,
		CurrencyCode: currencyCode,
	}.Build()
}

// NewLineItem constructs a LineItem. Fractional quantities are allowed
// (e.g. 1.5 TB, 0.5 FTE). pricePerUnit and totalPrice may each be nil.
func NewLineItem(productOrService string, quantity float64, unit string, pricePerUnit *v2.Money, totalPrice *v2.Money) *v2.LineItem {
	return v2.LineItem_builder{
		ProductOrServiceName: productOrService,
		Quantity:             quantity,
		Unit:                 unit,
		PricePerUnit:         pricePerUnit,
		TotalPrice:           totalPrice,
	}.Build()
}

// WithAgreementTerm sets the agreement label, dates, and auto-renewal
// flag. Zero time.Time omits a date.
func WithAgreementTerm(agreementName string, startDate time.Time, endDate time.Time, autoRenewal bool) VendorAgreementTraitOption {
	return func(t *v2.VendorAgreementTrait) error {
		t.SetAgreementName(agreementName)
		if !startDate.IsZero() {
			t.SetStartDate(timestamppb.New(startDate))
		}
		if !endDate.IsZero() {
			t.SetEndDate(timestamppb.New(endDate))
		}
		t.SetAutoRenewal(autoRenewal)
		return nil
	}
}

// WithLastDateToTerminate sets the last-date-to-terminate timestamp.
// Skipped on zero. NewVendorAgreementTrait enforces lastDate <= end_date.
func WithLastDateToTerminate(lastDate time.Time) VendorAgreementTraitOption {
	return func(t *v2.VendorAgreementTrait) error {
		if lastDate.IsZero() {
			return nil
		}
		t.SetLastDateToTerminate(timestamppb.New(lastDate))
		return nil
	}
}

// WithRenewalStatus sets the normalized enum and preserves the raw source
// string for forward-compat.
func WithRenewalStatus(status v2.VendorAgreementTrait_RenewalStatus, raw string) VendorAgreementTraitOption {
	return func(t *v2.VendorAgreementTrait) error {
		t.SetRenewalStatus(status)
		t.SetRenewalStatusRaw(raw)
		return nil
	}
}

// WithTotalValue sets the total agreement value.
func WithTotalValue(m *v2.Money) VendorAgreementTraitOption {
	return func(t *v2.VendorAgreementTrait) error {
		t.SetTotalValue(m)
		return nil
	}
}

// WithLineItems sets the per-line-item billing detail.
func WithLineItems(items ...*v2.LineItem) VendorAgreementTraitOption {
	return func(t *v2.VendorAgreementTrait) error {
		t.SetLineItems(items)
		return nil
	}
}

// WithPricingModel sets the pricing model. PRICING_MODEL_UNKNOWN means
// "looked, gave up"; PRICING_MODEL_UNSPECIFIED (the default) means
// "didn't look".
func WithPricingModel(pm v2.VendorAgreementTrait_PricingModel) VendorAgreementTraitOption {
	return func(t *v2.VendorAgreementTrait) error {
		t.SetPricingModel(pm)
		return nil
	}
}

// Trailing-window spend helpers (WithTrailing30DaySpend, WithTrailing365DaySpend,
// WithYTDSpend) live on VendorTrait — see vendor_trait.go. Spend is a
// property of the vendor (not of any individual agreement) in every
// vendor-management system the SDK targets.

// WithExternalAccountManager sets the vendor-side point of contact: the
// email and display name of the account manager at the vendor (not at
// the customer). Either field may be empty. For internal (customer-side)
// owners, model entitlements + grants on the resource instead.
func WithExternalAccountManager(email string, name string) VendorAgreementTraitOption {
	return func(t *v2.VendorAgreementTrait) error {
		t.SetExternalAccountManagerEmail(email)
		t.SetExternalAccountManagerName(name)
		return nil
	}
}

// NewVendorAgreementTrait builds a VendorAgreementTrait. All fields are
// optional individually; the SDK enforces only cross-field date ordering
// (end_date >= start_date; last_date_to_terminate <= end_date).
//
// Example:
//
//	trait, err := NewVendorAgreementTrait(
//	    WithAgreementTerm("Annual Subscription 2026",
//	        time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
//	        time.Date(2026, 12, 31, 0, 0, 0, 0, time.UTC),
//	        true),
//	    WithRenewalStatus(v2.VendorAgreementTrait_RENEWAL_STATUS_NOT_STARTED, "NOT_STARTED"),
//	    WithTotalValue(NewMoney(4_800_000, "USD")),
//	    WithLineItems(NewLineItem("Enterprise Plan", 200, "seat",
//	        NewMoney(2_000, "USD"), NewMoney(4_800_000, "USD"))),
//	    WithPricingModel(v2.VendorAgreementTrait_PRICING_MODEL_PER_SEAT),
//	    WithExternalAccountManager("am@vendor.example", "Account Manager"))
func NewVendorAgreementTrait(opts ...VendorAgreementTraitOption) (*v2.VendorAgreementTrait, error) {
	trait := &v2.VendorAgreementTrait{}

	for _, opt := range opts {
		if err := opt(trait); err != nil {
			return nil, err
		}
	}

	if err := validateDateOrdering(trait); err != nil {
		return nil, err
	}

	return trait, nil
}

// validateDateOrdering checks the cross-field timestamp constraints. Each
// pair is checked only when both endpoints are set.
func validateDateOrdering(t *v2.VendorAgreementTrait) error {
	start := t.GetStartDate()
	end := t.GetEndDate()
	last := t.GetLastDateToTerminate()
	if start != nil && end != nil && end.AsTime().Before(start.AsTime()) {
		return fmt.Errorf("end_date must not precede start_date")
	}
	if last != nil && end != nil && end.AsTime().Before(last.AsTime()) {
		return fmt.Errorf("last_date_to_terminate must not exceed end_date")
	}
	return nil
}

// GetVendorAgreementTrait returns the VendorAgreementTrait from a
// resource's annotations.
func GetVendorAgreementTrait(resource *v2.Resource) (*v2.VendorAgreementTrait, error) {
	ret := &v2.VendorAgreementTrait{}
	annos := annotations.Annotations(resource.GetAnnotations())
	ok, err := annos.Pick(ret)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("vendor agreement trait was not found on resource")
	}
	return ret, nil
}

// WithVendorAgreementTrait adds or updates a VendorAgreementTrait
// annotation on a resource.
//
// Merge semantics: if the resource already carries a VendorAgreementTrait,
// each option overwrites only its field. Repeated fields (e.g. line_items)
// are replaced wholesale, not appended — pass the full intended list each
// time. To clear a singular message field, construct the trait fresh and
// re-attach.
func WithVendorAgreementTrait(opts ...VendorAgreementTraitOption) ResourceOption {
	return func(r *v2.Resource) error {
		t := &v2.VendorAgreementTrait{}
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

		if err := validateDateOrdering(t); err != nil {
			return err
		}

		annos.Update(t)
		r.SetAnnotations(annos)
		return nil
	}
}

// IsVendorAgreementResource returns true if the resource type declares
// TRAIT_VENDOR_AGREEMENT.
func IsVendorAgreementResource(resourceType *v2.ResourceType) bool {
	for _, trait := range resourceType.GetTraits() {
		if trait == v2.ResourceType_TRAIT_VENDOR_AGREEMENT {
			return true
		}
	}
	return false
}
