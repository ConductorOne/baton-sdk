package resource

import (
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/stretchr/testify/require"
)

// TestNewVendorAgreementTrait_AllFieldsOptional verifies the constructor
// accepts an empty trait. After the split with VendorTrait, the agreement
// trait has no required fields; date ordering is the only invariant.
func TestNewVendorAgreementTrait_AllFieldsOptional(t *testing.T) {
	trait, err := NewVendorAgreementTrait()
	require.NoError(t, err)
	require.Empty(t, trait.GetAgreementName())
	require.Nil(t, trait.GetTotalValue())
}

// TestNewVendorAgreementTrait_FullPopulation exercises every option to
// confirm fields round-trip through the proto.
func TestNewVendorAgreementTrait_FullPopulation(t *testing.T) {
	startDate := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	endDate := time.Date(2026, 12, 31, 0, 0, 0, 0, time.UTC)
	terminateBy := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)

	trait, err := NewVendorAgreementTrait(
		WithAgreementTerm("Annual Subscription 2026", startDate, endDate, true),
		WithLastDateToTerminate(terminateBy),
		WithRenewalStatus(v2.VendorAgreementTrait_RENEWAL_STATUS_NOT_STARTED, "NOT_STARTED"),
		WithTotalValue(NewMoney(4_800_000, "USD")),
		WithLineItems(
			NewLineItem(
				"Enterprise Plan",
				200,
				"seat",
				NewMoney(2_000, "USD"),
				NewMoney(4_800_000, "USD"),
			),
		),
		WithPricingModel(v2.VendorAgreementTrait_PRICING_MODEL_PER_SEAT),
		WithTrailing30DaySpend(NewMoney(400_000, "USD")),
		WithTrailing365DaySpend(NewMoney(4_800_000, "USD")),
		WithYTDSpend(NewMoney(400_000, "USD")),
		WithExternalAccountManager("am@vendor.example", "Account Manager"),
	)
	require.NoError(t, err)

	// Term
	require.Equal(t, "Annual Subscription 2026", trait.GetAgreementName())
	require.Equal(t, startDate.Unix(), trait.GetStartDate().AsTime().Unix())
	require.Equal(t, endDate.Unix(), trait.GetEndDate().AsTime().Unix())
	require.Equal(t, terminateBy.Unix(), trait.GetLastDateToTerminate().AsTime().Unix())
	require.True(t, trait.GetAutoRenewal())

	// Renewal status
	require.Equal(t, v2.VendorAgreementTrait_RENEWAL_STATUS_NOT_STARTED, trait.GetRenewalStatus())
	require.Equal(t, "NOT_STARTED", trait.GetRenewalStatusRaw())

	// Value
	require.NotNil(t, trait.GetTotalValue())
	require.Equal(t, int64(4_800_000), trait.GetTotalValue().GetAmountMinor())
	require.Equal(t, "USD", trait.GetTotalValue().GetCurrencyCode())

	// Line items
	require.Len(t, trait.GetLineItems(), 1)
	li := trait.GetLineItems()[0]
	require.Equal(t, "Enterprise Plan", li.GetProductOrServiceName())
	require.Equal(t, float64(200), li.GetQuantity())
	require.Equal(t, "seat", li.GetUnit())
	require.Equal(t, int64(2_000), li.GetPricePerUnit().GetAmountMinor())
	require.Equal(t, int64(4_800_000), li.GetTotalPrice().GetAmountMinor())

	// Pricing model
	require.Equal(t, v2.VendorAgreementTrait_PRICING_MODEL_PER_SEAT, trait.GetPricingModel())

	// Spend windows
	require.Equal(t, int64(400_000), trait.GetTrailing_30DSpend().GetAmountMinor())
	require.Equal(t, int64(4_800_000), trait.GetTrailing_365DSpend().GetAmountMinor())
	require.Equal(t, int64(400_000), trait.GetYtdSpend().GetAmountMinor())

	// External account manager
	require.Equal(t, "am@vendor.example", trait.GetExternalAccountManagerEmail())
	require.Equal(t, "Account Manager", trait.GetExternalAccountManagerName())
}

// TestWithLastDateToTerminate_ZeroTimeSkips verifies that passing a zero
// time.Time leaves the field unset.
func TestWithLastDateToTerminate_ZeroTimeSkips(t *testing.T) {
	trait, err := NewVendorAgreementTrait(
		WithLastDateToTerminate(time.Time{}),
	)
	require.NoError(t, err)
	require.Nil(t, trait.GetLastDateToTerminate())
}

// TestSpendWindows_IndependentOptions verifies each spend-window option
// sets only its own field.
func TestSpendWindows_IndependentOptions(t *testing.T) {
	t.Run("only 30d", func(t *testing.T) {
		trait, err := NewVendorAgreementTrait(
			WithTrailing30DaySpend(NewMoney(400_000, "USD")),
		)
		require.NoError(t, err)
		require.NotNil(t, trait.GetTrailing_30DSpend())
		require.Nil(t, trait.GetTrailing_365DSpend())
		require.Nil(t, trait.GetYtdSpend())
	})

	t.Run("only ytd", func(t *testing.T) {
		trait, err := NewVendorAgreementTrait(
			WithYTDSpend(NewMoney(12_345, "USD")),
		)
		require.NoError(t, err)
		require.Nil(t, trait.GetTrailing_30DSpend())
		require.Nil(t, trait.GetTrailing_365DSpend())
		require.Equal(t, int64(12_345), trait.GetYtdSpend().GetAmountMinor())
	})

	t.Run("all three", func(t *testing.T) {
		trait, err := NewVendorAgreementTrait(
			WithTrailing30DaySpend(NewMoney(100, "USD")),
			WithTrailing365DaySpend(NewMoney(200, "USD")),
			WithYTDSpend(NewMoney(300, "USD")),
		)
		require.NoError(t, err)
		require.Equal(t, int64(100), trait.GetTrailing_30DSpend().GetAmountMinor())
		require.Equal(t, int64(200), trait.GetTrailing_365DSpend().GetAmountMinor())
		require.Equal(t, int64(300), trait.GetYtdSpend().GetAmountMinor())
	})
}

// TestNewVendorAgreementTrait_DateOrdering verifies cross-field date
// ordering checks fire when both endpoints of a pair are set, and accept
// partial data.
func TestNewVendorAgreementTrait_DateOrdering(t *testing.T) {
	jan := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	feb := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	mar := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)

	t.Run("end before start rejected", func(t *testing.T) {
		_, err := NewVendorAgreementTrait(
			WithAgreementTerm("Plan", feb, jan, false),
		)
		require.Error(t, err)
		require.ErrorContains(t, err, "end_date must not precede start_date")
	})

	t.Run("last_date_to_terminate after end rejected", func(t *testing.T) {
		_, err := NewVendorAgreementTrait(
			WithAgreementTerm("Plan", jan, feb, false),
			WithLastDateToTerminate(mar),
		)
		require.Error(t, err)
		require.ErrorContains(t, err, "last_date_to_terminate must not exceed end_date")
	})

	t.Run("last_date_to_terminate equal to end accepted", func(t *testing.T) {
		_, err := NewVendorAgreementTrait(
			WithAgreementTerm("Plan", jan, feb, false),
			WithLastDateToTerminate(feb),
		)
		require.NoError(t, err)
	})

	t.Run("partial data accepted (only end_date)", func(t *testing.T) {
		_, err := NewVendorAgreementTrait(
			WithAgreementTerm("Plan", time.Time{}, feb, false),
		)
		require.NoError(t, err)
	})

	t.Run("partial data accepted (only last_date_to_terminate)", func(t *testing.T) {
		_, err := NewVendorAgreementTrait(
			WithLastDateToTerminate(jan),
		)
		require.NoError(t, err)
	})
}

// TestNewVendorAgreementTrait_NilLineItemPrices confirms LineItem fields
// with nil price_per_unit and total_price are accepted.
func TestNewVendorAgreementTrait_NilLineItemPrices(t *testing.T) {
	trait, err := NewVendorAgreementTrait(
		WithLineItems(NewLineItem("Plan A", 200, "seat", nil, nil)),
	)
	require.NoError(t, err)
	require.Len(t, trait.GetLineItems(), 1)
	require.Nil(t, trait.GetLineItems()[0].GetPricePerUnit())
	require.Nil(t, trait.GetLineItems()[0].GetTotalPrice())
}

// TestNewVendorAgreementTrait_FractionalQuantity confirms fractional
// quantities round-trip — the reason quantity is double rather than int64.
func TestNewVendorAgreementTrait_FractionalQuantity(t *testing.T) {
	trait, err := NewVendorAgreementTrait(
		WithLineItems(NewLineItem("Storage", 1.5, "TB", nil, nil)),
	)
	require.NoError(t, err)
	require.Equal(t, float64(1.5), trait.GetLineItems()[0].GetQuantity())
}

// TestWithVendorAgreementTrait_AppliesToResource verifies the
// ResourceOption attaches the trait to a resource's annotations and that
// GetVendorAgreementTrait round-trips it.
func TestWithVendorAgreementTrait_AppliesToResource(t *testing.T) {
	rt := &v2.ResourceType{}
	rt.SetId("vendor_agreement")
	rt.SetTraits([]v2.ResourceType_Trait{v2.ResourceType_TRAIT_VENDOR_AGREEMENT})

	r, err := NewResource(
		"Annual Subscription 2026",
		rt,
		"agreement-uuid",
		WithVendorAgreementTrait(
			WithAgreementTerm("Annual Subscription 2026",
				time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2026, 12, 31, 0, 0, 0, 0, time.UTC),
				true),
			WithTotalValue(NewMoney(4_800_000, "USD")),
		),
	)
	require.NoError(t, err)

	got, err := GetVendorAgreementTrait(r)
	require.NoError(t, err)
	require.Equal(t, "Annual Subscription 2026", got.GetAgreementName())
	require.Equal(t, int64(4_800_000), got.GetTotalValue().GetAmountMinor())
}

// TestWithVendorAgreementTrait_UpdatesExistingAnnotation confirms merge
// semantics on subsequent applications.
func TestWithVendorAgreementTrait_UpdatesExistingAnnotation(t *testing.T) {
	rt := &v2.ResourceType{}
	rt.SetId("vendor_agreement")
	rt.SetTraits([]v2.ResourceType_Trait{v2.ResourceType_TRAIT_VENDOR_AGREEMENT})

	r, err := NewResource(
		"Annual Subscription 2026",
		rt,
		"agreement-uuid",
		WithVendorAgreementTrait(
			WithAgreementTerm("Annual Subscription 2026",
				time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2026, 12, 31, 0, 0, 0, 0, time.UTC),
				true),
		),
	)
	require.NoError(t, err)

	require.NoError(t, WithVendorAgreementTrait(
		WithTotalValue(NewMoney(4_800_000, "USD")),
	)(r))

	got, err := GetVendorAgreementTrait(r)
	require.NoError(t, err)
	require.Equal(t, "Annual Subscription 2026", got.GetAgreementName())
	require.Equal(t, int64(4_800_000), got.GetTotalValue().GetAmountMinor())
}

// TestGetVendorAgreementTrait_NotPresent verifies the read helper surfaces
// a clear error when the trait is missing.
func TestGetVendorAgreementTrait_NotPresent(t *testing.T) {
	rt := &v2.ResourceType{}
	rt.SetId("user")
	rt.SetTraits([]v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER})

	r, err := NewResource("Alice", rt, "alice-id")
	require.NoError(t, err)

	_, err = GetVendorAgreementTrait(r)
	require.Error(t, err)
	require.ErrorContains(t, err, "vendor agreement trait was not found")
}

// TestIsVendorAgreementResource verifies the trait check.
func TestIsVendorAgreementResource(t *testing.T) {
	t.Run("with trait", func(t *testing.T) {
		rt := &v2.ResourceType{}
		rt.SetTraits([]v2.ResourceType_Trait{v2.ResourceType_TRAIT_VENDOR_AGREEMENT})
		require.True(t, IsVendorAgreementResource(rt))
	})

	t.Run("without trait", func(t *testing.T) {
		rt := &v2.ResourceType{}
		rt.SetTraits([]v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER})
		require.False(t, IsVendorAgreementResource(rt))
	})

	t.Run("nil resource type", func(t *testing.T) {
		require.False(t, IsVendorAgreementResource(nil))
	})
}

// TestVendorAgreementTrait_Validate exercises the proto-generated Validate
// method directly. Pins the proto-level rules so future edits that loosen
// them are caught in CI.
func TestVendorAgreementTrait_Validate(t *testing.T) {
	validMoney := func() *v2.Money {
		return NewMoney(4_800_000, "USD")
	}
	validTrait := func(mut func(*v2.VendorAgreementTrait)) *v2.VendorAgreementTrait {
		t := &v2.VendorAgreementTrait{}
		t.SetTotalValue(validMoney())
		if mut != nil {
			mut(t)
		}
		return t
	}

	cases := []struct {
		name    string
		trait   *v2.VendorAgreementTrait
		wantErr string
	}{
		{
			name:  "happy path",
			trait: validTrait(nil),
		},
		{
			name: "money: zero amount accepted",
			trait: validTrait(func(t *v2.VendorAgreementTrait) {
				t.SetTotalValue(NewMoney(0, "USD"))
			}),
		},
		{
			name: "money: negative amount accepted",
			trait: validTrait(func(t *v2.VendorAgreementTrait) {
				t.SetTotalValue(NewMoney(-150, "USD"))
			}),
		},
		{
			name: "money: currency too short",
			trait: validTrait(func(t *v2.VendorAgreementTrait) {
				t.SetTotalValue(NewMoney(100, "U"))
			}),
			wantErr: "CurrencyCode",
		},
		{
			name: "money: currency_code with non-alphanumeric rejected",
			trait: validTrait(func(t *v2.VendorAgreementTrait) {
				t.SetTotalValue(NewMoney(100, "US-D"))
			}),
			wantErr: "CurrencyCode",
		},
		{
			name: "external_account_manager_email: invalid rejected",
			trait: validTrait(func(t *v2.VendorAgreementTrait) {
				t.SetExternalAccountManagerEmail("not-an-email")
			}),
			wantErr: "ExternalAccountManagerEmail",
		},
		{
			name: "external_account_manager_email: empty accepted (ignore_empty)",
			trait: validTrait(func(t *v2.VendorAgreementTrait) {
				t.SetExternalAccountManagerEmail("")
			}),
		},
		{
			name: "external_account_manager: name without email accepted",
			trait: validTrait(func(t *v2.VendorAgreementTrait) {
				t.SetExternalAccountManagerName("Account Manager")
			}),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.trait.Validate()
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.wantErr)
		})
	}
}
