package resource

import (
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/stretchr/testify/require"
)

// TestNewLicenseProfileTrait_AllFieldsOptional verifies the constructor
// accepts an empty trait — every field is optional.
func TestNewLicenseProfileTrait_AllFieldsOptional(t *testing.T) {
	trait, err := NewLicenseProfileTrait()
	require.NoError(t, err)
	require.Empty(t, trait.GetLicenseName())
	require.Equal(t, int64(0), trait.GetPurchasedSeats())
	require.Empty(t, trait.GetCurrency())
}

// TestNewLicenseProfileTrait_FullPopulation exercises every option to
// confirm fields round-trip through the proto.
func TestNewLicenseProfileTrait_FullPopulation(t *testing.T) {
	trait, err := NewLicenseProfileTrait(
		WithLicenseName("Enterprise"),
		WithLicenseSeats(200, 175),
		WithLicenseCost(2400, "USD"),
		WithLicenseEntitlementIDs("ent-enterprise-member", "ent-enterprise-admin"),
	)
	require.NoError(t, err)

	require.Equal(t, "Enterprise", trait.GetLicenseName())
	require.Equal(t, int64(200), trait.GetPurchasedSeats())
	require.Equal(t, int64(175), trait.GetConsumedSeats())
	require.Equal(t, int64(2400), trait.GetCostPerUnitInCents())
	require.Equal(t, "USD", trait.GetCurrency())
	require.Equal(t,
		[]string{"ent-enterprise-member", "ent-enterprise-admin"},
		trait.GetEntitlementIds(),
	)
}

// TestNewLicenseProfileTrait_CurrencyDefaultsToUSD confirms that when a
// cost is set without a currency, the trait fills in USD.
func TestNewLicenseProfileTrait_CurrencyDefaultsToUSD(t *testing.T) {
	trait, err := NewLicenseProfileTrait(
		WithLicenseCost(1000, ""),
	)
	require.NoError(t, err)
	require.Equal(t, "USD", trait.GetCurrency())
}

// TestWithLicenseSeats_ZeroValuesAccepted documents the spec: zero means
// "not available from the vendor" and is a valid value.
func TestWithLicenseSeats_ZeroValuesAccepted(t *testing.T) {
	trait, err := NewLicenseProfileTrait(
		WithLicenseName("Free Tier"),
		WithLicenseSeats(0, 0),
	)
	require.NoError(t, err)
	require.Equal(t, int64(0), trait.GetPurchasedSeats())
	require.Equal(t, int64(0), trait.GetConsumedSeats())
}

// TestWithLicenseProfileTrait_AppliesToResource verifies the ResourceOption
// attaches the trait to a resource's annotations and that
// GetLicenseProfileTrait round-trips it.
func TestWithLicenseProfileTrait_AppliesToResource(t *testing.T) {
	rt := &v2.ResourceType{}
	rt.SetId("license")
	rt.SetTraits([]v2.ResourceType_Trait{v2.ResourceType_TRAIT_LICENSE_PROFILE})

	r, err := NewResource(
		"Enterprise",
		rt,
		"license-enterprise",
		WithLicenseProfileTrait(
			WithLicenseName("Enterprise"),
			WithLicenseSeats(200, 175),
			WithLicenseCost(2400, "USD"),
			WithLicenseEntitlementIDs("ent-enterprise-member"),
		),
	)
	require.NoError(t, err)

	got, err := GetLicenseProfileTrait(r)
	require.NoError(t, err)
	require.Equal(t, "Enterprise", got.GetLicenseName())
	require.Equal(t, int64(200), got.GetPurchasedSeats())
	require.Equal(t, int64(175), got.GetConsumedSeats())
	require.Equal(t, int64(2400), got.GetCostPerUnitInCents())
	require.Equal(t, "USD", got.GetCurrency())
	require.Equal(t, []string{"ent-enterprise-member"}, got.GetEntitlementIds())
}

// TestWithLicenseProfileTrait_UpdatesExistingAnnotation confirms merge
// semantics on subsequent applications: each option overwrites only its
// field, and repeated fields are replaced wholesale.
func TestWithLicenseProfileTrait_UpdatesExistingAnnotation(t *testing.T) {
	rt := &v2.ResourceType{}
	rt.SetId("license")
	rt.SetTraits([]v2.ResourceType_Trait{v2.ResourceType_TRAIT_LICENSE_PROFILE})

	r, err := NewResource(
		"Enterprise",
		rt,
		"license-enterprise",
		WithLicenseProfileTrait(
			WithLicenseName("Enterprise"),
			WithLicenseSeats(100, 50),
			WithLicenseEntitlementIDs("ent-a", "ent-b"),
		),
	)
	require.NoError(t, err)

	require.NoError(t, WithLicenseProfileTrait(
		WithLicenseSeats(200, 175),
		WithLicenseEntitlementIDs("ent-c"),
	)(r))

	got, err := GetLicenseProfileTrait(r)
	require.NoError(t, err)
	require.Equal(t, "Enterprise", got.GetLicenseName())
	require.Equal(t, int64(200), got.GetPurchasedSeats())
	require.Equal(t, int64(175), got.GetConsumedSeats())
	require.Equal(t, []string{"ent-c"}, got.GetEntitlementIds())
}

// TestGetLicenseProfileTrait_NotPresent verifies the read helper surfaces a
// clear error when the trait is missing.
func TestGetLicenseProfileTrait_NotPresent(t *testing.T) {
	rt := &v2.ResourceType{}
	rt.SetId("user")
	rt.SetTraits([]v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER})

	r, err := NewResource("Alice", rt, "alice-id")
	require.NoError(t, err)

	_, err = GetLicenseProfileTrait(r)
	require.Error(t, err)
	require.ErrorContains(t, err, "license profile trait was not found")
}

// TestIsLicenseProfileResource verifies the trait check.
func TestIsLicenseProfileResource(t *testing.T) {
	t.Run("with trait", func(t *testing.T) {
		rt := &v2.ResourceType{}
		rt.SetTraits([]v2.ResourceType_Trait{v2.ResourceType_TRAIT_LICENSE_PROFILE})
		require.True(t, IsLicenseProfileResource(rt))
	})

	t.Run("without trait", func(t *testing.T) {
		rt := &v2.ResourceType{}
		rt.SetTraits([]v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER})
		require.False(t, IsLicenseProfileResource(rt))
	})

	t.Run("nil resource type", func(t *testing.T) {
		require.False(t, IsLicenseProfileResource(nil))
	})

	t.Run("multiple traits including license profile", func(t *testing.T) {
		rt := &v2.ResourceType{}
		rt.SetTraits([]v2.ResourceType_Trait{
			v2.ResourceType_TRAIT_GROUP,
			v2.ResourceType_TRAIT_LICENSE_PROFILE,
		})
		require.True(t, IsLicenseProfileResource(rt))
	})
}

// TestLicenseProfileTrait_Validate exercises the proto-generated Validate
// method directly. Pins the proto-level rules so future edits that loosen
// them are caught in CI.
func TestLicenseProfileTrait_Validate(t *testing.T) {
	validTrait := func(mut func(*v2.LicenseProfileTrait)) *v2.LicenseProfileTrait {
		p := &v2.LicenseProfileTrait{}
		p.SetLicenseName("Enterprise")
		p.SetCurrency("USD")
		if mut != nil {
			mut(p)
		}
		return p
	}

	cases := []struct {
		name    string
		trait   *v2.LicenseProfileTrait
		wantErr string
	}{
		{
			name:  "happy path",
			trait: validTrait(nil),
		},
		{
			name: "empty currency accepted (ignore_empty)",
			trait: validTrait(func(p *v2.LicenseProfileTrait) {
				p.SetCurrency("")
			}),
		},
		{
			name: "currency too long rejected",
			trait: validTrait(func(p *v2.LicenseProfileTrait) {
				p.SetCurrency("VERYLONGCURRENCY")
			}),
			wantErr: "Currency",
		},
		{
			name: "duplicate entitlement_ids rejected",
			trait: validTrait(func(p *v2.LicenseProfileTrait) {
				p.SetEntitlementIds([]string{"ent-a", "ent-a"})
			}),
			wantErr: "EntitlementIds",
		},
		{
			name: "negative seats accepted (proto has no rule)",
			trait: validTrait(func(p *v2.LicenseProfileTrait) {
				p.SetPurchasedSeats(-1)
				p.SetConsumedSeats(-1)
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
