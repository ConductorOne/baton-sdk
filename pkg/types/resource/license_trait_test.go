package resource

import (
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/stretchr/testify/require"
)

// TestNewLicenseTrait_AllFieldsOptional verifies the constructor accepts
// an empty trait — every field is optional.
func TestNewLicenseTrait_AllFieldsOptional(t *testing.T) {
	trait, err := NewLicenseTrait()
	require.NoError(t, err)
	require.Nil(t, trait.GetProfile())
}

// TestNewLicenseTrait_FullPopulation exercises every option to confirm
// fields round-trip through the proto.
func TestNewLicenseTrait_FullPopulation(t *testing.T) {
	trait, err := NewLicenseTrait(
		WithLicenseName("Enterprise"),
		WithLicenseSeats(200, 175),
		WithLicenseCost(2400, "USD"),
		WithLicenseEntitlementIDs("ent-enterprise-member", "ent-enterprise-admin"),
	)
	require.NoError(t, err)

	p := trait.GetProfile()
	require.NotNil(t, p)
	require.Equal(t, "Enterprise", p.GetLicenseName())
	require.Equal(t, int64(200), p.GetPurchasedSeats())
	require.Equal(t, int64(175), p.GetConsumedSeats())
	require.Equal(t, int64(2400), p.GetCostPerUnitInCents())
	require.Equal(t, "USD", p.GetCurrency())
	require.Equal(t,
		[]string{"ent-enterprise-member", "ent-enterprise-admin"},
		p.GetEntitlementIds(),
	)
}

// TestNewLicenseTrait_CurrencyDefaultsToUSD confirms that when a cost is
// set without a currency, the trait fills in USD on construction.
func TestNewLicenseTrait_CurrencyDefaultsToUSD(t *testing.T) {
	trait, err := NewLicenseTrait(
		WithLicenseCost(1000, ""),
	)
	require.NoError(t, err)
	require.Equal(t, "USD", trait.GetProfile().GetCurrency())
}

// TestNewLicenseTrait_NoDefaultCurrencyWithoutProfile confirms that when
// no options touch the profile, the constructor does not synthesize one
// just to set a currency default.
func TestNewLicenseTrait_NoDefaultCurrencyWithoutProfile(t *testing.T) {
	trait, err := NewLicenseTrait()
	require.NoError(t, err)
	require.Nil(t, trait.GetProfile())
}

// TestWithLicenseSeats_ZeroValuesAccepted documents the spec: zero means
// "not available from the vendor" and is a valid value.
func TestWithLicenseSeats_ZeroValuesAccepted(t *testing.T) {
	trait, err := NewLicenseTrait(
		WithLicenseName("Free Tier"),
		WithLicenseSeats(0, 0),
	)
	require.NoError(t, err)
	require.Equal(t, int64(0), trait.GetProfile().GetPurchasedSeats())
	require.Equal(t, int64(0), trait.GetProfile().GetConsumedSeats())
}

// TestWithLicenseTrait_AppliesToResource verifies the ResourceOption
// attaches the trait to a resource's annotations and that GetLicenseTrait
// round-trips it.
func TestWithLicenseTrait_AppliesToResource(t *testing.T) {
	rt := &v2.ResourceType{}
	rt.SetId("license")
	rt.SetTraits([]v2.ResourceType_Trait{v2.ResourceType_TRAIT_LICENSE})

	r, err := NewResource(
		"Enterprise",
		rt,
		"license-enterprise",
		WithLicenseTrait(
			WithLicenseName("Enterprise"),
			WithLicenseSeats(200, 175),
			WithLicenseCost(2400, "USD"),
			WithLicenseEntitlementIDs("ent-enterprise-member"),
		),
	)
	require.NoError(t, err)

	got, err := GetLicenseTrait(r)
	require.NoError(t, err)
	require.Equal(t, "Enterprise", got.GetProfile().GetLicenseName())
	require.Equal(t, int64(200), got.GetProfile().GetPurchasedSeats())
	require.Equal(t, int64(175), got.GetProfile().GetConsumedSeats())
	require.Equal(t, int64(2400), got.GetProfile().GetCostPerUnitInCents())
	require.Equal(t, "USD", got.GetProfile().GetCurrency())
	require.Equal(t, []string{"ent-enterprise-member"}, got.GetProfile().GetEntitlementIds())
}

// TestWithLicenseTrait_UpdatesExistingAnnotation confirms merge semantics
// on subsequent applications: each option overwrites only its field, and
// repeated fields are replaced wholesale.
func TestWithLicenseTrait_UpdatesExistingAnnotation(t *testing.T) {
	rt := &v2.ResourceType{}
	rt.SetId("license")
	rt.SetTraits([]v2.ResourceType_Trait{v2.ResourceType_TRAIT_LICENSE})

	r, err := NewResource(
		"Enterprise",
		rt,
		"license-enterprise",
		WithLicenseTrait(
			WithLicenseName("Enterprise"),
			WithLicenseSeats(100, 50),
			WithLicenseEntitlementIDs("ent-a", "ent-b"),
		),
	)
	require.NoError(t, err)

	require.NoError(t, WithLicenseTrait(
		WithLicenseSeats(200, 175),
		WithLicenseEntitlementIDs("ent-c"),
	)(r))

	got, err := GetLicenseTrait(r)
	require.NoError(t, err)
	require.Equal(t, "Enterprise", got.GetProfile().GetLicenseName())
	require.Equal(t, int64(200), got.GetProfile().GetPurchasedSeats())
	require.Equal(t, int64(175), got.GetProfile().GetConsumedSeats())
	require.Equal(t, []string{"ent-c"}, got.GetProfile().GetEntitlementIds())
}

// TestGetLicenseTrait_NotPresent verifies the read helper surfaces a clear
// error when the trait is missing.
func TestGetLicenseTrait_NotPresent(t *testing.T) {
	rt := &v2.ResourceType{}
	rt.SetId("user")
	rt.SetTraits([]v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER})

	r, err := NewResource("Alice", rt, "alice-id")
	require.NoError(t, err)

	_, err = GetLicenseTrait(r)
	require.Error(t, err)
	require.ErrorContains(t, err, "license trait was not found")
}

// TestIsLicenseResource verifies the trait check.
func TestIsLicenseResource(t *testing.T) {
	t.Run("with trait", func(t *testing.T) {
		rt := &v2.ResourceType{}
		rt.SetTraits([]v2.ResourceType_Trait{v2.ResourceType_TRAIT_LICENSE})
		require.True(t, IsLicenseResource(rt))
	})

	t.Run("without trait", func(t *testing.T) {
		rt := &v2.ResourceType{}
		rt.SetTraits([]v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER})
		require.False(t, IsLicenseResource(rt))
	})

	t.Run("nil resource type", func(t *testing.T) {
		require.False(t, IsLicenseResource(nil))
	})

	t.Run("multiple traits including license", func(t *testing.T) {
		rt := &v2.ResourceType{}
		rt.SetTraits([]v2.ResourceType_Trait{
			v2.ResourceType_TRAIT_GROUP,
			v2.ResourceType_TRAIT_LICENSE,
		})
		require.True(t, IsLicenseResource(rt))
	})
}

// TestLicenseProfile_Validate exercises the proto-generated Validate method
// directly. Pins the proto-level rules so future edits that loosen them are
// caught in CI.
func TestLicenseProfile_Validate(t *testing.T) {
	validProfile := func(mut func(*v2.LicenseProfile)) *v2.LicenseProfile {
		p := &v2.LicenseProfile{}
		p.SetLicenseName("Enterprise")
		p.SetCurrency("USD")
		if mut != nil {
			mut(p)
		}
		return p
	}

	cases := []struct {
		name    string
		profile *v2.LicenseProfile
		wantErr string
	}{
		{
			name:    "happy path",
			profile: validProfile(nil),
		},
		{
			name: "empty currency accepted (ignore_empty)",
			profile: validProfile(func(p *v2.LicenseProfile) {
				p.SetCurrency("")
			}),
		},
		{
			name: "currency too long rejected",
			profile: validProfile(func(p *v2.LicenseProfile) {
				p.SetCurrency("VERYLONGCURRENCY")
			}),
			wantErr: "Currency",
		},
		{
			name: "duplicate entitlement_ids rejected",
			profile: validProfile(func(p *v2.LicenseProfile) {
				p.SetEntitlementIds([]string{"ent-a", "ent-a"})
			}),
			wantErr: "EntitlementIds",
		},
		{
			name: "negative seats accepted (proto has no rule)",
			profile: validProfile(func(p *v2.LicenseProfile) {
				p.SetPurchasedSeats(-1)
				p.SetConsumedSeats(-1)
			}),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.profile.Validate()
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.wantErr)
		})
	}
}
