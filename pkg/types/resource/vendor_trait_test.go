package resource

import (
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/stretchr/testify/require"
)

// TestNewVendorTrait_RequiresVendorIdentity verifies that the constructor
// enforces the two required fields (vendor_id, vendor_name).
func TestNewVendorTrait_RequiresVendorIdentity(t *testing.T) {
	t.Run("no options", func(t *testing.T) {
		_, err := NewVendorTrait()
		require.Error(t, err)
		require.ErrorContains(t, err, "vendor_id must be set")
	})

	t.Run("empty vendor_id", func(t *testing.T) {
		_, err := NewVendorTrait(WithVendorIdentity("", "Acme", ""))
		require.Error(t, err)
		require.ErrorContains(t, err, "vendor_id cannot be empty")
	})

	t.Run("empty vendor_name", func(t *testing.T) {
		_, err := NewVendorTrait(WithVendorIdentity("vendor-uuid", "", ""))
		require.Error(t, err)
		require.ErrorContains(t, err, "vendor_name cannot be empty")
	})

	t.Run("only required fields succeeds", func(t *testing.T) {
		trait, err := NewVendorTrait(
			WithVendorIdentity("vendor-uuid", "Acme Software, Inc.", ""),
		)
		require.NoError(t, err)
		require.Equal(t, "vendor-uuid", trait.GetVendorId())
		require.Equal(t, "Acme Software, Inc.", trait.GetVendorName())
		require.Empty(t, trait.GetVendorDbaName())
		require.Empty(t, trait.GetWebsiteDomain())
	})
}

// TestNewVendorTrait_FullPopulation exercises every option setter to
// confirm fields round-trip through the proto correctly.
func TestNewVendorTrait_FullPopulation(t *testing.T) {
	trait, err := NewVendorTrait(
		WithVendorIdentity("vendor-uuid", "Acme Software, Inc.", "Acme"),
		WithVendorWebsite("example.com"),
		WithExternalVendorID("acme-prod"),
		WithVendorDeepLinkURL("https://example.com/vendors/v123"),
		WithVendorSourceScoping("source-business-id", "source-entity-id"),
		WithTrailing30DaySpend(NewMoney(400_000, "USD")),
		WithTrailing365DaySpend(NewMoney(4_800_000, "USD")),
		WithYTDSpend(NewMoney(400_000, "USD")),
	)
	require.NoError(t, err)

	require.Equal(t, "vendor-uuid", trait.GetVendorId())
	require.Equal(t, "Acme Software, Inc.", trait.GetVendorName())
	require.Equal(t, "Acme", trait.GetVendorDbaName())
	require.Equal(t, "example.com", trait.GetWebsiteDomain())
	require.Equal(t, "acme-prod", trait.GetExternalVendorId())
	require.Equal(t, "https://example.com/vendors/v123", trait.GetDeepLinkUrl())
	require.Equal(t, "source-business-id", trait.GetSourceBusinessId())
	require.Equal(t, "source-entity-id", trait.GetSourceEntityId())
	require.Equal(t, int64(400_000), trait.GetTrailing_30DSpend().GetAmountMinor())
	require.Equal(t, int64(4_800_000), trait.GetTrailing_365DSpend().GetAmountMinor())
	require.Equal(t, int64(400_000), trait.GetYtdSpend().GetAmountMinor())
}

// TestVendorTrait_SpendWindowsIndependent verifies each spend-window
// option sets only its own field. Spend lives on VendorTrait because spend
// is a property of the vendor (not of any individual agreement).
func TestVendorTrait_SpendWindowsIndependent(t *testing.T) {
	t.Run("only 30d", func(t *testing.T) {
		trait, err := NewVendorTrait(
			WithVendorIdentity("vendor-uuid", "Acme", ""),
			WithTrailing30DaySpend(NewMoney(400_000, "USD")),
		)
		require.NoError(t, err)
		require.NotNil(t, trait.GetTrailing_30DSpend())
		require.Nil(t, trait.GetTrailing_365DSpend())
		require.Nil(t, trait.GetYtdSpend())
	})

	t.Run("only ytd", func(t *testing.T) {
		trait, err := NewVendorTrait(
			WithVendorIdentity("vendor-uuid", "Acme", ""),
			WithYTDSpend(NewMoney(12_345, "USD")),
		)
		require.NoError(t, err)
		require.Nil(t, trait.GetTrailing_30DSpend())
		require.Nil(t, trait.GetTrailing_365DSpend())
		require.Equal(t, int64(12_345), trait.GetYtdSpend().GetAmountMinor())
	})

	t.Run("all three", func(t *testing.T) {
		trait, err := NewVendorTrait(
			WithVendorIdentity("vendor-uuid", "Acme", ""),
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

// TestWithVendorTrait_AppliesToResource verifies the ResourceOption
// attaches the trait to a resource's annotations and that GetVendorTrait
// round-trips it.
func TestWithVendorTrait_AppliesToResource(t *testing.T) {
	rt := &v2.ResourceType{}
	rt.SetId("vendor")
	rt.SetTraits([]v2.ResourceType_Trait{v2.ResourceType_TRAIT_VENDOR})

	r, err := NewResource(
		"Acme Software, Inc.",
		rt,
		"vendor-uuid",
		WithVendorTrait(
			WithVendorIdentity("vendor-uuid", "Acme Software, Inc.", "Acme"),
			WithVendorWebsite("example.com"),
			WithVendorDeepLinkURL("https://example.com/vendors/v123"),
		),
	)
	require.NoError(t, err)

	got, err := GetVendorTrait(r)
	require.NoError(t, err)
	require.Equal(t, "vendor-uuid", got.GetVendorId())
	require.Equal(t, "Acme Software, Inc.", got.GetVendorName())
	require.Equal(t, "Acme", got.GetVendorDbaName())
	require.Equal(t, "example.com", got.GetWebsiteDomain())
	require.Equal(t, "https://example.com/vendors/v123", got.GetDeepLinkUrl())
}

// TestWithVendorTrait_UpdatesExistingAnnotation confirms merge semantics:
// applying the option a second time updates fields rather than replacing
// the whole trait.
func TestWithVendorTrait_UpdatesExistingAnnotation(t *testing.T) {
	rt := &v2.ResourceType{}
	rt.SetId("vendor")
	rt.SetTraits([]v2.ResourceType_Trait{v2.ResourceType_TRAIT_VENDOR})

	r, err := NewResource(
		"Acme",
		rt,
		"vendor-uuid",
		WithVendorTrait(
			WithVendorIdentity("vendor-uuid", "Acme Software, Inc.", "Acme"),
		),
	)
	require.NoError(t, err)

	require.NoError(t, WithVendorTrait(
		WithVendorIdentity("vendor-uuid", "Acme Software, Inc.", "Acme"),
		WithVendorDeepLinkURL("https://example.com/vendors/v123"),
	)(r))

	got, err := GetVendorTrait(r)
	require.NoError(t, err)
	require.Equal(t, "vendor-uuid", got.GetVendorId())
	require.Equal(t, "https://example.com/vendors/v123", got.GetDeepLinkUrl())
}

// TestGetVendorTrait_NotPresent verifies the read helper surfaces a
// clear error when the trait is missing.
func TestGetVendorTrait_NotPresent(t *testing.T) {
	rt := &v2.ResourceType{}
	rt.SetId("user")
	rt.SetTraits([]v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER})

	r, err := NewResource("Alice", rt, "alice-id")
	require.NoError(t, err)

	_, err = GetVendorTrait(r)
	require.Error(t, err)
	require.ErrorContains(t, err, "vendor trait was not found")
}

// TestIsVendorResource verifies the trait check.
func TestIsVendorResource(t *testing.T) {
	t.Run("with trait", func(t *testing.T) {
		rt := &v2.ResourceType{}
		rt.SetTraits([]v2.ResourceType_Trait{v2.ResourceType_TRAIT_VENDOR})
		require.True(t, IsVendorResource(rt))
	})

	t.Run("without trait", func(t *testing.T) {
		rt := &v2.ResourceType{}
		rt.SetTraits([]v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER})
		require.False(t, IsVendorResource(rt))
	})

	t.Run("with multiple traits including vendor", func(t *testing.T) {
		rt := &v2.ResourceType{}
		rt.SetTraits([]v2.ResourceType_Trait{
			v2.ResourceType_TRAIT_VENDOR,
			v2.ResourceType_TRAIT_VENDOR_AGREEMENT,
		})
		require.True(t, IsVendorResource(rt))
	})

	t.Run("nil resource type", func(t *testing.T) {
		require.False(t, IsVendorResource(nil))
	})
}

// TestVendorTrait_Validate exercises the proto-generated Validate method
// directly, pinning the proto-level validation rules.
func TestVendorTrait_Validate(t *testing.T) {
	validTrait := func(mut func(*v2.VendorTrait)) *v2.VendorTrait {
		t := &v2.VendorTrait{}
		t.SetVendorId("v1")
		t.SetVendorName("Acme")
		if mut != nil {
			mut(t)
		}
		return t
	}

	cases := []struct {
		name    string
		trait   *v2.VendorTrait
		wantErr string
	}{
		{
			name:  "happy path (only required)",
			trait: validTrait(nil),
		},
		{
			name:  "empty vendor_id rejected",
			trait: &v2.VendorTrait{},
			// Bare construction: vendor_id empty AND vendor_name empty.
			// The first failure surfaces VendorId.
			wantErr: "VendorId",
		},
		{
			name: "deep_link_url: http rejected",
			trait: validTrait(func(t *v2.VendorTrait) {
				t.SetDeepLinkUrl("http://example.com/x")
			}),
			wantErr: "DeepLinkUrl",
		},
		{
			name: "deep_link_url: https accepted",
			trait: validTrait(func(t *v2.VendorTrait) {
				t.SetDeepLinkUrl("https://example.com/x")
			}),
		},
		{
			name: "deep_link_url: empty accepted",
			trait: validTrait(func(t *v2.VendorTrait) {
				t.SetDeepLinkUrl("")
			}),
		},
		{
			name: "all optional fields populated",
			trait: validTrait(func(t *v2.VendorTrait) {
				t.SetVendorDbaName("Acme")
				t.SetWebsiteDomain("example.com")
				t.SetExternalVendorId("acme-prod")
				t.SetDeepLinkUrl("https://example.com/vendors/v123")
				t.SetSourceBusinessId("biz-1")
				t.SetSourceEntityId("ent-1")
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

// TestVendorTrait_CoexistsWithVendorAgreementTrait confirms a single
// resource can carry both VendorTrait and VendorAgreementTrait — the
// canonical case for sources that emit agreements.
func TestVendorTrait_CoexistsWithVendorAgreementTrait(t *testing.T) {
	rt := &v2.ResourceType{}
	rt.SetTraits([]v2.ResourceType_Trait{
		v2.ResourceType_TRAIT_VENDOR,
		v2.ResourceType_TRAIT_VENDOR_AGREEMENT,
	})

	r, err := NewResource(
		"Annual Subscription 2026",
		rt,
		"agreement-uuid",
		WithVendorTrait(
			WithVendorIdentity("vendor-uuid", "Acme Software, Inc.", "Acme"),
			WithVendorWebsite("example.com"),
		),
		WithVendorAgreementTrait(
			WithAgreementTerm("Annual Subscription 2026",
				time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2026, 12, 31, 0, 0, 0, 0, time.UTC),
				true),
			WithTotalValue(NewMoney(4_800_000, "USD")),
		),
	)
	require.NoError(t, err)

	v, err := GetVendorTrait(r)
	require.NoError(t, err)
	require.Equal(t, "vendor-uuid", v.GetVendorId())
	require.Equal(t, "example.com", v.GetWebsiteDomain())

	a, err := GetVendorAgreementTrait(r)
	require.NoError(t, err)
	require.Equal(t, "Annual Subscription 2026", a.GetAgreementName())
	require.Equal(t, int64(4_800_000), a.GetTotalValue().GetAmountMinor())
}
