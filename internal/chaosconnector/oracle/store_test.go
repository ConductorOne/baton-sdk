package oracle

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/internal/chaosconnector"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

func TestIdentityOracleRejectsPlantedLossAndDuplication(t *testing.T) {
	scenario, err := chaosconnector.NewFullScenario()
	require.NoError(t, err)
	manifest, err := scenario.Manifest(scenario.InitialEpoch)
	require.NoError(t, err)
	expected := ExpectedIdentities(manifest)
	require.NotEmpty(t, expected.Resources)
	require.NotEmpty(t, expected.Entitlements)
	require.NotEmpty(t, expected.Grants)

	t.Run("control", func(t *testing.T) {
		require.NoError(t, CompareIdentities(expected, expected))
	})

	t.Run("lost resource", func(t *testing.T) {
		actual := expected
		actual.Resources = append([]string(nil), expected.Resources[1:]...)
		require.ErrorContains(t, CompareIdentities(expected, actual), "resources mismatch")
	})

	t.Run("duplicated grant", func(t *testing.T) {
		actual := expected
		actual.Grants = append(append([]string(nil), expected.Grants...), expected.Grants[0])
		require.ErrorContains(t, CompareIdentities(expected, actual), "grants mismatch")
	})
}

func TestIdentityOracleScopesDuplicateEntitlementIDsByResource(t *testing.T) {
	entitlementFor := func(resourceID string) *v2.Entitlement {
		return v2.Entitlement_builder{
			Id: "shared-public-id",
			Resource: v2.Resource_builder{
				Id: v2.ResourceId_builder{
					ResourceType: "group",
					Resource:     resourceID,
				}.Build(),
			}.Build(),
		}.Build()
	}
	left := entitlementFor("left")
	right := entitlementFor("right")
	require.NotEqual(t, entitlementKey(left), entitlementKey(right),
		"the oracle must distinguish equal public IDs on different resources")

	principal := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "user", Resource: "alice"}.Build(),
	}.Build()
	leftGrant := v2.Grant_builder{Entitlement: left, Principal: principal}.Build()
	rightGrant := v2.Grant_builder{Entitlement: right, Principal: principal}.Build()
	require.NotEqual(t, grantKey(leftGrant), grantKey(rightGrant),
		"grant identity must retain the entitlement's resource scope")
}

func TestLogicalContentOracleRejectsPlantedMutation(t *testing.T) {
	expected := LogicalContentSnapshot{
		ResourceTypes: []string{"type-content"},
		Resources:     []string{"resource-before"},
		Entitlements:  []string{"entitlement-content"},
		Grants:        []string{"grant-content"},
	}
	actual := expected
	actual.Resources = []string{"resource-after"}

	require.ErrorContains(t, CompareLogicalContent(expected, actual), "resource content mismatch")
	require.NoError(t, CompareLogicalContent(expected, expected))
}
