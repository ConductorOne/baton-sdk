package oracle

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/internal/chaosconnector"
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
