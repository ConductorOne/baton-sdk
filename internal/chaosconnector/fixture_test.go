package chaosconnector

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestGeneratedSyncScenarioIsSeedDeterministic(t *testing.T) {
	first, err := NewGeneratedSyncScenario(42)
	require.NoError(t, err)
	second, err := NewGeneratedSyncScenario(42)
	require.NoError(t, err)

	firstManifest, err := first.Manifest(first.InitialEpoch)
	require.NoError(t, err)
	secondManifest, err := second.Manifest(second.InitialEpoch)
	require.NoError(t, err)
	require.Equal(t, firstManifest.ResourceTypes, secondManifest.ResourceTypes)
	require.Equal(t, firstManifest.Resources, secondManifest.Resources)
	require.Equal(t, firstManifest.Entitlements, secondManifest.Entitlements)
	require.Equal(t, firstManifest.Grants, secondManifest.Grants)
	for id, entitlement := range firstManifest.Entitlements {
		require.True(t, proto.Equal(entitlement, secondManifest.Entitlements[id]))
	}

	require.Equal(t, GeneratedRetrySchedule(first), GeneratedRetrySchedule(second))
}

func TestGeneratedSyncScenarioVariesBySeed(t *testing.T) {
	first, err := NewGeneratedSyncScenario(1)
	require.NoError(t, err)
	second, err := NewGeneratedSyncScenario(2)
	require.NoError(t, err)
	require.NotEqual(t,
		first.Epochs[first.InitialEpoch].Entitlements[FullCapabilityResourceTypeID],
		second.Epochs[second.InitialEpoch].Entitlements[FullCapabilityResourceTypeID],
	)
}
