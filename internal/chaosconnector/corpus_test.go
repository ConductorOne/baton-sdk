package chaosconnector

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInitialDataCorpusIsNamedAndPolicyBearing(t *testing.T) {
	seen := make(map[string]struct{})
	for _, corpusCase := range InitialDataCorpus() {
		require.NotEmpty(t, corpusCase.Name)
		require.NotEmpty(t, corpusCase.Class)
		require.NotEmpty(t, corpusCase.Policy)
		_, duplicate := seen[corpusCase.Name]
		require.False(t, duplicate, "corpus case names are replay identities")
		seen[corpusCase.Name] = struct{}{}
		if corpusCase.Policy != DataPolicyUnresolved {
			require.NotNil(t, corpusCase.Apply, "executable policy case must construct its premise")
		}
	}
	require.NotEmpty(t, seen)
}

func TestMissingResourceEntitlementCorpusBuildsPremise(t *testing.T) {
	scenario, err := NewFullScenario()
	require.NoError(t, err)
	corpus := InitialDataCorpus()
	require.Equal(t, "entitlement-missing-resource", corpus[0].Name)
	require.NoError(t, corpus[0].Apply(scenario))

	dataset := scenario.Epochs[scenario.InitialEpoch]
	malformed := dataset.Entitlements[FullCapabilityResourceTypeID][""].List[1]
	require.Equal(t, "chaos:malformed:no-resource", malformed.GetId())
	require.Nil(t, malformed.GetResource())

	manifest, err := scenario.Manifest(scenario.InitialEpoch)
	require.NoError(t, err)
	require.Len(t, manifest.Entitlements, 2, "manifest preserves hostile input; the oracle decides policy")
}
