package chaosconnector

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReferentialCorpusIsClosedNamedAndExecutable(t *testing.T) {
	corpus := ReferentialCorpus()
	require.Len(t, corpus, 77)

	seen := make(map[string]struct{}, len(corpus))
	counts := make(map[ReferentialEntity]int)
	for _, corpusCase := range corpus {
		require.NotEmpty(t, corpusCase.Name)
		require.NotEmpty(t, corpusCase.Entity)
		require.NotEmpty(t, corpusCase.Policy)
		require.NotEqual(t, DataPolicyUnresolved, corpusCase.Policy)
		require.NotNil(t, corpusCase.Apply)
		_, duplicate := seen[corpusCase.Name]
		require.False(t, duplicate, "corpus names are replay identities")
		seen[corpusCase.Name] = struct{}{}
		counts[corpusCase.Entity]++

		scenario, err := NewFullScenario()
		require.NoError(t, err)
		require.NoError(t, corpusCase.Apply(scenario), corpusCase.Name)
		require.NoError(t, scenario.Validate(), corpusCase.Name)
	}

	require.Equal(t, 4, counts[ReferentialResource])
	require.Equal(t, 9, counts[ReferentialEntitlement])
	require.Equal(t, 64, counts[ReferentialGrant])
}

func TestGrantReferentialCorpusContainsFullCrossProduct(t *testing.T) {
	pairs := make(map[[2]ReferenceShape]struct{})
	nilCarrier := 0
	for _, corpusCase := range ReferentialCorpus() {
		if corpusCase.Entity != ReferentialGrant {
			continue
		}
		if corpusCase.Name == "grant/carrier-nil" {
			nilCarrier++
			continue
		}
		pairs[[2]ReferenceShape{
			corpusCase.EntitlementReference,
			corpusCase.PrincipalReference,
		}] = struct{}{}
	}
	require.Equal(t, 1, nilCarrier)
	require.Len(t, pairs, 9*7)
}
