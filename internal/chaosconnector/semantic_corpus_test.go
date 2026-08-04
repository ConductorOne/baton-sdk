package chaosconnector

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSemanticCorpusIsNamedAndExecutable(t *testing.T) {
	corpus := SemanticCorpus()
	require.Len(t, corpus, 9)

	seen := make(map[string]struct{}, len(corpus))
	categories := make(map[SemanticCategory]int)
	for _, corpusCase := range corpus {
		require.NotEmpty(t, corpusCase.Name)
		require.NotEmpty(t, corpusCase.Category)
		require.NotEqual(t, DataPolicyUnresolved, corpusCase.Policy)
		require.NotNil(t, corpusCase.Apply)
		require.NotEmpty(t, corpusCase.Expectation.CanonicalIdentity)
		require.Positive(t, corpusCase.Expectation.Multiplicity)
		_, duplicate := seen[corpusCase.Name]
		require.False(t, duplicate)
		seen[corpusCase.Name] = struct{}{}
		categories[corpusCase.Category]++

		scenario, err := NewFullScenario()
		require.NoError(t, err)
		require.NoError(t, corpusCase.Apply(scenario))
		require.NoError(t, scenario.Validate())
	}

	require.Equal(t, 5, categories[SemanticDuplicate])
	require.Equal(t, 4, categories[SemanticParent])
}
