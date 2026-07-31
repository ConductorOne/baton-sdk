package chaosconnector

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTemporalCorpusIsNamedAndReplayable(t *testing.T) {
	corpus := TemporalCorpus()
	require.Len(t, corpus, 4)

	seen := make(map[string]struct{}, len(corpus))
	for _, corpusCase := range corpus {
		require.NotEmpty(t, corpusCase.Name)
		require.NotNil(t, corpusCase.Build)
		require.NotEmpty(t, corpusCase.Expectation.CanonicalIdentity)
		_, duplicate := seen[corpusCase.Name]
		require.False(t, duplicate)
		seen[corpusCase.Name] = struct{}{}

		scenario, err := corpusCase.Build()
		require.NoError(t, err)
		require.NoError(t, scenario.Validate())
		require.NoError(t, corpusCase.Schedule.Validate())
		require.Contains(t, scenario.Epochs, retryDriftEpoch)
	}
}
