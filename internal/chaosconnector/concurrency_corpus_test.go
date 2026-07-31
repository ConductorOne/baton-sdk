package chaosconnector

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConcurrentDuplicateCorpusCoversBothOrders(t *testing.T) {
	corpus := ConcurrentDuplicateCorpus()
	require.Len(t, corpus, 2)
	require.ElementsMatch(t, []string{"left", "right"}, []string{
		corpus[0].BlockedToken,
		corpus[1].BlockedToken,
	})

	scenario, err := NewConcurrentDuplicateScenario()
	require.NoError(t, err)
	require.NoError(t, scenario.Validate())
	for _, corpusCase := range corpus {
		require.NotEqual(t, corpusCase.BlockedToken, corpusCase.FirstToken)
		require.NotEmpty(t, corpusCase.ExpectedName)
		require.NotEmpty(t, corpusCase.ResumeExpectedName)
		require.NoError(t, corpusCase.Schedule.Validate())
		require.NoError(t, corpusCase.CrashSchedule.Validate())
	}
}
