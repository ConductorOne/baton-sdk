package chaosconnector

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConcurrentDuplicateCorpusCoversBothOrders(t *testing.T) {
	corpus := ConcurrentDuplicateCorpus()
	require.Len(t, corpus, 6)
	ordersByEntity := make(map[ReferentialEntity][]string)
	for _, corpusCase := range corpus {
		ordersByEntity[corpusCase.Entity] = append(
			ordersByEntity[corpusCase.Entity],
			corpusCase.BlockedToken,
		)
		require.NotEqual(t, corpusCase.BlockedToken, corpusCase.FirstToken)
		require.NotEmpty(t, corpusCase.Method())
		require.NotEmpty(t, corpusCase.Expectation(corpusCase.BlockedToken).CanonicalIdentity)
		require.NoError(t, corpusCase.Schedule.Validate())
		require.NoError(t, corpusCase.CrashSchedule.Validate())

		scenario, err := NewConcurrentDuplicateScenario(corpusCase.Entity)
		require.NoError(t, err)
		require.NoError(t, scenario.Validate())
	}
	for _, entity := range []ReferentialEntity{
		ReferentialResource,
		ReferentialEntitlement,
		ReferentialGrant,
	} {
		require.ElementsMatch(t, []string{"left", "right"}, ordersByEntity[entity])
	}
}
