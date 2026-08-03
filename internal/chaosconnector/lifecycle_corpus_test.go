package chaosconnector

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLifecycleCorpusCoversPolicyEquivalenceClasses(t *testing.T) {
	corpus := LifecycleCorpus()
	require.Len(t, corpus, 4)

	policies := make(map[DataPolicy]bool)
	names := make(map[string]struct{}, len(corpus))
	for _, corpusCase := range corpus {
		require.NotEmpty(t, corpusCase.Name)
		require.NotNil(t, corpusCase.BuildInitial)
		require.NotNil(t, corpusCase.BuildResume)
		require.NotEmpty(t, corpusCase.InterruptSchedule.Rules)
		_, duplicate := names[corpusCase.Name]
		require.False(t, duplicate)
		names[corpusCase.Name] = struct{}{}
		policies[corpusCase.Policy] = true

		initial, err := corpusCase.BuildInitial()
		require.NoError(t, err)
		require.NoError(t, initial.Validate())
		resume, err := corpusCase.BuildResume()
		require.NoError(t, err)
		require.NoError(t, resume.Validate())
		require.NoError(t, corpusCase.InterruptSchedule.Validate())
	}

	require.True(t, policies[DataPolicySkipReport])
	require.True(t, policies[DataPolicyFail])
	require.True(t, policies[DataPolicyWarnRetain])
	require.True(t, policies[DataPolicyAccept])
}
