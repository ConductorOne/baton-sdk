package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func marshalledStatsToken(t *testing.T, stepMs map[string]time.Duration, calls map[string]time.Duration) string {
	t.Helper()
	st := newState()
	st.SetShouldSkipGrants()
	for bucket, d := range stepMs {
		st.AddStepDuration(bucket, d)
	}
	for method, d := range calls {
		st.RecordConnectorCall(method, d)
	}
	token, err := st.Marshal()
	require.NoError(t, err)
	return token
}

func TestBuildCompactedTokenFold(t *testing.T) {
	baseToken := marshalledStatsToken(t,
		map[string]time.Duration{"list-grants": 90 * time.Minute},
		map[string]time.Duration{"list-grants": 2 * time.Second},
	)
	partial1 := marshalledStatsToken(t,
		map[string]time.Duration{"list-grants": 5 * time.Minute, "rate_limit_wait": time.Minute},
		map[string]time.Duration{"list-grants": time.Second},
	)
	partial2 := marshalledStatsToken(t,
		map[string]time.Duration{"list-grants": 3 * time.Minute},
		map[string]time.Duration{"list-grants": 3 * time.Second},
	)

	token, err := BuildCompactedToken(baseToken, CompactionTokenInput{
		Mode:           "fold",
		BaseSyncID:     "base-sync",
		PartialSyncIDs: []string{"p1", "p2"},
		PartialTokens:  []string{partial1, partial2, ""},
		RecordCounts: map[string]CompactionRecordCounts{
			"grants": {Output: 100, Added: 10, Replaced: 5, Carried: 85},
		},
	})
	require.NoError(t, err)

	st := newState()
	require.NoError(t, st.Unmarshal(token))
	// The base token's resume state and stats survive the rewrite.
	require.True(t, st.ShouldSkipGrants())
	require.EqualValues(t, (90 * time.Minute).Milliseconds(), st.StepDurations()["list-grants"])
	require.EqualValues(t, 1, st.ConnectorCallStats()["list-grants"].Count)

	comp, err := CompactionStatsFromToken(token)
	require.NoError(t, err)
	require.NotNil(t, comp)
	require.Equal(t, "fold", comp.Mode)
	require.Equal(t, "base-sync", comp.StatsSyncID)
	require.Equal(t, "base-sync", comp.BaseSyncID)
	require.Equal(t, []string{"p1", "p2"}, comp.PartialSyncIDs)
	require.EqualValues(t, 2, comp.PartialCount)
	require.Equal(t, &CompactionRecordCounts{Output: 100, Added: 10, Replaced: 5, Carried: 85}, comp.RecordCounts["grants"])

	agg := comp.PartialsAggregate
	require.NotNil(t, agg)
	require.EqualValues(t, 2, agg.Executions)
	require.EqualValues(t, (8 * time.Minute).Milliseconds(), agg.StepDurationsMs["list-grants"])
	require.EqualValues(t, time.Minute.Milliseconds(), agg.StepDurationsMs["rate_limit_wait"])
	require.EqualValues(t, 2, agg.ConnectorCallStats["list-grants"].Count)
	require.EqualValues(t, 4000, agg.ConnectorCallStats["list-grants"].TotalMs)
	require.EqualValues(t, 3000, agg.ConnectorCallStats["list-grants"].MaxMs)
}

func TestBuildCompactedTokenChainedFoldPreservesOriginalAttribution(t *testing.T) {
	baseToken := marshalledStatsToken(t,
		map[string]time.Duration{"list-resources": 10 * time.Minute},
		nil,
	)
	partialToken := marshalledStatsToken(t, map[string]time.Duration{"list-resources": time.Minute}, nil)

	first, err := BuildCompactedToken(baseToken, CompactionTokenInput{
		Mode:           "fold",
		BaseSyncID:     "original-sync",
		PartialSyncIDs: []string{"p1"},
		PartialTokens:  []string{partialToken},
	})
	require.NoError(t, err)

	second, err := BuildCompactedToken(first, CompactionTokenInput{
		Mode:           "fold",
		BaseSyncID:     "first-fold-output",
		PartialSyncIDs: []string{"p2", "p3"},
		PartialTokens:  []string{partialToken, partialToken},
	})
	require.NoError(t, err)

	comp, err := CompactionStatsFromToken(second)
	require.NoError(t, err)
	require.NotNil(t, comp)
	// Timing stats still describe the original collection run.
	require.Equal(t, "original-sync", comp.StatsSyncID)
	// The immediate base of the second fold is the first fold's output.
	require.Equal(t, "first-fold-output", comp.BaseSyncID)
	require.Equal(t, []string{"p1", "p2", "p3"}, comp.PartialSyncIDs)
	require.EqualValues(t, 3, comp.PartialCount)
	require.EqualValues(t, 3, comp.PartialsAggregate.Executions)
	require.EqualValues(t, (3 * time.Minute).Milliseconds(), comp.PartialsAggregate.StepDurationsMs["list-resources"])
}

func TestBuildCompactedTokenEmptyBase(t *testing.T) {
	token, err := BuildCompactedToken("", CompactionTokenInput{
		Mode:           "overlay",
		BaseSyncID:     "base-sync",
		PartialSyncIDs: []string{"p1"},
		RecordCounts: map[string]CompactionRecordCounts{
			"grants": {Output: 42},
		},
	})
	require.NoError(t, err)

	st := newState()
	require.NoError(t, st.Unmarshal(token))
	// A rebuild output's token must not carry a pending action stack.
	require.Nil(t, st.Current())
	require.Empty(t, st.StepDurations())

	comp, err := CompactionStatsFromToken(token)
	require.NoError(t, err)
	require.NotNil(t, comp)
	require.Equal(t, "overlay", comp.Mode)
	require.Nil(t, comp.PartialsAggregate)
	require.EqualValues(t, 42, comp.RecordCounts["grants"].Output)
	require.Zero(t, comp.RecordCounts["grants"].Added)
}

func TestBuildCompactedTokenCapsPartialIDs(t *testing.T) {
	ids := make([]string, maxCompactionPartialIDs+8)
	for i := range ids {
		ids[i] = fmt.Sprintf("p%03d", i)
	}
	token, err := BuildCompactedToken("", CompactionTokenInput{
		Mode:           "fold",
		BaseSyncID:     "base",
		PartialSyncIDs: ids,
	})
	require.NoError(t, err)

	comp, err := CompactionStatsFromToken(token)
	require.NoError(t, err)
	require.Len(t, comp.PartialSyncIDs, maxCompactionPartialIDs)
	require.EqualValues(t, len(ids), comp.PartialCount)
}

func TestBuildCompactedTokenIgnoresUnparseablePartials(t *testing.T) {
	token, err := BuildCompactedToken("", CompactionTokenInput{
		Mode:           "fold",
		BaseSyncID:     "base",
		PartialSyncIDs: []string{"p1", "p2"},
		PartialTokens:  []string{"{not json", ""},
	})
	require.NoError(t, err)

	comp, err := CompactionStatsFromToken(token)
	require.NoError(t, err)
	require.Nil(t, comp.PartialsAggregate)
	require.EqualValues(t, 2, comp.PartialCount)
}

func TestCompactionSectionSurvivesTokenRoundTrips(t *testing.T) {
	token, err := BuildCompactedToken("", CompactionTokenInput{
		Mode:       "fold",
		BaseSyncID: "base",
	})
	require.NoError(t, err)

	// An expansion replay rewrites the token; provenance must survive.
	replay, err := PrepareExpansionReplayToken(token)
	require.NoError(t, err)
	comp, err := CompactionStatsFromToken(replay)
	require.NoError(t, err)
	require.NotNil(t, comp)
	require.Equal(t, "fold", comp.Mode)
	require.Equal(t, "base", comp.BaseSyncID)
}
