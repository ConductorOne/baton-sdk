package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func marshalledStatsToken(t *testing.T, stepMs map[string]time.Duration, calls map[string]time.Duration) string {
	t.Helper()
	run, stats, _ := newTestRun()
	run.setFact(factShouldSkipGrants)
	for bucket, d := range stepMs {
		stats.addStepDuration(bucket, d)
	}
	for method, d := range calls {
		stats.recordConnectorCall(method, d)
	}
	return encodeTestRun(t, run, stats)
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

	run, stats, _ := decodeTestRun(t, token)
	// Resume state survives; timings are base + partials.
	require.True(t, run.hasFact(factShouldSkipGrants))
	require.EqualValues(t, (98 * time.Minute).Milliseconds(), stats.stepDurations()["list-grants"])
	require.EqualValues(t, time.Minute.Milliseconds(), stats.stepDurations()["rate_limit_wait"])
	require.EqualValues(t, 3, stats.connectorCallStats()["list-grants"].Count)

	comp, err := CompactionStatsFromToken(token)
	require.NoError(t, err)
	require.NotNil(t, comp)
	require.Equal(t, "fold", comp.Mode)
	require.Equal(t, "base-sync", comp.StatsSyncID)
	require.Equal(t, "base-sync", comp.BaseSyncID)
	require.Equal(t, []string{"p1", "p2"}, comp.PartialSyncIDs)
	require.EqualValues(t, 2, comp.PartialCount)
	require.Equal(t, &CompactionRecordCounts{Output: 100, Added: 10, Replaced: 5, Carried: 85}, comp.RecordCounts["grants"])
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

	_, stats, _ := decodeTestRun(t, second)
	// Top-level timings accumulate across chained folds.
	require.EqualValues(t, (13 * time.Minute).Milliseconds(), stats.stepDurations()["list-resources"])

	comp, err := CompactionStatsFromToken(second)
	require.NoError(t, err)
	require.NotNil(t, comp)
	require.Equal(t, "original-sync", comp.StatsSyncID)
	require.Equal(t, "first-fold-output", comp.BaseSyncID)
	require.Equal(t, []string{"p1", "p2", "p3"}, comp.PartialSyncIDs)
	require.EqualValues(t, 3, comp.PartialCount)
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

	run, stats, _ := decodeTestRun(t, token)
	// A rebuild output's token must not carry a pending action stack.
	require.Nil(t, run.current())
	require.Empty(t, stats.stepDurations())

	comp, err := CompactionStatsFromToken(token)
	require.NoError(t, err)
	require.NotNil(t, comp)
	require.Equal(t, "overlay", comp.Mode)
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

	_, stats, _ := decodeTestRun(t, token)
	require.Empty(t, stats.stepDurations())

	comp, err := CompactionStatsFromToken(token)
	require.NoError(t, err)
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
