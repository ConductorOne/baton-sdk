package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"context"
	native_sync "sync"
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/retry"
	"github.com/stretchr/testify/require"
)

func TestRecordRetryWaitWithResourceType(t *testing.T) {
	s := &syncer{
		recordStats: true,
		state:       newState(),
	}

	s.recordRetryWait(retry.WithWaitLabel(t.Context(), "rt1"), 2*time.Second, false)
	s.recordRetryWait(retry.WithWaitLabel(t.Context(), "rt1"), 3*time.Second, true)

	durations := s.state.StepDurations()
	require.EqualValues(t, 2000, durations["retry_wait"])
	require.EqualValues(t, 2000, durations["retry_wait:rt1"])
	require.EqualValues(t, 3000, durations["rate_limit_wait"])
	require.EqualValues(t, 3000, durations["rate_limit_wait:rt1"])
}

func TestRecordRetryWaitWithoutResourceType(t *testing.T) {
	s := &syncer{
		recordStats: true,
		state:       newState(),
	}

	s.recordRetryWait(t.Context(), time.Second, false)

	require.Equal(t, map[string]int64{"retry_wait": 1000}, s.state.StepDurations())
}

func TestObserveConnectorCallRecordsPerResourceType(t *testing.T) {
	s := &syncer{
		recordStats: true,
		state:       newState(),
	}

	s.observeConnectorCall(t.Context(), "list-grants", time.Now().Add(-10*time.Millisecond), "repo", "repo-1")
	s.observeConnectorCall(t.Context(), "list-grants", time.Now().Add(-20*time.Millisecond), "user", "user-1")
	// No resource type (e.g. list-resource-types): flat entry only.
	s.observeConnectorCall(t.Context(), "list-resource-types", time.Now(), "", "")

	stats := s.state.ConnectorCallStats()
	require.EqualValues(t, 2, stats["list-grants"].Count)
	require.EqualValues(t, 1, stats["list-grants:repo"].Count)
	require.EqualValues(t, 1, stats["list-grants:user"].Count)
	// The labeled entries decompose the flat one.
	require.Equal(t, stats["list-grants"].TotalMs, stats["list-grants:repo"].TotalMs+stats["list-grants:user"].TotalMs)
	require.EqualValues(t, 1, stats["list-resource-types"].Count)
	require.NotContains(t, stats, "list-resource-types:")
}

func TestRecordSessionUsageFoldsAnnotationIntoState(t *testing.T) {
	s := &syncer{
		recordStats: true,
		state:       newState(),
	}

	usage := v2.SessionStoreUsage_builder{
		Kind: "connector_backend",
		Ops: []*v2.SessionStoreUsage_OpStats{
			v2.SessionStoreUsage_OpStats_builder{
				Op: "get", Count: 10, Errors: 10, Timeouts: 10, TotalMs: 300_000, MaxMs: 30_000,
			}.Build(),
			v2.SessionStoreUsage_OpStats_builder{
				Op: "set", Count: 3, TotalMs: 9, MaxMs: 4,
			}.Build(),
		},
	}.Build()
	annos := annotations.New(usage)

	// Two pages' worth of reports accumulate.
	s.recordSessionUsage(annos)
	s.recordSessionUsage(annos)

	stats := s.state.SessionStoreStats()
	require.Equal(t, SessionStoreStat{Count: 20, Errors: 20, Timeouts: 20, TotalMs: 600_000, MaxMs: 30_000}, stats["connector.get"])
	require.Equal(t, SessionStoreStat{Count: 6, TotalMs: 18, MaxMs: 4}, stats["connector.set"])

	// Store-side observations land under the store. prefix.
	s.recordSessionOp("get", 2*time.Second, context.DeadlineExceeded)
	require.Equal(t, SessionStoreStat{Count: 1, Errors: 1, Timeouts: 1, TotalMs: 2000, MaxMs: 2000}, s.state.SessionStoreStats()["store.get"])

	// Empty and non-matching annotations are no-ops.
	s.recordSessionUsage(nil)
	s.recordSessionUsage(annotations.New(&v2.RateLimitDescription{}))
	require.Len(t, s.state.SessionStoreStats(), 3)

	// The gate suppresses recording entirely.
	gated := &syncer{recordStats: false, state: newState()}
	gated.recordSessionUsage(annos)
	require.Empty(t, gated.state.SessionStoreStats())
}

// TestStatsRecordingConcurrentWithMarshal exercises the parallel-worker shape:
// step durations, connector calls, and retry waits recorded from many
// goroutines while checkpoints marshal the token. Run under -race.
func TestStatsRecordingConcurrentWithMarshal(t *testing.T) {
	s := &syncer{
		recordStats: true,
		state:       newState(),
	}

	const workers = 8
	const iterations = 200

	var wg native_sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Go(func() {
			for i := 0; i < iterations; i++ {
				s.state.AddStepDuration("list-grants", time.Millisecond)
				s.state.RecordConnectorCall("list-grants", 2*time.Millisecond)
				s.recordRetryWait(retry.WithWaitLabel(t.Context(), "rt1"), time.Millisecond, i%2 == 0)
			}
		})
	}
	wg.Go(func() {
		for i := 0; i < iterations; i++ {
			_, err := s.state.Marshal()
			require.NoError(t, err)
			_ = s.state.StepDurations()
			_ = s.state.ConnectorCallStats()
		}
	})
	wg.Wait()

	durations := s.state.StepDurations()
	require.EqualValues(t, workers*iterations, durations["list-grants"])
	require.EqualValues(t, workers*iterations, durations["retry_wait:rt1"]+durations["rate_limit_wait:rt1"])
	require.EqualValues(t, workers*iterations, s.state.ConnectorCallStats()["list-grants"].Count)
}
