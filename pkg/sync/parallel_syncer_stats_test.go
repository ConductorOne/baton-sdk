package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"context"
	native_sync "sync"
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/ratelimit"
	"github.com/conductorone/baton-sdk/pkg/retry"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
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

// TestRetryerReportsThroughWaitObserver pins the retryer's reporting channel:
// backoff sleeps reach sync stats via the context wait observer (the OnWait
// callback is gone), with the event's Retry flag routing plain backoff to
// retry_wait and rate-limited backoff to rate_limit_wait.
func TestRetryerReportsThroughWaitObserver(t *testing.T) {
	s := &syncer{
		recordStats: true,
		state:       newState(),
	}
	ctx := s.withRateLimitWaitObserver(t.Context())
	retryer := retry.NewRetryer(ctx, retry.RetryConfig{
		InitialDelay: time.Millisecond,
		MaxDelay:     time.Millisecond,
	})

	// Plain transient error: linear backoff, lands in retry_wait.
	require.True(t, retryer.ShouldWaitAndRetry(ratelimit.WithWaitLabel(ctx, "rt1"),
		status.Error(codes.Unavailable, "transient")))

	// Rate-limited error: RateLimitDescription in the error details, lands
	// in rate_limit_wait. The reset must still be in the future when the
	// retryer evaluates it; MaxDelay clamps the actual sleep to 1ms.
	st, err := status.New(codes.Unavailable, "rate limited").WithDetails(&v2.RateLimitDescription{
		Remaining: 0,
		ResetAt:   timestamppb.New(time.Now().Add(30 * time.Second)),
	})
	require.NoError(t, err)
	require.True(t, retryer.ShouldWaitAndRetry(ratelimit.WithWaitLabel(ctx, "rt1"), st.Err()))

	durations := s.state.StepDurations()
	require.Positive(t, durations["retry_wait"])
	require.Positive(t, durations["retry_wait:rt1"])
	require.Positive(t, durations["rate_limit_wait"])
	require.Positive(t, durations["rate_limit_wait:rt1"])
}

func TestWaitObserverRecordsRateLimitWait(t *testing.T) {
	s := &syncer{
		recordStats: true,
		state:       newState(),
	}

	ctx := s.withRateLimitWaitObserver(t.Context())
	// Simulate a rate-limit gate (SDK interceptor / hosted manager) sleeping
	// before a request, attributed to a resource type.
	ratelimit.ObserveWait(retry.WithWaitLabel(ctx, "repository"), ratelimit.WaitEvent{Duration: 30 * time.Second})
	ratelimit.ObserveWait(ctx, ratelimit.WaitEvent{Duration: 10 * time.Second})

	durations := s.state.StepDurations()
	require.EqualValues(t, 40000, durations["rate_limit_wait"])
	require.EqualValues(t, 30000, durations["rate_limit_wait:repository"])
}

func TestWaitObserverDisabledWithoutStats(t *testing.T) {
	s := &syncer{
		recordStats: false,
		state:       newState(),
	}

	ctx := s.withRateLimitWaitObserver(t.Context())
	// The observer is installed regardless (recordStats is only decided
	// after store load), but reports are dropped when stats are off.
	ratelimit.ObserveWait(ctx, ratelimit.WaitEvent{Duration: time.Second})
	require.Empty(t, s.state.StepDurations())
}

// TestWaitObserverBeforeStateExists covers the window at the top of Sync:
// the observer is installed (and recordStats may already be true) before the
// state token is loaded. A gate wait during the initial Validate call must be
// dropped, not panic on a nil state.
func TestWaitObserverBeforeStateExists(t *testing.T) {
	s := &syncer{
		recordStats: true,
		state:       nil,
	}

	ctx := s.withRateLimitWaitObserver(t.Context())
	require.NotPanics(t, func() {
		ratelimit.ObserveWait(ctx, ratelimit.WaitEvent{Duration: time.Second})
	})
}

func TestRecordConnectorWaitReport(t *testing.T) {
	s := &syncer{
		recordStats: true,
		state:       newState(),
	}

	var annos annotations.Annotations
	annos.WithRateLimitWaitReport(1500)

	s.recordConnectorWaitReport(annos, "repository")
	s.recordConnectorWaitReport(annos, "")

	durations := s.state.StepDurations()
	require.EqualValues(t, 3000, durations["rate_limit_wait"])
	require.EqualValues(t, 1500, durations["rate_limit_wait:repository"])

	// Responses without the annotation, zero waits, and disabled stats are no-ops.
	s.recordConnectorWaitReport(annotations.Annotations{}, "repository")
	var zeroAnnos annotations.Annotations
	zeroAnnos.WithRateLimitWaitReport(0)
	require.Empty(t, zeroAnnos)
	s.recordStats = false
	s.recordConnectorWaitReport(annos, "repository")
	require.EqualValues(t, 3000, s.state.StepDurations()["rate_limit_wait"])
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
