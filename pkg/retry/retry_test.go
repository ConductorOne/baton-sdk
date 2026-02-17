package retry

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestBasicRetry(t *testing.T) {
	ctx := t.Context()
	retryer := NewRetryer(ctx, RetryConfig{
		MaxAttempts:  3,
		InitialDelay: 100 * time.Millisecond,
		MaxDelay:     1 * time.Second,
	})

	shouldRetry := retryer.ShouldWaitAndRetry(ctx, errors.New("generic unrecoverable error"))
	require.False(t, shouldRetry, "generic unrecoverable error should not be retried")

	shouldRetry = retryer.ShouldWaitAndRetry(ctx, status.Error(codes.Unavailable, "recoverable error"))
	require.True(t, shouldRetry, "recoverable error should be retried")

	shouldRetry = retryer.ShouldWaitAndRetry(ctx, status.Error(codes.Unknown, "unknown error"))
	require.False(t, shouldRetry, "unknown error should not be retried")

	// This has the side effect of resetting attempts to 0.
	shouldRetry = retryer.ShouldWaitAndRetry(ctx, nil)
	require.True(t, shouldRetry, "nil error should be retried")

	shouldRetry = retryer.ShouldWaitAndRetry(ctx, status.Error(codes.Unavailable, "first attempt"))
	require.True(t, shouldRetry, "first attempt should be retried")

	startTime := time.Now()
	shouldRetry = retryer.ShouldWaitAndRetry(ctx, status.Error(codes.Unavailable, "second attempt"))
	require.True(t, shouldRetry, "second attempt should be retried")
	elapsed := time.Since(startTime)
	require.Greater(t, elapsed, 100*time.Millisecond, "second attempt should take longer than 100ms")
	require.Less(t, elapsed, 300*time.Millisecond, "second attempt should take less than 300ms")

	shouldRetry = retryer.ShouldWaitAndRetry(ctx, status.Error(codes.Unavailable, "third attempt"))
	require.True(t, shouldRetry, "third attempt should be retried")

	shouldRetry = retryer.ShouldWaitAndRetry(ctx, status.Error(codes.Unavailable, "fourth attempt"))
	require.False(t, shouldRetry, "fourth attempt should not be retried")
}

func TestRetryWithRateLimitData(t *testing.T) {
	ctx := t.Context()
	retryer := NewRetryer(ctx, RetryConfig{
		MaxAttempts:  3,
		InitialDelay: 100 * time.Millisecond,
		MaxDelay:     2 * time.Second,
	})

	// Create a rate limit error with reset at 5 seconds from now and limit of 100
	resetAt := time.Now().Add(5 * time.Second)
	rlData := &v2.RateLimitDescription{
		Limit:     100,
		Remaining: 100,
		ResetAt:   timestamppb.New(resetAt),
	}

	st := status.New(codes.Unavailable, "rate limited")
	st, err := st.WithDetails(rlData)
	require.NoError(t, err)

	startTime := time.Now()
	shouldRetry := retryer.ShouldWaitAndRetry(ctx, st.Err())
	elapsed := time.Since(startTime)

	require.True(t, shouldRetry, "rate limited request should be retried")
	// Expected wait: 5s / 100 = 50ms, rounded up to 1s
	t.Logf("Actual wait time: %v (expected ~1s)", elapsed)
	require.GreaterOrEqual(t, elapsed, 1*time.Second, "should wait at least 1 second based on rate limit data")
	require.Less(t, elapsed, 1500*time.Millisecond, "should wait approximately 1 second")
}

func TestRetryWithHTTPResponse(t *testing.T) {
	tests := []struct {
		name             string
		statusCode       int
		headers          map[string][]string
		expectedRetry    bool
		expectedMinWait  time.Duration
		expectedMaxWait  time.Duration
		buildRateLimitFn func(*http.Response) error
	}{
		{
			name:       "429 with rate limit headers",
			statusCode: http.StatusTooManyRequests,
			headers: map[string][]string{
				"X-Ratelimit-Limit":     {"100"},
				"X-Ratelimit-Remaining": {"0"},
				"X-Ratelimit-Reset":     {"3"},
			},
			expectedRetry:   true,
			expectedMinWait: 1 * time.Second,
			expectedMaxWait: 3 * time.Second,
			buildRateLimitFn: func(resp *http.Response) error {
				// Simulate what uhttp does - attach rate limit data to error
				resetAt := time.Now().Add(3 * time.Second)
				rlData := &v2.RateLimitDescription{
					Limit:     100,
					Remaining: 0,
					ResetAt:   timestamppb.New(resetAt),
				}
				st := status.New(codes.Unavailable, "rate limited")
				st, err := st.WithDetails(rlData)
				if err != nil {
					return err
				}
				return st.Err()
			},
		},
		{
			name:       "503 Service Unavailable with small Retry-After",
			statusCode: http.StatusServiceUnavailable,
			headers: map[string][]string{
				"Retry-After": {"1"},
			},
			expectedRetry:   true,
			expectedMinWait: 1 * time.Second,
			expectedMaxWait: 2 * time.Second,
			buildRateLimitFn: func(resp *http.Response) error {
				resetAt := time.Now().Add(1 * time.Second)
				rlData := &v2.RateLimitDescription{
					Limit:     1,
					Remaining: 0,
					ResetAt:   timestamppb.New(resetAt),
				}
				st := status.New(codes.Unavailable, "service unavailable")
				st, err := st.WithDetails(rlData)
				if err != nil {
					return err
				}
				return st.Err()
			},
		},
		{
			name:       "408 Request Timeout",
			statusCode: http.StatusRequestTimeout,
			headers:    map[string][]string{},
			buildRateLimitFn: func(resp *http.Response) error {
				return status.Error(codes.DeadlineExceeded, "request timeout")
			},
			expectedRetry:   true,
			expectedMinWait: 100 * time.Millisecond,
			expectedMaxWait: 300 * time.Millisecond,
		},
		{
			name:       "404 Not Found - should not retry",
			statusCode: http.StatusNotFound,
			headers:    map[string][]string{},
			buildRateLimitFn: func(resp *http.Response) error {
				return status.Error(codes.NotFound, "not found")
			},
			expectedRetry: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			retryer := NewRetryer(ctx, RetryConfig{
				MaxAttempts:  3,
				InitialDelay: 100 * time.Millisecond,
				MaxDelay:     2 * time.Second,
			})

			resp := &http.Response{
				StatusCode: tt.statusCode,
				Status:     http.StatusText(tt.statusCode),
				Header:     tt.headers,
			}

			err := tt.buildRateLimitFn(resp)
			require.NotNil(t, err)

			startTime := time.Now()
			shouldRetry := retryer.ShouldWaitAndRetry(ctx, err)
			elapsed := time.Since(startTime)

			require.Equal(t, tt.expectedRetry, shouldRetry, "retry decision should match expected")

			if tt.expectedRetry {
				t.Logf("Actual wait time: %v (expected between %v and %v)", elapsed, tt.expectedMinWait, tt.expectedMaxWait)
				if tt.expectedMinWait > 0 {
					require.GreaterOrEqual(t, elapsed, tt.expectedMinWait, "wait time should be at least minimum")
				}
				if tt.expectedMaxWait > 0 {
					require.LessOrEqual(t, elapsed, tt.expectedMaxWait, "wait time should not exceed maximum")
				}
			} else {
				t.Logf("No retry as expected, elapsed time: %v", elapsed)
			}
		})
	}
}

func TestRetryContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	retryer := NewRetryer(ctx, RetryConfig{
		MaxAttempts:  5,
		InitialDelay: 2 * time.Second,
		MaxDelay:     10 * time.Second,
	})

	// Cancel context after 100ms
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	err := status.Error(codes.Unavailable, "test error")
	startTime := time.Now()
	shouldRetry := retryer.ShouldWaitAndRetry(ctx, err)
	elapsed := time.Since(startTime)

	t.Logf("Context cancellation response time: %v (should be ~100ms)", elapsed)
	require.False(t, shouldRetry, "should not retry when context is cancelled")
	require.GreaterOrEqual(t, elapsed, 100*time.Millisecond, "should wait at least until cancellation")
	require.Less(t, elapsed, 500*time.Millisecond, "should return quickly on context cancellation")
}

func TestRetryWaitTimeCalculations(t *testing.T) {
	tests := []struct {
		name            string
		resetAfter      time.Duration
		remaining       int64
		maxDelay        time.Duration
		expectedMinWait time.Duration
		expectedMaxWait time.Duration
	}{
		{
			name:            "Short reset with high limit - rounds up to 1s",
			resetAfter:      5 * time.Second,
			remaining:       100,
			maxDelay:        10 * time.Second,
			expectedMinWait: 1 * time.Second,
			expectedMaxWait: 1500 * time.Millisecond,
		},
		{
			name:            "Longer reset with lower limit - rounds up to 1s",
			resetAfter:      10 * time.Second,
			remaining:       50,
			maxDelay:        10 * time.Second,
			expectedMinWait: 1 * time.Second,
			expectedMaxWait: 1500 * time.Millisecond,
		},
		{
			name:            "Very short reset with limit 1",
			resetAfter:      500 * time.Millisecond,
			remaining:       1,
			maxDelay:        10 * time.Second,
			expectedMinWait: 1 * time.Second,
			expectedMaxWait: 1500 * time.Millisecond,
		},
		{
			name:            "Long reset capped by maxDelay",
			resetAfter:      100 * time.Second,
			remaining:       1,
			maxDelay:        2 * time.Second,
			expectedMinWait: 2 * time.Second,
			expectedMaxWait: 2500 * time.Millisecond,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			retryer := NewRetryer(ctx, RetryConfig{
				MaxAttempts:  3,
				InitialDelay: 100 * time.Millisecond,
				MaxDelay:     tt.maxDelay,
			})

			resetAt := time.Now().Add(tt.resetAfter)
			rlData := &v2.RateLimitDescription{
				Remaining: tt.remaining,
				ResetAt:   timestamppb.New(resetAt),
			}

			st := status.New(codes.Unavailable, "rate limited")
			st, err := st.WithDetails(rlData)
			require.NoError(t, err)

			startTime := time.Now()
			shouldRetry := retryer.ShouldWaitAndRetry(ctx, st.Err())
			elapsed := time.Since(startTime)

			require.True(t, shouldRetry)
			t.Logf("Reset: %v, Remaining: %d, MaxDelay: %v → Actual wait: %v (expected %v-%v)",
				tt.resetAfter, tt.remaining, tt.maxDelay, elapsed, tt.expectedMinWait, tt.expectedMaxWait)
			require.GreaterOrEqual(t, elapsed, tt.expectedMinWait, "wait time too short")
			require.LessOrEqual(t, elapsed, tt.expectedMaxWait, "wait time too long")
		})
	}
}

func TestRetryWithGitHubRateLimitHeaders(t *testing.T) {
	tests := []struct {
		name            string
		limit           int64
		remaining       int64
		resetAfter      time.Duration
		maxDelay        time.Duration
		expectedRetry   bool
		expectedMinWait time.Duration
		expectedMaxWait time.Duration
		description     string
	}{
		{
			name:            "GitHub typical rate limit - 5000 limit with 1 hour reset",
			limit:           5000,
			remaining:       5000,
			resetAfter:      3600 * time.Second, // 1 hour
			maxDelay:        60 * time.Second,
			expectedRetry:   true,
			expectedMinWait: 1 * time.Second,
			expectedMaxWait: 1500 * time.Millisecond,
			description:     "Should calculate (3600s / 5000 = 0.72s) rounded up to 1s, capped by maxDelay",
		},
		{
			name:            "GitHub secondary rate limit - 5000 limit with 30 min reset",
			limit:           5000,
			remaining:       5000,
			resetAfter:      1800 * time.Second, // 30 minutes
			maxDelay:        60 * time.Second,
			expectedRetry:   true,
			expectedMinWait: 1 * time.Second,
			expectedMaxWait: 1500 * time.Millisecond,
			description:     "Should calculate (1800s / 5000 = 0.36s) rounded up to 1s",
		},
		{
			name:            "GitHub abuse detection - 1000 limit with 1 min reset",
			limit:           1000,
			remaining:       1000,
			resetAfter:      60 * time.Second,
			maxDelay:        2 * time.Second,
			expectedRetry:   true,
			expectedMinWait: 1 * time.Second,
			expectedMaxWait: 1500 * time.Millisecond,
			description:     "Should calculate (60s / 1000 = 0.06s) rounded up to 1s",
		},
		{
			name:            "GitHub search API - 30 requests per minute",
			limit:           30,
			remaining:       30,
			resetAfter:      60 * time.Second,
			maxDelay:        10 * time.Second,
			expectedRetry:   true,
			expectedMinWait: 2 * time.Second,
			expectedMaxWait: 2500 * time.Millisecond,
			description:     "Should calculate (60s / 30 = 2s), no rounding needed",
		},
		{
			name:            "GitHub GraphQL - 5000 points per hour",
			limit:           5000,
			remaining:       5000,
			resetAfter:      3600 * time.Second,
			maxDelay:        30 * time.Second,
			expectedRetry:   true,
			expectedMinWait: 1 * time.Second,
			expectedMaxWait: 1500 * time.Millisecond,
			description:     "Should calculate (3600s / 5000 = 0.72s) rounded up to 1s",
		},
		{
			name:            "GitHub Enterprise - custom limit 10000",
			limit:           10000,
			remaining:       10000,
			resetAfter:      3600 * time.Second,
			maxDelay:        60 * time.Second,
			expectedRetry:   true,
			expectedMinWait: 1 * time.Second,
			expectedMaxWait: 1500 * time.Millisecond,
			description:     "Should calculate (3600s / 10000 = 0.36s) rounded up to 1s",
		},
		{
			name:            "GitHub near reset - 5 seconds remaining",
			limit:           5000,
			remaining:       5000,
			resetAfter:      5 * time.Second,
			maxDelay:        60 * time.Second,
			expectedRetry:   true,
			expectedMinWait: 1 * time.Second,
			expectedMaxWait: 1500 * time.Millisecond,
			description:     "Should calculate (5s / 5000 = 0.001s) rounded up to 1s",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			retryer := NewRetryer(ctx, RetryConfig{
				MaxAttempts:  3,
				InitialDelay: 100 * time.Millisecond,
				MaxDelay:     tt.maxDelay,
			})

			// Simulate GitHub rate limit response
			resetAt := time.Now().Add(tt.resetAfter)
			rlData := &v2.RateLimitDescription{
				Limit:     tt.limit,
				Remaining: tt.remaining,
				ResetAt:   timestamppb.New(resetAt),
				Status:    v2.RateLimitDescription_STATUS_OVERLIMIT,
			}

			st := status.New(codes.Unavailable, "GitHub rate limit exceeded")
			st, err := st.WithDetails(rlData)
			require.NoError(t, err)

			t.Logf("Testing: %s", tt.description)
			t.Logf("Headers: X-Ratelimit-Limit=%d, X-Ratelimit-Remaining=%d, X-Ratelimit-Reset=%v",
				tt.limit, tt.remaining, resetAt.Unix())

			startTime := time.Now()
			shouldRetry := retryer.ShouldWaitAndRetry(ctx, st.Err())
			elapsed := time.Since(startTime)

			require.Equal(t, tt.expectedRetry, shouldRetry, "retry decision should match expected")

			if tt.expectedRetry {
				calculatedWait := tt.resetAfter / time.Duration(tt.remaining)
				roundedWait := time.Second * time.Duration(int64((calculatedWait.Seconds() + 0.999)))
				if roundedWait > tt.maxDelay {
					roundedWait = tt.maxDelay
				}

				t.Logf("Calculated wait: %v (from %v / %d)", calculatedWait, tt.resetAfter, tt.remaining)
				t.Logf("Rounded wait: %v", roundedWait)
				t.Logf("Actual wait: %v (expected %v-%v)", elapsed, tt.expectedMinWait, tt.expectedMaxWait)

				require.GreaterOrEqual(t, elapsed, tt.expectedMinWait, "wait time should be at least minimum")
				require.LessOrEqual(t, elapsed, tt.expectedMaxWait, "wait time should not exceed maximum")
			}
		})
	}
}
