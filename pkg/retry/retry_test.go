package retry

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestBasicRetry(t *testing.T) {
	ctx := context.Background()
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
	require.False(t, shouldRetry, "third attempt should not be retried")
}
