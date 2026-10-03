package c1api

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	sdkSync "github.com/conductorone/baton-sdk/pkg/sync"
)

// TestClassifySyncErrorInvariantVerdictNonRetryable pins the retry
// classification at the c1api layer (final-round finding: it was only
// tested at the sync layer). An ingestion-invariant DATA VERDICT is
// deterministic on the connector's dataset — the task manager must see
// ErrTaskNonRetryable through every wrap shape the production path
// produces, or C1 retries a failure that re-fails identically forever.
func TestClassifySyncErrorInvariantVerdictNonRetryable(t *testing.T) {
	verdict := fmt.Errorf("ingest invariant I5 violated: %w", sdkSync.ErrIngestInvariantViolated)

	t.Run("bare verdict maps to non-retryable", func(t *testing.T) {
		got := classifySyncError(verdict)
		require.ErrorIs(t, got, ErrTaskNonRetryable)
		require.ErrorIs(t, got, sdkSync.ErrIngestInvariantViolated, "the original verdict must survive the mapping")
	})

	t.Run("verdict wrapped by sync and joined with a close error still maps", func(t *testing.T) {
		// The production chain: syncer.Sync wraps with %w, then sync()
		// errors.Join's a Close failure on the error path
		// (full_sync.go). errors.Is must traverse both.
		err := errors.Join(
			fmt.Errorf("failed to sync: %w", verdict),
			errors.New("close failed: fsync: file already closed"),
		)
		got := classifySyncError(err)
		require.ErrorIs(t, got, ErrTaskNonRetryable)
	})

	t.Run("errors without the sentinel stay retryable", func(t *testing.T) {
		// The invariant pass's IO failures (and every other sync error)
		// deliberately do not carry the sentinel: transient failures
		// must keep retrying.
		ioErr := fmt.Errorf("ingest invariant I5: listing entitlements: %w", errors.New("pebble: closed"))
		got := classifySyncError(ioErr)
		require.NotErrorIs(t, got, ErrTaskNonRetryable)
		require.Equal(t, ioErr, got, "non-verdict errors must pass through unmodified")
	})
}
