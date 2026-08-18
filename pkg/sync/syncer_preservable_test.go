package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestIsSyncPreservable is a FROZEN-surface pin (RFC 0009 §4.4): runners
// built against older SDKs branch on exactly these classifications during
// the SDK-first rollout window, so this table asserts today's behavior and
// must not be "fixed" to widen it — the preserve-by-default policy ships as
// the separate ShouldDiscardSyncArtifact helper instead. The joined and
// %w-wrapped rows pin the production shapes (run-duration expiry joins a
// checkpoint error with ErrSyncNotComplete; the warning-budget exit wraps
// ErrTooManyWarnings with %w).
func TestIsSyncPreservable(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil error", err: nil, want: true},
		{name: "ErrSyncNotComplete", err: ErrSyncNotComplete, want: true},
		{name: "ErrTooManyWarnings", err: ErrTooManyWarnings, want: true},
		{name: "wrapped ErrSyncNotComplete", err: fmt.Errorf("wrapped: %w", ErrSyncNotComplete), want: true},
		{name: "ErrSyncNotComplete joined with checkpoint error (production shape)", err: errors.Join(errors.New("checkpoint failed"), ErrSyncNotComplete), want: true},
		{name: "ErrTooManyWarnings production shape", err: fmt.Errorf("%w: warnings: %v completed actions: %d", ErrTooManyWarnings, []error{errors.New("w")}, 5), want: true},
		{name: "status Unavailable", err: status.Error(codes.Unavailable, "server unavailable"), want: true},
		{name: "status DeadlineExceeded", err: status.Error(codes.DeadlineExceeded, "lambda_transport: function timed out"), want: true},
		{name: "bare context.DeadlineExceeded", err: context.DeadlineExceeded, want: true},
		{name: "wrapped context.DeadlineExceeded", err: fmt.Errorf("wrapped: %w", context.DeadlineExceeded), want: true},
		{name: "status Internal", err: status.Error(codes.Internal, "internal error"), want: false},
		{name: "plain error", err: errors.New("plain"), want: false},
		// Frozen discards: the old policy loses these; the fix is the new
		// helper at migrated call sites, never an in-place widening here.
		{name: "bare context.Canceled", err: context.Canceled, want: false},
		{name: "status Canceled", err: status.Error(codes.Canceled, "context canceled"), want: false},
		// A storage verdict is not preservable under the old branch either
		// — the correct outcome for both policies (RFC 0009 §4.4 obligation
		// 1: attaching the sentinel must not change old-runner behavior).
		{name: "ErrArtifactUnusable", err: fmt.Errorf("error closing store: %w", ErrArtifactUnusable), want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, IsSyncPreservable(tt.err))
		})
	}
}
