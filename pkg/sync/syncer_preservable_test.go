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
		{name: "status Unavailable", err: status.Error(codes.Unavailable, "server unavailable"), want: true},
		{name: "status DeadlineExceeded", err: status.Error(codes.DeadlineExceeded, "lambda_transport: function timed out"), want: true},
		{name: "bare context.DeadlineExceeded", err: context.DeadlineExceeded, want: true},
		{name: "wrapped context.DeadlineExceeded", err: fmt.Errorf("wrapped: %w", context.DeadlineExceeded), want: true},
		{name: "status Internal", err: status.Error(codes.Internal, "internal error"), want: false},
		{name: "plain error", err: errors.New("plain"), want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, IsSyncPreservable(tt.err))
		})
	}
}
