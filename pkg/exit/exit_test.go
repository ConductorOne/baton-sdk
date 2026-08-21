package exit

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type okStatusError struct{}

func (okStatusError) Error() string { return "ok-as-error" }

func (okStatusError) GRPCStatus() *status.Status {
	return status.New(codes.OK, "ok")
}

type nilStatusError struct{}

func (nilStatusError) Error() string { return "nil-status" }

func (nilStatusError) GRPCStatus() *status.Status { return nil }

func TestExitCode(t *testing.T) {
	t.Parallel()

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	deadlineCtx, deadlineCancel := context.WithTimeout(context.Background(), 0)
	t.Cleanup(deadlineCancel)
	<-deadlineCtx.Done()

	testCases := []struct {
		name string
		err  error
		want int
	}{
		{name: "nil", err: nil, want: 0},
		{name: "status.Error OK is nil", err: status.Error(codes.OK, "ok"), want: 0},
		{name: "plain error", err: errors.New("boom"), want: int(codes.Unknown)},
		{name: "wrapped plain error", err: fmt.Errorf("wrap: %w", errors.New("boom")), want: int(codes.Unknown)},
		{name: "context.Canceled", err: context.Canceled, want: int(codes.Canceled)},
		{name: "wrapped context.Canceled", err: fmt.Errorf("wrap: %w", context.Canceled), want: int(codes.Canceled)},
		{name: "canceled context err", err: canceledCtx.Err(), want: int(codes.Canceled)},
		{name: "context.DeadlineExceeded", err: context.DeadlineExceeded, want: int(codes.DeadlineExceeded)},
		{name: "wrapped context.DeadlineExceeded", err: fmt.Errorf("wrap: %w", context.DeadlineExceeded), want: int(codes.DeadlineExceeded)},
		{name: "deadline context err", err: deadlineCtx.Err(), want: int(codes.DeadlineExceeded)},
		{name: "status Canceled", err: status.Error(codes.Canceled, "canceled"), want: int(codes.Canceled)},
		{name: "custom GRPCStatus OK", err: okStatusError{}, want: int(codes.Internal)},
		{name: "wrapped custom GRPCStatus OK", err: fmt.Errorf("wrap: %w", okStatusError{}), want: int(codes.Internal)},
		{name: "custom GRPCStatus nil", err: nilStatusError{}, want: int(codes.Unknown)},
		{
			name: "joined status and cancel prefers status",
			err:  errors.Join(status.Error(codes.PermissionDenied, "denied"), context.Canceled),
			want: int(codes.PermissionDenied),
		},
		{
			name: "joined cancel and deadline prefers cancel",
			err:  errors.Join(context.Canceled, context.DeadlineExceeded),
			want: int(codes.Canceled),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, exitCode(tc.err))
		})
	}
}

func TestExitCode_AllGRPCCodes(t *testing.T) {
	t.Parallel()

	for code := codes.Canceled; code <= codes.Unauthenticated; code++ {
		t.Run(code.String(), func(t *testing.T) {
			t.Parallel()
			err := status.Error(code, "msg")
			require.Equal(t, int(code), exitCode(err))
			require.Equal(t, int(code), exitCode(fmt.Errorf("wrap: %w", err)))
		})
	}
}
