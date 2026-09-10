//go:build !windows

package uhttp

import (
	"errors"
	"net"
	"net/url"
	"os"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// transientSocketConditions is the single list both classifiers read, which
// means deleting an entry removes a condition from both of them at once and
// no agreement test can notice. This is the floor: the POSIX spellings
// production has produced, each asserted to still classify. Its Windows
// counterpart is TestWrapTransientNetworkError_Winsock.
func TestTransientSocketConditions_POSIXSpellings(t *testing.T) {
	tests := []struct {
		errno    syscall.Errno
		op       string
		wantCode codes.Code
		wantMsg  string
	}{
		{errno: syscall.ECONNRESET, op: "read", wantCode: codes.Unavailable, wantMsg: "connection reset"},
		{errno: syscall.ECONNABORTED, op: "read", wantCode: codes.Unavailable, wantMsg: "connection reset"},
		{errno: syscall.ECONNREFUSED, op: "dial", wantCode: codes.Unavailable, wantMsg: "connection refused"},
		{errno: syscall.EPIPE, op: "write", wantCode: codes.Unavailable, wantMsg: "broken pipe"},
		{errno: syscall.EHOSTUNREACH, op: "dial", wantCode: codes.Unavailable, wantMsg: "network unreachable"},
		{errno: syscall.ENETUNREACH, op: "dial", wantCode: codes.Unavailable, wantMsg: "network unreachable"},
		{errno: syscall.ENETDOWN, op: "dial", wantCode: codes.Unavailable, wantMsg: "network unreachable"},
		// ETIMEDOUT reaches DeadlineExceeded through the url.Error timeout
		// branch rather than a predicate, since Errno.Timeout() reports true
		// for it, so its message is "request timeout" and not the
		// socketTimeout class message. Windows differs: Errno.Timeout() does
		// not recognize WSAETIMEDOUT, so that spelling falls through to
		// isSocketTimeout and reports "network timeout".
		{errno: syscall.ETIMEDOUT, op: "dial", wantCode: codes.DeadlineExceeded, wantMsg: "request timeout"},
	}

	for _, tt := range tests {
		t.Run(tt.errno.Error(), func(t *testing.T) {
			present := false
			for _, condition := range transientSocketConditions {
				if errors.Is(tt.errno, condition.err) {
					present = true
					break
				}
			}
			require.True(t, present, "%v is no longer on the shared condition list", tt.errno)

			err := &url.Error{Op: "Get", URL: "https://example.com", Err: &net.OpError{
				Op:  tt.op,
				Net: "tcp",
				Err: os.NewSyscallError(tt.op, tt.errno),
			}}

			got := wrapTransientNetworkError(err)
			st, ok := status.FromError(got)
			require.True(t, ok, "got %v", got)
			require.Equal(t, tt.wantCode, st.Code())
			require.Contains(t, st.Message(), tt.wantMsg)
			require.ErrorIs(t, got, err)
		})
	}
}
