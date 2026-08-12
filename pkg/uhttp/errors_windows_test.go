//go:build windows

package uhttp

import (
	"net"
	"net/url"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/windows"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Winsock reports socket failures with WSAE* codes rather than the POSIX
// errnos, and syscall.Errno.Is does not bridge them, so matching only the
// POSIX names would silently classify every real Windows socket failure as
// codes.Unknown: non-retryable and non-preservable.
func TestWrapTransientNetworkError_Winsock(t *testing.T) {
	tests := []struct {
		name     string
		op       string
		errno    error
		wantCode codes.Code
		wantMsg  string
	}{
		{name: "WSAECONNRESET", op: "read", errno: windows.WSAECONNRESET, wantCode: codes.Unavailable, wantMsg: "connection reset"},
		{name: "WSAECONNABORTED", op: "read", errno: windows.WSAECONNABORTED, wantCode: codes.Unavailable, wantMsg: "connection reset"},
		{name: "WSAECONNREFUSED", op: "dial", errno: windows.WSAECONNREFUSED, wantCode: codes.Unavailable, wantMsg: "connection refused"},
		{name: "WSAESHUTDOWN", op: "write", errno: windows.WSAESHUTDOWN, wantCode: codes.Unavailable, wantMsg: "broken pipe"},
		{name: "WSAEHOSTUNREACH", op: "dial", errno: windows.WSAEHOSTUNREACH, wantCode: codes.Unavailable, wantMsg: "network unreachable"},
		{name: "WSAENETUNREACH", op: "dial", errno: windows.WSAENETUNREACH, wantCode: codes.Unavailable, wantMsg: "network unreachable"},
		{name: "WSAENETDOWN", op: "dial", errno: windows.WSAENETDOWN, wantCode: codes.Unavailable, wantMsg: "network unreachable"},
		{name: "WSAETIMEDOUT", op: "dial", errno: windows.WSAETIMEDOUT, wantCode: codes.DeadlineExceeded, wantMsg: "network timeout"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := &url.Error{
				Op:  "Get",
				URL: "https://example.com",
				Err: &net.OpError{
					Op:  tt.op,
					Net: "tcp",
					Err: os.NewSyscallError(tt.op, tt.errno),
				},
			}

			got := wrapTransientNetworkError(err)
			st, ok := status.FromError(got)
			require.True(t, ok, "got %v", got)
			require.Equal(t, tt.wantCode, st.Code())
			require.Contains(t, st.Message(), tt.wantMsg)
			require.ErrorIs(t, got, err)
		})
	}
}

// The transport's own retry of idempotent requests must recognize the Winsock
// spellings too, or a stale pooled connection is never retried on Windows.
func TestIsStaleConnectionError_Winsock(t *testing.T) {
	for _, errno := range []error{windows.WSAECONNRESET, windows.WSAECONNABORTED, windows.WSAESHUTDOWN} {
		err := &net.OpError{Op: "read", Net: "tcp", Err: os.NewSyscallError("read", errno)}
		require.True(t, isStaleConnectionError(err), "errno %v", errno)
	}

	notStale := &net.OpError{Op: "dial", Net: "tcp", Err: os.NewSyscallError("connect", windows.WSAECONNREFUSED)}
	require.False(t, isStaleConnectionError(notStale),
		"connection refused is a dial failure, not a stale pooled connection")
}
