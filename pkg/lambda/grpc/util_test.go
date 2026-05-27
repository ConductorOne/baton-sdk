package grpc

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestIsTransientNetworkError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "random error", err: fmt.Errorf("something went wrong"), want: false},
		{name: "connection reset by peer", err: fmt.Errorf("read tcp 169.254.100.6:53312->10.102.197.53:3128: read: connection reset by peer"), want: true},
		{name: "broken pipe", err: fmt.Errorf("write: broken pipe"), want: true},
		{name: "connection refused", err: fmt.Errorf("dial tcp 10.0.0.1:443: connection refused"), want: true},
		{name: "http2 client connection lost", err: fmt.Errorf("Get \"https://api.github.com/repos/example/repo/collaborators\": http2: client connection lost"), want: true},
		{name: "i/o timeout", err: fmt.Errorf("dial tcp 10.0.0.1:443: i/o timeout"), want: true},
		{name: "no such host", err: fmt.Errorf("dial tcp: lookup example.invalid: no such host"), want: true},
		{name: "unexpected EOF", err: fmt.Errorf("unexpected EOF"), want: true},
		{name: "unexpected EOF on client connection", err: fmt.Errorf("unexpected EOF on client connection with an open transaction"), want: false},
		{name: "wrapped connection reset", err: fmt.Errorf("failed to fetch: %w", fmt.Errorf("connection reset by peer")), want: true},
		{name: "grpc status error", err: status.Errorf(codes.Internal, "internal error"), want: false},
		{name: "context canceled", err: context.Canceled, want: false},
		{name: "context deadline exceeded", err: context.DeadlineExceeded, want: false},
		{name: "TLS handshake timeout", err: fmt.Errorf("net/http: TLS handshake timeout"), want: true},
		{name: "connection timed out", err: fmt.Errorf("read tcp 10.0.0.1:443: connection timed out"), want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isTransientNetworkError(tt.err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestErrorResponse_TransientNetworkError(t *testing.T) {
	err := fmt.Errorf("read tcp 169.254.100.6:53312->10.102.197.53:3128: read: connection reset by peer")
	resp := ErrorResponse(err)
	require.NotNil(t, resp)

	st, stErr := resp.Status()
	require.NoError(t, stErr)
	require.Equal(t, codes.Unavailable, st.Code())
	require.Contains(t, st.Message(), "transient network error")
	require.Contains(t, st.Message(), "connection reset by peer")
}

func TestErrorResponse_HTTP2ClientConnectionLost(t *testing.T) {
	err := fmt.Errorf("github-connector: failed to list collaborators: Get \"https://api.github.com/repos/example/repo/collaborators\": http2: client connection lost")
	resp := ErrorResponse(err)
	require.NotNil(t, resp)

	st, stErr := resp.Status()
	require.NoError(t, stErr)
	require.Equal(t, codes.Unavailable, st.Code())
	require.Contains(t, st.Message(), "transient network error")
	require.Contains(t, st.Message(), "http2: client connection lost")
}

func TestStatusForApplicationError_ConnectionResetWrapped(t *testing.T) {
	err := &url.Error{
		Op:  "Get",
		URL: "https://example.com",
		Err: &net.OpError{
			Op:  "read",
			Net: "tcp",
			Err: os.NewSyscallError("read", syscall.ECONNRESET),
		},
	}

	st := statusForApplicationError(err)
	require.Equal(t, codes.Unavailable, st.Code())
	require.Contains(t, st.Message(), "connection reset")
	require.Contains(t, st.Message(), "connection reset by peer")
}

func TestStatusForApplicationError_UnexpectedEOF(t *testing.T) {
	st := statusForApplicationError(fmt.Errorf("wrapped: %w", io.ErrUnexpectedEOF))
	require.Equal(t, codes.Unavailable, st.Code())
	require.Contains(t, st.Message(), "unexpected EOF")
}

func TestStatusForApplicationError_URLTimeout(t *testing.T) {
	err := &url.Error{
		Op:  "Get",
		URL: "https://example.com",
		Err: timeoutError{err: fmt.Errorf("i/o timeout")},
	}

	st := statusForApplicationError(err)
	require.Equal(t, codes.DeadlineExceeded, st.Code())
	require.Contains(t, st.Message(), "request timeout")
}

func TestStatusForApplicationError_NetErrorTimeout(t *testing.T) {
	err := &net.OpError{
		Op:  "read",
		Net: "tcp",
		Err: timeoutError{err: fmt.Errorf("net/http: TLS handshake timeout")},
	}
	st := statusForApplicationError(err)
	require.Equal(t, codes.DeadlineExceeded, st.Code())
	require.Contains(t, st.Message(), "network timeout")
}

func TestErrorResponse_UnknownError(t *testing.T) {
	err := fmt.Errorf("some application error")
	resp := ErrorResponse(err)
	require.NotNil(t, resp)

	st, stErr := resp.Status()
	require.NoError(t, stErr)
	require.Equal(t, codes.Unknown, st.Code())
}

func TestErrorResponse_ContextCanceled(t *testing.T) {
	resp := ErrorResponse(context.Canceled)
	require.NotNil(t, resp)

	st, stErr := resp.Status()
	require.NoError(t, stErr)
	require.Equal(t, codes.Canceled, st.Code())
}

func TestErrorResponse_DeadlineExceeded(t *testing.T) {
	resp := ErrorResponse(context.DeadlineExceeded)
	require.NotNil(t, resp)

	st, stErr := resp.Status()
	require.NoError(t, stErr)
	require.Equal(t, codes.DeadlineExceeded, st.Code())
}

func TestErrorResponse_GRPCStatusPassthrough(t *testing.T) {
	err := status.Errorf(codes.PermissionDenied, "access denied")
	resp := ErrorResponse(err)
	require.NotNil(t, resp)

	st, stErr := resp.Status()
	require.NoError(t, stErr)
	require.Equal(t, codes.PermissionDenied, st.Code())
	require.Contains(t, st.Message(), "access denied")
}

type timeoutError struct {
	err error
}

func (e timeoutError) Error() string {
	return e.err.Error()
}

func (timeoutError) Timeout() bool {
	return true
}

func (timeoutError) Temporary() bool {
	return false
}
