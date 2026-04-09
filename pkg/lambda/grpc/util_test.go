package grpc

import (
	"context"
	"fmt"
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
		{name: "i/o timeout", err: fmt.Errorf("dial tcp 10.0.0.1:443: i/o timeout"), want: true},
		{name: "no such host", err: fmt.Errorf("dial tcp: lookup example.invalid: no such host"), want: true},
		{name: "unexpected EOF", err: fmt.Errorf("unexpected EOF"), want: true},
		{name: "unexpected EOF on client connection", err: fmt.Errorf("unexpected EOF on client connection with an open transaction"), want: false},
		{name: "wrapped connection reset", err: fmt.Errorf("failed to fetch: %w", fmt.Errorf("connection reset by peer")), want: true},
		{name: "grpc status error", err: status.Errorf(codes.Internal, "internal error"), want: false},
		{name: "context canceled", err: context.Canceled, want: false},
		{name: "context deadline exceeded", err: context.DeadlineExceeded, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isTransientNetworkError(tt.err)
			require.Equal(t, tt.want, got)
		})
	}
}

// TestErrorResponse_TransientNetworkError verifies that a plain transient
// network error (not wrapped in a gRPC status) falls through to codes.Unknown
// in ErrorResponse. The transient network classification is handled upstream:
// connectors should use uhttp (which wraps ECONNRESET as Unavailable) or
// classify errors in their own error handling before they reach ErrorResponse.
func TestErrorResponse_TransientNetworkError(t *testing.T) {
	err := fmt.Errorf("read tcp 169.254.100.6:53312->10.102.197.53:3128: read: connection reset by peer")
	resp := ErrorResponse(err)
	require.NotNil(t, resp)

	st, stErr := resp.Status()
	require.NoError(t, stErr)
	require.Equal(t, codes.Unknown, st.Code())
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
