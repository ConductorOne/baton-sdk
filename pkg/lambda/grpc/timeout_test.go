package grpc

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"

	pbtransport "github.com/conductorone/baton-sdk/pb/c1/transport/v1"
)

func TestEncodeDecodeTimeoutRoundTrip(t *testing.T) {
	tests := []struct {
		name     string
		duration time.Duration
		encoded  string
	}{
		{name: "1ns", duration: time.Nanosecond, encoded: "1n"},
		{name: "100ms", duration: 100 * time.Millisecond, encoded: "100000u"},
		{name: "5s", duration: 5 * time.Second, encoded: "5000000u"},
		{name: "90m", duration: 90 * time.Minute, encoded: "5400000m"},
		{name: "24h", duration: 24 * time.Hour, encoded: "86400000m"},
		{name: "3000h", duration: 3000 * time.Hour, encoded: "10800000S"},
		{name: "2000000h requires H unit", duration: 2000000 * time.Hour, encoded: "2000000H"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := encodeTimeout(tt.duration)
			require.Equal(t, tt.encoded, encoded)
			decoded, err := decodeTimeout(encoded)
			require.NoError(t, err)
			require.Equal(t, tt.duration, decoded)
		})
	}
}

func TestEncodeTimeoutNonPositive(t *testing.T) {
	require.Equal(t, "0n", encodeTimeout(0))
	require.Equal(t, "0n", encodeTimeout(-time.Second))
}

func TestTimeoutForRequest(t *testing.T) {
	t.Run("single grpc-timeout header is honored", func(t *testing.T) {
		req, err := NewRequest("/test.Service/Method", &structpb.Struct{}, metadata.Pairs("grpc-timeout", "5S"))
		require.NoError(t, err)

		timeout, ok, err := TimeoutForRequest(req)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, 5*time.Second, timeout)
	})

	t.Run("no header yields no timeout", func(t *testing.T) {
		req, err := NewRequest("/test.Service/Method", &structpb.Struct{}, metadata.MD{})
		require.NoError(t, err)

		timeout, ok, err := TimeoutForRequest(req)
		require.NoError(t, err)
		require.False(t, ok)
		require.Equal(t, time.Duration(0), timeout)
	})

	t.Run("malformed value yields error", func(t *testing.T) {
		req, err := NewRequest("/test.Service/Method", &structpb.Struct{}, metadata.Pairs("grpc-timeout", "bogus"))
		require.NoError(t, err)

		_, _, err = TimeoutForRequest(req)
		require.Error(t, err)
		require.Equal(t, codes.Internal, status.Code(err))
	})
}

// fakeClientTransport captures the request and returns a canned OK response.
type fakeClientTransport struct {
	req    *Request
	called bool
}

func (f *fakeClientTransport) RoundTrip(ctx context.Context, req *Request) (*Response, error) {
	f.called = true
	f.req = req
	respAny, err := anypb.New(&structpb.Struct{})
	if err != nil {
		return nil, err
	}
	stAny, err := anypb.New(status.New(codes.OK, "OK").Proto())
	if err != nil {
		return nil, err
	}
	return &Response{
		msg: pbtransport.Response_builder{
			Resp:   respAny,
			Status: stAny,
		}.Build(),
	}, nil
}

func TestInvokeSetsGrpcTimeoutFromDeadline(t *testing.T) {
	transport := &fakeClientTransport{}
	cc := NewClientConn(transport)

	callerMD := metadata.Pairs("some-key", "some-value")
	ctx := metadata.NewOutgoingContext(context.Background(), callerMD)
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	err := cc.Invoke(ctx, "/test.Service/Method", &structpb.Struct{}, &structpb.Struct{})
	require.NoError(t, err)
	require.True(t, transport.called)

	values := transport.req.Headers().Get("grpc-timeout")
	require.Len(t, values, 1)
	timeout, err := decodeTimeout(values[0])
	require.NoError(t, err)
	require.Greater(t, timeout, time.Duration(0))
	require.LessOrEqual(t, timeout, 5*time.Second)

	// The caller's metadata must not be mutated.
	require.Empty(t, callerMD.Get("grpc-timeout"))
	// Other caller metadata is preserved on the request.
	require.Equal(t, []string{"some-value"}, transport.req.Headers().Get("some-key"))
}

func TestInvokeWithoutDeadlineDoesNotSetGrpcTimeout(t *testing.T) {
	transport := &fakeClientTransport{}
	cc := NewClientConn(transport)

	err := cc.Invoke(context.Background(), "/test.Service/Method", &structpb.Struct{}, &structpb.Struct{})
	require.NoError(t, err)
	require.True(t, transport.called)
	require.Empty(t, transport.req.Headers().Get("grpc-timeout"))
}

func TestInvokeExpiredDeadlineReturnsDeadlineExceeded(t *testing.T) {
	transport := &fakeClientTransport{}
	cc := NewClientConn(transport)

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	err := cc.Invoke(ctx, "/test.Service/Method", &structpb.Struct{}, &structpb.Struct{})
	require.Error(t, err)
	require.Equal(t, codes.DeadlineExceeded, status.Code(err))
	require.False(t, transport.called)
}

// TestServerHandlerAppliesGrpcTimeoutEndToEnd locks in the full server timeout
// path, which was dead code before the len(v) > 0 fix in TimeoutForRequest: a
// request carrying a grpc-timeout header must yield a handler context with a
// deadline, and a handler that blocks past it must produce a DeadlineExceeded
// status response.
func TestServerHandlerAppliesGrpcTimeoutEndToEnd(t *testing.T) {
	s := NewServer(nil)

	var handlerHadDeadline bool
	var handlerDeadline time.Time

	// The handler returns immediately when no deadline is set. When a deadline
	// is present it blocks until cancellation, so a completed response proves
	// the timeout was never applied.
	sd := &grpc.ServiceDesc{
		ServiceName: "test.TimeoutService",
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{{
			MethodName: "Wait",
			Handler: func(_ any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
				if err := dec(&structpb.Struct{}); err != nil {
					return nil, err
				}
				handlerDeadline, handlerHadDeadline = ctx.Deadline()
				if !handlerHadDeadline {
					return &structpb.Struct{}, nil
				}
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(10 * time.Second):
					return nil, status.Error(codes.Internal, "handler context was never cancelled")
				}
			},
		}},
	}
	s.RegisterService(sd, &struct{}{})

	t.Run("grpc-timeout cancels the handler and returns DeadlineExceeded", func(t *testing.T) {
		handlerHadDeadline = false
		req, err := NewRequest("/test.TimeoutService/Wait", &structpb.Struct{}, metadata.Pairs("grpc-timeout", "50m")) // 50ms
		require.NoError(t, err)

		start := time.Now()
		resp, err := s.Handler(context.Background(), req)
		require.NoError(t, err)

		st, err := resp.Status()
		require.NoError(t, err)
		require.Equal(t, codes.DeadlineExceeded, st.Code())

		require.True(t, handlerHadDeadline, "handler context must carry the propagated deadline")
		require.WithinDuration(t, start.Add(50*time.Millisecond), handlerDeadline, time.Second)
		require.Less(t, time.Since(start), 5*time.Second, "handler must be cancelled by the timeout, not run to completion")
	})

	t.Run("no grpc-timeout leaves the handler context without a deadline", func(t *testing.T) {
		handlerHadDeadline = false
		req, err := NewRequest("/test.TimeoutService/Wait", &structpb.Struct{}, metadata.MD{})
		require.NoError(t, err)

		resp, err := s.Handler(context.Background(), req)
		require.NoError(t, err)

		st, err := resp.Status()
		require.NoError(t, err)
		require.Equal(t, codes.OK, st.Code())
		require.False(t, handlerHadDeadline)
	})
}
