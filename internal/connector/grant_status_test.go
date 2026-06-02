package connector

import (
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/conductorone/baton-sdk/pkg/types/grant"
)

// TestGrantCancelledStatusInterceptorPreservesMarkerOverGRPC proves the marker
// survives the connector-subprocess -> runner gRPC hop once the interceptor is
// installed. Without it, a wrapped grant.ErrGrantCancelled is flattened to a bare
// codes.Unknown status and the structured marker is lost before the runner can
// read it (which is exactly what the c1api finishTask path depends on).
func TestGrantCancelledStatusInterceptorPreservesMarkerOverGRPC(t *testing.T) {
	var lc net.ListenConfig
	lis, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv := grpc.NewServer(grpc.ChainUnaryInterceptor(grantCancelledStatusUnaryServerInterceptor()))
	srv.RegisterService(&grpc.ServiceDesc{
		ServiceName: "test.GrantService",
		HandlerType: (*interface{})(nil),
		Methods: []grpc.MethodDesc{{
			MethodName: "Grant",
			Handler: func(_ interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
				in := new(emptypb.Empty)
				if err := dec(in); err != nil {
					return nil, err
				}
				// Mimics builder.Grant returning a wrapped ErrGrantCancelled.
				handler := func(ctx context.Context, _ interface{}) (interface{}, error) {
					return nil, fmt.Errorf("grant failed: %w", grant.NewErrGrantCancelled("reject_if reason"))
				}
				info := &grpc.UnaryServerInfo{FullMethod: "/test.GrantService/Grant"}
				if interceptor == nil {
					return handler(ctx, in)
				}
				return interceptor(ctx, in, info, handler)
			},
		}},
	}, struct{}{})
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	recvErr := conn.Invoke(context.Background(), "/test.GrantService/Grant", &emptypb.Empty{}, &emptypb.Empty{})
	require.Error(t, recvErr)

	// The exact detection the runner's finishTask path relies on must succeed on
	// the post-gRPC error.
	reason, ok := grant.GrantCancelledReasonFromError(recvErr)
	require.True(t, ok, "grant cancellation marker must survive gRPC; got %v", recvErr)
	require.Equal(t, "reject_if reason", reason)
}

// TestGrantCancelledStatusInterceptorPassesThroughOtherErrors confirms the
// interceptor only rewrites grant cancellations and leaves every other error
// untouched.
func TestGrantCancelledStatusInterceptorPassesThroughOtherErrors(t *testing.T) {
	interceptor := grantCancelledStatusUnaryServerInterceptor()
	sentinel := fmt.Errorf("some other failure")

	_, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{},
		func(context.Context, interface{}) (interface{}, error) { return nil, sentinel })

	require.ErrorIs(t, err, sentinel)
	_, ok := grant.GrantCancelledReasonFromError(err)
	require.False(t, ok)
}
