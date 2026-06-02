package connector

import (
	"context"

	"google.golang.org/grpc"

	"github.com/conductorone/baton-sdk/pkg/types/grant"
)

// grantCancelledStatusUnaryServerInterceptor encodes a grant cancellation marker
// into the gRPC status returned to the caller.
func grantCancelledStatusUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		resp, err := handler(ctx, req)
		if err != nil {
			if st, ok := grant.StatusForGrantCancelledError(err); ok {
				return resp, st.Err()
			}
		}
		return resp, err
	}
}
