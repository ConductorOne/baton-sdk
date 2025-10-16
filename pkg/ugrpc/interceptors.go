package ugrpc

import (
	"context"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	grpc_validator "github.com/grpc-ecosystem/go-grpc-middleware/validator"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func ChainUnaryInterceptors(interceptors ...grpc.UnaryServerInterceptor) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		// Compose from last to first
		chained := handler
		for i := len(interceptors) - 1; i >= 0; i-- {
			currentInterceptor := interceptors[i]
			next := chained
			chained = func(currentCtx context.Context, currentReq interface{}) (interface{}, error) {
				return currentInterceptor(currentCtx, currentReq, info, next)
			}
		}
		return chained(ctx, req)
	}
}

// StreamServerInterceptors returns a slice of interceptors that includes the default interceptors,
// plus any interceptors passed in as arguments.
func StreamServerInterceptors(ctx context.Context, interceptors ...grpc.StreamServerInterceptor) []grpc.StreamServerInterceptor {
	rv := []grpc.StreamServerInterceptor{
		grpc_ctxtags.StreamServerInterceptor(),
		LoggingStreamServerInterceptor(ctxzap.Extract(ctx)),
		grpc_recovery.StreamServerInterceptor(grpc_recovery.WithRecoveryHandlerContext(recoveryHandler)),
		grpc_validator.StreamServerInterceptor(),
	}

	rv = append(rv, interceptors...)
	return rv
}

// UnaryServerInterceptor returns a slice of interceptors that includes the default interceptors,
// plus any interceptors that were passed in.
func UnaryServerInterceptor(ctx context.Context, interceptors ...grpc.UnaryServerInterceptor) []grpc.UnaryServerInterceptor {
	rv := []grpc.UnaryServerInterceptor{
		grpc_ctxtags.UnaryServerInterceptor(),
		LoggingUnaryServerInterceptor(ctxzap.Extract(ctx)),
		grpc_recovery.UnaryServerInterceptor(grpc_recovery.WithRecoveryHandlerContext(recoveryHandler)),
		grpc_validator.UnaryServerInterceptor(),
	}

	rv = append(rv, interceptors...)
	return rv
}

func recoveryHandler(ctx context.Context, p interface{}) error {
	l := ctxzap.Extract(ctx)
	if p == nil {
		return nil
	}

	err := status.Errorf(codes.Internal, "Internal Server Error")
	l.Error("gRPC handler panic",
		zap.Stack("stack"),
		zap.Any("panic", p),
		zap.Error(err),
	)
	return err
}
