package ugrpc

import (
	"context"

	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	grpc_validator "github.com/grpc-ecosystem/go-grpc-middleware/validator"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// SessionCacheInterceptor creates a unary interceptor that propagates the session cache
// from the server context to the handler context.
func SessionCacheInterceptor(serverCtx context.Context) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		// Propagate session cache from server context to handler context
		if sessionCache, ok := serverCtx.Value(types.SessionCacheKey{}).(types.SessionCache); ok {
			ctx = context.WithValue(ctx, types.SessionCacheKey{}, sessionCache)
		}
		return handler(ctx, req)
	}
}

// SessionCacheStreamInterceptor creates a stream interceptor that propagates the session cache
// from the server context to the stream handler context.
func SessionCacheStreamInterceptor(serverCtx context.Context) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		// Propagate session cache from server context to stream context
		ctx := ss.Context()
		if sessionCache, ok := serverCtx.Value(types.SessionCacheKey{}).(types.SessionCache); ok {
			ctx = context.WithValue(ctx, types.SessionCacheKey{}, sessionCache)
			// Create a new server stream with the updated context
			wrappedStream := &wrappedServerStream{
				ServerStream: ss,
				ctx:          ctx,
			}
			return handler(srv, wrappedStream)
		}
		return handler(srv, ss)
	}
}

// wrappedServerStream wraps a grpc.ServerStream to provide a custom context
type wrappedServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedServerStream) Context() context.Context {
	return w.ctx
}

// StreamServerInterceptors returns a slice of interceptors that includes the default interceptors,
// plus any interceptors passed in as arguments.
func StreamServerInterceptors(ctx context.Context, interceptors ...grpc.StreamServerInterceptor) []grpc.StreamServerInterceptor {
	rv := []grpc.StreamServerInterceptor{
		grpc_ctxtags.StreamServerInterceptor(),
		LoggingStreamServerInterceptor(ctxzap.Extract(ctx)),
		grpc_recovery.StreamServerInterceptor(grpc_recovery.WithRecoveryHandlerContext(recoveryHandler)),
		grpc_validator.StreamServerInterceptor(),
		SessionCacheStreamInterceptor(ctx), // Add session cache interceptor
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
		SessionCacheInterceptor(ctx), // Add session cache interceptor
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
