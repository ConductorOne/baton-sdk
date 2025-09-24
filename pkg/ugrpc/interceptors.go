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

/*
SessionCacheUnaryInterceptor creates a unary interceptor that:
1. Propagates the session cache from the server context to the handler context.
2. Extracts annotations from requests and adds syncID to context (for the session manager).
*/
func SessionCacheUnaryInterceptor(serverCtx context.Context) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		// Propagate session cache from server context to handler context
		ctx = ContextWithSyncID(ctx, req)

		if sessionCache, ok := serverCtx.Value(types.SessionCacheKey{}).(types.SessionCache); ok {
			ctx = context.WithValue(ctx, types.SessionCacheKey{}, sessionCache)
		}

		return handler(ctx, req)
	}
}

func SessionCacheStreamInterceptor(serverCtx context.Context) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		// Start with the original stream context
		ctx := ss.Context()

		// Propagate session cache from server context to stream context
		if sessionCache, ok := serverCtx.Value(types.SessionCacheKey{}).(types.SessionCache); ok {
			ctx = context.WithValue(ctx, types.SessionCacheKey{}, sessionCache)
		}

		// Create a wrapped stream that handles both session cache and annotation extraction
		wrappedStream := &sessionCacheServerStream{
			ServerStream: ss,
			ctx:          ctx,
		}

		return handler(srv, wrappedStream)
	}
}

type sessionCacheServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *sessionCacheServerStream) Context() context.Context {
	return s.ctx
}

func (s *sessionCacheServerStream) RecvMsg(m interface{}) error {
	err := s.ServerStream.RecvMsg(m)
	if err != nil {
		return err
	}
	s.ctx = ContextWithSyncID(s.ctx, m)
	return nil
}

// StreamServerInterceptors returns a slice of interceptors that includes the default interceptors,
// plus any interceptors passed in as arguments.
func StreamServerInterceptors(ctx context.Context, interceptors ...grpc.StreamServerInterceptor) []grpc.StreamServerInterceptor {
	rv := []grpc.StreamServerInterceptor{
		grpc_ctxtags.StreamServerInterceptor(),
		LoggingStreamServerInterceptor(ctxzap.Extract(ctx)),
		grpc_recovery.StreamServerInterceptor(grpc_recovery.WithRecoveryHandlerContext(recoveryHandler)),
		grpc_validator.StreamServerInterceptor(),
		SessionCacheStreamInterceptor(ctx),
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
		SessionCacheUnaryInterceptor(ctx),
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

type syncIDGetter interface {
	GetActiveSyncId() string
}

// ContextWithSyncID extracts syncID from a request annotations and adds it to the context.
func ContextWithSyncID(ctx context.Context, req any) context.Context {
	if ctx == nil || req == nil {
		return ctx
	}

	var syncID string
	syncIDGetter, ok := req.(syncIDGetter)
	if !ok {
		return ctx
	}

	syncID = syncIDGetter.GetActiveSyncId()
	if syncID == "" {
		return ctx
	}

	return context.WithValue(ctx, types.SyncIDKey{}, syncID)
}
