package ugrpc

import (
	"context"

	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
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

// wrappedServerStream wraps a grpc.ServerStream to provide a custom context.
type wrappedServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedServerStream) Context() context.Context {
	return w.ctx
}

// annotationExtractionUnaryInterceptor extracts annotations from requests and adds syncID to context.
// This is used by the server side (connector) to make enable the seesion cache.

func annotationExtractionUnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	ctx = connectorbuilder.WithAnnotationsFromRequest(ctx, req)
	return handler(ctx, req)
}

// annotationExtractionStreamInterceptor extracts annotations from streaming requests and adds syncID to context.
func annotationExtractionStreamInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	// For streaming, we need to wrap the stream to intercept messages
	wrappedStream := &annotationExtractionServerStream{
		ServerStream: ss,
		ctx:          ss.Context(),
	}

	return handler(srv, wrappedStream)
}

type annotationExtractionServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *annotationExtractionServerStream) Context() context.Context {
	return s.ctx
}

func (s *annotationExtractionServerStream) RecvMsg(m interface{}) error {
	err := s.ServerStream.RecvMsg(m)
	if err != nil {
		return err
	}

	// Extract annotations from the message and update context
	annos := connectorbuilder.ExtractAnnotationsFromRequest(m)
	if len(annos) > 0 {
		s.ctx = connectorbuilder.WithAnnotationsFromRequest(s.ctx, m)

		// Extract syncID from ActiveSync annotation if present
		syncID, err := annotations.GetActiveSyncIdFromAnnotations(annos)
		if err == nil && syncID != "" {
			s.ctx = types.SetSyncIDInContext(s.ctx, syncID)
		}
	}

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
		SessionCacheStreamInterceptor(ctx), // Add session cache interceptor
		annotationExtractionStreamInterceptor,
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
		annotationExtractionUnaryInterceptor,
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
