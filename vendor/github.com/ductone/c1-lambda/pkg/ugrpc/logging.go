package ugrpc

import (
	"context"
	"path"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_logging "github.com/grpc-ecosystem/go-grpc-middleware/logging"
	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

// UnaryServerInterceptor returns a new unary server interceptors that adds zap.Logger to the context.
func LoggingUnaryServerInterceptor(logger *zap.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		startTime := time.Now()

		newCtx := newLoggerForCall(ctx, logger, info.FullMethod, startTime)

		resp, err := handler(newCtx, req)

		code := grpc_logging.DefaultErrorToCode(err)
		level := grpc_zap.DefaultCodeToLevel(code)
		if code == codes.OK {
			level = zap.DebugLevel
		}

		duration := grpc_zap.DefaultDurationToField(time.Since(startTime))

		ctxzap.Extract(newCtx).Check(level, "finished unary call with code "+code.String()).Write(
			zap.Error(err),
			zap.String("grpc.code", code.String()),
			duration,
		)
		return resp, err
	}
}

// StreamServerInterceptor returns a new streaming server interceptor that adds zap.Logger to the context.
func LoggingStreamServerInterceptor(logger *zap.Logger) grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		startTime := time.Now()
		newCtx := newLoggerForCall(stream.Context(), logger, info.FullMethod, startTime)
		wrapped := grpc_middleware.WrapServerStream(stream)
		wrapped.WrappedContext = newCtx

		err := handler(srv, wrapped)
		code := grpc_logging.DefaultErrorToCode(err)
		level := grpc_zap.DefaultCodeToLevel(code)
		if code == codes.OK {
			level = zap.DebugLevel
		}
		duration := grpc_zap.DefaultDurationToField(time.Since(startTime))

		ctxzap.Extract(newCtx).Check(level, "finished stream call with code "+code.String()).Write(
			zap.Error(err),
			zap.String("grpc.code", code.String()),
			duration,
		)
		return err
	}
}

func newLoggerForCall(ctx context.Context, logger *zap.Logger, fullMethodString string, start time.Time) context.Context {
	var f []zapcore.Field
	f = append(f, zap.String("grpc.start_time", start.Format(time.RFC3339)))
	if d, ok := ctx.Deadline(); ok {
		f = append(f, zap.String("grpc.request.deadline", d.Format(time.RFC3339)))
	}
	callLog := logger.With(append(f, serverCallFields(fullMethodString)...)...)
	return ctxzap.ToContext(ctx, callLog)
}

func serverCallFields(fullMethodString string) []zapcore.Field {
	service := path.Dir(fullMethodString)[1:]
	method := path.Base(fullMethodString)
	return []zapcore.Field{
		zap.String("grpc.service", service),
		zap.String("grpc.method", method),
	}
}
