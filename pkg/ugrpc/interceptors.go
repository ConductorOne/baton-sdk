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
// plus any interceptors passed in as arguments. Recovery sits below ctxtags
// and logging for the reasons recorded on UnaryServerInterceptor.
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
//
// Recovery deliberately sits below ctxtags and logging rather than outermost
// the way the lambda chain places it. Everything with real panic surface —
// the validator, the caller's interceptors, and the connector method — is
// below recovery in both arrangements, while the two interceptors above it
// are small fixed middleware. Staying inside logging buys two things for a
// recovered panic: RecoveredPanicError extracts the request-scoped ctxzap
// logger, so the panic record carries the call's fields, and the access-log
// line above still fires with codes.Internal instead of being unwound past.
// The lambda chain carries no logging interceptor, so recovery-first costs
// it nothing.
func UnaryServerInterceptor(ctx context.Context, interceptors ...grpc.UnaryServerInterceptor) []grpc.UnaryServerInterceptor {
	rv := []grpc.UnaryServerInterceptor{
		grpc_ctxtags.UnaryServerInterceptor(),
		LoggingUnaryServerInterceptor(ctxzap.Extract(ctx)),
		RecoveryUnaryInterceptor(),
		grpc_validator.UnaryServerInterceptor(),
	}

	rv = append(rv, interceptors...)
	return rv
}

// RecoveryUnaryInterceptor converts a panic in a downstream interceptor or
// handler into a codes.Internal status. Any server that hosts connector RPCs
// needs it: an escaping panic unwinds past the transport, which surfaces it as
// an invocation failure carrying no gRPC status at all, leaving callers unable
// to classify the failure or locate it without the raw runtime log.
func RecoveryUnaryInterceptor() grpc.UnaryServerInterceptor {
	return grpc_recovery.UnaryServerInterceptor(grpc_recovery.WithRecoveryHandlerContext(recoveryHandler))
}

// Recovery sites, reported as the recovery_site log field. The message stays
// the same across sites so one query finds every recovered panic; the field is
// what says which side of the interceptor chain it came from.
const (
	// RecoverySiteInterceptor is a panic the recovery interceptor caught, so it
	// came from a handler or another interceptor below it.
	RecoverySiteInterceptor = "interceptor"
	// RecoverySiteLambdaHandler is a panic raised outside the lambda transport's
	// interceptor chain, which covers config reload, log level application, and
	// the transport's own dispatch — method lookup, metadata and timeout parsing,
	// and request unmarshalling, since generated handlers decode before invoking
	// the interceptor. The panic value and stack say which of those it was.
	RecoverySiteLambdaHandler = "lambda_handler"
)

// RecoveredPanicError converts a recovered panic value into the error the caller
// sees, logging the panic value and a stack that includes the panic site — the
// status message is deliberately generic, so the log is the only record of what
// happened. Recovery sites that cannot be an interceptor route through this too,
// naming themselves with one of the RecoverySite constants, so the status code
// and log fields stay identical across them.
func RecoveredPanicError(ctx context.Context, p interface{}, site string) error {
	err := status.Error(codes.Internal, "Internal Server Error")
	ctxzap.Extract(ctx).Error("gRPC handler panic",
		zap.Stack("stack"),
		zap.Any("panic", p),
		zap.String("recovery_site", site),
		zap.Error(err),
	)
	return err
}

// recoveryHandler is only ever reached after a panic: grpc_recovery calls it
// when its own completion flag says the handler did not return, so a nil value
// here means a nil panic rather than no panic. Returning nil for that case would
// hand the caller an empty success instead of a status.
func recoveryHandler(ctx context.Context, p interface{}) error {
	return RecoveredPanicError(ctx, p, RecoverySiteInterceptor)
}
