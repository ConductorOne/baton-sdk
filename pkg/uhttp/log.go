package uhttp

import (
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// isClientErrorCode returns true for gRPC codes that represent client errors (4xx equivalent).
func isClientErrorCode(code codes.Code) bool {
	switch code {
	case codes.InvalidArgument,
		codes.NotFound,
		codes.AlreadyExists,
		codes.PermissionDenied,
		codes.Unauthenticated,
		codes.FailedPrecondition,
		codes.OutOfRange,
		codes.Unimplemented,
		codes.Canceled:
		return true
	default:
		return false
	}
}

// LogError logs the error at the appropriate level based on its gRPC status code.
// Client errors (4xx equivalent) are logged at Warn level, server errors (5xx) at Error level.
// If the error does not carry a gRPC status, it defaults to Error level.
func LogError(l *zap.Logger, msg string, err error, fields ...zap.Field) {
	if err == nil {
		l.Warn(msg, fields...)
		return
	}

	allFields := append([]zap.Field{zap.Error(err)}, fields...)

	st, ok := status.FromError(err)
	if ok && isClientErrorCode(st.Code()) {
		l.Warn(msg, allFields...)
		return
	}

	l.Error(msg, allFields...)
}
