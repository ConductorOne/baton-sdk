package ugrpc

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var testMethodInfo = &grpc.UnaryServerInfo{FullMethod: "/test.Service/Method"}

func TestRecoveryUnaryInterceptorConvertsPanicToInternal(t *testing.T) {
	t.Parallel()

	resp, err := RecoveryUnaryInterceptor()(
		context.Background(),
		"request",
		testMethodInfo,
		func(ctx context.Context, req interface{}) (interface{}, error) {
			panic("connector exploded")
		},
	)

	require.Nil(t, resp)
	require.Equal(t, codes.Internal, status.Code(err))
}

// The panic value and stack are the only record of what happened, since the
// status message is deliberately generic. Losing them would leave a recovered
// panic undiagnosable.
func TestRecoveryUnaryInterceptorLogsPanicDetail(t *testing.T) {
	t.Parallel()

	buf := &bytes.Buffer{}
	ctx := ctxzap.ToContext(context.Background(), newCaptureLogger(buf))

	_, err := RecoveryUnaryInterceptor()(
		ctx,
		nil,
		testMethodInfo,
		func(ctx context.Context, req interface{}) (interface{}, error) {
			panicForStackTest()
			return nil, nil
		},
	)
	require.Equal(t, codes.Internal, status.Code(err))

	logged := buf.String()
	require.Contains(t, logged, "gRPC handler panic")
	require.Contains(t, logged, "connector exploded")
	// Naming a frame that exists only below the panic: the test function's own
	// frame is the interceptor's caller, so it would appear even in a stack
	// captured after the panicking frames were unwound.
	require.Contains(t, logged, "panicForStackTest")
	// The site is the only thing distinguishing this from a panic recovered
	// before a chain could run, since the message is shared.
	require.Contains(t, logged, `"recovery_site":"`+RecoverySiteInterceptor+`"`)
}

// panicForStackTest gives the panic a uniquely named frame that exists only
// below the recovery interceptor. TestRecoveryUnaryInterceptorLogsPanicDetail
// asserts this name in the logged stack: it disappears if the stack is captured
// after unwinding, while the test function's own frame would survive.
func panicForStackTest() {
	panic("connector exploded")
}

// grpc_recovery calls the handler whenever its completion flag says the wrapped
// handler did not return, passing whatever recover() gave it. Under
// GODEBUG=panicnil=1 that value is nil for a real panic, so treating nil as "no
// panic" would return a nil error and turn the panic into an empty success.
func TestRecoveryHandlerTreatsNilPanicValueAsAPanic(t *testing.T) {
	t.Parallel()

	err := recoveryHandler(context.Background(), nil)
	require.Equal(t, codes.Internal, status.Code(err))
}

func TestRecoveryUnaryInterceptorPassesThroughNonPanics(t *testing.T) {
	t.Parallel()

	t.Run("successful response", func(t *testing.T) {
		t.Parallel()

		resp, err := RecoveryUnaryInterceptor()(
			context.Background(),
			nil,
			testMethodInfo,
			func(ctx context.Context, req interface{}) (interface{}, error) {
				return "response", nil
			},
		)

		require.NoError(t, err)
		require.Equal(t, "response", resp)
	})

	// Recovery must not reclassify errors the handler chose to return; only an
	// escaping panic becomes Internal.
	t.Run("handler error keeps its code", func(t *testing.T) {
		t.Parallel()

		_, err := RecoveryUnaryInterceptor()(
			context.Background(),
			nil,
			testMethodInfo,
			func(ctx context.Context, req interface{}) (interface{}, error) {
				return nil, status.Error(codes.NotFound, "no such resource")
			},
		)

		require.Equal(t, codes.NotFound, status.Code(err))
	})

	t.Run("bare handler error is left uncoded", func(t *testing.T) {
		t.Parallel()

		sentinel := errors.New("plain failure")

		_, err := RecoveryUnaryInterceptor()(
			context.Background(),
			nil,
			testMethodInfo,
			func(ctx context.Context, req interface{}) (interface{}, error) {
				return nil, sentinel
			},
		)

		require.ErrorIs(t, err, sentinel)
	})
}
