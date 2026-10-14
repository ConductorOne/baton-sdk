package uotel

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

// TestCloseOrderAndBudget pins Close's contract: metrics, then traces, then
// logs, all before the connection is closed, and a provider that blocks
// until its deadline does not starve the ones after it.
func TestCloseOrderAndBudget(t *testing.T) {
	cfg := newConfig(WithServiceName("uotel-test"), WithInsecureOtelEndpoint("127.0.0.1:1"))
	cfg.mtx.Lock()
	cc, err := cfg.getConnection()
	cfg.mtx.Unlock()
	require.NoError(t, err)

	var order []string
	step := func(name string, block bool) func(context.Context) error {
		return func(ctx context.Context) error {
			require.NotEqual(t, connectivity.Shutdown, cc.GetState(), "%s shut down after the connection closed", name)
			_, hasDeadline := ctx.Deadline()
			require.True(t, hasDeadline, "%s got no deadline", name)
			require.NoError(t, ctx.Err(), "%s started with an expired context", name)
			order = append(order, name)
			if block {
				<-ctx.Done()
				return ctx.Err()
			}
			return nil
		}
	}
	// Registered in init order (logs, traces, metrics); the traces step
	// hangs until its budget runs out.
	cfg.shutdown = []providerShutdown{
		{name: "logs", order: shutdownOrderLogs, fn: step("logs", false)},
		{name: "traces", order: shutdownOrderTraces, fn: step("traces", true)},
		{name: "metrics", order: shutdownOrderMetrics, fn: step("metrics", false)},
	}

	ctx, cancel := context.WithTimeout(t.Context(), 900*time.Millisecond)
	defer cancel()
	err = cfg.Close(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, []string{"metrics", "traces", "logs"}, order)
	require.Equal(t, connectivity.Shutdown, cc.GetState())
}

func TestShutdownBudgetSplitsRemainingTime(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	sub, subCancel := shutdownBudget(ctx, 3)
	defer subCancel()
	dl, ok := sub.Deadline()
	require.True(t, ok)
	require.InDelta(t, time.Second, time.Until(dl), float64(100*time.Millisecond))

	noDeadline, ndCancel := shutdownBudget(t.Context(), 3)
	defer ndCancel()
	_, ok = noDeadline.Deadline()
	require.False(t, ok)
}

// TestMetricsInitFailureDoesNotAbort checks that a metrics exporter that
// cannot be created leaves the run going with traces.
func TestMetricsInitFailureDoesNotAbort(t *testing.T) {
	isolateGlobals(t)
	prevNew := newMetricExporter
	newMetricExporter = func(context.Context, *grpc.ClientConn) (sdkmetric.Exporter, error) {
		return nil, errors.New("boom")
	}
	t.Cleanup(func() { newMetricExporter = prevNew })

	r, addr := startReceiver(t)
	prevMP := otel.GetMeterProvider()
	cfg := newConfig(WithServiceName("uotel-test"), WithInsecureOtelEndpoint(addr))
	ctx, err := cfg.init(t.Context())
	require.NoError(t, err)
	require.NotNil(t, ctx)
	require.Same(t, prevMP, otel.GetMeterProvider())

	emitAndClose(t, ctx, cfg)
	require.ElementsMatch(t, testSpanNames, r.spanNames())
	require.Empty(t, r.receivedMetrics())
}

func TestErrorHandlerDeduplicates(t *testing.T) {
	core, logs := observer.New(zapcore.DebugLevel)
	t.Cleanup(zap.ReplaceGlobals(zap.New(core)))

	now := time.Unix(1000, 0)
	h := newDedupErrorHandler()
	h.now = func() time.Time { return now }
	h.active.Store(1)

	boom := errors.New("export failed: connection refused")
	h.Handle(boom)
	h.Handle(boom)
	h.Handle(boom)
	h.Handle(errors.New("another error"))
	require.Equal(t, 2, logs.Len(), "repeats within the window must be suppressed")

	now = now.Add(errorLogWindow)
	h.Handle(boom)
	entries := logs.All()
	require.Len(t, entries, 3)
	last := entries[2]
	require.Equal(t, zapcore.WarnLevel, last.Level)
	require.Equal(t, int64(2), last.ContextMap()["suppressed_repeats"])

	// Inactive (no initialized config): nothing goes to zap.
	h.active.Store(0)
	h.Handle(errors.New("after close"))
	require.Equal(t, 3, logs.Len())
}

func TestErrorHandlerBoundsKeys(t *testing.T) {
	h := newDedupErrorHandler()
	for i := range maxErrorKeys + 10 {
		_, ok := h.record(time.Duration(i).String())
		require.True(t, ok)
	}
	require.Len(t, h.entries, maxErrorKeys)
}
