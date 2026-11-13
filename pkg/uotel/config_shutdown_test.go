package uotel

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"google.golang.org/grpc"
)

func TestCloseRunsShutdownsInInitOrder(t *testing.T) {
	cfg := newConfig(WithServiceName("uotel-test"))

	var order []string
	step := func(name string) func(context.Context) error {
		return func(context.Context) error {
			order = append(order, name)
			return nil
		}
	}
	cfg.shutdown = append(cfg.shutdown, step("logs"))
	cfg.shutdown = append(cfg.shutdown, step("traces"))
	cfg.shutdown = append(cfg.shutdown, step("metrics"))

	require.NoError(t, cfg.Close(t.Context()))
	require.Equal(t, []string{"logs", "traces", "metrics"}, order)
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
