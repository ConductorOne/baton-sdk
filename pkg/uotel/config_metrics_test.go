package uotel

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/grpc/metadata"

	"github.com/conductorone/baton-sdk/pkg/metrics"
	"github.com/conductorone/baton-sdk/pkg/types/tasks"
)

// receivedMetric is one data point's metric name and string attributes.
type receivedMetric struct {
	name   string
	kind   string // sum, gauge or histogram
	attrs  map[string]string
	header string // value of the x-test-header gRPC metadata on the export call
}

// metricsService adapts otlpReceiver to MetricsServiceServer, whose Export
// method collides by name with TraceServiceServer.Export.
type metricsService struct {
	colmetricspb.UnimplementedMetricsServiceServer
	r *otlpReceiver
}

func (m metricsService) Export(ctx context.Context, req *colmetricspb.ExportMetricsServiceRequest) (*colmetricspb.ExportMetricsServiceResponse, error) {
	var header string
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if v := md.Get("x-test-header"); len(v) > 0 {
			header = v[0]
		}
	}
	m.r.mtx.Lock()
	defer m.r.mtx.Unlock()
	for _, rm := range req.GetResourceMetrics() {
		for _, sm := range rm.GetScopeMetrics() {
			for _, met := range sm.GetMetrics() {
				kind, points := dataPointAttrs(met)
				for _, attrs := range points {
					m.r.metrics = append(m.r.metrics, receivedMetric{name: met.GetName(), kind: kind, attrs: attrs, header: header})
				}
			}
		}
	}
	return &colmetricspb.ExportMetricsServiceResponse{}, nil
}

func dataPointAttrs(m *metricspb.Metric) (string, []map[string]string) {
	var out []map[string]string
	switch {
	case m.GetSum() != nil:
		for _, dp := range m.GetSum().GetDataPoints() {
			out = append(out, kvMap(dp.GetAttributes()))
		}
		return "sum", out
	case m.GetGauge() != nil:
		for _, dp := range m.GetGauge().GetDataPoints() {
			out = append(out, kvMap(dp.GetAttributes()))
		}
		return "gauge", out
	case m.GetHistogram() != nil:
		for _, dp := range m.GetHistogram().GetDataPoints() {
			out = append(out, kvMap(dp.GetAttributes()))
		}
		return "histogram", out
	}
	return "other", out
}

func kvMap(kvs []*commonpb.KeyValue) map[string]string {
	out := make(map[string]string, len(kvs))
	for _, kv := range kvs {
		out[kv.GetKey()] = kv.GetValue().GetStringValue()
	}
	return out
}

func (r *otlpReceiver) receivedMetrics() []receivedMetric {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	return append([]receivedMetric(nil), r.metrics...)
}

func findMetric(ms []receivedMetric, name string) (receivedMetric, bool) {
	for _, m := range ms {
		if m.name == name {
			return m, true
		}
	}
	return receivedMetric{}, false
}

// emitMetricsAndClose records SDK metrics through a handler on the installed
// MeterProvider and a plain global meter, then calls Close long before the
// export interval elapses.
func emitMetricsAndClose(t *testing.T, ctx context.Context, cfg *otelConfig) {
	t.Helper()
	m := metrics.New(installedProviderHandler(ctx))
	m.RecordTaskSuccess(ctx, tasks.FullSyncType, 12*time.Millisecond)

	counter, err := otel.Meter("uotel-test").Int64Counter("uotel_test_counter")
	require.NoError(t, err)
	counter.Add(ctx, 3)

	closeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	require.NoError(t, cfg.Close(closeCtx))
}

// installedProviderHandler returns a handler on the MeterProvider init
// installed. metrics.NewDefaultHandler records only to the first provider the
// test binary installs, which an earlier test has already shut down.
func installedProviderHandler(ctx context.Context) metrics.Handler {
	return metrics.NewOtelHandler(ctx, otel.GetMeterProvider(), metrics.DefaultHandlerName)
}

func TestCloseFlushesMetrics(t *testing.T) {
	isolateGlobals(t)
	t.Setenv("OTEL_EXPORTER_OTLP_HEADERS", "x-test-header=hello")
	r, addr := startReceiver(t)
	ctx := t.Context()

	// An hour-long interval means only the final collection in Close can export.
	cfg := newConfig(WithServiceName("uotel-test"), WithInsecureOtelEndpoint(addr), WithMetricsExportInterval(time.Hour))
	ctx, err := cfg.init(ctx)
	require.NoError(t, err)

	emitMetricsAndClose(t, ctx, cfg)

	got := r.receivedMetrics()
	success, ok := findMetric(got, "baton_sdk.task_success")
	require.True(t, ok, "task_success not exported: %v", got)
	require.Equal(t, "sum", success.kind)
	require.Equal(t, tasks.FullSyncType.String(), success.attrs["task_type"])
	require.Equal(t, "hello", success.header, "OTEL_EXPORTER_OTLP_HEADERS not sent on the shared conn")

	latency, ok := findMetric(got, "baton_sdk.task_latency")
	require.True(t, ok, "task_latency not exported: %v", got)
	require.Equal(t, "histogram", latency.kind)
	require.Equal(t, "success", latency.attrs["task_status"])

	_, ok = findMetric(got, "uotel_test_counter")
	require.True(t, ok, "global meter counter not exported: %v", got)
}

func TestMetricsIntervalFromEnv(t *testing.T) {
	isolateGlobals(t)
	t.Setenv("OTEL_METRIC_EXPORT_INTERVAL", "50")
	r, addr := startReceiver(t)
	ctx := t.Context()

	cfg := newConfig(WithServiceName("uotel-test"), WithInsecureOtelEndpoint(addr))
	ctx, err := cfg.init(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cfg.Close(context.Background()) })

	metrics.New(installedProviderHandler(ctx)).RecordTaskSuccess(ctx, tasks.FullSyncType, time.Millisecond)

	// Exported by the periodic reader, before any Close.
	require.Eventually(t, func() bool {
		_, ok := findMetric(r.receivedMetrics(), "baton_sdk.task_success")
		return ok
	}, 5*time.Second, 20*time.Millisecond)
}

func TestMetricsDisabledExportsNoMetrics(t *testing.T) {
	isolateGlobals(t)
	r, addr := startReceiver(t)
	ctx := t.Context()

	prevMP := otel.GetMeterProvider()
	cfg := newConfig(WithServiceName("uotel-test"), WithInsecureOtelEndpoint(addr), WithMetricsDisabled())
	ctx, err := cfg.init(ctx)
	require.NoError(t, err)
	require.Same(t, prevMP, otel.GetMeterProvider())

	emitAndClose(t, ctx, cfg)

	require.Empty(t, r.receivedMetrics())
	require.ElementsMatch(t, testSpanNames, r.spanNames())
}

func TestNoEndpointInstallsNoMeterProvider(t *testing.T) {
	isolateGlobals(t)
	prevMP := otel.GetMeterProvider()

	cfg := newConfig(WithServiceName("uotel-test"))
	_, err := cfg.init(t.Context())
	require.NoError(t, err)

	require.Same(t, prevMP, otel.GetMeterProvider())
	require.Empty(t, cfg.shutdown)
	require.NoError(t, cfg.Close(t.Context()))
}
