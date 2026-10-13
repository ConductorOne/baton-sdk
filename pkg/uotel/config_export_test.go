package uotel

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
)

// otlpReceiver is an in-process OTLP/gRPC collector that counts the spans and
// log records it receives.
type otlpReceiver struct {
	coltracepb.UnimplementedTraceServiceServer

	mtx   sync.Mutex
	spans []string
	logs  []string
}

func (r *otlpReceiver) Export(_ context.Context, req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	for _, rs := range req.GetResourceSpans() {
		for _, ss := range rs.GetScopeSpans() {
			for _, s := range ss.GetSpans() {
				r.spans = append(r.spans, s.GetName())
			}
		}
	}
	return &coltracepb.ExportTraceServiceResponse{}, nil
}

func (r *otlpReceiver) spanNames() []string {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	return append([]string(nil), r.spans...)
}

func (r *otlpReceiver) logBodies() []string {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	return append([]string(nil), r.logs...)
}

// logsService adapts otlpReceiver to LogsServiceServer, whose Export method
// collides by name with TraceServiceServer.Export.
type logsService struct {
	collogspb.UnimplementedLogsServiceServer
	r *otlpReceiver
}

func (l logsService) Export(_ context.Context, req *collogspb.ExportLogsServiceRequest) (*collogspb.ExportLogsServiceResponse, error) {
	l.r.mtx.Lock()
	defer l.r.mtx.Unlock()
	for _, rl := range req.GetResourceLogs() {
		for _, sl := range rl.GetScopeLogs() {
			for _, lr := range sl.GetLogRecords() {
				l.r.logs = append(l.r.logs, lr.GetBody().GetStringValue())
			}
		}
	}
	return &collogspb.ExportLogsServiceResponse{}, nil
}

// startReceiver starts an OTLP/gRPC receiver on 127.0.0.1 and returns its address.
func startReceiver(t *testing.T) (*otlpReceiver, string) {
	t.Helper()
	lis, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	r := &otlpReceiver{}
	srv := grpc.NewServer()
	coltracepb.RegisterTraceServiceServer(srv, r)
	collogspb.RegisterLogsServiceServer(srv, logsService{r: r})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return r, lis.Addr().String()
}

// isolateGlobals gives the test a zap logger that enables Info, and restores the
// global zap logger and otel providers that init replaces.
func isolateGlobals(t *testing.T) {
	t.Helper()
	prevTP := otel.GetTracerProvider()
	prevProp := otel.GetTextMapPropagator()
	restoreZap := zap.ReplaceGlobals(zap.New(zapcore.NewCore(
		zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()), zapcore.AddSync(io.Discard), zapcore.DebugLevel)))
	t.Cleanup(func() {
		restoreZap()
		if otel.GetTracerProvider() != prevTP {
			otel.SetTracerProvider(prevTP)
		}
		if otel.GetTextMapPropagator() != prevProp {
			otel.SetTextMapPropagator(prevProp)
		}
	})
}

var (
	testSpanNames = []string{"span-1", "span-2", "span-3"}
	testLogBodies = []string{"log-1", "log-2", "log-3"}
)

// emitAndClose emits spans and logs through the globals installed by init, then
// calls Close before any batch interval elapses.
func emitAndClose(t *testing.T, ctx context.Context, cfg *otelConfig) {
	t.Helper()
	tracer := otel.Tracer("uotel-test")
	for _, name := range testSpanNames {
		_, span := tracer.Start(ctx, name)
		span.End()
	}
	for _, body := range testLogBodies {
		zap.L().Info(body)
	}
	closeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	require.NoError(t, cfg.Close(closeCtx))
}

func TestCloseFlushesSpansAndLogs(t *testing.T) {
	isolateGlobals(t)
	r, addr := startReceiver(t)
	ctx := t.Context()

	cfg := newConfig(WithServiceName("uotel-test"), WithInsecureOtelEndpoint(addr))
	ctx, err := cfg.init(ctx)
	require.NoError(t, err)

	emitAndClose(t, ctx, cfg)

	require.ElementsMatch(t, testSpanNames, r.spanNames())
	require.Subset(t, r.logBodies(), testLogBodies)
}

func TestLoggingDisabledExportsOnlySpans(t *testing.T) {
	isolateGlobals(t)
	r, addr := startReceiver(t)
	ctx := t.Context()

	cfg := newConfig(WithServiceName("uotel-test"), WithInsecureOtelEndpoint(addr), WithLoggingDisabled())
	ctx, err := cfg.init(ctx)
	require.NoError(t, err)

	emitAndClose(t, ctx, cfg)

	require.ElementsMatch(t, testSpanNames, r.spanNames())
	require.Empty(t, r.logBodies())
}

func TestTracingDisabledExportsOnlyLogs(t *testing.T) {
	isolateGlobals(t)
	r, addr := startReceiver(t)
	ctx := t.Context()

	cfg := newConfig(WithServiceName("uotel-test"), WithInsecureOtelEndpoint(addr), WithTracingDisabled())
	ctx, err := cfg.init(ctx)
	require.NoError(t, err)

	emitAndClose(t, ctx, cfg)

	require.Empty(t, r.spanNames())
	require.Subset(t, r.logBodies(), testLogBodies)
}

func TestBothDisabledCreatesNoConnection(t *testing.T) {
	isolateGlobals(t)
	_, addr := startReceiver(t)

	cfg := newConfig(WithServiceName("uotel-test"), WithInsecureOtelEndpoint(addr), WithLoggingDisabled(), WithTracingDisabled())
	_, err := cfg.init(t.Context())
	require.NoError(t, err)

	require.Empty(t, cfg.c)
	require.Empty(t, cfg.shutdown)
	require.NoError(t, cfg.Close(t.Context()))
}
