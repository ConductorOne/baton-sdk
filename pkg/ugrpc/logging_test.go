package ugrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// newCaptureLogger returns a logger that writes JSON-encoded entries to buf so
// tests can assert that specific fields land on log lines.
func newCaptureLogger(buf *bytes.Buffer) *zap.Logger {
	encCfg := zap.NewProductionEncoderConfig()
	encCfg.TimeKey = "" // keep test output deterministic
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(encCfg),
		zapcore.AddSync(buf),
		zap.DebugLevel,
	)
	return zap.New(core)
}

func TestNewLoggerForCall_AttachesTraceFieldsWhenSpanPresent(t *testing.T) {
	traceID, err := trace.TraceIDFromHex("0123456789abcdef0123456789abcdef")
	require.NoError(t, err)
	spanID, err := trace.SpanIDFromHex("fedcba9876543210")
	require.NoError(t, err)
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)

	var buf bytes.Buffer
	base := newCaptureLogger(&buf)
	newCtx := newLoggerForCall(ctx, base, "/svc.v1.Svc/Method", time.Unix(0, 0))
	ctxzap.Extract(newCtx).Info("hello")

	var entry map[string]any
	err = json.Unmarshal(buf.Bytes(), &entry)
	require.NoError(t, err, "unmarshal log entry: %s", buf.String())
	require.Equal(t, "81985529216486895", entry["dd.trace_id"])
	require.Equal(t, "18364758544493064720", entry["dd.span_id"])
	require.Equal(t, "svc.v1.Svc", entry["grpc.service"])
	require.Equal(t, "Method", entry["grpc.method"])
}

func TestNewLoggerForCall_NoTraceFieldsWhenSpanAbsent(t *testing.T) {
	var buf bytes.Buffer
	base := newCaptureLogger(&buf)
	newCtx := newLoggerForCall(context.Background(), base, "/svc.v1.Svc/Method", time.Unix(0, 0))
	ctxzap.Extract(newCtx).Info("hello")

	var entry map[string]any
	err := json.Unmarshal(buf.Bytes(), &entry)
	require.NoError(t, err, "unmarshal log entry: %s", buf.String())
	_, ok := entry["dd.trace_id"]
	require.False(t, ok, "dd.trace_id should not be set when no span on ctx; entry=%v", entry)
	_, ok = entry["dd.span_id"]
	require.False(t, ok, "dd.span_id should not be set when no span on ctx; entry=%v", entry)
}
