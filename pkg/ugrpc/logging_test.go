package ugrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
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
	if err != nil {
		t.Fatalf("TraceIDFromHex: %v", err)
	}
	spanID, err := trace.SpanIDFromHex("fedcba9876543210")
	if err != nil {
		t.Fatalf("SpanIDFromHex: %v", err)
	}
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
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("unmarshal log entry: %v\nentry: %s", err, buf.String())
	}
	if got, want := entry["dd.trace_id"], "81985529216486895"; got != want {
		t.Errorf("dd.trace_id = %v, want %v", got, want)
	}
	if got, want := entry["dd.span_id"], "18364758544493064720"; got != want {
		t.Errorf("dd.span_id = %v, want %v", got, want)
	}
	if got, want := entry["grpc.service"], "svc.v1.Svc"; got != want {
		t.Errorf("grpc.service = %v, want %v", got, want)
	}
	if got, want := entry["grpc.method"], "Method"; got != want {
		t.Errorf("grpc.method = %v, want %v", got, want)
	}
}

func TestNewLoggerForCall_NoTraceFieldsWhenSpanAbsent(t *testing.T) {
	var buf bytes.Buffer
	base := newCaptureLogger(&buf)
	newCtx := newLoggerForCall(context.Background(), base, "/svc.v1.Svc/Method", time.Unix(0, 0))
	ctxzap.Extract(newCtx).Info("hello")

	var entry map[string]any
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("unmarshal log entry: %v\nentry: %s", err, buf.String())
	}
	if _, ok := entry["dd.trace_id"]; ok {
		t.Errorf("dd.trace_id should not be set when no span on ctx; entry=%v", entry)
	}
	if _, ok := entry["dd.span_id"]; ok {
		t.Errorf("dd.span_id should not be set when no span on ctx; entry=%v", entry)
	}
}
