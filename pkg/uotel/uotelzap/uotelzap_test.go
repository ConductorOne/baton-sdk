package uotelzap

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestConvertTraceID(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"empty", "", ""},
		{"short", "abc", ""},
		{"invalid_hex", "zzzzzzzzzzzzzzzz", ""},
		{"16_lower_hex_max", "ffffffffffffffff", "18446744073709551615"},
		{"32_hex_uses_lower_64", "0123456789abcdef0123456789abcdef", "81985529216486895"},
		{"32_hex_zero_upper", "00000000000000000000000000000001", "1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ConvertTraceID(tc.in)
			if got != tc.want {
				t.Fatalf("ConvertTraceID(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestSpanToLogFields_InvalidSpanReturnsNil(t *testing.T) {
	if got := SpanToLogFields(trace.SpanContext{}); got != nil {
		t.Fatalf("expected nil for zero SpanContext, got %v", got)
	}
	if got := LogFieldsFromContext(context.Background()); got != nil {
		t.Fatalf("expected nil for context without span, got %v", got)
	}
}

func TestSpanToLogFields_ValidSpanProducesDDFields(t *testing.T) {
	traceID, err := trace.TraceIDFromHex("0123456789abcdef0123456789abcdef")
	if err != nil {
		t.Fatalf("TraceIDFromHex: %v", err)
	}
	// Pick a span ID with a distinct decimal form so a future regression that
	// swaps the trace/span fields can't pass silently.
	spanID, err := trace.SpanIDFromHex("fedcba9876543210")
	if err != nil {
		t.Fatalf("SpanIDFromHex: %v", err)
	}
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
	})

	fields := SpanToLogFields(sc)
	if len(fields) != 2 {
		t.Fatalf("expected 2 fields, got %d", len(fields))
	}
	wantTrace := "81985529216486895"
	wantSpan := "18364758544493064720"
	if fields[0].Key != TraceIDLogKey || fields[0].String != wantTrace {
		t.Fatalf("trace field = %q=%q, want %q=%q", fields[0].Key, fields[0].String, TraceIDLogKey, wantTrace)
	}
	if fields[1].Key != SpanIDLogKey || fields[1].String != wantSpan {
		t.Fatalf("span field = %q=%q, want %q=%q", fields[1].Key, fields[1].String, SpanIDLogKey, wantSpan)
	}

	ctx := trace.ContextWithSpanContext(context.Background(), sc)
	if got := LogFieldsFromContext(ctx); len(got) != 2 {
		t.Fatalf("LogFieldsFromContext returned %d fields, want 2", len(got))
	}
}

func TestWithSpanLogFields(t *testing.T) {
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

	t.Run("attaches_fields_when_span_present", func(t *testing.T) {
		var buf bytes.Buffer
		enc := zap.NewProductionEncoderConfig()
		enc.TimeKey = ""
		logger := zap.New(zapcore.NewCore(zapcore.NewJSONEncoder(enc), zapcore.AddSync(&buf), zap.DebugLevel))

		ctx := ctxzap.ToContext(trace.ContextWithSpanContext(context.Background(), sc), logger)
		ctx = WithSpanLogFields(ctx)
		ctxzap.Extract(ctx).Info("hello")

		var entry map[string]any
		if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
			t.Fatalf("unmarshal: %v\n%s", err, buf.String())
		}
		if entry[TraceIDLogKey] != "81985529216486895" {
			t.Errorf("trace = %v, want 81985529216486895", entry[TraceIDLogKey])
		}
		if entry[SpanIDLogKey] != "18364758544493064720" {
			t.Errorf("span = %v, want 18364758544493064720", entry[SpanIDLogKey])
		}
	})

	t.Run("no_op_without_span", func(t *testing.T) {
		ctx := context.Background()
		if WithSpanLogFields(ctx) != ctx {
			t.Errorf("expected ctx to be returned unchanged when no span")
		}
	})
}
