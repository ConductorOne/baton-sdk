package uotelzap

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/stretchr/testify/require"
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
			require.Equal(t, tc.want, got, "ConvertTraceID(%q)", tc.in)
		})
	}
}

func TestSpanToLogFields_InvalidSpanReturnsNil(t *testing.T) {
	require.Nil(t, SpanToLogFields(trace.SpanContext{}), "expected nil for zero SpanContext")
	require.Nil(t, LogFieldsFromContext(context.Background()), "expected nil for context without span")
}

func TestSpanToLogFields_ValidSpanProducesDDFields(t *testing.T) {
	traceID, err := trace.TraceIDFromHex("0123456789abcdef0123456789abcdef")
	require.NoError(t, err)
	// Pick a span ID with a distinct decimal form so a future regression that
	// swaps the trace/span fields can't pass silently.
	spanID, err := trace.SpanIDFromHex("fedcba9876543210")
	require.NoError(t, err)
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
	})

	fields := SpanToLogFields(sc)
	require.Len(t, fields, 2)
	wantTrace := "81985529216486895"
	wantSpan := "18364758544493064720"
	require.Equal(t, TraceIDLogKey, fields[0].Key)
	require.Equal(t, wantTrace, fields[0].String)
	require.Equal(t, SpanIDLogKey, fields[1].Key)
	require.Equal(t, wantSpan, fields[1].String)

	ctx := trace.ContextWithSpanContext(context.Background(), sc)
	require.Len(t, LogFieldsFromContext(ctx), 2)
}

func TestWithSpanLogFields(t *testing.T) {
	traceID, err := trace.TraceIDFromHex("0123456789abcdef0123456789abcdef")
	require.NoError(t, err)
	spanID, err := trace.SpanIDFromHex("fedcba9876543210")
	require.NoError(t, err)
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
		err := json.Unmarshal(buf.Bytes(), &entry)
		require.NoError(t, err, "unmarshal: %s", buf.String())
		require.Equal(t, "81985529216486895", entry[TraceIDLogKey], "trace")
		require.Equal(t, "18364758544493064720", entry[SpanIDLogKey], "span")
	})

	t.Run("no_op_without_span", func(t *testing.T) {
		ctx := context.Background()
		require.True(t, WithSpanLogFields(ctx) == ctx, "expected ctx to be returned unchanged when no span")
	})
}
