package uotelzap

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/trace"
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
	spanID, err := trace.SpanIDFromHex("0123456789abcdef")
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
	wantSpan := "81985529216486895"
	if fields[0].Key != TraceIdLogKey || fields[0].String != wantTrace {
		t.Fatalf("trace field = %q=%q, want %q=%q", fields[0].Key, fields[0].String, TraceIdLogKey, wantTrace)
	}
	if fields[1].Key != SpanIdLogKey || fields[1].String != wantSpan {
		t.Fatalf("span field = %q=%q, want %q=%q", fields[1].Key, fields[1].String, SpanIdLogKey, wantSpan)
	}

	ctx := trace.ContextWithSpanContext(context.Background(), sc)
	if got := LogFieldsFromContext(ctx); len(got) != 2 {
		t.Fatalf("LogFieldsFromContext returned %d fields, want 2", len(got))
	}
}
