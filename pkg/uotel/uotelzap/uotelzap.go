// Package uotelzap converts OpenTelemetry span context into zap log fields so
// log lines can be correlated with traces in the OTel-to-Datadog pipeline. It
// is intentionally kept dependency-light (otel/trace + zap only) so callers
// can import it without pulling in the full OTel SDK exporters held by
// pkg/uotel.
package uotelzap

import (
	"context"
	"strconv"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

const (
	// TraceIDLogKey is the zap field name Datadog uses to correlate logs with
	// traces when the SDK is running under the OTel-to-Datadog pipeline.
	TraceIDLogKey = "dd.trace_id"
	// SpanIDLogKey is the zap field name Datadog uses to correlate logs with
	// spans when the SDK is running under the OTel-to-Datadog pipeline.
	SpanIDLogKey = "dd.span_id"
)

// ConvertTraceID converts an OTel hex trace or span id to the decimal form
// Datadog expects for log<->trace correlation.
// https://docs.datadoghq.com/tracing/connect_logs_and_traces/opentelemetry/
//
// For 128-bit OTel trace IDs the low 64 bits are returned, matching Datadog's
// OTel ingestion behavior; span IDs are already 64 bits and are passed through.
// Returns "" for inputs shorter than 16 hex chars or that fail to parse — most
// callers should prefer SpanToLogFields, which gates on SpanContext validity
// before getting here.
func ConvertTraceID(id string) string {
	if len(id) < 16 {
		return ""
	}
	if len(id) > 16 {
		id = id[16:]
	}
	intValue, err := strconv.ParseUint(id, 16, 64)
	if err != nil {
		return ""
	}
	return strconv.FormatUint(intValue, 10)
}

// SpanToLogFields returns zap fields that associate a log line with the given
// span. Returns nil when the span context is not valid so callers can safely
// append the result unconditionally.
func SpanToLogFields(spanContext trace.SpanContext) []zap.Field {
	if !spanContext.IsValid() {
		return nil
	}
	return []zap.Field{
		zap.String(TraceIDLogKey, ConvertTraceID(spanContext.TraceID().String())),
		zap.String(SpanIDLogKey, ConvertTraceID(spanContext.SpanID().String())),
	}
}

// LogFieldsFromContext is a convenience for SpanToLogFields(trace.SpanContextFromContext(ctx)).
func LogFieldsFromContext(ctx context.Context) []zap.Field {
	return SpanToLogFields(trace.SpanContextFromContext(ctx))
}

// WithSpanLogFields enriches the ctx-scoped zap logger with the trace_id and
// span_id of the active OTel span so subsequent log lines correlate with the
// surrounding trace. It is a no-op when there is no valid span on ctx (in that
// case the ctx is returned unchanged). Call it immediately after starting a
// new root span so descendant log lines inherit the fields.
func WithSpanLogFields(ctx context.Context) context.Context {
	fields := LogFieldsFromContext(ctx)
	if fields == nil {
		return ctx
	}
	return ctxzap.ToContext(ctx, ctxzap.Extract(ctx).With(fields...))
}
