package uotel

import (
	"context"
	"errors"

	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	grpccodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// StartWithLink starts a new root span linked to the current span in ctx.
//
// When ctx already carries a valid span context, the new span is started with
// trace.WithNewRoot() and a trace.Link back to that span. This breaks the
// causal chain into a new trace while preserving navigability via the link.
//
// Prefer plain tracer.Start for ordinary nested spans. Reach for this only
// when an outer span would otherwise accumulate hundreds or thousands of
// children — e.g. a long-running loop that processes each item via a chain
// of helper spans. Mirrors ctxotel.StartWithLink in the ConductorOne
// platform repo.
//
// Safe to call with the no-op tracer or with a ctx that has no span: the
// link is omitted and behavior matches tracer.Start. The returned ctx
// carries the new span, so descendants attach to the new root.
func StartWithLink(
	ctx context.Context,
	tracer trace.Tracer,
	name string,
	opts ...trace.SpanStartOption,
) (context.Context, trace.Span) {
	parentSpanCtx := trace.SpanContextFromContext(ctx)
	if parentSpanCtx.IsValid() {
		opts = append(opts, trace.WithNewRoot(), trace.WithLinks(trace.Link{SpanContext: parentSpanCtx}))
	}
	return tracer.Start(ctx, name, opts...)
}

// IsExpectedError returns true for errors that are expected during normal
// operation and should not be recorded as span errors.
func IsExpectedError(err error) bool {
	if err == nil {
		return true
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if s, ok := status.FromError(err); ok {
		switch s.Code() {
		case grpccodes.Canceled, grpccodes.DeadlineExceeded:
			return true
		default:
		}
	}
	return false
}

// EndSpanWithError ends a span and records the error if it is non-nil and not
// an expected error. Expected errors (context cancellation, deadline exceeded)
// are recorded with an Unset status to avoid polluting error dashboards.
func EndSpanWithError(span trace.Span, err error) {
	if err != nil {
		if IsExpectedError(err) {
			span.SetStatus(otelcodes.Unset, err.Error())
		} else {
			span.RecordError(err)
			span.SetStatus(otelcodes.Error, err.Error())
		}
	}
	span.End()
}
