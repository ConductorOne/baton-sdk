package uotel

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestIsExpectedError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{"nil error", nil, true},
		{"context canceled", context.Canceled, true},
		{"context deadline exceeded", context.DeadlineExceeded, true},
		{"wrapped context canceled", fmt.Errorf("wrapped: %w", context.Canceled), true},
		{"grpc canceled", status.Error(codes.Canceled, "canceled"), true},
		{"grpc deadline exceeded", status.Error(codes.DeadlineExceeded, "deadline"), true},
		{"generic error", errors.New("something broke"), false},
		{"grpc not found", status.Error(codes.NotFound, "not found"), false},
		{"grpc internal", status.Error(codes.Internal, "internal"), false},
		// Joined errors: every child must be expected for the whole
		// to be expected. The regression we are guarding against is
		// errors.Join(context.Canceled, ioerr) being silently
		// downgraded to Unset span status, hiding the real ioerr.
		{"join of only expected errors", errors.Join(context.Canceled, context.DeadlineExceeded), true},
		{"join with a real error is not expected", errors.Join(context.Canceled, errors.New("upload failed")), false},
		{"join with only a real error is not expected", errors.Join(errors.New("upload failed")), false},
		{"nested join with a real error is not expected", errors.Join(errors.Join(context.Canceled, errors.New("network down"))), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsExpectedError(tt.err)
			require.Equal(t, tt.expected, got, "IsExpectedError(%v)", tt.err)
		})
	}
}

func TestParentCtxErrLabelRoundtrip(t *testing.T) {
	base := context.Background()
	v, ok := ParentCtxErrLabel(base)
	require.False(t, ok, "empty ctx should not carry a label")
	require.Empty(t, v)

	tagged := WithParentCtxErrLabel(base, "canceled")
	got, ok := ParentCtxErrLabel(tagged)
	require.True(t, ok)
	require.Equal(t, "canceled", got)

	// Survives context.WithoutCancel — that's the whole point.
	detached := context.WithoutCancel(tagged)
	got, ok = ParentCtxErrLabel(detached)
	require.True(t, ok, "label lost across WithoutCancel")
	require.Equal(t, "canceled", got)
}

// recordingProcessor is a minimal SpanProcessor that captures ReadOnlySpans
// so tests can assert on their links/parent without depending on
// otel/sdk/trace/tracetest (not vendored).
type recordingProcessor struct {
	ended []sdktrace.ReadOnlySpan
}

func (p *recordingProcessor) OnStart(context.Context, sdktrace.ReadWriteSpan) {}
func (p *recordingProcessor) OnEnd(s sdktrace.ReadOnlySpan)                   { p.ended = append(p.ended, s) }
func (p *recordingProcessor) Shutdown(context.Context) error                  { return nil }
func (p *recordingProcessor) ForceFlush(context.Context) error                { return nil }

// findSpan returns the first recorded span with the given name.
func findSpan(rec *recordingProcessor, name string) sdktrace.ReadOnlySpan {
	for _, s := range rec.ended {
		if s.Name() == name {
			return s
		}
	}
	return nil
}

func TestStartWithLink(t *testing.T) {
	t.Run("with parent span produces new root with link", func(t *testing.T) {
		rec := &recordingProcessor{}
		tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
		tracer := tp.Tracer("test")

		ctx, parent := tracer.Start(context.Background(), "parent")
		parentTraceID := parent.SpanContext().TraceID()

		_, child := StartWithLink(ctx, tracer, "child")
		child.End()
		parent.End()

		childSpan := findSpan(rec, "child")
		require.NotNil(t, childSpan, "child span not recorded")
		require.NotEqual(t, parentTraceID, childSpan.SpanContext().TraceID(), "expected new trace ID, got same as parent: %s", parentTraceID)
		require.False(t, childSpan.Parent().IsValid(), "expected no parent span ID, got %s", childSpan.Parent().SpanID())
		links := childSpan.Links()
		require.Len(t, links, 1, "expected 1 link to parent")
		require.Equal(t, parentTraceID, links[0].SpanContext.TraceID(), "expected link to parent trace %s, got %s", parentTraceID, links[0].SpanContext.TraceID())
	})

	t.Run("without parent span behaves like normal Start", func(t *testing.T) {
		rec := &recordingProcessor{}
		tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
		tracer := tp.Tracer("test")

		ctx := context.Background()
		require.False(t, trace.SpanContextFromContext(ctx).IsValid(), "background context should not carry a valid span context")

		_, span := StartWithLink(ctx, tracer, "orphan")
		span.End()

		require.Len(t, rec.ended, 1, "expected 1 span")
		require.Empty(t, rec.ended[0].Links(), "expected no links for orphan span")
		require.True(t, rec.ended[0].SpanContext().IsValid(), "expected valid span context")
	})

	t.Run("caller-supplied SpanStartOptions are preserved", func(t *testing.T) {
		rec := &recordingProcessor{}
		tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
		tracer := tp.Tracer("test")

		ctx, parent := tracer.Start(context.Background(), "parent")
		_, child := StartWithLink(ctx, tracer, "child",
			trace.WithAttributes(attribute.String("custom", "val")),
			trace.WithSpanKind(trace.SpanKindClient),
		)
		child.End()
		parent.End()

		childSpan := findSpan(rec, "child")
		require.NotNil(t, childSpan, "child span not recorded")

		var gotCustom string
		for _, attr := range childSpan.Attributes() {
			if attr.Key == "custom" {
				gotCustom = attr.Value.AsString()
			}
		}
		require.Equal(t, "val", gotCustom, "expected attribute custom=val to pass through")
		require.Equal(t, trace.SpanKindClient, childSpan.SpanKind())
		require.Len(t, childSpan.Links(), 1, "expected 1 link (parent)")
	})

	t.Run("caller-supplied links are additive with the parent link", func(t *testing.T) {
		rec := &recordingProcessor{}
		tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
		tracer := tp.Tracer("test")

		// Build a synthetic link target by starting and ending an unrelated span.
		_, other := tracer.Start(context.Background(), "other")
		otherSC := other.SpanContext()
		other.End()

		ctx, parent := tracer.Start(context.Background(), "parent")
		parentTraceID := parent.SpanContext().TraceID()

		_, child := StartWithLink(ctx, tracer, "child",
			trace.WithLinks(trace.Link{SpanContext: otherSC}),
		)
		child.End()
		parent.End()

		childSpan := findSpan(rec, "child")
		require.NotNil(t, childSpan, "child span not recorded")
		links := childSpan.Links()
		require.Len(t, links, 2, "expected 2 links (caller-supplied + parent)")
		var sawOther, sawParent bool
		for _, l := range links {
			switch l.SpanContext.TraceID() {
			case otherSC.TraceID():
				sawOther = true
			case parentTraceID:
				sawParent = true
			}
		}
		require.True(t, sawOther, "expected caller-supplied link to be preserved")
		require.True(t, sawParent, "expected parent link to be added")
	})
}
