package uotel

import (
	"context"
	"errors"
	"fmt"
	"testing"

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
			if got != tt.expected {
				t.Errorf("IsExpectedError(%v) = %v, want %v", tt.err, got, tt.expected)
			}
		})
	}
}

func TestParentCtxErrLabelRoundtrip(t *testing.T) {
	base := context.Background()
	if v, ok := ParentCtxErrLabel(base); ok || v != "" {
		t.Fatalf("empty ctx should not carry a label, got (%q, %v)", v, ok)
	}
	tagged := WithParentCtxErrLabel(base, "canceled")
	got, ok := ParentCtxErrLabel(tagged)
	if !ok || got != "canceled" {
		t.Fatalf("ParentCtxErrLabel = (%q, %v), want (\"canceled\", true)", got, ok)
	}
	// Survives context.WithoutCancel — that's the whole point.
	detached := context.WithoutCancel(tagged)
	got, ok = ParentCtxErrLabel(detached)
	if !ok || got != "canceled" {
		t.Fatalf("label lost across WithoutCancel: got (%q, %v)", got, ok)
	}
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
		if childSpan == nil {
			t.Fatal("child span not recorded")
		}
		if childSpan.SpanContext().TraceID() == parentTraceID {
			t.Errorf("expected new trace ID, got same as parent: %s", parentTraceID)
		}
		if childSpan.Parent().IsValid() {
			t.Errorf("expected no parent span ID, got %s", childSpan.Parent().SpanID())
		}
		links := childSpan.Links()
		if len(links) != 1 {
			t.Fatalf("expected 1 link to parent, got %d", len(links))
		}
		if links[0].SpanContext.TraceID() != parentTraceID {
			t.Errorf("expected link to parent trace %s, got %s", parentTraceID, links[0].SpanContext.TraceID())
		}
	})

	t.Run("without parent span behaves like normal Start", func(t *testing.T) {
		rec := &recordingProcessor{}
		tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
		tracer := tp.Tracer("test")

		ctx := context.Background()
		if trace.SpanContextFromContext(ctx).IsValid() {
			t.Fatal("background context should not carry a valid span context")
		}

		_, span := StartWithLink(ctx, tracer, "orphan")
		span.End()

		if len(rec.ended) != 1 {
			t.Fatalf("expected 1 span, got %d", len(rec.ended))
		}
		if len(rec.ended[0].Links()) != 0 {
			t.Errorf("expected no links for orphan span, got %d", len(rec.ended[0].Links()))
		}
		if !rec.ended[0].SpanContext().IsValid() {
			t.Errorf("expected valid span context")
		}
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
		if childSpan == nil {
			t.Fatal("child span not recorded")
		}

		var gotCustom string
		for _, attr := range childSpan.Attributes() {
			if attr.Key == "custom" {
				gotCustom = attr.Value.AsString()
			}
		}
		if gotCustom != "val" {
			t.Errorf("expected attribute custom=val to pass through, got %q", gotCustom)
		}
		if childSpan.SpanKind() != trace.SpanKindClient {
			t.Errorf("expected SpanKindClient, got %v", childSpan.SpanKind())
		}
		if len(childSpan.Links()) != 1 {
			t.Errorf("expected 1 link (parent), got %d", len(childSpan.Links()))
		}
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
		if childSpan == nil {
			t.Fatal("child span not recorded")
		}
		links := childSpan.Links()
		if len(links) != 2 {
			t.Fatalf("expected 2 links (caller-supplied + parent), got %d", len(links))
		}
		var sawOther, sawParent bool
		for _, l := range links {
			switch l.SpanContext.TraceID() {
			case otherSC.TraceID():
				sawOther = true
			case parentTraceID:
				sawParent = true
			}
		}
		if !sawOther {
			t.Errorf("expected caller-supplied link to be preserved")
		}
		if !sawParent {
			t.Errorf("expected parent link to be added")
		}
	})
}
