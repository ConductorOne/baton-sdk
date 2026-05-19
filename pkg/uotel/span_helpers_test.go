package uotel

import (
	"context"
	"errors"
	"fmt"
	"testing"

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

func TestStartLinkedRoot(t *testing.T) {
	rec := &recordingProcessor{}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	tracer := tp.Tracer("test")

	t.Run("with parent span produces new root with link", func(t *testing.T) {
		rec.ended = nil

		ctx, parent := tracer.Start(context.Background(), "parent")
		parentTraceID := parent.SpanContext().TraceID()

		_, child := StartLinkedRoot(ctx, tracer, "child")
		child.End()
		parent.End()

		var childSpan sdktrace.ReadOnlySpan
		for _, s := range rec.ended {
			if s.Name() == "child" {
				childSpan = s
			}
		}
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
		rec.ended = nil

		_, span := StartLinkedRoot(context.Background(), tracer, "orphan")
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
}

// Reference trace package to make the import explicit beyond the embedded use.
var _ = trace.SpanContextFromContext
