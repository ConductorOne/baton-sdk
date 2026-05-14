package connectorbuilder

import (
	"context"
	"sync"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	otelcodes "go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// recordingExporter is a minimal in-memory SpanExporter for the regression
// test below. Spans are appended on export and looked up by name.
type recordingExporter struct {
	mu    sync.Mutex
	spans []sdktrace.ReadOnlySpan
}

func (r *recordingExporter) ExportSpans(_ context.Context, spans []sdktrace.ReadOnlySpan) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.spans = append(r.spans, spans...)
	return nil
}

func (r *recordingExporter) Shutdown(_ context.Context) error { return nil }

func (r *recordingExporter) spansByName(name string) []sdktrace.ReadOnlySpan {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []sdktrace.ReadOnlySpan
	for _, s := range r.spans {
		if s.Name() == name {
			out = append(out, s)
		}
	}
	return out
}

// installSpanRecorder swaps the global otel tracer provider for an in-memory
// recorder for the duration of the test. The package-level tracer var in
// connectorbuilder.go is captured at init via otel.Tracer(name), which
// returns a delegating wrapper that lazily routes through the current global
// provider — so this swap reaches the existing tracer references.
//
// Mutates global state; callers must not call t.Parallel().
func installSpanRecorder(t *testing.T) *recordingExporter {
	t.Helper()
	exp := &recordingExporter{}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		otel.SetTracerProvider(prev)
		_ = tp.Shutdown(context.Background())
	})
	return exp
}

// TestSpanErrorRecorded_EarlyReturns is the regression test for OPS-1543.
// Pre-fix, ~30 early-return error paths in pkg/connectorbuilder shadowed
// the function-scope err that the deferred uotel.EndSpanWithError reads,
// so spans ended status:ok despite the function returning an error. The
// bug was invisible to all existing tests because they asserted on the
// returned error value but never inspected the span. This test exercises
// one representative path per bug-pattern class: err := inside if-block
// with InvalidArgument, err := inside if-block with NotFound, and direct
// return (no outer assignment) with NotFound.
func TestSpanErrorRecorded_EarlyReturns(t *testing.T) {
	ctx := context.Background()
	exp := installSpanRecorder(t)

	connector, err := NewConnector(ctx, newTestConnector(nil))
	require.NoError(t, err)

	type call struct {
		name     string
		spanName string
		invoke   func() error
	}
	calls := []call{
		{
			// codes.InvalidArgument early-return — shadowed err in if-block.
			name:     "ListGrants nil resource",
			spanName: "builder.ListGrants",
			invoke: func() error {
				_, e := connector.ListGrants(ctx, &v2.GrantsServiceListGrantsRequest{})
				return e
			},
		},
		{
			// codes.NotFound early-return — shadowed err in if-block.
			name:     "ListResources unknown resource type",
			spanName: "builder.ListResources",
			invoke: func() error {
				req := v2.ResourcesServiceListResourcesRequest_builder{
					ResourceTypeId: "unknown",
				}.Build()
				_, e := connector.ListResources(ctx, req)
				return e
			},
		},
		{
			// codes.NotFound direct return — bypass path (no err assignment).
			name:     "ListEvents unknown feed",
			spanName: "builder.ListEvents",
			invoke: func() error {
				req := v2.ListEventsRequest_builder{
					EventFeedId: "unknown",
				}.Build()
				_, e := connector.ListEvents(ctx, req)
				return e
			},
		},
	}

	for _, c := range calls {
		t.Run(c.name, func(t *testing.T) {
			invokeErr := c.invoke()
			require.Error(t, invokeErr, "RPC should return an error")
			spans := exp.spansByName(c.spanName)
			require.NotEmpty(t, spans, "expected at least one %s span", c.spanName)
			s := spans[len(spans)-1]
			require.Equal(t, otelcodes.Error, s.Status().Code,
				"span %s must end with status:error; pre-OPS-1543 it incorrectly ended status:ok because the deferred EndSpanWithError saw a shadowed nil err",
				c.spanName)
		})
	}
}
