package local

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	otelcodes "go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/grpc"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/types"
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
// recorder for the duration of the test. Same pattern as
// pkg/connectorbuilder/span_error_test.go's installSpanRecorder.
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

// erroringListEventsClient succeeds on every call before failOn (returning an
// advancing cursor with HasMore true), then fails on call number failOn.
type erroringListEventsClient struct {
	types.ConnectorClient
	failOn int
	calls  int
	err    error
}

func (f *erroringListEventsClient) ListEvents(
	_ context.Context,
	_ *v2.ListEventsRequest,
	_ ...grpc.CallOption,
) (*v2.ListEventsResponse, error) {
	f.calls++
	if f.calls == f.failOn {
		return nil, f.err
	}
	return v2.ListEventsResponse_builder{
		Cursor:  "cursor-1",
		HasMore: true,
	}.Build(), nil
}

// TestEventFeed_Process_ListEventsFailure is the regression test for two
// defects in the failure path of Process: (1) `resp, err := cc.ListEvents(...)`
// shadowed the function-scope err that the deferred uotel.EndSpanWithError
// reads, so the span ended status:ok even though Process returned an error
// (the same bug class as OPS-1543, see pkg/connectorbuilder/span_error_test.go);
// and (2) the failure path logged nothing, so a mid-pagination failure (the
// CXP-533 shape) gave no page number or cursor to debug from.
func TestEventFeed_Process_ListEventsFailure(t *testing.T) {
	exp := installSpanRecorder(t)
	ctx, logs := observedLogger(t)

	wantErr := errors.New("upstream page fetch timed out")
	cc := &erroringListEventsClient{failOn: 2, err: wantErr}

	mgr := NewEventFeed(ctx, "feed", time.Now(), "")
	task, _, err := mgr.Next(ctx)
	require.NoError(t, err)

	processErr := mgr.Process(ctx, task, cc)
	require.ErrorIs(t, processErr, wantErr)

	spans := exp.spansByName("localEventFeed.Process")
	require.NotEmpty(t, spans, "expected a localEventFeed.Process span")
	require.Equal(t, otelcodes.Error, spans[len(spans)-1].Status().Code,
		"span must end with status:error; pre-fix the deferred EndSpanWithError saw a shadowed nil err")

	failures := logs.filterMessage("event feed page failed")
	require.Len(t, failures, 1)
	require.EqualValues(t, 2, failures[0].Fields["page"])
	require.Equal(t, "cursor-1", failures[0].Fields["cursor"])
	require.Contains(t, failures[0].Fields, "duration_ms")
	require.Equal(t, wantErr.Error(), failures[0].Fields["error"])
}
