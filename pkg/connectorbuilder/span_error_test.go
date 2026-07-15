package connectorbuilder

import (
	"context"
	"sync"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
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

// installSpanRecorder installs an in-memory span recorder as the global
// otel tracer provider, ONCE per test process, and returns it. A singleton
// because otel's global delegation is one-shot: the package-level tracer in
// connectorbuilder.go (captured at init via otel.Tracer) wires itself to
// the FIRST SetTracerProvider and never re-delegates — a second install
// would silently record nothing, and restoring the previous provider on
// cleanup would shut the only wired exporter down for every later test.
//
// The recorder accumulates spans across tests; assert on the LAST span of
// a name. Mutates global state; callers must not call t.Parallel().
func installSpanRecorder(t *testing.T) *recordingExporter {
	t.Helper()
	spanRecorderOnce.Do(func() {
		spanRecorderExp = &recordingExporter{}
		otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSyncer(spanRecorderExp)))
	})
	return spanRecorderExp
}

var (
	spanRecorderOnce sync.Once
	spanRecorderExp  *recordingExporter
)

// TestSpanErrorRecorded_EarlyReturns is the regression test for OPS-1543.
// Pre-fix, ~30 early-return error paths in pkg/connectorbuilder shadowed
// the function-scope err that the deferred uotel.EndSpanWithError reads,
// so spans ended status:ok despite the function returning an error. The
// bug was invisible to all existing tests because they asserted on the
// returned error value but never inspected the span. This test exercises
// one representative path per bug-pattern class: err := inside if-block
// with InvalidArgument, err := inside if-block with NotFound, and direct
// return (no outer assignment) with NotFound.
// TestSpanOKOnDeferredProtocolTurn is the inverse regression: a deferred
// source-cache lookup (ask/answer bounce) is a PROTOCOL TURN — the RPC
// succeeds — but the handler's ErrLookupDeferred used to linger in the
// function-scoped err that the deferred EndSpanWithError reads, marking
// every normal bounce as a trace error. The span must end status:ok.
func TestSpanOKOnDeferredProtocolTurn(t *testing.T) {
	ctx := context.Background()
	exp := installSpanRecorder(t)

	ts := &continuationTestSyncer{testResourceSyncerV2Simple: testResourceSyncerV2Simple{resourceType: "group"}, scope: "groups/g1/members"}
	b := newContinuationTestBuilder(t, ts)

	offer := annotations.New(&v2.SourceCacheLookupOffer{})
	resp, err := b.ListGrants(ctx, continuationGrantsRequest(offer))
	require.NoError(t, err, "a deferral is a protocol turn, not a failure")
	respAnnos := annotations.Annotations(resp.GetAnnotations())
	require.True(t, respAnnos.Contains(&v2.SourceCacheLookupAsk{}), "sanity: this must be the ask turn")

	spans := exp.spansByName("builder.ListGrants")
	require.NotEmpty(t, spans)
	s := spans[len(spans)-1]
	require.NotEqual(t, otelcodes.Error, s.Status().Code,
		"a deferred ask/answer turn must not record a span error; ErrLookupDeferred must be cleared before the deferred EndSpanWithError reads err")
}

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
