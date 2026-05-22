package expand

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// recordingProcessor captures ReadOnlySpans for assertion. Mirrors the helper
// in pkg/uotel/span_helpers_test.go (tracetest is not vendored).
type recordingProcessor struct {
	ended []sdktrace.ReadOnlySpan
}

func (p *recordingProcessor) OnStart(context.Context, sdktrace.ReadWriteSpan) {}
func (p *recordingProcessor) OnEnd(s sdktrace.ReadOnlySpan)                   { p.ended = append(p.ended, s) }
func (p *recordingProcessor) Shutdown(context.Context) error                  { return nil }
func (p *recordingProcessor) ForceFlush(context.Context) error                { return nil }

// setupRecordingTracer installs a TracerProvider with a recording processor
// for the duration of the test, and rebinds the package-level expander tracer
// to it. Restoration on t.Cleanup keeps test isolation.
func setupRecordingTracer(t *testing.T) *recordingProcessor {
	t.Helper()
	rec := &recordingProcessor{}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))

	prevProvider := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)

	prevTracer := tracer
	tracer = tp.Tracer("baton-sdk/sync.expand")

	t.Cleanup(func() {
		tracer = prevTracer
		otel.SetTracerProvider(prevProvider)
	})
	return rec
}

// TestRunActionEmitsLinkedRootSpan locks in the contract that
// Expander.runAction emits a new root span with a link back to the calling
// span and the documented attributes. A regression in the span name, link
// shape, or attribute keys would silently break Datadog dashboards.
func TestRunActionEmitsLinkedRootSpan(t *testing.T) {
	rec := setupRecordingTracer(t)

	// Same scenario as TestExpanderWithMockStore: A → B, Alice has A.
	store := NewMockExpanderStore()
	groupResource := makeResource("group", "admins")
	userResource := makeResource("user", "alice")
	entA := makeEntitlement("ent:group:admins:member", groupResource)
	entB := makeEntitlement("ent:role:admin", groupResource)
	store.AddEntitlement(entA)
	store.AddEntitlement(entB)
	store.AddGrant(makeGrant("grant:alice:member", entA, userResource))

	// Start a parent span so the new root has something to link back to.
	parentCtx, parent := otel.Tracer("test").Start(context.Background(), "test.parent")
	parentTraceID := parent.SpanContext().TraceID()

	graph := NewEntitlementGraph(parentCtx)
	graph.AddEntitlementID(entA.GetId())
	graph.AddEntitlementID(entB.GetId())
	require.NoError(t, graph.AddEdge(parentCtx, entA.GetId(), entB.GetId(), false, []string{"user"}))

	expander := NewExpander(store, graph)
	require.NoError(t, expander.Run(parentCtx))
	parent.End()

	// Locate the runAction span(s) recorded during expansion.
	var runActionSpans []sdktrace.ReadOnlySpan
	for _, s := range rec.ended {
		if s.Name() == "expand.runAction" {
			runActionSpans = append(runActionSpans, s)
		}
	}
	require.NotEmpty(t, runActionSpans, "expected at least one expand.runAction span")

	first := runActionSpans[0]

	// New root: different trace ID than parent, no parent span ID.
	require.NotEqual(t, parentTraceID, first.SpanContext().TraceID(), "runAction span must start a new trace")
	require.False(t, first.Parent().IsValid(), "runAction span must have no parent span ID")

	// Exactly one link back to the parent trace.
	links := first.Links()
	require.Len(t, links, 1, "runAction span must carry one link to the originating expansion span")
	require.Equal(t, parentTraceID, links[0].SpanContext.TraceID(), "link must point to the parent trace ID")

	// All four documented attributes are present.
	want := map[string]bool{
		"source_entitlement_id":     true,
		"descendant_entitlement_id": true,
		"depth":                     true,
		"shallow":                   true,
	}
	got := map[string]bool{}
	for _, a := range first.Attributes() {
		got[string(a.Key)] = true
		switch string(a.Key) {
		case "source_entitlement_id":
			require.Equal(t, entA.GetId(), a.Value.AsString())
		case "descendant_entitlement_id":
			require.Equal(t, entB.GetId(), a.Value.AsString())
		case "shallow":
			require.False(t, a.Value.AsBool(), "edge in this fixture is non-shallow")
		case "depth":
			// Depth starts at 0 on the first runAction call.
			require.GreaterOrEqual(t, a.Value.AsInt64(), int64(0))
		}
	}
	for k := range want {
		require.True(t, got[k], "missing attribute %q on runAction span", k)
	}
}

// TestRunActionWithoutParentDoesNotLink confirms that StartWithLink degrades
// to a normal Start when ctx carries no active span (e.g. a CLI invocation
// outside the syncer). The runAction span should still be emitted, just
// without a link.
func TestRunActionWithoutParentDoesNotLink(t *testing.T) {
	rec := setupRecordingTracer(t)

	store := NewMockExpanderStore()
	groupResource := makeResource("group", "admins")
	userResource := makeResource("user", "alice")
	entA := makeEntitlement("ent:group:admins:member", groupResource)
	entB := makeEntitlement("ent:role:admin", groupResource)
	store.AddEntitlement(entA)
	store.AddEntitlement(entB)
	store.AddGrant(makeGrant("grant:alice:member", entA, userResource))

	ctx := context.Background()
	require.False(t, trace.SpanContextFromContext(ctx).IsValid(), "fixture should start with no active span")

	graph := NewEntitlementGraph(ctx)
	graph.AddEntitlementID(entA.GetId())
	graph.AddEntitlementID(entB.GetId())
	require.NoError(t, graph.AddEdge(ctx, entA.GetId(), entB.GetId(), false, []string{"user"}))

	expander := NewExpander(store, graph)
	require.NoError(t, expander.Run(ctx))

	var first sdktrace.ReadOnlySpan
	for _, s := range rec.ended {
		if s.Name() == "expand.runAction" {
			first = s
			break
		}
	}
	require.NotNil(t, first, "expected expand.runAction span")
	require.Empty(t, first.Links(), "runAction span must not carry a link when no parent span is in context")
}
