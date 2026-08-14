package sync

import (
	"context"
	"os"
	"path/filepath"
	native_sync "sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	otelcodes "go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	gt "github.com/conductorone/baton-sdk/pkg/types/grant"
)

// fakeSpanExporter is a minimal sdktrace.SpanExporter -- just enough to
// capture exported spans by name, without pulling in the unvendored
// go.opentelemetry.io/otel/sdk/trace/tracetest package (this repo builds
// with -mod=vendor and that test-support package isn't part of the vendor
// tree).
//
// It captures only while armed, because the provider it feeds stays installed
// for the rest of the package's tests (see installFakeSpanExporter) and those
// tests emit tens of thousands of spans nobody looks at. The mutex is not
// optional: sdktrace's simple span processor exports under a read lock, so
// spans ended by the parallel syncer's workers land here concurrently.
type fakeSpanExporter struct {
	mu    native_sync.Mutex
	armed bool
	spans []sdktrace.ReadOnlySpan
}

func (f *fakeSpanExporter) ExportSpans(_ context.Context, spans []sdktrace.ReadOnlySpan) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.armed {
		f.spans = append(f.spans, spans...)
	}
	return nil
}

func (f *fakeSpanExporter) Shutdown(_ context.Context) error {
	return nil
}

func (f *fakeSpanExporter) arm(armed bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.armed = armed
	f.spans = nil
}

func (f *fakeSpanExporter) spanNamed(name string) sdktrace.ReadOnlySpan {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, s := range f.spans {
		if s.Name() == name {
			return s
		}
	}
	return nil
}

var (
	sharedFakeExporterOnce native_sync.Once
	sharedFakeExporter     *fakeSpanExporter
	sharedFakeProvider     *sdktrace.TracerProvider
)

// installFakeSpanExporter points the process-global tracer provider at a
// fakeSpanExporter and arms it for the duration of the test.
//
// The provider is installed once per test binary and never swapped back out.
// otel's global provider delegates exactly once: the package-level tracer this
// package builds at init binds to whichever provider is Set() first, and every
// later Set() is invisible to it. A per-test install/restore pair therefore
// works only for whichever span test happens to run first and silently sends
// the next one's spans nowhere -- which is a test that passes for the wrong
// reason, or fails for one.
//
// The provider is a *synchronous* syncer, so a span reaches the exporter the
// instant it is ended -- which is what makes "the exporter never saw this
// span" a sound assertion that the span was never ended at all.
//
// Callers must not t.Parallel(): one exporter is shared, and arming it is
// process-wide.
func installFakeSpanExporter(t *testing.T) (*fakeSpanExporter, *sdktrace.TracerProvider) {
	t.Helper()
	sharedFakeExporterOnce.Do(func() {
		sharedFakeExporter = &fakeSpanExporter{}
		sharedFakeProvider = sdktrace.NewTracerProvider(sdktrace.WithSyncer(sharedFakeExporter))
		otel.SetTracerProvider(sharedFakeProvider)
	})
	sharedFakeExporter.arm(true)
	t.Cleanup(func() { sharedFakeExporter.arm(false) })
	return sharedFakeExporter, sharedFakeProvider
}

// newExternalMatchSpanFixture builds the smallest sync that reaches
// processGrantsWithExternalPrincipals: one external user, and one internal
// group grant annotated ExternalResourceMatchAll, so the scan expands
// exactly one replacement grant. wrapStore wraps the internal store, letting
// a test make that expansion's write fail or panic; the native grant sync's
// own PutGrants call runs first, so a wrapper keyed on "everything after the
// first call" lands the fault inside processGrantsWithExternalPrincipalsInner
// itself rather than somewhere earlier in the sync.
func newExternalMatchSpanFixture(t *testing.T, wrapStore func(c1zstore.Store) c1zstore.Store) Syncer {
	t.Helper()
	ctx := context.Background()
	tempDir, err := os.MkdirTemp("", "span-verify-test")
	require.NoError(t, err)
	// Not t.TempDir: a test that unwinds through a panic never closes its
	// syncer, and t.TempDir's cleanup failing on a still-open file would
	// fail the test for a reason that has nothing to do with what it asserts.
	t.Cleanup(func() { _ = os.RemoveAll(tempDir) })

	internalMc := newMockConnector()
	internalMc.rtDB = append(internalMc.rtDB, userResourceType, groupResourceType)
	externalMc := newMockConnector()
	externalMc.rtDB = append(externalMc.rtDB, userResourceType, groupResourceType)

	_, err = externalMc.AddUserProfile(ctx, "ext_user_0", map[string]any{})
	require.NoError(t, err)

	internalGroup, _, err := internalMc.AddGroup(ctx, "internal_group")
	require.NoError(t, err)
	internalMc.grantDB[internalGroup.GetId().GetResource()] = []*v2.Grant{
		gt.NewGrant(
			internalGroup, "member",
			v2.ResourceId_builder{ResourceType: userResourceType.GetId(), Resource: "placeholder"}.Build(),
			gt.WithAnnotation(v2.ExternalResourceMatchAll_builder{ResourceType: v2.ResourceType_TRAIT_USER}.Build()),
		),
	}

	externalC1zpath := filepath.Join(tempDir, "external.c1z")
	externalSyncer, err := NewSyncer(ctx, externalMc, WithC1ZPath(externalC1zpath), WithTmpDir(tempDir))
	require.NoError(t, err)
	require.NoError(t, externalSyncer.Sync(ctx))
	require.NoError(t, externalSyncer.Close(ctx))

	internalC1zpath := filepath.Join(tempDir, "internal.c1z")
	rawStore, err := dotc1z.NewStore(ctx, internalC1zpath, dotc1z.WithEngine(c1zstore.EngineSQLite), dotc1z.WithTmpDir(tempDir))
	require.NoError(t, err)

	internalSyncer, err := NewSyncer(ctx, internalMc,
		WithConnectorStore(wrapStore(rawStore)), WithTmpDir(tempDir), WithExternalResourceC1ZPath(externalC1zpath))
	require.NoError(t, err)
	return internalSyncer
}

// TestProcessGrantsWithExternalPrincipalsRecordsSpanErrorOnWriteFailure is a
// regression test for the processGrantsWithExternalPrincipals /
// processGrantsWithExternalPrincipalsInner split. The original function had
// its span-ending defer close over a var err error that a range-over-func
// loop and several per-branch "X, err := ..." declarations each shadowed --
// so a write failure deep in the scan still correctly failed the sync (the
// function's own return value was always right) but the defer's closure saw
// an unset outer err and recorded the span as successful anyway. Splitting
// into a thin wrapper (owning the span, the defer, and a single err assigned
// once from the inner call's return) sidesteps the shadowing instead of
// chasing it through the whole function. This test drives a real mid-scan
// write failure and asserts the span this wrapper starts is actually marked
// as an error, which the pre-split code would have failed.
func TestProcessGrantsWithExternalPrincipalsRecordsSpanErrorOnWriteFailure(t *testing.T) {
	exporter, tp := installFakeSpanExporter(t)

	ctx := context.Background()
	// n:1 lets the native grant-sync's own PutGrants call through, then
	// fails the external-match flush's PutGrants call -- landing the
	// failure inside processGrantsWithExternalPrincipalsInner itself.
	internalSyncer := newExternalMatchSpanFixture(t, func(s c1zstore.Store) c1zstore.Store {
		return &failAfterNPutGrants{Store: s, n: 1}
	})
	require.ErrorIs(t, internalSyncer.Sync(ctx), errMidScanCut)
	require.NoError(t, internalSyncer.Close(ctx))

	require.NoError(t, tp.ForceFlush(ctx))

	span := exporter.spanNamed("processGrantsWithExternalPrincipals")
	require.NotNil(t, span, "expected a processGrantsWithExternalPrincipals span to be exported")
	require.Equal(t, otelcodes.Error, span.Status().Code,
		"span must be marked as error when the inner call fails -- this is exactly what the wrapper split fixes")
}

// panicAfterNPutGrants panics instead of returning an error once the call
// count is exceeded, so a test can unwind processGrantsWithExternalPrincipals
// without going through any of its own error returns.
type panicAfterNPutGrants struct {
	c1zstore.Store
	n     int
	calls int
}

const panicMidScanValue = "test: panic after N PutGrants calls"

func (p *panicAfterNPutGrants) PutGrants(ctx context.Context, grants ...*v2.Grant) error {
	p.calls++
	if p.calls > p.n {
		panic(panicMidScanValue)
	}
	return p.Store.PutGrants(ctx, grants...)
}

// TestProcessGrantsWithExternalPrincipalsEndsSpanOnPanic pins the *defer* in
// processGrantsWithExternalPrincipals, not just its error propagation. The
// wrapper split briefly ended its span with a plain call after the inner
// call returned; that gets the error status right but leaks the span
// entirely on any panic below it, because the call is simply skipped while
// the stack unwinds. Since the tracer provider here exports synchronously on
// End, "the exporter never saw this span" is exactly equivalent to "the span
// was never ended", which is what a non-deferred close would produce.
func TestProcessGrantsWithExternalPrincipalsEndsSpanOnPanic(t *testing.T) {
	exporter, tp := installFakeSpanExporter(t)

	ctx := context.Background()
	internalSyncer := newExternalMatchSpanFixture(t, func(s c1zstore.Store) c1zstore.Store {
		return &panicAfterNPutGrants{Store: s, n: 1}
	})

	require.PanicsWithValue(t, panicMidScanValue, func() {
		_ = internalSyncer.Sync(ctx)
	}, "the panic must propagate -- this test is meaningless if something swallows it")

	require.NoError(t, tp.ForceFlush(ctx))

	require.NotNil(t, exporter.spanNamed("processGrantsWithExternalPrincipals"),
		"span must still be ended when the inner call panics; only a deferred close does that")
}
