package sync

import (
	"context"
	"os"
	"path/filepath"
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
type fakeSpanExporter struct {
	spans []sdktrace.ReadOnlySpan
}

func (f *fakeSpanExporter) ExportSpans(_ context.Context, spans []sdktrace.ReadOnlySpan) error {
	f.spans = append(f.spans, spans...)
	return nil
}

func (f *fakeSpanExporter) Shutdown(_ context.Context) error {
	return nil
}

func (f *fakeSpanExporter) spanNamed(name string) sdktrace.ReadOnlySpan {
	for _, s := range f.spans {
		if s.Name() == name {
			return s
		}
	}
	return nil
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
	exporter := &fakeSpanExporter{}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	prevTP := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	defer otel.SetTracerProvider(prevTP)
	defer func() { require.NoError(t, tp.Shutdown(context.Background())) }()

	ctx := context.Background()
	tempDir, err := os.MkdirTemp("", "span-verify-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

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
	// n:1 lets the native grant-sync's own PutGrants call through, then
	// fails the external-match flush's PutGrants call -- landing the
	// failure inside processGrantsWithExternalPrincipalsInner itself.
	cutStore := &failAfterNPutGrants{Store: rawStore, n: 1}
	internalSyncer, err := NewSyncer(ctx, internalMc, WithConnectorStore(cutStore), WithTmpDir(tempDir), WithExternalResourceC1ZPath(externalC1zpath))
	require.NoError(t, err)
	require.ErrorIs(t, internalSyncer.Sync(ctx), errMidScanCut)
	require.NoError(t, internalSyncer.Close(ctx))

	require.NoError(t, tp.ForceFlush(ctx))

	span := exporter.spanNamed("processGrantsWithExternalPrincipals")
	require.NotNil(t, span, "expected a processGrantsWithExternalPrincipals span to be exported")
	require.Equal(t, otelcodes.Error, span.Status().Code,
		"span must be marked as error when the inner call fails -- this is exactly what the wrapper split fixes")
}
