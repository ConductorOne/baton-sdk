package dotc1z

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// installSpanRecorder swaps in a recording TracerProvider for the duration of
// the test, re-binding the package-level tracer to it. Restores the previous
// provider on cleanup.
//
// We re-bind the package var directly because the `tracer` value was captured
// at package init from the global provider; later swaps via otel.SetTracerProvider
// don't retroactively update already-captured Tracer handles.
func installSpanRecorder(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	prevProvider := otel.GetTracerProvider()
	prevTracer := tracer

	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	otel.SetTracerProvider(tp)
	tracer = tp.Tracer("baton-sdk/pkg.dotc1z")

	t.Cleanup(func() {
		tracer = prevTracer
		otel.SetTracerProvider(prevProvider)
		_ = tp.Shutdown(context.Background())
	})
	return sr
}

// findSpan returns the first finished span with the given name, or nil.
func findSpan(spans []sdktrace.ReadOnlySpan, name string) sdktrace.ReadOnlySpan {
	for _, s := range spans {
		if s.Name() == name {
			return s
		}
	}
	return nil
}

// attrValue returns the value of a string attribute from the span, or empty
// string if absent.
func attrValue(s sdktrace.ReadOnlySpan, key string) attribute.Value {
	for _, kv := range s.Attributes() {
		if string(kv.Key) == key {
			return kv.Value
		}
	}
	return attribute.Value{}
}

// TestCloseSpans_ReadOnlyClose verifies that Close on a never-modified c1z
// emits the outer C1File.Close span with read_only/db_updated/db_file_path
// attributes, plus the inner closeRawDb span, but not C1File.saveC1z (the
// file was never updated).
//
// This is the cheap read-only close path — the one uplift activities hit.
// Operators want to filter Datadog by `db_updated:false` to isolate it from
// writer-side closes.
func TestCloseSpans_ReadOnlyClose(t *testing.T) {
	sr := installSpanRecorder(t)

	ctx := t.Context()
	testFilePath := filepath.Join(c1zTests.workingDir, "test-close-spans-readonly.c1z")

	f, err := NewC1ZFile(ctx, testFilePath)
	require.NoError(t, err)
	require.NoError(t, f.Close(ctx))

	spans := sr.Ended()

	closeSpan := findSpan(spans, "C1File.Close")
	require.NotNil(t, closeSpan, "expected outer C1File.Close span")

	require.Equal(t, f.dbFilePath, attrValue(closeSpan, "db_file_path").AsString())
	require.Equal(t, false, attrValue(closeSpan, "read_only").AsBool())
	require.Equal(t, false, attrValue(closeSpan, "db_updated").AsBool())

	rawDbSpan := findSpan(spans, "C1File.closeRawDb")
	require.NotNil(t, rawDbSpan, "expected C1File.closeRawDb span")

	require.Nil(t, findSpan(spans, "C1File.saveC1z"),
		"saveC1z span should not be emitted when db was not updated")
}

// TestCloseSpans_WriterClose verifies that Close on a written-to c1z emits
// the full span set: outer C1File.Close (with db_updated=true), the inner
// truncateWAL/closeRawDb spans, and the C1File.saveC1z span carrying
// input_size_bytes and output_size_bytes attributes.
//
// This is the heavy writer-side close — the one sync activity hits and the
// one Geoff's hypothesis (slow close on Lilly's ~200GB c1z) is about.
func TestCloseSpans_WriterClose(t *testing.T) {
	sr := installSpanRecorder(t)

	ctx := t.Context()
	testFilePath := filepath.Join(c1zTests.workingDir, "test-close-spans-writer.c1z")

	f, err := NewC1ZFile(ctx, testFilePath, WithPragma("journal_mode", "WAL"))
	require.NoError(t, err)

	// Touch the database so dbUpdated flips to true and saveC1z runs.
	_, _, err = f.StartOrResumeSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	require.NoError(t, f.Close(ctx))

	spans := sr.Ended()

	closeSpan := findSpan(spans, "C1File.Close")
	require.NotNil(t, closeSpan, "expected outer C1File.Close span")
	require.Equal(t, true, attrValue(closeSpan, "db_updated").AsBool())
	require.Equal(t, false, attrValue(closeSpan, "read_only").AsBool())

	require.NotNil(t, findSpan(spans, "C1File.truncateWAL"),
		"writer-side close should run a WAL truncate")
	require.NotNil(t, findSpan(spans, "C1File.closeRawDb"),
		"expected closeRawDb span on writer-side close")

	saveSpan := findSpan(spans, "C1File.saveC1z")
	require.NotNil(t, saveSpan, "writer-side close must emit saveC1z span")

	// input_size_bytes is recorded before saveC1z runs; output_size_bytes
	// is recorded after a successful save. Both must be present on the
	// success path so latency-by-size dashboards work.
	require.Greater(t, attrValue(saveSpan, "input_size_bytes").AsInt64(), int64(0),
		"saveC1z span should record input_size_bytes")
	require.Greater(t, attrValue(saveSpan, "output_size_bytes").AsInt64(), int64(0),
		"saveC1z span should record output_size_bytes")
}

// TestCloseSpans_ReadOnlyOpen verifies that opening a c1z with WithReadOnly
// records read_only=true on the outer Close span. This lets Datadog split
// uplift (read-only) closes from sync (writer) closes by attribute.
func TestCloseSpans_ReadOnlyOpen(t *testing.T) {
	ctx := t.Context()

	// First, create a real c1z to read back. Spans from this setup pass
	// are discarded — we only care about the read-only close below.
	setupPath := filepath.Join(c1zTests.workingDir, "test-close-spans-ro-setup.c1z")
	{
		setup, err := NewC1ZFile(ctx, setupPath, WithPragma("journal_mode", "WAL"))
		require.NoError(t, err)
		_, _, err = setup.StartOrResumeSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)
		require.NoError(t, setup.Close(ctx))
	}

	sr := installSpanRecorder(t)

	f, err := NewC1ZFile(ctx, setupPath, WithReadOnly(true))
	require.NoError(t, err)
	require.NoError(t, f.Close(ctx))

	spans := sr.Ended()
	closeSpan := findSpan(spans, "C1File.Close")
	require.NotNil(t, closeSpan, "expected outer C1File.Close span")
	require.Equal(t, true, attrValue(closeSpan, "read_only").AsBool())

	// Read-only close should NOT run saveC1z (would be a no-op anyway).
	require.Nil(t, findSpan(spans, "C1File.saveC1z"),
		"read-only close must not emit saveC1z span")
}
