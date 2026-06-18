package uotel

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func TestSyncIdentityAttrs(t *testing.T) {
	t.Run("all fields", func(t *testing.T) {
		id := SyncIdentity{TenantID: "t1", AppID: "a1", ConnectorID: "c1", CatalogID: "cat1", CatalogName: "okta"}
		got := id.Attrs()
		want := map[attribute.Key]string{
			"tenant_id":    "t1",
			"app_id":       "a1",
			"connector_id": "c1",
			"catalog_id":   "cat1",
			"catalog_name": "okta",
		}
		require.Len(t, got, len(want))
		for _, kv := range got {
			require.Equal(t, want[kv.Key], kv.Value.AsString(), "attr %s", kv.Key)
		}
	})

	t.Run("empty fields omitted", func(t *testing.T) {
		id := SyncIdentity{ConnectorID: "c1"}
		got := id.Attrs()
		require.Len(t, got, 1, "expected 1 attr, got %v", got)
		require.Equal(t, attribute.Key("connector_id"), got[0].Key)
		require.Equal(t, "c1", got[0].Value.AsString())
	})

	t.Run("zero", func(t *testing.T) {
		require.True(t, (SyncIdentity{}).IsZero(), "empty SyncIdentity should be zero")
		require.False(t, (SyncIdentity{TenantID: "t"}).IsZero(), "populated SyncIdentity should not be zero")
		require.Empty(t, (SyncIdentity{}).Attrs(), "zero identity should produce no attrs")
	})
}

func TestSyncIdentityContextRoundTrip(t *testing.T) {
	id := SyncIdentity{TenantID: "t1", ConnectorID: "c1"}
	ctx := WithSyncIdentity(context.Background(), id)

	got, ok := SyncIdentityFromContext(ctx)
	require.True(t, ok, "expected identity in context")
	require.Equal(t, id, got)

	_, ok = SyncIdentityFromContext(context.Background())
	require.False(t, ok, "bare context should not carry identity")
}

// TestSetSyncIdentityAttrs is the regression guard for the span-stamping path
// that every sync/dotc1z span relies on: break the ctx-propagation chain and
// spans silently lose all identity attributes with no other test failing.
func TestSetSyncIdentityAttrs(t *testing.T) {
	t.Run("stamps span when ctx carries identity", func(t *testing.T) {
		rec := &recordingProcessor{}
		tr := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec)).Tracer("test")

		id := SyncIdentity{TenantID: "t1", AppID: "a1", ConnectorID: "c1", CatalogID: "cat1", CatalogName: "okta"}
		ctx, span := tr.Start(WithSyncIdentity(context.Background(), id), "test.span")
		SetSyncIdentityAttrs(ctx, span)
		span.End()

		got := findSpan(rec, "test.span")
		require.NotNil(t, got, "span not recorded")
		attrs := map[attribute.Key]string{}
		for _, a := range got.Attributes() {
			attrs[a.Key] = a.Value.AsString()
		}
		for _, want := range id.Attrs() {
			require.Equal(t, want.Value.AsString(), attrs[want.Key], "attr %s", want.Key)
		}
	})

	t.Run("no-op when ctx carries no identity", func(t *testing.T) {
		rec := &recordingProcessor{}
		tr := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec)).Tracer("test")

		_, span := tr.Start(context.Background(), "test.noop")
		SetSyncIdentityAttrs(context.Background(), span)
		span.End()

		got := findSpan(rec, "test.noop")
		require.NotNil(t, got, "span not recorded")
		for _, a := range got.Attributes() {
			switch a.Key {
			case "tenant_id", "app_id", "connector_id", "catalog_id", "catalog_name":
				require.Failf(t, "unexpected identity attr on span with no identity in ctx", "attr %s", string(a.Key))
			}
		}
	})
}
