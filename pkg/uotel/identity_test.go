package uotel

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func TestSyncIdentityAttrs(t *testing.T) {
	t.Run("all fields", func(t *testing.T) {
		id := SyncIdentity{TenantID: "t1", ConnectorID: "c1", CatalogID: "cat1", CatalogName: "okta"}
		got := id.Attrs()
		want := map[attribute.Key]string{
			"tenant_id":    "t1",
			"connector_id": "c1",
			"catalog_id":   "cat1",
			"catalog_name": "okta",
		}
		if len(got) != len(want) {
			t.Fatalf("got %d attrs, want %d", len(got), len(want))
		}
		for _, kv := range got {
			if want[kv.Key] != kv.Value.AsString() {
				t.Errorf("attr %s = %q, want %q", kv.Key, kv.Value.AsString(), want[kv.Key])
			}
		}
	})

	t.Run("empty fields omitted", func(t *testing.T) {
		id := SyncIdentity{ConnectorID: "c1"}
		got := id.Attrs()
		if len(got) != 1 {
			t.Fatalf("expected 1 attr, got %d: %v", len(got), got)
		}
		if got[0].Key != "connector_id" || got[0].Value.AsString() != "c1" {
			t.Errorf("expected connector_id=c1, got %s=%q", got[0].Key, got[0].Value.AsString())
		}
	})

	t.Run("zero", func(t *testing.T) {
		if !(SyncIdentity{}).IsZero() {
			t.Error("empty SyncIdentity should be zero")
		}
		if (SyncIdentity{TenantID: "t"}).IsZero() {
			t.Error("populated SyncIdentity should not be zero")
		}
		if len((SyncIdentity{}).Attrs()) != 0 {
			t.Error("zero identity should produce no attrs")
		}
	})
}

func TestSyncIdentityContextRoundTrip(t *testing.T) {
	id := SyncIdentity{TenantID: "t1", ConnectorID: "c1"}
	ctx := WithSyncIdentity(context.Background(), id)

	got, ok := SyncIdentityFromContext(ctx)
	if !ok {
		t.Fatal("expected identity in context")
	}
	if got != id {
		t.Errorf("round-trip mismatch: got %+v, want %+v", got, id)
	}

	if _, ok := SyncIdentityFromContext(context.Background()); ok {
		t.Error("bare context should not carry identity")
	}
}

// TestSetSyncIdentityAttrs is the regression guard for the span-stamping path
// that every sync/dotc1z span relies on: break the ctx-propagation chain and
// spans silently lose all identity attributes with no other test failing.
func TestSetSyncIdentityAttrs(t *testing.T) {
	t.Run("stamps span when ctx carries identity", func(t *testing.T) {
		rec := &recordingProcessor{}
		tr := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec)).Tracer("test")

		id := SyncIdentity{TenantID: "t1", ConnectorID: "c1", CatalogID: "cat1", CatalogName: "okta"}
		ctx, span := tr.Start(WithSyncIdentity(context.Background(), id), "test.span")
		SetSyncIdentityAttrs(ctx, span)
		span.End()

		got := findSpan(rec, "test.span")
		if got == nil {
			t.Fatal("span not recorded")
		}
		attrs := map[attribute.Key]string{}
		for _, a := range got.Attributes() {
			attrs[a.Key] = a.Value.AsString()
		}
		for _, want := range id.Attrs() {
			if attrs[want.Key] != want.Value.AsString() {
				t.Errorf("attr %s = %q, want %q", want.Key, attrs[want.Key], want.Value.AsString())
			}
		}
	})

	t.Run("no-op when ctx carries no identity", func(t *testing.T) {
		rec := &recordingProcessor{}
		tr := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec)).Tracer("test")

		_, span := tr.Start(context.Background(), "test.noop")
		SetSyncIdentityAttrs(context.Background(), span)
		span.End()

		got := findSpan(rec, "test.noop")
		if got == nil {
			t.Fatal("span not recorded")
		}
		for _, a := range got.Attributes() {
			switch a.Key {
			case "tenant_id", "connector_id", "catalog_id", "catalog_name":
				t.Errorf("unexpected identity attr %s on span with no identity in ctx", a.Key)
			}
		}
	})
}
