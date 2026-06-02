package uotel

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/attribute"
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
		if len(got) != 1 || got[0].Key != "connector_id" {
			t.Fatalf("expected only connector_id, got %v", got)
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
