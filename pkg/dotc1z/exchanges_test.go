package dotc1z

import (
	"context"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// Round-trips provenance exchanges through the SQLite Exchanges() sub-store and
// confirms the optional ExchangesProvider capability is advertised. This is the
// SDK side of the per-object-request-provenance handoff (RFC §6.6).
func TestExchangesSubStoreRoundTrip(t *testing.T) {
	ctx := context.Background()

	f, err := NewC1ZFile(ctx, t.TempDir()+"/test.c1z")
	if err != nil {
		t.Fatalf("NewC1ZFile: %v", err)
	}
	defer f.Close(ctx)

	// Capability detection: the store advertises ExchangesProvider.
	xp, ok := any(f).(c1zstore.ExchangesProvider)
	if !ok {
		t.Fatalf("C1File does not implement c1zstore.ExchangesProvider")
	}
	store := xp.Exchanges()

	if _, err := f.StartNewSync(ctx, connectorstore.SyncTypeFull, ""); err != nil {
		t.Fatalf("StartNewSync: %v", err)
	}

	in := []*c1zstore.Exchange{
		{RequestID: "req-s-1", TransportKind: "http", RequestJSON: []byte(`{"method":"GET","url":"/users"}`), ResponseJSON: []byte(`{"status":200}`)},
		{RequestID: "req-s-2", TransportKind: "http", RequestJSON: []byte(`{"method":"GET","url":"/users?cursor=c1"}`), ResponseJSON: []byte(`{"status":200}`)},
	}
	if err := store.PutExchanges(ctx, in...); err != nil {
		t.Fatalf("PutExchanges: %v", err)
	}

	got, err := store.GetExchange(ctx, "req-s-1")
	if err != nil || got == nil {
		t.Fatalf("GetExchange(req-s-1) = %v, %v", got, err)
	}
	if got.TransportKind != "http" || string(got.ResponseJSON) != `{"status":200}` {
		t.Fatalf("GetExchange round-trip mismatch: %+v", got)
	}

	list, err := store.ListExchangesForObject(ctx, []string{"req-s-2", "req-missing", "req-s-1"})
	if err != nil {
		t.Fatalf("ListExchangesForObject: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("ListExchangesForObject returned %d, want 2 (missing id skipped)", len(list))
	}

	if missing, err := store.GetExchange(ctx, "nope"); err != nil || missing != nil {
		t.Fatalf("GetExchange(absent) = %v, %v; want nil, nil", missing, err)
	}
}
