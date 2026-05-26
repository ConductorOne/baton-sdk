package pebble

import (
	"context"
	"testing"

	"github.com/segmentio/ksuid"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
)

// TestGrantReadArenaReconcileAbsent verifies the reconcile pass
// correctly nulls a pre-pointed nested struct that the wire bytes
// didn't populate. Stores a GrantRecord with neither Entitlement
// nor Principal set (the on-wire layout that triggers the arena
// quirk), reads it back, and asserts the nested getters return nil.
func TestGrantReadArenaReconcileAbsent(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	syncID := ksuid.New().String()
	if err := e.MarkFreshSync(syncID); err != nil {
		t.Fatal(err)
	}

	// Write a grant with no Entitlement, no Principal (only required
	// fields). This is the on-wire shape the reconcile pass exists
	// to recover.
	r := v3.GrantRecord_builder{
		SyncId:     syncID,
		ExternalId: "bare",
	}.Build()
	if err := e.PutGrantRecord(ctx, r); err != nil {
		t.Fatalf("PutGrantRecord: %v", err)
	}

	got, _, err := e.PaginateGrantsBySync(ctx, syncID, "", 0)
	if err != nil {
		t.Fatalf("PaginateGrantsBySync: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("len(got) = %d, want 1", len(got))
	}
	if got[0].GetEntitlement() != nil {
		t.Errorf("GetEntitlement() = %+v, want nil", got[0].GetEntitlement())
	}
	if got[0].GetPrincipal() != nil {
		t.Errorf("GetPrincipal() = %+v, want nil", got[0].GetPrincipal())
	}
	if got[0].GetExternalId() != "bare" {
		t.Errorf("GetExternalId() = %q, want %q", got[0].GetExternalId(), "bare")
	}
}

// TestGrantReadArenaPopulatedRoundtrip is the happy-path
// counterpart: stored records WITH Entitlement + Principal must
// come back populated, identical to the input.
func TestGrantReadArenaPopulatedRoundtrip(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	syncID := ksuid.New().String()
	if err := e.MarkFreshSync(syncID); err != nil {
		t.Fatal(err)
	}

	const n = 64
	for i := 0; i < n; i++ {
		if err := e.PutGrantRecord(ctx, makeGrant(syncID, "g-"+ksuid.New().String(), "ent-A", "alice")); err != nil {
			t.Fatalf("PutGrantRecord: %v", err)
		}
	}

	got, _, err := e.PaginateGrantsBySync(ctx, syncID, "", n)
	if err != nil {
		t.Fatalf("PaginateGrantsBySync: %v", err)
	}
	if len(got) != n {
		t.Fatalf("len(got) = %d, want %d", len(got), n)
	}
	for _, g := range got {
		if e := g.GetEntitlement(); e == nil || e.GetEntitlementId() != "ent-A" {
			t.Errorf("GetEntitlement() = %+v, want ent-A", e)
		}
		if p := g.GetPrincipal(); p == nil || p.GetResourceId() != "alice" {
			t.Errorf("GetPrincipal() = %+v, want user/alice", p)
		}
	}
}
