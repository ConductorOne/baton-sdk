package pebble

import (
	"context"
	"testing"

	"github.com/segmentio/ksuid"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// TestPendingExpansionIndexRoundtrip is the correctness regression
// for the by_needs_expansion index keyspace: grants written with
// NeedsExpansion=true must surface in IterateGrantsByNeedsExpansion;
// grants without the flag must not. Mirrors the SQLite partial
// index `WHERE needs_expansion = 1`.
func TestPendingExpansionIndexRoundtrip(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	syncID := ksuid.New().String()
	if err := e.MarkFreshSync(syncID); err != nil {
		t.Fatalf("MarkFreshSync: %v", err)
	}

	gPending := v3.GrantRecord_builder{
		ExternalId: "g-pending",
		Entitlement: v3.EntitlementRef_builder{
			ResourceTypeId: "app", ResourceId: "github", EntitlementId: "ent-A",
		}.Build(),
		Principal: v3.PrincipalRef_builder{
			ResourceTypeId: "group", ResourceId: "admins",
		}.Build(),
		Expansion: v3.GrantExpandableRecord_builder{
			EntitlementIds: []string{"src-ent-1"},
		}.Build(),
		NeedsExpansion: true,
	}.Build()
	gProcessed := v3.GrantRecord_builder{
		ExternalId: "g-processed",
		Entitlement: v3.EntitlementRef_builder{
			ResourceTypeId: "app", ResourceId: "github", EntitlementId: "ent-B",
		}.Build(),
		Principal: v3.PrincipalRef_builder{
			ResourceTypeId: "group", ResourceId: "users",
		}.Build(),
		Expansion: v3.GrantExpandableRecord_builder{
			EntitlementIds: []string{"src-ent-2"},
		}.Build(),
		NeedsExpansion: false,
	}.Build()
	gPlain := v3.GrantRecord_builder{
		ExternalId: "g-plain",
		Entitlement: v3.EntitlementRef_builder{
			ResourceTypeId: "app", ResourceId: "github", EntitlementId: "ent-C",
		}.Build(),
		Principal: v3.PrincipalRef_builder{
			ResourceTypeId: "user", ResourceId: "alice",
		}.Build(),
	}.Build()

	if err := e.PutGrantRecords(ctx, gPending, gProcessed, gPlain); err != nil {
		t.Fatalf("PutGrantRecords: %v", err)
	}

	seen := []string{}
	if err := e.IterateGrantsByNeedsExpansion(ctx, func(r *v3.GrantRecord) bool {
		seen = append(seen, r.GetExternalId())
		return true
	}); err != nil {
		t.Fatalf("IterateGrantsByNeedsExpansion: %v", err)
	}
	if len(seen) != 1 || seen[0] != "g-pending" {
		t.Errorf("IterateGrantsByNeedsExpansion: got %v, want [g-pending]", seen)
	}

	// Flip g-pending to NeedsExpansion=false. The index entry must
	// disappear (mirrors the syncer's post-expansion write).
	gPendingDone := v3.GrantRecord_builder{
		ExternalId: "g-pending",
		Entitlement: v3.EntitlementRef_builder{
			ResourceTypeId: "app", ResourceId: "github", EntitlementId: "ent-A",
		}.Build(),
		Principal: v3.PrincipalRef_builder{
			ResourceTypeId: "group", ResourceId: "admins",
		}.Build(),
		Expansion: v3.GrantExpandableRecord_builder{
			EntitlementIds: []string{"src-ent-1"},
		}.Build(),
		NeedsExpansion: false,
	}.Build()
	if err := e.PutGrantRecord(ctx, gPendingDone); err != nil {
		t.Fatalf("PutGrantRecord (flip): %v", err)
	}
	seen = seen[:0]
	if err := e.IterateGrantsByNeedsExpansion(ctx, func(r *v3.GrantRecord) bool {
		seen = append(seen, r.GetExternalId())
		return true
	}); err != nil {
		t.Fatalf("IterateGrantsByNeedsExpansion (post-flip): %v", err)
	}
	if len(seen) != 0 {
		t.Errorf("after flip to NeedsExpansion=false, IterateGrantsByNeedsExpansion: got %v, want []", seen)
	}
}

// TestPebbleGrantStorePendingExpansionPage exercises the adapter
// layer: the GrantStore wrapper translates index iteration into
// PendingExpansion rows shaped for the syncer's ExpandGrants
// consumer.
func TestPebbleGrantStorePendingExpansionPage(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	if err != nil {
		t.Fatalf("StartNewSync: %v", err)
	}
	rec := v3.GrantRecord_builder{
		ExternalId: "g1",
		Entitlement: v3.EntitlementRef_builder{
			ResourceTypeId: "app", ResourceId: "github", EntitlementId: "ent-A",
		}.Build(),
		Principal: v3.PrincipalRef_builder{
			ResourceTypeId: "group", ResourceId: "admins",
		}.Build(),
		Expansion: v3.GrantExpandableRecord_builder{
			EntitlementIds: []string{"src-ent-1"},
		}.Build(),
		NeedsExpansion: true,
	}.Build()
	if err := a.engine.PutGrantRecord(ctx, rec); err != nil {
		t.Fatalf("PutGrantRecord: %v", err)
	}

	rows, _, err := a.Grants().PendingExpansionPage(ctx, "")
	if err != nil {
		t.Fatalf("PendingExpansionPage: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("PendingExpansionPage: got %d rows, want 1", len(rows))
	}
	if rows[0].GrantExternalID != "g1" {
		t.Errorf("PendingExpansion[0].GrantExternalID = %q, want g1", rows[0].GrantExternalID)
	}
	if rows[0].TargetEntitlementID != "ent-A" {
		t.Errorf("PendingExpansion[0].TargetEntitlementID = %q, want ent-A", rows[0].TargetEntitlementID)
	}
	if !rows[0].NeedsExpansion {
		t.Error("PendingExpansion[0].NeedsExpansion = false, want true")
	}
	if rows[0].Annotation == nil || len(rows[0].Annotation.GetEntitlementIds()) != 1 {
		t.Errorf("PendingExpansion[0].Annotation = %v, want non-nil with 1 entitlement id", rows[0].Annotation)
	}
}
