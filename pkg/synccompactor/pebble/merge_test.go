package pebble

import (
	"context"
	"testing"
	"time"

	"github.com/segmentio/ksuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
)

func grantAt(syncID, externalID string, at time.Time) *v3.GrantRecord {
	return v3.GrantRecord_builder{
		ExternalId: externalID,
		Entitlement: v3.EntitlementRef_builder{
			ResourceTypeId: "app", ResourceId: "github", EntitlementId: "ent-A",
		}.Build(),
		Principal: v3.PrincipalRef_builder{
			ResourceTypeId: "user", ResourceId: externalID,
		}.Build(),
		DiscoveredAt: timestamppb.New(at),
	}.Build()
}

// TestMergeIntoUnionNewerWins is the core parity pin: a k-way merge of
// two sources into an empty dest sync yields the UNION of records, and
// for a key present in both, the record with the strictly-newer
// discovered_at survives (ties keep the earlier-applied incumbent). All
// surviving records are re-keyed to the destination sync id.
func TestMergeIntoUnionNewerWins(t *testing.T) {
	ctx := context.Background()
	src1, _ := newEngine(t, "src1")
	src2, _ := newEngine(t, "src2")
	dst, _ := newEngine(t, "dst")

	syncA := ksuid.New().String()
	syncB := ksuid.New().String()
	destSync := ksuid.New().String()

	older := time.Unix(1000, 0).UTC()
	newer := time.Unix(2000, 0).UTC()

	// src1 (applied first): shared key @older + a unique key.
	if err := src1.SetCurrentSync(syncA); err != nil {
		t.Fatal(err)
	}
	if err := src1.PutGrantRecords(ctx, grantAt(syncA, "g-shared", older), grantAt(syncA, "g-only1", older)); err != nil {
		t.Fatal(err)
	}
	// src2 (applied second): shared key @newer (must win) + a unique key.
	if err := src2.SetCurrentSync(syncB); err != nil {
		t.Fatal(err)
	}
	if err := src2.PutGrantRecords(ctx, grantAt(syncB, "g-shared", newer), grantAt(syncB, "g-only2", newer)); err != nil {
		t.Fatal(err)
	}

	// No SetCurrentSync here: MergeInto must bind the dest engine to
	// destSyncID itself (record values carry no sync_id, so a stale
	// binding would silently write into the wrong sync's keyspace).
	if err := MergeInto(ctx, dst, []SourceSync{{Engine: src1, SyncID: syncA}, {Engine: src2, SyncID: syncB}}, destSync); err != nil {
		t.Fatalf("MergeInto: %v", err)
	}

	// Union of distinct external_ids: g-shared, g-only1, g-only2 = 3.
	if got := countGrants(t, dst, destSync); got != 3 {
		t.Fatalf("merged grant count = %d, want 3 (union, deduped)", got)
	}

	// Iterating under destSync proves every survivor is re-keyed there,
	// and g-shared kept the newer discovered_at.
	seen := map[string]*v3.GrantRecord{}
	if err := dst.IterateGrants(ctx, func(r *v3.GrantRecord) bool {
		seen[r.GetExternalId()] = r
		return true
	}); err != nil {
		t.Fatalf("IterateGrants: %v", err)
	}
	shared, ok := seen["g-shared"]
	if !ok {
		t.Fatal("g-shared missing from merged output")
	}
	if !shared.GetDiscoveredAt().AsTime().Equal(newer) {
		t.Fatalf("g-shared discovered_at = %s, want newer %s (newer-wins)", shared.GetDiscoveredAt().AsTime(), newer)
	}
	for _, id := range []string{"g-only1", "g-only2"} {
		if _, ok := seen[id]; !ok {
			t.Errorf("%q missing from merged union", id)
		}
	}
}

// TestMergeIntoTieKeepsIncumbent pins the equal-discovered_at tie rule
// to SQLite's strict `>`: on a tie, the earlier-applied source's record
// is kept (it is the incumbent the newer write does not replace).
func TestMergeIntoTieKeepsIncumbent(t *testing.T) {
	ctx := context.Background()
	src1, _ := newEngine(t, "tsrc1")
	src2, _ := newEngine(t, "tsrc2")
	dst, _ := newEngine(t, "tdst")

	syncA := ksuid.New().String()
	syncB := ksuid.New().String()
	destSync := ksuid.New().String()
	tie := time.Unix(1500, 0).UTC()

	// Same key + same discovered_at in both, distinguished by principal id.
	g1 := grantAt(syncA, "g-tie", tie)
	g1.SetPrincipal(v3.PrincipalRef_builder{ResourceTypeId: "user", ResourceId: "from-src1"}.Build())
	g2 := grantAt(syncB, "g-tie", tie)
	g2.SetPrincipal(v3.PrincipalRef_builder{ResourceTypeId: "user", ResourceId: "from-src2"}.Build())

	_ = src1.SetCurrentSync(syncA)
	if err := src1.PutGrantRecords(ctx, g1); err != nil {
		t.Fatal(err)
	}
	_ = src2.SetCurrentSync(syncB)
	if err := src2.PutGrantRecords(ctx, g2); err != nil {
		t.Fatal(err)
	}
	// src1 applied first → incumbent wins the tie. MergeInto binds the
	// dest sync itself.
	if err := MergeInto(ctx, dst, []SourceSync{{Engine: src1, SyncID: syncA}, {Engine: src2, SyncID: syncB}}, destSync); err != nil {
		t.Fatal(err)
	}

	var winner string
	if err := dst.IterateGrants(ctx, func(r *v3.GrantRecord) bool {
		winner = r.GetPrincipal().GetResourceId()
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if winner != "from-src1" {
		t.Fatalf("tie winner = %q, want from-src1 (earliest-applied incumbent, strict-> parity)", winner)
	}
}
