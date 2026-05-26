package pebble

import (
	"context"
	"strconv"
	"testing"

	"github.com/segmentio/ksuid"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
)

// TestPutGrantRecordOverwriteCleansIndexes is the canonical mutation
// test the reviewer-bot flagged: when the same primary key is written
// twice with different indexed fields, the OLD index entries must be
// removed and the NEW index entries written. Otherwise IterateGrantsBy*
// returns phantom rows on the old indexed values.
//
// This exercises the read-before-write index-cleanup path. Fresh-sync
// skips that path by design (the sync_id is guaranteed empty) — so
// for this test we use SetCurrentSync (which doesn't flip freshSync)
// to force the read-before-write path.
func TestPutGrantRecordOverwriteCleansIndexes(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	syncID := ksuid.New().String()
	if err := e.SetCurrentSync(syncID); err != nil {
		t.Fatal(err)
	}

	// Initial: grant g1 on ent-A, principal alice
	g1Old := makeGrant(syncID, "g1", "ent-A", "alice")
	if err := e.PutGrantRecord(ctx, g1Old); err != nil {
		t.Fatal(err)
	}
	// Sanity: by_ent(ent-A) returns 1
	count := 0
	if err := e.IterateGrantsByEntitlement(ctx, syncID, "ent-A", func(*v3.GrantRecord) bool {
		count++
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("setup: ent-A count = %d, want 1", count)
	}

	// Overwrite g1 with ent-B + bob.
	g1New := makeGrant(syncID, "g1", "ent-B", "bob")
	if err := e.PutGrantRecord(ctx, g1New); err != nil {
		t.Fatal(err)
	}

	// After overwrite:
	// - by_ent(ent-A) must be empty (old index entry removed)
	// - by_ent(ent-B) must have 1
	// - by_principal(alice) must be empty
	// - by_principal(bob) must have 1
	for _, c := range []struct {
		name     string
		fn       func(yield func(*v3.GrantRecord) bool) error
		expected int
	}{
		{"ent-A", func(y func(*v3.GrantRecord) bool) error {
			return e.IterateGrantsByEntitlement(ctx, syncID, "ent-A", y)
		}, 0},
		{"ent-B", func(y func(*v3.GrantRecord) bool) error {
			return e.IterateGrantsByEntitlement(ctx, syncID, "ent-B", y)
		}, 1},
		{"alice", func(y func(*v3.GrantRecord) bool) error {
			return e.IterateGrantsByPrincipal(ctx, syncID, "user", "alice", y)
		}, 0},
		{"bob", func(y func(*v3.GrantRecord) bool) error {
			return e.IterateGrantsByPrincipal(ctx, syncID, "user", "bob", y)
		}, 1},
	} {
		n := 0
		if err := c.fn(func(*v3.GrantRecord) bool {
			n++
			return true
		}); err != nil {
			t.Fatalf("%s iter: %v", c.name, err)
		}
		if n != c.expected {
			t.Errorf("%s: got %d, want %d", c.name, n, c.expected)
		}
	}
}

// TestPutResourceRecordOverwriteCleansIndexes — same shape for the
// by_parent index on ResourceRecord. Changing the parent must remove
// the old by_parent entry and add the new one.
func TestPutResourceRecordOverwriteCleansIndexes(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	syncID := ksuid.New().String()
	if err := e.SetCurrentSync(syncID); err != nil {
		t.Fatal(err)
	}

	mkRes := func(parentRT, parentID string) *v3.ResourceRecord {
		return v3.ResourceRecord_builder{
			SyncId:         syncID,
			ResourceTypeId: "user",
			ResourceId:     "alice",
			Parent: v3.ResourceRef_builder{
				ResourceTypeId: parentRT,
				ResourceId:     parentID,
			}.Build(),
		}.Build()
	}

	if err := e.PutResourceRecord(ctx, mkRes("group", "admins")); err != nil {
		t.Fatal(err)
	}
	// Re-parent alice from admins → users.
	if err := e.PutResourceRecord(ctx, mkRes("group", "users")); err != nil {
		t.Fatal(err)
	}

	for _, c := range []struct {
		parentID string
		expected int
	}{{"admins", 0}, {"users", 1}} {
		n := 0
		if err := e.IterateResourcesByParent(ctx, syncID, "group", c.parentID, func(*v3.ResourceRecord) bool {
			n++
			return true
		}); err != nil {
			t.Fatal(err)
		}
		if n != c.expected {
			t.Errorf("by_parent(%s): got %d, want %d", c.parentID, n, c.expected)
		}
	}
}

// TestPutEntitlementRecordOverwriteCleansIndexes — same for the
// by_resource index on EntitlementRecord.
func TestPutEntitlementRecordOverwriteCleansIndexes(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	syncID := ksuid.New().String()
	if err := e.SetCurrentSync(syncID); err != nil {
		t.Fatal(err)
	}
	mkEnt := func(resID string) *v3.EntitlementRecord {
		return v3.EntitlementRecord_builder{
			SyncId:     syncID,
			ExternalId: "read",
			Resource: v3.ResourceRef_builder{
				ResourceTypeId: "app",
				ResourceId:     resID,
			}.Build(),
		}.Build()
	}
	if err := e.PutEntitlementRecord(ctx, mkEnt("github")); err != nil {
		t.Fatal(err)
	}
	// Re-target read from github → gitlab.
	if err := e.PutEntitlementRecord(ctx, mkEnt("gitlab")); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		resID    string
		expected int
	}{{"github", 0}, {"gitlab", 1}} {
		n := 0
		if err := e.IterateEntitlementsByResource(ctx, syncID, "app", c.resID, func(*v3.EntitlementRecord) bool {
			n++
			return true
		}); err != nil {
			t.Fatal(err)
		}
		if n != c.expected {
			t.Errorf("by_resource(%s): got %d, want %d", c.resID, n, c.expected)
		}
	}
}

// TestFreshSyncDuplicateExternalIDCleansIndexes verifies the
// freshSync write path correctly handles in-sync duplicate writes
// to the same external_id with different indexed fields. The
// previous implementation skipped read-before-write under freshSync
// to save a Get per record, but that leaked orphan index entries
// when a connector (legitimately or buggily) emitted the same
// external_id twice. Now read-before-write runs unconditionally,
// preserving freshSync's main perf win (NoSync per batch) while
// guaranteeing index integrity.
func TestFreshSyncDuplicateExternalIDCleansIndexes(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	syncID, err := a.StartNewSync(ctx, "full", "")
	if err != nil {
		t.Fatal(err)
	}
	// Write g1 on ent-A, then overwrite with ent-B — all within one
	// fresh sync.
	if err := a.PutGrants(ctx, mkV2Grant("g1", "ent-A", "user", "alice")); err != nil {
		t.Fatal(err)
	}
	if err := a.PutGrants(ctx, mkV2Grant("g1", "ent-B", "user", "bob")); err != nil {
		t.Fatal(err)
	}
	if err := a.EndSync(ctx); err != nil {
		t.Fatal(err)
	}

	// ent-A must be empty (old index entry cleaned).
	for _, c := range []struct {
		ent      string
		expected int
	}{{"ent-A", 0}, {"ent-B", 1}} {
		n := 0
		if err := a.engine.IterateGrantsByEntitlement(ctx, syncID, c.ent, func(*v3.GrantRecord) bool {
			n++
			return true
		}); err != nil {
			t.Fatal(err)
		}
		if n != c.expected {
			t.Errorf("fresh-sync dup overwrite: by_ent(%s) = %d, want %d", c.ent, n, c.expected)
		}
	}
	// Same for the by_principal index.
	for _, c := range []struct {
		principal string
		expected  int
	}{{"alice", 0}, {"bob", 1}} {
		n := 0
		if err := a.engine.IterateGrantsByPrincipal(ctx, syncID, "user", c.principal, func(*v3.GrantRecord) bool {
			n++
			return true
		}); err != nil {
			t.Fatal(err)
		}
		if n != c.expected {
			t.Errorf("fresh-sync dup overwrite: by_principal(%s) = %d, want %d", c.principal, n, c.expected)
		}
	}
}

// TestFreshSyncWithinCallDuplicateExternalIDDedup exercises the
// within-call dedup path: the same external_id appears TWICE in a
// single PutGrants call (paginated source emitting the same record
// on two pages and the connector concatenating before the call to
// the engine). The dedup map must keep only the latest occurrence;
// no orphan index entry should remain pointing at the earlier
// (ent-A / alice) record. This case is independent of skipGet
// because db.Get doesn't see in-batch writes — without the dedup
// pre-pass both records would write their own index entries.
func TestFreshSyncWithinCallDuplicateExternalIDDedup(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	syncID, err := a.StartNewSync(ctx, "full", "")
	if err != nil {
		t.Fatal(err)
	}
	// One PutGrants call carrying two records with the same id g1.
	if err := a.PutGrants(ctx,
		mkV2Grant("g1", "ent-A", "user", "alice"),
		mkV2Grant("g1", "ent-B", "user", "bob"),
	); err != nil {
		t.Fatal(err)
	}
	if err := a.EndSync(ctx); err != nil {
		t.Fatal(err)
	}

	for _, c := range []struct {
		ent      string
		expected int
	}{{"ent-A", 0}, {"ent-B", 1}} {
		n := 0
		if err := a.engine.IterateGrantsByEntitlement(ctx, syncID, c.ent, func(*v3.GrantRecord) bool {
			n++
			return true
		}); err != nil {
			t.Fatal(err)
		}
		if n != c.expected {
			t.Errorf("within-call dup: by_ent(%s) = %d, want %d", c.ent, n, c.expected)
		}
	}
	for _, c := range []struct {
		principal string
		expected  int
	}{{"alice", 0}, {"bob", 1}} {
		n := 0
		if err := a.engine.IterateGrantsByPrincipal(ctx, syncID, "user", c.principal, func(*v3.GrantRecord) bool {
			n++
			return true
		}); err != nil {
			t.Fatal(err)
		}
		if n != c.expected {
			t.Errorf("within-call dup: by_principal(%s) = %d, want %d", c.principal, n, c.expected)
		}
	}
}

// TestNonFreshSyncOverwriteWorksThroughAdapter exercises the adapter
// path (which sets freshSync=true via StartNewSync). After EndSync,
// SetCurrentSync clears freshSync, and a subsequent write of a known
// external_id correctly cleans up old index entries.
func TestNonFreshSyncOverwriteWorksThroughAdapter(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	syncID, err := a.StartNewSync(ctx, "full", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := a.PutGrants(ctx, mkV2Grant("g1", "ent-A", "user", "alice")); err != nil {
		t.Fatal(err)
	}
	if err := a.EndSync(ctx); err != nil {
		t.Fatal(err)
	}
	// Re-attach via SetCurrentSync — clears freshSync flag.
	if err := a.SetCurrentSync(ctx, syncID); err != nil {
		t.Fatal(err)
	}
	if err := a.PutGrants(ctx, mkV2Grant("g1", "ent-B", "user", "bob")); err != nil {
		t.Fatal(err)
	}

	// ent-A should now be empty (old index cleaned up).
	n := 0
	if err := a.engine.IterateGrantsByEntitlement(ctx, syncID, "ent-A", func(*v3.GrantRecord) bool {
		n++
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("ent-A after non-fresh overwrite: got %d, want 0", n)
	}
	// ent-B should have 1.
	n = 0
	if err := a.engine.IterateGrantsByEntitlement(ctx, syncID, "ent-B", func(*v3.GrantRecord) bool {
		n++
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("ent-B after non-fresh overwrite: got %d, want 1", n)
	}
}

// TestDeleteThenPutCleansAndRewrites ensures Delete → Put returns
// to a clean state on indexes.
func TestDeleteThenPutCleansAndRewrites(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	syncID := ksuid.New().String()
	if err := e.SetCurrentSync(syncID); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if err := e.PutGrantRecord(ctx, makeGrant(syncID, "g"+strconv.Itoa(i), "ent-A", "alice")); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.DeleteGrantRecord(ctx, syncID, "g2"); err != nil {
		t.Fatal(err)
	}
	// by_ent(ent-A) should have 4 entries (5 - 1 deleted).
	n := 0
	if err := e.IterateGrantsByEntitlement(ctx, syncID, "ent-A", func(*v3.GrantRecord) bool {
		n++
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if n != 4 {
		t.Errorf("after delete: by_ent count = %d, want 4", n)
	}
	// Re-Put g2 with different indexed fields.
	if err := e.PutGrantRecord(ctx, makeGrant(syncID, "g2", "ent-C", "carol")); err != nil {
		t.Fatal(err)
	}
	// by_ent(ent-A) still 4; by_ent(ent-C) = 1.
	n = 0
	if err := e.IterateGrantsByEntitlement(ctx, syncID, "ent-A", func(*v3.GrantRecord) bool {
		n++
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if n != 4 {
		t.Errorf("after delete + repuT: by_ent(A) = %d, want 4", n)
	}
	n = 0
	if err := e.IterateGrantsByEntitlement(ctx, syncID, "ent-C", func(*v3.GrantRecord) bool {
		n++
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("after delete + reput: by_ent(C) = %d, want 1", n)
	}
}
