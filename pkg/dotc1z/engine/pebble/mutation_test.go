package pebble

import (
	"context"
	"strconv"
	"testing"

	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/require"

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
	require.NoError(t, e.SetCurrentSync(syncID))

	// Initial: grant g1 on ent-A, principal alice
	g1Old := makeGrant(syncID, "g1", "ent-A", "alice")
	require.NoError(t, e.PutGrantRecord(ctx, g1Old))
	// Sanity: by_ent(ent-A) returns 1
	count := 0
	err := e.IterateGrantsByEntitlement(ctx, "ent-A", func(*v3.GrantRecord) bool {
		count++
		return true
	})
	require.NoError(t, err)
	require.Equal(t, 1, count, "setup: ent-A count")

	// Overwrite g1 with ent-B + bob.
	g1New := makeGrant(syncID, "g1", "ent-B", "bob")
	require.NoError(t, e.PutGrantRecord(ctx, g1New))

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
			return e.IterateGrantsByEntitlement(ctx, "ent-A", y)
		}, 0},
		{"ent-B", func(y func(*v3.GrantRecord) bool) error {
			return e.IterateGrantsByEntitlement(ctx, "ent-B", y)
		}, 1},
		{"alice", func(y func(*v3.GrantRecord) bool) error {
			return e.IterateGrantsByPrincipal(ctx, "user", "alice", y)
		}, 0},
		{"bob", func(y func(*v3.GrantRecord) bool) error {
			return e.IterateGrantsByPrincipal(ctx, "user", "bob", y)
		}, 1},
	} {
		n := 0
		err := c.fn(func(*v3.GrantRecord) bool {
			n++
			return true
		})
		require.NoError(t, err, "%s iter", c.name)
		require.Equal(t, c.expected, n, "%s", c.name)
	}
}

// TestPutResourceRecordOverwriteCleansIndexes — same shape for the
// by_parent index on ResourceRecord. Changing the parent must remove
// the old by_parent entry and add the new one.
func TestPutResourceRecordOverwriteCleansIndexes(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	syncID := ksuid.New().String()
	require.NoError(t, e.SetCurrentSync(syncID))

	mkRes := func(parentRT, parentID string) *v3.ResourceRecord {
		return v3.ResourceRecord_builder{
			ResourceTypeId: "user",
			ResourceId:     "alice",
			Parent: v3.ResourceRef_builder{
				ResourceTypeId: parentRT,
				ResourceId:     parentID,
			}.Build(),
		}.Build()
	}

	require.NoError(t, e.PutResourceRecord(ctx, mkRes("group", "admins")))
	// Re-parent alice from admins → users.
	require.NoError(t, e.PutResourceRecord(ctx, mkRes("group", "users")))

	for _, c := range []struct {
		parentID string
		expected int
	}{{"admins", 0}, {"users", 1}} {
		n := 0
		err := e.IterateResourcesByParent(ctx, "group", c.parentID, func(*v3.ResourceRecord) bool {
			n++
			return true
		})
		require.NoError(t, err)
		require.Equal(t, c.expected, n, "by_parent(%s)", c.parentID)
	}
}

// TestPutEntitlementRecordOverwriteCleansIndexes — same for the
// by_resource index on EntitlementRecord.
func TestPutEntitlementRecordOverwriteCleansIndexes(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	syncID := ksuid.New().String()
	require.NoError(t, e.SetCurrentSync(syncID))
	mkEnt := func(resID string) *v3.EntitlementRecord {
		return v3.EntitlementRecord_builder{
			ExternalId: "read",
			Resource: v3.ResourceRef_builder{
				ResourceTypeId: "app",
				ResourceId:     resID,
			}.Build(),
		}.Build()
	}
	require.NoError(t, e.PutEntitlementRecord(ctx, mkEnt("github")))
	// Re-target read from github → gitlab.
	require.NoError(t, e.PutEntitlementRecord(ctx, mkEnt("gitlab")))
	for _, c := range []struct {
		resID    string
		expected int
	}{{"github", 0}, {"gitlab", 1}} {
		n := 0
		err := e.IterateEntitlementsByResource(ctx, "app", c.resID, func(*v3.EntitlementRecord) bool {
			n++
			return true
		})
		require.NoError(t, err)
		require.Equal(t, c.expected, n, "by_resource(%s)", c.resID)
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
	_, err := a.StartNewSync(ctx, "full", "")
	require.NoError(t, err)
	// Write g1 on ent-A, then overwrite with ent-B — all within one
	// fresh sync.
	require.NoError(t, a.PutGrants(ctx, mkV2Grant("g1", "ent-A", "user", "alice")))
	require.NoError(t, a.PutGrants(ctx, mkV2Grant("g1", "ent-B", "user", "bob")))
	require.NoError(t, a.EndSync(ctx))

	// ent-A must be empty (old index entry cleaned).
	for _, c := range []struct {
		ent      string
		expected int
	}{{"ent-A", 0}, {"ent-B", 1}} {
		n := 0
		err := a.engine.IterateGrantsByEntitlement(ctx, c.ent, func(*v3.GrantRecord) bool {
			n++
			return true
		})
		require.NoError(t, err)
		require.Equal(t, c.expected, n, "fresh-sync dup overwrite: by_ent(%s)", c.ent)
	}
	// Same for the by_principal index.
	for _, c := range []struct {
		principal string
		expected  int
	}{{"alice", 0}, {"bob", 1}} {
		n := 0
		err := a.engine.IterateGrantsByPrincipal(ctx, "user", c.principal, func(*v3.GrantRecord) bool {
			n++
			return true
		})
		require.NoError(t, err)
		require.Equal(t, c.expected, n, "fresh-sync dup overwrite: by_principal(%s)", c.principal)
	}
}

// TestFreshSyncWithinCallDuplicateResourceDedup covers the
// (sync, rt, res_id) dedup in PutResourceRecords. Two records with
// the same identity but different by_parent indexes in one call:
// the second must win, the first must leave no orphan by_parent
// entry.
func TestFreshSyncWithinCallDuplicateResourceDedup(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	syncID := ksuid.New().String()
	require.NoError(t, e.MarkFreshSync(syncID))
	mkRes := func(parentID string) *v3.ResourceRecord {
		return v3.ResourceRecord_builder{
			ResourceTypeId: "user",
			ResourceId:     "alice",
			Parent: v3.ResourceRef_builder{
				ResourceTypeId: "group",
				ResourceId:     parentID,
			}.Build(),
		}.Build()
	}
	require.NoError(t, e.PutResourceRecords(ctx, mkRes("admins"), mkRes("users")))
	for _, c := range []struct {
		parentID string
		expected int
	}{{"admins", 0}, {"users", 1}} {
		n := 0
		err := e.IterateResourcesByParent(ctx, "group", c.parentID, func(*v3.ResourceRecord) bool {
			n++
			return true
		})
		require.NoError(t, err)
		require.Equal(t, c.expected, n, "within-call dup resource: by_parent(%s)", c.parentID)
	}
}

// TestFreshSyncWithinCallDuplicateEntitlementDedup covers the
// (sync, external_id) dedup in PutEntitlementRecords.
func TestFreshSyncWithinCallDuplicateEntitlementDedup(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	syncID := ksuid.New().String()
	require.NoError(t, e.MarkFreshSync(syncID))
	mkEnt := func(resID string) *v3.EntitlementRecord {
		return v3.EntitlementRecord_builder{
			ExternalId: "read",
			Resource: v3.ResourceRef_builder{
				ResourceTypeId: "app",
				ResourceId:     resID,
			}.Build(),
		}.Build()
	}
	require.NoError(t, e.PutEntitlementRecords(ctx, mkEnt("github"), mkEnt("gitlab")))
	for _, c := range []struct {
		resID    string
		expected int
	}{{"github", 0}, {"gitlab", 1}} {
		n := 0
		err := e.IterateEntitlementsByResource(ctx, "app", c.resID, func(*v3.EntitlementRecord) bool {
			n++
			return true
		})
		require.NoError(t, err)
		require.Equal(t, c.expected, n, "within-call dup entitlement: by_resource(%s)", c.resID)
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
	_, err := a.StartNewSync(ctx, "full", "")
	require.NoError(t, err)
	// One PutGrants call carrying two records with the same id g1.
	require.NoError(t, a.PutGrants(ctx,
		mkV2Grant("g1", "ent-A", "user", "alice"),
		mkV2Grant("g1", "ent-B", "user", "bob"),
	))
	require.NoError(t, a.EndSync(ctx))

	for _, c := range []struct {
		ent      string
		expected int
	}{{"ent-A", 0}, {"ent-B", 1}} {
		n := 0
		err := a.engine.IterateGrantsByEntitlement(ctx, c.ent, func(*v3.GrantRecord) bool {
			n++
			return true
		})
		require.NoError(t, err)
		require.Equal(t, c.expected, n, "within-call dup: by_ent(%s)", c.ent)
	}
	for _, c := range []struct {
		principal string
		expected  int
	}{{"alice", 0}, {"bob", 1}} {
		n := 0
		err := a.engine.IterateGrantsByPrincipal(ctx, "user", c.principal, func(*v3.GrantRecord) bool {
			n++
			return true
		})
		require.NoError(t, err)
		require.Equal(t, c.expected, n, "within-call dup: by_principal(%s)", c.principal)
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
	require.NoError(t, err)
	require.NoError(t, a.PutGrants(ctx, mkV2Grant("g1", "ent-A", "user", "alice")))
	require.NoError(t, a.EndSync(ctx))
	// Re-attach via SetCurrentSync — clears freshSync flag.
	require.NoError(t, a.SetCurrentSync(ctx, syncID))
	require.NoError(t, a.PutGrants(ctx, mkV2Grant("g1", "ent-B", "user", "bob")))

	// ent-A should now be empty (old index cleaned up).
	n := 0
	err = a.engine.IterateGrantsByEntitlement(ctx, "ent-A", func(*v3.GrantRecord) bool {
		n++
		return true
	})
	require.NoError(t, err)
	require.Equal(t, 0, n, "ent-A after non-fresh overwrite")
	// ent-B should have 1.
	n = 0
	err = a.engine.IterateGrantsByEntitlement(ctx, "ent-B", func(*v3.GrantRecord) bool {
		n++
		return true
	})
	require.NoError(t, err)
	require.Equal(t, 1, n, "ent-B after non-fresh overwrite")
}

// TestDeleteThenPutCleansAndRewrites ensures Delete → Put returns
// to a clean state on indexes.
func TestDeleteThenPutCleansAndRewrites(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	syncID := ksuid.New().String()
	require.NoError(t, e.SetCurrentSync(syncID))
	for i := 0; i < 5; i++ {
		require.NoError(t, e.PutGrantRecord(ctx, makeGrant(syncID, "g"+strconv.Itoa(i), "ent-A", "alice")))
	}
	require.NoError(t, e.DeleteGrantRecord(ctx, "g2"))
	// by_ent(ent-A) should have 4 entries (5 - 1 deleted).
	n := 0
	err := e.IterateGrantsByEntitlement(ctx, "ent-A", func(*v3.GrantRecord) bool {
		n++
		return true
	})
	require.NoError(t, err)
	require.Equal(t, 4, n, "after delete: by_ent count")
	// Re-Put g2 with different indexed fields.
	require.NoError(t, e.PutGrantRecord(ctx, makeGrant(syncID, "g2", "ent-C", "carol")))
	// by_ent(ent-A) still 4; by_ent(ent-C) = 1.
	n = 0
	err = e.IterateGrantsByEntitlement(ctx, "ent-A", func(*v3.GrantRecord) bool {
		n++
		return true
	})
	require.NoError(t, err)
	require.Equal(t, 4, n, "after delete + repuT: by_ent(A)")
	n = 0
	err = e.IterateGrantsByEntitlement(ctx, "ent-C", func(*v3.GrantRecord) bool {
		n++
		return true
	})
	require.NoError(t, err)
	require.Equal(t, 1, n, "after delete + reput: by_ent(C)")
}
