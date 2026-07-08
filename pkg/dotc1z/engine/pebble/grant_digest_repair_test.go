package pebble

import (
	"context"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/segmentio/ksuid"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
)

// TestRepairMissingGrantDigestsLeavesGlobalRootMissingOnPartialFailure
// pins the invariant RepairMissingGrantDigests's own fast path relies
// on: if even one invalidated entitlement fails to repair, the
// whole-file global root must stay absent rather than get recomputed
// over an incomplete file — otherwise the NEXT call's fast path
// (trusting the root's mere presence) would wrongly skip the scan and
// leave the failed entitlement missing forever.
func TestRepairMissingGrantDigestsLeavesGlobalRootMissingOnPartialFailure(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	syncID := ksuid.New().String()
	if err := e.SetCurrentSync(syncID); err != nil {
		t.Fatalf("SetCurrentSync: %v", err)
	}
	putEnt(t, e, ctx, "ent-good")
	putEnt(t, e, ctx, "ent-bad")
	if err := e.PutGrantRecords(ctx,
		makeGrant("", "g-good", "ent-good", "alice"),
		makeGrant("", "g-bad", "ent-bad", "bob"),
	); err != nil {
		t.Fatalf("PutGrantRecords: %v", err)
	}
	sealGrantDigests(t, e)

	goodPartition := testEntPartition("ent-good")
	badPartition := testEntPartition("ent-bad")
	if err := e.InvalidateGrantDigestPartitions(ctx, []string{goodPartition, badPartition}); err != nil {
		t.Fatalf("InvalidateGrantDigestPartitions: %v", err)
	}

	// Corrupt ent-bad's grant VALUE (leaving its key, and ent-good,
	// untouched) so its repair scan fails deriving the content hash.
	iter, err := e.db.NewIter(&pebble.IterOptions{LowerBound: GrantLowerBound(), UpperBound: GrantUpperBound()})
	if err != nil {
		t.Fatalf("NewIter: %v", err)
	}
	var badKey []byte
	for iter.First(); iter.Valid(); iter.Next() {
		if p, ok := GrantPartitionFromPrimaryKey(iter.Key()); ok && p == badPartition {
			badKey = append([]byte(nil), iter.Key()...)
		}
	}
	if err := iter.Close(); err != nil {
		t.Fatalf("iter close: %v", err)
	}
	if badKey == nil {
		t.Fatal("could not find ent-bad's grant primary key")
	}
	if err := e.db.Set(badKey, []byte{0xFF, 0xFF, 0xFF}, pebble.NoSync); err != nil {
		t.Fatalf("corrupt ent-bad grant value: %v", err)
	}

	if err := e.RepairMissingGrantDigests(ctx); err != nil {
		t.Fatalf("RepairMissingGrantDigests must not fail the caller: %v", err)
	}

	if _, ok, err := e.GetEntitlementDigestRoot(ctx, testEntIdentity("ent-good")); err != nil || !ok {
		t.Fatalf("ent-good root: ok=%v err=%v, want repaired", ok, err)
	}
	if _, ok, err := e.GetEntitlementDigestRoot(ctx, testEntIdentity("ent-bad")); err != nil || ok {
		t.Fatalf("ent-bad root: ok=%v err=%v, want still missing (repair failed)", ok, err)
	}
	if _, ok, err := e.GetGrantDigestGlobalRoot(ctx); err != nil || ok {
		t.Fatalf("global root: ok=%v err=%v, want still missing (repair incomplete)", ok, err)
	}
}

// TestRepairMissingGrantDigestsHealsOnlyInvalidatedPartition is the
// core pin for the targeted repair path: after invalidating ONE
// entitlement's digest + hash-index state out of several, repair must
// reproduce a byte-identical whole-file digest keyspace to the
// original fused build — proving both that the invalidated partition
// is correctly rebuilt AND that every untouched partition (and the
// recomputed whole-file root) is left exactly as it was, not
// coincidentally recomputed to the same values.
func TestRepairMissingGrantDigestsHealsOnlyInvalidatedPartition(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	syncID := ksuid.New().String()
	if err := e.SetCurrentSync(syncID); err != nil {
		t.Fatalf("SetCurrentSync: %v", err)
	}
	counts := map[string]int64{"ent-a": 5, "ent-b": 600, "ent-c": 3}
	for entID, n := range counts {
		putEnt(t, e, ctx, entID)
		grants := make([]*v3.GrantRecord, 0, n)
		for range n {
			grants = append(grants, makeGrant("", ksuid.New().String(), entID, ksuid.New().String()))
		}
		if err := e.PutGrantRecords(ctx, grants...); err != nil {
			t.Fatalf("PutGrantRecords(%s): %v", entID, err)
		}
	}
	sealGrantDigests(t, e)
	want := dumpDigestNodes(t, e)

	partition := testEntPartition("ent-b")
	if err := e.InvalidateGrantDigestPartitions(ctx, []string{partition}); err != nil {
		t.Fatalf("InvalidateGrantDigestPartitions: %v", err)
	}

	// Sanity: exactly ent-b and the global root went missing; ent-a and
	// ent-c are untouched.
	if _, ok, err := e.GetEntitlementDigestRoot(ctx, testEntIdentity("ent-b")); err != nil || ok {
		t.Fatalf("ent-b root after invalidate: ok=%v err=%v, want missing", ok, err)
	}
	if _, ok, err := e.GetGrantDigestGlobalRoot(ctx); err != nil || ok {
		t.Fatalf("global root after invalidate: ok=%v err=%v, want missing", ok, err)
	}
	for _, entID := range []string{"ent-a", "ent-c"} {
		if _, ok, err := e.GetEntitlementDigestRoot(ctx, testEntIdentity(entID)); err != nil || !ok {
			t.Fatalf("%s root after invalidate: ok=%v err=%v, want intact", entID, ok, err)
		}
	}

	if err := e.RepairMissingGrantDigests(ctx); err != nil {
		t.Fatalf("RepairMissingGrantDigests: %v", err)
	}

	requireSameDigestNodes(t, dumpDigestNodes(t, e), want)
}

// TestRepairMissingGrantDigestsNoOpWhenNothingMissing verifies a
// repair call against a fully-built file changes nothing.
func TestRepairMissingGrantDigestsNoOpWhenNothingMissing(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	seedEntitlement(t, e, "ent-A", []*v3.GrantRecord{makeGrant("", "g1", "ent-A", "alice")})
	before := dumpDigestNodes(t, e)

	if err := e.RepairMissingGrantDigests(ctx); err != nil {
		t.Fatalf("RepairMissingGrantDigests: %v", err)
	}
	requireSameDigestNodes(t, dumpDigestNodes(t, e), before)
}

// TestRepairMissingGrantDigestsFallsBackWhenNeverBuilt verifies that a
// file whose digests were never built at all (grantDigestsPresent
// false) is repaired via a full BuildGrantDigests rather than a
// point-Get-per-entitlement scan that would all miss anyway.
func TestRepairMissingGrantDigestsFallsBackWhenNeverBuilt(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	syncID := ksuid.New().String()
	if err := e.SetCurrentSync(syncID); err != nil {
		t.Fatalf("SetCurrentSync: %v", err)
	}
	putEnt(t, e, ctx, "ent-A")
	if err := e.PutGrantRecords(ctx, makeGrant("", "g1", "ent-A", "alice")); err != nil {
		t.Fatalf("PutGrantRecords: %v", err)
	}

	if err := e.RepairMissingGrantDigests(ctx); err != nil {
		t.Fatalf("RepairMissingGrantDigests: %v", err)
	}

	root, ok, err := e.GetEntitlementDigestRoot(ctx, testEntIdentity("ent-A"))
	if err != nil || !ok {
		t.Fatalf("ent-A root: ok=%v err=%v", ok, err)
	}
	if root.Count != 1 {
		t.Fatalf("ent-A root count = %d, want 1", root.Count)
	}
	if _, ok, err := e.GetGrantDigestGlobalRoot(ctx); err != nil || !ok {
		t.Fatalf("global root: ok=%v err=%v", ok, err)
	}
}

// TestRepairMissingGrantDigestsDisabledIsNoOp verifies the digest-
// index-disabled gate matches EndSync's own (GrantDigestIndexEnabled).
func TestRepairMissingGrantDigestsDisabledIsNoOp(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t, WithGrantDigestIndex(false))
	if err := e.RepairMissingGrantDigests(ctx); err != nil {
		t.Fatalf("RepairMissingGrantDigests: %v", err)
	}
}

// TestGrantPartitionFromPrimaryKey pins the raw splice against the
// decoded-identity form the digest keyspace itself keys off.
func TestGrantPartitionFromPrimaryKey(t *testing.T) {
	e, _ := newTestEngine(t)
	seedEntitlement(t, e, "ent-A", []*v3.GrantRecord{makeGrant("", "g1", "ent-A", "alice")})

	iter, err := e.db.NewIter(&pebble.IterOptions{LowerBound: GrantLowerBound(), UpperBound: GrantUpperBound()})
	if err != nil {
		t.Fatalf("NewIter: %v", err)
	}
	defer iter.Close()
	if !iter.First() {
		t.Fatal("expected at least one grant primary key")
	}
	key := append([]byte(nil), iter.Key()...)

	got, ok := GrantPartitionFromPrimaryKey(key)
	if !ok {
		t.Fatal("GrantPartitionFromPrimaryKey: ok = false")
	}
	if want := testEntPartition("ent-A"); got != want {
		t.Fatalf("partition = %q, want %q", got, want)
	}
}
