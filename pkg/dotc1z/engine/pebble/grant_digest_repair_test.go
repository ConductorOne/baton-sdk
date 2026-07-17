package pebble

import (
	"context"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/segmentio/ksuid"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

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
	if err := e.db.UnsafeForTesting().Set(badKey, []byte{0xFF, 0xFF, 0xFF}, pebble.NoSync); err != nil {
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

// TestRepairMissingGrantDigestsRediscoversInvalidatedOrphan pins that
// the missing-partition scan discovers partitions from the grant
// primary keyspace, not entitlement records: after invalidating an
// ORPHAN partition (grants with no entitlement record) alongside a
// regular one — the shape a fold merge produces when the incoming
// sync carries a grant whose entitlement record is absent — repair
// must rebuild both and leave the whole digest keyspace (global root
// included) byte-identical to the seal-time build's. An
// entitlement-record-only scan would never rediscover the orphan, and
// the recomputed global root would silently exclude its grants —
// permanently, since the root's presence satisfies the repair fast
// path on every later call.
func TestRepairMissingGrantDigestsRediscoversInvalidatedOrphan(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	syncID := ksuid.New().String()
	if err := e.SetCurrentSync(syncID); err != nil {
		t.Fatalf("SetCurrentSync: %v", err)
	}
	putEnt(t, e, ctx, "ent-real")
	// No putEnt for ent-orphan: its grants have no entitlement record.
	if err := e.PutGrantRecords(ctx,
		makeGrant("", "g1", "ent-real", "alice"),
		makeGrant("", "g2", "ent-orphan", "bob"),
		makeGrant("", "g3", "ent-orphan", "carol"),
	); err != nil {
		t.Fatalf("PutGrantRecords: %v", err)
	}
	sealGrantDigests(t, e)
	if _, ok, err := e.GetEntitlementDigestRoot(ctx, testEntIdentity("ent-orphan")); err != nil || !ok {
		t.Fatalf("orphan root after seal: ok=%v err=%v, want present (seal covers orphans)", ok, err)
	}
	want := dumpDigestNodes(t, e)

	if err := e.InvalidateGrantDigestPartitions(ctx, []string{
		testEntPartition("ent-orphan"),
		testEntPartition("ent-real"),
	}); err != nil {
		t.Fatalf("InvalidateGrantDigestPartitions: %v", err)
	}
	if err := e.RepairMissingGrantDigests(ctx); err != nil {
		t.Fatalf("RepairMissingGrantDigests: %v", err)
	}

	requireSameDigestNodes(t, dumpDigestNodes(t, e), want)
}

// TestRepairMissingGrantDigestsRediscoversZeroGrantEntitlement pins
// the scan's second pass: a zero-grant entitlement's {count: 0} root
// is invisible to the grant-keyspace pass (it has no grant keys), so
// after invalidation it must be rediscovered from the entitlement
// records and rebuilt — again byte-identical to the seal-time build.
func TestRepairMissingGrantDigestsRediscoversZeroGrantEntitlement(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	syncID := ksuid.New().String()
	if err := e.SetCurrentSync(syncID); err != nil {
		t.Fatalf("SetCurrentSync: %v", err)
	}
	putEnt(t, e, ctx, "ent-empty")
	putEnt(t, e, ctx, "ent-a")
	if err := e.PutGrantRecords(ctx, makeGrant("", "g1", "ent-a", "alice")); err != nil {
		t.Fatalf("PutGrantRecords: %v", err)
	}
	sealGrantDigests(t, e)
	want := dumpDigestNodes(t, e)

	if err := e.InvalidateGrantDigestPartitions(ctx, []string{testEntPartition("ent-empty")}); err != nil {
		t.Fatalf("InvalidateGrantDigestPartitions: %v", err)
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

// repairLogCore is a minimal zapcore.Core recording entries in-memory
// so tests can assert on repair logs without zaptest/observer (not
// vendored) — same pattern as progresslog's capturingCore.
type repairLogCore struct {
	zapcore.LevelEnabler
	entries []repairLogEntry
}

type repairLogEntry struct {
	msg    string
	fields map[string]any
}

func newRepairLogCore() *repairLogCore {
	return &repairLogCore{LevelEnabler: zapcore.ErrorLevel}
}

func (c *repairLogCore) With([]zapcore.Field) zapcore.Core { return c }
func (c *repairLogCore) Check(ent zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if c.Enabled(ent.Level) {
		return ce.AddCore(ent, c)
	}
	return ce
}

func (c *repairLogCore) Write(ent zapcore.Entry, fields []zapcore.Field) error {
	enc := zapcore.NewMapObjectEncoder()
	for _, f := range fields {
		f.AddTo(enc)
	}
	c.entries = append(c.entries, repairLogEntry{msg: ent.Message, fields: enc.Fields})
	return nil
}

func (c *repairLogCore) Sync() error { return nil }

// TestRepairMissingGrantDigestsCountsMalformedKeys pins the repair
// path's loud skip of grant primary keys that fail the 6-segment
// split, matching the build paths: the row cannot be represented in
// the digest, and a principal silently losing grants must be
// observable. The repair itself still succeeds, covering only the
// well-formed rows.
func TestRepairMissingGrantDigestsCountsMalformedKeys(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	syncID := ksuid.New().String()
	if err := e.SetCurrentSync(syncID); err != nil {
		t.Fatalf("SetCurrentSync: %v", err)
	}
	putEnt(t, e, ctx, "ent-a")
	if err := e.PutGrantRecords(ctx,
		makeGrant("", "g1", "ent-a", "alice"),
		makeGrant("", "g2", "ent-a", "bob"),
	); err != nil {
		t.Fatalf("PutGrantRecords: %v", err)
	}
	sealGrantDigests(t, e)

	// A key inside ent-a's primary range with SEVEN tuple segments —
	// the key-layout-drift/corruption shape splitGrantPrimaryKey
	// rejects (a well-formed grant identity has exactly six).
	partition := testEntPartition("ent-a")
	lower, _ := grantPrimaryEntitlementBoundsFromPartition(partition)
	malformed := append(append([]byte(nil), lower...), 'p', 0, 'q', 0, 'r')
	if err := e.db.UnsafeForTesting().Set(malformed, []byte("junk"), pebble.NoSync); err != nil {
		t.Fatalf("inject malformed key: %v", err)
	}

	if err := e.InvalidateGrantDigestPartitions(ctx, []string{partition}); err != nil {
		t.Fatalf("InvalidateGrantDigestPartitions: %v", err)
	}

	core := newRepairLogCore()
	lctx := ctxzap.ToContext(ctx, zap.New(core))
	if err := e.RepairMissingGrantDigests(lctx); err != nil {
		t.Fatalf("RepairMissingGrantDigests: %v", err)
	}

	root, ok, err := e.GetEntitlementDigestRoot(ctx, testEntIdentity("ent-a"))
	if err != nil || !ok {
		t.Fatalf("ent-a root after repair: ok=%v err=%v, want repaired", ok, err)
	}
	if root.Count != 2 {
		t.Fatalf("repaired root count = %d, want 2 (malformed row must not be represented)", root.Count)
	}

	var logged bool
	for _, entry := range core.entries {
		if !strings.Contains(entry.msg, "NOT represented in the repaired digest") {
			continue
		}
		logged = true
		if dropped, _ := entry.fields["dropped"].(int64); dropped != 1 {
			t.Fatalf("dropped field = %v, want 1", entry.fields["dropped"])
		}
	}
	if !logged {
		t.Fatal("expected an Error log counting the malformed key the repair skipped")
	}
}

// TestRepairStreamsPartitionInBoundedBatches pins the repair's
// streaming pass-1: with the flush threshold forced to rotate the
// hash-index batch after every row (the smallest possible bound, i.e.
// maximum rotations — the shape a multi-million-grant "everyone"
// entitlement would produce at the real 4MiB threshold), the repaired
// hash index and digest nodes must still be byte-identical to the
// seal-time build's.
func TestRepairStreamsPartitionInBoundedBatches(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	syncID := ksuid.New().String()
	if err := e.SetCurrentSync(syncID); err != nil {
		t.Fatalf("SetCurrentSync: %v", err)
	}
	for entID, n := range map[string]int{"ent-big": 700, "ent-small": 2} {
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
	wantNodes := dumpDigestNodes(t, e)
	wantHash := dumpKeyRangeTest(t, e, GrantByEntPrincHashLowerBound(), GrantByEntPrincHashUpperBound())

	// Rotate the repair's hash-index batch on every Set.
	e.test.digestNodeFlushBytes = 1

	partition := testEntPartition("ent-big")
	if err := e.InvalidateGrantDigestPartitions(ctx, []string{partition}); err != nil {
		t.Fatalf("InvalidateGrantDigestPartitions: %v", err)
	}
	if err := e.RepairMissingGrantDigests(ctx); err != nil {
		t.Fatalf("RepairMissingGrantDigests: %v", err)
	}

	requireSameDigestNodes(t, dumpDigestNodes(t, e), wantNodes)
	requireSameDigestNodes(t, dumpKeyRangeTest(t, e, GrantByEntPrincHashLowerBound(), GrantByEntPrincHashUpperBound()), wantHash)
}
