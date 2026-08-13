package pebble

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/require"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/internal/rawdb"
)

// Tests for the digest ABI stamp (rawdb.GrantDigestABIStampKey,
// verifyGrantDigestABI, grantDigestABIStampValue/GrantDigestABIVersion
// in grant_digest.go): digest nodes present whose stamp (a missing
// stamp reads as version 1, the pre-stamp ABI) does not name the
// current GrantDigestABIVersion must make a writable Open drop ALL
// digest state so the next seal rebuilds it, and a read-only Open
// report the digest roots as "never built" instead. See grant_digest.go
// and engine.go's Open for the production contract these pin.

// makeTestGrants builds n distinct grants for one entitlement,
// following the same shape as digest_test.go's makeGrant.
func makeTestGrants(entID string, n int) []*v3.GrantRecord {
	grants := make([]*v3.GrantRecord, 0, n)
	for i := range n {
		grants = append(grants, makeGrant("", fmt.Sprintf("g-%s-%03d", entID, i), entID, fmt.Sprintf("user-%03d", i)))
	}
	return grants
}

// staleABIVersion is a fake ABI version guaranteed to differ from the
// current GrantDigestABIVersion (and from the implicit version 1 that a
// missing stamp reads as), for tests that need "a stamp naming some
// other ABI".
const staleABIVersion = GrantDigestABIVersion + 1

// abiStampBytes encodes a (possibly fake) ABI version the way the
// production stamp does: uint32 BE.
func abiStampBytes(version uint32) []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, version)
	return buf
}

// setABIStamp overwrites the durable ABI stamp with an arbitrary
// version — DigestSet is the production write for a digest-keyspace
// row (the family the stamp key itself lives in), so this exercises
// exactly the "stamp names a different version" state Open must guard
// against, without going through any other digest bookkeeping.
func setABIStamp(t *testing.T, e *Engine, version uint32) {
	t.Helper()
	require.NoError(t, e.db.DigestSet(rawdb.GrantDigestABIStampKey(), abiStampBytes(version), pebble.Sync))
}

// deleteABIStamp removes the stamp key entirely, simulating a file
// sealed by a pre-stamp SDK build (digest nodes present, no stamp at
// all). No exported DB operation deletes a single digest-family key by
// design (digest.go's writers only ever Set), so this is exactly the
// kind of production-inexpressible state rawdb.DB.UnsafeForTesting
// exists for.
func deleteABIStamp(t *testing.T, e *Engine) {
	t.Helper()
	require.NoError(t, e.db.UnsafeForTesting().Delete(rawdb.GrantDigestABIStampKey(), pebble.Sync))
}

// sealedGrantDigestEngine builds a small sealed file through the
// normal StartNewSync -> EndSync path — so a durable SyncRunRecord
// exists and a later SetCurrentSync/EndSync can resume the same sync
// after a reopen, exactly the pattern
// grant_digest_build_crash_test.go uses to drive Open's
// crash-recovery paths — and returns the engine, its on-disk "db"
// directory (ready for a bare Open(ctx, dbDir, ...) reopen), and the
// sync id.
func sealedGrantDigestEngine(t *testing.T, entID string, n int, opts ...Option) (*Engine, string, string) {
	t.Helper()
	ctx := context.Background()
	e, dir := newTestEngine(t, opts...)
	a := NewAdapter(e)
	syncID, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err, "StartNewSync")
	putEnt(t, e, ctx, entID)
	require.NoError(t, e.PutGrantRecords(ctx, makeTestGrants(entID, n)...), "PutGrantRecords")
	require.NoError(t, a.EndSync(ctx), "EndSync")
	return e, filepath.Join(dir, "db"), syncID
}

// verifyGrantHashIndexAgainstPrimaries is the positive-evidence oracle
// for a built hash index: for every grant PRIMARY record it decodes
// the record, independently recomputes the expected content hash
// (grantContentHashForRecord — the from-record path, not the
// seal-time splice), splices the same grant's hash-index key from the
// raw primary key, and requires the stored row's 8-byte value to match
// — then requires the hash-index row count to equal the grant count
// exactly (no missing or orphaned rows). Returns an error rather than
// failing the test directly so a test can assert BOTH that it passes
// after a clean seal and that it can detect a tampered row (see
// TestGrantDigestABIOracle).
func verifyGrantHashIndexAgainstPrimaries(t testing.TB, e *Engine) error {
	t.Helper()
	ctx := context.Background()
	giter, err := e.db.NewIter(&pebble.IterOptions{LowerBound: GrantLowerBound(), UpperBound: GrantUpperBound()})
	if err != nil {
		return err
	}
	defer giter.Close()
	var grantCount int
	for giter.First(); giter.Valid(); giter.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		grantCount++
		key := append([]byte(nil), giter.Key()...)
		sep4, ok := rawdb.SplitGrantPrimaryKey(key)
		if !ok {
			return fmt.Errorf("verifyGrantHashIndexAgainstPrimaries: grant primary key %x did not split as a 6-segment identity", key)
		}
		rec := &v3.GrantRecord{}
		if err := unmarshalRecord(giter.Value(), rec); err != nil {
			return fmt.Errorf("verifyGrantHashIndexAgainstPrimaries: unmarshal grant %x: %w", key, err)
		}
		wantHash, err := grantContentHashForRecord(rec)
		if err != nil {
			return fmt.Errorf("verifyGrantHashIndexAgainstPrimaries: grantContentHashForRecord(%x): %w", key, err)
		}
		bh64 := grantPrincipalBucketHash64(key[sep4+1:])
		idxKey := appendGrantHashIndexKeyFromPrimary(nil, key, sep4, bh64)
		val, closer, err := e.db.Get(idxKey)
		if err != nil {
			return fmt.Errorf("verifyGrantHashIndexAgainstPrimaries: hash-index row for grant %x: %w", key, err)
		}
		gotHash := append([]byte(nil), val...)
		closer.Close()
		if !bytes.Equal(gotHash, wantHash) {
			return fmt.Errorf("verifyGrantHashIndexAgainstPrimaries: hash-index row for grant %x = %x, want %x", key, gotHash, wantHash)
		}
	}
	if err := giter.Error(); err != nil {
		return err
	}

	iiter, err := e.db.NewIter(&pebble.IterOptions{LowerBound: GrantByEntPrincHashLowerBound(), UpperBound: GrantByEntPrincHashUpperBound()})
	if err != nil {
		return err
	}
	defer iiter.Close()
	var rowCount int
	for iiter.First(); iiter.Valid(); iiter.Next() {
		rowCount++
	}
	if err := iiter.Error(); err != nil {
		return err
	}
	if rowCount != grantCount {
		return fmt.Errorf("verifyGrantHashIndexAgainstPrimaries: hash-index row count = %d, want %d (one per grant)", rowCount, grantCount)
	}
	return nil
}

// TestGrantDigestABIStampWrittenBySeal verifies the write half of the
// ABI stamp contract: a normal seal (grants present, and separately
// zero grants at all) writes rawdb.GrantDigestABIStampKey() ==
// GrantDigestABIVersion (uint32 BE), the stamp is visible inside
// [DigestLowerBound, DigestUpperBound), and it is excluded from
// rawdb.DigestNodeKeyspaceBounds() — the presence-probe range that
// must never see it (DigestMetaIndexID).
func TestGrantDigestABIStampWrittenBySeal(t *testing.T) {
	const entID = "ent-A"
	stampKey := rawdb.GrantDigestABIStampKey()

	e, _ := newTestEngine(t)
	seedEntitlement(t, e, entID, makeTestGrants(entID, 20))

	val, closer, err := e.db.Get(stampKey)
	require.NoError(t, err, "stamp must be present after a normal seal")
	got := append([]byte(nil), val...)
	closer.Close()
	require.Equal(t, grantDigestABIStampValue(), got, "stamp value must be the current ABI version, uint32 BE")

	// dumpDigestNodes-style iteration of the whole digest keyspace
	// must surface the stamp.
	nodes := dumpDigestNodes(t, e)
	stampVal, ok := nodes[string(stampKey)]
	require.True(t, ok, "stamp key must be inside [DigestLowerBound, DigestUpperBound)")
	require.Equal(t, grantDigestABIStampValue(), stampVal)

	// But the NODE-only bounds (the presence probe's range) must
	// exclude it.
	nodeLo, nodeHi := rawdb.DigestNodeKeyspaceBounds()
	nodeIter, err := e.db.NewIter(&pebble.IterOptions{LowerBound: nodeLo, UpperBound: nodeHi})
	require.NoError(t, err)
	foundInNodeRange := nodeIter.SeekGE(stampKey) && bytes.Equal(nodeIter.Key(), stampKey)
	require.NoError(t, nodeIter.Error())
	require.NoError(t, nodeIter.Close())
	require.False(t, foundInNodeRange, "stamp key must be excluded from the digest node-keyspace probe bounds")

	// Zero-grant seal path: no entitlements, no grants at all — the
	// stamp must still be written (the "digest was built" certificate
	// covers the zero-chunks branch too).
	e2, _ := newTestEngine(t)
	require.NoError(t, e2.bindCurrentSync(ksuid.New().String()))
	sealGrantDigests(t, e2)
	val2, closer2, err := e2.db.Get(stampKey)
	require.NoError(t, err, "stamp must be present after a zero-grant seal")
	got2 := append([]byte(nil), val2...)
	closer2.Close()
	require.Equal(t, grantDigestABIStampValue(), got2)
}

// TestGrantDigestABIStaleStampDroppedAtWritableOpen verifies the core
// writable-open contract: digest nodes present with a stamp naming a
// different (older) ABI version make Open drop the ENTIRE digest
// state — nodes and the by_entitlement_principal_hash index alike —
// rather than trusting anything under it, so that a subsequent
// EndSync rebuilds it all from scratch at the current ABI.
func TestGrantDigestABIStaleStampDroppedAtWritableOpen(t *testing.T) {
	ctx := context.Background()
	const entID = "ent-A"
	const n = 20

	e, dbDir, syncID := sealedGrantDigestEngine(t, entID, n)
	require.NotZero(t, digestNodeCount(t, e), "precondition: seal must have built digest nodes")
	require.NotZero(t, entHashIndexRowCount(t, e, entID), "precondition: seal must have built hash-index rows")
	require.NoError(t, verifyGrantHashIndexAgainstPrimaries(t, e), "precondition: oracle must pass right after seal")

	setABIStamp(t, e, staleABIVersion) // a fake ABI version that is not the current one
	require.NoError(t, e.Close())

	e2, err := Open(ctx, dbDir)
	require.NoError(t, err, "writable open over a stale-ABI stamp must not error")
	t.Cleanup(func() { _ = e2.Close() })

	require.Zero(t, digestNodeCount(t, e2), "stale-ABI writable open must drop every digest node")
	require.Zero(t, countKeyRangeTest(t, e2, GrantByEntPrincHashLowerBound(), GrantByEntPrincHashUpperBound()),
		"stale-ABI writable open must drop the whole hash index")
	_, ok, err := e2.GetGrantDigestGlobalRoot(ctx)
	require.NoError(t, err)
	require.False(t, ok, "global root must read as absent after the drop")
	require.False(t, e2.grantDigestAbiStale.Load(), "a writable open must drop the state, never set the read-only stale flag")
	require.False(t, e2.db.GrantDigestsPresent())

	// Reseal through the normal repair path (resume the sync +
	// EndSync, like a real second process would) and require the
	// rebuilt state to check out.
	a2 := NewAdapter(e2)
	require.NoError(t, a2.SetCurrentSync(ctx, syncID))
	require.NoError(t, a2.EndSync(ctx))

	require.NotZero(t, digestNodeCount(t, e2), "reseal must rebuild digest nodes")
	require.EqualValues(t, n, entHashIndexRowCount(t, e2, entID), "reseal must rebuild every hash-index row")
	require.NoError(t, verifyGrantHashIndexAgainstPrimaries(t, e2), "oracle must pass over the rebuilt state")

	stampVal, closer, err := e2.db.Get(rawdb.GrantDigestABIStampKey())
	require.NoError(t, err)
	gotStamp := append([]byte(nil), stampVal...)
	closer.Close()
	require.Equal(t, grantDigestABIStampValue(), gotStamp, "reseal must write the CURRENT ABI version")
}

// TestGrantDigestABIMissingStampReadsAsVersion1 pins the reading of a
// file that has digest nodes but no stamp key at all — one sealed by a
// pre-stamp SDK build. Every such build hashed at ABI version 1, so the
// stamp reader must report exactly that, independent of what the
// current GrantDigestABIVersion is: Open then treats the file like any
// other stamped-at-1 file (kept while the current ABI is 1, dropped and
// rebuilt once it is not). The reader-level assertion is the durable
// contract; the Open-level half below is only meaningful while the
// current ABI is still 1 (once it moves, the missing-stamp file is
// just another stale file, covered by
// TestGrantDigestABIStaleStampDroppedAtWritableOpen).
func TestGrantDigestABIMissingStampReadsAsVersion1(t *testing.T) {
	ctx := context.Background()
	const entID = "ent-A"
	const n = 20

	e, dbDir, syncID := sealedGrantDigestEngine(t, entID, n)
	require.NotZero(t, digestNodeCount(t, e), "precondition: seal must have built digest nodes")
	nodesBefore := digestNodeCount(t, e)

	deleteABIStamp(t, e)
	_, _, err := e.db.Get(rawdb.GrantDigestABIStampKey())
	require.ErrorIs(t, err, pebble.ErrNotFound, "precondition: stamp key must be gone")

	stamped, err := e.readGrantDigestABIStamp()
	require.NoError(t, err)
	require.EqualValues(t, 1, stamped, "a missing stamp must read as ABI version 1, the only version pre-stamp SDKs ever hashed at")
	require.NoError(t, e.Close())

	if GrantDigestABIVersion != 1 {
		t.Skip("current ABI is past 1; a missing stamp is now just a stale stamp — see TestGrantDigestABIStaleStampDroppedAtWritableOpen")
	}

	// Current ABI is 1: introducing the stamp must cost a pre-stamp file
	// nothing. Open keeps its digest state, and the next seal merely
	// adds the stamp.
	e2, err := Open(ctx, dbDir)
	require.NoError(t, err, "writable open over digest nodes with NO stamp at all must not error")
	t.Cleanup(func() { _ = e2.Close() })

	require.Equal(t, nodesBefore, digestNodeCount(t, e2), "missing-stamp file at ABI 1 must keep every digest node")
	require.EqualValues(t, n, entHashIndexRowCount(t, e2, entID), "missing-stamp file at ABI 1 must keep every hash-index row")
	_, ok, err := e2.GetGrantDigestGlobalRoot(ctx)
	require.NoError(t, err)
	require.True(t, ok, "global root must still read as present")
	require.False(t, e2.grantDigestAbiStale.Load())
	require.True(t, e2.db.GrantDigestsPresent())
	require.NoError(t, verifyGrantHashIndexAgainstPrimaries(t, e2), "kept state must still check out against the primaries")

	// An unstamped file stays unstamped until something rewrites the
	// global root — a seal that finds nothing missing takes the repair
	// fast path and writes nothing, and that is fine: absence keeps
	// reading as version 1. The first root rewrite (here: one grant
	// mutation invalidates its partition + the root, and EndSync's
	// targeted repair rebuilds both) must stamp the file explicitly.
	a2 := NewAdapter(e2)
	require.NoError(t, a2.SetCurrentSync(ctx, syncID))
	require.NoError(t, e2.PutGrantRecords(ctx, makeGrant("", "g-"+entID+"-extra", entID, "user-extra")))
	require.NoError(t, a2.EndSync(ctx))

	require.EqualValues(t, n+1, entHashIndexRowCount(t, e2, entID), "repair must cover the mutated partition")
	require.NoError(t, verifyGrantHashIndexAgainstPrimaries(t, e2), "repaired state must check out against the primaries")
	stampVal, closer, err := e2.db.Get(rawdb.GrantDigestABIStampKey())
	require.NoError(t, err, "the first global-root rewrite must write the stamp")
	gotStamp := append([]byte(nil), stampVal...)
	closer.Close()
	require.Equal(t, grantDigestABIStampValue(), gotStamp, "the root rewrite must stamp the file at the current ABI")
}

// TestGrantDigestABIStaleReadOnlyOpen verifies the read-only-open
// counterpart: a read-only Open can never drop anything, so a
// stale-ABI file must instead make the digest root getters report
// "never built" (ok=false, err=nil) while leaving every underlying key
// exactly where it was on disk.
func TestGrantDigestABIStaleReadOnlyOpen(t *testing.T) {
	ctx := context.Background()
	const entID = "ent-A"

	e, dbDir, _ := sealedGrantDigestEngine(t, entID, 20)
	setABIStamp(t, e, staleABIVersion)
	require.NoError(t, e.Close())

	e2, err := Open(ctx, dbDir, WithReadOnly(true))
	require.NoError(t, err, "read-only open over a stale-ABI stamp must not error")
	t.Cleanup(func() { _ = e2.Close() })

	require.True(t, e2.grantDigestAbiStale.Load(), "read-only open must set the stale flag rather than drop")

	_, ok, err := e2.GetGrantDigestGlobalRoot(ctx)
	require.NoError(t, err)
	require.False(t, ok, "global root must report not-built under a stale ABI")

	_, ok, err = e2.GetEntitlementDigestRoot(ctx, testEntIdentity(entID))
	require.NoError(t, err)
	require.False(t, ok, "entitlement root must report not-built under a stale ABI")

	// Nothing was dropped: the keys are still on disk.
	require.NotZero(t, digestNodeCount(t, e2), "read-only open must not drop digest nodes")
	require.NotZero(t, countKeyRangeTest(t, e2, GrantByEntPrincHashLowerBound(), GrantByEntPrincHashUpperBound()),
		"read-only open must not drop the hash index")
}

// TestGrantDigestABIStampOrphanIgnored verifies the "empty node
// keyspace" carve-out: a stamp naming the WRONG version sitting over
// an otherwise digest-EMPTY file (no digest nodes ever built) must be
// left alone by a writable open — there is nothing to trust or drop —
// and a subsequent normal sync+seal must build fine and end with the
// CURRENT stamp.
func TestGrantDigestABIStampOrphanIgnored(t *testing.T) {
	ctx := context.Background()
	const entID = "ent-A"
	const n = 10

	e, dir := newTestEngine(t)
	setABIStamp(t, e, 999) // wrong version, no digest nodes exist at all
	require.False(t, e.db.GrantDigestsPresent(), "precondition: no digest nodes exist yet")
	require.NoError(t, e.Close())

	dbDir := filepath.Join(dir, "db")
	e2, err := Open(ctx, dbDir)
	require.NoError(t, err, "writable open over an orphaned stamp with an empty node keyspace must not error")
	t.Cleanup(func() { _ = e2.Close() })
	require.False(t, e2.grantDigestAbiStale.Load())

	a2 := NewAdapter(e2)
	_, err = a2.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	putEnt(t, e2, ctx, entID)
	require.NoError(t, e2.PutGrantRecords(ctx, makeTestGrants(entID, n)...))
	require.NoError(t, a2.EndSync(ctx))

	root, ok, err := e2.GetGrantDigestGlobalRoot(ctx)
	require.NoError(t, err)
	require.True(t, ok, "digests must build fine over an ignored orphan stamp")
	require.EqualValues(t, n, root.Count)
	require.NoError(t, verifyGrantHashIndexAgainstPrimaries(t, e2))

	stampVal, closer, err := e2.db.Get(rawdb.GrantDigestABIStampKey())
	require.NoError(t, err)
	gotStamp := append([]byte(nil), stampVal...)
	closer.Close()
	require.Equal(t, grantDigestABIStampValue(), gotStamp)
}

// TestGrantDigestABIOracle validates verifyGrantHashIndexAgainstPrimaries
// itself: it must pass right after a clean seal, and it must detect a
// deliberately tampered hash-index row (proving it is a real oracle,
// not a tautology) — the same helper TestGrantDigestABIStaleStampDroppedAtWritableOpen
// and TestGrantDigestABIMissingStampTreatedAsStale rely on to certify a
// rebuilt file.
func TestGrantDigestABIOracle(t *testing.T) {
	const entID = "ent-A"
	e, _ := newTestEngine(t)
	seedEntitlement(t, e, entID, makeTestGrants(entID, 20))

	require.NoError(t, verifyGrantHashIndexAgainstPrimaries(t, e), "oracle must pass right after a clean seal")

	// Flip a byte in one hash-index row's stored content hash. There is
	// no production API for this (the family's writers only ever Set a
	// row they themselves derived), so this goes through the raw
	// pebble handle — exactly the corruption-planter use case
	// rawdb.DB.UnsafeForTesting documents.
	prefix := rawdb.GrantHashIndexEntitlementPrefix(testEntPartition(entID))
	iter, err := e.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: upperBoundOf(prefix)})
	require.NoError(t, err)
	require.True(t, iter.First(), "expected at least one hash-index row to tamper")
	key := append([]byte(nil), iter.Key()...)
	val := append([]byte(nil), iter.Value()...)
	require.NoError(t, iter.Close())

	val[len(val)-1] ^= 0xFF
	require.NoError(t, e.db.UnsafeForTesting().Set(key, val, pebble.Sync))

	err = verifyGrantHashIndexAgainstPrimaries(t, e)
	require.Error(t, err, "the oracle must detect a tampered hash-index row")
}

// TestGrantDigestABIStaleWithPendingMarker verifies Open handles BOTH
// crash markers armed at once: a stale ABI stamp AND the digest-build
// pending marker (encodeGrantDigestBuildPendingKey, the crash-window
// guard grant_digest_build_crash_test.go exercises on its own). Open
// must succeed, end with every digest range empty, and still support a
// normal reseal afterward.
func TestGrantDigestABIStaleWithPendingMarker(t *testing.T) {
	ctx := context.Background()
	const entID = "ent-A"
	const n = 20

	e, dbDir, syncID := sealedGrantDigestEngine(t, entID, n)
	require.NotZero(t, digestNodeCount(t, e), "precondition: seal must have built digest nodes")

	setABIStamp(t, e, staleABIVersion)
	require.NoError(t, e.db.MetaSet(encodeGrantDigestBuildPendingKey(), nil, pebble.Sync))
	require.NoError(t, e.Close())

	e2, err := Open(ctx, dbDir)
	require.NoError(t, err, "open must succeed with both the stale stamp and the pending marker armed")
	t.Cleanup(func() { _ = e2.Close() })

	require.False(t, e2.grantDigestBuildPending.Load(), "the pending marker must be consumed at open")
	require.Zero(t, digestNodeCount(t, e2), "digest nodes must be empty after open")
	require.Zero(t, countKeyRangeTest(t, e2, GrantByEntPrincHashLowerBound(), GrantByEntPrincHashUpperBound()),
		"the hash index must be empty after open")
	_, ok, err := e2.GetGrantDigestGlobalRoot(ctx)
	require.NoError(t, err)
	require.False(t, ok)

	a2 := NewAdapter(e2)
	require.NoError(t, a2.SetCurrentSync(ctx, syncID))
	require.NoError(t, a2.EndSync(ctx))

	require.NotZero(t, digestNodeCount(t, e2), "reseal must rebuild digest nodes")
	require.EqualValues(t, n, entHashIndexRowCount(t, e2, entID))
	require.NoError(t, verifyGrantHashIndexAgainstPrimaries(t, e2))
}
