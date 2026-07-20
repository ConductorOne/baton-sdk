package pebble

// Direct engine-level coverage for the typed-op paths a coverage audit
// (2026-07-17) found exercised only cross-package or not at all:
// the synthesized-grant puts (deferred regime, driven in production by
// pkg/sync's expander), the trusted-import path, DeleteResourceRecord
// (exported but currently caller-less — kept honest here until its
// removal is decided), and the IfNewer resource branches restructured
// in 2b. Obligations are asserted directly against the index keyspaces
// so a regression fails HERE, not two packages away in an equivalence
// suite.

import (
	"context"
	"errors"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/internal/rawdb"
	batonGrant "github.com/conductorone/baton-sdk/pkg/types/grant"
)

func countKeys(t *testing.T, e *Engine, prefix []byte) int {
	t.Helper()
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upperBoundOf(prefix),
	})
	require.NoError(t, err)
	defer iter.Close()
	n := 0
	for iter.First(); iter.Valid(); iter.Next() {
		n++
	}
	require.NoError(t, iter.Error())
	return n
}

func testGrantRecord(ent, principal string) *v3.GrantRecord {
	return v3.GrantRecord_builder{
		ExternalId: "app:github:" + ent + ":user:" + principal,
		Entitlement: v3.EntitlementRef_builder{
			ResourceTypeId: "app", ResourceId: "github", EntitlementId: "app:github:" + ent,
		}.Build(),
		Principal: v3.PrincipalRef_builder{
			ResourceTypeId: "user", ResourceId: principal,
		}.Build(),
		DiscoveredAt: timestamppb.Now(),
	}.Build()
}

// TestPutSynthesizedGrantRecordsObligations drives the deferred-regime
// synthesized put directly: rows land, the deferred marker is armed,
// no needs_expansion entries appear (synth grants are never
// expandable), and the seal-rebuilt by_principal serves every row.
func TestPutSynthesizedGrantRecordsObligations(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	e := a.PebbleEngine()

	require.NoError(t, e.PutSynthesizedGrantRecords(ctx, []*v3.GrantRecord{
		testGrantRecord("ent-A", "alice"),
		testGrantRecord("ent-A", "bob"),
	}))
	require.True(t, e.db.DeferredIdxPending(), "synthesized puts must arm the deferred rebuild marker")
	require.Equal(t, 0, countKeys(t, e, encodeGrantByNeedsExpansionPrefix()),
		"synthesized grants are never expandable")

	require.NoError(t, a.EndSync(ctx))
	for _, p := range []string{"alice", "bob"} {
		n := 0
		require.NoError(t, e.IterateGrantsByPrincipal(ctx, "user", p, func(*v3.GrantRecord) bool {
			n++
			return true
		}))
		require.Equalf(t, 1, n, "seal-rebuilt by_principal must serve %s", p)
	}
}

// TestPutSynthesizedGrantContributionsBatchObligations drives the
// non-layer contributions fallback (the expander path when no layer
// session is open) through the same obligation assertions.
func TestPutSynthesizedGrantContributionsBatchObligations(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	e := a.PebbleEngine()

	dest := v3.EntitlementRef_builder{
		ResourceTypeId: "app", ResourceId: "github", EntitlementId: "app:github:ent-A",
	}.Build()
	records := []synthesizedGrantRecord{
		{
			id: grantIdentity{
				entitlement:     entitlementIdentityFromParts("app", "github", "app:github:ent-A"),
				principalTypeID: "user",
				principalID:     "carol",
			},
			entitlement: dest,
			principal:   v3.PrincipalRef_builder{ResourceTypeId: "user", ResourceId: "carol"}.Build(),
			sources:     batonGrant.Sources{{EntitlementID: "app:github:src", IsDirect: true}},
		},
	}
	// Through the exported dispatcher so its counter bookkeeping and the
	// batch body are both exercised.
	require.NoError(t, e.PutSynthesizedGrantContributions(ctx, records))
	require.True(t, e.db.DeferredIdxPending(), "contributions must arm the deferred rebuild marker")

	require.NoError(t, a.EndSync(ctx))
	n := 0
	require.NoError(t, e.IterateGrantsByPrincipal(ctx, "user", "carol", func(r *v3.GrantRecord) bool {
		require.NotEmpty(t, r.GetSources(), "contribution sources must round-trip the wire append")
		n++
		return true
	}))
	require.Equal(t, 1, n)
}

// TestUnsafePutUniqueGrantRecordsObligations covers the trusted-import
// body: fresh-sync gate, parallel encode, inline-regime staging with
// needs_expansion, and the digest-present refusal guard.
func TestUnsafePutUniqueGrantRecordsObligations(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	syncID, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	e := a.PebbleEngine()

	expandable := testGrantRecord("ent-A", "alice")
	expandable.SetNeedsExpansion(true)
	plain := testGrantRecord("ent-B", "bob")
	require.NoError(t, e.UnsafePutUniqueGrantRecords(ctx, expandable, plain))

	require.Equal(t, 2, countKeys(t, e, encodeGrantPrefix()), "both rows must land")
	require.Equal(t, 1, countKeys(t, e, encodeGrantByNeedsExpansionPrefix()),
		"inline regime must index exactly the expandable row")
	require.Equal(t, 2, countKeys(t, e, GrantByPrincipalLowerBound()),
		"inline regime must write by_principal for every row")

	// Digest-present refusal: on a fresh sync digest state is provably
	// absent (StartNewSync excised it), so a present flag means the
	// freshness contract itself broke — the guard must refuse rather
	// than silently tombstone a trusted import's digest keyspace.
	e.db.SetGrantDigestsPresent(true)
	require.ErrorContains(t, e.UnsafePutUniqueGrantRecords(ctx, testGrantRecord("ent-C", "carol")),
		"digest state present on a fresh sync")
	e.db.SetGrantDigestsPresent(false)

	// Non-fresh syncs are refused (SetCurrentSync clears freshness).
	require.NoError(t, a.EndSync(ctx))
	require.NoError(t, a.SetCurrentSync(ctx, syncID))
	require.ErrorContains(t, e.UnsafePutUniqueGrantRecords(ctx, testGrantRecord("ent-C", "carol")),
		"sync is not fresh")
}

// TestGrantDeleteMalformedValueStillCleansObligations pins the
// key-derived cleanup contract that motivated the 2b delete-path
// tightening: a grant whose stored VALUE is garbage (unmarshalable
// legacy row) but whose KEY is well-formed must still get full index
// cleanup and digest invalidation on delete. The pre-2b path derived
// cleanup from the prior value and silently SKIPPED both when the
// value's identity fields were missing — deleting the primary while
// leaving a stale-but-present digest (a present-means-exact hole) and
// orphan index rows. Key-derived cleanup cannot be skipped.
func TestGrantDeleteMalformedValueStillCleansObligations(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	syncID, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	e := a.PebbleEngine()

	// A healthy grant first, sealed so digests exist; rebind for the
	// post-seal mutation (the partial-sync shape where digests are
	// present and the delete's invalidation obligation is live).
	require.NoError(t, e.PutGrantRecords(ctx, testGrantRecord("ent-A", "alice")))
	require.NoError(t, a.EndSync(ctx))
	require.NoError(t, a.SetCurrentSync(ctx, syncID))
	_, ok, err := e.GetGrantDigestGlobalRoot(ctx)
	require.NoError(t, err)
	require.True(t, ok, "seal must build digests")

	// Corrupt the stored VALUE in place (key untouched) — the malformed
	// legacy-row shape the old value-derived cleanup choked on.
	id := grantIdentity{
		entitlement:     entitlementIdentityFromParts("app", "github", canonicalTestEntID("ent-A")),
		principalTypeID: "user",
		principalID:     "alice",
	}
	key := encodeGrantIdentityKey(id)
	require.NoError(t, e.db.UnsafeForTesting().Set(key, []byte("\xff not a proto"), pebble.Sync))

	// Delete via structural refs (DeleteGrantByIdentityRefs encodes the
	// key from the record's refs; it never unmarshals the stored
	// value). Both index families and the digest root must be cleaned
	// even though the value is garbage.
	rec := testGrantRecord("ent-A", "alice")
	require.NoError(t, e.DeleteGrantByIdentityRefs(ctx, rec))

	_, closer, err := e.db.Get(key)
	require.ErrorIs(t, err, pebble.ErrNotFound, "primary must be gone")
	_ = closer
	require.Equal(t, 0, countKeys(t, e, GrantByPrincipalLowerBound()), "by_principal must be cleaned from the key alone")
	require.Equal(t, 0, countKeys(t, e, encodeGrantByNeedsExpansionPrefix()), "needs_expansion must be cleaned from the key alone")
	_, ok, err = e.GetGrantDigestGlobalRoot(ctx)
	require.NoError(t, err)
	require.False(t, ok, "digest must read as invalidated, never stale-but-present over a deleted row")
}

// TestDeleteResourceRecordObligations covers the exported delete path
// end-to-end: the by_parent entry created by the put must be cleaned
// by the delete, and deleting a non-existent resource is a no-op.
// (DeleteResourceRecord currently has no in-repo callers; this test
// keeps its obligations honest until removal is decided.)
func TestDeleteResourceRecordObligations(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	e := a.PebbleEngine()

	child := v3.ResourceRecord_builder{
		ResourceTypeId: "group",
		ResourceId:     "child-1",
		Parent: v3.ResourceRef_builder{
			ResourceTypeId: "org", ResourceId: "parent-1",
		}.Build(),
	}.Build()
	require.NoError(t, e.PutResourceRecords(ctx, child))

	byParentPrefix := []byte{versionV3, typeIndex, idxResourceByParent}
	require.Equal(t, 1, countKeys(t, e, byParentPrefix), "put must index the parent edge")

	require.NoError(t, e.DeleteResourceRecord(ctx, "group", "child-1"))
	require.Equal(t, 0, countKeys(t, e, byParentPrefix), "delete must clean the parent edge")
	_, err = e.GetResourceRecord(ctx, "group", "child-1")
	require.ErrorIs(t, err, pebble.ErrNotFound)

	// Non-existent delete: clean no-op.
	require.NoError(t, e.DeleteResourceRecord(ctx, "group", "never-existed"))
}

// TestDeferredMarkerArmFailureRollsBackCAS pins the crash contract of
// the deferred-index marker: when the marker's durable commit fails,
// the in-memory CAS must roll back so flag and key never disagree.
// An armed flag with no durable key is the silent-divergence state —
// the in-process EndSync would rebuild by_principal while a
// crash+resume, seeing no key, would skip the rebuild and seal an
// artifact missing index rows.
func TestDeferredMarkerArmFailureRollsBackCAS(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	e := a.PebbleEngine()

	injected := errNoRetryArmFailure
	e.db.SetDeferredMarkerTestHooks(func() error { return injected }, nil)

	// The deferred-regime write must FAIL when the marker can't arm —
	// committing deferred rows without the durable marker is the lie.
	err = e.PutSynthesizedGrantRecords(ctx, []*v3.GrantRecord{testGrantRecord("ent-A", "alice")})
	require.ErrorIs(t, err, injected, "deferred write must surface the arm failure")

	// THE CONTRACT: flag rolled back, durable key absent — in agreement.
	require.False(t, e.db.DeferredIdxPending(), "failed arm must roll the CAS back")
	_, closer, getErr := e.db.Get(rawdb.DeferredIdxPendingKey())
	require.ErrorIs(t, getErr, pebble.ErrNotFound, "no durable marker may exist after a failed arm")
	if getErr == nil {
		closer.Close()
	}
	// And no row from the failed batch may have landed (the batch was
	// abandoned uncommitted).
	require.Equal(t, 0, countKeys(t, e, encodeGrantPrefix()), "the failed batch must not have committed rows")

	// Retry converges: with the fault gone, the same write arms the
	// marker durably and lands.
	e.db.SetDeferredMarkerTestHooks(nil, nil)
	require.NoError(t, e.PutSynthesizedGrantRecords(ctx, []*v3.GrantRecord{testGrantRecord("ent-A", "alice")}))
	require.True(t, e.db.DeferredIdxPending())
	_, closer, getErr = e.db.Get(rawdb.DeferredIdxPendingKey())
	require.NoError(t, getErr, "the retried arm must persist the durable marker")
	closer.Close()

	// Same-process EndSync consumes the armed marker and rebuilds the
	// index. (The crash side of the contract — the durable key
	// re-arming the flag on a REOPEN — is pinned by the errorfs sweep's
	// resume arm, not here; this test stays in-process.)
	require.NoError(t, a.EndSync(ctx))
	n := 0
	require.NoError(t, e.IterateGrantsByPrincipal(ctx, "user", "alice", func(*v3.GrantRecord) bool {
		n++
		return true
	}))
	require.Equal(t, 1, n)
}

var errNoRetryArmFailure = errors.New("injected deferred-marker arm failure")

// TestDeferredMarkerClearFailureKeepsAgreement pins the CLEAR edge of
// the flag/key-agreement contract: when the marker's durable delete
// fails at EndSync, both halves must stay ARMED so the retried EndSync
// re-runs the (idempotent) rebuild and retries the clear — never the
// old flag-cleared/key-present split, which skipped the retry's
// rebuild and left a stale key forcing a spurious rebuild at the next
// open.
func TestDeferredMarkerClearFailureKeepsAgreement(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	e := a.PebbleEngine()

	// Deferred-regime write arms the marker.
	require.NoError(t, e.PutSynthesizedGrantRecords(ctx, []*v3.GrantRecord{testGrantRecord("ent-A", "alice")}))
	require.True(t, e.db.DeferredIdxPending())

	injected := errors.New("injected deferred-marker clear failure")
	e.db.SetDeferredMarkerTestHooks(nil, func() error { return injected })
	err = a.EndSync(ctx)
	require.ErrorIs(t, err, injected, "a failed marker clear must fail EndSync")

	// THE CONTRACT: both halves still armed, in agreement.
	require.True(t, e.db.DeferredIdxPending(), "failed clear must leave the flag armed")
	_, closer, getErr := e.db.Get(rawdb.DeferredIdxPendingKey())
	require.NoError(t, getErr, "failed clear must leave the durable key present")
	closer.Close()

	// Retry converges: rebuild re-runs (idempotent), clear succeeds,
	// both halves disarm, and the index serves the rows.
	e.db.SetDeferredMarkerTestHooks(nil, nil)
	require.NoError(t, a.EndSync(ctx))
	require.False(t, e.db.DeferredIdxPending())
	_, _, getErr = e.db.Get(rawdb.DeferredIdxPendingKey())
	require.ErrorIs(t, getErr, pebble.ErrNotFound)
	n := 0
	require.NoError(t, e.IterateGrantsByPrincipal(ctx, "user", "alice", func(*v3.GrantRecord) bool {
		n++
		return true
	}))
	require.Equal(t, 1, n)
}

// TestPutResourceRecordsIfNewerBranches exercises all three branches
// of the 2b-restructured control flow: no prior (write), prior older
// (overwrite with by_parent swap), prior newer (skip).
func TestPutResourceRecordsIfNewerBranches(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	e := a.PebbleEngine()

	old := timestamppb.New(timestamppb.Now().AsTime().Add(-1000))
	newer := timestamppb.Now()

	mk := func(parent string, at *timestamppb.Timestamp) *v3.ResourceRecord {
		b := v3.ResourceRecord_builder{
			ResourceTypeId: "group",
			ResourceId:     "r1",
			DiscoveredAt:   at,
		}
		if parent != "" {
			b.Parent = v3.ResourceRef_builder{ResourceTypeId: "org", ResourceId: parent}.Build()
		}
		return b.Build()
	}
	byParentPrefix := []byte{versionV3, typeIndex, idxResourceByParent}

	// Branch 1: no prior → written, indexed under parent-A.
	require.NoError(t, e.PutResourceRecordsIfNewer(ctx, mk("parent-A", old)))
	require.Equal(t, 1, countKeys(t, e, byParentPrefix))

	// Branch 2: strictly newer → overwritten, index swapped to parent-B.
	require.NoError(t, e.PutResourceRecordsIfNewer(ctx, mk("parent-B", newer)))
	require.Equal(t, 1, countKeys(t, e, byParentPrefix), "old parent edge must be cleaned, new one written")
	got, err := e.GetResourceRecord(ctx, "group", "r1")
	require.NoError(t, err)
	require.Equal(t, "parent-B", got.GetParent().GetResourceId())

	// Branch 3: not newer → skipped entirely (record and index untouched).
	require.NoError(t, e.PutResourceRecordsIfNewer(ctx, mk("parent-C", old)))
	got, err = e.GetResourceRecord(ctx, "group", "r1")
	require.NoError(t, err)
	require.Equal(t, "parent-B", got.GetParent().GetResourceId(), "stale write must not land")
}
