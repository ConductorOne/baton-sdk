package pebble

// Pins the orphan by_principal handling in the dangling-principal sweep
// (I9's engine side): an index entry with no primary grant row must be
// HEALED (deleted), never vacuously classified. Without the heal, a
// principal whose entries are all orphans comes back as "all
// match-annotated" with zero carriers — skipping the warn/fail arms,
// emitting no signal, and leaving the garbage for every future sweep.

import (
	"context"
	"fmt"
	"testing"

	cpebble "github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/internal/rawdb"
)

// plantOrphanPrincipalIndexEntry writes ONLY a by_principal index key —
// no primary row — simulating a writer bug that stranded the entry.
// This state is unconstructible through the production API (every
// index write rides a typed rawdb op that stages the primary row with
// it), hence the UnsafeForTesting corruption planter.
func plantOrphanPrincipalIndexEntry(t *testing.T, e *Engine, principalID, entTail string) grantIdentity {
	t.Helper()
	id := grantIdentity{
		entitlement:     entitlementIdentityFromParts("group", "g1", "group:g1:"+entTail),
		principalTypeID: "user",
		principalID:     principalID,
	}
	require.NoError(t, e.db.UnsafeForTesting().Set(encodeGrantByPrincipalIdentityIndexKey(id), nil, nil))
	return id
}

func countPrincipalIndexEntries(t *testing.T, e *Engine, principalRT, principalID string) int {
	t.Helper()
	ids, err := e.grantIdentitiesForPrincipal(context.Background(), principalRT, principalID)
	require.NoError(t, err)
	return len(ids)
}

func TestDanglingPrincipalSweepHealsAllOrphanIndexEntries(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	e := a.PebbleEngine()

	// "ghost" has index entries but no grant rows and no resource row:
	// the all-orphan shape. Without the heal this would visit as
	// (matchAnnotatedOnly=true, carrierGrants=0) and nothing would be
	// cleaned.
	for i := 0; i < 3; i++ {
		plantOrphanPrincipalIndexEntry(t, e, "ghost", fmt.Sprintf("member%d", i))
	}

	visited := 0
	require.NoError(t, e.ForEachDanglingGrantPrincipal(ctx, func(rt, rid string, matchOnly bool, carriers int64) error {
		visited++
		return nil
	}))
	require.Zero(t, visited, "an all-orphan principal has no grants to judge and must not be visited")
	require.Zero(t, countPrincipalIndexEntries(t, e, "user", "ghost"),
		"orphan index entries must be healed, not tolerated forever")

	// Idempotent: a re-run sees a clean index.
	require.NoError(t, e.ForEachDanglingGrantPrincipal(ctx, func(rt, rid string, matchOnly bool, carriers int64) error {
		visited++
		return nil
	}))
	require.Zero(t, visited)
}

// TestEnsureGrantIndexesClearsDeferredMarker pins the marker half of
// EnsureGrantIndexes' contract (mutation-sweep finding: the build ran
// and I9 scanned correctly even when the clear was deleted, so nothing
// else defends it): after the forced build, the in-memory flag AND the
// durable key must both be retired — a leftover marker makes EndSync
// repeat the whole O(grants) rebuild, and the I9 crash seam
// ("invariants-I9-indexes-ensured") documents "marker durably cleared".
func TestEnsureGrantIndexesClearsDeferredMarker(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	e := a.PebbleEngine()

	// The deferred regime arms the marker.
	require.NoError(t, a.Grants().StoreExpandedGrants(ctx, mkV2Grant("", "ent-x", "user", "alice")))
	require.True(t, e.db.DeferredIdxPending(), "a deferred grant write must arm the marker")

	require.NoError(t, e.EnsureGrantIndexes(ctx))
	require.False(t, e.db.DeferredIdxPending(),
		"EnsureGrantIndexes must retire the in-memory flag or EndSync repeats the O(grants) rebuild")
	_, closer, err := e.db.Get(rawdb.DeferredIdxPendingKey())
	if err == nil {
		closer.Close()
	}
	require.ErrorIs(t, err, cpebble.ErrNotFound,
		"the durable marker key must be deleted with the flag (flag/key agreement; the I9 crash seam claims 'marker durably cleared')")

	// Idempotent: a re-run takes the no-pending fast path.
	require.NoError(t, e.EnsureGrantIndexes(ctx))
}

// TestHealOrphanPrincipalIndexEntriesKeepsLiveRows pins the heal's
// under-lock re-probe (mutation-sweep finding: every existing fixture
// reaches the heal only via the all-orphan sweep gate, so a heal that
// skipped the probe and deleted EVERY entry was behaviorally invisible).
// Called directly with a mixed population, the heal must delete exactly
// the orphan and keep the live row's index entry — healing a live row's
// entry would orphan the row from its own index.
func TestHealOrphanPrincipalIndexEntriesKeepsLiveRows(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	e := a.PebbleEngine()

	require.NoError(t, a.PutGrants(ctx, mkV2Grant("", "ent-live", "user", "victim")))
	plantOrphanPrincipalIndexEntry(t, e, "victim", "ghost-ent")
	require.Equal(t, 2, countPrincipalIndexEntries(t, e, "user", "victim"))

	healed, err := e.healOrphanPrincipalIndexEntries(ctx, "user", "victim")
	require.NoError(t, err)
	require.Equal(t, int64(1), healed, "exactly the orphan heals; the re-probe must keep the live row's entry")

	var ents []string
	require.NoError(t, e.IterateGrantsByPrincipal(ctx, "user", "victim", func(r *v3.GrantRecord) bool {
		ents = append(ents, r.GetEntitlement().GetEntitlementId())
		return true
	}))
	require.Equal(t, []string{canonicalTestEntID("ent-live")}, ents,
		"the live grant must still be served through by_principal after the heal")
}

// TestIngestScansFailLoudOnMalformedKeys pins the malformed-key error
// arms of the invariant scans: a keyspace row that does not decode as
// its family's tuple shape must fail the scan loudly — silently
// skipping it would let a corrupted keyspace pass referential
// validation. Malformed keys are unconstructible through the
// production API (every write rides a typed, key-validated rawdb op),
// hence the UnsafeForTesting planters.
func TestIngestScansFailLoudOnMalformedKeys(t *testing.T) {
	ctx := context.Background()

	t.Run("grant primary keyspace", func(t *testing.T) {
		a := newAdapter(t)
		_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)
		e := a.PebbleEngine()
		// A grant-family key whose tail has too few tuple components.
		bad := append(encodeGrantPrefix(), 0x00, 'x')
		require.NoError(t, e.db.UnsafeForTesting().Set(bad, nil, nil))

		err = e.ForEachDistinctGrantEntitlementResource(ctx, func(string, string) error { return nil })
		require.Error(t, err)
		require.Contains(t, err.Error(), "malformed")
		err = e.ForEachDanglingGrantEntitlement(ctx, func(string, string, string) error { return nil })
		require.Error(t, err)
		require.Contains(t, err.Error(), "malformed")
	})

	t.Run("entitlement primary keyspace", func(t *testing.T) {
		a := newAdapter(t)
		_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)
		e := a.PebbleEngine()
		bad := append(encodeEntitlementPrefix(), 0x00, 'x')
		require.NoError(t, e.db.UnsafeForTesting().Set(bad, nil, nil))

		err = e.ForEachDistinctEntitlementResource(ctx, func(string, string) error { return nil })
		require.Error(t, err)
		require.Contains(t, err.Error(), "malformed")
	})

	t.Run("by_principal index keyspace", func(t *testing.T) {
		a := newAdapter(t)
		_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)
		e := a.PebbleEngine()
		bad := []byte{versionV3, typeIndex, idxGrantByPrincipal, 0x00, 'x'}
		require.NoError(t, e.db.UnsafeForTesting().Set(bad, nil, nil))

		err = e.ForEachDanglingGrantPrincipal(ctx, func(string, string, bool, int64) error { return nil })
		require.Error(t, err)
		require.Contains(t, err.Error(), "malformed")
	})
}

// TestIngestScanSurfaceAfterCloseReturnsClosing pins the lifecycle
// contract on the invariant scan surface: a retained handle used after
// Close must get ErrEngineClosing, never a nil-map/nil-pointer panic —
// the same per-call guard the promoted merge surface carries.
func TestIngestScanSurfaceAfterCloseReturnsClosing(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	e := a.PebbleEngine()
	require.NoError(t, e.Close())

	visit2 := func(string, string) error { return nil }
	require.ErrorIs(t, e.ForEachDistinctGrantEntitlementResource(ctx, visit2), ErrEngineClosing)
	require.ErrorIs(t, e.ForEachDistinctEntitlementResource(ctx, visit2), ErrEngineClosing)
	require.ErrorIs(t, e.ForEachDanglingGrantEntitlement(ctx, func(string, string, string) error { return nil }), ErrEngineClosing)
	require.ErrorIs(t, e.EnsureGrantIndexes(ctx), ErrEngineClosing)
	require.ErrorIs(t, e.ForEachDanglingGrantPrincipal(ctx, func(string, string, bool, int64) error { return nil }), ErrEngineClosing)
	_, err = e.HasResourceRecord(ctx, "user", "alice")
	require.ErrorIs(t, err, ErrEngineClosing)
	_, err = e.GrantsForEntResourceCarryInsertFact(ctx, "group", "g1")
	require.ErrorIs(t, err, ErrEngineClosing)
	_, err = e.GrantsForEntitlementAllCarryInsertFact(ctx, "group:g1:member", "group", "g1")
	require.ErrorIs(t, err, ErrEngineClosing)
}

// TestDanglingPrincipalSweepKeepsMixedPopulations pins the boundary of
// the heal: a dangling principal with REAL grant rows is visited (its
// verdict belongs to the syncer's policy arms), and its live index
// entries — including entries alongside a planted orphan — survive.
func TestDanglingPrincipalSweepKeepsMixedPopulations(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	e := a.PebbleEngine()

	// A real grant to a principal with no resource row (a genuine
	// dangling reference) PLUS a planted orphan entry under the same
	// principal: the population is mixed, so the principal must be
	// visited for the syncer's verdict — and the sweep must not touch
	// its index entries, live OR orphan (healing is reserved for the
	// all-orphan shape; a mixed principal's orphan is a documented
	// residual until the drop pass reworks this in a later PR).
	g := mkV2Grant("", "ent-a", "user", "mixed")
	require.NoError(t, a.PutGrants(ctx, g))
	plantOrphanPrincipalIndexEntry(t, e, "mixed", "stranded")
	require.Equal(t, 2, countPrincipalIndexEntries(t, e, "user", "mixed"))

	visited := 0
	require.NoError(t, e.ForEachDanglingGrantPrincipal(ctx, func(rt, rid string, matchOnly bool, carriers int64) error {
		visited++
		require.Equal(t, "user", rt)
		require.Equal(t, "mixed", rid)
		require.False(t, matchOnly, "a plain grant is not a match carrier")
		require.Zero(t, carriers, "no carrier grants in this population")
		return nil
	}))
	require.Equal(t, 1, visited, "a principal with real grant rows must be visited, not healed")
	require.Equal(t, 2, countPrincipalIndexEntries(t, e, "user", "mixed"),
		"the sweep must not delete ANY index entry of a visited (mixed) principal — the live entry especially")

	// The live entry still serves reads after the sweep.
	var ents []string
	require.NoError(t, e.IterateGrantsByPrincipal(ctx, "user", "mixed", func(r *v3.GrantRecord) bool {
		ents = append(ents, r.GetEntitlement().GetEntitlementId())
		return true
	}))
	require.Equal(t, []string{canonicalTestEntID("ent-a")}, ents)
}
