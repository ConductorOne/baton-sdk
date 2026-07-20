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

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
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

	// A real grant to a principal with no resource row: a genuine
	// dangling reference, not index garbage.
	g := mkV2Grant("", "ent-a", "user", "mixed")
	require.NoError(t, a.PutGrants(ctx, g))
	require.Equal(t, 1, countPrincipalIndexEntries(t, e, "user", "mixed"))

	visited := 0
	require.NoError(t, e.ForEachDanglingGrantPrincipal(ctx, func(rt, rid string, matchOnly bool, carriers int64) error {
		visited++
		require.Equal(t, "user", rt)
		require.Equal(t, "mixed", rid)
		require.False(t, matchOnly, "a plain grant is not a match carrier")
		return nil
	}))
	require.Equal(t, 1, visited, "a principal with real grant rows must be visited, not healed")
	require.Equal(t, 1, countPrincipalIndexEntries(t, e, "user", "mixed"),
		"live index entries must survive the sweep")
}
