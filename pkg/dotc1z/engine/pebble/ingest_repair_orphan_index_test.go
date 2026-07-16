package pebble

// Pins the orphan by_principal handling in the dangling-principal sweep
// (I9's engine side): an index entry with no primary grant row must be
// HEALED (deleted), never vacuously classified. Before the fix, a
// principal whose entries were all orphans came back as "all
// match-annotated" with zero carriers — skipping the fail/drop arms,
// emitting no signal, and leaving the garbage for every future sweep —
// and the drop path kept orphan keys behind the rows it deleted.

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// plantOrphanPrincipalIndexEntry writes ONLY a by_principal index key —
// no primary row — simulating a writer bug that stranded the entry.
func plantOrphanPrincipalIndexEntry(t *testing.T, e *Engine, principalID, entTail string) grantIdentity {
	t.Helper()
	id := grantIdentity{
		entitlement:     entitlementIdentityFromParts("group", "g1", "group:g1:"+entTail),
		principalTypeID: "user",
		principalID:     principalID,
	}
	require.NoError(t, e.db.Set(encodeGrantByPrincipalIdentityIndexKey(id), nil, nil))
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
	// the all-orphan shape. Before the fix this visited as
	// (matchAnnotatedOnly=true, carrierGrants=0) and nothing was cleaned.
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

func TestDeleteGrantsForPrincipalHealsOrphanEntries(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	e := a.PebbleEngine()

	// Mixed population: one REAL dangling grant row (drops) plus one
	// orphan index entry (must be healed by the same pass, not left
	// behind the dropped rows).
	g := mkV2Grant("g-real", "group:g1:member", "user", "mixed")
	require.NoError(t, a.PutGrants(ctx, g))
	plantOrphanPrincipalIndexEntry(t, e, "mixed", "owner")
	require.Equal(t, 2, countPrincipalIndexEntries(t, e, "user", "mixed"))

	deleted, skipped, err := e.DeleteGrantsForPrincipal(ctx, "user", "mixed")
	require.NoError(t, err)
	require.Equal(t, int64(1), deleted, "the real dangling row drops")
	require.Zero(t, skipped)
	require.Zero(t, countPrincipalIndexEntries(t, e, "user", "mixed"),
		"the orphan entry must be healed in the same pass")

	_, err = e.GetGrantRecord(ctx, g.GetId())
	require.ErrorIs(t, err, pebble.ErrNotFound)
}
