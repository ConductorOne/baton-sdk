package pebble

import (
	"context"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/require"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
)

// TestMergeFilesIntoOverlayNewerDiscoveredAtWins locks in the overlay
// winner rule: per key, the record with the strictly newest
// discovered_at wins regardless of source order, and ties keep the
// earliest admission (the newest source) — identical to K-way's
// runRecordIsNewer and the sqlite attached compactor. This exercises
// the replaceRaw path: sources[1] (the OLDER source) carries a newer
// discovered_at for "shared", so it must replace sources[0]'s
// already-admitted record, including its derived index keys.
func TestMergeFilesIntoOverlayNewerDiscoveredAtWins(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	older := time.Unix(1000, 0).UTC()
	newer := time.Unix(2000, 0).UTC()
	tie := time.Unix(3000, 0).UTC()
	// sources[0]: scanned first (overlay goes newest-to-oldest).
	src1 := writeKWaySource(t, ctx, filepath.Join(dir, "src1.c1z"), []kwayGrantSpec{
		{id: "shared-old", principalID: "alice", entitlement: "shared", discovered: older},
		{id: "tie-old", principalID: "alice", entitlement: "tie", discovered: tie},
	}, false)
	// sources[1]: older source (the LAST source, so it takes the
	// filtered whole-source SST path) with all three branches:
	// "shared" is seen with a strictly newer discovered_at (replace),
	// "tie" is seen with an equal one (filtered out), and "only-src2"
	// is unseen (streams into the ingested SST).
	src2 := writeKWaySource(t, ctx, filepath.Join(dir, "src2.c1z"), []kwayGrantSpec{
		{id: "shared-new", principalID: "alice", entitlement: "shared", discovered: newer},
		{id: "tie-new", principalID: "alice", entitlement: "tie", discovered: tie},
		{id: "only-src2", principalID: "carol", entitlement: "member", discovered: older},
	}, false)

	// Force the filtered whole-source SST path despite the tiny fixture
	// so all three last-source branches (replace, filter, SST stream)
	// stay covered; the size gate would otherwise route this through
	// the batch path.
	oldMin := overlayWholeSourceMinKeys
	overlayWholeSourceMinKeys = 1
	defer func() { overlayWholeSourceMinKeys = oldMin }()

	dest, _ := newEngine(t, "overlay-winner-dest")
	destSyncID := ksuid.New().String()
	stats, err := MergeFilesIntoOverlay(ctx, dest, []SourceFile{
		{Path: src1.path, SyncID: src1.syncID},
		{Path: src2.path, SyncID: src2.syncID},
	}, destSyncID, t.TempDir())
	require.NoError(t, err, "MergeFilesIntoOverlay")

	grants := map[string]*v3.GrantRecord{}
	require.NoError(t, dest.IterateGrants(ctx, func(g *v3.GrantRecord) bool {
		grants[g.GetExternalId()] = g
		return true
	}))
	require.Equal(t, 3, len(grants), "merged grant count")
	require.Contains(t, grants, "shared-new", "shared grant from older source with newer discovered_at")
	require.NotContains(t, grants, "shared-old", "stale shared grant should be replaced")
	require.Contains(t, grants, "tie-old", "tie keeps first admission")
	require.NotContains(t, grants, "tie-new", "tie-new should be filtered")
	require.Equal(t, "carol", grants["only-src2"].GetPrincipal().GetResourceId(), "only-src2 grant principal (unseen key via ingested SST)")
	require.Equal(t, int64(3), stats.GetGrants(), "stats grants (replacement must not double count)")

	// Index correctness after replacement: the stale by_principal entry
	// for alice/"shared" must be gone, and bob's must exist; carol's
	// SST-ingested index entry must be present.
	byPrincipal := map[string][]string{}
	for _, principal := range []string{"alice", "bob", "carol"} {
		require.NoError(t, dest.IterateGrantsByPrincipal(ctx, "user", principal, func(g *v3.GrantRecord) bool {
			byPrincipal[principal] = append(byPrincipal[principal], g.GetExternalId())
			return true
		}))
		sort.Strings(byPrincipal[principal])
	}
	require.Equal(t, fmtSprint([]string{"shared-new", "tie-old"}), fmtSprint(byPrincipal["alice"]), "by_principal[alice]")
	require.Empty(t, byPrincipal["bob"], "by_principal[bob]")
	require.Equal(t, fmtSprint([]string{"only-src2"}), fmtSprint(byPrincipal["carol"]), "by_principal[carol]")
}
