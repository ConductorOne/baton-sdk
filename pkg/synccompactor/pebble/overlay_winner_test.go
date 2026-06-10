package pebble

import (
	"context"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/segmentio/ksuid"

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
		{id: "shared", principalID: "alice", entitlement: "member", discovered: older},
		{id: "tie", principalID: "alice", entitlement: "member", discovered: tie},
	}, false)
	// sources[1]: older source with a strictly newer discovered_at for
	// "shared" and an equal one for "tie".
	src2 := writeKWaySource(t, ctx, filepath.Join(dir, "src2.c1z"), []kwayGrantSpec{
		{id: "shared", principalID: "bob", entitlement: "member", discovered: newer},
		{id: "tie", principalID: "bob", entitlement: "member", discovered: tie},
	}, false)

	dest, _ := newEngine(t, "overlay-winner-dest")
	destSyncID := ksuid.New().String()
	stats, err := MergeFilesIntoOverlay(ctx, dest, []SourceFile{
		{Path: src1.path, SyncID: src1.syncID},
		{Path: src2.path, SyncID: src2.syncID},
	}, destSyncID, t.TempDir())
	if err != nil {
		t.Fatalf("MergeFilesIntoOverlay: %v", err)
	}

	grants := map[string]*v3.GrantRecord{}
	if err := dest.IterateGrantsBySync(ctx, destSyncID, func(g *v3.GrantRecord) bool {
		grants[g.GetExternalId()] = g
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if len(grants) != 2 {
		t.Fatalf("merged grant count = %d, want 2", len(grants))
	}
	if got := grants["shared"].GetPrincipal().GetResourceId(); got != "bob" {
		t.Fatalf("shared grant principal = %q, want bob (older source, newer discovered_at)", got)
	}
	if got := grants["tie"].GetPrincipal().GetResourceId(); got != "alice" {
		t.Fatalf("tie grant principal = %q, want alice (first admission keeps ties)", got)
	}
	if got := stats.GetGrants(); got != 2 {
		t.Fatalf("stats grants = %d, want 2 (replacement must not double count)", got)
	}

	// Index correctness after replacement: the stale by_principal entry
	// for alice/"shared" must be gone, and bob's must exist.
	byPrincipal := map[string][]string{}
	for _, principal := range []string{"alice", "bob"} {
		if err := dest.IterateGrantsByPrincipal(ctx, destSyncID, "user", principal, func(g *v3.GrantRecord) bool {
			byPrincipal[principal] = append(byPrincipal[principal], g.GetExternalId())
			return true
		}); err != nil {
			t.Fatal(err)
		}
		sort.Strings(byPrincipal[principal])
	}
	if got, want := byPrincipal["alice"], []string{"tie"}; fmtSprint(got) != fmtSprint(want) {
		t.Fatalf("by_principal[alice] = %v, want %v (stale index entry after replacement)", got, want)
	}
	if got, want := byPrincipal["bob"], []string{"shared"}; fmtSprint(got) != fmtSprint(want) {
		t.Fatalf("by_principal[bob] = %v, want %v", got, want)
	}
}
