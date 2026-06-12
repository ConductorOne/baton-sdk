package pebble

import (
	"context"
	"testing"

	"github.com/cockroachdb/pebble/v2"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

func mkV2Ent(id string) *v2.Entitlement {
	return v2.Entitlement_builder{
		Id: id,
		Resource: v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: "app",
				Resource:     "github",
			}.Build(),
		}.Build(),
	}.Build()
}

// runSync starts a sync, writes the given entitlements + grants, and
// seals it (EndSync builds the per-entitlement tries). Returns the
// sync id.
func runSync(t *testing.T, a *Adapter, ents []*v2.Entitlement, grants []*v2.Grant) string {
	t.Helper()
	ctx := context.Background()
	syncID, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	if err != nil {
		t.Fatalf("StartNewSync: %v", err)
	}
	if len(ents) > 0 {
		if err := a.PutEntitlements(ctx, ents...); err != nil {
			t.Fatalf("PutEntitlements: %v", err)
		}
	}
	if len(grants) > 0 {
		if err := a.PutGrants(ctx, grants...); err != nil {
			t.Fatalf("PutGrants: %v", err)
		}
	}
	if err := a.EndSync(ctx); err != nil {
		t.Fatalf("EndSync: %v", err)
	}
	return syncID
}

// diffGrantIDs runs generateSyncDiff and returns the set of grant
// external_ids that landed in the diff sync.
func diffGrantIDs(t *testing.T, a *Adapter, baseSyncID, appliedSyncID string) map[string]bool {
	t.Helper()
	ctx := context.Background()
	diffID, err := generateSyncDiff(ctx, a, baseSyncID, appliedSyncID)
	if err != nil {
		t.Fatalf("generateSyncDiff: %v", err)
	}
	got := map[string]bool{}
	if err := a.engine.IterateGrantsBySync(ctx, diffID, func(r *v3.GrantRecord) bool {
		got[r.GetExternalId()] = true
		return true
	}); err != nil {
		t.Fatalf("IterateGrantsBySync(diff): %v", err)
	}
	return got
}

func requireDiffIDs(t *testing.T, got map[string]bool, want ...string) {
	t.Helper()
	wantSet := map[string]bool{}
	for _, id := range want {
		wantSet[id] = true
		if !got[id] {
			t.Errorf("diff missing expected grant %q", id)
		}
	}
	for id := range got {
		if !wantSet[id] {
			t.Errorf("diff contains unexpected grant %q", id)
		}
	}
}

// TestGenerateSyncDiffTrieMultiEntitlement exercises the trie-driven
// grants diff across the full semantic matrix in one pair of syncs:
// unchanged entitlements (pruned at the root), an addition inside an
// existing entitlement, an addition under a brand-new entitlement, a
// removal (must not be emitted), and a grant that MOVED to a different
// entitlement while keeping its external_id (dirty bucket on both
// entitlements, but per the additions-only-by-external_id contract it
// must not be emitted).
func TestGenerateSyncDiffTrieMultiEntitlement(t *testing.T) {
	a := newAdapter(t)

	base := runSync(t, a,
		[]*v2.Entitlement{mkV2Ent("ent-A"), mkV2Ent("ent-B"), mkV2Ent("ent-C")},
		[]*v2.Grant{
			mkV2Grant("g1", "ent-A", "user", "alice"),
			mkV2Grant("g2", "ent-A", "user", "bob"),
			mkV2Grant("g3", "ent-B", "user", "carol"),
			mkV2Grant("g4", "ent-B", "user", "dave"),
			mkV2Grant("g5", "ent-C", "user", "eve"),
			mkV2Grant("g6", "ent-A", "user", "frank"),
		})
	applied := runSync(t, a,
		[]*v2.Entitlement{mkV2Ent("ent-A"), mkV2Ent("ent-B"), mkV2Ent("ent-C"), mkV2Ent("ent-D")},
		[]*v2.Grant{
			mkV2Grant("g1", "ent-A", "user", "alice"), // unchanged
			mkV2Grant("g2", "ent-A", "user", "bob"),   // unchanged
			mkV2Grant("g3", "ent-B", "user", "carol"), // unchanged
			mkV2Grant("g4", "ent-B", "user", "dave"),  // unchanged
			// g5 removed (ent-C empty in applied) — removals not emitted.
			mkV2Grant("g6", "ent-B", "user", "frank"), // moved ent-A→ent-B, same external_id — NOT an addition
			mkV2Grant("g7", "ent-B", "user", "grace"), // addition in an existing entitlement
			mkV2Grant("g8", "ent-D", "user", "heidi"), // addition under a new entitlement
		})

	got := diffGrantIDs(t, a, base, applied)
	requireDiffIDs(t, got, "g7", "g8")
}

// TestGenerateSyncDiffOrphanGrantSkipped documents that a grant
// without entitlement/principal refs has no hash-index entry and is
// invisible to the trie-driven diff path. It is silently skipped.
// Normal grants in the same sync (g2) are still emitted correctly.
// TODO: restore detection via an O(1) coverage check (e.g. a stored
// grant count in the sync run record).
func TestGenerateSyncDiffOrphanGrantSkipped(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)

	base := runSync(t, a,
		[]*v2.Entitlement{mkV2Ent("ent-A")},
		[]*v2.Grant{mkV2Grant("g1", "ent-A", "user", "alice")})

	applied, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	if err != nil {
		t.Fatalf("StartNewSync: %v", err)
	}
	if err := a.PutEntitlements(ctx, mkV2Ent("ent-A")); err != nil {
		t.Fatalf("PutEntitlements: %v", err)
	}
	if err := a.PutGrants(ctx,
		mkV2Grant("g1", "ent-A", "user", "alice"),
		mkV2Grant("g2", "ent-A", "user", "bob"),
	); err != nil {
		t.Fatalf("PutGrants: %v", err)
	}
	orphan := v3.GrantRecord_builder{
		ExternalId: "g-orphan",
	}.Build()
	if err := a.engine.PutGrantRecord(ctx, orphan); err != nil {
		t.Fatalf("PutGrantRecord(orphan): %v", err)
	}
	if err := a.EndSync(ctx); err != nil {
		t.Fatalf("EndSync: %v", err)
	}

	got := diffGrantIDs(t, a, base, applied)
	// g-orphan has no hash-index entry — silently skipped by trie diff.
	requireDiffIDs(t, got, "g2")
}

// TestGenerateSyncDiffTrieWithoutTrees simulates a file whose tries
// are missing (e.g. written before they existed and not yet
// backfilled): the hash index is intact, so the trie path runs, and
// DirtyEntitlementBuckets degrades to the on-demand index fold per
// entitlement. The diff must still be exact.
func TestGenerateSyncDiffTrieWithoutTrees(t *testing.T) {
	a := newAdapter(t)

	base := runSync(t, a,
		[]*v2.Entitlement{mkV2Ent("ent-A"), mkV2Ent("ent-B")},
		[]*v2.Grant{
			mkV2Grant("g1", "ent-A", "user", "alice"),
			mkV2Grant("g2", "ent-B", "user", "bob"),
		})
	applied := runSync(t, a,
		[]*v2.Entitlement{mkV2Ent("ent-A"), mkV2Ent("ent-B")},
		[]*v2.Grant{
			mkV2Grant("g1", "ent-A", "user", "alice"),
			mkV2Grant("g2", "ent-B", "user", "bob"),
			mkV2Grant("g3", "ent-B", "user", "carol"),
		})

	for _, sid := range []string{base, applied} {
		idBytes, err := a.engine.resolveSyncBytes(sid)
		if err != nil {
			t.Fatal(err)
		}
		if err := a.engine.db.DeleteRange(DigestSyncLowerBound(idBytes), DigestSyncUpperBound(idBytes), pebble.Sync); err != nil {
			t.Fatalf("DeleteRange(digest %s): %v", sid, err)
		}
	}

	got := diffGrantIDs(t, a, base, applied)
	requireDiffIDs(t, got, "g3")
}
