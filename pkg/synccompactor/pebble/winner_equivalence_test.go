package pebble

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	enginepkg "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
)

// recordSet is one sync's worth of primary records, used to build a
// merge source either as an on-disk c1z (kway/overlay take SourceFiles)
// or as a live engine (fold's MergeInto takes SourceSyncs).
type recordSet struct {
	rts []*v3.ResourceTypeRecord
	rs  []*v3.ResourceRecord
	es  []*v3.EntitlementRecord
	gs  []*v3.GrantRecord
}

func winRT(externalID, displayName string, at time.Time) *v3.ResourceTypeRecord {
	return v3.ResourceTypeRecord_builder{
		ExternalId:   externalID,
		DisplayName:  displayName,
		DiscoveredAt: timestamppb.New(at),
	}.Build()
}

func winRes(rtID, rID, displayName string, at time.Time) *v3.ResourceRecord {
	return v3.ResourceRecord_builder{
		ResourceTypeId: rtID,
		ResourceId:     rID,
		DisplayName:    displayName,
		DiscoveredAt:   timestamppb.New(at),
	}.Build()
}

func winEnt(externalID, displayName string, at time.Time) *v3.EntitlementRecord {
	return v3.EntitlementRecord_builder{
		ExternalId:   externalID,
		Resource:     v3.ResourceRef_builder{ResourceTypeId: "user", ResourceId: "u1"}.Build(),
		DisplayName:  displayName,
		DiscoveredAt: timestamppb.New(at),
	}.Build()
}

// winGrant tags the winning source in Principal.ResourceId so the merge
// result names which version survived.
func winGrant(externalID, principal string, at time.Time) *v3.GrantRecord {
	return v3.GrantRecord_builder{
		ExternalId: externalID,
		Entitlement: v3.EntitlementRef_builder{
			ResourceTypeId: "app", ResourceId: "github", EntitlementId: externalID,
		}.Build(),
		Principal:    v3.PrincipalRef_builder{ResourceTypeId: "user", ResourceId: principal}.Build(),
		DiscoveredAt: timestamppb.New(at),
	}.Build()
}

func populateEngine(t *testing.T, ctx context.Context, eng *enginepkg.Engine, rs recordSet) {
	t.Helper()
	if len(rs.rts) > 0 {
		require.NoError(t, eng.PutResourceTypeRecords(ctx, rs.rts...))
	}
	if len(rs.rs) > 0 {
		require.NoError(t, eng.PutResourceRecords(ctx, rs.rs...))
	}
	if len(rs.es) > 0 {
		require.NoError(t, eng.PutEntitlementRecords(ctx, rs.es...))
	}
	if len(rs.gs) > 0 {
		require.NoError(t, eng.PutGrantRecords(ctx, rs.gs...))
	}
}

// buildC1ZSource writes rs to a fresh Pebble c1z and returns its path +
// sync id, for the file-based strategies (kway, overlay).
func buildC1ZSource(t *testing.T, ctx context.Context, path string, rs recordSet) SourceFile {
	t.Helper()
	w, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(c1zstore.EnginePebble), dotc1z.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	eng, ok := enginepkg.AsEngine(w)
	require.True(t, ok)
	syncID, err := w.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	populateEngine(t, ctx, eng, rs)
	require.NoError(t, w.EndSync(ctx))
	require.NoError(t, w.Close(ctx))
	return SourceFile{Path: path, SyncID: syncID}
}

// buildEngineSource builds a standalone engine holding rs under a fresh
// sync id, for fold's MergeInto (which takes live engines).
func buildEngineSource(t *testing.T, ctx context.Context, name string, rs recordSet) SourceSync {
	t.Helper()
	eng, _ := newEngine(t, name)
	syncID := ksuid.New().String()
	require.NoError(t, eng.SetCurrentSync(syncID))
	populateEngine(t, ctx, eng, rs)
	return SourceSync{Engine: eng, SyncID: syncID}
}

// winners is the observed survivor identity for every probed key, keyed
// so the three strategies can be compared field-for-field.
type winners struct {
	resourceTypes map[string]string // externalID -> display name
	resources     map[string]string // resourceID -> display name
	entitlements  map[string]string // externalID -> display name
	grants        map[string]string // externalID -> principal id
}

func collectWinners(t *testing.T, ctx context.Context, eng *enginepkg.Engine) winners {
	t.Helper()
	w := winners{
		resourceTypes: map[string]string{},
		resources:     map[string]string{},
		entitlements:  map[string]string{},
		grants:        map[string]string{},
	}
	require.NoError(t, eng.IterateResourceTypes(ctx, func(r *v3.ResourceTypeRecord) bool {
		w.resourceTypes[r.GetExternalId()] = r.GetDisplayName()
		return true
	}))
	require.NoError(t, eng.IterateResources(ctx, func(r *v3.ResourceRecord) bool {
		w.resources[r.GetResourceId()] = r.GetDisplayName()
		return true
	}))
	require.NoError(t, eng.IterateEntitlements(ctx, func(e *v3.EntitlementRecord) bool {
		w.entitlements[e.GetExternalId()] = e.GetDisplayName()
		return true
	}))
	require.NoError(t, eng.IterateGrants(ctx, func(g *v3.GrantRecord) bool {
		w.grants[g.GetExternalId()] = g.GetPrincipal().GetResourceId()
		return true
	}))
	return w
}

// TestPebbleStrategiesAgreeOnWinners is the cross-strategy correctness
// pin: the same overlapping inputs, fed through all three Pebble merge
// strategies (k-way, overlay, in-place fold), must converge on the
// identical winner set — the record with the strictly-newest
// discovered_at per logical key — for EVERY primary record type, and
// must leave byte-identical derived index keyspaces.
//
// The scenario is deliberately adversarial on two axes the per-strategy
// tests don't jointly cover:
//
//   - Winner position: the survivor lives in the newest-applied source
//     (g-A), a middle source (g-B), AND the OLDEST-applied source
//     (g-C) — a newer discovered_at carried by an older sync must win
//     regardless of scan/application order. A strategy that shortcuts
//     to "first writer wins" or "last writer wins" diverges here.
//   - Record type: resource_types, resources, entitlements, and grants
//     each carry discovered_at in a different proto field
//     (discoveredAtFieldNumber: 6 / 8 / 8 / 5). A transposed field
//     number silently breaks newest-wins for one type while the others
//     pass — caught only by probing every type.
//
// Distinct timestamps everywhere means there are no ties, so all three
// strategies must agree unconditionally; the tie-break conventions
// (which legitimately differ) are pinned by the per-strategy tests.
func TestPebbleStrategiesAgreeOnWinners(t *testing.T) {
	ctx := context.Background()

	t1 := time.Unix(1000, 0).UTC()
	t2 := time.Unix(2000, 0).UTC()
	t3 := time.Unix(3000, 0).UTC()
	t4 := time.Unix(4000, 0).UTC()

	// Three sources, passed newest-first (s0, s1, s2) — the convention
	// the compactor uses so ties resolve to the newest sync. The
	// discovered_at values deliberately do NOT track that order.
	srcSets := []recordSet{
		{ // s0 (newest-applied)
			rts: []*v3.ResourceTypeRecord{winRT("rt-1", "rt-from-s0", t1)},
			rs:  []*v3.ResourceRecord{winRes("user", "u1", "res-from-s0", t3)},
			gs: []*v3.GrantRecord{
				winGrant("g-A", "s0", t4), // wins: newest, in newest source
				winGrant("g-B", "s0", t1),
				winGrant("g-C", "s0", t1),
			},
		},
		{ // s1 (middle)
			rs: []*v3.ResourceRecord{winRes("user", "u1", "res-from-s1", t1)},
			es: []*v3.EntitlementRecord{winEnt("e-1", "ent-from-s1", t1)},
			gs: []*v3.GrantRecord{
				winGrant("g-A", "s1", t2),
				winGrant("g-B", "s1", t4), // wins: newest, in middle source
				winGrant("g-C", "s1", t2),
			},
		},
		{ // s2 (oldest-applied) — carries the newest data for several keys
			rts: []*v3.ResourceTypeRecord{winRT("rt-1", "rt-from-s2", t3)},
			es:  []*v3.EntitlementRecord{winEnt("e-1", "ent-from-s2", t3)},
			gs: []*v3.GrantRecord{
				winGrant("g-A", "s2", t3),
				winGrant("g-B", "s2", t2),
				winGrant("g-C", "s2", t4), // wins: newest, in OLDEST source
			},
		},
	}

	want := winners{
		resourceTypes: map[string]string{"rt-1": "rt-from-s2"}, // t3 (s2) > t1 (s0)
		resources:     map[string]string{"u1": "res-from-s0"},  // t3 (s0) > t1 (s1)
		entitlements:  map[string]string{"e-1": "ent-from-s2"}, // t3 (s2) > t1 (s1)
		grants: map[string]string{
			"g-A": "s2", // principal is part of identity; retained external_id map observes the last iterated variant
			"g-B": "s2", // principal is part of identity; retained external_id map observes the last iterated variant
			"g-C": "s2", // t4 in oldest source
		},
	}

	// Build the file-backed sources once; kway and overlay both read
	// them, fold gets independent live engines from the same sets.
	dir := t.TempDir()
	files := make([]SourceFile, len(srcSets))
	for i, rs := range srcSets {
		files[i] = buildC1ZSource(t, ctx, fmt.Sprintf("%s/src%d.c1z", dir, i), rs)
	}

	runStrategy := func(t *testing.T, merge func(dest *enginepkg.Engine, destSyncID string) error) winners {
		t.Helper()
		dest, _ := newEngine(t, "dest")
		destSyncID := ksuid.New().String()
		require.NoError(t, merge(dest, destSyncID))
		assertIndexesMatchDerived(t, ctx, dest)
		return collectWinners(t, ctx, dest)
	}

	t.Run("kway", func(t *testing.T) {
		got := runStrategy(t, func(dest *enginepkg.Engine, destSyncID string) error {
			// fanIn=2 forces a multi-round recursive merge over the 3
			// sources, exercising run-file round-tripping rather than a
			// single direct pass.
			_, err := MergeFilesInto(ctx, dest, files, destSyncID, t.TempDir(), WithFanIn(2))
			return err
		})
		require.Equal(t, want, got)
	})

	t.Run("overlay", func(t *testing.T) {
		got := runStrategy(t, func(dest *enginepkg.Engine, destSyncID string) error {
			_, err := MergeFilesIntoOverlay(ctx, dest, files, destSyncID, t.TempDir())
			return err
		})
		require.Equal(t, want, got)
	})

	t.Run("fold", func(t *testing.T) {
		got := runStrategy(t, func(dest *enginepkg.Engine, destSyncID string) error {
			sources := make([]SourceSync, len(srcSets))
			for i, rs := range srcSets {
				sources[i] = buildEngineSource(t, ctx, fmt.Sprintf("fold-src%d", i), rs)
			}
			_, err := MergeInto(ctx, dest, sources, destSyncID)
			return err
		})
		require.Equal(t, want, got)
	})
}
