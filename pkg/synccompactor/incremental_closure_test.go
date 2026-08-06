package synccompactor

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	sdksync "github.com/conductorone/baton-sdk/pkg/sync"
	"github.com/conductorone/baton-sdk/pkg/sync/expand"
	batonGrant "github.com/conductorone/baton-sdk/pkg/types/grant"
)

func TestConcurrentDuplicateIncrementalCompactions(t *testing.T) {
	ctx := context.Background()
	entries := buildIncrementalFixtures(t, ctx, t.TempDir())
	type result struct {
		grants []string
		err    error
	}
	results := make(chan result, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			compactor, cleanup, err := NewCompactor(ctx, t.TempDir(), entries,
				WithTmpDir(t.TempDir()), WithEngine(c1zstore.EnginePebble), WithIncrementalExpansion())
			if err != nil {
				results <- result{err: err}
				return
			}
			defer cleanup() //nolint:errcheck // the result below verifies the artifact.
			out, err := compactor.Compact(ctx)
			if err != nil {
				results <- result{err: err}
				return
			}
			if !compactor.incrementalExpansionRan {
				results <- result{err: errors.New("incremental expansion did not run")}
				return
			}
			results <- result{grants: grantOutcome(t, ctx, out.FilePath, out.SyncID)}
		}()
	}
	wg.Wait()
	close(results)
	var outcomes [][]string
	for got := range results {
		require.NoError(t, got.err)
		outcomes = append(outcomes, got.grants)
	}
	require.Len(t, outcomes, 2)
	require.Equal(t, outcomes[0], outcomes[1])
}

// The compactor has no session-store input. This pins the production default:
// incremental graph reuse must work when connector sessions are disabled.
func TestIncrementalCompactionWithNoSessionStore(t *testing.T) {
	ctx := context.Background()
	entries := buildIncrementalFixtures(t, ctx, t.TempDir())
	compactor, cleanup, err := NewCompactor(ctx, t.TempDir(), entries,
		WithTmpDir(t.TempDir()), WithEngine(c1zstore.EnginePebble), WithIncrementalExpansion())
	require.NoError(t, err)
	defer func() { require.NoError(t, cleanup()) }()
	out, err := compactor.Compact(ctx)
	require.NoError(t, err)
	require.True(t, compactor.incrementalExpansionRan)
	assertSealedCompactionArtifact(t, ctx, out, true)
}

func TestCompactorIncrementalResourceTypeFilterChanges(t *testing.T) {
	ctx := context.Background()
	t.Run("widen", func(t *testing.T) {
		incEntries := buildFilterChangeFixtures(t, ctx, t.TempDir(), []string{"user"}, nil)
		inc, cleanupInc, err := NewCompactor(ctx, t.TempDir(), incEntries,
			WithTmpDir(t.TempDir()), WithEngine(c1zstore.EnginePebble), WithIncrementalExpansion())
		require.NoError(t, err)
		defer func() { require.NoError(t, cleanupInc()) }()
		incOut, err := inc.Compact(ctx)
		require.NoError(t, err)
		require.True(t, inc.incrementalExpansionRan)

		fullEntries := buildFilterChangeFixtures(t, ctx, t.TempDir(), []string{"user"}, nil)
		full, cleanupFull, err := NewCompactor(ctx, t.TempDir(), fullEntries,
			WithTmpDir(t.TempDir()), WithEngine(c1zstore.EnginePebble))
		require.NoError(t, err)
		defer func() { require.NoError(t, cleanupFull()) }()
		fullOut, err := full.Compact(ctx)
		require.NoError(t, err)
		require.Equal(t, grantOutcome(t, ctx, fullOut.FilePath, fullOut.SyncID),
			grantOutcome(t, ctx, incOut.FilePath, incOut.SyncID))
		hasGrant(t, grantOutcome(t, ctx, incOut.FilePath, incOut.SyncID), "ent-c|group|nested")
	})

	t.Run("narrow", func(t *testing.T) {
		entries := buildFilterChangeFixtures(t, ctx, t.TempDir(), nil, []string{"user"})
		compactor, cleanup, err := NewCompactor(ctx, t.TempDir(), entries,
			WithTmpDir(t.TempDir()), WithEngine(c1zstore.EnginePebble), WithIncrementalExpansion())
		require.NoError(t, err)
		defer func() { require.NoError(t, cleanup()) }()
		_, err = compactor.Compact(ctx)
		require.NoError(t, err)
		require.False(t, compactor.incrementalExpansionRan, "filter narrowing must decline")
	})
}

func buildFilterChangeFixtures(
	t testing.TB,
	ctx context.Context,
	dir string,
	baseFilter, currentFilter []string,
) []*CompactableSync {
	t.Helper()
	groupB, groupC, nested := grp("grpB"), grp("grpC"), grp("nested")
	alice := usr("alice")
	entB, entC := ent("ent-b", groupB), ent("ent-c", groupC)
	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()
	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()

	basePath := filepath.Join(dir, "base.c1z")
	base, err := dotc1z.NewStore(ctx, basePath, dotc1z.WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	baseSyncID, err := base.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, base.PutResourceTypes(ctx, userRT, groupRT))
	require.NoError(t, base.PutResources(ctx, groupB, groupC, nested, alice))
	require.NoError(t, base.PutEntitlements(ctx, entB, entC))
	graph := expand.NewEntitlementGraph(ctx)
	graph.AddEntitlementID("ent-b")
	graph.AddEntitlementID("ent-c")
	require.NoError(t, graph.AddEdge(ctx, "ent-b", "ent-c", false, baseFilter))
	require.NoError(t, base.PutGrants(ctx,
		memberGrant(entB, alice),
		memberGrant(entB, nested),
		ruleGrantFilter(entC, groupB, "ent-b", baseFilter),
	))
	require.NoError(t, graph.FixCycles(ctx))
	require.NoError(t, expand.NewExpander(sdksync.NewExpanderStore(base), graph).Run(ctx))
	graph.Loaded = true
	graph.MarkExpansionComplete()
	require.NoError(t, base.EndSync(ctx))
	persistFixtureGraph(t, ctx, base, baseSyncID, graph)
	require.NoError(t, base.Close(ctx))

	incPath := filepath.Join(dir, "inc.c1z")
	inc, err := dotc1z.NewStore(ctx, incPath, dotc1z.WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	incSyncID, err := inc.StartNewSync(ctx, connectorstore.SyncTypePartial, "")
	require.NoError(t, err)
	require.NoError(t, inc.PutResourceTypes(ctx, userRT, groupRT))
	require.NoError(t, inc.PutResources(ctx, groupB, groupC))
	require.NoError(t, inc.PutEntitlements(ctx, entB, entC))
	require.NoError(t, inc.PutGrants(ctx, ruleGrantFilter(entC, groupB, "ent-b", currentFilter)))
	require.NoError(t, inc.EndSync(ctx))
	require.NoError(t, inc.Close(ctx))
	return []*CompactableSync{{FilePath: basePath, SyncID: baseSyncID}, {FilePath: incPath, SyncID: incSyncID}}
}

func ruleGrantFilter(dest *v2.Entitlement, source *v2.Resource, sourceEntitlementID string, filter []string) *v2.Grant {
	grant := v2.Grant_builder{
		Id: batonGrant.NewGrantID(source, dest), Entitlement: dest, Principal: source,
	}.Build()
	grant.SetAnnotations(annotations.New(v2.GrantExpandable_builder{
		EntitlementIds: []string{sourceEntitlementID}, ResourceTypeIds: filter,
	}.Build()))
	return grant
}

func TestCompactorIncrementalKWayParity(t *testing.T) {
	ctx := context.Background()
	incEntries := buildNewMemberFixtures(t, ctx, t.TempDir())
	inc, cleanupInc, err := NewCompactor(ctx, t.TempDir(), incEntries,
		WithTmpDir(t.TempDir()), WithEngine(c1zstore.EnginePebble),
		WithPebbleCompactorMode(PebbleCompactorModeKWay), WithIncrementalExpansion())
	require.NoError(t, err)
	defer func() { require.NoError(t, cleanupInc()) }()
	incOut, err := inc.Compact(ctx)
	require.NoError(t, err)
	require.True(t, inc.incrementalExpansionRan)

	fullEntries := buildNewMemberFixtures(t, ctx, t.TempDir())
	full, cleanupFull, err := NewCompactor(ctx, t.TempDir(), fullEntries,
		WithTmpDir(t.TempDir()), WithEngine(c1zstore.EnginePebble),
		WithPebbleCompactorMode(PebbleCompactorModeKWay))
	require.NoError(t, err)
	defer func() { require.NoError(t, cleanupFull()) }()
	fullOut, err := full.Compact(ctx)
	require.NoError(t, err)
	require.Equal(t, grantOutcome(t, ctx, fullOut.FilePath, fullOut.SyncID),
		grantOutcome(t, ctx, incOut.FilePath, incOut.SyncID))
}

func TestCompactorIncrementalThreeGenerationChain(t *testing.T) {
	ctx := context.Background()
	initial := buildIncrementalFixtures(t, ctx, t.TempDir())
	allInputs := append([]*CompactableSync(nil), initial...)
	currentEntries := initial
	var last *CompactableSync
	for generation, user := range []string{"zoe", "yuki", "xavier"} {
		if generation > 0 {
			partial := buildMemberPartial(t, ctx, filepath.Join(t.TempDir(), user+".c1z"), "ent-b", "grpB", user)
			allInputs = append(allInputs, partial)
			currentEntries = []*CompactableSync{last, partial}
		}
		compactor, cleanup, err := NewCompactor(ctx, t.TempDir(), currentEntries,
			WithTmpDir(t.TempDir()), WithEngine(c1zstore.EnginePebble), WithIncrementalExpansion())
		require.NoError(t, err)
		out, err := compactor.Compact(ctx)
		require.NoError(t, err)
		require.True(t, compactor.incrementalExpansionRan)
		require.NoError(t, cleanup())
		last = out
	}

	full, cleanupFull, err := NewCompactor(ctx, t.TempDir(), allInputs,
		WithTmpDir(t.TempDir()), WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	defer func() { require.NoError(t, cleanupFull()) }()
	fullOut, err := full.Compact(ctx)
	require.NoError(t, err)
	require.Equal(t, grantOutcome(t, ctx, fullOut.FilePath, fullOut.SyncID),
		grantOutcome(t, ctx, last.FilePath, last.SyncID))
	assertSealedCompactionArtifact(t, ctx, last, true)
}

// A partial can replace an existing grant record even though the format has no
// tombstones. This test distinguishes that case from a grant merely being
// absent from a partial sync.
func TestCompactorIncrementalDirectToIndirectReplacement(t *testing.T) {
	ctx := context.Background()
	build := func(t *testing.T, dir string) []*CompactableSync {
		groupA, groupB := grp("group-a"), grp("group-b")
		alice := usr("alice")
		entA, entB := ent("ent-a", groupA), ent("ent-b", groupB)
		userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()
		groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()

		basePath := filepath.Join(dir, "base.c1z")
		base, err := dotc1z.NewStore(ctx, basePath, dotc1z.WithEngine(c1zstore.EnginePebble))
		require.NoError(t, err)
		baseSyncID, err := base.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)
		require.NoError(t, base.PutResourceTypes(ctx, userRT, groupRT))
		require.NoError(t, base.PutResources(ctx, groupA, groupB, alice))
		require.NoError(t, base.PutEntitlements(ctx, entA, entB))
		require.NoError(t, base.PutGrants(ctx,
			memberGrant(entA, alice),
			expandedGrant(entB, alice, entA.GetId()),
			ruleGrantSpec(entB, groupA, entA.GetId(), true),
		))
		require.NoError(t, base.EndSync(ctx))
		graph := expand.NewEntitlementGraph(ctx)
		graph.AddEntitlementID(entA.GetId())
		graph.AddEntitlementID(entB.GetId())
		require.NoError(t, graph.AddEdge(ctx, entA.GetId(), entB.GetId(), true, nil))
		graph.MarkEdgeExpanded(entA.GetId(), entB.GetId())
		graph.Loaded = true
		graph.HasNoCycles = true
		persistFixtureGraph(t, ctx, base, baseSyncID, graph)
		require.NoError(t, base.Close(ctx))

		partialPath := filepath.Join(dir, "partial.c1z")
		partial, err := dotc1z.NewStore(ctx, partialPath, dotc1z.WithEngine(c1zstore.EnginePebble))
		require.NoError(t, err)
		partialSyncID, err := partial.StartNewSync(ctx, connectorstore.SyncTypePartial, "")
		require.NoError(t, err)
		require.NoError(t, partial.PutResourceTypes(ctx, userRT, groupRT))
		require.NoError(t, partial.PutResources(ctx, groupA, alice))
		require.NoError(t, partial.PutEntitlements(ctx, entA))
		replacement := memberGrant(entA, alice)
		replacement.SetSources(v2.GrantSources_builder{
			Sources: map[string]*v2.GrantSources_GrantSource{"some-other-source": {}},
		}.Build())
		require.NoError(t, partial.PutGrants(ctx, replacement))
		require.NoError(t, partial.EndSync(ctx))
		require.NoError(t, partial.Close(ctx))

		return []*CompactableSync{
			{FilePath: basePath, SyncID: baseSyncID},
			{FilePath: partialPath, SyncID: partialSyncID},
		}
	}

	incEntries := build(t, t.TempDir())
	inc, incCleanup, err := NewCompactor(ctx, t.TempDir(), incEntries,
		WithTmpDir(t.TempDir()), WithEngine(c1zstore.EnginePebble), WithIncrementalExpansion())
	require.NoError(t, err)
	defer func() { require.NoError(t, incCleanup()) }()
	incOut, err := inc.Compact(ctx)
	require.NoError(t, err)
	require.True(t, inc.incrementalExpansionRan, "the replacement currently remains eligible for incremental expansion")

	fullEntries := build(t, t.TempDir())
	full, fullCleanup, err := NewCompactor(ctx, t.TempDir(), fullEntries,
		WithTmpDir(t.TempDir()), WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	defer func() { require.NoError(t, fullCleanup()) }()
	fullOut, err := full.Compact(ctx)
	require.NoError(t, err)

	incGrants := grantOutcome(t, ctx, incOut.FilePath, incOut.SyncID)
	fullGrants := grantOutcome(t, ctx, fullOut.FilePath, fullOut.SyncID)
	require.Equal(t,
		fullGrants,
		incGrants,
		"incremental replacement handling must equal full expansion",
	)
}

func TestCompactorIncrementalRejectsGraphIncoherentWithBaseGrants(t *testing.T) {
	ctx := context.Background()
	build := func(t *testing.T, dir string) []*CompactableSync {
		groupA, groupB := grp("group-a"), grp("group-b")
		alice := usr("alice")
		entA, entB := ent("ent-a", groupA), ent("ent-b", groupB)
		userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()
		groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()

		basePath := filepath.Join(dir, "base.c1z")
		base, err := dotc1z.NewStore(ctx, basePath, dotc1z.WithEngine(c1zstore.EnginePebble))
		require.NoError(t, err)
		baseSyncID, err := base.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)
		require.NoError(t, base.PutResourceTypes(ctx, userRT, groupRT))
		require.NoError(t, base.PutResources(ctx, groupA, groupB, alice))
		require.NoError(t, base.PutEntitlements(ctx, entA, entB))
		// The sidecar claims A -> B was expanded, but Alice's derived B grant
		// is deliberately absent.
		require.NoError(t, base.PutGrants(ctx,
			memberGrant(entA, alice),
			ruleGrant(entB, groupA, entA.GetId()),
		))
		require.NoError(t, base.EndSync(ctx))
		graph := expand.NewEntitlementGraph(ctx)
		graph.AddEntitlementID(entA.GetId())
		graph.AddEntitlementID(entB.GetId())
		require.NoError(t, graph.AddEdge(ctx, entA.GetId(), entB.GetId(), false, nil))
		graph.MarkEdgeExpanded(entA.GetId(), entB.GetId())
		graph.Loaded = true
		graph.HasNoCycles = true
		digestReader, ok := base.(c1zstore.GrantGenerationDigestReader)
		require.True(t, ok)
		digest, found, err := digestReader.GrantGenerationDigest(ctx)
		require.NoError(t, err)
		require.True(t, found)
		// Simulate a graph copied from a different grant generation while
		// retaining a valid structure, sync ID, and verification marker.
		digest.Hash[0] ^= 0xff
		data, err := expand.MarshalGraphBlobWithGrantDigest(baseSyncID, graph, digest)
		require.NoError(t, err)
		graphStore, ok := base.(sdksync.EntitlementGraphStore)
		require.True(t, ok)
		require.NoError(t, graphStore.PutEntitlementGraphBlob(ctx, data))
		verificationWriter, ok := base.SyncMeta().(c1zstore.IngestInvariantVerificationWriter)
		require.True(t, ok)
		require.NoError(t, verificationWriter.MarkIngestInvariantsVerified(ctx, baseSyncID, c1zstore.IngestInvariantVerification{
			Generation: sdksync.IngestInvariantGeneration,
			Coverage:   []string{"test-fixture"},
			Mode:       c1zstore.IngestInvariantVerificationModeConnector,
		}))
		require.NoError(t, base.Close(ctx))

		partial := buildEmptyPartial(t, ctx, filepath.Join(dir, "partial.c1z"))
		return []*CompactableSync{
			{FilePath: basePath, SyncID: baseSyncID},
			partial,
		}
	}

	entries := build(t, t.TempDir())
	compactor, cleanup, err := NewCompactor(ctx, t.TempDir(), entries,
		WithTmpDir(t.TempDir()), WithEngine(c1zstore.EnginePebble), WithIncrementalExpansion())
	require.NoError(t, err)
	defer func() { require.NoError(t, cleanup()) }()
	out, err := compactor.Compact(ctx)
	require.NoError(t, err)
	require.False(t, compactor.incrementalExpansionRan,
		"a sidecar that is not coherent with its base grants must fall back to full expansion")
	hasGrant(t, grantOutcome(t, ctx, out.FilePath, out.SyncID), "ent-b|user|alice")
}

func TestCompactorGraphCompatibilityHealing(t *testing.T) {
	ctx := context.Background()
	t.Run("old pebble without sidecar", func(t *testing.T) {
		entries := buildIncrementalFixtures(t, ctx, t.TempDir())
		store, err := dotc1z.NewStore(ctx, entries[0].FilePath, dotc1z.WithTmpDir(t.TempDir()))
		require.NoError(t, err)
		graphStore, ok := store.(interface{ DeleteEntitlementGraphBlob(context.Context) error })
		require.True(t, ok)
		require.NoError(t, graphStore.DeleteEntitlementGraphBlob(ctx))
		require.NoError(t, store.Close(ctx))
		assertFallbackHealsAndReuses(t, ctx, entries)
	})

	t.Run("sqlite converted to pebble", func(t *testing.T) {
		entries := buildIncrementalFixturesEngine(t, ctx, t.TempDir(), c1zstore.EngineSQLite)
		assertFallbackHealsAndReuses(t, ctx, entries)
	})
}

func assertFallbackHealsAndReuses(t *testing.T, ctx context.Context, entries []*CompactableSync) {
	t.Helper()
	first, cleanupFirst, err := NewCompactor(ctx, t.TempDir(), entries,
		WithTmpDir(t.TempDir()), WithEngine(c1zstore.EnginePebble), WithIncrementalExpansion())
	require.NoError(t, err)
	firstOut, err := first.Compact(ctx)
	require.NoError(t, err)
	require.False(t, first.incrementalExpansionRan)
	require.NoError(t, cleanupFirst())
	assertSealedCompactionArtifact(t, ctx, firstOut, true)

	partial := buildMemberPartial(t, ctx, filepath.Join(t.TempDir(), "next.c1z"), "ent-b", "grpB", "next-user")
	second, cleanupSecond, err := NewCompactor(ctx, t.TempDir(), []*CompactableSync{firstOut, partial},
		WithTmpDir(t.TempDir()), WithEngine(c1zstore.EnginePebble), WithIncrementalExpansion())
	require.NoError(t, err)
	defer func() { require.NoError(t, cleanupSecond()) }()
	secondOut, err := second.Compact(ctx)
	require.NoError(t, err)
	require.True(t, second.incrementalExpansionRan)
	assertSealedCompactionArtifact(t, ctx, secondOut, true)
}
