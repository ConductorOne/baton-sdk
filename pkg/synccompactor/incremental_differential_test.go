package synccompactor

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	sdksync "github.com/conductorone/baton-sdk/pkg/sync"
	"github.com/conductorone/baton-sdk/pkg/sync/expand"
)

// TestCompactorIncrementalDifferentialRandom is Stage 2b's real-artifact
// oracle. For each deterministic seed it writes equivalent base+partial
// Pebble c1z inputs, compacts one through incremental expansion and one through
// full expansion, and compares complete grant rows (including provenance).
func TestCompactorIncrementalDifferentialRandom(t *testing.T) {
	const cases = 10
	incrementalRuns := 0
	for seed := int64(0); seed < cases; seed++ {
		seed := seed
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			ctx := context.Background()

			incEntries := buildRandomDifferentialFixtures(t, ctx, t.TempDir(), seed)
			incCompactor, incCleanup, err := NewCompactor(ctx, t.TempDir(), incEntries,
				WithTmpDir(t.TempDir()),
				WithEngine(c1zstore.EnginePebble),
				WithIncrementalExpansion(),
			)
			require.NoError(t, err)
			defer func() { require.NoError(t, incCleanup()) }()
			incOut, err := incCompactor.Compact(ctx)
			require.NoError(t, err)
			require.NotNil(t, incOut)
			require.True(t, incCompactor.incrementalExpansionRan,
				"seed=%d silently fell back instead of exercising Stage 2b", seed)
			incrementalRuns++

			fullEntries := buildRandomDifferentialFixtures(t, ctx, t.TempDir(), seed)
			fullCompactor, fullCleanup, err := NewCompactor(ctx, t.TempDir(), fullEntries,
				WithTmpDir(t.TempDir()),
				WithEngine(c1zstore.EnginePebble),
			)
			require.NoError(t, err)
			defer func() { require.NoError(t, fullCleanup()) }()
			fullOut, err := fullCompactor.Compact(ctx)
			require.NoError(t, err)
			require.NotNil(t, fullOut)

			require.Equal(t,
				grantOutcome(t, ctx, fullOut.FilePath, fullOut.SyncID),
				grantOutcome(t, ctx, incOut.FilePath, incOut.SyncID),
				"seed=%d incremental grants/provenance differ from full expansion", seed,
			)
			assertSealedCompactionArtifact(t, ctx, incOut, true)
			assertSealedCompactionArtifact(t, ctx, fullOut, false)
		})
	}
	require.Equal(t, cases, incrementalRuns, "every additive case must use the incremental path")
}

func buildRandomDifferentialFixtures(
	t *testing.T,
	ctx context.Context,
	dir string,
	seed int64,
) []*CompactableSync {
	t.Helper()
	rng := rand.New(rand.NewSource(seed)) //nolint:gosec // deterministic test fixture
	nodeCount := 4 + rng.Intn(4)

	groups := make([]*v2.Resource, nodeCount)
	entitlements := make([]*v2.Entitlement, nodeCount)
	for i := 0; i < nodeCount; i++ {
		groups[i] = grp(fmt.Sprintf("seed-%d-group-%d", seed, i))
		entitlements[i] = ent(fmt.Sprintf("seed-%d-ent-%d", seed, i), groups[i])
	}

	type edge struct{ source, destination int }
	edges := make([]edge, 0, nodeCount*2)
	edgeSet := make(map[[2]int]struct{})
	addEdge := func(source, destination int) {
		key := [2]int{source, destination}
		if _, exists := edgeSet[key]; exists {
			return
		}
		edgeSet[key] = struct{}{}
		edges = append(edges, edge{source: source, destination: destination})
	}
	// A spine guarantees useful transitive expansion. Extra forward edges add
	// diamonds and multi-parent provenance while keeping the graph acyclic.
	for i := 0; i+1 < nodeCount; i++ {
		addEdge(i, i+1)
	}
	for i := 0; i < nodeCount; i++ {
		for j := i + 2; j < nodeCount; j++ {
			if rng.Intn(3) == 0 {
				addEdge(i, j)
			}
		}
	}

	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()
	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	users := make([]*v2.Resource, nodeCount)
	for i := range users {
		users[i] = usr(fmt.Sprintf("seed-%d-user-%d", seed, i))
	}

	basePath := filepath.Join(dir, "base.c1z")
	base, err := dotc1z.NewStore(ctx, basePath, dotc1z.WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	baseSyncID, err := base.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, base.PutResourceTypes(ctx, userRT, groupRT))
	baseResources := append(append([]*v2.Resource(nil), groups...), users...)
	require.NoError(t, base.PutResources(ctx, baseResources...))
	require.NoError(t, base.PutEntitlements(ctx, entitlements...))

	baseGraph := expand.NewEntitlementGraph(ctx)
	for _, entitlement := range entitlements {
		baseGraph.AddEntitlementID(entitlement.GetId())
	}
	grants := make([]*v2.Grant, 0, len(users)+len(edges))
	for i, user := range users {
		// Every node gets one direct member; repeated downstream paths exercise
		// union and de-duplication of provenance.
		grants = append(grants, memberGrant(entitlements[i], user))
	}
	for _, e := range edges {
		grants = append(grants, ruleGrant(entitlements[e.destination], groups[e.source], entitlements[e.source].GetId()))
		require.NoError(t, baseGraph.AddEdge(ctx,
			entitlements[e.source].GetId(), entitlements[e.destination].GetId(), false, nil))
	}
	require.NoError(t, base.PutGrants(ctx, grants...))
	require.NoError(t, baseGraph.FixCycles(ctx))
	require.NoError(t, expand.NewExpander(sdksync.NewExpanderStore(base), baseGraph).Run(ctx))
	baseGraph.Loaded = true
	baseGraph.MarkExpansionComplete()
	require.NoError(t, base.EndSync(ctx))
	persistFixtureGraph(t, ctx, base, baseSyncID, baseGraph)
	require.NoError(t, base.Close(ctx))

	incPath := filepath.Join(dir, "increment.c1z")
	inc, err := dotc1z.NewStore(ctx, incPath, dotc1z.WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	incSyncID, err := inc.StartNewSync(ctx, connectorstore.SyncTypePartial, "")
	require.NoError(t, err)
	require.NoError(t, inc.PutResourceTypes(ctx, userRT, groupRT))

	if seed%2 == 0 {
		// Membership addition on an existing entitlement.
		target := rng.Intn(nodeCount - 1)
		newUser := usr(fmt.Sprintf("seed-%d-delta-user", seed))
		require.NoError(t, inc.PutResources(ctx, groups[target], newUser))
		require.NoError(t, inc.PutEntitlements(ctx, entitlements[target]))
		require.NoError(t, inc.PutGrants(ctx, memberGrant(entitlements[target], newUser)))
	} else {
		// Brand-new forward edge. A missing direct edge may already have a
		// transitive path; that still exercises provenance reconciliation.
		source, destination, ok := missingRandomEdge(nodeCount, edgeSet)
		require.True(t, ok)
		require.NoError(t, inc.PutResources(ctx, groups[source], groups[destination]))
		require.NoError(t, inc.PutEntitlements(ctx, entitlements[source], entitlements[destination]))
		require.NoError(t, inc.PutGrants(ctx,
			ruleGrant(entitlements[destination], groups[source], entitlements[source].GetId())))
	}
	require.NoError(t, inc.EndSync(ctx))
	require.NoError(t, inc.Close(ctx))

	return []*CompactableSync{
		{FilePath: basePath, SyncID: baseSyncID},
		{FilePath: incPath, SyncID: incSyncID},
	}
}

func missingRandomEdge(nodeCount int, existing map[[2]int]struct{}) (int, int, bool) {
	for distance := 2; distance < nodeCount; distance++ {
		for source := 0; source+distance < nodeCount; source++ {
			destination := source + distance
			if _, found := existing[[2]int{source, destination}]; !found {
				return source, destination, true
			}
		}
	}
	return 0, 0, false
}

// assertSealedCompactionArtifact is the Stage 3.2 artifact fsck. It validates
// the sealed sync record and invariant marker for every compaction path, and
// validates graph ownership, completion, and serialization when the path
// promises a sidecar.
func assertSealedCompactionArtifact(
	t *testing.T,
	ctx context.Context,
	out *CompactableSync,
	expectGraph bool,
) {
	t.Helper()
	store, err := dotc1z.NewStore(ctx, out.FilePath,
		dotc1z.WithReadOnly(true), dotc1z.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close(ctx)) }()

	finished, err := store.GetLatestFinishedSync(ctx,
		reader_v2.SyncsReaderServiceGetLatestFinishedSyncRequest_builder{}.Build())
	require.NoError(t, err)
	require.Equal(t, out.SyncID, finished.GetSync().GetId(), "artifact must be sealed")

	run, err := store.SyncMeta().LatestFinishedSyncOfAnyType(ctx)
	require.NoError(t, err)
	require.Equal(t, out.SyncID, run.ID)
	require.NotNil(t, run.EndedAt)
	require.True(t, run.IsVerified())
	require.Equal(t, sdksync.IngestInvariantGeneration, run.Generation)
	require.Equal(t, c1zstore.IngestInvariantVerificationModeCompactionMerge, run.Mode)

	graph, err := sdksync.GraphFromStore(ctx, store, out.SyncID)
	require.NoError(t, err)
	if !expectGraph {
		require.Nil(t, graph, "non-incremental artifact must not inherit a stale graph")
		return
	}
	require.NotNil(t, graph, "artifact must carry a graph stamped for its output sync")
	require.NoError(t, graph.ValidateCompleted())

	data, err := expand.MarshalGraphBlob(out.SyncID, graph)
	require.NoError(t, err)
	roundTrip, err := expand.UnmarshalGraphBlob(data, out.SyncID)
	require.NoError(t, err)
	require.NotNil(t, roundTrip)
	require.NoError(t, roundTrip.ValidateCompleted())
	require.Equal(t, graph, roundTrip, "graph must survive a serialization round trip")
}

// TestCompactorIncrementalDifferentialFoldMode is the fold-mode counterpart of
// TestCompactorIncrementalDifferentialRandom. The base graph reaches expansion
// by a different route in each mode: fold captures it before its merge, every
// other mode reopens the base artifact. Auto mode resolves these fixtures to an
// overlay rebuild, so without forcing fold the captured route never reaches the
// differential oracle and its grant equivalence rests on hand-built fixtures.
func TestCompactorIncrementalDifferentialFoldMode(t *testing.T) {
	const cases = 10
	capturedRuns := 0
	for seed := int64(0); seed < cases; seed++ {
		seed := seed
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			ctx := context.Background()

			incEntries := buildRandomDifferentialFixtures(t, ctx, t.TempDir(), seed)
			incCompactor, incCleanup, err := NewCompactor(ctx, t.TempDir(), incEntries,
				WithTmpDir(t.TempDir()),
				WithEngine(c1zstore.EnginePebble),
				WithPebbleCompactorMode(PebbleCompactorModeFold),
				WithIncrementalExpansion(),
			)
			require.NoError(t, err)
			defer func() { require.NoError(t, incCleanup()) }()
			incOut, err := incCompactor.Compact(ctx)
			require.NoError(t, err)
			require.NotNil(t, incOut)
			require.True(t, incCompactor.incrementalExpansionRan,
				"seed=%d silently fell back instead of expanding incrementally", seed)
			require.NotNil(t, incCompactor.foldBaseGraph,
				"seed=%d fold mode must serve the base graph from its own capture", seed)
			capturedRuns++

			// Full expansion, same inputs, same fold mode: the only difference
			// under comparison is how expansion obtained the base graph.
			fullEntries := buildRandomDifferentialFixtures(t, ctx, t.TempDir(), seed)
			fullCompactor, fullCleanup, err := NewCompactor(ctx, t.TempDir(), fullEntries,
				WithTmpDir(t.TempDir()),
				WithEngine(c1zstore.EnginePebble),
				WithPebbleCompactorMode(PebbleCompactorModeFold),
			)
			require.NoError(t, err)
			defer func() { require.NoError(t, fullCleanup()) }()
			fullOut, err := fullCompactor.Compact(ctx)
			require.NoError(t, err)
			require.NotNil(t, fullOut)

			require.Equal(t,
				grantOutcome(t, ctx, fullOut.FilePath, fullOut.SyncID),
				grantOutcome(t, ctx, incOut.FilePath, incOut.SyncID),
				"seed=%d captured-graph incremental grants differ from full expansion", seed,
			)
			assertSealedCompactionArtifact(t, ctx, incOut, true)
			assertSealedCompactionArtifact(t, ctx, fullOut, false)
		})
	}
	require.Equal(t, cases, capturedRuns, "every case must exercise the captured base graph")
}
