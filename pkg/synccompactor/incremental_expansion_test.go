package synccompactor

import (
	"bytes"
	"context"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	enginepkg "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	sdksync "github.com/conductorone/baton-sdk/pkg/sync"
	"github.com/conductorone/baton-sdk/pkg/sync/expand"
	batonGrant "github.com/conductorone/baton-sdk/pkg/types/grant"
)

// hasGrant asserts some row in outcome starts with the given
// entitlement|type|resource prefix (grantOutcome rows carry a sources suffix).
func hasGrant(t *testing.T, outcome []string, prefix string) {
	t.Helper()
	for _, k := range outcome {
		if strings.HasPrefix(k, prefix+"|") {
			return
		}
	}
	t.Fatalf("expected a grant with prefix %q in %v", prefix, outcome)
}

// This test proves the compactor's diff-aware incremental expansion produces
// the SAME grants as a full expansion, on real Pebble c1z files.
//
// Scenario:
//   base (full, already expanded): ent-b -> ent-c ; mandy is a member of B,
//     and (already expanded) of C.
//   increment (partial): ent-a -> ent-b ; sam is a member of A.
// After compaction, both sam and mandy should be members of B and C.

func grp(id string) *v2.Resource {
	return v2.Resource_builder{
		Id:          v2.ResourceId_builder{ResourceType: "group", Resource: id}.Build(),
		DisplayName: id,
	}.Build()
}

func usr(id string) *v2.Resource {
	return v2.Resource_builder{
		Id:          v2.ResourceId_builder{ResourceType: "user", Resource: id}.Build(),
		DisplayName: id,
	}.Build()
}

func ent(id string, resource *v2.Resource) *v2.Entitlement {
	return v2.Entitlement_builder{
		Id:       id,
		Resource: resource,
		Purpose:  v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT,
	}.Build()
}

// ruleGrant builds an expandable grant: the source group is granted the
// destination entitlement, with a GrantExpandable annotation naming the source
// entitlement — i.e. "members of sourceEntID also get destEnt".
func ruleGrant(destEnt *v2.Entitlement, sourceGroup *v2.Resource, sourceEntID string) *v2.Grant {
	return ruleGrantSources(destEnt, sourceGroup, []string{sourceEntID})
}

func ruleGrantSources(destEnt *v2.Entitlement, sourceGroup *v2.Resource, sourceEntIDs []string) *v2.Grant {
	g := v2.Grant_builder{
		Id:          batonGrant.NewGrantID(sourceGroup, destEnt),
		Entitlement: destEnt,
		Principal:   sourceGroup,
	}.Build()
	g.SetAnnotations(annotations.New(v2.GrantExpandable_builder{
		EntitlementIds: sourceEntIDs,
	}.Build()))
	return g
}

func persistFixtureGraph(t testing.TB, ctx context.Context, store c1zstore.Store, syncID string, graph *expand.EntitlementGraph) {
	t.Helper()
	gs, ok := store.(sdksync.EntitlementGraphStore)
	if !ok {
		return
	}
	digestReader, ok := store.(c1zstore.GrantGenerationDigestReader)
	require.True(t, ok)
	digest, found, err := digestReader.GrantGenerationDigest(ctx)
	require.NoError(t, err)
	require.True(t, found)
	data, err := expand.MarshalGraphBlobWithGrantDigest(syncID, graph, digest)
	require.NoError(t, err)
	require.NoError(t, gs.PutEntitlementGraphBlob(ctx, data))
	verificationWriter, ok := store.SyncMeta().(c1zstore.IngestInvariantVerificationWriter)
	require.True(t, ok)
	require.NoError(t, verificationWriter.MarkIngestInvariantsVerified(ctx, syncID, c1zstore.IngestInvariantVerification{
		Generation: sdksync.IngestInvariantGeneration,
		Coverage:   []string{"test-fixture"},
		Mode:       c1zstore.IngestInvariantVerificationModeConnector,
	}))
}

func overwriteFixtureGraph(t *testing.T, ctx context.Context, path, stampedSyncID string, graph *expand.EntitlementGraph) {
	t.Helper()
	store, err := dotc1z.NewStore(ctx, path, dotc1z.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	data, err := expand.MarshalGraphBlob(stampedSyncID, graph)
	require.NoError(t, err)
	graphStore, ok := store.(sdksync.EntitlementGraphStore)
	require.True(t, ok)
	require.NoError(t, graphStore.PutEntitlementGraphBlob(ctx, data))
	require.NoError(t, store.Close(ctx))
}

func overwriteFixtureGraphRaw(t *testing.T, ctx context.Context, path string, data []byte) {
	t.Helper()
	store, err := dotc1z.NewStore(ctx, path, dotc1z.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	gs, ok := store.(sdksync.EntitlementGraphStore)
	require.True(t, ok)
	require.NoError(t, gs.PutEntitlementGraphBlob(ctx, data))
	require.NoError(t, store.Close(ctx))
}

func buildEmptyPartial(t *testing.T, ctx context.Context, path string) *CompactableSync {
	t.Helper()
	store, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	syncID, err := store.StartNewSync(ctx, connectorstore.SyncTypePartial, "")
	require.NoError(t, err)
	require.NoError(t, store.EndSync(ctx))
	require.NoError(t, store.Close(ctx))
	return &CompactableSync{FilePath: path, SyncID: syncID}
}

func buildDroppedEdgeFixtures(t *testing.T, ctx context.Context, dir string) []*CompactableSync {
	t.Helper()
	grpA, grpB, grpC := grp("grpA"), grp("grpB"), grp("grpC")
	bob := usr("bob")
	entA, entB, entC := ent("ent-a", grpA), ent("ent-b", grpB), ent("ent-c", grpC)
	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()
	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()

	basePath := filepath.Join(dir, "base.c1z")
	base, err := dotc1z.NewStore(ctx, basePath, dotc1z.WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	baseSyncID, err := base.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, base.PutResourceTypes(ctx, userRT, groupRT))
	require.NoError(t, base.PutResources(ctx, grpA, grpB, grpC))
	require.NoError(t, base.PutEntitlements(ctx, entA, entB, entC))
	require.NoError(t, base.PutGrants(ctx, ruleGrantSources(entC, grpA, []string{"ent-a", "ent-b"})))
	require.NoError(t, base.EndSync(ctx))
	persistFixtureGraph(t, ctx, base, baseSyncID, droppedEdgeBaseGraph(t, ctx))
	require.NoError(t, base.Close(ctx))

	incPath := filepath.Join(dir, "inc.c1z")
	inc, err := dotc1z.NewStore(ctx, incPath, dotc1z.WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	incSyncID, err := inc.StartNewSync(ctx, connectorstore.SyncTypePartial, "")
	require.NoError(t, err)
	require.NoError(t, inc.PutResourceTypes(ctx, userRT, groupRT))
	require.NoError(t, inc.PutResources(ctx, grpA, grpB, grpC, bob))
	require.NoError(t, inc.PutEntitlements(ctx, entA, entB, entC))
	require.NoError(t, inc.PutGrants(ctx,
		ruleGrantSources(entC, grpA, []string{"ent-a"}), // drops ent-b -> ent-c
		memberGrant(entB, bob),
	))
	require.NoError(t, inc.EndSync(ctx))
	require.NoError(t, inc.Close(ctx))

	return []*CompactableSync{{FilePath: basePath, SyncID: baseSyncID}, {FilePath: incPath, SyncID: incSyncID}}
}

func droppedEdgeBaseGraph(t *testing.T, ctx context.Context) *expand.EntitlementGraph {
	t.Helper()
	g := expand.NewEntitlementGraph(ctx)
	for _, id := range []string{"ent-a", "ent-b", "ent-c"} {
		g.AddEntitlementID(id)
	}
	for _, src := range []string{"ent-a", "ent-b"} {
		require.NoError(t, g.AddEdge(ctx, src, "ent-c", false, nil))
		g.MarkEdgeExpanded(src, "ent-c")
	}
	g.Loaded = true
	g.HasNoCycles = true
	return g
}

// ruleGrantSpec is ruleGrant with an explicit shallow flag, for edge-spec
// change tests.
func ruleGrantSpec(destEnt *v2.Entitlement, sourceGroup *v2.Resource, sourceEntID string, shallow bool) *v2.Grant {
	g := v2.Grant_builder{
		Id:          batonGrant.NewGrantID(sourceGroup, destEnt),
		Entitlement: destEnt,
		Principal:   sourceGroup,
	}.Build()
	g.SetAnnotations(annotations.New(v2.GrantExpandable_builder{
		EntitlementIds: []string{sourceEntID},
		Shallow:        shallow,
	}.Build()))
	return g
}

func memberGrant(e *v2.Entitlement, principal *v2.Resource) *v2.Grant {
	return v2.Grant_builder{
		Id:          batonGrant.NewGrantID(principal, e),
		Entitlement: e,
		Principal:   principal,
	}.Build()
}

// expandedGrant builds a grant expanded from a direct membership on sourceEntID
// (IsDirect: true) — matching what a real expansion records for a principal
// that is a direct member of the source.
func expandedGrant(e *v2.Entitlement, principal *v2.Resource, sourceEntID string) *v2.Grant {
	g := memberGrant(e, principal)
	g.SetSources(v2.GrantSources_builder{
		Sources: map[string]*v2.GrantSources_GrantSource{sourceEntID: {IsDirect: true}},
	}.Build())
	return g
}

// buildIncrementalFixtures writes a base (full, pre-expanded) c1z and an
// increment (partial) c1z into dir, returning the compactable entries.
func buildIncrementalFixtures(t testing.TB, ctx context.Context, dir string) []*CompactableSync {
	return buildIncrementalFixturesEngine(t, ctx, dir, c1zstore.EnginePebble)
}

// buildIncrementalFixturesEngine is buildIncrementalFixtures with a chosen
// storage engine, so the SQLite degrade path can be exercised too.
func buildIncrementalFixturesEngine(t testing.TB, ctx context.Context, dir string, engine c1zstore.Engine) []*CompactableSync {
	t.Helper()

	grpA, grpB, grpC := grp("grpA"), grp("grpB"), grp("grpC")
	sam, mandy := usr("sam"), usr("mandy")
	entA, entB, entC := ent("ent-a", grpA), ent("ent-b", grpB), ent("ent-c", grpC)

	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()
	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()

	// --- base: full, already expanded (ent-b -> ent-c, mandy on both) ---
	basePath := filepath.Join(dir, "base.c1z")
	base, err := dotc1z.NewStore(ctx, basePath, dotc1z.WithEngine(engine))
	require.NoError(t, err)
	baseSyncID, err := base.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, base.PutResourceTypes(ctx, userRT, groupRT))
	require.NoError(t, base.PutResources(ctx, grpB, grpC, mandy))
	require.NoError(t, base.PutEntitlements(ctx, entB, entC))
	require.NoError(t, base.PutGrants(ctx,
		memberGrant(entB, mandy),            // mandy is a direct member of B
		expandedGrant(entC, mandy, "ent-b"), // already expanded: mandy on C via B
		ruleGrant(entC, grpB, "ent-b"),      // rule: members of B get C
	))
	require.NoError(t, base.EndSync(ctx))
	persistFixtureGraph(t, ctx, base, baseSyncID, baseGraphForFixtures(t, ctx))
	require.NoError(t, base.Close(ctx))

	// --- increment: partial, adds ent-a -> ent-b with sam on A ---
	incPath := filepath.Join(dir, "inc.c1z")
	inc, err := dotc1z.NewStore(ctx, incPath, dotc1z.WithEngine(engine))
	require.NoError(t, err)
	incSyncID, err := inc.StartNewSync(ctx, connectorstore.SyncTypePartial, "")
	require.NoError(t, err)
	require.NoError(t, inc.PutResourceTypes(ctx, userRT, groupRT))
	require.NoError(t, inc.PutResources(ctx, grpA, sam))
	require.NoError(t, inc.PutEntitlements(ctx, entA))
	require.NoError(t, inc.PutGrants(ctx,
		memberGrant(entA, sam),         // sam is a direct member of A
		ruleGrant(entB, grpA, "ent-a"), // rule: members of A get B
	))
	require.NoError(t, inc.EndSync(ctx))
	require.NoError(t, inc.Close(ctx))

	return []*CompactableSync{
		{FilePath: basePath, SyncID: baseSyncID},
		{FilePath: incPath, SyncID: incSyncID},
	}
}

// baseGraphForFixtures returns the in-memory graph the base sync would have
// persisted (ent-b -> ent-c, already expanded) — what sync.GraphFromToken
// would hand back in production.
func baseGraphForFixtures(t testing.TB, ctx context.Context) *expand.EntitlementGraph {
	t.Helper()
	g := expand.NewEntitlementGraph(ctx)
	g.AddEntitlementID("ent-b")
	g.AddEntitlementID("ent-c")
	require.NoError(t, g.AddEdge(ctx, "ent-b", "ent-c", false, nil))
	g.MarkEdgeExpanded("ent-b", "ent-c")
	g.Loaded = true
	g.HasNoCycles = true
	return g
}

// grantOutcome reads every grant from a compacted c1z and returns the set of
// full-row keys "entitlement|principalType|principalResource|sources=..." —
// INCLUDING the sources/provenance map, so the differential also pins that
// incremental produces the same provenance as full expansion.
func grantOutcome(t *testing.T, ctx context.Context, path, syncID string) []string {
	t.Helper()
	store, err := dotc1z.NewStore(ctx, path, dotc1z.WithReadOnly(true))
	require.NoError(t, err)
	defer store.Close(ctx)
	require.NoError(t, store.SetCurrentSync(ctx, syncID))

	set := map[string]struct{}{}
	pageToken := ""
	for {
		resp, err := store.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{
			PageSize:  1000,
			PageToken: pageToken,
		}.Build())
		require.NoError(t, err)
		for _, g := range resp.GetList() {
			pid := g.GetPrincipal().GetId()
			srcs := g.GetSources().GetSources()
			parts := make([]string, 0, len(srcs))
			for id, s := range srcs {
				parts = append(parts, id+":"+strconv.FormatBool(s.GetIsDirect()))
			}
			sort.Strings(parts)
			set[g.GetEntitlement().GetId()+"|"+pid.GetResourceType()+"|"+pid.GetResource()+"|sources="+strings.Join(parts, ",")] = struct{}{}
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func TestCompactor_IncrementalExpansionMatchesFull(t *testing.T) {
	ctx := context.Background()

	// --- Path A: incremental expansion (base graph supplied) ---
	incDir := t.TempDir()
	incEntries := buildIncrementalFixtures(t, ctx, incDir)
	cInc, cleanupInc, err := NewCompactor(ctx, t.TempDir(), incEntries,
		WithTmpDir(t.TempDir()),
		WithEngine(c1zstore.EnginePebble),
		WithIncrementalExpansion(),
	)
	require.NoError(t, err)
	defer func() { _ = cleanupInc() }()
	incOut, err := cInc.Compact(ctx)
	require.NoError(t, err)
	require.NotNil(t, incOut)
	require.True(t, cInc.incrementalExpansionRan, "incremental path must have run, not fallen back to full")

	// --- Path B: full expansion (no base graph) ---
	fullDir := t.TempDir()
	fullEntries := buildIncrementalFixtures(t, ctx, fullDir)
	cFull, cleanupFull, err := NewCompactor(ctx, t.TempDir(), fullEntries,
		WithTmpDir(t.TempDir()),
		WithEngine(c1zstore.EnginePebble),
	)
	require.NoError(t, err)
	defer func() { _ = cleanupFull() }()
	fullOut, err := cFull.Compact(ctx)
	require.NoError(t, err)
	require.NotNil(t, fullOut)

	// --- The two must agree ---
	incGrants := grantOutcome(t, ctx, incOut.FilePath, incOut.SyncID)
	fullGrants := grantOutcome(t, ctx, fullOut.FilePath, fullOut.SyncID)
	require.Equal(t, fullGrants, incGrants, "incremental expansion must equal full expansion")

	// sam (from A) must have reached B and C via the cascade.
	hasGrant(t, incGrants, "ent-b|user|sam")
	hasGrant(t, incGrants, "ent-c|user|sam")
	hasGrant(t, incGrants, "ent-c|user|mandy")
}

// buildNewMemberFixtures writes a base (full, pre-expanded ent-b -> ent-c) c1z
// and an increment that adds a NEW MEMBER (bob) to the existing ent-b — with
// NO new rule grant / edge. This is the blocker case: bob must still reach
// ent-c.
func buildNewMemberFixtures(t *testing.T, ctx context.Context, dir string) []*CompactableSync {
	t.Helper()

	grpB, grpC := grp("grpB"), grp("grpC")
	mandy, bob := usr("mandy"), usr("bob")
	entB, entC := ent("ent-b", grpB), ent("ent-c", grpC)

	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()
	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()

	// base: full, already expanded (ent-b -> ent-c, mandy on both).
	basePath := filepath.Join(dir, "base.c1z")
	base, err := dotc1z.NewStore(ctx, basePath, dotc1z.WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	baseSyncID, err := base.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, base.PutResourceTypes(ctx, userRT, groupRT))
	require.NoError(t, base.PutResources(ctx, grpB, grpC, mandy))
	require.NoError(t, base.PutEntitlements(ctx, entB, entC))
	require.NoError(t, base.PutGrants(ctx,
		memberGrant(entB, mandy),
		expandedGrant(entC, mandy, "ent-b"),
		ruleGrant(entC, grpB, "ent-b"),
	))
	require.NoError(t, base.EndSync(ctx))
	persistFixtureGraph(t, ctx, base, baseSyncID, baseGraphForFixtures(t, ctx))
	require.NoError(t, base.Close(ctx))

	// increment: partial, adds bob as a direct member of the EXISTING ent-b.
	// No rule grant → no new edge.
	incPath := filepath.Join(dir, "inc.c1z")
	inc, err := dotc1z.NewStore(ctx, incPath, dotc1z.WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	incSyncID, err := inc.StartNewSync(ctx, connectorstore.SyncTypePartial, "")
	require.NoError(t, err)
	require.NoError(t, inc.PutResourceTypes(ctx, userRT, groupRT))
	require.NoError(t, inc.PutResources(ctx, grpB, bob))
	require.NoError(t, inc.PutEntitlements(ctx, entB))
	require.NoError(t, inc.PutGrants(ctx, memberGrant(entB, bob)))
	require.NoError(t, inc.EndSync(ctx))
	require.NoError(t, inc.Close(ctx))

	return []*CompactableSync{
		{FilePath: basePath, SyncID: baseSyncID},
		{FilePath: incPath, SyncID: incSyncID},
	}
}

func buildMemberPartial(t *testing.T, ctx context.Context, path, entitlementID, groupID, userID string) *CompactableSync {
	t.Helper()
	group, user := grp(groupID), usr(userID)
	entitlement := ent(entitlementID, group)
	store, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	syncID, err := store.StartNewSync(ctx, connectorstore.SyncTypePartial, "")
	require.NoError(t, err)
	require.NoError(t, store.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build(),
		v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()))
	require.NoError(t, store.PutResources(ctx, group, user))
	require.NoError(t, store.PutEntitlements(ctx, entitlement))
	require.NoError(t, store.PutGrants(ctx, memberGrant(entitlement, user)))
	require.NoError(t, store.EndSync(ctx))
	require.NoError(t, store.Close(ctx))
	return &CompactableSync{FilePath: path, SyncID: syncID}
}

// TestCompactor_IncrementalNewMemberMatchesFull is the blocker regression at
// the compactor level: an increment that adds a new member to an existing
// group (no new edge) must still propagate that member downstream, and match
// full expansion. The changed entitlement id ("ent-b") is passed so the walk
// is seeded from it.
func TestCompactor_IncrementalNewMemberMatchesFull(t *testing.T) {
	ctx := context.Background()

	// Path A: incremental, seeded with the changed entitlement.
	incEntries := buildNewMemberFixtures(t, ctx, t.TempDir())
	cInc, cleanupInc, err := NewCompactor(ctx, t.TempDir(), incEntries,
		WithTmpDir(t.TempDir()),
		WithEngine(c1zstore.EnginePebble),
		WithIncrementalExpansion(),
	)
	require.NoError(t, err)
	defer func() { _ = cleanupInc() }()
	incOut, err := cInc.Compact(ctx)
	require.NoError(t, err)
	require.NotNil(t, incOut)
	require.True(t, cInc.incrementalExpansionRan, "incremental path must have run, not fallen back to full")

	// Path B: full expansion (no base graph).
	fullEntries := buildNewMemberFixtures(t, ctx, t.TempDir())
	cFull, cleanupFull, err := NewCompactor(ctx, t.TempDir(), fullEntries,
		WithTmpDir(t.TempDir()),
		WithEngine(c1zstore.EnginePebble),
	)
	require.NoError(t, err)
	defer func() { _ = cleanupFull() }()
	fullOut, err := cFull.Compact(ctx)
	require.NoError(t, err)
	require.NotNil(t, fullOut)

	incGrants := grantOutcome(t, ctx, incOut.FilePath, incOut.SyncID)
	fullGrants := grantOutcome(t, ctx, fullOut.FilePath, fullOut.SyncID)
	require.Equal(t, fullGrants, incGrants, "incremental (new member) must equal full expansion")

	// The blocker: bob (new member of B) must have reached C.
	hasGrant(t, incGrants, "ent-b|user|bob")
	hasGrant(t, incGrants, "ent-c|user|bob")
	hasGrant(t, incGrants, "ent-c|user|mandy")
}

// buildSpecChangeFixtures builds a base with a B->C rule at baseShallow, plus
// mandy (direct on B) and bob (indirect on B), pre-expanded per the base spec;
// and an increment that overwrites the B->C rule to incShallow (same grant id).
func buildSpecChangeFixtures(t testing.TB, ctx context.Context, dir string, baseShallow, incShallow bool) []*CompactableSync {
	t.Helper()
	grpB, grpC := grp("grpB"), grp("grpC")
	mandy, bob := usr("mandy"), usr("bob")
	entB, entC := ent("ent-b", grpB), ent("ent-c", grpC)
	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()
	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()

	basePath := filepath.Join(dir, "base.c1z")
	base, err := dotc1z.NewStore(ctx, basePath, dotc1z.WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	baseSyncID, err := base.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, base.PutResourceTypes(ctx, userRT, groupRT))
	require.NoError(t, base.PutResources(ctx, grpB, grpC, mandy, bob))
	require.NoError(t, base.PutEntitlements(ctx, entB, entC))
	baseGrants := []*v2.Grant{
		memberGrant(entB, mandy),          // mandy: direct member of B
		expandedGrant(entB, bob, "ent-x"), // bob: indirect on B (source is not B)
		ruleGrantSpec(entC, grpB, "ent-b", baseShallow),
		expandedGrant(entC, mandy, "ent-b"), // mandy on C (direct qualifies either way)
	}
	if !baseShallow {
		baseGrants = append(baseGrants, expandedGrant(entC, bob, "ent-b")) // bob on C only when deep
	}
	require.NoError(t, base.PutGrants(ctx, baseGrants...))
	require.NoError(t, base.EndSync(ctx))
	persistFixtureGraph(t, ctx, base, baseSyncID, specChangeBaseGraph(t, ctx, baseShallow))
	require.NoError(t, base.Close(ctx))

	incPath := filepath.Join(dir, "inc.c1z")
	inc, err := dotc1z.NewStore(ctx, incPath, dotc1z.WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	incSyncID, err := inc.StartNewSync(ctx, connectorstore.SyncTypePartial, "")
	require.NoError(t, err)
	require.NoError(t, inc.PutResourceTypes(ctx, userRT, groupRT))
	require.NoError(t, inc.PutResources(ctx, grpB, grpC))
	require.NoError(t, inc.PutEntitlements(ctx, entB, entC))
	require.NoError(t, inc.PutGrants(ctx, ruleGrantSpec(entC, grpB, "ent-b", incShallow)))
	require.NoError(t, inc.EndSync(ctx))
	require.NoError(t, inc.Close(ctx))

	return []*CompactableSync{
		{FilePath: basePath, SyncID: baseSyncID},
		{FilePath: incPath, SyncID: incSyncID},
	}
}

func specChangeBaseGraph(t testing.TB, ctx context.Context, shallow bool) *expand.EntitlementGraph {
	t.Helper()
	g := expand.NewEntitlementGraph(ctx)
	g.AddEntitlementID("ent-b")
	g.AddEntitlementID("ent-c")
	require.NoError(t, g.AddEdge(ctx, "ent-b", "ent-c", shallow, nil))
	g.MarkEdgeExpanded("ent-b", "ent-c")
	g.Loaded = true
	g.HasNoCycles = true
	return g
}

// TestCompactor_IncrementalWidenedEdgeReExpands (C3): an increment that widens
// an existing edge (shallow -> deep) must re-expand it — the previously-excluded
// indirect member now propagates — and match full expansion.
func TestCompactor_IncrementalWidenedEdgeReExpands(t *testing.T) {
	ctx := context.Background()

	incEntries := buildSpecChangeFixtures(t, ctx, t.TempDir(), true, false) // shallow -> deep
	cInc, cleanupInc, err := NewCompactor(ctx, t.TempDir(), incEntries,
		WithTmpDir(t.TempDir()),
		WithEngine(c1zstore.EnginePebble),
		WithIncrementalExpansion(), // base edge is shallow
	)
	require.NoError(t, err)
	defer func() { _ = cleanupInc() }()
	incOut, err := cInc.Compact(ctx)
	require.NoError(t, err)
	require.True(t, cInc.incrementalExpansionRan, "widened edge must re-expand, not fall back")

	fullEntries := buildSpecChangeFixtures(t, ctx, t.TempDir(), true, false)
	cFull, cleanupFull, err := NewCompactor(ctx, t.TempDir(), fullEntries,
		WithTmpDir(t.TempDir()), WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	defer func() { _ = cleanupFull() }()
	fullOut, err := cFull.Compact(ctx)
	require.NoError(t, err)

	incGrants := grantOutcome(t, ctx, incOut.FilePath, incOut.SyncID)
	fullGrants := grantOutcome(t, ctx, fullOut.FilePath, fullOut.SyncID)
	require.Equal(t, fullGrants, incGrants, "widened re-expansion must equal full")
	hasGrant(t, incGrants, "ent-c|user|bob") // bob now qualifies (deep)
}

// TestCompactor_IncrementalNarrowedEdgeDeclines (C3/#6/#11c): an increment that
// narrows an existing edge (deep -> shallow) is revocation-shaped; incremental
// must decline (via the named branch) and fall back to full. Pins today's
// behavior: incremental-with-fallback == full. The future deletion stage turns
// this red then green.
func TestCompactor_IncrementalNarrowedEdgeDeclines(t *testing.T) {
	ctx := context.Background()

	incEntries := buildSpecChangeFixtures(t, ctx, t.TempDir(), false, true) // deep -> shallow
	cInc, cleanupInc, err := NewCompactor(ctx, t.TempDir(), incEntries,
		WithTmpDir(t.TempDir()),
		WithEngine(c1zstore.EnginePebble),
		WithIncrementalExpansion(), // base edge is deep
	)
	require.NoError(t, err)
	defer func() { _ = cleanupInc() }()
	incOut, err := cInc.Compact(ctx)
	require.NoError(t, err)
	require.False(t, cInc.incrementalExpansionRan, "narrowed edge must decline to full expansion")

	fullEntries := buildSpecChangeFixtures(t, ctx, t.TempDir(), false, true)
	cFull, cleanupFull, err := NewCompactor(ctx, t.TempDir(), fullEntries,
		WithTmpDir(t.TempDir()), WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	defer func() { _ = cleanupFull() }()
	fullOut, err := cFull.Compact(ctx)
	require.NoError(t, err)

	incGrants := grantOutcome(t, ctx, incOut.FilePath, incOut.SyncID)
	fullGrants := grantOutcome(t, ctx, fullOut.FilePath, fullOut.SyncID)
	require.Equal(t, fullGrants, incGrants, "declined incremental must equal full expansion")
}

func TestSplitEdgeSpecsAreMergedBeforeClassification(t *testing.T) {
	base := &expand.Edge{
		IsShallow:       false,
		ResourceTypeIDs: []string{"group", "user"},
	}
	users := expand.NewEdge{
		SourceEntitlementID: "ent-a",
		DestEntitlementID:   "ent-b",
		Shallow:             true,
		ResourceTypeIDs:     []string{"user"},
	}
	groups := expand.NewEdge{
		SourceEntitlementID: "ent-a",
		DestEntitlementID:   "ent-b",
		Shallow:             false,
		ResourceTypeIDs:     []string{"group"},
	}

	// Comparing either fragment by itself produces the previous false
	// revocation: each fragment is narrower than the effective base edge.
	require.Equal(t, edgeSpecNarrowed, classifyEdgeSpecChange(base, users))
	require.Equal(t, edgeSpecNarrowed, classifyEdgeSpecChange(base, groups))

	// Production now merges both fragments first. Deep wins and the filters
	// union, exactly matching the effective base edge.
	current := mergeCurrentEdgeSpecs(users, groups)
	require.False(t, current.Shallow)
	require.ElementsMatch(t, []string{"group", "user"}, current.ResourceTypeIDs)
	require.Equal(t, edgeSpecUnchanged, classifyEdgeSpecChange(base, current))

	unfiltered := users
	unfiltered.ResourceTypeIDs = nil
	current = mergeCurrentEdgeSpecs(groups, unfiltered)
	require.Nil(t, current.ResourceTypeIDs, "an unfiltered fragment makes the effective edge unfiltered")
}

// TestCompactor_IncrementalDoesNotMutateBaseGraph (U1): running incremental
// expansion must not mutate the graph persisted in the caller's base artifact.
func TestCompactor_IncrementalDoesNotMutateBaseGraph(t *testing.T) {
	ctx := context.Background()

	entries := buildIncrementalFixtures(t, ctx, t.TempDir()) // increment adds ent-a -> ent-b
	base := artifactGraph(t, ctx, entries[0].FilePath, entries[0].SyncID)
	edgesBefore := len(base.Edges)
	nodesBefore := len(base.Nodes)

	c, cleanup, err := NewCompactor(ctx, t.TempDir(), entries,
		WithTmpDir(t.TempDir()),
		WithEngine(c1zstore.EnginePebble),
		WithIncrementalExpansion(),
	)
	require.NoError(t, err)
	defer func() { _ = cleanup() }()

	_, err = c.Compact(ctx)
	require.NoError(t, err)
	require.True(t, c.incrementalExpansionRan)

	after := artifactGraph(t, ctx, entries[0].FilePath, entries[0].SyncID)
	require.Equal(t, edgesBefore, len(after.Edges), "base graph edges must be unchanged")
	require.Equal(t, nodesBefore, len(after.Nodes), "base graph nodes must be unchanged")
	require.Nil(t, after.GetNode("ent-a"), "new edge's node must not leak into the base artifact")
}

// buildEdgeValidationFixtures builds a valid base plus one new rule targeting
// ent-d. sourceEntitlementID selects either a missing source ("ent-ghost") or
// an existing source whose resource differs from the rule principal ("ent-b").
func buildEdgeValidationFixtures(t *testing.T, ctx context.Context, dir, sourceEntitlementID string) []*CompactableSync {
	t.Helper()
	grpB, grpC, grpD := grp("grpB"), grp("grpC"), grp("grpD")
	mandy := usr("mandy")
	entB, entC, entD := ent("ent-b", grpB), ent("ent-c", grpC), ent("ent-d", grpD)
	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()
	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()

	basePath := filepath.Join(dir, "base.c1z")
	base, err := dotc1z.NewStore(ctx, basePath, dotc1z.WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	baseSyncID, err := base.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, base.PutResourceTypes(ctx, userRT, groupRT))
	require.NoError(t, base.PutResources(ctx, grpB, grpC, grpD, mandy))
	require.NoError(t, base.PutEntitlements(ctx, entB, entC, entD))
	require.NoError(t, base.PutGrants(ctx,
		memberGrant(entB, mandy),
		expandedGrant(entC, mandy, "ent-b"),
		ruleGrant(entC, grpB, "ent-b"),
	))
	require.NoError(t, base.EndSync(ctx))
	baseGraph := baseGraphForFixtures(t, ctx)
	baseGraph.AddEntitlementID("ent-d")
	baseGraph.HasNoCycles = true
	persistFixtureGraph(t, ctx, base, baseSyncID, baseGraph)
	require.NoError(t, base.Close(ctx))

	incPath := filepath.Join(dir, "inc.c1z")
	inc, err := dotc1z.NewStore(ctx, incPath, dotc1z.WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	incSyncID, err := inc.StartNewSync(ctx, connectorstore.SyncTypePartial, "")
	require.NoError(t, err)
	require.NoError(t, inc.PutResourceTypes(ctx, userRT, groupRT))
	require.NoError(t, inc.PutResources(ctx, grpD))
	require.NoError(t, inc.PutEntitlements(ctx, entD))
	require.NoError(t, inc.PutGrants(ctx, ruleGrant(entD, grpD, sourceEntitlementID)))
	require.NoError(t, inc.EndSync(ctx))
	require.NoError(t, inc.Close(ctx))

	return []*CompactableSync{
		{FilePath: basePath, SyncID: baseSyncID},
		{FilePath: incPath, SyncID: incSyncID},
	}
}

// TestCompactor_IncrementalDanglingRefMatchesFull (#11a): a missing source is
// skipped before it can create a phantom graph node or edge.
func TestCompactor_IncrementalDanglingRefMatchesFull(t *testing.T) {
	ctx := context.Background()

	incEntries := buildEdgeValidationFixtures(t, ctx, t.TempDir(), "ent-ghost")
	cInc, cleanupInc, err := NewCompactor(ctx, t.TempDir(), incEntries,
		WithTmpDir(t.TempDir()),
		WithEngine(c1zstore.EnginePebble),
		WithIncrementalExpansion(),
	)
	require.NoError(t, err)
	defer func() { _ = cleanupInc() }()
	incOut, err := cInc.Compact(ctx)
	require.NoError(t, err)
	require.True(t, cInc.incrementalExpansionRan, "a dangling new edge must be skipped without forcing fallback")

	fullEntries := buildEdgeValidationFixtures(t, ctx, t.TempDir(), "ent-ghost")
	cFull, cleanupFull, err := NewCompactor(ctx, t.TempDir(), fullEntries,
		WithTmpDir(t.TempDir()), WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	defer func() { _ = cleanupFull() }()
	fullOut, err := cFull.Compact(ctx)
	require.NoError(t, err)

	incGrants := grantOutcome(t, ctx, incOut.FilePath, incOut.SyncID)
	fullGrants := grantOutcome(t, ctx, fullOut.FilePath, fullOut.SyncID)
	require.Equal(t, fullGrants, incGrants, "dangling-ref incremental must equal full")
	graph := artifactGraph(t, ctx, incOut.FilePath, incOut.SyncID)
	require.NotNil(t, graph)
	require.Nil(t, graph.GetNode("ent-ghost"), "missing source must not persist as a phantom graph node")
}

func TestCompactor_IncrementalPrincipalMismatchMatchesFullError(t *testing.T) {
	ctx := context.Background()

	incEntries := buildEdgeValidationFixtures(t, ctx, t.TempDir(), "ent-b")
	cInc, cleanupInc, err := NewCompactor(ctx, t.TempDir(), incEntries,
		WithTmpDir(t.TempDir()), WithEngine(c1zstore.EnginePebble), WithIncrementalExpansion())
	require.NoError(t, err)
	defer func() { _ = cleanupInc() }()
	_, incErr := cInc.Compact(ctx)
	require.ErrorContains(t, incErr, "source entitlement resource id did not match grant principal id")

	fullEntries := buildEdgeValidationFixtures(t, ctx, t.TempDir(), "ent-b")
	cFull, cleanupFull, err := NewCompactor(ctx, t.TempDir(), fullEntries,
		WithTmpDir(t.TempDir()), WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	defer func() { _ = cleanupFull() }()
	_, fullErr := cFull.Compact(ctx)
	require.ErrorContains(t, fullErr, "source entitlement resource id did not match grant principal id")
}

// TestCompactor_IncrementalSealedArtifactLifecycle (#11b): after the
// incremental end→resume→write→end sequence, the reopened artifact's sync must
// be sealed (finished) and the Pebble by_principal index must cover the
// incrementally-written grants (rebuilt at the final EndSync).
func TestCompactor_IncrementalSealedArtifactLifecycle(t *testing.T) {
	ctx := context.Background()

	entries := buildIncrementalFixtures(t, ctx, t.TempDir()) // increment brings sam
	c, cleanup, err := NewCompactor(ctx, t.TempDir(), entries,
		WithTmpDir(t.TempDir()),
		WithEngine(c1zstore.EnginePebble),
		WithIncrementalExpansion(),
	)
	require.NoError(t, err)
	defer func() { _ = cleanup() }()
	out, err := c.Compact(ctx)
	require.NoError(t, err)
	require.True(t, c.incrementalExpansionRan)

	store, err := dotc1z.NewStore(ctx, out.FilePath, dotc1z.WithReadOnly(true))
	require.NoError(t, err)
	defer store.Close(ctx)

	// (1) Sealed: the compacted sync is finished.
	fin, err := store.GetLatestFinishedSync(ctx, reader_v2.SyncsReaderServiceGetLatestFinishedSyncRequest_builder{}.Build())
	require.NoError(t, err)
	require.Equal(t, out.SyncID, fin.GetSync().GetId(), "compacted sync must be sealed/finished")
	run, err := store.SyncMeta().LatestFinishedSyncOfAnyType(ctx)
	require.NoError(t, err)
	require.Equal(t, sdksync.IngestInvariantGeneration, run.Generation)
	require.Equal(t, c1zstore.IngestInvariantVerificationModeCompactionMerge, run.Mode)

	// (2) by_principal index is populated and covers sam (written incrementally).
	eng, ok := enginepkg.AsEngine(store)
	require.True(t, ok, "expected a pebble engine")
	it, err := eng.NewIter(&pebble.IterOptions{
		LowerBound: enginepkg.GrantByPrincipalLowerBound(),
		UpperBound: enginepkg.GrantByPrincipalUpperBound(),
	})
	require.NoError(t, err)
	defer it.Close()
	total, sawSam := 0, false
	for it.First(); it.Valid(); it.Next() {
		total++
		if bytes.Contains(it.Key(), []byte("sam")) {
			sawSam = true
		}
	}
	require.NoError(t, it.Error())
	require.Positive(t, total, "by_principal index must have entries")
	require.True(t, sawSam, "by_principal index must cover sam's incrementally-written grants")
}

func TestCompactor_IncrementalFailFastInvariantMarker(t *testing.T) {
	ctx := context.Background()
	entries := buildIncrementalFixtures(t, ctx, t.TempDir())
	c, cleanup, err := NewCompactor(ctx, t.TempDir(), entries,
		WithTmpDir(t.TempDir()), WithEngine(c1zstore.EnginePebble),
		WithIncrementalExpansion(), WithFailFastInvariants())
	require.NoError(t, err)
	defer func() { _ = cleanup() }()
	out, err := c.Compact(ctx)
	require.NoError(t, err)
	require.True(t, c.incrementalExpansionRan)

	store, err := dotc1z.NewStore(ctx, out.FilePath, dotc1z.WithReadOnly(true), dotc1z.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	defer store.Close(ctx)
	run, err := store.SyncMeta().LatestFinishedSyncOfAnyType(ctx)
	require.NoError(t, err)
	require.Equal(t, c1zstore.IngestInvariantVerificationModeCompactionMergeFailFast, run.Mode)
}

// TestCompactor_IncrementalDegradesGracefullyOnSQLite: the fast path is
// Pebble-only (it reopens an ended sync, which SQLite refuses). On a SQLite
// output, requesting incremental must degrade to full expansion — no error,
// correct grants, and incrementalExpansionRan must be false.
func TestCompactor_IncrementalDegradesGracefullyOnSQLite(t *testing.T) {
	ctx := context.Background()

	entries := buildIncrementalFixturesEngine(t, ctx, t.TempDir(), c1zstore.EngineSQLite)
	// No WithEngine → engine inferred from the SQLite inputs.
	c, cleanup, err := NewCompactor(ctx, t.TempDir(), entries,
		WithTmpDir(t.TempDir()),
		WithIncrementalExpansion(),
	)
	require.NoError(t, err)
	defer func() { _ = cleanup() }()

	out, err := c.Compact(ctx)
	require.NoError(t, err, "SQLite must degrade gracefully, not error")
	require.NotNil(t, out)
	require.False(t, c.incrementalExpansionRan, "SQLite must fall back to full expansion")

	// Grants are still correct (produced by full expansion).
	grants := grantOutcome(t, ctx, out.FilePath, out.SyncID)
	hasGrant(t, grants, "ent-b|user|sam")
	hasGrant(t, grants, "ent-c|user|sam")
	hasGrant(t, grants, "ent-c|user|mandy")

	// SQLite has no graph sidecar, so a preserved graph could never be read
	// back — the only place it could land is the final sync token, as
	// unreadable bloat. Pin that the final token carries no graph (enforced
	// twice over: graph preservation is Pebble-gated in expandGrants, and
	// state.Marshal drops the graph from tokens by default).
	store, err := dotc1z.NewStore(ctx, out.FilePath, dotc1z.WithReadOnly(true), dotc1z.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	defer store.Close(ctx)
	run, err := store.SyncMeta().LatestFinishedSyncOfAnyType(ctx)
	require.NoError(t, err)
	if run.SyncToken != "" {
		tokenGraph, err := sdksync.GraphFromToken(run.SyncToken)
		require.NoError(t, err)
		require.Nil(t, tokenGraph, "SQLite final sync token must not carry an entitlement graph")
	}
}

// TestCompactor_IncrementalBaseWithNoFinishedSyncDeclines: a base c1z whose
// sync never ended (interrupted collection) makes LatestFinishedSyncOfAnyType
// return (nil, nil) on both engines. The base-graph loader must return an
// error — declining to full expansion — not panic on run.ID.
func TestCompactor_IncrementalBaseWithNoFinishedSyncDeclines(t *testing.T) {
	ctx := context.Background()

	basePath := filepath.Join(t.TempDir(), "unfinished.c1z")
	store, err := dotc1z.NewStore(ctx, basePath, dotc1z.WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	syncID, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	// No EndSync: the artifact holds no finished sync run.
	require.NoError(t, store.Close(ctx))

	c := &Compactor{
		entries: []*CompactableSync{{FilePath: basePath, SyncID: syncID}},
		tmpDir:  t.TempDir(),
	}
	graph, err := c.loadIncrementalBaseGraph(ctx)
	require.Error(t, err, "an unfinished base must decline, not panic")
	require.ErrorContains(t, err, "no finished sync")
	require.Nil(t, graph)
}

// TestCompactor_IncrementalNewMemberFoldCollectsChangedEnts: fold mode
// collects the changed-entitlement set during the merge (no re-read) and
// still matches full expansion.
func TestCompactor_IncrementalNewMemberFoldCollectsChangedEnts(t *testing.T) {
	ctx := context.Background()

	incEntries := buildNewMemberFixtures(t, ctx, t.TempDir())
	cInc, cleanupInc, err := NewCompactor(ctx, t.TempDir(), incEntries,
		WithTmpDir(t.TempDir()),
		WithEngine(c1zstore.EnginePebble),
		WithPebbleCompactorMode(PebbleCompactorModeFold),
		WithIncrementalExpansion(),
	)
	require.NoError(t, err)
	defer func() { _ = cleanupInc() }()
	incOut, err := cInc.Compact(ctx)
	require.NoError(t, err)
	require.NotNil(t, incOut)
	require.True(t, cInc.incrementalExpansionRan, "incremental path must have run, not fallen back to full")

	require.NotNil(t, cInc.foldChangedEntitlementIDs, "fold mode must hand its collected set to expansion")
	require.Contains(t, cInc.foldChangedEntitlementIDs, "ent-b",
		"bob's new membership grant on ent-b was applied by the fold")

	fullEntries := buildNewMemberFixtures(t, ctx, t.TempDir())
	cFull, cleanupFull, err := NewCompactor(ctx, t.TempDir(), fullEntries,
		WithTmpDir(t.TempDir()),
		WithEngine(c1zstore.EnginePebble),
		WithPebbleCompactorMode(PebbleCompactorModeFold),
	)
	require.NoError(t, err)
	defer func() { _ = cleanupFull() }()
	fullOut, err := cFull.Compact(ctx)
	require.NoError(t, err)

	incGrants := grantOutcome(t, ctx, incOut.FilePath, incOut.SyncID)
	fullGrants := grantOutcome(t, ctx, fullOut.FilePath, fullOut.SyncID)
	require.Equal(t, fullGrants, incGrants, "fold-collected incremental must equal full expansion")
	hasGrant(t, incGrants, "ent-c|user|bob")
}

// TestCompactor_IncrementalNewMemberRebuildFallsBackToDerive: no fold ->
// derive fallback; the fast path still runs and matches full.
func TestCompactor_IncrementalNewMemberRebuildFallsBackToDerive(t *testing.T) {
	ctx := context.Background()

	incEntries := buildNewMemberFixtures(t, ctx, t.TempDir())
	cInc, cleanupInc, err := NewCompactor(ctx, t.TempDir(), incEntries,
		WithTmpDir(t.TempDir()),
		WithEngine(c1zstore.EnginePebble),
		WithPebbleCompactorMode(PebbleCompactorModeOverlay),
		WithIncrementalExpansion(),
	)
	require.NoError(t, err)
	defer func() { _ = cleanupInc() }()
	incOut, err := cInc.Compact(ctx)
	require.NoError(t, err)
	require.NotNil(t, incOut)
	require.True(t, cInc.incrementalExpansionRan, "incremental path must have run, not fallen back to full")
	require.Nil(t, cInc.foldChangedEntitlementIDs, "no fold ran, so the derive fallback must have been used")

	fullEntries := buildNewMemberFixtures(t, ctx, t.TempDir())
	cFull, cleanupFull, err := NewCompactor(ctx, t.TempDir(), fullEntries,
		WithTmpDir(t.TempDir()),
		WithEngine(c1zstore.EnginePebble),
		WithPebbleCompactorMode(PebbleCompactorModeOverlay),
	)
	require.NoError(t, err)
	defer func() { _ = cleanupFull() }()
	fullOut, err := cFull.Compact(ctx)
	require.NoError(t, err)

	incGrants := grantOutcome(t, ctx, incOut.FilePath, incOut.SyncID)
	fullGrants := grantOutcome(t, ctx, fullOut.FilePath, fullOut.SyncID)
	require.Equal(t, fullGrants, incGrants, "derive-fallback incremental must equal full expansion")
	hasGrant(t, incGrants, "ent-c|user|bob")
}

// artifactGraph loads the graph sidecar from a compacted artifact.
func artifactGraph(t *testing.T, ctx context.Context, path, syncID string) *expand.EntitlementGraph {
	t.Helper()
	store, err := dotc1z.NewStore(ctx, path, dotc1z.WithReadOnly(true), dotc1z.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	defer store.Close(ctx)
	g, err := sdksync.GraphFromStore(ctx, store, syncID)
	require.NoError(t, err)
	return g
}

// TestCompactor_ArtifactCarriesGraphSidecar: the compacted artifact self-carries
// its post-expansion graph for the next incremental run — on the incremental
// path (updated clone), on the decline->full path (fresh graph via preserve),
// and not at all when incremental wasn't requested.
func TestCompactor_ArtifactCarriesGraphSidecar(t *testing.T) {
	ctx := context.Background()

	// (a) Incremental success: sidecar = base graph + the increment's new edge.
	entries := buildIncrementalFixtures(t, ctx, t.TempDir()) // increment adds ent-a -> ent-b
	cInc, cleanupInc, err := NewCompactor(ctx, t.TempDir(), entries,
		WithTmpDir(t.TempDir()),
		WithEngine(c1zstore.EnginePebble),
		WithIncrementalExpansion(), // ent-b -> ent-c
	)
	require.NoError(t, err)
	defer func() { _ = cleanupInc() }()
	incOut, err := cInc.Compact(ctx)
	require.NoError(t, err)
	require.True(t, cInc.incrementalExpansionRan)

	g := artifactGraph(t, ctx, incOut.FilePath, incOut.SyncID)
	require.NotNil(t, g, "incremental artifact must carry its graph sidecar")
	require.NotNil(t, g.GetNode("ent-a"), "sidecar graph must include the increment's new edge source")
	require.Len(t, g.Edges, 2, "base edge + folded-in new edge")
	require.True(t, g.IsExpanded(), "persisted graph must describe completed expansion")
	require.True(t, g.HasNoCycles, "persisted graph must record the successful cycle check")

	// (b) Decline -> full (narrowed edge) with incremental requested: the full
	// path preserves a fresh graph so the chain heals after the fallback.
	declEntries := buildSpecChangeFixtures(t, ctx, t.TempDir(), false, true) // deep -> shallow
	cDecl, cleanupDecl, err := NewCompactor(ctx, t.TempDir(), declEntries,
		WithTmpDir(t.TempDir()),
		WithEngine(c1zstore.EnginePebble),
		WithIncrementalExpansion(),
	)
	require.NoError(t, err)
	defer func() { _ = cleanupDecl() }()
	declOut, err := cDecl.Compact(ctx)
	require.NoError(t, err)
	require.False(t, cDecl.incrementalExpansionRan, "narrowed edge must decline to full")

	g = artifactGraph(t, ctx, declOut.FilePath, declOut.SyncID)
	require.NotNil(t, g, "declined-to-full artifact must still carry a fresh graph sidecar")
	require.NotNil(t, g.GetNode("ent-b"))

	// (c) Incremental not requested: no sidecar.
	plainEntries := buildIncrementalFixtures(t, ctx, t.TempDir())
	cPlain, cleanupPlain, err := NewCompactor(ctx, t.TempDir(), plainEntries,
		WithTmpDir(t.TempDir()),
		WithEngine(c1zstore.EnginePebble),
	)
	require.NoError(t, err)
	defer func() { _ = cleanupPlain() }()
	plainOut, err := cPlain.Compact(ctx)
	require.NoError(t, err)

	g = artifactGraph(t, ctx, plainOut.FilePath, plainOut.SyncID)
	require.Nil(t, g, "artifact without incremental opt-in must carry no graph sidecar")
}

func TestCompactor_IncrementalDroppedEdgeDeclinesToFull(t *testing.T) {
	ctx := context.Background()
	incEntries := buildDroppedEdgeFixtures(t, ctx, t.TempDir())
	cInc, cleanupInc, err := NewCompactor(ctx, t.TempDir(), incEntries,
		WithTmpDir(t.TempDir()), WithEngine(c1zstore.EnginePebble),
		WithIncrementalExpansion())
	require.NoError(t, err)
	defer func() { _ = cleanupInc() }()
	incOut, err := cInc.Compact(ctx)
	require.NoError(t, err)
	require.False(t, cInc.incrementalExpansionRan, "a dropped edge must decline to full expansion")

	fullEntries := buildDroppedEdgeFixtures(t, ctx, t.TempDir())
	cFull, cleanupFull, err := NewCompactor(ctx, t.TempDir(), fullEntries,
		WithTmpDir(t.TempDir()), WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	defer func() { _ = cleanupFull() }()
	fullOut, err := cFull.Compact(ctx)
	require.NoError(t, err)
	require.Equal(t,
		grantOutcome(t, ctx, fullOut.FilePath, fullOut.SyncID),
		grantOutcome(t, ctx, incOut.FilePath, incOut.SyncID),
		"dropped-edge fallback must equal full expansion")
}

func TestCompactor_IncrementalRejectsWrongBaseSyncAndInvalidGraph(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name   string
		mutate func(*testing.T, context.Context, []*CompactableSync)
	}{
		{
			name: "wrong base sync",
			mutate: func(t *testing.T, ctx context.Context, entries []*CompactableSync) {
				overwriteFixtureGraph(t, ctx, entries[0].FilePath, "another-sync", baseGraphForFixtures(t, ctx))
			},
		},
		{
			name: "inconsistent adjacency",
			mutate: func(t *testing.T, ctx context.Context, entries []*CompactableSync) {
				g := baseGraphForFixtures(t, ctx)
				src, dst := g.GetNode("ent-b"), g.GetNode("ent-c")
				delete(g.SourcesToDestinations[src.Id], dst.Id)
				overwriteFixtureGraph(t, ctx, entries[0].FilePath, entries[0].SyncID, g)
			},
		},
		{
			name: "malformed sidecar",
			mutate: func(t *testing.T, ctx context.Context, entries []*CompactableSync) {
				overwriteFixtureGraphRaw(t, ctx, entries[0].FilePath, []byte("{"))
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			entries := buildIncrementalFixtures(t, ctx, t.TempDir())
			tc.mutate(t, ctx, entries)
			c, cleanup, err := NewCompactor(ctx, t.TempDir(), entries,
				WithTmpDir(t.TempDir()), WithEngine(c1zstore.EnginePebble),
				WithIncrementalExpansion())
			require.NoError(t, err)
			defer func() { _ = cleanup() }()
			out, err := c.Compact(ctx)
			require.NoError(t, err)
			require.False(t, c.incrementalExpansionRan)
			grants := grantOutcome(t, ctx, out.FilePath, out.SyncID)
			hasGrant(t, grants, "ent-c|user|sam")
		})
	}
}

func TestCompactor_AbsentPartialMembershipIsNotADeletion(t *testing.T) {
	ctx := context.Background()
	baseFixtures := buildIncrementalFixtures(t, ctx, t.TempDir())
	base := baseFixtures[0]

	incEmpty := buildEmptyPartial(t, ctx, filepath.Join(t.TempDir(), "inc-empty.c1z"))
	inc, cleanupInc, err := NewCompactor(ctx, t.TempDir(), []*CompactableSync{base, incEmpty},
		WithTmpDir(t.TempDir()), WithEngine(c1zstore.EnginePebble), WithIncrementalExpansion())
	require.NoError(t, err)
	defer func() { _ = cleanupInc() }()
	incOut, err := inc.Compact(ctx)
	require.NoError(t, err)
	require.True(t, inc.incrementalExpansionRan)

	fullEmpty := buildEmptyPartial(t, ctx, filepath.Join(t.TempDir(), "full-empty.c1z"))
	full, cleanupFull, err := NewCompactor(ctx, t.TempDir(), []*CompactableSync{base, fullEmpty},
		WithTmpDir(t.TempDir()), WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	defer func() { _ = cleanupFull() }()
	fullOut, err := full.Compact(ctx)
	require.NoError(t, err)

	require.Equal(t,
		grantOutcome(t, ctx, fullOut.FilePath, fullOut.SyncID),
		grantOutcome(t, ctx, incOut.FilePath, incOut.SyncID))
	grants := grantOutcome(t, ctx, incOut.FilePath, incOut.SyncID)
	hasGrant(t, grants, "ent-c|user|mandy")
}

func TestCompactor_IncrementalExistingCollapsedCycleFallsBack(t *testing.T) {
	ctx := context.Background()
	entries := buildIncrementalFixtures(t, ctx, t.TempDir())
	g := expand.NewEntitlementGraph(ctx)
	g.AddEntitlementID("cycle-a")
	g.AddEntitlementID("cycle-b")
	require.NoError(t, g.AddEdge(ctx, "cycle-a", "cycle-b", false, nil))
	require.NoError(t, g.AddEdge(ctx, "cycle-b", "cycle-a", false, nil))
	require.NoError(t, g.FixCycles(ctx))
	g.Loaded = true
	g.MarkExpansionComplete()
	require.True(t, g.HasCollapsedCycles())
	overwriteFixtureGraph(t, ctx, entries[0].FilePath, entries[0].SyncID, g)

	c, cleanup, err := NewCompactor(ctx, t.TempDir(), entries,
		WithTmpDir(t.TempDir()), WithEngine(c1zstore.EnginePebble),
		WithIncrementalExpansion())
	require.NoError(t, err)
	defer func() { _ = cleanup() }()
	out, err := c.Compact(ctx)
	require.NoError(t, err)
	require.False(t, c.incrementalExpansionRan)
	grants := grantOutcome(t, ctx, out.FilePath, out.SyncID)
	hasGrant(t, grants, "ent-c|user|sam")
}

func TestCompactor_IncrementalGraphReusedByNextGeneration(t *testing.T) {
	ctx := context.Background()
	firstEntries := buildIncrementalFixtures(t, ctx, t.TempDir())
	first, cleanupFirst, err := NewCompactor(ctx, t.TempDir(), firstEntries,
		WithTmpDir(t.TempDir()), WithEngine(c1zstore.EnginePebble),
		WithIncrementalExpansion())
	require.NoError(t, err)
	defer func() { _ = cleanupFirst() }()
	firstOut, err := first.Compact(ctx)
	require.NoError(t, err)
	require.True(t, first.incrementalExpansionRan)
	baseGraph := artifactGraph(t, ctx, firstOut.FilePath, firstOut.SyncID)
	require.NoError(t, baseGraph.ValidateCompleted())

	incPartial := buildMemberPartial(t, ctx, filepath.Join(t.TempDir(), "inc-next.c1z"), "ent-b", "grpB", "zoe")
	incEntries := []*CompactableSync{{FilePath: firstOut.FilePath, SyncID: firstOut.SyncID}, incPartial}
	inc, cleanupInc, err := NewCompactor(ctx, t.TempDir(), incEntries,
		WithTmpDir(t.TempDir()), WithEngine(c1zstore.EnginePebble),
		WithIncrementalExpansion())
	require.NoError(t, err)
	defer func() { _ = cleanupInc() }()
	incOut, err := inc.Compact(ctx)
	require.NoError(t, err)
	require.True(t, inc.incrementalExpansionRan)

	fullPartial := buildMemberPartial(t, ctx, filepath.Join(t.TempDir(), "full-next.c1z"), "ent-b", "grpB", "zoe")
	fullEntries := []*CompactableSync{{FilePath: firstOut.FilePath, SyncID: firstOut.SyncID}, fullPartial}
	full, cleanupFull, err := NewCompactor(ctx, t.TempDir(), fullEntries,
		WithTmpDir(t.TempDir()), WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	defer func() { _ = cleanupFull() }()
	fullOut, err := full.Compact(ctx)
	require.NoError(t, err)

	require.Equal(t,
		grantOutcome(t, ctx, fullOut.FilePath, fullOut.SyncID),
		grantOutcome(t, ctx, incOut.FilePath, incOut.SyncID))
	nextGraph := artifactGraph(t, ctx, incOut.FilePath, incOut.SyncID)
	require.NoError(t, nextGraph.ValidateCompleted())
}
