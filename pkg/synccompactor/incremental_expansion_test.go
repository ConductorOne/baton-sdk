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
	g := v2.Grant_builder{
		Id:          batonGrant.NewGrantID(sourceGroup, destEnt),
		Entitlement: destEnt,
		Principal:   sourceGroup,
	}.Build()
	g.SetAnnotations(annotations.New(v2.GrantExpandable_builder{
		EntitlementIds: []string{sourceEntID},
	}.Build()))
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
func buildIncrementalFixtures(t *testing.T, ctx context.Context, dir string) []*CompactableSync {
	return buildIncrementalFixturesEngine(t, ctx, dir, c1zstore.EnginePebble)
}

// buildIncrementalFixturesEngine is buildIncrementalFixtures with a chosen
// storage engine, so the SQLite degrade path can be exercised too.
func buildIncrementalFixturesEngine(t *testing.T, ctx context.Context, dir string, engine c1zstore.Engine) []*CompactableSync {
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
func baseGraphForFixtures(t *testing.T, ctx context.Context) *expand.EntitlementGraph {
	t.Helper()
	g := expand.NewEntitlementGraph(ctx)
	g.AddEntitlementID("ent-b")
	g.AddEntitlementID("ent-c")
	require.NoError(t, g.AddEdge(ctx, "ent-b", "ent-c", false, nil))
	g.MarkEdgeExpanded("ent-b", "ent-c")
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
		WithIncrementalExpansion(baseGraphForFixtures(t, ctx)),
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
		WithIncrementalExpansion(baseGraphForFixtures(t, ctx)),
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
func buildSpecChangeFixtures(t *testing.T, ctx context.Context, dir string, baseShallow, incShallow bool) []*CompactableSync {
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

func specChangeBaseGraph(t *testing.T, ctx context.Context, shallow bool) *expand.EntitlementGraph {
	t.Helper()
	g := expand.NewEntitlementGraph(ctx)
	g.AddEntitlementID("ent-b")
	g.AddEntitlementID("ent-c")
	require.NoError(t, g.AddEdge(ctx, "ent-b", "ent-c", shallow, nil))
	g.MarkEdgeExpanded("ent-b", "ent-c")
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
		WithIncrementalExpansion(specChangeBaseGraph(t, ctx, true)), // base edge is shallow
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
		WithIncrementalExpansion(specChangeBaseGraph(t, ctx, false)), // base edge is deep
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

// TestCompactor_IncrementalDoesNotMutateBaseGraph (U1): running the incremental
// expansion must not mutate the caller-held base graph, so a retry with the same
// graph can't treat never-expanded edges as already present.
func TestCompactor_IncrementalDoesNotMutateBaseGraph(t *testing.T) {
	ctx := context.Background()

	entries := buildIncrementalFixtures(t, ctx, t.TempDir()) // increment adds ent-a -> ent-b
	base := baseGraphForFixtures(t, ctx)                     // holds only ent-b -> ent-c
	edgesBefore := len(base.Edges)
	nodesBefore := len(base.Nodes)

	c, cleanup, err := NewCompactor(ctx, t.TempDir(), entries,
		WithTmpDir(t.TempDir()),
		WithEngine(c1zstore.EnginePebble),
		WithIncrementalExpansion(base),
	)
	require.NoError(t, err)
	defer func() { _ = cleanup() }()

	_, err = c.Compact(ctx)
	require.NoError(t, err)
	require.True(t, c.incrementalExpansionRan)

	// The caller's graph is untouched — the new ent-a -> ent-b edge went into a
	// clone, not this graph.
	require.Equal(t, edgesBefore, len(base.Edges), "base graph edges must be unchanged")
	require.Equal(t, nodesBefore, len(base.Nodes), "base graph nodes must be unchanged")
	require.Nil(t, base.GetNode("ent-a"), "new edge's node must not leak into the caller's graph")
}

// buildDanglingRefFixtures builds a base (ent-b -> ent-c, mandy) and a single
// increment whose only change is a rule grant whose SOURCE entitlement
// ("ent-ghost") is absent from the merged set — a dangling ref, which both
// paths must skip.
func buildDanglingRefFixtures(t *testing.T, ctx context.Context, dir string) []*CompactableSync {
	t.Helper()
	grpB, grpC := grp("grpB"), grp("grpC")
	mandy := usr("mandy")
	entB, entC := ent("ent-b", grpB), ent("ent-c", grpC)
	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()
	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()

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
	require.NoError(t, base.Close(ctx))

	incPath := filepath.Join(dir, "inc.c1z")
	inc, err := dotc1z.NewStore(ctx, incPath, dotc1z.WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	incSyncID, err := inc.StartNewSync(ctx, connectorstore.SyncTypePartial, "")
	require.NoError(t, err)
	require.NoError(t, inc.PutResourceTypes(ctx, userRT, groupRT))
	require.NoError(t, inc.PutResources(ctx, grpB, grpC))
	require.NoError(t, inc.PutEntitlements(ctx, entC))
	// rule grant: members of ent-ghost (which does not exist) get ent-c.
	require.NoError(t, inc.PutGrants(ctx, ruleGrant(entC, grpB, "ent-ghost")))
	require.NoError(t, inc.EndSync(ctx))
	require.NoError(t, inc.Close(ctx))

	return []*CompactableSync{
		{FilePath: basePath, SyncID: baseSyncID},
		{FilePath: incPath, SyncID: incSyncID},
	}
}

// TestCompactor_IncrementalDanglingRefMatchesFull (#11a): an increment with a
// grant referencing an entitlement absent from the merged set is skipped by
// both paths; incremental (skip-with-warn) must equal full (NotFound skip).
func TestCompactor_IncrementalDanglingRefMatchesFull(t *testing.T) {
	ctx := context.Background()

	incEntries := buildDanglingRefFixtures(t, ctx, t.TempDir())
	cInc, cleanupInc, err := NewCompactor(ctx, t.TempDir(), incEntries,
		WithTmpDir(t.TempDir()),
		WithEngine(c1zstore.EnginePebble),
		WithIncrementalExpansion(baseGraphForFixtures(t, ctx)),
	)
	require.NoError(t, err)
	defer func() { _ = cleanupInc() }()
	incOut, err := cInc.Compact(ctx)
	require.NoError(t, err)
	require.True(t, cInc.incrementalExpansionRan, "dangling ref must skip, not fall back")

	fullEntries := buildDanglingRefFixtures(t, ctx, t.TempDir())
	cFull, cleanupFull, err := NewCompactor(ctx, t.TempDir(), fullEntries,
		WithTmpDir(t.TempDir()), WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	defer func() { _ = cleanupFull() }()
	fullOut, err := cFull.Compact(ctx)
	require.NoError(t, err)

	incGrants := grantOutcome(t, ctx, incOut.FilePath, incOut.SyncID)
	fullGrants := grantOutcome(t, ctx, fullOut.FilePath, fullOut.SyncID)
	require.Equal(t, fullGrants, incGrants, "dangling-ref incremental must equal full")
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
		WithIncrementalExpansion(baseGraphForFixtures(t, ctx)),
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

	// (2) by_principal index is populated and covers sam (written incrementally).
	eng, ok := enginepkg.AsEngine(store)
	require.True(t, ok, "expected a pebble engine")
	it, err := eng.DB().NewIter(&pebble.IterOptions{
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
		WithIncrementalExpansion(baseGraphForFixtures(t, ctx)),
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
}
