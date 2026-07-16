package synccompactor

import (
	"context"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/sync/expand"
	batonGrant "github.com/conductorone/baton-sdk/pkg/types/grant"
)

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

func memberGrant(e *v2.Entitlement, principal *v2.Resource) *v2.Grant {
	return v2.Grant_builder{
		Id:          batonGrant.NewGrantID(principal, e),
		Entitlement: e,
		Principal:   principal,
	}.Build()
}

func expandedGrant(e *v2.Entitlement, principal *v2.Resource, sourceEntID string) *v2.Grant {
	g := memberGrant(e, principal)
	g.SetSources(v2.GrantSources_builder{
		Sources: map[string]*v2.GrantSources_GrantSource{sourceEntID: {}},
	}.Build())
	return g
}

// buildIncrementalFixtures writes a base (full, pre-expanded) c1z and an
// increment (partial) c1z into dir, returning the compactable entries.
func buildIncrementalFixtures(t *testing.T, ctx context.Context, dir string) []*CompactableSync {
	t.Helper()

	grpA, grpB, grpC := grp("grpA"), grp("grpB"), grp("grpC")
	sam, mandy := usr("sam"), usr("mandy")
	entA, entB, entC := ent("ent-a", grpA), ent("ent-b", grpB), ent("ent-c", grpC)

	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()
	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()

	// --- base: full, already expanded (ent-b -> ent-c, mandy on both) ---
	basePath := filepath.Join(dir, "base.c1z")
	base, err := dotc1z.NewStore(ctx, basePath, dotc1z.WithEngine(c1zstore.EnginePebble))
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
	inc, err := dotc1z.NewStore(ctx, incPath, dotc1z.WithEngine(c1zstore.EnginePebble))
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
// "entitlement|principalType|principalResource" keys.
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
			set[g.GetEntitlement().GetId()+"|"+pid.GetResourceType()+"|"+pid.GetResource()] = struct{}{}
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
		WithIncrementalExpansion(baseGraphForFixtures(t, ctx), nil),
	)
	require.NoError(t, err)
	defer func() { _ = cleanupInc() }()
	incOut, err := cInc.Compact(ctx)
	require.NoError(t, err)
	require.NotNil(t, incOut)

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
	require.Contains(t, incGrants, "ent-b|user|sam")
	require.Contains(t, incGrants, "ent-c|user|sam")
	require.Contains(t, incGrants, "ent-c|user|mandy")
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
		WithIncrementalExpansion(baseGraphForFixtures(t, ctx), []string{"ent-b"}),
	)
	require.NoError(t, err)
	defer func() { _ = cleanupInc() }()
	incOut, err := cInc.Compact(ctx)
	require.NoError(t, err)
	require.NotNil(t, incOut)

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
	require.Contains(t, incGrants, "ent-b|user|bob")
	require.Contains(t, incGrants, "ent-c|user|bob")
	require.Contains(t, incGrants, "ent-c|user|mandy")
}
