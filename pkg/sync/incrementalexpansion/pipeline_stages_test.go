package incrementalexpansion_test

import (
	"context"
	"path/filepath"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/sync/incrementalexpansion"
	batonEntitlement "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	batonGrant "github.com/conductorone/baton-sdk/pkg/types/grant"
	"github.com/stretchr/testify/require"
)

// setupExpandableSync creates a C1File with the given expandable grants and
// returns the file and sync ID. Caller must close the file.
func setupExpandableSync(
	ctx context.Context,
	t *testing.T,
	tmpDir, name string,
	resourceTypes []*v2.ResourceType,
	resources []*v2.Resource,
	entitlements []*v2.Entitlement,
	grants []*v2.Grant,
	opts ...dotc1z.C1ZOption,
) (*dotc1z.C1File, string) {
	t.Helper()
	dbPath := filepath.Join(tmpDir, name+".c1z")
	c1f, err := dotc1z.NewC1ZFile(ctx, dbPath, opts...)
	require.NoError(t, err)

	syncID, err := c1f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	if len(resourceTypes) > 0 {
		require.NoError(t, c1f.PutResourceTypes(ctx, resourceTypes...))
	}
	if len(resources) > 0 {
		require.NoError(t, c1f.PutResources(ctx, resources...))
	}
	if len(entitlements) > 0 {
		require.NoError(t, c1f.PutEntitlements(ctx, entitlements...))
	}
	if len(grants) > 0 {
		require.NoError(t, c1f.PutGrants(ctx, grants...))
	}
	require.NoError(t, c1f.EndSync(ctx))
	require.NoError(t, c1f.SetSupportsDiff(ctx, syncID))

	return c1f, syncID
}

// -----------------------------------------------------------------------
// EdgeDeltaFromDiffSyncs unit tests
// -----------------------------------------------------------------------

func TestEdgeDelta_EmptySyncs(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()

	walNormal := []dotc1z.C1ZOption{dotc1z.WithPragma("journal_mode", "WAL"), dotc1z.WithPragma("locking_mode", "normal")}
	walOpts := []dotc1z.C1ZOption{dotc1z.WithPragma("journal_mode", "WAL")}

	oldFile, oldSyncID := setupExpandableSync(ctx, t, tmpDir, "old_empty",
		[]*v2.ResourceType{groupRT, userRT}, nil, nil, nil, walNormal...)
	defer oldFile.Close(ctx)

	newFile, newSyncID := setupExpandableSync(ctx, t, tmpDir, "new_empty",
		[]*v2.ResourceType{groupRT, userRT}, nil, nil, nil, walOpts...)
	defer newFile.Close(ctx)

	// Generate diff with no grants in either sync
	attached, err := newFile.AttachFile(oldFile, "attached")
	require.NoError(t, err)
	upsertsSyncID, deletionsSyncID, err := attached.GenerateSyncDiffFromFile(ctx, oldSyncID, newSyncID)
	require.NoError(t, err)
	_, err = attached.DetachFile("attached")
	require.NoError(t, err)

	delta, err := incrementalexpansion.EdgeDeltaFromDiffSyncs(ctx, newFile, upsertsSyncID, deletionsSyncID)
	require.NoError(t, err)
	require.Empty(t, delta.Added)
	require.Empty(t, delta.Removed)
}

func TestEdgeDelta_AddsAndRemoves(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()

	walNormal := []dotc1z.C1ZOption{dotc1z.WithPragma("journal_mode", "WAL"), dotc1z.WithPragma("locking_mode", "normal")}
	walOpts := []dotc1z.C1ZOption{dotc1z.WithPragma("journal_mode", "WAL")}

	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(), DisplayName: "G1"}.Build()
	g2 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g2"}.Build(), DisplayName: "G2"}.Build()
	g3 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g3"}.Build(), DisplayName: "G3"}.Build()

	e1 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g1, "member"), Resource: g1, Slug: "member", DisplayName: "member"}.Build()
	e2 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g2, "member"), Resource: g2, Slug: "member", DisplayName: "member"}.Build()
	e3 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g3, "member"), Resource: g3, Slug: "member", DisplayName: "member"}.Build()

	// OLD: edge E1→E2
	grantE1E2 := v2.Grant_builder{
		Id: batonGrant.NewGrantID(g1, e2), Entitlement: e2, Principal: g1,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds: []string{e1.GetId()}, ResourceTypeIds: []string{"user"},
		}.Build()),
	}.Build()

	oldFile, oldSyncID := setupExpandableSync(ctx, t, tmpDir, "old_delta",
		[]*v2.ResourceType{groupRT, userRT},
		[]*v2.Resource{g1, g2, g3},
		[]*v2.Entitlement{e1, e2, e3},
		[]*v2.Grant{grantE1E2},
		walNormal...)
	defer oldFile.Close(ctx)

	// NEW: edge E2→E3 (E1→E2 removed)
	grantE2E3 := v2.Grant_builder{
		Id: batonGrant.NewGrantID(g2, e3), Entitlement: e3, Principal: g2,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds: []string{e2.GetId()}, ResourceTypeIds: []string{"user"},
		}.Build()),
	}.Build()

	newFile, newSyncID := setupExpandableSync(ctx, t, tmpDir, "new_delta",
		[]*v2.ResourceType{groupRT, userRT},
		[]*v2.Resource{g1, g2, g3},
		[]*v2.Entitlement{e1, e2, e3},
		[]*v2.Grant{grantE2E3},
		walOpts...)
	defer newFile.Close(ctx)

	attached, err := newFile.AttachFile(oldFile, "attached")
	require.NoError(t, err)
	upsertsSyncID, deletionsSyncID, err := attached.GenerateSyncDiffFromFile(ctx, oldSyncID, newSyncID)
	require.NoError(t, err)
	_, err = attached.DetachFile("attached")
	require.NoError(t, err)

	delta, err := incrementalexpansion.EdgeDeltaFromDiffSyncs(ctx, newFile, upsertsSyncID, deletionsSyncID)
	require.NoError(t, err)

	require.Len(t, delta.Added, 1, "should have one added edge (E2→E3)")
	require.Len(t, delta.Removed, 1, "should have one removed edge (E1→E2)")

	for _, e := range delta.Added {
		require.Equal(t, e2.GetId(), e.SrcEntitlementID)
		require.Equal(t, e3.GetId(), e.DstEntitlementID)
	}
	for _, e := range delta.Removed {
		require.Equal(t, e1.GetId(), e.SrcEntitlementID)
		require.Equal(t, e2.GetId(), e.DstEntitlementID)
	}
}

func TestEdgeDelta_ShallowFlagCreatesDistinctKeys(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()

	walNormal := []dotc1z.C1ZOption{dotc1z.WithPragma("journal_mode", "WAL"), dotc1z.WithPragma("locking_mode", "normal")}
	walOpts := []dotc1z.C1ZOption{dotc1z.WithPragma("journal_mode", "WAL")}

	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(), DisplayName: "G1"}.Build()
	g2 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g2"}.Build(), DisplayName: "G2"}.Build()

	e1 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g1, "member"), Resource: g1, Slug: "member", DisplayName: "member"}.Build()
	e2 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g2, "member"), Resource: g2, Slug: "member", DisplayName: "member"}.Build()

	// OLD: shallow edge
	nestingID := batonGrant.NewGrantID(g1, e2)
	grantShallow := v2.Grant_builder{
		Id: nestingID, Entitlement: e2, Principal: g1,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds: []string{e1.GetId()}, Shallow: true, ResourceTypeIds: []string{"user"},
		}.Build()),
	}.Build()

	oldFile, oldSyncID := setupExpandableSync(ctx, t, tmpDir, "old_shallow_key",
		[]*v2.ResourceType{groupRT, userRT},
		[]*v2.Resource{g1, g2},
		[]*v2.Entitlement{e1, e2},
		[]*v2.Grant{grantShallow},
		walNormal...)
	defer oldFile.Close(ctx)

	// NEW: deep edge (same src/dst)
	grantDeep := v2.Grant_builder{
		Id: nestingID, Entitlement: e2, Principal: g1,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds: []string{e1.GetId()}, Shallow: false, ResourceTypeIds: []string{"user"},
		}.Build()),
	}.Build()

	newFile, newSyncID := setupExpandableSync(ctx, t, tmpDir, "new_shallow_key",
		[]*v2.ResourceType{groupRT, userRT},
		[]*v2.Resource{g1, g2},
		[]*v2.Entitlement{e1, e2},
		[]*v2.Grant{grantDeep},
		walOpts...)
	defer newFile.Close(ctx)

	attached, err := newFile.AttachFile(oldFile, "attached")
	require.NoError(t, err)
	upsertsSyncID, deletionsSyncID, err := attached.GenerateSyncDiffFromFile(ctx, oldSyncID, newSyncID)
	require.NoError(t, err)
	_, err = attached.DetachFile("attached")
	require.NoError(t, err)

	delta, err := incrementalexpansion.EdgeDeltaFromDiffSyncs(ctx, newFile, upsertsSyncID, deletionsSyncID)
	require.NoError(t, err)

	// Shallow→deep means the old shallow edge is removed and a new deep edge is added
	require.Len(t, delta.Added, 1, "new deep edge should be in Added")
	require.Len(t, delta.Removed, 1, "old shallow edge should be in Removed")

	for _, e := range delta.Added {
		require.False(t, e.Shallow)
	}
	for _, e := range delta.Removed {
		require.True(t, e.Shallow)
	}
}

// -----------------------------------------------------------------------
// AffectedEntitlements unit tests
// -----------------------------------------------------------------------

func TestAffectedEntitlements_NilDelta(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	c1f, syncID := setupExpandableSync(ctx, t, tmpDir, "nil_delta",
		[]*v2.ResourceType{groupRT}, nil, nil, nil)
	defer c1f.Close(ctx)

	affected, err := incrementalexpansion.AffectedEntitlements(ctx, c1f, syncID, nil)
	require.NoError(t, err)
	require.Empty(t, affected)
}

func TestAffectedEntitlements_EmptyDelta(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	c1f, syncID := setupExpandableSync(ctx, t, tmpDir, "empty_delta",
		[]*v2.ResourceType{groupRT}, nil, nil, nil)
	defer c1f.Close(ctx)

	delta := &incrementalexpansion.EdgeDelta{
		Added:   map[string]incrementalexpansion.Edge{},
		Removed: map[string]incrementalexpansion.Edge{},
	}

	affected, err := incrementalexpansion.AffectedEntitlements(ctx, c1f, syncID, delta)
	require.NoError(t, err)
	require.Empty(t, affected)
}

func TestAffectedEntitlements_ForwardClosure(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()

	// Chain: E1→E2→E3→E4. Delta touches E1. Should affect E1, E2, E3, E4.
	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(), DisplayName: "G1"}.Build()
	g2 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g2"}.Build(), DisplayName: "G2"}.Build()
	g3 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g3"}.Build(), DisplayName: "G3"}.Build()
	g4 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g4"}.Build(), DisplayName: "G4"}.Build()

	e1 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g1, "member"), Resource: g1, Slug: "member", DisplayName: "member"}.Build()
	e2 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g2, "member"), Resource: g2, Slug: "member", DisplayName: "member"}.Build()
	e3 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g3, "member"), Resource: g3, Slug: "member", DisplayName: "member"}.Build()
	e4 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g4, "member"), Resource: g4, Slug: "member", DisplayName: "member"}.Build()

	mkEdge := func(src *v2.Entitlement, dst *v2.Entitlement, principal *v2.Resource) *v2.Grant {
		return v2.Grant_builder{
			Id: batonGrant.NewGrantID(principal, dst), Entitlement: dst, Principal: principal,
			Annotations: annotations.New(v2.GrantExpandable_builder{
				EntitlementIds: []string{src.GetId()}, ResourceTypeIds: []string{"user"},
			}.Build()),
		}.Build()
	}

	c1f, syncID := setupExpandableSync(ctx, t, tmpDir, "chain",
		[]*v2.ResourceType{groupRT, userRT},
		[]*v2.Resource{g1, g2, g3, g4},
		[]*v2.Entitlement{e1, e2, e3, e4},
		[]*v2.Grant{mkEdge(e1, e2, g1), mkEdge(e2, e3, g2), mkEdge(e3, e4, g3)})
	defer c1f.Close(ctx)

	edge := incrementalexpansion.Edge{SrcEntitlementID: e1.GetId(), DstEntitlementID: e2.GetId()}
	delta := &incrementalexpansion.EdgeDelta{
		Added: map[string]incrementalexpansion.Edge{edge.Key(): edge},
	}

	affected, err := incrementalexpansion.AffectedEntitlements(ctx, c1f, syncID, delta)
	require.NoError(t, err)

	for _, eid := range []string{e1.GetId(), e2.GetId(), e3.GetId(), e4.GetId()} {
		_, ok := affected[eid]
		require.True(t, ok, "entitlement %s should be in affected set", eid)
	}
}

// -----------------------------------------------------------------------
// MarkNeedsExpansionForAffectedEdges unit tests
// -----------------------------------------------------------------------

func TestMarkNeedsExpansion_EmptyAffected(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	c1f, syncID := setupExpandableSync(ctx, t, tmpDir, "empty_affected",
		[]*v2.ResourceType{groupRT}, nil, nil, nil)
	defer c1f.Close(ctx)

	err := incrementalexpansion.MarkNeedsExpansionForAffectedEdges(ctx, c1f, syncID, map[string]struct{}{})
	require.NoError(t, err)
}

func TestMarkNeedsExpansion_MarksCorrectGrants(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()

	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(), DisplayName: "G1"}.Build()
	g2 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g2"}.Build(), DisplayName: "G2"}.Build()
	g3 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g3"}.Build(), DisplayName: "G3"}.Build()

	e1 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g1, "member"), Resource: g1, Slug: "member", DisplayName: "member"}.Build()
	e2 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g2, "member"), Resource: g2, Slug: "member", DisplayName: "member"}.Build()
	e3 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g3, "member"), Resource: g3, Slug: "member", DisplayName: "member"}.Build()

	// Edge E1→E2 (should be marked if E1 or E2 is affected)
	grantE1E2 := v2.Grant_builder{
		Id: batonGrant.NewGrantID(g1, e2), Entitlement: e2, Principal: g1,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds: []string{e1.GetId()}, ResourceTypeIds: []string{"user"},
		}.Build()),
	}.Build()

	// Edge E2→E3 (should NOT be marked if only E1 is affected, since neither src(E2) nor dst(E3) is in the set)
	grantE2E3 := v2.Grant_builder{
		Id: batonGrant.NewGrantID(g2, e3), Entitlement: e3, Principal: g2,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds: []string{e2.GetId()}, ResourceTypeIds: []string{"user"},
		}.Build()),
	}.Build()

	c1f, syncID := setupExpandableSync(ctx, t, tmpDir, "mark_grants",
		[]*v2.ResourceType{groupRT, userRT},
		[]*v2.Resource{g1, g2, g3},
		[]*v2.Entitlement{e1, e2, e3},
		[]*v2.Grant{grantE1E2, grantE2E3})
	defer c1f.Close(ctx)

	// Clear initial needs_expansion flags set during insert so we can test marking in isolation.
	require.NoError(t, c1f.ClearNeedsExpansionForSync(ctx, syncID))

	// Only E1 is affected
	affected := map[string]struct{}{e1.GetId(): {}}
	err := incrementalexpansion.MarkNeedsExpansionForAffectedEdges(ctx, c1f, syncID, affected)
	require.NoError(t, err)

	// Check which grants have needs_expansion=1
	resp, err := c1f.ListGrantsInternal(ctx, connectorstore.GrantListOptions{
		Mode:               connectorstore.GrantListModeExpansionNeedsOnly,
		SyncID:             syncID,
		NeedsExpansionOnly: true,
	})
	require.NoError(t, err)

	markedIDs := make(map[string]bool)
	for _, row := range resp.Rows {
		markedIDs[row.Expansion.GrantExternalID] = true
	}

	require.True(t, markedIDs[grantE1E2.GetId()], "E1→E2 should be marked (source E1 is affected)")
	require.False(t, markedIDs[grantE2E3.GetId()], "E2→E3 should NOT be marked (neither E2 nor E3 is in affected set)")
}

// -----------------------------------------------------------------------
// InvalidateRemovedEdges unit tests
// -----------------------------------------------------------------------

func TestInvalidateRemovedEdges_NilDelta(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	c1f, syncID := setupExpandableSync(ctx, t, tmpDir, "nil_delta_inv",
		[]*v2.ResourceType{groupRT}, nil, nil, nil)
	defer c1f.Close(ctx)

	err := incrementalexpansion.InvalidateRemovedEdges(ctx, c1f, syncID, nil)
	require.NoError(t, err)
}

func TestInvalidateRemovedEdges_EmptyRemoved(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	c1f, syncID := setupExpandableSync(ctx, t, tmpDir, "empty_removed",
		[]*v2.ResourceType{groupRT}, nil, nil, nil)
	defer c1f.Close(ctx)

	delta := &incrementalexpansion.EdgeDelta{Removed: map[string]incrementalexpansion.Edge{}}
	err := incrementalexpansion.InvalidateRemovedEdges(ctx, c1f, syncID, delta)
	require.NoError(t, err)
}

// -----------------------------------------------------------------------
// InvalidateChangedSourceEntitlements unit tests
// -----------------------------------------------------------------------

func TestInvalidateChangedSources_EmptySet(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	c1f, syncID := setupExpandableSync(ctx, t, tmpDir, "empty_changed",
		[]*v2.ResourceType{groupRT}, nil, nil, nil)
	defer c1f.Close(ctx)

	err := incrementalexpansion.InvalidateChangedSourceEntitlements(ctx, c1f, syncID, map[string]struct{}{})
	require.NoError(t, err)
}

// -----------------------------------------------------------------------
// GrantImmutable vs non-immutable sourceless behavior
// -----------------------------------------------------------------------

func TestIncrementalExpansion_ImmutableSourcelessGrantDeleted(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	oldPath := filepath.Join(tmpDir, "old_immutable.c1z")
	newPath := filepath.Join(tmpDir, "new_immutable.c1z")
	expectedPath := filepath.Join(tmpDir, "expected_immutable.c1z")

	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()

	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(), DisplayName: "G1"}.Build()
	g2 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g2"}.Build(), DisplayName: "G2"}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(), DisplayName: "U1"}.Build()

	e1 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g1, "member"), Resource: g1, Slug: "member", DisplayName: "member"}.Build()
	e2 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g2, "member"), Resource: g2, Slug: "member", DisplayName: "member"}.Build()

	grantU1E1 := v2.Grant_builder{Id: batonGrant.NewGrantID(u1, e1), Entitlement: e1, Principal: u1}.Build()

	nestingID := batonGrant.NewGrantID(g1, e2)
	grantG1E2 := v2.Grant_builder{
		Id:          nestingID,
		Entitlement: e2,
		Principal:   g1,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds:  []string{e1.GetId()},
			Shallow:         false,
			ResourceTypeIds: []string{"user"},
		}.Build()),
	}.Build()

	// OLD (expanded): U1→E1, edge E1→E2 → derived U1→E2 (with GrantImmutable)
	oldFile, err := dotc1z.NewC1ZFile(ctx, oldPath, dotc1z.WithPragma("journal_mode", "WAL"), dotc1z.WithPragma("locking_mode", "normal"))
	require.NoError(t, err)
	defer oldFile.Close(ctx)

	oldSyncID, err := oldFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, oldFile.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, oldFile.PutResources(ctx, g1, g2, u1))
	require.NoError(t, oldFile.PutEntitlements(ctx, e1, e2))
	require.NoError(t, oldFile.PutGrants(ctx, grantU1E1, grantG1E2))
	require.NoError(t, oldFile.EndSync(ctx))
	require.NoError(t, runFullExpansion(ctx, oldFile, oldSyncID))

	// Verify the expansion created a derived grant (it should have GrantImmutable annotation)
	oldGrants, err := loadGrantSourcesByKey(ctx, oldFile, oldSyncID)
	require.NoError(t, err)
	require.Contains(t, oldGrants, "group:g2:member|user|u1", "derived grant U1→E2 should exist after expansion")

	// NEW: remove the nesting grant (edge removed), keep direct grant
	newFile, err := dotc1z.NewC1ZFile(ctx, newPath)
	require.NoError(t, err)
	defer newFile.Close(ctx)

	newSyncID, err := newFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, newFile.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, newFile.PutResources(ctx, g1, g2, u1))
	require.NoError(t, newFile.PutEntitlements(ctx, e1, e2))

	require.NoError(t, oldFile.SetSyncID(ctx, ""))
	require.NoError(t, oldFile.ViewSync(ctx, oldSyncID))
	grantsToCopy, err := listGrantsWithStoredExpansion(ctx, oldFile)
	require.NoError(t, err)
	for _, g := range grantsToCopy {
		if g.GetId() == nestingID {
			continue
		}
		require.NoError(t, newFile.PutGrants(ctx, g))
	}
	require.NoError(t, newFile.EndSync(ctx))

	attached, err := newFile.AttachFile(oldFile, "attached")
	require.NoError(t, err)
	upsertsSyncID, deletionsSyncID, err := attached.GenerateSyncDiffFromFile(ctx, oldSyncID, newSyncID)
	require.NoError(t, err)
	_, err = attached.DetachFile("attached")
	require.NoError(t, err)

	require.NoError(t, incrementalexpansion.ApplyIncrementalExpansionFromDiff(ctx, newFile, newSyncID, upsertsSyncID, deletionsSyncID))

	// EXPECTED: derived U1→E2 (GrantImmutable) should be DELETED since it became sourceless
	expectedFile, err := dotc1z.NewC1ZFile(ctx, expectedPath)
	require.NoError(t, err)
	defer expectedFile.Close(ctx)
	expectedSyncID, err := expectedFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, expectedFile.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, expectedFile.PutResources(ctx, g1, g2, u1))
	require.NoError(t, expectedFile.PutEntitlements(ctx, e1, e2))
	require.NoError(t, expectedFile.PutGrants(ctx, grantU1E1))
	require.NoError(t, expectedFile.EndSync(ctx))
	require.NoError(t, runFullExpansion(ctx, expectedFile, expectedSyncID))

	got, err := loadGrantSourcesByKey(ctx, newFile, newSyncID)
	require.NoError(t, err)
	want, err := loadGrantSourcesByKey(ctx, expectedFile, expectedSyncID)
	require.NoError(t, err)
	require.Equal(t, want, got)

	// The derived grant U1→E2 should be gone completely
	require.NotContains(t, got, "group:g2:member|user|u1", "immutable derived grant should be deleted when sourceless")
}

// TestIncrementalExpansion_NonImmutableSourcelessGrantKept verifies that when a
// non-immutable (direct) grant loses all expansion sources, it persists with
// sources=nil rather than being deleted.
func TestIncrementalExpansion_NonImmutableSourcelessGrantKept(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	oldPath := filepath.Join(tmpDir, "old_nonimmutable.c1z")
	newPath := filepath.Join(tmpDir, "new_nonimmutable.c1z")
	expectedPath := filepath.Join(tmpDir, "expected_nonimmutable.c1z")

	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()

	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(), DisplayName: "G1"}.Build()
	g2 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g2"}.Build(), DisplayName: "G2"}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(), DisplayName: "U1"}.Build()

	e1 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g1, "member"), Resource: g1, Slug: "member", DisplayName: "member"}.Build()
	e2 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g2, "member"), Resource: g2, Slug: "member", DisplayName: "member"}.Build()

	// U1 is a DIRECT member of both E1 and E2
	grantU1E1 := v2.Grant_builder{Id: batonGrant.NewGrantID(u1, e1), Entitlement: e1, Principal: u1}.Build()
	grantU1E2 := v2.Grant_builder{Id: batonGrant.NewGrantID(u1, e2), Entitlement: e2, Principal: u1}.Build()

	nestingID := batonGrant.NewGrantID(g1, e2)
	grantG1E2 := v2.Grant_builder{
		Id:          nestingID,
		Entitlement: e2,
		Principal:   g1,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds:  []string{e1.GetId()},
			Shallow:         false,
			ResourceTypeIds: []string{"user"},
		}.Build()),
	}.Build()

	// OLD: U1→E1 (direct), U1→E2 (direct), edge E1→E2
	// After expansion, U1→E2 acquires sources (it was both direct AND derived)
	oldFile, err := dotc1z.NewC1ZFile(ctx, oldPath, dotc1z.WithPragma("journal_mode", "WAL"), dotc1z.WithPragma("locking_mode", "normal"))
	require.NoError(t, err)
	defer oldFile.Close(ctx)

	oldSyncID, err := oldFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, oldFile.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, oldFile.PutResources(ctx, g1, g2, u1))
	require.NoError(t, oldFile.PutEntitlements(ctx, e1, e2))
	require.NoError(t, oldFile.PutGrants(ctx, grantU1E1, grantU1E2, grantG1E2))
	require.NoError(t, oldFile.EndSync(ctx))
	require.NoError(t, runFullExpansion(ctx, oldFile, oldSyncID))

	// NEW: remove the edge — U1→E2 should remain (it's a direct grant) but lose sources
	newFile, err := dotc1z.NewC1ZFile(ctx, newPath)
	require.NoError(t, err)
	defer newFile.Close(ctx)

	newSyncID, err := newFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, newFile.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, newFile.PutResources(ctx, g1, g2, u1))
	require.NoError(t, newFile.PutEntitlements(ctx, e1, e2))

	require.NoError(t, oldFile.SetSyncID(ctx, ""))
	require.NoError(t, oldFile.ViewSync(ctx, oldSyncID))
	grantsToCopy, err := listGrantsWithStoredExpansion(ctx, oldFile)
	require.NoError(t, err)
	for _, g := range grantsToCopy {
		if g.GetId() == nestingID {
			continue
		}
		require.NoError(t, newFile.PutGrants(ctx, g))
	}
	require.NoError(t, newFile.EndSync(ctx))

	attached, err := newFile.AttachFile(oldFile, "attached")
	require.NoError(t, err)
	upsertsSyncID, deletionsSyncID, err := attached.GenerateSyncDiffFromFile(ctx, oldSyncID, newSyncID)
	require.NoError(t, err)
	_, err = attached.DetachFile("attached")
	require.NoError(t, err)

	require.NoError(t, incrementalexpansion.ApplyIncrementalExpansionFromDiff(ctx, newFile, newSyncID, upsertsSyncID, deletionsSyncID))

	// EXPECTED: U1→E2 persists (non-immutable direct grant) with sources=nil
	expectedFile, err := dotc1z.NewC1ZFile(ctx, expectedPath)
	require.NoError(t, err)
	defer expectedFile.Close(ctx)
	expectedSyncID, err := expectedFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, expectedFile.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, expectedFile.PutResources(ctx, g1, g2, u1))
	require.NoError(t, expectedFile.PutEntitlements(ctx, e1, e2))
	require.NoError(t, expectedFile.PutGrants(ctx, grantU1E1, grantU1E2))
	require.NoError(t, expectedFile.EndSync(ctx))
	require.NoError(t, runFullExpansion(ctx, expectedFile, expectedSyncID))

	got, err := loadGrantSourcesByKey(ctx, newFile, newSyncID)
	require.NoError(t, err)
	want, err := loadGrantSourcesByKey(ctx, expectedFile, expectedSyncID)
	require.NoError(t, err)
	require.Equal(t, want, got)

	// The direct grant U1→E2 must still exist
	require.Contains(t, got, "group:g2:member|user|u1", "non-immutable direct grant should persist when sourceless")
}
