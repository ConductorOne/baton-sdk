package incrementalexpansion

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	batonEntitlement "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	batonGrant "github.com/conductorone/baton-sdk/pkg/types/grant"
	"github.com/stretchr/testify/require"
)

// -----------------------------------------------------------------------
// EdgeDelta unit tests (cross-DB attached queries)
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

	attached, err := newFile.AttachFile(oldFile, "attached")
	require.NoError(t, err)
	addedDefs, err := attached.ComputeAddedExpandableGrants(ctx, oldSyncID, newSyncID)
	require.NoError(t, err)
	removedDefs, err := attached.ComputeRemovedExpandableGrants(ctx, oldSyncID, newSyncID)
	require.NoError(t, err)
	_, err = attached.DetachFile("attached")
	require.NoError(t, err)

	delta := edgeDeltaFromExpandableGrants(addedDefs, removedDefs)
	require.Empty(t, delta.added)
	require.Empty(t, delta.removed)
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
	addedDefs, err := attached.ComputeAddedExpandableGrants(ctx, oldSyncID, newSyncID)
	require.NoError(t, err)
	removedDefs, err := attached.ComputeRemovedExpandableGrants(ctx, oldSyncID, newSyncID)
	require.NoError(t, err)
	_, err = attached.DetachFile("attached")
	require.NoError(t, err)

	delta := edgeDeltaFromExpandableGrants(addedDefs, removedDefs)

	require.Len(t, delta.added, 1, "should have one added edge (E2→E3)")
	require.Len(t, delta.removed, 1, "should have one removed edge (E1→E2)")

	for _, e := range delta.added {
		require.Equal(t, e2.GetId(), e.srcEntitlementID)
		require.Equal(t, e3.GetId(), e.dstEntitlementID)
	}
	for _, e := range delta.removed {
		require.Equal(t, e1.GetId(), e.srcEntitlementID)
		require.Equal(t, e2.GetId(), e.dstEntitlementID)
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
	addedDefs, err := attached.ComputeAddedExpandableGrants(ctx, oldSyncID, newSyncID)
	require.NoError(t, err)
	removedDefs, err := attached.ComputeRemovedExpandableGrants(ctx, oldSyncID, newSyncID)
	require.NoError(t, err)
	_, err = attached.DetachFile("attached")
	require.NoError(t, err)

	delta := edgeDeltaFromExpandableGrants(addedDefs, removedDefs)

	require.Len(t, delta.added, 1, "new deep edge should be in added")
	require.Len(t, delta.removed, 1, "old shallow edge should be in removed")

	for _, e := range delta.added {
		require.False(t, e.shallow)
	}
	for _, e := range delta.removed {
		require.True(t, e.shallow)
	}
}

// -----------------------------------------------------------------------
// affectedEntitlements unit tests
// -----------------------------------------------------------------------

func TestAffectedEntitlements_NilDelta(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	c1f, syncID := setupExpandableSync(ctx, t, tmpDir, "nil_delta",
		[]*v2.ResourceType{groupRT}, nil, nil, nil)
	defer c1f.Close(ctx)

	affected, err := affectedEntitlements(ctx, c1f, syncID, nil)
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

	delta := &edgeDelta{
		added:   map[string]edge{},
		removed: map[string]edge{},
	}

	affected, err := affectedEntitlements(ctx, c1f, syncID, delta)
	require.NoError(t, err)
	require.Empty(t, affected)
}

func TestAffectedEntitlements_ForwardClosure(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()

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

	e := edge{srcEntitlementID: e1.GetId(), dstEntitlementID: e2.GetId()}
	delta := &edgeDelta{
		added: map[string]edge{e.key(): e},
	}

	affected, err := affectedEntitlements(ctx, c1f, syncID, delta)
	require.NoError(t, err)

	for _, eid := range []string{e1.GetId(), e2.GetId(), e3.GetId(), e4.GetId()} {
		_, ok := affected[eid]
		require.True(t, ok, "entitlement %s should be in affected set", eid)
	}
}

// -----------------------------------------------------------------------
// markNeedsExpansionForAffectedEdges unit tests
// -----------------------------------------------------------------------

func TestMarkNeedsExpansion_EmptyAffected(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	c1f, syncID := setupExpandableSync(ctx, t, tmpDir, "empty_affected",
		[]*v2.ResourceType{groupRT}, nil, nil, nil)
	defer c1f.Close(ctx)

	err := markNeedsExpansionForAffectedEdges(ctx, c1f, syncID, map[string]struct{}{})
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

	grantE1E2 := v2.Grant_builder{
		Id: batonGrant.NewGrantID(g1, e2), Entitlement: e2, Principal: g1,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds: []string{e1.GetId()}, ResourceTypeIds: []string{"user"},
		}.Build()),
	}.Build()

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

	require.NoError(t, c1f.ClearNeedsExpansionForSync(ctx, syncID))

	affected := map[string]struct{}{e1.GetId(): {}}
	err := markNeedsExpansionForAffectedEdges(ctx, c1f, syncID, affected)
	require.NoError(t, err)

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

// Regression coverage for "expand too much" bugs:
// - verifies mixed edge delta shapes (add/remove/source-list/shallow/filter changes),
// - verifies the exact affected entitlement closure,
// - verifies exact dirty expandable grants (no extras) are marked for re-expansion.
func TestMarkNeedsExpansion_WhitelistExpectedDirtySubgraph(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()

	walNormal := []dotc1z.C1ZOption{dotc1z.WithPragma("journal_mode", "WAL"), dotc1z.WithPragma("locking_mode", "normal")}
	walOpts := []dotc1z.C1ZOption{dotc1z.WithPragma("journal_mode", "WAL")}

	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(), DisplayName: "G1"}.Build()
	g2 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g2"}.Build(), DisplayName: "G2"}.Build()
	g3 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g3"}.Build(), DisplayName: "G3"}.Build()
	g4 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g4"}.Build(), DisplayName: "G4"}.Build()
	g5 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g5"}.Build(), DisplayName: "G5"}.Build()
	g6 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g6"}.Build(), DisplayName: "G6"}.Build()
	g7 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g7"}.Build(), DisplayName: "G7"}.Build()
	g8 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g8"}.Build(), DisplayName: "G8"}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(), DisplayName: "U1"}.Build()
	u2 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u2"}.Build(), DisplayName: "U2"}.Build()

	e1 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g1, "member"), Resource: g1, Slug: "member", DisplayName: "member"}.Build()
	e2m := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g2, "member"), Resource: g2, Slug: "member", DisplayName: "member"}.Build()
	e2a := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g2, "admin"), Resource: g2, Slug: "admin", DisplayName: "admin"}.Build()
	e3 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g3, "member"), Resource: g3, Slug: "member", DisplayName: "member"}.Build()
	e4 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g4, "member"), Resource: g4, Slug: "member", DisplayName: "member"}.Build()
	e5 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g5, "member"), Resource: g5, Slug: "member", DisplayName: "member"}.Build()
	e6 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g6, "member"), Resource: g6, Slug: "member", DisplayName: "member"}.Build()
	e7 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g7, "member"), Resource: g7, Slug: "member", DisplayName: "member"}.Build()
	e8 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g8, "member"), Resource: g8, Slug: "member", DisplayName: "member"}.Build()

	// Old graph edges:
	// e1->e2m, e2m->e3(deep), e3->e4(filter=user), e2m->e5, e5->e6, e7->e8(unaffected branch)
	oldEdge12 := v2.Grant_builder{
		Id: batonGrant.NewGrantID(g1, e2m), Entitlement: e2m, Principal: g1,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds: []string{e1.GetId()}, ResourceTypeIds: []string{"user"},
		}.Build()),
	}.Build()
	oldEdge23 := v2.Grant_builder{
		Id: batonGrant.NewGrantID(g2, e3), Entitlement: e3, Principal: g2,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds: []string{e2m.GetId()}, Shallow: false, ResourceTypeIds: []string{"user"},
		}.Build()),
	}.Build()
	oldEdge34 := v2.Grant_builder{
		Id: batonGrant.NewGrantID(g3, e4), Entitlement: e4, Principal: g3,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds: []string{e3.GetId()}, ResourceTypeIds: []string{"user"},
		}.Build()),
	}.Build()
	oldEdge25 := v2.Grant_builder{
		Id: batonGrant.NewGrantID(g2, e5), Entitlement: e5, Principal: g2,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds: []string{e2m.GetId()}, ResourceTypeIds: []string{"user"},
		}.Build()),
	}.Build()
	oldEdge56 := v2.Grant_builder{
		Id: batonGrant.NewGrantID(g5, e6), Entitlement: e6, Principal: g5,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds: []string{e5.GetId()}, ResourceTypeIds: []string{"user"},
		}.Build()),
	}.Build()
	oldEdge78 := v2.Grant_builder{
		Id: batonGrant.NewGrantID(g7, e8), Entitlement: e8, Principal: g7,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds: []string{e7.GetId()}, ResourceTypeIds: []string{"user"},
		}.Build()),
	}.Build()
	oldDirectU1E1 := v2.Grant_builder{Id: batonGrant.NewGrantID(u1, e1), Entitlement: e1, Principal: u1}.Build()

	oldFile, oldSyncID := setupExpandableSync(ctx, t, tmpDir, "old_whitelist",
		[]*v2.ResourceType{groupRT, userRT},
		[]*v2.Resource{g1, g2, g3, g4, g5, g6, g7, g8, u1, u2},
		[]*v2.Entitlement{e1, e2m, e2a, e3, e4, e5, e6, e7, e8},
		[]*v2.Grant{oldDirectU1E1, oldEdge12, oldEdge23, oldEdge34, oldEdge25, oldEdge56, oldEdge78},
		walNormal...)
	defer oldFile.Close(ctx)

	// New graph edges:
	// - keep e1->e2m
	// - toggle e2m->e3 to shallow=true
	// - change e3->e4 filter to user+group
	// - source-list replacement e2m->e5 -> e2a->e5
	// - remove e5->e6
	// - add e4->e6
	// - keep unaffected e7->e8
	newEdge12 := oldEdge12
	newEdge23 := v2.Grant_builder{
		Id: oldEdge23.GetId(), Entitlement: e3, Principal: g2,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds: []string{e2m.GetId()}, Shallow: true, ResourceTypeIds: []string{"user"},
		}.Build()),
	}.Build()
	newEdge34 := v2.Grant_builder{
		Id: oldEdge34.GetId(), Entitlement: e4, Principal: g3,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds: []string{e3.GetId()}, ResourceTypeIds: []string{"user", "group"},
		}.Build()),
	}.Build()
	newEdge25 := v2.Grant_builder{
		Id: oldEdge25.GetId(), Entitlement: e5, Principal: g2,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds: []string{e2a.GetId()}, ResourceTypeIds: []string{"user"},
		}.Build()),
	}.Build()
	newEdge46 := v2.Grant_builder{
		Id: batonGrant.NewGrantID(g4, e6), Entitlement: e6, Principal: g4,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds: []string{e4.GetId()}, ResourceTypeIds: []string{"user"},
		}.Build()),
	}.Build()
	newEdge78 := oldEdge78
	// Source-grant change independent of edge-definition changes: add direct grant on e1.
	newDirectU2E1 := v2.Grant_builder{Id: batonGrant.NewGrantID(u2, e1), Entitlement: e1, Principal: u2}.Build()

	newFile, newSyncID := setupExpandableSync(ctx, t, tmpDir, "new_whitelist",
		[]*v2.ResourceType{groupRT, userRT},
		[]*v2.Resource{g1, g2, g3, g4, g5, g6, g7, g8, u1, u2},
		[]*v2.Entitlement{e1, e2m, e2a, e3, e4, e5, e6, e7, e8},
		[]*v2.Grant{oldDirectU1E1, newDirectU2E1, newEdge12, newEdge23, newEdge34, newEdge25, newEdge46, newEdge78},
		walOpts...)
	defer newFile.Close(ctx)

	attached, err := newFile.AttachFile(oldFile, "attached")
	require.NoError(t, err)
	addedDefs, err := attached.ComputeAddedExpandableGrants(ctx, oldSyncID, newSyncID)
	require.NoError(t, err)
	removedDefs, err := attached.ComputeRemovedExpandableGrants(ctx, oldSyncID, newSyncID)
	require.NoError(t, err)
	_, err = attached.DetachFile("attached")
	require.NoError(t, err)

	delta := edgeDeltaFromExpandableGrants(addedDefs, removedDefs)
	require.Len(t, delta.added, 4, "expected added edges: e2a->e5, e2m->e3(shallow), e3->e4(filter), e4->e6")
	require.Len(t, delta.removed, 4, "expected removed edges: e2m->e5, e2m->e3(old shallow), e3->e4(old filter), e5->e6")

	affected, err := affectedEntitlements(ctx, newFile, newSyncID, delta)
	require.NoError(t, err)

	wantAffected := map[string]struct{}{
		e2a.GetId(): {},
		e5.GetId():  {},
		e4.GetId():  {},
		e6.GetId():  {},
		e2m.GetId(): {},
		e3.GetId():  {},
	}
	require.Equal(t, wantAffected, affected, "affected entitlement closure should match exact whitelist")

	// Add a changed source seed (grant-set change on e1) as Apply/Run do.
	affected[e1.GetId()] = struct{}{}

	require.NoError(t, newFile.ClearNeedsExpansionForSync(ctx, newSyncID))
	require.NoError(t, markNeedsExpansionForAffectedEdges(ctx, newFile, newSyncID, affected))

	resp, err := newFile.ListGrantsInternal(ctx, connectorstore.GrantListOptions{
		Mode:               connectorstore.GrantListModeExpansionNeedsOnly,
		SyncID:             newSyncID,
		NeedsExpansionOnly: true,
	})
	require.NoError(t, err)

	gotMarked := make(map[string]struct{}, len(resp.Rows))
	for _, row := range resp.Rows {
		gotMarked[row.Expansion.GrantExternalID] = struct{}{}
	}
	wantMarked := map[string]struct{}{
		newEdge12.GetId(): {},
		newEdge23.GetId(): {},
		newEdge34.GetId(): {},
		newEdge25.GetId(): {},
		newEdge46.GetId(): {},
	}
	require.Equal(t, wantMarked, gotMarked, "dirty expandable grants should match exact whitelist (no over-marking)")
}

// -----------------------------------------------------------------------
// invalidateRemovedEdges unit tests
// -----------------------------------------------------------------------

func TestInvalidateRemovedEdges_NilDelta(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	c1f, syncID := setupExpandableSync(ctx, t, tmpDir, "nil_delta_inv",
		[]*v2.ResourceType{groupRT}, nil, nil, nil)
	defer c1f.Close(ctx)

	err := invalidateRemovedEdges(ctx, c1f, syncID, nil)
	require.NoError(t, err)
}

func TestInvalidateRemovedEdges_EmptyRemoved(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	c1f, syncID := setupExpandableSync(ctx, t, tmpDir, "empty_removed",
		[]*v2.ResourceType{groupRT}, nil, nil, nil)
	defer c1f.Close(ctx)

	delta := &edgeDelta{removed: map[string]edge{}}
	err := invalidateRemovedEdges(ctx, c1f, syncID, delta)
	require.NoError(t, err)
}

// -----------------------------------------------------------------------
// invalidateChangedSourceEntitlements unit tests
// -----------------------------------------------------------------------

func TestInvalidateChangedSources_EmptySet(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	c1f, syncID := setupExpandableSync(ctx, t, tmpDir, "empty_changed",
		[]*v2.ResourceType{groupRT}, nil, nil, nil)
	defer c1f.Close(ctx)

	err := invalidateChangedSourceEntitlements(ctx, c1f, syncID, map[string]struct{}{})
	require.NoError(t, err)
}

func TestInvalidateChangedSources_NonEmptyRemovesDownstreamDerived(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	dbPath := filepath.Join(tmpDir, "changed_sources_nonempty.c1z")
	c1f, err := dotc1z.NewC1ZFile(ctx, dbPath)
	require.NoError(t, err)
	defer c1f.Close(ctx)

	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()

	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(), DisplayName: "G1"}.Build()
	g2 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g2"}.Build(), DisplayName: "G2"}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(), DisplayName: "U1"}.Build()

	e1 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g1, "member"), Resource: g1, Slug: "member", DisplayName: "member"}.Build()
	e2 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g2, "member"), Resource: g2, Slug: "member", DisplayName: "member"}.Build()

	grantU1E1 := v2.Grant_builder{Id: batonGrant.NewGrantID(u1, e1), Entitlement: e1, Principal: u1}.Build()
	grantG1E2 := v2.Grant_builder{
		Id:          batonGrant.NewGrantID(g1, e2),
		Entitlement: e2,
		Principal:   g1,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds:  []string{e1.GetId()},
			Shallow:         false,
			ResourceTypeIds: []string{"user"},
		}.Build()),
	}.Build()

	syncID, err := c1f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, c1f.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, c1f.PutResources(ctx, g1, g2, u1))
	require.NoError(t, c1f.PutEntitlements(ctx, e1, e2))
	require.NoError(t, c1f.PutGrants(ctx, grantU1E1, grantG1E2))
	require.NoError(t, c1f.EndSync(ctx))
	require.NoError(t, runFullExpansion(ctx, c1f, syncID))

	before, err := loadGrantSourcesByKey(ctx, c1f, syncID)
	require.NoError(t, err)
	require.Contains(t, before, "group:g2:member|user|u1", "derived grant should exist pre-invalidation")

	err = invalidateChangedSourceEntitlements(ctx, c1f, syncID, map[string]struct{}{e1.GetId(): {}})
	require.NoError(t, err)

	after, err := loadGrantSourcesByKey(ctx, c1f, syncID)
	require.NoError(t, err)
	require.NotContains(t, after, "group:g2:member|user|u1", "changed source invalidation should remove downstream derived grant")
}

// -----------------------------------------------------------------------
// markNeedsExpansionForAffectedEdges chunk boundary
// -----------------------------------------------------------------------

func TestMarkNeedsExpansion_ChunkBoundary5001(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()

	srcResource := v2.Resource_builder{
		Id:          v2.ResourceId_builder{ResourceType: "group", Resource: "g-src"}.Build(),
		DisplayName: "G-Src",
	}.Build()
	srcEnt := v2.Entitlement_builder{
		Id:          batonEntitlement.NewEntitlementID(srcResource, "member"),
		Resource:    srcResource,
		Slug:        "member",
		DisplayName: "member",
	}.Build()

	resources := []*v2.Resource{srcResource}
	entitlements := []*v2.Entitlement{srcEnt}
	grants := make([]*v2.Grant, 0, 5001)

	for i := 0; i < 5001; i++ {
		dstResource := v2.Resource_builder{
			Id:          v2.ResourceId_builder{ResourceType: "group", Resource: fmt.Sprintf("g-dst-%d", i)}.Build(),
			DisplayName: fmt.Sprintf("G-Dst-%d", i),
		}.Build()
		dstEnt := v2.Entitlement_builder{
			Id:          batonEntitlement.NewEntitlementID(dstResource, "member"),
			Resource:    dstResource,
			Slug:        "member",
			DisplayName: "member",
		}.Build()

		resources = append(resources, dstResource)
		entitlements = append(entitlements, dstEnt)
		grants = append(grants, v2.Grant_builder{
			Id:          batonGrant.NewGrantID(srcResource, dstEnt),
			Entitlement: dstEnt,
			Principal:   srcResource,
			Annotations: annotations.New(v2.GrantExpandable_builder{
				EntitlementIds:  []string{srcEnt.GetId()},
				Shallow:         false,
				ResourceTypeIds: []string{"user"},
			}.Build()),
		}.Build())
	}

	c1f, syncID := setupExpandableSync(ctx, t, tmpDir, "chunk_5001",
		[]*v2.ResourceType{groupRT, userRT},
		resources,
		entitlements,
		grants,
	)
	defer c1f.Close(ctx)

	require.NoError(t, c1f.ClearNeedsExpansionForSync(ctx, syncID))

	err := markNeedsExpansionForAffectedEdges(ctx, c1f, syncID, map[string]struct{}{srcEnt.GetId(): {}})
	require.NoError(t, err)

	pageToken := ""
	count := 0
	for {
		resp, err := c1f.ListGrantsInternal(ctx, connectorstore.GrantListOptions{
			Mode:               connectorstore.GrantListModeExpansionNeedsOnly,
			SyncID:             syncID,
			PageToken:          pageToken,
			NeedsExpansionOnly: true,
		})
		require.NoError(t, err)
		count += len(resp.Rows)
		if resp.NextPageToken == "" {
			break
		}
		pageToken = resp.NextPageToken
	}
	require.Equal(t, 5001, count, "all expandable grants should be marked across chunk boundary")
}

// -----------------------------------------------------------------------
// EdgeDelta pagination
// -----------------------------------------------------------------------

func TestEdgeDelta_PaginationOver10000Rows(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()

	walNormal := []dotc1z.C1ZOption{dotc1z.WithPragma("journal_mode", "WAL"), dotc1z.WithPragma("locking_mode", "normal")}
	walOpts := []dotc1z.C1ZOption{dotc1z.WithPragma("journal_mode", "WAL")}
	oldFile, oldSyncID := setupExpandableSync(ctx, t, tmpDir, "old_paged_delta",
		[]*v2.ResourceType{groupRT, userRT}, nil, nil, nil, walNormal...)
	defer oldFile.Close(ctx)

	srcResource := v2.Resource_builder{
		Id:          v2.ResourceId_builder{ResourceType: "group", Resource: "g-src"}.Build(),
		DisplayName: "G-Src",
	}.Build()
	srcEnt := v2.Entitlement_builder{
		Id:          batonEntitlement.NewEntitlementID(srcResource, "member"),
		Resource:    srcResource,
		Slug:        "member",
		DisplayName: "member",
	}.Build()

	resources := []*v2.Resource{srcResource}
	entitlements := []*v2.Entitlement{srcEnt}
	grants := make([]*v2.Grant, 0, 10001)
	for i := 0; i < 10001; i++ {
		dstResource := v2.Resource_builder{
			Id:          v2.ResourceId_builder{ResourceType: "group", Resource: fmt.Sprintf("g-delta-%d", i)}.Build(),
			DisplayName: fmt.Sprintf("G-Delta-%d", i),
		}.Build()
		dstEnt := v2.Entitlement_builder{
			Id:          batonEntitlement.NewEntitlementID(dstResource, "member"),
			Resource:    dstResource,
			Slug:        "member",
			DisplayName: "member",
		}.Build()

		resources = append(resources, dstResource)
		entitlements = append(entitlements, dstEnt)
		grants = append(grants, v2.Grant_builder{
			Id:          batonGrant.NewGrantID(srcResource, dstEnt),
			Entitlement: dstEnt,
			Principal:   srcResource,
			Annotations: annotations.New(v2.GrantExpandable_builder{
				EntitlementIds:  []string{srcEnt.GetId()},
				Shallow:         false,
				ResourceTypeIds: []string{"user"},
			}.Build()),
		}.Build())
	}

	newFile, newSyncID := setupExpandableSync(ctx, t, tmpDir, "new_paged_delta",
		[]*v2.ResourceType{groupRT, userRT},
		resources,
		entitlements,
		grants,
		walOpts...)
	defer newFile.Close(ctx)

	attached, err := newFile.AttachFile(oldFile, "attached")
	require.NoError(t, err)
	addedDefs, err := attached.ComputeAddedExpandableGrants(ctx, oldSyncID, newSyncID)
	require.NoError(t, err)
	removedDefs, err := attached.ComputeRemovedExpandableGrants(ctx, oldSyncID, newSyncID)
	require.NoError(t, err)
	_, err = attached.DetachFile("attached")
	require.NoError(t, err)

	delta := edgeDeltaFromExpandableGrants(addedDefs, removedDefs)
	require.Len(t, delta.added, 10001, "all edges should be read across page boundaries")
	require.Len(t, delta.removed, 0)
}

// -----------------------------------------------------------------------
// expandDirtySubgraph unit tests
// -----------------------------------------------------------------------

func TestExpandDirtySubgraph_MissingSourceEntitlementSkipped(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.c1z")

	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()

	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(), DisplayName: "G1"}.Build()
	g2 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g2"}.Build(), DisplayName: "G2"}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(), DisplayName: "U1"}.Build()

	e2 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g2, "member"), Resource: g2, Slug: "member", DisplayName: "member"}.Build()

	grantU1E2 := v2.Grant_builder{
		Id: batonGrant.NewGrantID(u1, e2), Entitlement: e2, Principal: u1,
	}.Build()

	nonExistentEntitlementID := batonEntitlement.NewEntitlementID(g1, "member")
	grantG1E2 := v2.Grant_builder{
		Id: batonGrant.NewGrantID(g1, e2), Entitlement: e2, Principal: g1,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds:  []string{nonExistentEntitlementID},
			Shallow:         false,
			ResourceTypeIds: []string{"user"},
		}.Build()),
	}.Build()

	c1f, err := dotc1z.NewC1ZFile(ctx, dbPath)
	require.NoError(t, err)
	defer c1f.Close(ctx)

	syncID, err := c1f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, c1f.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, c1f.PutResources(ctx, g1, g2, u1))
	require.NoError(t, c1f.PutEntitlements(ctx, e2))
	require.NoError(t, c1f.PutGrants(ctx, grantU1E2, grantG1E2))
	require.NoError(t, c1f.EndSync(ctx))

	require.NoError(t, c1f.SetNeedsExpansionForGrants(ctx, syncID, []string{grantG1E2.GetId()}, true))

	err = expandDirtySubgraph(ctx, c1f, syncID)
	require.NoError(t, err, "expandDirtySubgraph should skip missing source entitlements gracefully")

	require.NoError(t, c1f.SetSyncID(ctx, ""))
	require.NoError(t, c1f.ViewSync(ctx, syncID))

	grantCount := 0
	pageToken := ""
	for {
		resp, err := c1f.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{PageToken: pageToken}.Build())
		require.NoError(t, err)
		grantCount += len(resp.GetList())
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	require.Equal(t, 2, grantCount, "should have exactly 2 grants (no derived grants from missing source)")
}

func TestExpandDirtySubgraph_ValidSourceEntitlementWorks(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.c1z")

	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()

	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(), DisplayName: "G1"}.Build()
	g2 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g2"}.Build(), DisplayName: "G2"}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(), DisplayName: "U1"}.Build()

	e1 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g1, "member"), Resource: g1, Slug: "member", DisplayName: "member"}.Build()
	e2 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g2, "member"), Resource: g2, Slug: "member", DisplayName: "member"}.Build()

	grantU1E1 := v2.Grant_builder{
		Id: batonGrant.NewGrantID(u1, e1), Entitlement: e1, Principal: u1,
	}.Build()

	grantG1E2 := v2.Grant_builder{
		Id: batonGrant.NewGrantID(g1, e2), Entitlement: e2, Principal: g1,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds:  []string{e1.GetId()},
			Shallow:         false,
			ResourceTypeIds: []string{"user"},
		}.Build()),
	}.Build()

	c1f, err := dotc1z.NewC1ZFile(ctx, dbPath)
	require.NoError(t, err)
	defer c1f.Close(ctx)

	syncID, err := c1f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, c1f.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, c1f.PutResources(ctx, g1, g2, u1))
	require.NoError(t, c1f.PutEntitlements(ctx, e1, e2))
	require.NoError(t, c1f.PutGrants(ctx, grantU1E1, grantG1E2))
	require.NoError(t, c1f.EndSync(ctx))

	require.NoError(t, c1f.SetNeedsExpansionForGrants(ctx, syncID, []string{grantG1E2.GetId()}, true))

	err = expandDirtySubgraph(ctx, c1f, syncID)
	require.NoError(t, err)

	require.NoError(t, c1f.SetSyncID(ctx, ""))
	require.NoError(t, c1f.ViewSync(ctx, syncID))

	grantCount := 0
	pageToken := ""
	for {
		resp, err := c1f.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{PageToken: pageToken}.Build())
		require.NoError(t, err)
		grantCount += len(resp.GetList())
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	require.Equal(t, 3, grantCount, "should have 3 grants (2 original + 1 derived)")
}
