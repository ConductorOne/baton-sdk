package incrementalexpansion_test

import (
	"context"
	"path/filepath"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/sync/expand"
	"github.com/conductorone/baton-sdk/pkg/sync/incrementalexpansion"
	batonEntitlement "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	batonGrant "github.com/conductorone/baton-sdk/pkg/types/grant"
	"github.com/stretchr/testify/require"
)

func runFullExpansion(ctx context.Context, c1f *dotc1z.C1File, syncID string) error {
	if err := c1f.SetSyncID(ctx, syncID); err != nil {
		return err
	}

	// Mark the sync as supporting diff operations (SQL-layer data is ready).
	if err := c1f.SetSupportsDiff(ctx, syncID); err != nil {
		return err
	}

	graph := expand.NewEntitlementGraph(ctx)

	pageToken := ""
	for {
		defs, next, err := c1f.ListExpandableGrants(
			ctx,
			dotc1z.WithExpandableGrantsSyncID(syncID),
			dotc1z.WithExpandableGrantsPageToken(pageToken),
			dotc1z.WithExpandableGrantsNeedsExpansionOnly(false),
		)
		if err != nil {
			return err
		}
		for _, def := range defs {
			for _, src := range def.SrcEntitlementIDs {
				graph.AddEntitlementID(def.DstEntitlementID)
				graph.AddEntitlementID(src)
				if err := graph.AddEdge(ctx, src, def.DstEntitlementID, def.Shallow, def.PrincipalResourceTypeIDs); err != nil {
					return err
				}
			}
		}
		if next == "" {
			break
		}
		pageToken = next
	}

	graph.Loaded = true
	if err := graph.FixCycles(ctx); err != nil {
		return err
	}

	return expand.NewExpander(c1f, graph).Run(ctx)
}

func loadGrantSourcesByKey(ctx context.Context, c1f *dotc1z.C1File, syncID string) (map[string]map[string]bool, error) {
	// Ensure we're not in a "current sync" context (ViewSync rejects that).
	err := c1f.SetSyncID(ctx, "")
	if err != nil {
		return nil, err
	}
	if err := c1f.ViewSync(ctx, syncID); err != nil {
		return nil, err
	}

	out := make(map[string]map[string]bool)
	pageToken := ""
	for {
		resp, err := c1f.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{PageToken: pageToken}.Build())
		if err != nil {
			return nil, err
		}
		for _, g := range resp.GetList() {
			key := g.GetEntitlement().GetId() + "|" + g.GetPrincipal().GetId().GetResourceType() + "|" + g.GetPrincipal().GetId().GetResource()
			srcs := make(map[string]bool)
			for s := range g.GetSources().GetSources() {
				srcs[s] = true
			}
			out[key] = srcs
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	return out, nil
}

func TestIncrementalExpansion_RemovedEdgeDeletesDerivedGrant(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	oldPath := filepath.Join(tmpDir, "old.c1z")
	newPath := filepath.Join(tmpDir, "new.c1z")
	expectedPath := filepath.Join(tmpDir, "expected.c1z")

	// Common objects
	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()

	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(), DisplayName: "G1"}.Build()
	g2 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g2"}.Build(), DisplayName: "G2"}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(), DisplayName: "U1"}.Build()

	e1 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g1, "member"), Resource: g1, Slug: "member", DisplayName: "member"}.Build()
	e2 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g2, "member"), Resource: g2, Slug: "member", DisplayName: "member"}.Build()

	// Direct grant U1 -> E1
	grantU1E1 := v2.Grant_builder{
		Id:          batonGrant.NewGrantID(u1, e1),
		Entitlement: e1,
		Principal:   u1,
	}.Build()

	// Nesting grant G1 -> E2 defining edge E1 -> E2
	nestingGrantID := batonGrant.NewGrantID(g1, e2)
	grantG1E2 := v2.Grant_builder{
		Id:          nestingGrantID,
		Entitlement: e2,
		Principal:   g1,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds:  []string{e1.GetId()},
			Shallow:         false,
			ResourceTypeIds: []string{"user"},
		}.Build()),
	}.Build()

	// OLD (expanded)
	oldFile, err := dotc1z.NewC1ZFile(ctx, oldPath)
	require.NoError(t, err)
	defer oldFile.Close(ctx)

	oldSyncID, err := oldFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, oldFile.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, oldFile.PutResources(ctx, g1, g2, u1))
	require.NoError(t, oldFile.PutEntitlements(ctx, e1, e2))
	require.NoError(t, oldFile.PutGrants(ctx, grantU1E1, grantG1E2))
	require.NoError(t, oldFile.EndSync(ctx))

	// Expand old sync in place
	require.NoError(t, runFullExpansion(ctx, oldFile, oldSyncID))

	// NEW starts as a copy of OLD expanded data, but without the nesting grant row.
	newFile, err := dotc1z.NewC1ZFile(ctx, newPath)
	require.NoError(t, err)
	defer newFile.Close(ctx)

	newSyncID, err := newFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, newFile.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, newFile.PutResources(ctx, g1, g2, u1))
	require.NoError(t, newFile.PutEntitlements(ctx, e1, e2))

	// Copy all grants from old expanded sync -> new sync, skipping nesting grant (edge removal).
	require.NoError(t, oldFile.SetSyncID(ctx, ""))
	require.NoError(t, oldFile.ViewSync(ctx, oldSyncID))
	pageToken := ""
	for {
		resp, err := oldFile.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{PageToken: pageToken}.Build())
		require.NoError(t, err)
		for _, g := range resp.GetList() {
			if g.GetId() == nestingGrantID {
				continue
			}
			require.NoError(t, newFile.PutGrants(ctx, g))
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	require.NoError(t, newFile.EndSync(ctx))

	// Generate diff syncs: main=NEW, attached=OLD
	attached, err := newFile.AttachFile(oldFile, "attached")
	require.NoError(t, err)
	upsertsSyncID, deletionsSyncID, err := attached.GenerateSyncDiffFromFile(ctx, oldSyncID, newSyncID)
	require.NoError(t, err)
	_, err = attached.DetachFile("attached")
	require.NoError(t, err)

	// Incremental invalidation + expansion on NEW
	require.NoError(t, incrementalexpansion.ApplyIncrementalExpansionFromDiff(ctx, newFile, newSyncID, upsertsSyncID, deletionsSyncID))

	// EXPECTED: start from connector truth after removal (no nesting grant), then expand fully.
	expectedFile, err := dotc1z.NewC1ZFile(ctx, expectedPath)
	require.NoError(t, err)
	defer expectedFile.Close(ctx)

	expectedSyncID, err := expectedFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, expectedFile.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, expectedFile.PutResources(ctx, g1, g2, u1))
	require.NoError(t, expectedFile.PutEntitlements(ctx, e1, e2))
	require.NoError(t, expectedFile.PutGrants(ctx, grantU1E1 /* no nesting */))
	require.NoError(t, expectedFile.EndSync(ctx))
	require.NoError(t, runFullExpansion(ctx, expectedFile, expectedSyncID))

	got, err := loadGrantSourcesByKey(ctx, newFile, newSyncID)
	require.NoError(t, err)
	want, err := loadGrantSourcesByKey(ctx, expectedFile, expectedSyncID)
	require.NoError(t, err)

	require.Equal(t, want, got)
}

func TestIncrementalExpansion_RemovedEdgeRemovesOnlyOneSource(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	oldPath := filepath.Join(tmpDir, "old2.c1z")
	newPath := filepath.Join(tmpDir, "new2.c1z")
	expectedPath := filepath.Join(tmpDir, "expected2.c1z")

	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()

	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(), DisplayName: "G1"}.Build()
	g2 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g2"}.Build(), DisplayName: "G2"}.Build()
	g3 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g3"}.Build(), DisplayName: "G3"}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(), DisplayName: "U1"}.Build()

	e1 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g1, "member"), Resource: g1, Slug: "member", DisplayName: "member"}.Build()
	e2 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g2, "member"), Resource: g2, Slug: "member", DisplayName: "member"}.Build()
	e3 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g3, "member"), Resource: g3, Slug: "member", DisplayName: "member"}.Build()

	grantU1E1 := v2.Grant_builder{Id: batonGrant.NewGrantID(u1, e1), Entitlement: e1, Principal: u1}.Build()
	grantU1E3 := v2.Grant_builder{Id: batonGrant.NewGrantID(u1, e3), Entitlement: e3, Principal: u1}.Build()

	nestingGrantID1 := batonGrant.NewGrantID(g1, e2)
	grantG1E2 := v2.Grant_builder{
		Id:          nestingGrantID1,
		Entitlement: e2,
		Principal:   g1,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds:  []string{e1.GetId()},
			Shallow:         false,
			ResourceTypeIds: []string{"user"},
		}.Build()),
	}.Build()

	nestingGrantID3 := batonGrant.NewGrantID(g3, e2)
	grantG3E2 := v2.Grant_builder{
		Id:          nestingGrantID3,
		Entitlement: e2,
		Principal:   g3,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds:  []string{e3.GetId()},
			Shallow:         false,
			ResourceTypeIds: []string{"user"},
		}.Build()),
	}.Build()

	oldFile, err := dotc1z.NewC1ZFile(ctx, oldPath)
	require.NoError(t, err)
	defer oldFile.Close(ctx)

	oldSyncID, err := oldFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, oldFile.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, oldFile.PutResources(ctx, g1, g2, g3, u1))
	require.NoError(t, oldFile.PutEntitlements(ctx, e1, e2, e3))
	require.NoError(t, oldFile.PutGrants(ctx, grantU1E1, grantU1E3, grantG1E2, grantG3E2))
	require.NoError(t, oldFile.EndSync(ctx))
	require.NoError(t, runFullExpansion(ctx, oldFile, oldSyncID))

	newFile, err := dotc1z.NewC1ZFile(ctx, newPath)
	require.NoError(t, err)
	defer newFile.Close(ctx)

	newSyncID, err := newFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, newFile.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, newFile.PutResources(ctx, g1, g2, g3, u1))
	require.NoError(t, newFile.PutEntitlements(ctx, e1, e2, e3))

	require.NoError(t, oldFile.SetSyncID(ctx, ""))
	require.NoError(t, oldFile.ViewSync(ctx, oldSyncID))
	pageToken := ""
	for {
		resp, err := oldFile.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{PageToken: pageToken}.Build())
		require.NoError(t, err)
		for _, g := range resp.GetList() {
			if g.GetId() == nestingGrantID1 {
				continue
			}
			require.NoError(t, newFile.PutGrants(ctx, g))
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	require.NoError(t, newFile.EndSync(ctx))

	attached, err := newFile.AttachFile(oldFile, "attached")
	require.NoError(t, err)
	upsertsSyncID, deletionsSyncID, err := attached.GenerateSyncDiffFromFile(ctx, oldSyncID, newSyncID)
	require.NoError(t, err)
	_, err = attached.DetachFile("attached")
	require.NoError(t, err)

	require.NoError(t, incrementalexpansion.ApplyIncrementalExpansionFromDiff(ctx, newFile, newSyncID, upsertsSyncID, deletionsSyncID))

	expectedFile, err := dotc1z.NewC1ZFile(ctx, expectedPath)
	require.NoError(t, err)
	defer expectedFile.Close(ctx)

	expectedSyncID, err := expectedFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, expectedFile.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, expectedFile.PutResources(ctx, g1, g2, g3, u1))
	require.NoError(t, expectedFile.PutEntitlements(ctx, e1, e2, e3))
	require.NoError(t, expectedFile.PutGrants(ctx, grantU1E1, grantU1E3 /* no G1->E2 */, grantG3E2))
	require.NoError(t, expectedFile.EndSync(ctx))
	require.NoError(t, runFullExpansion(ctx, expectedFile, expectedSyncID))

	got, err := loadGrantSourcesByKey(ctx, newFile, newSyncID)
	require.NoError(t, err)
	want, err := loadGrantSourcesByKey(ctx, expectedFile, expectedSyncID)
	require.NoError(t, err)

	require.Equal(t, want, got)
}

func TestIncrementalExpansion_NoChangesIsNoop(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	oldPath := filepath.Join(tmpDir, "old_noop.c1z")
	newPath := filepath.Join(tmpDir, "new_noop.c1z")

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

	// OLD (expanded)
	oldFile, err := dotc1z.NewC1ZFile(ctx, oldPath)
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

	// NEW is identical to OLD expanded state.
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
	pageToken := ""
	for {
		resp, err := oldFile.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{PageToken: pageToken}.Build())
		require.NoError(t, err)
		for _, g := range resp.GetList() {
			require.NoError(t, newFile.PutGrants(ctx, g))
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	require.NoError(t, newFile.EndSync(ctx))

	attached, err := newFile.AttachFile(oldFile, "attached")
	require.NoError(t, err)
	upsertsSyncID, deletionsSyncID, err := attached.GenerateSyncDiffFromFile(ctx, oldSyncID, newSyncID)
	require.NoError(t, err)
	_, err = attached.DetachFile("attached")
	require.NoError(t, err)

	// Should be a no-op.
	require.NoError(t, incrementalexpansion.ApplyIncrementalExpansionFromDiff(ctx, newFile, newSyncID, upsertsSyncID, deletionsSyncID))

	got, err := loadGrantSourcesByKey(ctx, newFile, newSyncID)
	require.NoError(t, err)
	want, err := loadGrantSourcesByKey(ctx, oldFile, oldSyncID)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestIncrementalExpansion_MultipleDisjointSubgraphs(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	oldPath := filepath.Join(tmpDir, "old_multi.c1z")
	newPath := filepath.Join(tmpDir, "new_multi.c1z")
	expectedPath := filepath.Join(tmpDir, "expected_multi.c1z")

	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()

	// Subgraph A
	ga1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "ga1"}.Build(), DisplayName: "GA1"}.Build()
	ga2 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "ga2"}.Build(), DisplayName: "GA2"}.Build()
	ua := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "ua"}.Build(), DisplayName: "UA"}.Build()

	ea1 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(ga1, "member"), Resource: ga1, Slug: "member", DisplayName: "member"}.Build()
	ea2 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(ga2, "member"), Resource: ga2, Slug: "member", DisplayName: "member"}.Build()

	grantUAEA1 := v2.Grant_builder{Id: batonGrant.NewGrantID(ua, ea1), Entitlement: ea1, Principal: ua}.Build()
	nestingAID := batonGrant.NewGrantID(ga1, ea2)
	grantGA1EA2 := v2.Grant_builder{
		Id:          nestingAID,
		Entitlement: ea2,
		Principal:   ga1,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds:  []string{ea1.GetId()},
			Shallow:         false,
			ResourceTypeIds: []string{"user"},
		}.Build()),
	}.Build()

	// Subgraph B
	gb1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "gb1"}.Build(), DisplayName: "GB1"}.Build()
	gb2 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "gb2"}.Build(), DisplayName: "GB2"}.Build()
	ub := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "ub"}.Build(), DisplayName: "UB"}.Build()

	eb1 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(gb1, "member"), Resource: gb1, Slug: "member", DisplayName: "member"}.Build()
	eb2 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(gb2, "member"), Resource: gb2, Slug: "member", DisplayName: "member"}.Build()

	grantUBEB1 := v2.Grant_builder{Id: batonGrant.NewGrantID(ub, eb1), Entitlement: eb1, Principal: ub}.Build()
	nestingBID := batonGrant.NewGrantID(gb1, eb2)
	grantGB1EB2 := v2.Grant_builder{
		Id:          nestingBID,
		Entitlement: eb2,
		Principal:   gb1,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds:  []string{eb1.GetId()},
			Shallow:         false,
			ResourceTypeIds: []string{"user"},
		}.Build()),
	}.Build()

	// OLD (expanded)
	oldFile, err := dotc1z.NewC1ZFile(ctx, oldPath)
	require.NoError(t, err)
	defer oldFile.Close(ctx)

	oldSyncID, err := oldFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, oldFile.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, oldFile.PutResources(ctx, ga1, ga2, ua, gb1, gb2, ub))
	require.NoError(t, oldFile.PutEntitlements(ctx, ea1, ea2, eb1, eb2))
	require.NoError(t, oldFile.PutGrants(ctx, grantUAEA1, grantGA1EA2, grantUBEB1, grantGB1EB2))
	require.NoError(t, oldFile.EndSync(ctx))
	require.NoError(t, runFullExpansion(ctx, oldFile, oldSyncID))

	// NEW is old expanded minus both nesting grants.
	newFile, err := dotc1z.NewC1ZFile(ctx, newPath)
	require.NoError(t, err)
	defer newFile.Close(ctx)

	newSyncID, err := newFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, newFile.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, newFile.PutResources(ctx, ga1, ga2, ua, gb1, gb2, ub))
	require.NoError(t, newFile.PutEntitlements(ctx, ea1, ea2, eb1, eb2))

	require.NoError(t, oldFile.SetSyncID(ctx, ""))
	require.NoError(t, oldFile.ViewSync(ctx, oldSyncID))
	pageToken := ""
	for {
		resp, err := oldFile.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{PageToken: pageToken}.Build())
		require.NoError(t, err)
		for _, g := range resp.GetList() {
			if g.GetId() == nestingAID || g.GetId() == nestingBID {
				continue
			}
			require.NoError(t, newFile.PutGrants(ctx, g))
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	require.NoError(t, newFile.EndSync(ctx))

	attached, err := newFile.AttachFile(oldFile, "attached")
	require.NoError(t, err)
	upsertsSyncID, deletionsSyncID, err := attached.GenerateSyncDiffFromFile(ctx, oldSyncID, newSyncID)
	require.NoError(t, err)
	_, err = attached.DetachFile("attached")
	require.NoError(t, err)

	require.NoError(t, incrementalexpansion.ApplyIncrementalExpansionFromDiff(ctx, newFile, newSyncID, upsertsSyncID, deletionsSyncID))

	// EXPECTED: connector truth after removal (no nesting grants), fully expanded.
	expectedFile, err := dotc1z.NewC1ZFile(ctx, expectedPath)
	require.NoError(t, err)
	defer expectedFile.Close(ctx)
	expectedSyncID, err := expectedFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, expectedFile.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, expectedFile.PutResources(ctx, ga1, ga2, ua, gb1, gb2, ub))
	require.NoError(t, expectedFile.PutEntitlements(ctx, ea1, ea2, eb1, eb2))
	require.NoError(t, expectedFile.PutGrants(ctx, grantUAEA1, grantUBEB1 /* no nesting */))
	require.NoError(t, expectedFile.EndSync(ctx))
	require.NoError(t, runFullExpansion(ctx, expectedFile, expectedSyncID))

	got, err := loadGrantSourcesByKey(ctx, newFile, newSyncID)
	require.NoError(t, err)
	want, err := loadGrantSourcesByKey(ctx, expectedFile, expectedSyncID)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestIncrementalExpansion_EntitlementDeleted(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	oldPath := filepath.Join(tmpDir, "old_ent_deleted.c1z")
	newPath := filepath.Join(tmpDir, "new_ent_deleted.c1z")
	expectedPath := filepath.Join(tmpDir, "expected_ent_deleted.c1z")

	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()

	// Chain: e1 -> e2 -> e3. Deleting e2 should remove propagated membership on e3.
	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(), DisplayName: "G1"}.Build()
	g2 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g2"}.Build(), DisplayName: "G2"}.Build()
	g3 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g3"}.Build(), DisplayName: "G3"}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(), DisplayName: "U1"}.Build()

	e1 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g1, "member"), Resource: g1, Slug: "member", DisplayName: "member"}.Build()
	e2 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g2, "member"), Resource: g2, Slug: "member", DisplayName: "member"}.Build()
	e3 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g3, "member"), Resource: g3, Slug: "member", DisplayName: "member"}.Build()

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

	grantG2E3 := v2.Grant_builder{
		Id:          batonGrant.NewGrantID(g2, e3),
		Entitlement: e3,
		Principal:   g2,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds:  []string{e2.GetId()},
			Shallow:         false,
			ResourceTypeIds: []string{"user"},
		}.Build()),
	}.Build()

	// OLD (expanded)
	oldFile, err := dotc1z.NewC1ZFile(ctx, oldPath)
	require.NoError(t, err)
	defer oldFile.Close(ctx)

	oldSyncID, err := oldFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, oldFile.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, oldFile.PutResources(ctx, g1, g2, g3, u1))
	require.NoError(t, oldFile.PutEntitlements(ctx, e1, e2, e3))
	require.NoError(t, oldFile.PutGrants(ctx, grantU1E1, grantG1E2, grantG2E3))
	require.NoError(t, oldFile.EndSync(ctx))
	require.NoError(t, runFullExpansion(ctx, oldFile, oldSyncID))

	// NEW is OLD expanded minus the e2 entitlement and any grants on e2.
	newFile, err := dotc1z.NewC1ZFile(ctx, newPath)
	require.NoError(t, err)
	defer newFile.Close(ctx)

	newSyncID, err := newFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, newFile.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, newFile.PutResources(ctx, g1, g2, g3, u1))
	require.NoError(t, newFile.PutEntitlements(ctx, e1 /* e2 deleted */, e3))

	require.NoError(t, oldFile.SetSyncID(ctx, ""))
	require.NoError(t, oldFile.ViewSync(ctx, oldSyncID))
	pageToken := ""
	for {
		resp, err := oldFile.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{PageToken: pageToken}.Build())
		require.NoError(t, err)
		for _, g := range resp.GetList() {
			if g.GetEntitlement().GetId() == e2.GetId() {
				continue
			}
			require.NoError(t, newFile.PutGrants(ctx, g))
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	require.NoError(t, newFile.EndSync(ctx))

	attached, err := newFile.AttachFile(oldFile, "attached")
	require.NoError(t, err)
	upsertsSyncID, deletionsSyncID, err := attached.GenerateSyncDiffFromFile(ctx, oldSyncID, newSyncID)
	require.NoError(t, err)
	_, err = attached.DetachFile("attached")
	require.NoError(t, err)

	require.NoError(t, incrementalexpansion.ApplyIncrementalExpansionFromDiff(ctx, newFile, newSyncID, upsertsSyncID, deletionsSyncID))

	// EXPECTED: connector truth with e2 removed (no grants on e2), expanded fully.
	expectedFile, err := dotc1z.NewC1ZFile(ctx, expectedPath)
	require.NoError(t, err)
	defer expectedFile.Close(ctx)
	expectedSyncID, err := expectedFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, expectedFile.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, expectedFile.PutResources(ctx, g1, g2, g3, u1))
	require.NoError(t, expectedFile.PutEntitlements(ctx, e1 /* e2 deleted */, e3))
	require.NoError(t, expectedFile.PutGrants(ctx, grantU1E1 /* no g1->e2 */, grantG2E3))
	require.NoError(t, expectedFile.EndSync(ctx))
	require.NoError(t, runFullExpansion(ctx, expectedFile, expectedSyncID))

	got, err := loadGrantSourcesByKey(ctx, newFile, newSyncID)
	require.NoError(t, err)
	want, err := loadGrantSourcesByKey(ctx, expectedFile, expectedSyncID)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestIncrementalExpansion_GrantNoLongerExpandable(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	oldPath := filepath.Join(tmpDir, "old_noexpand.c1z")
	newPath := filepath.Join(tmpDir, "new_noexpand.c1z")
	expectedPath := filepath.Join(tmpDir, "expected_noexpand.c1z")

	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()

	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(), DisplayName: "G1"}.Build()
	g2 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g2"}.Build(), DisplayName: "G2"}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(), DisplayName: "U1"}.Build()

	e1 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g1, "member"), Resource: g1, Slug: "member", DisplayName: "member"}.Build()
	e2 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g2, "member"), Resource: g2, Slug: "member", DisplayName: "member"}.Build()

	grantU1E1 := v2.Grant_builder{Id: batonGrant.NewGrantID(u1, e1), Entitlement: e1, Principal: u1}.Build()
	nestingID := batonGrant.NewGrantID(g1, e2)
	grantExpandable := v2.Grant_builder{
		Id:          nestingID,
		Entitlement: e2,
		Principal:   g1,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds:  []string{e1.GetId()},
			Shallow:         false,
			ResourceTypeIds: []string{"user"},
		}.Build()),
	}.Build()
	grantNotExpandable := v2.Grant_builder{
		Id:          nestingID,
		Entitlement: e2,
		Principal:   g1,
		Annotations: nil, // removed GrantExpandable
	}.Build()

	// OLD (expanded)
	oldFile, err := dotc1z.NewC1ZFile(ctx, oldPath)
	require.NoError(t, err)
	defer oldFile.Close(ctx)
	oldSyncID, err := oldFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, oldFile.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, oldFile.PutResources(ctx, g1, g2, u1))
	require.NoError(t, oldFile.PutEntitlements(ctx, e1, e2))
	require.NoError(t, oldFile.PutGrants(ctx, grantU1E1, grantExpandable))
	require.NoError(t, oldFile.EndSync(ctx))
	require.NoError(t, runFullExpansion(ctx, oldFile, oldSyncID))

	// NEW is OLD expanded, but the nesting grant is still present and is no longer expandable.
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
	pageToken := ""
	for {
		resp, err := oldFile.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{PageToken: pageToken}.Build())
		require.NoError(t, err)
		for _, g := range resp.GetList() {
			// Replace the edge-defining grant with the non-expandable version.
			if g.GetId() == nestingID {
				require.NoError(t, newFile.PutGrants(ctx, grantNotExpandable))
				continue
			}
			require.NoError(t, newFile.PutGrants(ctx, g))
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	require.NoError(t, newFile.EndSync(ctx))

	attached, err := newFile.AttachFile(oldFile, "attached")
	require.NoError(t, err)
	upsertsSyncID, deletionsSyncID, err := attached.GenerateSyncDiffFromFile(ctx, oldSyncID, newSyncID)
	require.NoError(t, err)
	_, err = attached.DetachFile("attached")
	require.NoError(t, err)

	require.NoError(t, incrementalexpansion.ApplyIncrementalExpansionFromDiff(ctx, newFile, newSyncID, upsertsSyncID, deletionsSyncID))

	// EXPECTED: connector truth with nesting grant non-expandable, expanded fully.
	expectedFile, err := dotc1z.NewC1ZFile(ctx, expectedPath)
	require.NoError(t, err)
	defer expectedFile.Close(ctx)
	expectedSyncID, err := expectedFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, expectedFile.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, expectedFile.PutResources(ctx, g1, g2, u1))
	require.NoError(t, expectedFile.PutEntitlements(ctx, e1, e2))
	require.NoError(t, expectedFile.PutGrants(ctx, grantU1E1, grantNotExpandable))
	require.NoError(t, expectedFile.EndSync(ctx))
	require.NoError(t, runFullExpansion(ctx, expectedFile, expectedSyncID))

	got, err := loadGrantSourcesByKey(ctx, newFile, newSyncID)
	require.NoError(t, err)
	want, err := loadGrantSourcesByKey(ctx, expectedFile, expectedSyncID)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestIncrementalExpansion_ResourceDeleted(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	oldPath := filepath.Join(tmpDir, "old_res_deleted.c1z")
	newPath := filepath.Join(tmpDir, "new_res_deleted.c1z")
	expectedPath := filepath.Join(tmpDir, "expected_res_deleted.c1z")

	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()

	// Chain: e1 -> e2 -> e3. Deleting resource g2 (and therefore entitlement e2) should remove propagated membership on e3.
	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(), DisplayName: "G1"}.Build()
	g2 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g2"}.Build(), DisplayName: "G2"}.Build()
	g3 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g3"}.Build(), DisplayName: "G3"}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(), DisplayName: "U1"}.Build()

	e1 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g1, "member"), Resource: g1, Slug: "member", DisplayName: "member"}.Build()
	e2 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g2, "member"), Resource: g2, Slug: "member", DisplayName: "member"}.Build()
	e3 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g3, "member"), Resource: g3, Slug: "member", DisplayName: "member"}.Build()

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

	grantG2E3 := v2.Grant_builder{
		Id:          batonGrant.NewGrantID(g2, e3),
		Entitlement: e3,
		Principal:   g2,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds:  []string{e2.GetId()},
			Shallow:         false,
			ResourceTypeIds: []string{"user"},
		}.Build()),
	}.Build()

	// OLD (expanded)
	oldFile, err := dotc1z.NewC1ZFile(ctx, oldPath)
	require.NoError(t, err)
	defer oldFile.Close(ctx)

	oldSyncID, err := oldFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, oldFile.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, oldFile.PutResources(ctx, g1, g2, g3, u1))
	require.NoError(t, oldFile.PutEntitlements(ctx, e1, e2, e3))
	require.NoError(t, oldFile.PutGrants(ctx, grantU1E1, grantG1E2, grantG2E3))
	require.NoError(t, oldFile.EndSync(ctx))
	require.NoError(t, runFullExpansion(ctx, oldFile, oldSyncID))

	// NEW is OLD expanded minus resource g2, entitlement e2, and any grants on e2.
	newFile, err := dotc1z.NewC1ZFile(ctx, newPath)
	require.NoError(t, err)
	defer newFile.Close(ctx)

	newSyncID, err := newFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, newFile.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, newFile.PutResources(ctx, g1 /* g2 deleted */, g3, u1))
	require.NoError(t, newFile.PutEntitlements(ctx, e1 /* e2 deleted */, e3))

	require.NoError(t, oldFile.SetSyncID(ctx, ""))
	require.NoError(t, oldFile.ViewSync(ctx, oldSyncID))
	pageToken := ""
	for {
		resp, err := oldFile.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{PageToken: pageToken}.Build())
		require.NoError(t, err)
		for _, g := range resp.GetList() {
			if g.GetEntitlement().GetId() == e2.GetId() {
				continue
			}
			require.NoError(t, newFile.PutGrants(ctx, g))
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	require.NoError(t, newFile.EndSync(ctx))

	attached, err := newFile.AttachFile(oldFile, "attached")
	require.NoError(t, err)
	upsertsSyncID, deletionsSyncID, err := attached.GenerateSyncDiffFromFile(ctx, oldSyncID, newSyncID)
	require.NoError(t, err)
	_, err = attached.DetachFile("attached")
	require.NoError(t, err)

	require.NoError(t, incrementalexpansion.ApplyIncrementalExpansionFromDiff(ctx, newFile, newSyncID, upsertsSyncID, deletionsSyncID))

	// EXPECTED: connector truth with g2/e2 removed (no grants on e2), expanded fully.
	expectedFile, err := dotc1z.NewC1ZFile(ctx, expectedPath)
	require.NoError(t, err)
	defer expectedFile.Close(ctx)
	expectedSyncID, err := expectedFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, expectedFile.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, expectedFile.PutResources(ctx, g1 /* g2 deleted */, g3, u1))
	require.NoError(t, expectedFile.PutEntitlements(ctx, e1 /* e2 deleted */, e3))
	require.NoError(t, expectedFile.PutGrants(ctx, grantU1E1 /* no g1->e2 */, grantG2E3))
	require.NoError(t, expectedFile.EndSync(ctx))
	require.NoError(t, runFullExpansion(ctx, expectedFile, expectedSyncID))

	got, err := loadGrantSourcesByKey(ctx, newFile, newSyncID)
	require.NoError(t, err)
	want, err := loadGrantSourcesByKey(ctx, expectedFile, expectedSyncID)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestIncrementalExpansion_AddedEdgeCreatesNewDerivedGrant(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	oldPath := filepath.Join(tmpDir, "old_add_edge.c1z")
	newPath := filepath.Join(tmpDir, "new_add_edge.c1z")
	expectedPath := filepath.Join(tmpDir, "expected_add_edge.c1z")

	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()

	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(), DisplayName: "G1"}.Build()
	g2 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g2"}.Build(), DisplayName: "G2"}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(), DisplayName: "U1"}.Build()

	e1 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g1, "member"), Resource: g1, Slug: "member", DisplayName: "member"}.Build()
	e2 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g2, "member"), Resource: g2, Slug: "member", DisplayName: "member"}.Build()

	grantU1E1 := v2.Grant_builder{Id: batonGrant.NewGrantID(u1, e1), Entitlement: e1, Principal: u1}.Build()

	// Edge-defining grant to be added in NEW
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

	// OLD: just the direct grant, no edges, expanded (nothing to expand)
	oldFile, err := dotc1z.NewC1ZFile(ctx, oldPath)
	require.NoError(t, err)
	defer oldFile.Close(ctx)

	oldSyncID, err := oldFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, oldFile.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, oldFile.PutResources(ctx, g1, g2, u1))
	require.NoError(t, oldFile.PutEntitlements(ctx, e1, e2))
	require.NoError(t, oldFile.PutGrants(ctx, grantU1E1))
	require.NoError(t, oldFile.EndSync(ctx))
	require.NoError(t, runFullExpansion(ctx, oldFile, oldSyncID))

	// NEW: same as OLD but with the new edge-defining grant added
	newFile, err := dotc1z.NewC1ZFile(ctx, newPath)
	require.NoError(t, err)
	defer newFile.Close(ctx)

	newSyncID, err := newFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, newFile.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, newFile.PutResources(ctx, g1, g2, u1))
	require.NoError(t, newFile.PutEntitlements(ctx, e1, e2))

	// Copy grants from OLD expanded state
	require.NoError(t, oldFile.SetSyncID(ctx, ""))
	require.NoError(t, oldFile.ViewSync(ctx, oldSyncID))
	pageToken := ""
	for {
		resp, err := oldFile.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{PageToken: pageToken}.Build())
		require.NoError(t, err)
		for _, g := range resp.GetList() {
			require.NoError(t, newFile.PutGrants(ctx, g))
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	// Add the new edge-defining grant
	require.NoError(t, newFile.PutGrants(ctx, grantG1E2))
	require.NoError(t, newFile.EndSync(ctx))

	attached, err := newFile.AttachFile(oldFile, "attached")
	require.NoError(t, err)
	upsertsSyncID, deletionsSyncID, err := attached.GenerateSyncDiffFromFile(ctx, oldSyncID, newSyncID)
	require.NoError(t, err)
	_, err = attached.DetachFile("attached")
	require.NoError(t, err)

	require.NoError(t, incrementalexpansion.ApplyIncrementalExpansionFromDiff(ctx, newFile, newSyncID, upsertsSyncID, deletionsSyncID))

	// EXPECTED: connector truth with new edge, fully expanded
	expectedFile, err := dotc1z.NewC1ZFile(ctx, expectedPath)
	require.NoError(t, err)
	defer expectedFile.Close(ctx)
	expectedSyncID, err := expectedFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, expectedFile.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, expectedFile.PutResources(ctx, g1, g2, u1))
	require.NoError(t, expectedFile.PutEntitlements(ctx, e1, e2))
	require.NoError(t, expectedFile.PutGrants(ctx, grantU1E1, grantG1E2))
	require.NoError(t, expectedFile.EndSync(ctx))
	require.NoError(t, runFullExpansion(ctx, expectedFile, expectedSyncID))

	got, err := loadGrantSourcesByKey(ctx, newFile, newSyncID)
	require.NoError(t, err)
	want, err := loadGrantSourcesByKey(ctx, expectedFile, expectedSyncID)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestIncrementalExpansion_AddedDirectGrantPropagates(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	oldPath := filepath.Join(tmpDir, "old_add_direct.c1z")
	newPath := filepath.Join(tmpDir, "new_add_direct.c1z")
	expectedPath := filepath.Join(tmpDir, "expected_add_direct.c1z")

	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()

	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(), DisplayName: "G1"}.Build()
	g2 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g2"}.Build(), DisplayName: "G2"}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(), DisplayName: "U1"}.Build()
	u2 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u2"}.Build(), DisplayName: "U2"}.Build()

	e1 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g1, "member"), Resource: g1, Slug: "member", DisplayName: "member"}.Build()
	e2 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g2, "member"), Resource: g2, Slug: "member", DisplayName: "member"}.Build()

	grantU1E1 := v2.Grant_builder{Id: batonGrant.NewGrantID(u1, e1), Entitlement: e1, Principal: u1}.Build()
	grantU2E1 := v2.Grant_builder{Id: batonGrant.NewGrantID(u2, e1), Entitlement: e1, Principal: u2}.Build()

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

	// OLD: U1 → E1, edge E1 → E2, expanded (U1 → E2 derived)
	oldFile, err := dotc1z.NewC1ZFile(ctx, oldPath)
	require.NoError(t, err)
	defer oldFile.Close(ctx)

	oldSyncID, err := oldFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, oldFile.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, oldFile.PutResources(ctx, g1, g2, u1, u2))
	require.NoError(t, oldFile.PutEntitlements(ctx, e1, e2))
	require.NoError(t, oldFile.PutGrants(ctx, grantU1E1, grantG1E2))
	require.NoError(t, oldFile.EndSync(ctx))
	require.NoError(t, runFullExpansion(ctx, oldFile, oldSyncID))

	// NEW: same as OLD expanded, plus new direct grant U2 → E1
	newFile, err := dotc1z.NewC1ZFile(ctx, newPath)
	require.NoError(t, err)
	defer newFile.Close(ctx)

	newSyncID, err := newFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, newFile.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, newFile.PutResources(ctx, g1, g2, u1, u2))
	require.NoError(t, newFile.PutEntitlements(ctx, e1, e2))

	require.NoError(t, oldFile.SetSyncID(ctx, ""))
	require.NoError(t, oldFile.ViewSync(ctx, oldSyncID))
	pageToken := ""
	for {
		resp, err := oldFile.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{PageToken: pageToken}.Build())
		require.NoError(t, err)
		for _, g := range resp.GetList() {
			require.NoError(t, newFile.PutGrants(ctx, g))
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	// Add the new direct grant
	require.NoError(t, newFile.PutGrants(ctx, grantU2E1))
	require.NoError(t, newFile.EndSync(ctx))

	attached, err := newFile.AttachFile(oldFile, "attached")
	require.NoError(t, err)
	upsertsSyncID, deletionsSyncID, err := attached.GenerateSyncDiffFromFile(ctx, oldSyncID, newSyncID)
	require.NoError(t, err)
	_, err = attached.DetachFile("attached")
	require.NoError(t, err)

	require.NoError(t, incrementalexpansion.ApplyIncrementalExpansionFromDiff(ctx, newFile, newSyncID, upsertsSyncID, deletionsSyncID))

	// EXPECTED: connector truth with both direct grants, fully expanded (U2 → E2 derived)
	expectedFile, err := dotc1z.NewC1ZFile(ctx, expectedPath)
	require.NoError(t, err)
	defer expectedFile.Close(ctx)
	expectedSyncID, err := expectedFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, expectedFile.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, expectedFile.PutResources(ctx, g1, g2, u1, u2))
	require.NoError(t, expectedFile.PutEntitlements(ctx, e1, e2))
	require.NoError(t, expectedFile.PutGrants(ctx, grantU1E1, grantU2E1, grantG1E2))
	require.NoError(t, expectedFile.EndSync(ctx))
	require.NoError(t, runFullExpansion(ctx, expectedFile, expectedSyncID))

	got, err := loadGrantSourcesByKey(ctx, newFile, newSyncID)
	require.NoError(t, err)
	want, err := loadGrantSourcesByKey(ctx, expectedFile, expectedSyncID)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestIncrementalExpansion_ShallowEdgeRemoval(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	oldPath := filepath.Join(tmpDir, "old_shallow.c1z")
	newPath := filepath.Join(tmpDir, "new_shallow.c1z")
	expectedPath := filepath.Join(tmpDir, "expected_shallow.c1z")

	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()

	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(), DisplayName: "G1"}.Build()
	g2 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g2"}.Build(), DisplayName: "G2"}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(), DisplayName: "U1"}.Build()

	e1 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g1, "member"), Resource: g1, Slug: "member", DisplayName: "member"}.Build()
	e2 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g2, "member"), Resource: g2, Slug: "member", DisplayName: "member"}.Build()

	grantU1E1 := v2.Grant_builder{Id: batonGrant.NewGrantID(u1, e1), Entitlement: e1, Principal: u1}.Build()

	// Shallow edge E1 -> E2 (only direct grants on E1 propagate)
	nestingID := batonGrant.NewGrantID(g1, e2)
	grantG1E2Shallow := v2.Grant_builder{
		Id:          nestingID,
		Entitlement: e2,
		Principal:   g1,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds:  []string{e1.GetId()},
			Shallow:         true, // shallow!
			ResourceTypeIds: []string{"user"},
		}.Build()),
	}.Build()

	// OLD: U1 → E1, shallow edge E1 → E2, expanded (U1 → E2 derived)
	oldFile, err := dotc1z.NewC1ZFile(ctx, oldPath)
	require.NoError(t, err)
	defer oldFile.Close(ctx)

	oldSyncID, err := oldFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, oldFile.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, oldFile.PutResources(ctx, g1, g2, u1))
	require.NoError(t, oldFile.PutEntitlements(ctx, e1, e2))
	require.NoError(t, oldFile.PutGrants(ctx, grantU1E1, grantG1E2Shallow))
	require.NoError(t, oldFile.EndSync(ctx))
	require.NoError(t, runFullExpansion(ctx, oldFile, oldSyncID))

	// NEW: same as OLD expanded, but remove the shallow edge
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
	pageToken := ""
	for {
		resp, err := oldFile.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{PageToken: pageToken}.Build())
		require.NoError(t, err)
		for _, g := range resp.GetList() {
			if g.GetId() == nestingID {
				continue // skip the shallow edge-defining grant
			}
			require.NoError(t, newFile.PutGrants(ctx, g))
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	require.NoError(t, newFile.EndSync(ctx))

	attached, err := newFile.AttachFile(oldFile, "attached")
	require.NoError(t, err)
	upsertsSyncID, deletionsSyncID, err := attached.GenerateSyncDiffFromFile(ctx, oldSyncID, newSyncID)
	require.NoError(t, err)
	_, err = attached.DetachFile("attached")
	require.NoError(t, err)

	require.NoError(t, incrementalexpansion.ApplyIncrementalExpansionFromDiff(ctx, newFile, newSyncID, upsertsSyncID, deletionsSyncID))

	// EXPECTED: connector truth without edge, fully expanded (no derived grants)
	expectedFile, err := dotc1z.NewC1ZFile(ctx, expectedPath)
	require.NoError(t, err)
	defer expectedFile.Close(ctx)
	expectedSyncID, err := expectedFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, expectedFile.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, expectedFile.PutResources(ctx, g1, g2, u1))
	require.NoError(t, expectedFile.PutEntitlements(ctx, e1, e2))
	require.NoError(t, expectedFile.PutGrants(ctx, grantU1E1 /* no edge */))
	require.NoError(t, expectedFile.EndSync(ctx))
	require.NoError(t, runFullExpansion(ctx, expectedFile, expectedSyncID))

	got, err := loadGrantSourcesByKey(ctx, newFile, newSyncID)
	require.NoError(t, err)
	want, err := loadGrantSourcesByKey(ctx, expectedFile, expectedSyncID)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestIncrementalExpansion_DirectGrantRemovedFromSource(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	oldPath := filepath.Join(tmpDir, "old_remove_direct.c1z")
	newPath := filepath.Join(tmpDir, "new_remove_direct.c1z")
	expectedPath := filepath.Join(tmpDir, "expected_remove_direct.c1z")

	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()

	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(), DisplayName: "G1"}.Build()
	g2 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g2"}.Build(), DisplayName: "G2"}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(), DisplayName: "U1"}.Build()

	e1 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g1, "member"), Resource: g1, Slug: "member", DisplayName: "member"}.Build()
	e2 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g2, "member"), Resource: g2, Slug: "member", DisplayName: "member"}.Build()

	directGrantID := batonGrant.NewGrantID(u1, e1)
	grantU1E1 := v2.Grant_builder{Id: directGrantID, Entitlement: e1, Principal: u1}.Build()

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

	// OLD: U1 → E1, edge E1 → E2, expanded (U1 → E2 derived)
	oldFile, err := dotc1z.NewC1ZFile(ctx, oldPath)
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

	// NEW: same as OLD expanded, but remove the direct grant U1 → E1
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
	pageToken := ""
	for {
		resp, err := oldFile.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{PageToken: pageToken}.Build())
		require.NoError(t, err)
		for _, g := range resp.GetList() {
			if g.GetId() == directGrantID {
				continue // skip the direct grant we're removing
			}
			require.NoError(t, newFile.PutGrants(ctx, g))
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	require.NoError(t, newFile.EndSync(ctx))

	attached, err := newFile.AttachFile(oldFile, "attached")
	require.NoError(t, err)
	upsertsSyncID, deletionsSyncID, err := attached.GenerateSyncDiffFromFile(ctx, oldSyncID, newSyncID)
	require.NoError(t, err)
	_, err = attached.DetachFile("attached")
	require.NoError(t, err)

	require.NoError(t, incrementalexpansion.ApplyIncrementalExpansionFromDiff(ctx, newFile, newSyncID, upsertsSyncID, deletionsSyncID))

	// EXPECTED: connector truth without the direct grant, fully expanded (U1 → E2 should be gone)
	expectedFile, err := dotc1z.NewC1ZFile(ctx, expectedPath)
	require.NoError(t, err)
	defer expectedFile.Close(ctx)
	expectedSyncID, err := expectedFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, expectedFile.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, expectedFile.PutResources(ctx, g1, g2, u1))
	require.NoError(t, expectedFile.PutEntitlements(ctx, e1, e2))
	require.NoError(t, expectedFile.PutGrants(ctx, grantG1E2 /* no U1 → E1 */))
	require.NoError(t, expectedFile.EndSync(ctx))
	require.NoError(t, runFullExpansion(ctx, expectedFile, expectedSyncID))

	got, err := loadGrantSourcesByKey(ctx, newFile, newSyncID)
	require.NoError(t, err)
	want, err := loadGrantSourcesByKey(ctx, expectedFile, expectedSyncID)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestIncrementalExpansion_DirectGrantBecomesSourceless(t *testing.T) {
	// When a direct grant (no GrantImmutable) loses all expansion sources,
	// the grant should persist with sources=nil, matching a fresh full expansion.
	//
	// The expander adds a "self-source" during expansion to mark direct grants,
	// but when all expansion sources are removed, we also remove the self-source
	// to ensure incremental expansion produces the same result as full expansion.

	ctx := context.Background()
	tmpDir := t.TempDir()

	oldPath := filepath.Join(tmpDir, "old_sourceless.c1z")
	newPath := filepath.Join(tmpDir, "new_sourceless.c1z")
	expectedPath := filepath.Join(tmpDir, "expected_sourceless.c1z")

	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()

	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(), DisplayName: "G1"}.Build()
	g2 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g2"}.Build(), DisplayName: "G2"}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(), DisplayName: "U1"}.Build()

	e1 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g1, "member"), Resource: g1, Slug: "member", DisplayName: "member"}.Build()
	e2 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g2, "member"), Resource: g2, Slug: "member", DisplayName: "member"}.Build()

	// U1 is directly a member of both E1 and E2
	grantU1E1 := v2.Grant_builder{Id: batonGrant.NewGrantID(u1, e1), Entitlement: e1, Principal: u1}.Build()
	grantU1E2 := v2.Grant_builder{Id: batonGrant.NewGrantID(u1, e2), Entitlement: e2, Principal: u1}.Build()

	// Edge E1 → E2 via nesting grant
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

	// OLD: U1 → E1 (direct), U1 → E2 (direct), edge E1 → E2
	// After expansion: U1 → E2 acquires sources={E1}
	oldFile, err := dotc1z.NewC1ZFile(ctx, oldPath)
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

	// NEW: same as OLD expanded, but remove the edge (nesting grant)
	// U1 → E2 should lose sources but remain as a direct grant
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
	pageToken := ""
	for {
		resp, err := oldFile.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{PageToken: pageToken}.Build())
		require.NoError(t, err)
		for _, g := range resp.GetList() {
			if g.GetId() == nestingID {
				continue // remove the edge
			}
			require.NoError(t, newFile.PutGrants(ctx, g))
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	require.NoError(t, newFile.EndSync(ctx))

	attached, err := newFile.AttachFile(oldFile, "attached")
	require.NoError(t, err)
	upsertsSyncID, deletionsSyncID, err := attached.GenerateSyncDiffFromFile(ctx, oldSyncID, newSyncID)
	require.NoError(t, err)
	_, err = attached.DetachFile("attached")
	require.NoError(t, err)

	require.NoError(t, incrementalexpansion.ApplyIncrementalExpansionFromDiff(ctx, newFile, newSyncID, upsertsSyncID, deletionsSyncID))

	// EXPECTED: U1 → E2 still exists (it's a direct grant), but with no sources
	expectedFile, err := dotc1z.NewC1ZFile(ctx, expectedPath)
	require.NoError(t, err)
	defer expectedFile.Close(ctx)
	expectedSyncID, err := expectedFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, expectedFile.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, expectedFile.PutResources(ctx, g1, g2, u1))
	require.NoError(t, expectedFile.PutEntitlements(ctx, e1, e2))
	require.NoError(t, expectedFile.PutGrants(ctx, grantU1E1, grantU1E2 /* no edge, U1→E2 persists without sources */))
	require.NoError(t, expectedFile.EndSync(ctx))
	require.NoError(t, runFullExpansion(ctx, expectedFile, expectedSyncID))

	got, err := loadGrantSourcesByKey(ctx, newFile, newSyncID)
	require.NoError(t, err)
	want, err := loadGrantSourcesByKey(ctx, expectedFile, expectedSyncID)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestIncrementalExpansion_CycleEdgeRemoval(t *testing.T) {
	// Test removing an edge in a cycle: E1 → E2 → E1 (bidirectional cycle)
	// When one edge is removed, the cycle is broken.
	ctx := context.Background()
	tmpDir := t.TempDir()

	oldPath := filepath.Join(tmpDir, "old_cycle.c1z")
	newPath := filepath.Join(tmpDir, "new_cycle.c1z")
	expectedPath := filepath.Join(tmpDir, "expected_cycle.c1z")

	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()

	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(), DisplayName: "G1"}.Build()
	g2 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g2"}.Build(), DisplayName: "G2"}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(), DisplayName: "U1"}.Build()

	e1 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g1, "member"), Resource: g1, Slug: "member", DisplayName: "member"}.Build()
	e2 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g2, "member"), Resource: g2, Slug: "member", DisplayName: "member"}.Build()

	grantU1E1 := v2.Grant_builder{Id: batonGrant.NewGrantID(u1, e1), Entitlement: e1, Principal: u1}.Build()

	// Edge E1 → E2
	grantG1E2ID := batonGrant.NewGrantID(g1, e2)
	grantG1E2 := v2.Grant_builder{
		Id:          grantG1E2ID,
		Entitlement: e2,
		Principal:   g1,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds:  []string{e1.GetId()},
			Shallow:         false,
			ResourceTypeIds: []string{"user"},
		}.Build()),
	}.Build()

	// Edge E2 → E1 (creates a cycle)
	grantG2E1ID := batonGrant.NewGrantID(g2, e1)
	grantG2E1 := v2.Grant_builder{
		Id:          grantG2E1ID,
		Entitlement: e1,
		Principal:   g2,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds:  []string{e2.GetId()},
			Shallow:         false,
			ResourceTypeIds: []string{"user"},
		}.Build()),
	}.Build()

	// OLD: U1 → E1, edges E1 → E2 and E2 → E1 (cycle)
	oldFile, err := dotc1z.NewC1ZFile(ctx, oldPath)
	require.NoError(t, err)
	defer oldFile.Close(ctx)

	oldSyncID, err := oldFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, oldFile.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, oldFile.PutResources(ctx, g1, g2, u1))
	require.NoError(t, oldFile.PutEntitlements(ctx, e1, e2))
	require.NoError(t, oldFile.PutGrants(ctx, grantU1E1, grantG1E2, grantG2E1))
	require.NoError(t, oldFile.EndSync(ctx))
	require.NoError(t, runFullExpansion(ctx, oldFile, oldSyncID))

	// NEW: same as OLD expanded, but remove one edge (E2 → E1) to break the cycle
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
	pageToken := ""
	for {
		resp, err := oldFile.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{PageToken: pageToken}.Build())
		require.NoError(t, err)
		for _, g := range resp.GetList() {
			if g.GetId() == grantG2E1ID {
				continue // remove E2 → E1 edge
			}
			require.NoError(t, newFile.PutGrants(ctx, g))
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	require.NoError(t, newFile.EndSync(ctx))

	attached, err := newFile.AttachFile(oldFile, "attached")
	require.NoError(t, err)
	upsertsSyncID, deletionsSyncID, err := attached.GenerateSyncDiffFromFile(ctx, oldSyncID, newSyncID)
	require.NoError(t, err)
	_, err = attached.DetachFile("attached")
	require.NoError(t, err)

	require.NoError(t, incrementalexpansion.ApplyIncrementalExpansionFromDiff(ctx, newFile, newSyncID, upsertsSyncID, deletionsSyncID))

	// EXPECTED: connector truth with only E1 → E2 edge (no cycle), fully expanded
	expectedFile, err := dotc1z.NewC1ZFile(ctx, expectedPath)
	require.NoError(t, err)
	defer expectedFile.Close(ctx)
	expectedSyncID, err := expectedFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, expectedFile.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, expectedFile.PutResources(ctx, g1, g2, u1))
	require.NoError(t, expectedFile.PutEntitlements(ctx, e1, e2))
	require.NoError(t, expectedFile.PutGrants(ctx, grantU1E1, grantG1E2 /* no E2→E1 */))
	require.NoError(t, expectedFile.EndSync(ctx))
	require.NoError(t, runFullExpansion(ctx, expectedFile, expectedSyncID))

	got, err := loadGrantSourcesByKey(ctx, newFile, newSyncID)
	require.NoError(t, err)
	want, err := loadGrantSourcesByKey(ctx, expectedFile, expectedSyncID)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestIncrementalExpansion_PrincipalTypeFilterMismatch(t *testing.T) {
	// Test that PrincipalResourceTypeIDs filter excludes non-matching principals.
	// Edge E1 → E2 with filter ["user"] should only propagate user grants, not group grants.
	ctx := context.Background()
	tmpDir := t.TempDir()

	oldPath := filepath.Join(tmpDir, "old_filter.c1z")
	newPath := filepath.Join(tmpDir, "new_filter.c1z")
	expectedPath := filepath.Join(tmpDir, "expected_filter.c1z")

	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()

	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(), DisplayName: "G1"}.Build()
	g2 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g2"}.Build(), DisplayName: "G2"}.Build()
	g3 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g3"}.Build(), DisplayName: "G3"}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(), DisplayName: "U1"}.Build()

	e1 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g1, "member"), Resource: g1, Slug: "member", DisplayName: "member"}.Build()
	e2 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g2, "member"), Resource: g2, Slug: "member", DisplayName: "member"}.Build()

	// U1 (user) → E1 - should propagate
	grantU1E1 := v2.Grant_builder{Id: batonGrant.NewGrantID(u1, e1), Entitlement: e1, Principal: u1}.Build()
	// G3 (group) → E1 - should NOT propagate (filter is ["user"])
	grantG3E1 := v2.Grant_builder{Id: batonGrant.NewGrantID(g3, e1), Entitlement: e1, Principal: g3}.Build()

	// Edge E1 → E2 with filter ["user"]
	nestingID := batonGrant.NewGrantID(g1, e2)
	grantG1E2 := v2.Grant_builder{
		Id:          nestingID,
		Entitlement: e2,
		Principal:   g1,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds:  []string{e1.GetId()},
			Shallow:         false,
			ResourceTypeIds: []string{"user"}, // only users propagate
		}.Build()),
	}.Build()

	// OLD: U1 → E1, G3 → E1, edge E1 → E2 (filter=user)
	// After expansion: only U1 → E2 (G3 is excluded by filter)
	oldFile, err := dotc1z.NewC1ZFile(ctx, oldPath)
	require.NoError(t, err)
	defer oldFile.Close(ctx)

	oldSyncID, err := oldFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, oldFile.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, oldFile.PutResources(ctx, g1, g2, g3, u1))
	require.NoError(t, oldFile.PutEntitlements(ctx, e1, e2))
	require.NoError(t, oldFile.PutGrants(ctx, grantU1E1, grantG3E1, grantG1E2))
	require.NoError(t, oldFile.EndSync(ctx))
	require.NoError(t, runFullExpansion(ctx, oldFile, oldSyncID))

	// NEW: same as OLD expanded, but remove the edge
	// The derived grant U1 → E2 should be deleted, but G3 → E1 should remain unchanged
	newFile, err := dotc1z.NewC1ZFile(ctx, newPath)
	require.NoError(t, err)
	defer newFile.Close(ctx)

	newSyncID, err := newFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, newFile.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, newFile.PutResources(ctx, g1, g2, g3, u1))
	require.NoError(t, newFile.PutEntitlements(ctx, e1, e2))

	require.NoError(t, oldFile.SetSyncID(ctx, ""))
	require.NoError(t, oldFile.ViewSync(ctx, oldSyncID))
	pageToken := ""
	for {
		resp, err := oldFile.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{PageToken: pageToken}.Build())
		require.NoError(t, err)
		for _, g := range resp.GetList() {
			if g.GetId() == nestingID {
				continue // remove the edge
			}
			require.NoError(t, newFile.PutGrants(ctx, g))
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	require.NoError(t, newFile.EndSync(ctx))

	attached, err := newFile.AttachFile(oldFile, "attached")
	require.NoError(t, err)
	upsertsSyncID, deletionsSyncID, err := attached.GenerateSyncDiffFromFile(ctx, oldSyncID, newSyncID)
	require.NoError(t, err)
	_, err = attached.DetachFile("attached")
	require.NoError(t, err)

	require.NoError(t, incrementalexpansion.ApplyIncrementalExpansionFromDiff(ctx, newFile, newSyncID, upsertsSyncID, deletionsSyncID))

	// EXPECTED: connector truth without edge, fully expanded
	// U1 → E1 remains, G3 → E1 remains, no derived grants
	expectedFile, err := dotc1z.NewC1ZFile(ctx, expectedPath)
	require.NoError(t, err)
	defer expectedFile.Close(ctx)
	expectedSyncID, err := expectedFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, expectedFile.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, expectedFile.PutResources(ctx, g1, g2, g3, u1))
	require.NoError(t, expectedFile.PutEntitlements(ctx, e1, e2))
	require.NoError(t, expectedFile.PutGrants(ctx, grantU1E1, grantG3E1 /* no edge */))
	require.NoError(t, expectedFile.EndSync(ctx))
	require.NoError(t, runFullExpansion(ctx, expectedFile, expectedSyncID))

	got, err := loadGrantSourcesByKey(ctx, newFile, newSyncID)
	require.NoError(t, err)
	want, err := loadGrantSourcesByKey(ctx, expectedFile, expectedSyncID)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

// TestIncrementalExpansion_FullPartialCompactDiff tests the realistic workflow:
// 1. Full sync with full expansion
// 2. First partial sync (new data, no expansion)
// 3. Compaction (merge partial into full)
// 4. Second partial sync (more new data, no expansion)
// 5. Compaction (merge second partial into full)
// 6. Generate diff between old full and compacted
// 7. Apply incremental expansion using the diff
// 8. Compare against fresh full expansion of the compacted state.
func TestIncrementalExpansion_FullPartialCompactDiff(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	basePath := filepath.Join(tmpDir, "base.c1z")
	partial1Path := filepath.Join(tmpDir, "partial1.c1z")
	partial2Path := filepath.Join(tmpDir, "partial2.c1z")
	expectedPath := filepath.Join(tmpDir, "expected.c1z")

	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()

	// Resources
	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(), DisplayName: "G1"}.Build()
	g2 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g2"}.Build(), DisplayName: "G2"}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(), DisplayName: "U1"}.Build()
	u2 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u2"}.Build(), DisplayName: "U2"}.Build()
	u3 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u3"}.Build(), DisplayName: "U3"}.Build()

	// Entitlements
	e1 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g1, "member"), Resource: g1, Slug: "member", DisplayName: "member"}.Build()
	e2 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g2, "member"), Resource: g2, Slug: "member", DisplayName: "member"}.Build()

	// Grants
	// U1 is a direct member of G1
	grantU1E1 := v2.Grant_builder{Id: batonGrant.NewGrantID(u1, e1), Entitlement: e1, Principal: u1}.Build()
	// G1 is a member of G2 with expansion (G1 members propagate to G2)
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
	// U2 is a direct member of G1 (added in first partial sync)
	grantU2E1 := v2.Grant_builder{Id: batonGrant.NewGrantID(u2, e1), Entitlement: e1, Principal: u2}.Build()
	// U3 is a direct member of G1 (added in second partial sync)
	grantU3E1 := v2.Grant_builder{Id: batonGrant.NewGrantID(u3, e1), Entitlement: e1, Principal: u3}.Build()

	// ==========================================================================
	// STEP 1: Create BASE with full sync + full expansion
	// Initial state: U1 → E1, G1 → E2 (expandable). After expansion: U1 → E2 (derived).
	// ==========================================================================
	baseFile, err := dotc1z.NewC1ZFile(ctx, basePath)
	require.NoError(t, err)
	defer baseFile.Close(ctx)

	baseSyncID, err := baseFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, baseFile.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, baseFile.PutResources(ctx, g1, g2, u1))
	require.NoError(t, baseFile.PutEntitlements(ctx, e1, e2))
	require.NoError(t, baseFile.PutGrants(ctx, grantU1E1, grantG1E2))
	require.NoError(t, baseFile.EndSync(ctx))

	// Run full expansion on base
	require.NoError(t, runFullExpansion(ctx, baseFile, baseSyncID))

	// Verify base expansion worked: U1 should have derived membership in E2
	baseGrants, err := loadGrantSourcesByKey(ctx, baseFile, baseSyncID)
	require.NoError(t, err)
	require.Contains(t, baseGrants, "group:g2:member|user|u1", "U1 should have derived membership in G2 after base expansion")

	// ==========================================================================
	// STEP 2: Create FIRST PARTIAL sync with new user U2 as member of G1
	// Partial syncs don't run expansion.
	// ==========================================================================
	partial1File, err := dotc1z.NewC1ZFile(ctx, partial1Path)
	require.NoError(t, err)
	defer partial1File.Close(ctx)

	partial1SyncID, err := partial1File.StartNewSync(ctx, connectorstore.SyncTypePartial, "")
	require.NoError(t, err)
	// Partial sync: just the new user and their grant
	require.NoError(t, partial1File.PutResourceTypes(ctx, userRT))
	require.NoError(t, partial1File.PutResources(ctx, u2))
	require.NoError(t, partial1File.PutGrants(ctx, grantU2E1))
	require.NoError(t, partial1File.EndSync(ctx))

	// ==========================================================================
	// STEP 3: Compact first partial into base
	// After compaction, base has the new user U2 and grant U2→E1.
	// Note: CompactTable uses hardcoded "attached" as the database alias.
	// ==========================================================================
	compact1Attached, err := baseFile.AttachFile(partial1File, "attached")
	require.NoError(t, err)
	require.NoError(t, compact1Attached.CompactResourceTypes(ctx, baseSyncID, partial1SyncID))
	require.NoError(t, compact1Attached.CompactResources(ctx, baseSyncID, partial1SyncID))
	require.NoError(t, compact1Attached.CompactEntitlements(ctx, baseSyncID, partial1SyncID))
	require.NoError(t, compact1Attached.CompactGrants(ctx, baseSyncID, partial1SyncID))
	_, err = compact1Attached.DetachFile("attached")
	require.NoError(t, err)

	// ==========================================================================
	// STEP 4: Create SECOND PARTIAL sync with new user U3 as member of G1
	// ==========================================================================
	partial2File, err := dotc1z.NewC1ZFile(ctx, partial2Path)
	require.NoError(t, err)
	defer partial2File.Close(ctx)

	partial2SyncID, err := partial2File.StartNewSync(ctx, connectorstore.SyncTypePartial, "")
	require.NoError(t, err)
	// Partial sync: just the new user and their grant
	require.NoError(t, partial2File.PutResourceTypes(ctx, userRT))
	require.NoError(t, partial2File.PutResources(ctx, u3))
	require.NoError(t, partial2File.PutGrants(ctx, grantU3E1))
	require.NoError(t, partial2File.EndSync(ctx))

	// ==========================================================================
	// STEP 5: Compact second partial into base
	// After compaction, base has both U2 and U3 as G1 members.
	// ==========================================================================
	compact2Attached, err := baseFile.AttachFile(partial2File, "attached")
	require.NoError(t, err)
	require.NoError(t, compact2Attached.CompactResourceTypes(ctx, baseSyncID, partial2SyncID))
	require.NoError(t, compact2Attached.CompactResources(ctx, baseSyncID, partial2SyncID))
	require.NoError(t, compact2Attached.CompactEntitlements(ctx, baseSyncID, partial2SyncID))
	require.NoError(t, compact2Attached.CompactGrants(ctx, baseSyncID, partial2SyncID))
	_, err = compact2Attached.DetachFile("attached")
	require.NoError(t, err)

	// Sanity-check the compacted state before incremental expansion:
	// - U2/U3 direct memberships in G1 should exist (from partial syncs + compaction).
	// - U2/U3 derived memberships in G2 should NOT exist yet (because we haven't re-expanded).
	compactedBefore, err := loadGrantSourcesByKey(ctx, baseFile, baseSyncID)
	require.NoError(t, err)
	require.Contains(t, compactedBefore, "group:g1:member|user|u2")
	require.Contains(t, compactedBefore, "group:g1:member|user|u3")
	require.NotContains(t, compactedBefore, "group:g2:member|user|u2")
	require.NotContains(t, compactedBefore, "group:g2:member|user|u3")

	// ==========================================================================
	// STEP 6: Generate diff between pre-compaction and post-compaction states
	// We need a "snapshot" of the old state to diff against. In practice, this would
	// be the previous sync. Here we simulate by creating a copy of the old state.
	// ==========================================================================
	// For this test, we'll create a separate "old" file representing pre-compaction state
	oldPath := filepath.Join(tmpDir, "old_snapshot.c1z")
	oldFile, err := dotc1z.NewC1ZFile(ctx, oldPath)
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

	// Now generate diff: base (compacted) is NEW, oldFile is OLD
	// Note: GenerateSyncDiffFromFile uses hardcoded "attached" as the database alias.
	diffAttached, err := baseFile.AttachFile(oldFile, "attached")
	require.NoError(t, err)
	upsertsSyncID, deletionsSyncID, err := diffAttached.GenerateSyncDiffFromFile(ctx, oldSyncID, baseSyncID)
	require.NoError(t, err)
	_, err = diffAttached.DetachFile("attached")
	require.NoError(t, err)

	// Sanity-check the upserts diff: it should include the NEW direct grants (U2/U3→E1),
	// but it should not already contain the derived grants (U2/U3→E2).
	upserts, err := loadGrantSourcesByKey(ctx, baseFile, upsertsSyncID)
	require.NoError(t, err)
	require.Contains(t, upserts, "group:g1:member|user|u2")
	require.Contains(t, upserts, "group:g1:member|user|u3")
	require.NotContains(t, upserts, "group:g2:member|user|u2")
	require.NotContains(t, upserts, "group:g2:member|user|u3")

	// ==========================================================================
	// STEP 7: Apply incremental expansion using the diff
	// This should detect the new grants U2→E1 and U3→E1 and propagate them through the expansion graph.
	// ==========================================================================
	require.NoError(t, incrementalexpansion.ApplyIncrementalExpansionFromDiff(ctx, baseFile, baseSyncID, upsertsSyncID, deletionsSyncID))

	// ==========================================================================
	// STEP 8: Create EXPECTED file with fresh full expansion of the compacted state
	// ==========================================================================
	expectedFile, err := dotc1z.NewC1ZFile(ctx, expectedPath)
	require.NoError(t, err)
	defer expectedFile.Close(ctx)

	expectedSyncID, err := expectedFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, expectedFile.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, expectedFile.PutResources(ctx, g1, g2, u1, u2, u3))
	require.NoError(t, expectedFile.PutEntitlements(ctx, e1, e2))
	require.NoError(t, expectedFile.PutGrants(ctx, grantU1E1, grantG1E2, grantU2E1, grantU3E1))
	require.NoError(t, expectedFile.EndSync(ctx))
	require.NoError(t, runFullExpansion(ctx, expectedFile, expectedSyncID))

	// ==========================================================================
	// STEP 9: Compare incremental result against expected
	// ==========================================================================
	got, err := loadGrantSourcesByKey(ctx, baseFile, baseSyncID)
	require.NoError(t, err)
	want, err := loadGrantSourcesByKey(ctx, expectedFile, expectedSyncID)
	require.NoError(t, err)

	// U1, U2, and U3 should all have derived membership in G2
	require.Contains(t, want, "group:g2:member|user|u1", "Expected: U1 should have derived membership in G2")
	require.Contains(t, want, "group:g2:member|user|u2", "Expected: U2 should have derived membership in G2")
	require.Contains(t, want, "group:g2:member|user|u3", "Expected: U3 should have derived membership in G2")

	require.Equal(t, want, got, "Incremental expansion should match fresh full expansion")
}
