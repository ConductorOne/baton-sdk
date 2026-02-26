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

// diffAndApplyIncremental is a test helper that runs the full incremental expansion pipeline.
func diffAndApplyIncremental(t *testing.T, ctx context.Context, newFile *dotc1z.C1File, oldFile *dotc1z.C1File, oldSyncID, newSyncID string) {
	t.Helper()
	_, err := incrementalexpansion.Run(ctx, newFile, incrementalexpansion.RunParams{
		OldFile:   oldFile,
		OldSyncID: oldSyncID,
		NewSyncID: newSyncID,
	})
	require.NoError(t, err)
}

func runFullExpansion(ctx context.Context, c1f *dotc1z.C1File, syncID string) error {
	// Mark the sync as supporting diff operations (SQL-layer data is ready).
	if err := c1f.SetSupportsDiff(ctx, syncID); err != nil {
		return err
	}

	graph := expand.NewEntitlementGraph(ctx)

	pageToken := ""
	for {
		resp, err := c1f.ListGrantsInternal(ctx, connectorstore.GrantListOptions{
			Mode:           connectorstore.GrantListModeExpansion,
			SyncID:         syncID,
			PageToken:      pageToken,
			ExpandableOnly: true,
		})
		if err != nil {
			return err
		}
		for _, row := range resp.Rows {
			def := row.Expansion
			if def == nil {
				continue
			}

			// defs, next, err := c1f.ListExpandableGrants(
			// 	ctx,
			// 	dotc1z.WithExpandableGrantsSyncID(syncID),
			// 	dotc1z.WithExpandableGrantsPageToken(pageToken),
			// 	dotc1z.WithExpandableGrantsNeedsExpansionOnly(false),
			// )

			for _, src := range def.SourceEntitlementIDs {
				graph.AddEntitlementID(def.TargetEntitlementID)
				graph.AddEntitlementID(src)
				if err := graph.AddEdge(ctx, src, def.TargetEntitlementID, def.Shallow, def.ResourceTypeIDs); err != nil {
					return err
				}
			}
		}
		if resp.NextPageToken == "" {
			break
		}
		pageToken = resp.NextPageToken
	}

	graph.Loaded = true
	if err := graph.FixCycles(ctx); err != nil {
		return err
	}

	return expand.NewExpander(c1f, graph).WithSyncID(syncID).Run(ctx)
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
			for s, src := range g.GetSources().GetSources() {
				srcs[s] = src.GetIsDirect()
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

// listGrantsWithStoredExpansion returns grants from the currently selected/viewed sync
// and rehydrates GrantExpandable from internal expansion columns for test copy flows.
func listGrantsWithStoredExpansion(ctx context.Context, c1f *dotc1z.C1File) ([]*v2.Grant, error) {
	out := make([]*v2.Grant, 0, 1024)
	pageToken := ""
	for {
		resp, err := c1f.ListGrantsInternal(ctx, connectorstore.GrantListOptions{
			Mode:      connectorstore.GrantListModePayloadWithExpansion,
			PageToken: pageToken,
		})
		if err != nil {
			return nil, err
		}
		for _, row := range resp.Rows {
			g := row.Grant
			if g == nil {
				continue
			}
			if def := row.Expansion; def != nil {
				annos := annotations.Annotations(g.GetAnnotations())
				annos.Append(v2.GrantExpandable_builder{
					EntitlementIds:  append([]string(nil), def.SourceEntitlementIDs...),
					Shallow:         def.Shallow,
					ResourceTypeIds: append([]string(nil), def.ResourceTypeIDs...),
				}.Build())
				g.SetAnnotations(annos)
			}
			out = append(out, g)
		}
		if resp.NextPageToken == "" {
			break
		}
		pageToken = resp.NextPageToken
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
	diffAndApplyIncremental(t, ctx, newFile, oldFile, oldSyncID, newSyncID)

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

	oldFile, err := dotc1z.NewC1ZFile(ctx, oldPath, dotc1z.WithPragma("journal_mode", "WAL"), dotc1z.WithPragma("locking_mode", "normal"))
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
	grantsToCopy, err := listGrantsWithStoredExpansion(ctx, oldFile)
	require.NoError(t, err)
	for _, g := range grantsToCopy {
		if g.GetId() == nestingGrantID1 {
			continue
		}
		require.NoError(t, newFile.PutGrants(ctx, g))
	}
	require.NoError(t, newFile.EndSync(ctx))

	diffAndApplyIncremental(t, ctx, newFile, oldFile, oldSyncID, newSyncID)

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
	grantsToCopy, err := listGrantsWithStoredExpansion(ctx, oldFile)
	require.NoError(t, err)
	for _, g := range grantsToCopy {
		require.NoError(t, newFile.PutGrants(ctx, g))
	}
	require.NoError(t, newFile.EndSync(ctx))

	// Should be a no-op.
	diffAndApplyIncremental(t, ctx, newFile, oldFile, oldSyncID, newSyncID)

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
	oldFile, err := dotc1z.NewC1ZFile(ctx, oldPath, dotc1z.WithPragma("journal_mode", "WAL"), dotc1z.WithPragma("locking_mode", "normal"))
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

	diffAndApplyIncremental(t, ctx, newFile, oldFile, oldSyncID, newSyncID)

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
	oldFile, err := dotc1z.NewC1ZFile(ctx, oldPath, dotc1z.WithPragma("journal_mode", "WAL"), dotc1z.WithPragma("locking_mode", "normal"))
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

	diffAndApplyIncremental(t, ctx, newFile, oldFile, oldSyncID, newSyncID)

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
	oldFile, err := dotc1z.NewC1ZFile(ctx, oldPath, dotc1z.WithPragma("journal_mode", "WAL"), dotc1z.WithPragma("locking_mode", "normal"))
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

	diffAndApplyIncremental(t, ctx, newFile, oldFile, oldSyncID, newSyncID)

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
	oldFile, err := dotc1z.NewC1ZFile(ctx, oldPath, dotc1z.WithPragma("journal_mode", "WAL"), dotc1z.WithPragma("locking_mode", "normal"))
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

	diffAndApplyIncremental(t, ctx, newFile, oldFile, oldSyncID, newSyncID)

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
	oldFile, err := dotc1z.NewC1ZFile(ctx, oldPath, dotc1z.WithPragma("journal_mode", "WAL"), dotc1z.WithPragma("locking_mode", "normal"))
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

	diffAndApplyIncremental(t, ctx, newFile, oldFile, oldSyncID, newSyncID)

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
	oldFile, err := dotc1z.NewC1ZFile(ctx, oldPath, dotc1z.WithPragma("journal_mode", "WAL"), dotc1z.WithPragma("locking_mode", "normal"))
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
	grantsToCopy, err := listGrantsWithStoredExpansion(ctx, oldFile)
	require.NoError(t, err)
	for _, g := range grantsToCopy {
		require.NoError(t, newFile.PutGrants(ctx, g))
	}
	// Add the new direct grant
	require.NoError(t, newFile.PutGrants(ctx, grantU2E1))
	require.NoError(t, newFile.EndSync(ctx))

	diffAndApplyIncremental(t, ctx, newFile, oldFile, oldSyncID, newSyncID)

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
	oldFile, err := dotc1z.NewC1ZFile(ctx, oldPath, dotc1z.WithPragma("journal_mode", "WAL"), dotc1z.WithPragma("locking_mode", "normal"))
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

	diffAndApplyIncremental(t, ctx, newFile, oldFile, oldSyncID, newSyncID)

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

	diffAndApplyIncremental(t, ctx, newFile, oldFile, oldSyncID, newSyncID)

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

	diffAndApplyIncremental(t, ctx, newFile, oldFile, oldSyncID, newSyncID)

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
	oldFile, err := dotc1z.NewC1ZFile(ctx, oldPath, dotc1z.WithPragma("journal_mode", "WAL"), dotc1z.WithPragma("locking_mode", "normal"))
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
	grantsToCopy, err := listGrantsWithStoredExpansion(ctx, oldFile)
	require.NoError(t, err)
	for _, g := range grantsToCopy {
		if g.GetId() == grantG2E1ID {
			continue // remove E2 → E1 edge
		}
		require.NoError(t, newFile.PutGrants(ctx, g))
	}
	require.NoError(t, newFile.EndSync(ctx))

	diffAndApplyIncremental(t, ctx, newFile, oldFile, oldSyncID, newSyncID)

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
	oldFile, err := dotc1z.NewC1ZFile(ctx, oldPath, dotc1z.WithPragma("journal_mode", "WAL"), dotc1z.WithPragma("locking_mode", "normal"))
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

	diffAndApplyIncremental(t, ctx, newFile, oldFile, oldSyncID, newSyncID)

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
	partial1File, err := dotc1z.NewC1ZFile(ctx, partial1Path, dotc1z.WithPragma("journal_mode", "WAL"), dotc1z.WithPragma("locking_mode", "normal"))
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
	partial2File, err := dotc1z.NewC1ZFile(ctx, partial2Path, dotc1z.WithPragma("journal_mode", "WAL"), dotc1z.WithPragma("locking_mode", "normal"))
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

	// Now generate diff: base (compacted) is NEW, oldFile is OLD
	result, err := incrementalexpansion.Run(ctx, baseFile, incrementalexpansion.RunParams{
		OldFile:   oldFile,
		OldSyncID: oldSyncID,
		NewSyncID: baseSyncID,
	})
	require.NoError(t, err)
	upsertsSyncID := result.UpsertsSyncID

	// Sanity-check the upserts diff: it should include the NEW direct grants (U2/U3→E1),
	// but it should not already contain the derived grants (U2/U3→E2).
	upserts, err := loadGrantSourcesByKey(ctx, baseFile, upsertsSyncID)
	require.NoError(t, err)
	require.Contains(t, upserts, "group:g1:member|user|u2")
	require.Contains(t, upserts, "group:g1:member|user|u3")
	require.NotContains(t, upserts, "group:g2:member|user|u2")
	require.NotContains(t, upserts, "group:g2:member|user|u3")

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

// TestIncrementalExpansion_DiamondGraph tests a diamond-shaped graph where two
// independent paths converge on the same entitlement. Removing one path should
// retain the derived grant via the other path (with correct sources).
//
//	E1 → E3
//	E2 → E3
//	U1 → E1, U1 → E2
//
// After expansion: U1 → E3 with sources {E1, E2, E3(self)}.
// Remove edge E1→E3 → U1 → E3 should remain with source {E2, E3(self)}.
func TestIncrementalExpansion_DiamondGraph(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	oldPath := filepath.Join(tmpDir, "old_diamond.c1z")
	newPath := filepath.Join(tmpDir, "new_diamond.c1z")
	expectedPath := filepath.Join(tmpDir, "expected_diamond.c1z")

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
	grantU1E2 := v2.Grant_builder{Id: batonGrant.NewGrantID(u1, e2), Entitlement: e2, Principal: u1}.Build()

	// Edge E1 → E3
	nestingE1E3ID := batonGrant.NewGrantID(g1, e3)
	grantG1E3 := v2.Grant_builder{
		Id:          nestingE1E3ID,
		Entitlement: e3,
		Principal:   g1,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds:  []string{e1.GetId()},
			Shallow:         false,
			ResourceTypeIds: []string{"user"},
		}.Build()),
	}.Build()

	// Edge E2 → E3
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

	// OLD (expanded): both diamond paths active
	oldFile, err := dotc1z.NewC1ZFile(ctx, oldPath, dotc1z.WithPragma("journal_mode", "WAL"), dotc1z.WithPragma("locking_mode", "normal"))
	require.NoError(t, err)
	defer oldFile.Close(ctx)

	oldSyncID, err := oldFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, oldFile.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, oldFile.PutResources(ctx, g1, g2, g3, u1))
	require.NoError(t, oldFile.PutEntitlements(ctx, e1, e2, e3))
	require.NoError(t, oldFile.PutGrants(ctx, grantU1E1, grantU1E2, grantG1E3, grantG2E3))
	require.NoError(t, oldFile.EndSync(ctx))
	require.NoError(t, runFullExpansion(ctx, oldFile, oldSyncID))

	// NEW: remove edge E1→E3, keep edge E2→E3
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
	grantsToCopy, err := listGrantsWithStoredExpansion(ctx, oldFile)
	require.NoError(t, err)
	for _, g := range grantsToCopy {
		if g.GetId() == nestingE1E3ID {
			continue
		}
		require.NoError(t, newFile.PutGrants(ctx, g))
	}
	require.NoError(t, newFile.EndSync(ctx))

	diffAndApplyIncremental(t, ctx, newFile, oldFile, oldSyncID, newSyncID)

	// EXPECTED: U1→E3 still exists (via E2→E3 path), with correct sources
	expectedFile, err := dotc1z.NewC1ZFile(ctx, expectedPath)
	require.NoError(t, err)
	defer expectedFile.Close(ctx)
	expectedSyncID, err := expectedFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, expectedFile.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, expectedFile.PutResources(ctx, g1, g2, g3, u1))
	require.NoError(t, expectedFile.PutEntitlements(ctx, e1, e2, e3))
	require.NoError(t, expectedFile.PutGrants(ctx, grantU1E1, grantU1E2, grantG2E3))
	require.NoError(t, expectedFile.EndSync(ctx))
	require.NoError(t, runFullExpansion(ctx, expectedFile, expectedSyncID))

	got, err := loadGrantSourcesByKey(ctx, newFile, newSyncID)
	require.NoError(t, err)
	want, err := loadGrantSourcesByKey(ctx, expectedFile, expectedSyncID)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

// TestIncrementalExpansion_ShallowToDeepToggle tests that changing an edge's shallow flag
// from true to false (making it deep) is correctly handled as a remove+add in the delta.
func TestIncrementalExpansion_ShallowToDeepToggle(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	oldPath := filepath.Join(tmpDir, "old_shallow_toggle.c1z")
	newPath := filepath.Join(tmpDir, "new_shallow_toggle.c1z")
	expectedPath := filepath.Join(tmpDir, "expected_shallow_toggle.c1z")

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

	// Edge E1→E2 (deep)
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

	// Edge E2→E3 was SHALLOW, becomes DEEP
	nestingE2E3ID := batonGrant.NewGrantID(g2, e3)
	grantG2E3Shallow := v2.Grant_builder{
		Id:          nestingE2E3ID,
		Entitlement: e3,
		Principal:   g2,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds:  []string{e2.GetId()},
			Shallow:         true,
			ResourceTypeIds: []string{"user"},
		}.Build()),
	}.Build()
	grantG2E3Deep := v2.Grant_builder{
		Id:          nestingE2E3ID,
		Entitlement: e3,
		Principal:   g2,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds:  []string{e2.GetId()},
			Shallow:         false,
			ResourceTypeIds: []string{"user"},
		}.Build()),
	}.Build()

	// OLD: U1→E1, E1→E2 (deep), E2→E3 (shallow)
	// With shallow: U1 propagates to E2 (via deep E1→E2), but NOT to E3 (shallow blocks it)
	oldFile, err := dotc1z.NewC1ZFile(ctx, oldPath, dotc1z.WithPragma("journal_mode", "WAL"), dotc1z.WithPragma("locking_mode", "normal"))
	require.NoError(t, err)
	defer oldFile.Close(ctx)

	oldSyncID, err := oldFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, oldFile.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, oldFile.PutResources(ctx, g1, g2, g3, u1))
	require.NoError(t, oldFile.PutEntitlements(ctx, e1, e2, e3))
	require.NoError(t, oldFile.PutGrants(ctx, grantU1E1, grantG1E2, grantG2E3Shallow))
	require.NoError(t, oldFile.EndSync(ctx))
	require.NoError(t, runFullExpansion(ctx, oldFile, oldSyncID))

	// NEW: same but E2→E3 is now DEEP
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
	grantsToCopy, err := listGrantsWithStoredExpansion(ctx, oldFile)
	require.NoError(t, err)
	for _, g := range grantsToCopy {
		if g.GetId() == nestingE2E3ID {
			require.NoError(t, newFile.PutGrants(ctx, grantG2E3Deep))
			continue
		}
		require.NoError(t, newFile.PutGrants(ctx, g))
	}
	require.NoError(t, newFile.EndSync(ctx))

	diffAndApplyIncremental(t, ctx, newFile, oldFile, oldSyncID, newSyncID)

	// EXPECTED: E2→E3 is now deep, so U1 should propagate through to E3
	expectedFile, err := dotc1z.NewC1ZFile(ctx, expectedPath)
	require.NoError(t, err)
	defer expectedFile.Close(ctx)
	expectedSyncID, err := expectedFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, expectedFile.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, expectedFile.PutResources(ctx, g1, g2, g3, u1))
	require.NoError(t, expectedFile.PutEntitlements(ctx, e1, e2, e3))
	require.NoError(t, expectedFile.PutGrants(ctx, grantU1E1, grantG1E2, grantG2E3Deep))
	require.NoError(t, expectedFile.EndSync(ctx))
	require.NoError(t, runFullExpansion(ctx, expectedFile, expectedSyncID))

	got, err := loadGrantSourcesByKey(ctx, newFile, newSyncID)
	require.NoError(t, err)
	want, err := loadGrantSourcesByKey(ctx, expectedFile, expectedSyncID)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

// TestIncrementalExpansion_ResourceTypeFilterChange tests that changing the
// PrincipalResourceTypeIDs filter on an edge is handled correctly.
func TestIncrementalExpansion_ResourceTypeFilterChange(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	oldPath := filepath.Join(tmpDir, "old_filter_change.c1z")
	newPath := filepath.Join(tmpDir, "new_filter_change.c1z")
	expectedPath := filepath.Join(tmpDir, "expected_filter_change.c1z")

	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()
	saRT := v2.ResourceType_builder{Id: "serviceaccount", DisplayName: "ServiceAccount"}.Build()

	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(), DisplayName: "G1"}.Build()
	g2 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g2"}.Build(), DisplayName: "G2"}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(), DisplayName: "U1"}.Build()
	sa1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "serviceaccount", Resource: "sa1"}.Build(), DisplayName: "SA1"}.Build()

	e1 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g1, "member"), Resource: g1, Slug: "member", DisplayName: "member"}.Build()
	e2 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g2, "member"), Resource: g2, Slug: "member", DisplayName: "member"}.Build()

	grantU1E1 := v2.Grant_builder{Id: batonGrant.NewGrantID(u1, e1), Entitlement: e1, Principal: u1}.Build()
	grantSA1E1 := v2.Grant_builder{Id: batonGrant.NewGrantID(sa1, e1), Entitlement: e1, Principal: sa1}.Build()

	// Edge E1→E2 with filter ["user"] (only propagates users)
	nestingID := batonGrant.NewGrantID(g1, e2)
	grantG1E2UserOnly := v2.Grant_builder{
		Id:          nestingID,
		Entitlement: e2,
		Principal:   g1,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds:  []string{e1.GetId()},
			Shallow:         false,
			ResourceTypeIds: []string{"user"},
		}.Build()),
	}.Build()

	// Same edge but with filter ["user", "serviceaccount"] (now also propagates SAs)
	grantG1E2UserAndSA := v2.Grant_builder{
		Id:          nestingID,
		Entitlement: e2,
		Principal:   g1,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds:  []string{e1.GetId()},
			Shallow:         false,
			ResourceTypeIds: []string{"user", "serviceaccount"},
		}.Build()),
	}.Build()

	// OLD: filter=["user"], so only U1 propagates
	oldFile, err := dotc1z.NewC1ZFile(ctx, oldPath, dotc1z.WithPragma("journal_mode", "WAL"), dotc1z.WithPragma("locking_mode", "normal"))
	require.NoError(t, err)
	defer oldFile.Close(ctx)

	oldSyncID, err := oldFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, oldFile.PutResourceTypes(ctx, groupRT, userRT, saRT))
	require.NoError(t, oldFile.PutResources(ctx, g1, g2, u1, sa1))
	require.NoError(t, oldFile.PutEntitlements(ctx, e1, e2))
	require.NoError(t, oldFile.PutGrants(ctx, grantU1E1, grantSA1E1, grantG1E2UserOnly))
	require.NoError(t, oldFile.EndSync(ctx))
	require.NoError(t, runFullExpansion(ctx, oldFile, oldSyncID))

	// NEW: filter=["user","serviceaccount"], so both propagate
	newFile, err := dotc1z.NewC1ZFile(ctx, newPath)
	require.NoError(t, err)
	defer newFile.Close(ctx)

	newSyncID, err := newFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, newFile.PutResourceTypes(ctx, groupRT, userRT, saRT))
	require.NoError(t, newFile.PutResources(ctx, g1, g2, u1, sa1))
	require.NoError(t, newFile.PutEntitlements(ctx, e1, e2))

	require.NoError(t, oldFile.SetSyncID(ctx, ""))
	require.NoError(t, oldFile.ViewSync(ctx, oldSyncID))
	grantsToCopy, err := listGrantsWithStoredExpansion(ctx, oldFile)
	require.NoError(t, err)
	for _, g := range grantsToCopy {
		if g.GetId() == nestingID {
			require.NoError(t, newFile.PutGrants(ctx, grantG1E2UserAndSA))
			continue
		}
		require.NoError(t, newFile.PutGrants(ctx, g))
	}
	require.NoError(t, newFile.EndSync(ctx))

	diffAndApplyIncremental(t, ctx, newFile, oldFile, oldSyncID, newSyncID)

	// EXPECTED: both U1 and SA1 should now have derived membership in E2
	expectedFile, err := dotc1z.NewC1ZFile(ctx, expectedPath)
	require.NoError(t, err)
	defer expectedFile.Close(ctx)
	expectedSyncID, err := expectedFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, expectedFile.PutResourceTypes(ctx, groupRT, userRT, saRT))
	require.NoError(t, expectedFile.PutResources(ctx, g1, g2, u1, sa1))
	require.NoError(t, expectedFile.PutEntitlements(ctx, e1, e2))
	require.NoError(t, expectedFile.PutGrants(ctx, grantU1E1, grantSA1E1, grantG1E2UserAndSA))
	require.NoError(t, expectedFile.EndSync(ctx))
	require.NoError(t, runFullExpansion(ctx, expectedFile, expectedSyncID))

	got, err := loadGrantSourcesByKey(ctx, newFile, newSyncID)
	require.NoError(t, err)
	want, err := loadGrantSourcesByKey(ctx, expectedFile, expectedSyncID)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

// TestIncrementalExpansion_ConvergingEdgesRemoval tests two independent edges
// converging on the same destination entitlement. Removing one edge should keep
// derived grants from the surviving path and remove only those from the removed path.
func TestIncrementalExpansion_ConvergingEdgesRemoval(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	oldPath := filepath.Join(tmpDir, "old_multisrc.c1z")
	newPath := filepath.Join(tmpDir, "new_multisrc.c1z")
	expectedPath := filepath.Join(tmpDir, "expected_multisrc.c1z")

	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()

	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(), DisplayName: "G1"}.Build()
	g2 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g2"}.Build(), DisplayName: "G2"}.Build()
	g3 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g3"}.Build(), DisplayName: "G3"}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(), DisplayName: "U1"}.Build()
	u2 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u2"}.Build(), DisplayName: "U2"}.Build()

	e1 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g1, "member"), Resource: g1, Slug: "member", DisplayName: "member"}.Build()
	e2 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g2, "member"), Resource: g2, Slug: "member", DisplayName: "member"}.Build()
	e3 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g3, "member"), Resource: g3, Slug: "member", DisplayName: "member"}.Build()

	grantU1E1 := v2.Grant_builder{Id: batonGrant.NewGrantID(u1, e1), Entitlement: e1, Principal: u1}.Build()
	grantU2E2 := v2.Grant_builder{Id: batonGrant.NewGrantID(u2, e2), Entitlement: e2, Principal: u2}.Build()

	// Note: this is a converging-edges scenario (two separate edge-defining grants),
	// not a single grant with multiple source entitlement IDs.

	// Edge E1→E3 via g1 nesting
	nestingG1E3ID := batonGrant.NewGrantID(g1, e3)
	grantG1E3 := v2.Grant_builder{
		Id:          nestingG1E3ID,
		Entitlement: e3,
		Principal:   g1,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds:  []string{e1.GetId()},
			Shallow:         false,
			ResourceTypeIds: []string{"user"},
		}.Build()),
	}.Build()

	// Edge E2→E3 via g2 nesting
	nestingG2E3ID := batonGrant.NewGrantID(g2, e3)
	grantG2E3 := v2.Grant_builder{
		Id:          nestingG2E3ID,
		Entitlement: e3,
		Principal:   g2,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds:  []string{e2.GetId()},
			Shallow:         false,
			ResourceTypeIds: []string{"user"},
		}.Build()),
	}.Build()

	// OLD: U1→E1, U2→E2, edges E1→E3 and E2→E3.
	// After expansion: U1→E3 (via E1), U2→E3 (via E2)
	oldFile, err := dotc1z.NewC1ZFile(ctx, oldPath, dotc1z.WithPragma("journal_mode", "WAL"), dotc1z.WithPragma("locking_mode", "normal"))
	require.NoError(t, err)
	defer oldFile.Close(ctx)

	oldSyncID, err := oldFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, oldFile.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, oldFile.PutResources(ctx, g1, g2, g3, u1, u2))
	require.NoError(t, oldFile.PutEntitlements(ctx, e1, e2, e3))
	require.NoError(t, oldFile.PutGrants(ctx, grantU1E1, grantU2E2, grantG1E3, grantG2E3))
	require.NoError(t, oldFile.EndSync(ctx))
	require.NoError(t, runFullExpansion(ctx, oldFile, oldSyncID))

	// NEW: remove edge E2→E3, keep E1→E3
	newFile, err := dotc1z.NewC1ZFile(ctx, newPath)
	require.NoError(t, err)
	defer newFile.Close(ctx)

	newSyncID, err := newFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, newFile.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, newFile.PutResources(ctx, g1, g2, g3, u1, u2))
	require.NoError(t, newFile.PutEntitlements(ctx, e1, e2, e3))

	require.NoError(t, oldFile.SetSyncID(ctx, ""))
	require.NoError(t, oldFile.ViewSync(ctx, oldSyncID))
	grantsToCopy, err := listGrantsWithStoredExpansion(ctx, oldFile)
	require.NoError(t, err)
	for _, g := range grantsToCopy {
		if g.GetId() == nestingG2E3ID {
			continue
		}
		require.NoError(t, newFile.PutGrants(ctx, g))
	}
	require.NoError(t, newFile.EndSync(ctx))

	diffAndApplyIncremental(t, ctx, newFile, oldFile, oldSyncID, newSyncID)

	// EXPECTED: U1→E3 remains (via E1), U2→E3 removed
	expectedFile, err := dotc1z.NewC1ZFile(ctx, expectedPath)
	require.NoError(t, err)
	defer expectedFile.Close(ctx)
	expectedSyncID, err := expectedFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, expectedFile.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, expectedFile.PutResources(ctx, g1, g2, g3, u1, u2))
	require.NoError(t, expectedFile.PutEntitlements(ctx, e1, e2, e3))
	require.NoError(t, expectedFile.PutGrants(ctx, grantU1E1, grantU2E2, grantG1E3))
	require.NoError(t, expectedFile.EndSync(ctx))
	require.NoError(t, runFullExpansion(ctx, expectedFile, expectedSyncID))

	got, err := loadGrantSourcesByKey(ctx, newFile, newSyncID)
	require.NoError(t, err)
	want, err := loadGrantSourcesByKey(ctx, expectedFile, expectedSyncID)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

// TestIncrementalExpansion_GrantReplacementAtSource tests that when a direct grant
// at a source entitlement is replaced (U1→E1 removed, U2→E1 added), the incremental
// pipeline correctly removes the old derived grant and creates the new one.
func TestIncrementalExpansion_GrantReplacementAtSource(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	oldPath := filepath.Join(tmpDir, "old_replace.c1z")
	newPath := filepath.Join(tmpDir, "new_replace.c1z")
	expectedPath := filepath.Join(tmpDir, "expected_replace.c1z")

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

	// OLD: U1→E1, edge E1→E2, expanded (U1→E2 derived)
	oldFile, err := dotc1z.NewC1ZFile(ctx, oldPath, dotc1z.WithPragma("journal_mode", "WAL"), dotc1z.WithPragma("locking_mode", "normal"))
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

	// NEW: U1→E1 removed, U2→E1 added, edge still present
	// U1→E2 (derived) should be removed, U2→E2 (derived) should be created
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
	grantsToCopy, err := listGrantsWithStoredExpansion(ctx, oldFile)
	require.NoError(t, err)
	for _, g := range grantsToCopy {
		if g.GetId() == grantU1E1.GetId() {
			continue // remove U1→E1
		}
		require.NoError(t, newFile.PutGrants(ctx, g))
	}
	require.NoError(t, newFile.PutGrants(ctx, grantU2E1)) // add U2→E1
	require.NoError(t, newFile.EndSync(ctx))

	diffAndApplyIncremental(t, ctx, newFile, oldFile, oldSyncID, newSyncID)

	// EXPECTED: U2→E1, edge E1→E2, expanded (U2→E2 derived, no U1→E2)
	expectedFile, err := dotc1z.NewC1ZFile(ctx, expectedPath)
	require.NoError(t, err)
	defer expectedFile.Close(ctx)
	expectedSyncID, err := expectedFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, expectedFile.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, expectedFile.PutResources(ctx, g1, g2, u1, u2))
	require.NoError(t, expectedFile.PutEntitlements(ctx, e1, e2))
	require.NoError(t, expectedFile.PutGrants(ctx, grantU2E1, grantG1E2))
	require.NoError(t, expectedFile.EndSync(ctx))
	require.NoError(t, runFullExpansion(ctx, expectedFile, expectedSyncID))

	got, err := loadGrantSourcesByKey(ctx, newFile, newSyncID)
	require.NoError(t, err)
	want, err := loadGrantSourcesByKey(ctx, expectedFile, expectedSyncID)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

// TestIncrementalExpansion_AddNewEdgeAtEndOfChain tests adding a new edge
// at the end of an existing chain (E1→E2 exists, E2→E3 added).
func TestIncrementalExpansion_AddNewEdgeAtEndOfChain(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	oldPath := filepath.Join(tmpDir, "old_chain_extend.c1z")
	newPath := filepath.Join(tmpDir, "new_chain_extend.c1z")
	expectedPath := filepath.Join(tmpDir, "expected_chain_extend.c1z")

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

	// Existing edge E1→E2
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

	// New edge E2→E3 (added in NEW)
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

	// OLD: U1→E1, E1→E2, expanded (U1→E2 derived)
	oldFile, err := dotc1z.NewC1ZFile(ctx, oldPath, dotc1z.WithPragma("journal_mode", "WAL"), dotc1z.WithPragma("locking_mode", "normal"))
	require.NoError(t, err)
	defer oldFile.Close(ctx)

	oldSyncID, err := oldFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, oldFile.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, oldFile.PutResources(ctx, g1, g2, g3, u1))
	require.NoError(t, oldFile.PutEntitlements(ctx, e1, e2, e3))
	require.NoError(t, oldFile.PutGrants(ctx, grantU1E1, grantG1E2))
	require.NoError(t, oldFile.EndSync(ctx))
	require.NoError(t, runFullExpansion(ctx, oldFile, oldSyncID))

	// NEW: same as OLD expanded + E2→E3 edge added
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
	grantsToCopy, err := listGrantsWithStoredExpansion(ctx, oldFile)
	require.NoError(t, err)
	for _, g := range grantsToCopy {
		require.NoError(t, newFile.PutGrants(ctx, g))
	}
	require.NoError(t, newFile.PutGrants(ctx, grantG2E3))
	require.NoError(t, newFile.EndSync(ctx))

	diffAndApplyIncremental(t, ctx, newFile, oldFile, oldSyncID, newSyncID)

	// EXPECTED: U1→E1, E1→E2, E2→E3, expanded (U1→E2, U1→E3 both derived)
	expectedFile, err := dotc1z.NewC1ZFile(ctx, expectedPath)
	require.NoError(t, err)
	defer expectedFile.Close(ctx)
	expectedSyncID, err := expectedFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, expectedFile.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, expectedFile.PutResources(ctx, g1, g2, g3, u1))
	require.NoError(t, expectedFile.PutEntitlements(ctx, e1, e2, e3))
	require.NoError(t, expectedFile.PutGrants(ctx, grantU1E1, grantG1E2, grantG2E3))
	require.NoError(t, expectedFile.EndSync(ctx))
	require.NoError(t, runFullExpansion(ctx, expectedFile, expectedSyncID))

	got, err := loadGrantSourcesByKey(ctx, newFile, newSyncID)
	require.NoError(t, err)
	want, err := loadGrantSourcesByKey(ctx, expectedFile, expectedSyncID)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

// TestIncrementalExpansion_SourceListChange tests changing the source entitlement
// list on an edge-defining grant. G1 has two entitlements (member, admin). The nesting
// grant starts with source [e1_member] and changes to [e1_member, e1_admin].
// Since both source entitlements are on the same resource (G1 = the nesting grant's
// principal), this is a valid multi-source configuration.
func TestIncrementalExpansion_SourceListChange(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	oldPath := filepath.Join(tmpDir, "old_srclist.c1z")
	newPath := filepath.Join(tmpDir, "new_srclist.c1z")
	expectedPath := filepath.Join(tmpDir, "expected_srclist.c1z")

	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()

	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(), DisplayName: "G1"}.Build()
	g2 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g2"}.Build(), DisplayName: "G2"}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(), DisplayName: "U1"}.Build()
	u2 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u2"}.Build(), DisplayName: "U2"}.Build()

	// G1 has two entitlements: member and admin
	e1Member := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g1, "member"), Resource: g1, Slug: "member", DisplayName: "member"}.Build()
	e1Admin := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g1, "admin"), Resource: g1, Slug: "admin", DisplayName: "admin"}.Build()
	e2 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g2, "member"), Resource: g2, Slug: "member", DisplayName: "member"}.Build()

	// U1 is a member of G1, U2 is an admin of G1
	grantU1E1Member := v2.Grant_builder{Id: batonGrant.NewGrantID(u1, e1Member), Entitlement: e1Member, Principal: u1}.Build()
	grantU2E1Admin := v2.Grant_builder{Id: batonGrant.NewGrantID(u2, e1Admin), Entitlement: e1Admin, Principal: u2}.Build()

	// OLD: nesting grant with source [e1_member] only
	nestingID := batonGrant.NewGrantID(g1, e2)
	grantG1E2OneSrc := v2.Grant_builder{
		Id:          nestingID,
		Entitlement: e2,
		Principal:   g1,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds:  []string{e1Member.GetId()},
			Shallow:         false,
			ResourceTypeIds: []string{"user"},
		}.Build()),
	}.Build()

	// NEW: nesting grant with sources [e1_member, e1_admin]
	grantG1E2TwoSrc := v2.Grant_builder{
		Id:          nestingID,
		Entitlement: e2,
		Principal:   g1,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds:  []string{e1Member.GetId(), e1Admin.GetId()},
			Shallow:         false,
			ResourceTypeIds: []string{"user"},
		}.Build()),
	}.Build()

	// OLD: U1→e1_member, U2→e1_admin, edge [e1_member]→E2
	// After expansion: U1→E2 (via e1_member)
	oldFile, err := dotc1z.NewC1ZFile(ctx, oldPath, dotc1z.WithPragma("journal_mode", "WAL"), dotc1z.WithPragma("locking_mode", "normal"))
	require.NoError(t, err)
	defer oldFile.Close(ctx)

	oldSyncID, err := oldFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, oldFile.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, oldFile.PutResources(ctx, g1, g2, u1, u2))
	require.NoError(t, oldFile.PutEntitlements(ctx, e1Member, e1Admin, e2))
	require.NoError(t, oldFile.PutGrants(ctx, grantU1E1Member, grantU2E1Admin, grantG1E2OneSrc))
	require.NoError(t, oldFile.EndSync(ctx))
	require.NoError(t, runFullExpansion(ctx, oldFile, oldSyncID))

	// NEW: edge now has two sources [e1_member, e1_admin]
	newFile, err := dotc1z.NewC1ZFile(ctx, newPath)
	require.NoError(t, err)
	defer newFile.Close(ctx)

	newSyncID, err := newFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, newFile.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, newFile.PutResources(ctx, g1, g2, u1, u2))
	require.NoError(t, newFile.PutEntitlements(ctx, e1Member, e1Admin, e2))

	require.NoError(t, oldFile.SetSyncID(ctx, ""))
	require.NoError(t, oldFile.ViewSync(ctx, oldSyncID))
	grantsToCopy, err := listGrantsWithStoredExpansion(ctx, oldFile)
	require.NoError(t, err)
	for _, g := range grantsToCopy {
		if g.GetId() == nestingID {
			require.NoError(t, newFile.PutGrants(ctx, grantG1E2TwoSrc))
			continue
		}
		require.NoError(t, newFile.PutGrants(ctx, g))
	}
	require.NoError(t, newFile.EndSync(ctx))

	diffAndApplyIncremental(t, ctx, newFile, oldFile, oldSyncID, newSyncID)

	// EXPECTED: U1→E2 (via e1_member) AND U2→E2 (via e1_admin) should be derived
	expectedFile, err := dotc1z.NewC1ZFile(ctx, expectedPath)
	require.NoError(t, err)
	defer expectedFile.Close(ctx)
	expectedSyncID, err := expectedFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, expectedFile.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, expectedFile.PutResources(ctx, g1, g2, u1, u2))
	require.NoError(t, expectedFile.PutEntitlements(ctx, e1Member, e1Admin, e2))
	require.NoError(t, expectedFile.PutGrants(ctx, grantU1E1Member, grantU2E1Admin, grantG1E2TwoSrc))
	require.NoError(t, expectedFile.EndSync(ctx))
	require.NoError(t, runFullExpansion(ctx, expectedFile, expectedSyncID))

	got, err := loadGrantSourcesByKey(ctx, newFile, newSyncID)
	require.NoError(t, err)
	want, err := loadGrantSourcesByKey(ctx, expectedFile, expectedSyncID)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

// TestIncrementalExpansion_SourcesInDataBlobNoop verifies that when a connector produces
// identical grants across two syncs (a true no-op), the diff does not report false-positive
// changed grant entitlements caused by expansion having written Sources into the data blob.
//
// Scenario:
//   - U1 has a direct grant on both E1 and E2 (destination of edge E1→E2).
//   - After full expansion of the old sync, U1→E2 acquires Sources={E2, E1} in its data blob.
//   - The new sync has the same connector-provided grants (no Sources, since the connector
//     never sets them).
//   - The diff should detect zero changed grant entitlements because nothing changed from the
//     connector's perspective.
//
// Regression target: when Sources leaked into grant data blobs, this scenario falsely reported
// E2 as changed even though connector-provided grants were identical.
func TestIncrementalExpansion_SourcesInDataBlobNoop(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	oldPath := filepath.Join(tmpDir, "old_sources_noop.c1z")
	newPath := filepath.Join(tmpDir, "new_sources_noop.c1z")

	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()

	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(), DisplayName: "G1"}.Build()
	g2 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g2"}.Build(), DisplayName: "G2"}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(), DisplayName: "U1"}.Build()

	e1 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g1, "member"), Resource: g1, Slug: "member", DisplayName: "member"}.Build()
	e2 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g2, "member"), Resource: g2, Slug: "member", DisplayName: "member"}.Build()

	grantU1E1 := v2.Grant_builder{Id: batonGrant.NewGrantID(u1, e1), Entitlement: e1, Principal: u1}.Build()
	grantU1E2 := v2.Grant_builder{Id: batonGrant.NewGrantID(u1, e2), Entitlement: e2, Principal: u1}.Build()
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

	// OLD: connector grants + full expansion.
	// After expansion U1→E2 acquires Sources={E2: IsDirect=true, E1: IsDirect=true}
	// baked into its data blob.
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

	// NEW: exact same connector grants, fresh (no Sources).
	newFile, err := dotc1z.NewC1ZFile(ctx, newPath)
	require.NoError(t, err)
	defer newFile.Close(ctx)

	newSyncID, err := newFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, newFile.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, newFile.PutResources(ctx, g1, g2, u1))
	require.NoError(t, newFile.PutEntitlements(ctx, e1, e2))
	require.NoError(t, newFile.PutGrants(ctx, grantU1E1, grantU1E2, grantG1E2))
	require.NoError(t, newFile.EndSync(ctx))

	attached, err := newFile.AttachFile(oldFile, "attached")
	require.NoError(t, err)

	upsertsSyncID, deletionsSyncID, err := attached.GenerateSyncDiffFromFile(ctx, oldSyncID, newSyncID)
	require.NoError(t, err)

	changedEntIDs, err := attached.ComputeChangedGrantEntitlementIDs(ctx, oldSyncID, newSyncID)
	require.NoError(t, err)

	// The connector produced identical grants, so the diff should report no changed entitlements.
	require.Empty(t, changedEntIDs, "diff should report no changed grant entitlements for a connector-level no-op")

	// Grant-level diffs should also be empty (no false-positive upserts/deletions).
	upserts, err := newFile.ListGrantsInternal(ctx, connectorstore.GrantListOptions{
		Mode:   connectorstore.GrantListModePayload,
		SyncID: upsertsSyncID,
	})
	require.NoError(t, err)
	require.Empty(t, upserts.Rows, "grant upserts should be empty for connector-level no-op")

	deletions, err := newFile.ListGrantsInternal(ctx, connectorstore.GrantListOptions{
		Mode:   connectorstore.GrantListModePayload,
		SyncID: deletionsSyncID,
	})
	require.NoError(t, err)
	require.Empty(t, deletions.Rows, "grant deletions should be empty for connector-level no-op")

	_, err = attached.DetachFile("attached")
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

	oldGrants, err := loadGrantSourcesByKey(ctx, oldFile, oldSyncID)
	require.NoError(t, err)
	require.Contains(t, oldGrants, "group:g2:member|user|u1", "derived grant U1→E2 should exist after expansion")

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

	diffAndApplyIncremental(t, ctx, newFile, oldFile, oldSyncID, newSyncID)

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

	require.NotContains(t, got, "group:g2:member|user|u1", "immutable derived grant should be deleted when sourceless")
}

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

	diffAndApplyIncremental(t, ctx, newFile, oldFile, oldSyncID, newSyncID)

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

	require.Contains(t, got, "group:g2:member|user|u1", "non-immutable direct grant should persist when sourceless")
}

// TestIncrementalExpansion_InvalidateShouldPreserveExpansionColumn tests that the
// invalidation step does not clobber the expansion column of an expandable grant
// when that grant also has Sources set from a previous expansion.
//
// Topology (OLD):
//
//	GA ──self-membership──▶ EA ◀── GB (direct)
//	                          │
//	                       edge EA→EB (no type filter)
//	                          │
//	                          ▼
//	              GA ──────▶ EB (expandable grant, principal=GA)
//
// After full expansion of OLD, the expandable grant (GA on EB) picks up Sources
// because GA's self-membership on EA causes expansion EA→EB to reach GA on EB.
// GB also gets an expanded grant on EB from the EA→EB edge.
//
// NEW: same as OLD, but GA's self-membership on EA is removed.
//
// The incremental pipeline should:
//  1. Detect EA changed (self-membership removed).
//  2. Invalidate downstream grants via EA→EB, removing stale sources.
//  3. Preserve the EA→EB edge (expansion column on GA's grant on EB).
//  4. Re-expand the dirty subgraph, creating GB on EB.
func TestIncrementalExpansion_InvalidateShouldPreserveExpansionColumn(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	oldPath := filepath.Join(tmpDir, "old_preserve_exp.c1z")
	newPath := filepath.Join(tmpDir, "new_preserve_exp.c1z")
	expectedPath := filepath.Join(tmpDir, "expected_preserve_exp.c1z")

	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()

	ga := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "ga"}.Build(), DisplayName: "GA"}.Build()
	gb := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "gb"}.Build(), DisplayName: "GB"}.Build()

	ea := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(ga, "member"), Resource: ga, Slug: "member", DisplayName: "member"}.Build()
	eb := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(gb, "member"), Resource: gb, Slug: "member", DisplayName: "member"}.Build()

	// GA is member of GA (self-membership). This is the grant removed in the NEW state.
	selfMembershipID := batonGrant.NewGrantID(ga, ea)
	grantSelfMembership := v2.Grant_builder{
		Id: selfMembershipID, Entitlement: ea, Principal: ga,
	}.Build()

	// GB is member of GA (direct).
	grantGBonEA := v2.Grant_builder{
		Id: batonGrant.NewGrantID(gb, ea), Entitlement: ea, Principal: gb,
	}.Build()

	// GA is member of GB (expandable: EA→EB, NO resource type filter).
	// After expansion, this grant picks up Sources because GA is also on EA.
	grantGAonEB := v2.Grant_builder{
		Id:          batonGrant.NewGrantID(ga, eb),
		Entitlement: eb,
		Principal:   ga,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds: []string{ea.GetId()},
			Shallow:        false,
		}.Build()),
	}.Build()

	// ── OLD (expanded) ──
	oldFile, err := dotc1z.NewC1ZFile(ctx, oldPath, dotc1z.WithPragma("journal_mode", "WAL"), dotc1z.WithPragma("locking_mode", "normal"))
	require.NoError(t, err)
	defer oldFile.Close(ctx)

	oldSyncID, err := oldFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, oldFile.PutResourceTypes(ctx, groupRT))
	require.NoError(t, oldFile.PutResources(ctx, ga, gb))
	require.NoError(t, oldFile.PutEntitlements(ctx, ea, eb))
	require.NoError(t, oldFile.PutGrants(ctx, grantSelfMembership, grantGBonEA, grantGAonEB))
	require.NoError(t, oldFile.EndSync(ctx))
	require.NoError(t, runFullExpansion(ctx, oldFile, oldSyncID))

	// ── NEW (connector truth: self-membership removed) ──
	newFile, err := dotc1z.NewC1ZFile(ctx, newPath)
	require.NoError(t, err)
	defer newFile.Close(ctx)

	newSyncID, err := newFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, newFile.PutResourceTypes(ctx, groupRT))
	require.NoError(t, newFile.PutResources(ctx, ga, gb))
	require.NoError(t, newFile.PutEntitlements(ctx, ea, eb))

	// Copy expanded grants from old, skipping self-membership.
	require.NoError(t, oldFile.SetSyncID(ctx, ""))
	require.NoError(t, oldFile.ViewSync(ctx, oldSyncID))
	grantsToCopy, err := listGrantsWithStoredExpansion(ctx, oldFile)
	require.NoError(t, err)
	for _, g := range grantsToCopy {
		if g.GetId() == selfMembershipID {
			continue
		}
		require.NoError(t, newFile.PutGrants(ctx, g))
	}
	require.NoError(t, newFile.EndSync(ctx))

	// ── Incremental expansion ──
	diffAndApplyIncremental(t, ctx, newFile, oldFile, oldSyncID, newSyncID)

	// ── EXPECTED (full expansion from new connector truth) ──
	expectedFile, err := dotc1z.NewC1ZFile(ctx, expectedPath)
	require.NoError(t, err)
	defer expectedFile.Close(ctx)

	expectedSyncID, err := expectedFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, expectedFile.PutResourceTypes(ctx, groupRT))
	require.NoError(t, expectedFile.PutResources(ctx, ga, gb))
	require.NoError(t, expectedFile.PutEntitlements(ctx, ea, eb))
	require.NoError(t, expectedFile.PutGrants(ctx, grantGBonEA, grantGAonEB))
	require.NoError(t, expectedFile.EndSync(ctx))
	require.NoError(t, runFullExpansion(ctx, expectedFile, expectedSyncID))

	got, err := loadGrantSourcesByKey(ctx, newFile, newSyncID)
	require.NoError(t, err)
	want, err := loadGrantSourcesByKey(ctx, expectedFile, expectedSyncID)
	require.NoError(t, err)

	require.Equal(t, want, got, "incremental expansion should match full expansion after removing self-membership")
}
