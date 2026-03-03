package incrementalexpansion

import (
	"context"
	"path/filepath"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/sync/expand"
	"github.com/stretchr/testify/require"
)

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

func runFullExpansion(ctx context.Context, c1f *dotc1z.C1File, syncID string) error {
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

	expander := expand.NewExpander(c1f, graph).WithSyncID(syncID)
	if err := expander.Run(ctx); err != nil {
		return err
	}
	return nil
}

func loadGrantSourcesByKey(ctx context.Context, c1f *dotc1z.C1File, syncID string) (map[string]map[string]bool, error) {
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
