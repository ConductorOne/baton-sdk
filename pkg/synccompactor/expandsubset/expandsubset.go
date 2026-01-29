package expandsubset

import (
	"context"
	"database/sql"
	"fmt"

	c1zpb "github.com/conductorone/baton-sdk/pb/c1/c1z/v1"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/sync/expand"
)

// ExpandGrantsForEntitlementSubset runs grant expansion only for the subgraph reachable from seedEntitlementIDs
// using the persisted entitlement edges table in the given c1z file.
//
// This is additive-only; invalidation of stale expanded outputs will be layered on top.
func ExpandGrantsForEntitlementSubset(ctx context.Context, c1f *dotc1z.C1File, syncID string, seedEntitlementIDs []string) error {
	if c1f == nil {
		return fmt.Errorf("c1f is nil")
	}
	if syncID == "" {
		return fmt.Errorf("syncID is required")
	}
	if len(seedEntitlementIDs) == 0 {
		return nil
	}

	closure, err := c1f.EntitlementDescendantsClosure(ctx, syncID, seedEntitlementIDs)
	if err != nil {
		return err
	}
	if len(closure) == 0 {
		return nil
	}

	edges, err := c1f.ListEntitlementEdgesForSources(ctx, syncID, closure)
	if err != nil {
		return err
	}

	graph := expand.NewEntitlementGraph(ctx)

	ids := make(map[string]struct{}, 0)
	for _, e := range edges {
		ids[e.SrcEntitlementID] = struct{}{}
		ids[e.DstEntitlementID] = struct{}{}
	}

	syncAnnos := annotations.Annotations{}
	syncAnnos.Update(c1zpb.SyncDetails_builder{Id: syncID}.Build())

	for id := range ids {
		resp, err := c1f.GetEntitlement(ctx, reader_v2.EntitlementsReaderServiceGetEntitlementRequest_builder{
			EntitlementId: id,
			Annotations:   syncAnnos,
		}.Build())
		if err != nil {
			if err == sql.ErrNoRows {
				continue
			}
			continue
		}
		graph.AddEntitlement(resp.GetEntitlement())
	}

	for _, e := range edges {
		_ = graph.AddEdge(ctx, e.SrcEntitlementID, e.DstEntitlementID, e.Shallow, e.ResourceTypeIDs)
	}

	graph.Loaded = true
	comps, _ := graph.ComputeCyclicComponents(ctx)
	if len(comps) == 0 {
		graph.HasNoCycles = true
	} else {
		if err := graph.FixCyclesFromComponents(ctx, comps); err != nil {
			return err
		}
	}

	expander := expand.NewExpander(c1f, graph)
	return expander.Run(ctx)
}
