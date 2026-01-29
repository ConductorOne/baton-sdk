package dotc1z

import (
	"context"
	"fmt"

	c1zpb "github.com/conductorone/baton-sdk/pb/c1/c1z/v1"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
)

// BuildEntitlementEdgesForSync scans grants in the given sync and persists any expansion edges
// defined by GrantExpansionEdges (fallback: legacy GrantExpandable) into v1_entitlement_edges.
func (c *C1File) BuildEntitlementEdgesForSync(ctx context.Context, syncID string) error {
	if c.readOnly {
		return ErrReadOnly
	}
	if err := c.validateDb(ctx); err != nil {
		return err
	}
	if syncID == "" {
		return fmt.Errorf("syncID is required")
	}

	pageToken := ""
	for {
		annos := annotations.Annotations{}
		annos.Update(c1zpb.SyncDetails_builder{Id: syncID}.Build())
		resp, err := c.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{
			PageToken:    pageToken,
			Annotations:  annos,
			ActiveSyncId: "",
		}.Build())
		if err != nil {
			return err
		}

		for _, g := range resp.GetList() {
			edgesAnno, err := getGrantExpansionEdgesAnnotation(g)
			if err != nil {
				return err
			}
			edges := edgesFromGrantExpansionAnnotation(g, edgesAnno)
			if err := c.UpsertEntitlementEdgesForGrant(ctx, syncID, g.GetId(), edges); err != nil {
				return err
			}
		}

		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}

	return nil
}

// ApplyEntitlementEdgesDiff applies a diff (upserts + deletions sync IDs) to the entitlement-edge graph
// stored under targetSyncID.
//
// - For deleted grants: delete edges by source_grant_external_id.
// - For deleted entitlements: delete incident edges (src/dst match).
// - For upserted grants: replace edges by source_grant_external_id using GrantExpansionEdges (or legacy fallback).
func (c *C1File) ApplyEntitlementEdgesDiff(ctx context.Context, targetSyncID, upsertsSyncID, deletionsSyncID string) error {
	if c.readOnly {
		return ErrReadOnly
	}
	if err := c.validateDb(ctx); err != nil {
		return err
	}
	if targetSyncID == "" {
		return fmt.Errorf("targetSyncID is required")
	}
	if upsertsSyncID == "" || deletionsSyncID == "" {
		return fmt.Errorf("upsertsSyncID and deletionsSyncID are required")
	}

	// 1) Deletions: grants
	if err := c.forEachGrantInSync(ctx, deletionsSyncID, func(g *v2.Grant) error {
		return c.DeleteEntitlementEdgesForGrant(ctx, targetSyncID, g.GetId())
	}); err != nil {
		return err
	}

	// 2) Deletions: entitlements
	if err := c.forEachEntitlementInSync(ctx, deletionsSyncID, func(e *v2.Entitlement) error {
		return c.DeleteEntitlementEdgesForEntitlement(ctx, targetSyncID, e.GetId())
	}); err != nil {
		return err
	}

	// 3) Upserts: grants (replace edges for this grant)
	if err := c.forEachGrantInSync(ctx, upsertsSyncID, func(g *v2.Grant) error {
		edgesAnno, err := getGrantExpansionEdgesAnnotation(g)
		if err != nil {
			return err
		}
		edges := edgesFromGrantExpansionAnnotation(g, edgesAnno)
		return c.UpsertEntitlementEdgesForGrant(ctx, targetSyncID, g.GetId(), edges)
	}); err != nil {
		return err
	}

	return nil
}

func (c *C1File) forEachGrantInSync(ctx context.Context, syncID string, f func(*v2.Grant) error) error {
	pageToken := ""
	for {
		annos := annotations.Annotations{}
		annos.Update(c1zpb.SyncDetails_builder{Id: syncID}.Build())
		resp, err := c.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{
			PageToken:    pageToken,
			Annotations:  annos,
			ActiveSyncId: "",
		}.Build())
		if err != nil {
			return err
		}
		for _, g := range resp.GetList() {
			if err := f(g); err != nil {
				return err
			}
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			return nil
		}
	}
}

func (c *C1File) forEachEntitlementInSync(ctx context.Context, syncID string, f func(*v2.Entitlement) error) error {
	pageToken := ""
	for {
		annos := annotations.Annotations{}
		annos.Update(c1zpb.SyncDetails_builder{Id: syncID}.Build())
		resp, err := c.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{
			PageToken:   pageToken,
			Annotations: annos,
		}.Build())
		if err != nil {
			return err
		}
		for _, e := range resp.GetList() {
			if err := f(e); err != nil {
				return err
			}
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			return nil
		}
	}
}

func getGrantExpansionEdgesAnnotation(grant *v2.Grant) (*v2.GrantExpansionEdges, error) {
	if grant == nil {
		return nil, nil
	}
	annos := annotations.Annotations(grant.GetAnnotations())

	edges := &v2.GrantExpansionEdges{}
	ok, err := annos.Pick(edges)
	if err != nil {
		return nil, err
	}
	if ok {
		return edges, nil
	}

	legacy := &v2.GrantExpandable{}
	ok, err = annos.Pick(legacy)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}

	return v2.GrantExpansionEdges_builder{
		EntitlementIds:  legacy.GetEntitlementIds(),
		Shallow:         legacy.GetShallow(),
		ResourceTypeIds: legacy.GetResourceTypeIds(),
	}.Build(), nil
}
