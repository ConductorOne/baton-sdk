package incrementalexpansion

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/sync/expand"
)

// ExpandDirtySubgraph loads only expandable edges marked needs_expansion=1 for syncID,
// runs the standard expander, then clears needs_expansion.
//
// NOTE: This expands only edges present in the loaded subgraph. Callers are responsible for
// marking the correct edge-defining grants as needs_expansion based on the affected subgraph.
func ExpandDirtySubgraph(ctx context.Context, c1f *dotc1z.C1File, syncID string) error {
	if err := c1f.SetSyncID(ctx, syncID); err != nil {
		return err
	}

	// Mark the sync as supporting diff operations (SQL-layer data is ready). This is idempotent - subsequent calls are no-ops.
	// The marker is used to detect syncs that expanded with older code that dropped annotations.
	if err := c1f.SetSupportsDiff(ctx, syncID); err != nil {
		return err
	}

	graph := expand.NewEntitlementGraph(ctx)

	pageToken := ""
	for {
		resp, err := c1f.ListGrantsInternal(ctx, connectorstore.GrantListOptions{
			Mode:               connectorstore.GrantListModeExpansionNeedsOnly,
			SyncID:             syncID,
			PageToken:          pageToken,
			NeedsExpansionOnly: true,
		})
		// defs, next, err := c1f.ListExpandableGrants(
		// 	ctx,
		// 	dotc1z.WithExpandableGrantsSyncID(syncID),
		// 	dotc1z.WithExpandableGrantsPageToken(pageToken),
		// 	dotc1z.WithExpandableGrantsNeedsExpansionOnly(true),
		// )
		if err != nil {
			return err
		}

		for _, row := range resp.Rows {
			def := row.Expansion
			if def == nil {
				continue
			}
			principalID := v2.ResourceId_builder{
				ResourceType: def.PrincipalResourceTypeID,
				Resource:     def.PrincipalResourceID,
			}.Build()

			for _, srcEntitlementID := range def.SourceEntitlementIDs {
				srcEntitlement, err := c1f.GetEntitlement(ctx, reader_v2.EntitlementsReaderServiceGetEntitlementRequest_builder{
					EntitlementId: srcEntitlementID,
				}.Build())
				if err != nil {
					// Only skip not-found entitlements; propagate other errors
					// to avoid silently dropping edges and yielding incorrect expansions.
					if errors.Is(err, sql.ErrNoRows) {
						continue
					}
					return fmt.Errorf("error fetching source entitlement %q: %w", srcEntitlementID, err)
				}

				sourceEntitlementResourceID := srcEntitlement.GetEntitlement().GetResource().GetId()
				if sourceEntitlementResourceID == nil {
					return fmt.Errorf("source entitlement resource id was nil")
				}
				if principalID.GetResourceType() != sourceEntitlementResourceID.GetResourceType() ||
					principalID.GetResource() != sourceEntitlementResourceID.GetResource() {
					return fmt.Errorf("source entitlement resource id did not match grant principal id")
				}

				graph.AddEntitlementID(def.TargetEntitlementID)
				graph.AddEntitlementID(srcEntitlementID)
				if err := graph.AddEdge(ctx, srcEntitlementID, def.TargetEntitlementID, def.Shallow, def.ResourceTypeIDs); err != nil {
					return fmt.Errorf("error adding edge to graph: %w", err)
				}
			}
		}

		if resp.NextPageToken == "" {
			break
		}
		pageToken = resp.NextPageToken
	}
	graph.Loaded = true

	// Fix cycles before running expansion.
	if err := graph.FixCycles(ctx); err != nil {
		return err
	}

	expander := expand.NewExpander(c1f, graph)
	if err := expander.Run(ctx); err != nil {
		return err
	}

	return c1f.ClearNeedsExpansionForSync(ctx, syncID)
}
