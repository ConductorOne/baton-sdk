package incrementalexpansion

import (
	"context"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// InvalidateChangedSourceEntitlements invalidates propagated sources for any entitlement whose grant-set changed.
// It removes only the specific source key (the entitlement ID) from downstream grants along outgoing edges.
func InvalidateChangedSourceEntitlements(ctx context.Context, store connectorstore.InternalWriter, targetSyncID string, changedSources map[string]struct{}) error {
	if len(changedSources) == 0 {
		return nil
	}

	// Build a set of outgoing edges for only the changed sources.
	outgoing := make(map[string]Edge)
	pageToken := ""
	for {
		resp, err := store.ListGrantsInternal(ctx, connectorstore.GrantListOptions{
			Mode:           connectorstore.GrantListModeExpansion,
			SyncID:         targetSyncID,
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
				if _, ok := changedSources[src]; !ok {
					continue
				}
				e := Edge{
					SrcEntitlementID:         src,
					DstEntitlementID:         def.TargetEntitlementID,
					Shallow:                  def.Shallow,
					PrincipalResourceTypeIDs: def.ResourceTypeIDs,
				}
				outgoing[e.Key()] = e
			}
		}
		if resp.NextPageToken == "" {
			break
		}
		pageToken = resp.NextPageToken
	}

	if len(outgoing) == 0 {
		return nil
	}

	// Reuse the same invalidation path (remove sources[src] from dst grants).
	return InvalidateRemovedEdges(ctx, store, targetSyncID, &EdgeDelta{Removed: outgoing})
}
