package incrementalexpansion

import (
	"context"

	"github.com/conductorone/baton-sdk/pkg/dotc1z"
)

type changedSourceInvalidator interface {
	expandableGrantLister
	InvalidationStore
}

// InvalidateChangedSourceEntitlements invalidates propagated sources for any entitlement whose grant-set changed.
// It removes only the specific source key (the entitlement ID) from downstream grants along outgoing edges.
func InvalidateChangedSourceEntitlements(ctx context.Context, store changedSourceInvalidator, targetSyncID string, changedSources map[string]struct{}) error {
	if len(changedSources) == 0 {
		return nil
	}

	// Build a set of outgoing edges for only the changed sources.
	outgoing := make(map[string]Edge)
	pageToken := ""
	for {
		defs, next, err := store.ListExpandableGrants(
			ctx,
			dotc1z.WithExpandableGrantsSyncID(targetSyncID),
			dotc1z.WithExpandableGrantsPageToken(pageToken),
			dotc1z.WithExpandableGrantsNeedsExpansionOnly(false),
		)
		if err != nil {
			return err
		}
		for _, def := range defs {
			for _, src := range def.SrcEntitlementIDs {
				if _, ok := changedSources[src]; !ok {
					continue
				}
				e := Edge{
					SrcEntitlementID:         src,
					DstEntitlementID:         def.DstEntitlementID,
					Shallow:                  def.Shallow,
					PrincipalResourceTypeIDs: def.PrincipalResourceTypeIDs,
				}
				outgoing[e.Key()] = e
			}
		}
		if next == "" {
			break
		}
		pageToken = next
	}

	if len(outgoing) == 0 {
		return nil
	}

	// Reuse the same invalidation path (remove sources[src] from dst grants).
	return InvalidateRemovedEdges(ctx, store, targetSyncID, &EdgeDelta{Removed: outgoing})
}
