package incrementalexpansion

import (
	"context"
	"fmt"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// MarkNeedsExpansionForAffectedEdges sets needs_expansion=1 for expandable grants whose edges are
// in/leading into the affected subgraph.
//
// With grant-column storage, we conservatively mark an expandable grant dirty if:
// - its destination entitlement is affected, OR
// - any of its source entitlement IDs is affected.
func MarkNeedsExpansionForAffectedEdges(ctx context.Context, store connectorstore.InternalWriter, targetSyncID string, affected map[string]struct{}) error {
	if len(affected) == 0 {
		return nil
	}

	pageToken := ""
	toMark := make([]string, 0, 1024)

	for {
		resp, err := store.ListGrantsInternal(ctx, connectorstore.GrantListOptions{
			SyncID:         targetSyncID,
			PageToken:      pageToken,
			ExpandableOnly: true,
		})
		// defs, next, err := store.ListGrantsInternal(
		// 	ctx,
		// 	dotc1z.WithExpandableGrantsSyncID(targetSyncID),
		// 	dotc1z.WithExpandableGrantsPageToken(pageToken),
		// 	dotc1z.WithExpandableGrantsNeedsExpansionOnly(false),
		// )
		if err != nil {
			return err
		}

		for _, def := range resp.Rows {
			_, dstAffected := affected[def.Expansion.TargetEntitlementID]
			srcAffected := false
			if !dstAffected {
				for _, src := range def.Expansion.SourceEntitlementIDs {
					if _, ok := affected[src]; ok {
						srcAffected = true
						break
					}
				}
			}
			if dstAffected || srcAffected {
				toMark = append(toMark, def.Expansion.GrantExternalID)
			}
		}

		if resp.NextPageToken == "" {
			break
		}
		pageToken = resp.NextPageToken
	}

	// Apply in chunks to avoid huge IN() clauses.
	const chunk = 5000
	for i := 0; i < len(toMark); i += chunk {
		j := i + chunk
		if j > len(toMark) {
			j = len(toMark)
		}
		if err := store.SetNeedsExpansionForGrants(ctx, targetSyncID, toMark[i:j], true); err != nil {
			return fmt.Errorf("mark needs_expansion: %w", err)
		}
	}
	return nil
}
