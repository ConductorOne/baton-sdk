package incrementalexpansion

import (
	"context"
	"fmt"

	"github.com/conductorone/baton-sdk/pkg/dotc1z"
)

type needsExpansionMarker interface {
	expandableGrantLister
	SetNeedsExpansionForGrants(ctx context.Context, syncID string, grantExternalIDs []string, needsExpansion bool) error
}

// MarkNeedsExpansionForAffectedEdges sets needs_expansion=1 for expandable grants whose edges are
// in/leading into the affected subgraph.
//
// With grant-column storage, we conservatively mark an expandable grant dirty if:
// - its destination entitlement is affected, OR
// - any of its source entitlement IDs is affected.
func MarkNeedsExpansionForAffectedEdges(ctx context.Context, store needsExpansionMarker, targetSyncID string, affected map[string]struct{}) error {
	if len(affected) == 0 {
		return nil
	}

	pageToken := ""
	toMark := make([]string, 0, 1024)

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
			_, dstAffected := affected[def.DstEntitlementID]
			srcAffected := false
			if !dstAffected {
				for _, src := range def.SrcEntitlementIDs {
					if _, ok := affected[src]; ok {
						srcAffected = true
						break
					}
				}
			}
			if dstAffected || srcAffected {
				toMark = append(toMark, def.GrantExternalID)
			}
		}

		if next == "" {
			break
		}
		pageToken = next
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
