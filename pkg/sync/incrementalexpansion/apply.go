package incrementalexpansion

import (
	"context"
	"fmt"
	"os"

	"github.com/conductorone/baton-sdk/pkg/dotc1z"
)

// ApplyIncrementalExpansionFromDiff applies invalidation + subgraph expansion to targetSyncID
// using pre-computed diff data.
//
// Parameters:
//   - delta: edge additions/removals computed from cross-DB attached queries.
//   - changedGrantEntitlementIDs: entitlement IDs with any grant change (add/remove/modify),
//     computed from cross-DB attached queries.
//   - upsertsSyncID, deletionsSyncID: diff syncs in the main file for entitlement-level seeding.
func ApplyIncrementalExpansionFromDiff(
	ctx context.Context,
	c1f *dotc1z.C1File,
	targetSyncID string,
	delta *EdgeDelta,
	changedGrantEntitlementIDs []string,
	upsertsSyncID string,
	deletionsSyncID string,
) error {
	if delta == nil {
		delta = &EdgeDelta{
			Added:   map[string]Edge{},
			Removed: map[string]Edge{},
		}
	}

	if os.Getenv("BATON_DEBUG_INCREMENTAL") != "" {
		_, _ = fmt.Fprintf(os.Stderr, "incremental: delta added=%d removed=%d\n", len(delta.Added), len(delta.Removed))
		i := 0
		for _, e := range delta.Added {
			_, _ = fmt.Fprintf(os.Stderr, "  added: %s -> %s\n", e.SrcEntitlementID, e.DstEntitlementID)
			i++
			if i >= 10 {
				break
			}
		}
		i = 0
		for _, e := range delta.Removed {
			_, _ = fmt.Fprintf(os.Stderr, "  removed: %s -> %s\n", e.SrcEntitlementID, e.DstEntitlementID)
			i++
			if i >= 10 {
				break
			}
		}
	}

	changedSources := make(map[string]struct{}, len(changedGrantEntitlementIDs))
	for _, id := range changedGrantEntitlementIDs {
		if id != "" {
			changedSources[id] = struct{}{}
		}
	}

	addSeeds := func(ids []string) {
		for _, id := range ids {
			if id != "" {
				changedSources[id] = struct{}{}
			}
		}
	}

	// Entitlement-level seeding still uses the diff syncs in the main file,
	// since entitlement rows are still diffed into upserts/deletions syncs.
	ids, err := c1f.ListDistinctEntitlementIDsForSync(ctx, upsertsSyncID)
	if err != nil {
		return err
	}
	addSeeds(ids)
	ids, err = c1f.ListDistinctEntitlementIDsForSync(ctx, deletionsSyncID)
	if err != nil {
		return err
	}
	addSeeds(ids)

	affected, err := AffectedEntitlements(ctx, c1f, targetSyncID, delta)
	if err != nil {
		return err
	}

	if err := InvalidateRemovedEdges(ctx, c1f, targetSyncID, delta); err != nil {
		return err
	}

	if err := InvalidateChangedSourceEntitlements(ctx, c1f, targetSyncID, changedSources); err != nil {
		return err
	}

	for id := range changedSources {
		affected[id] = struct{}{}
	}

	if err := MarkNeedsExpansionForAffectedEdges(ctx, c1f, targetSyncID, affected); err != nil {
		return err
	}

	if err := ExpandDirtySubgraph(ctx, c1f, targetSyncID); err != nil {
		return fmt.Errorf("expand dirty subgraph: %w", err)
	}

	return nil
}
