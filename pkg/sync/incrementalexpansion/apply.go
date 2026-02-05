package incrementalexpansion

import (
	"context"
	"fmt"
	"os"

	"github.com/conductorone/baton-sdk/pkg/dotc1z"
)

// ApplyIncrementalExpansionFromDiff applies invalidation + subgraph expansion to targetSyncID
// using paired diff syncs (upserts/deletions).
//
// This is intended to be called after the target sync has already had the base+diff data applied,
// and the diff syncs exist in the same file (so they can be read via ListExpandableGrants with forced sync IDs).
func ApplyIncrementalExpansionFromDiff(ctx context.Context, c1f *dotc1z.C1File, targetSyncID string, upsertsSyncID string, deletionsSyncID string) error {
	delta, err := EdgeDeltaFromDiffSyncs(ctx, c1f, upsertsSyncID, deletionsSyncID)
	if err != nil {
		return err
	}
	if os.Getenv("BATON_DEBUG_INCREMENTAL") != "" {
		fmt.Printf("incremental: delta added=%d removed=%d\n", len(delta.Added), len(delta.Removed))
		// Print a small sample for debugging.
		i := 0
		for _, e := range delta.Added {
			fmt.Printf("  added: %s -> %s\n", e.SrcEntitlementID, e.DstEntitlementID)
			i++
			if i >= 10 {
				break
			}
		}
		i = 0
		for _, e := range delta.Removed {
			fmt.Printf("  removed: %s -> %s\n", e.SrcEntitlementID, e.DstEntitlementID)
			i++
			if i >= 10 {
				break
			}
		}
	}

	// Seed invalidation from any entitlement/resource/grant changes, not just edge-definition changes.
	// If the set of grants for a source entitlement changes (including deletion), its propagated sources
	// must be recomputed along outgoing edges.
	changedSources := make(map[string]struct{}, 256)
	addSeeds := func(ids []string) {
		for _, id := range ids {
			if id == "" {
				continue
			}
			changedSources[id] = struct{}{}
		}
	}

	ids, err := c1f.ListDistinctGrantEntitlementIDsForSync(ctx, upsertsSyncID)
	if err != nil {
		return err
	}
	addSeeds(ids)
	ids, err = c1f.ListDistinctGrantEntitlementIDsForSync(ctx, deletionsSyncID)
	if err != nil {
		return err
	}
	addSeeds(ids)

	// Entitlement deletions/updates should also seed invalidation, even if they had no grant changes.
	ids, err = c1f.ListDistinctEntitlementIDsForSync(ctx, upsertsSyncID)
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

	// Include changed source entitlements in the affected closure for dirty marking.
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
