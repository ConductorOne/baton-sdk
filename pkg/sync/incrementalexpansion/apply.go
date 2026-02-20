package incrementalexpansion

import (
	"context"
	"errors"
	"fmt"

	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

// RunParams configures an incremental expansion run between old and new sync states.
type RunParams struct {
	OldFile   *dotc1z.C1File
	OldSyncID string
	NewSyncID string
}

// RunResult contains IDs for the generated grant diff syncs.
type RunResult struct {
	UpsertsSyncID   string
	DeletionsSyncID string
}

// Run computes grant/edge diffs from old->new and applies incremental expansion for NewSyncID.
// TODO(kans): pagination, checkpointing, resumability.
// TODO(kans): investigate if past some threshold, a full expansion pass is faster than an incremental one.
func Run(
	ctx context.Context,
	newFile *dotc1z.C1File,
	params RunParams,
) (_ *RunResult, err error) {
	if newFile == nil {
		return nil, errors.New("new file was nil")
	}
	if params.OldFile == nil {
		return nil, errors.New("old file was nil")
	}
	if params.OldSyncID == "" {
		return nil, errors.New("old sync id was empty")
	}
	if params.NewSyncID == "" {
		return nil, errors.New("new sync id was empty")
	}

	const attachedName = "attached"
	attached, err := newFile.AttachFile(params.OldFile, attachedName)
	if err != nil {
		return nil, err
	}
	defer func() {
		_, detachErr := attached.DetachFile(attachedName)
		if detachErr != nil {
			err = errors.Join(err, fmt.Errorf("detach old file: %w", detachErr))
		}
	}()

	upsertsSyncID, deletionsSyncID, err := attached.GenerateSyncDiffFromFile(ctx, params.OldSyncID, params.NewSyncID)
	if err != nil {
		return nil, err
	}

	addedDefs, err := attached.ComputeAddedExpandableGrants(ctx, params.OldSyncID, params.NewSyncID)
	if err != nil {
		return nil, err
	}
	removedDefs, err := attached.ComputeRemovedExpandableGrants(ctx, params.OldSyncID, params.NewSyncID)
	if err != nil {
		return nil, err
	}
	changedEntitlementIDs, err := attached.ComputeChangedGrantEntitlementIDs(ctx, params.OldSyncID, params.NewSyncID)
	if err != nil {
		return nil, err
	}

	delta := edgeDeltaFromExpandableGrants(addedDefs, removedDefs)

	l := ctxzap.Extract(ctx)
	l.Debug("incremental expansion: computed diff",
		zap.Int("edges_added", len(delta.added)),
		zap.Int("edges_removed", len(delta.removed)),
		zap.Int("changed_entitlements", len(changedEntitlementIDs)),
	)

	// Seed the set of "changed sources" from grant-level diffs. These are source
	// entitlements whose grants changed and therefore may need propagation updates.
	changedSources := make(map[string]struct{}, len(changedEntitlementIDs))
	for _, id := range changedEntitlementIDs {
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
	ids, err := newFile.ListDistinctEntitlementIDsForSync(ctx, upsertsSyncID)
	if err != nil {
		return nil, err
	}
	addSeeds(ids)
	ids, err = newFile.ListDistinctEntitlementIDsForSync(ctx, deletionsSyncID)
	if err != nil {
		return nil, err
	}
	addSeeds(ids)

	// Compute the transitive entitlement closure impacted by edge-definition changes
	// (added/removed expandable edges). This closure is used for dirty marking.
	affected, err := affectedEntitlements(ctx, newFile, params.NewSyncID, delta)
	if err != nil {
		return nil, err
	}

	// Stage 1: remove stale propagated sources implied by removed edges.
	// This may delete derived immutable grants that became sourceless.
	if err := invalidateRemovedEdges(ctx, newFile, params.NewSyncID, delta); err != nil {
		return nil, err
	}

	// Stage 2: invalidate grants for source entitlements whose grants changed
	// (add/remove/modify), even if edge definitions themselves did not change.
	if err := invalidateChangedSourceEntitlements(ctx, newFile, params.NewSyncID, changedSources); err != nil {
		return nil, err
	}

	// Changed sources themselves must be marked affected so outgoing propagation
	// is recomputed from these nodes during dirty expansion.
	for id := range changedSources {
		affected[id] = struct{}{}
	}

	// Mark only grants on affected nodes as needing expansion. This bounds the
	// subsequent expansion pass to the dirty subgraph instead of full recompute.
	if err := markNeedsExpansionForAffectedEdges(ctx, newFile, params.NewSyncID, affected); err != nil {
		return nil, err
	}

	// Re-expand just the dirty subgraph and clear needs_expansion flags when done.
	if err := expandDirtySubgraph(ctx, newFile, params.NewSyncID); err != nil {
		return nil, fmt.Errorf("expand dirty subgraph: %w", err)
	}

	return &RunResult{
		UpsertsSyncID:   upsertsSyncID,
		DeletionsSyncID: deletionsSyncID,
	}, nil
}
