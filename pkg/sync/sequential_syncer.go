package sync

import (
	"context"
	"errors"
	"fmt"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/retry"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

func (s *SequentialSyncer) sequentialSync(
	ctx context.Context,
	runCtx context.Context,
	targetedResources []*v2.Resource,
) ([]error, error) {
	l := ctxzap.Extract(ctx)

	retryer := retry.NewRetryer(ctx, retry.RetryConfig{
		MaxAttempts:  0,
		InitialDelay: 1 * time.Second,
		MaxDelay:     0,
	})
	var warnings []error

	for s.state.Current() != nil {
		err := s.Checkpoint(ctx, false)
		if err != nil {
			return warnings, err
		}

		// If we have more than 10 warnings and more than 10% of actions ended in a warning, exit the sync.
		if len(warnings) > 10 {
			completedActionsCount := s.state.GetCompletedActionsCount()
			if completedActionsCount > 0 && float64(len(warnings))/float64(completedActionsCount) > 0.1 {
				return warnings, fmt.Errorf("too many warnings, exiting sync. warnings: %v completed actions: %d", warnings, completedActionsCount)
			}
		}
		select {
		case <-runCtx.Done():
			err = context.Cause(runCtx)
			switch {
			case errors.Is(err, context.DeadlineExceeded):
				l.Info("sync run duration has expired, exiting sync early", zap.String("sync_id", s.syncID))
				// It would be nice to remove this once we're more confident in the checkpointing logic.
				checkpointErr := s.Checkpoint(ctx, true)
				if checkpointErr != nil {
					l.Error("error checkpointing before exiting sync", zap.Error(checkpointErr))
				}
				return warnings, errors.Join(checkpointErr, ErrSyncNotComplete)
			default:
				l.Error("sync context cancelled", zap.String("sync_id", s.syncID), zap.Error(err))
				return warnings, err
			}
		default:
		}

		stateAction := s.state.Current()

		switch stateAction.Op {
		case InitOp:
			s.state.FinishAction(ctx)

			if s.skipEntitlementsAndGrants {
				s.state.SetShouldSkipEntitlementsAndGrants()
			}
			if s.skipGrants {
				s.state.SetShouldSkipGrants()
			}
			if len(targetedResources) > 0 {
				for _, r := range targetedResources {
					s.state.PushAction(ctx, Action{
						Op:                   SyncTargetedResourceOp,
						ResourceID:           r.GetId().GetResource(),
						ResourceTypeID:       r.GetId().GetResourceType(),
						ParentResourceID:     r.GetParentResourceId().GetResource(),
						ParentResourceTypeID: r.GetParentResourceId().GetResourceType(),
					})
				}
				s.state.SetShouldFetchRelatedResources()
				s.state.PushAction(ctx, Action{Op: SyncResourceTypesOp})
				err = s.Checkpoint(ctx, true)
				if err != nil {
					return warnings, err
				}
				// Don't do grant expansion or external resources in partial syncs, as we likely lack related resources/entitlements/grants
				continue
			}

			// FIXME(jirwin): Disabling syncing assets for now
			// s.state.PushAction(ctx, Action{Op: SyncAssetsOp})
			if !s.state.ShouldSkipEntitlementsAndGrants() {
				s.state.PushAction(ctx, Action{Op: SyncGrantExpansionOp})
			}
			if s.externalResourceReader != nil {
				s.state.PushAction(ctx, Action{Op: SyncExternalResourcesOp})
			}
			if s.onlyExpandGrants {
				s.state.SetNeedsExpansion()
				err = s.Checkpoint(ctx, true)
				if err != nil {
					return warnings, err
				}
				continue
			}
			if !s.state.ShouldSkipEntitlementsAndGrants() {
				if !s.state.ShouldSkipGrants() {
					s.state.PushAction(ctx, Action{Op: SyncGrantsOp})
				}

				s.state.PushAction(ctx, Action{Op: SyncEntitlementsOp})

				s.state.PushAction(ctx, Action{Op: SyncStaticEntitlementsOp})
			}
			s.state.PushAction(ctx, Action{Op: SyncResourcesOp})
			s.state.PushAction(ctx, Action{Op: SyncResourceTypesOp})

			err = s.Checkpoint(ctx, true)
			if err != nil {
				return warnings, err
			}
			continue

		case SyncResourceTypesOp:
			err = s.SyncResourceTypes(ctx)
			if !retryer.ShouldWaitAndRetry(ctx, err) {
				return warnings, err
			}
			continue

		case SyncResourcesOp:
			err = s.SyncResources(ctx)
			if !retryer.ShouldWaitAndRetry(ctx, err) {
				return warnings, err
			}
			continue

		case SyncTargetedResourceOp:
			err = s.SyncTargetedResource(ctx)
			if isWarning(ctx, err) {
				l.Warn("skipping sync targeted resource action", zap.Any("stateAction", stateAction), zap.Error(err))
				warnings = append(warnings, err)
				s.state.FinishAction(ctx)
				continue
			}
			if !retryer.ShouldWaitAndRetry(ctx, err) {
				return warnings, err
			}
			continue

		case SyncStaticEntitlementsOp:
			err = s.SyncStaticEntitlements(ctx)
			if isWarning(ctx, err) {
				l.Warn("skipping sync static entitlements action", zap.Any("stateAction", stateAction), zap.Error(err))
				warnings = append(warnings, err)
				s.state.FinishAction(ctx)
				continue
			}
			if !retryer.ShouldWaitAndRetry(ctx, err) {
				return warnings, err
			}
			continue
		case SyncEntitlementsOp:
			err = s.SyncEntitlements(ctx)
			if isWarning(ctx, err) {
				l.Warn("skipping sync entitlement action", zap.Any("stateAction", stateAction), zap.Error(err))
				warnings = append(warnings, err)
				s.state.FinishAction(ctx)
				continue
			}
			if !retryer.ShouldWaitAndRetry(ctx, err) {
				return warnings, err
			}
			continue

		case SyncGrantsOp:
			err = s.SyncGrants(ctx)
			if isWarning(ctx, err) {
				l.Warn("skipping sync grant action", zap.Any("stateAction", stateAction), zap.Error(err))
				warnings = append(warnings, err)
				s.state.FinishAction(ctx)
				continue
			}
			if !retryer.ShouldWaitAndRetry(ctx, err) {
				return warnings, err
			}
			continue

		case SyncExternalResourcesOp:
			err = s.SyncExternalResources(ctx)
			if !retryer.ShouldWaitAndRetry(ctx, err) {
				return warnings, err
			}
			continue
		case SyncAssetsOp:
			err = s.SyncAssets(ctx)
			if !retryer.ShouldWaitAndRetry(ctx, err) {
				return warnings, err
			}
			continue

		case SyncGrantExpansionOp:
			if s.dontExpandGrants || !s.state.NeedsExpansion() {
				l.Debug("skipping grant expansion, no grants to expand")
				s.state.FinishAction(ctx)
				continue
			}

			err = s.SyncGrantExpansion(ctx)
			if !retryer.ShouldWaitAndRetry(ctx, err) {
				return warnings, err
			}
			continue
		default:
			return warnings, fmt.Errorf("unexpected sync step")
		}
	}

	return warnings, nil
}
