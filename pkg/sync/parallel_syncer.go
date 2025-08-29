package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"context"
	"errors"
	"fmt"
	native_sync "sync"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/retry"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

func (s *syncer) sequentialSync(
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
				return warnings, fmt.Errorf("%w: warnings: %v completed actions: %d", ErrTooManyWarnings, warnings, completedActionsCount)
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
			s.state.FinishAction(ctx, stateAction)

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
			err = s.SyncResourceTypes(ctx, stateAction)
			if !retryer.ShouldWaitAndRetry(ctx, err) {
				return warnings, err
			}
			continue

		case SyncResourcesOp:
			err = s.SyncResources(ctx, stateAction)
			if !retryer.ShouldWaitAndRetry(ctx, err) {
				return warnings, err
			}
			continue

		case SyncTargetedResourceOp:
			err = s.SyncTargetedResource(ctx, stateAction)
			if isWarning(ctx, err) {
				l.Warn("skipping sync targeted resource action", zap.Any("stateAction", stateAction), zap.Error(err))
				warnings = append(warnings, err)
				s.state.FinishAction(ctx, stateAction)
				continue
			}
			if !retryer.ShouldWaitAndRetry(ctx, err) {
				return warnings, err
			}
			continue

		case SyncStaticEntitlementsOp:
			err = s.SyncStaticEntitlements(ctx, stateAction)
			if isWarning(ctx, err) {
				l.Warn("skipping sync static entitlements action", zap.Any("stateAction", stateAction), zap.Error(err))
				warnings = append(warnings, err)
				s.state.FinishAction(ctx, stateAction)
				continue
			}
			if !retryer.ShouldWaitAndRetry(ctx, err) {
				return warnings, err
			}
			continue
		case SyncEntitlementsOp:
			err = s.SyncEntitlements(ctx, stateAction)
			if isWarning(ctx, err) {
				l.Warn("skipping sync entitlement action", zap.Any("stateAction", stateAction), zap.Error(err))
				warnings = append(warnings, err)
				s.state.FinishAction(ctx, stateAction)
				continue
			}
			if !retryer.ShouldWaitAndRetry(ctx, err) {
				return warnings, err
			}
			continue

		case SyncGrantsOp:
			err = s.SyncGrants(ctx, stateAction)
			if isWarning(ctx, err) {
				l.Warn("skipping sync grant action", zap.Any("stateAction", stateAction), zap.Error(err))
				warnings = append(warnings, err)
				s.state.FinishAction(ctx, stateAction)
				continue
			}
			if !retryer.ShouldWaitAndRetry(ctx, err) {
				return warnings, err
			}
			continue

		case SyncExternalResourcesOp:
			err = s.SyncExternalResources(ctx, stateAction)
			if !retryer.ShouldWaitAndRetry(ctx, err) {
				return warnings, err
			}
			continue
		case SyncAssetsOp:
			err = s.SyncAssets(ctx, stateAction)
			if !retryer.ShouldWaitAndRetry(ctx, err) {
				return warnings, err
			}
			continue

		case SyncGrantExpansionOp:
			// Mark the sync as supporting diff, but only if we're starting fresh.
			// If we're resuming (graph has edges or a page token), we may be continuing
			// from old code that didn't have this marker, so we must not set it.
			entitlementGraph := s.state.EntitlementGraph(ctx)
			isResumingExpansion := entitlementGraph.Loaded || len(entitlementGraph.Edges) > 0 || stateAction.PageToken != ""
			if !isResumingExpansion {
				if err := s.store.SetSupportsDiff(ctx, s.syncID); err != nil {
					l.Error("failed to set supports_diff marker", zap.Error(err))
					return warnings, err
				}
			}

			if s.dontExpandGrants || !s.state.NeedsExpansion() {
				l.Debug("skipping grant expansion, no grants to expand")
				s.state.FinishAction(ctx, stateAction)
				continue
			}

			err = s.SyncGrantExpansion(ctx, stateAction)
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

// TODO: Merge parallelSync and sequentialSync once we're confident in parallelSync's behavior.
func (s *syncer) parallelSync(
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
	for {
		stateAction := s.state.Current()
		if stateAction == nil {
			break
		}

		err := s.Checkpoint(ctx, false)
		if err != nil {
			return warnings, err
		}

		// If we have more than 10 warnings and more than 10% of actions ended in a warning, exit the sync.
		if len(warnings) > 10 {
			completedActionsCount := s.state.GetCompletedActionsCount()
			if completedActionsCount > 0 && float64(len(warnings))/float64(completedActionsCount) > 0.1 {
				return warnings, fmt.Errorf("%w: warnings: %v completed actions: %d", ErrTooManyWarnings, warnings, completedActionsCount)
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

		switch stateAction.Op {
		case InitOp:
			s.state.FinishAction(ctx, stateAction)

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
			err = s.SyncResourceTypes(ctx, stateAction)
			if !retryer.ShouldWaitAndRetry(ctx, err) {
				return warnings, err
			}
			continue

		case SyncResourcesOp:
			if stateAction.ResourceTypeID == "" && stateAction.ResourceID == "" {
				err = s.SyncResources(ctx, stateAction)
				if !retryer.ShouldWaitAndRetry(ctx, err) {
					return warnings, err
				}
				continue
			}
			resourceActions := s.state.PeekMatchingActions(ctx, SyncResourcesOp)
			w, resourceErr := s.syncParallel(ctx, resourceActions, s.SyncResources)
			warnings = append(warnings, w...)
			if !retryer.ShouldWaitAndRetry(ctx, resourceErr) {
				return warnings, resourceErr
			}
			continue

		case SyncTargetedResourceOp:
			targetedResourceActions := s.state.PeekMatchingActions(ctx, SyncTargetedResourceOp)
			w, targetedResourceErr := s.syncParallel(ctx, targetedResourceActions, s.SyncTargetedResource)
			warnings = append(warnings, w...)
			if !retryer.ShouldWaitAndRetry(ctx, targetedResourceErr) {
				return warnings, targetedResourceErr
			}
			continue

		case SyncStaticEntitlementsOp:
			err = s.SyncStaticEntitlements(ctx, stateAction)
			if isWarning(ctx, err) {
				l.Warn("skipping sync static entitlements action", zap.Any("stateAction", stateAction), zap.Error(err))
				warnings = append(warnings, err)
				s.state.FinishAction(ctx, stateAction)
				continue
			}
			if !retryer.ShouldWaitAndRetry(ctx, err) {
				return warnings, err
			}
			continue
		case SyncEntitlementsOp:
			if stateAction.ResourceTypeID == "" && stateAction.ResourceID == "" {
				err = s.SyncEntitlements(ctx, stateAction)
				if isWarning(ctx, err) {
					l.Warn("skipping sync entitlement action", zap.Any("stateAction", stateAction), zap.Error(err))
					warnings = append(warnings, err)
					s.state.FinishAction(ctx, stateAction)
					continue
				}
				if !retryer.ShouldWaitAndRetry(ctx, err) {
					return warnings, err
				}
				continue
			}
			entitlementActions := s.state.PeekMatchingActions(ctx, SyncEntitlementsOp)
			w, entitlementErr := s.syncParallel(ctx, entitlementActions, s.SyncEntitlements)
			warnings = append(warnings, w...)
			if !retryer.ShouldWaitAndRetry(ctx, entitlementErr) {
				return warnings, entitlementErr
			}
			continue

		case SyncGrantsOp:
			if stateAction.ResourceTypeID == "" && stateAction.ResourceID == "" {
				err = s.SyncGrants(ctx, stateAction)
				if isWarning(ctx, err) {
					l.Warn("skipping sync grant action", zap.Any("stateAction", stateAction), zap.Error(err))
					warnings = append(warnings, err)
					s.state.FinishAction(ctx, stateAction)
					continue
				}
				if !retryer.ShouldWaitAndRetry(ctx, err) {
					return warnings, err
				}
				continue
			}

			grantActions := s.state.PeekMatchingActions(ctx, SyncGrantsOp)
			w, grantErr := s.syncParallel(ctx, grantActions, s.SyncGrants)
			warnings = append(warnings, w...)
			if !retryer.ShouldWaitAndRetry(ctx, grantErr) {
				return warnings, grantErr
			}
			continue

		case SyncExternalResourcesOp:
			err = s.SyncExternalResources(ctx, stateAction)
			if !retryer.ShouldWaitAndRetry(ctx, err) {
				return warnings, err
			}
			continue
		case SyncAssetsOp:
			err = s.SyncAssets(ctx, stateAction)
			if !retryer.ShouldWaitAndRetry(ctx, err) {
				return warnings, err
			}
			continue

		case SyncGrantExpansionOp:
			// Mark the sync as supporting diff, but only if we're starting fresh.
			// If we're resuming (graph has edges or a page token), we may be continuing
			// from old code that didn't have this marker, so we must not set it.
			entitlementGraph := s.state.EntitlementGraph(ctx)
			isResumingExpansion := entitlementGraph.Loaded || len(entitlementGraph.Edges) > 0 || stateAction.PageToken != ""
			if !isResumingExpansion {
				if err := s.store.SetSupportsDiff(ctx, s.syncID); err != nil {
					l.Error("failed to set supports_diff marker", zap.Error(err))
					return warnings, err
				}
			}

			if s.dontExpandGrants || !s.state.NeedsExpansion() {
				l.Debug("skipping grant expansion, no grants to expand")
				s.state.FinishAction(ctx, stateAction)
				continue
			}

			err = s.SyncGrantExpansion(ctx, stateAction)
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

type workerResult struct {
	warnings []error
	err      error
}

func (s *syncer) syncParallel(ctx context.Context, actions []*Action, f func(ctx context.Context, action *Action) error) ([]error, error) {
	l := ctxzap.Extract(ctx)
	l.Info("syncing in parallel", zap.Int("actions", len(actions)), zap.Int("workers", s.workerCount))

	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	actionCh := make(chan *Action, len(actions))
	for _, a := range actions {
		actionCh <- a
	}
	close(actionCh)

	resultCh := make(chan workerResult, len(actions))

	var wg native_sync.WaitGroup
	for i := 0; i < s.workerCount; i++ {
		wg.Go(func() {
			for action := range actionCh {
				r := s.syncOneAction(ctx, l, action, f)
				resultCh <- r
				if r.err != nil {
					cancel(r.err)
					return
				}
			}
		})
	}

	wg.Wait()
	close(resultCh)

	var warnings []error
	var errs []error
	for r := range resultCh {
		warnings = append(warnings, r.warnings...)
		if r.err != nil {
			errs = append(errs, r.err)
		}
	}

	return warnings, errors.Join(errs...)
}

// syncOneAction processes a single action to completion,
// handling pagination by re-reading the action from state after each call.
func (s *syncer) syncOneAction(ctx context.Context, l *zap.Logger, action *Action, f func(ctx context.Context, action *Action) error) workerResult {
	for {
		err := f(ctx, action)
		if isWarning(ctx, err) {
			l.Warn("skipping sync action", zap.Any("action", action), zap.Error(err))
			s.state.FinishAction(ctx, action)
			return workerResult{warnings: []error{err}}
		}
		if err != nil {
			return workerResult{err: err}
		}

		updated := s.state.GetAction(action.ID)
		if updated == nil {
			return workerResult{}
		}
		action = updated
	}
}
