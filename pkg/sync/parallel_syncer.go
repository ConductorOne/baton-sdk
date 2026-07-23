package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	native_sync "sync"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/ratelimit"
	"github.com/conductorone/baton-sdk/pkg/retry"
	"github.com/conductorone/baton-sdk/pkg/uotel"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

func (s *syncer) timedShouldWaitAndRetry(ctx context.Context, op ActionOp, resourceTypeID string, retryer *retry.Retryer, err error) bool {
	var shouldRetry bool
	_ = s.timedStep(op, func() error {
		shouldRetry = retryer.ShouldWaitAndRetry(ratelimit.WithWaitLabel(ctx, resourceTypeID), err)
		return nil
	})
	return shouldRetry
}

// recordRetryWait accumulates one completed wait into the cumulative buckets.
// retry_wait / rate_limit_wait are worker-seconds: parallel workers each
// report their full sleep, so the totals can exceed wall-clock sync time
// (divide by worker_count for a utilization view). The per-resource-type
// sub-buckets decompose the flat totals. rate_limit_wait_wall is the
// wall-clock companion; see recordRateLimitWallInterval.
func (s *syncer) recordRetryWait(ctx context.Context, wait time.Duration, rateLimited bool) {
	// The wait observer is installed at the top of Sync, before the state
	// token exists; a gate wait during the initial Validate call must be
	// dropped, not dereference a nil state.
	if s.state == nil {
		return
	}
	bucket := "retry_wait"
	if rateLimited {
		bucket = "rate_limit_wait"
	}
	s.state.AddStepDuration(bucket, wait)
	if label, ok := ratelimit.WaitLabelFromContext(ctx); ok {
		s.state.AddStepDuration(bucket+":"+label, wait)
	}
	if rateLimited {
		s.recordRateLimitWallInterval(wait)
	}
}

// recordRateLimitWallInterval folds one completed rate-limit wait into
// rate_limit_wait_wall: wall-clock time with at least one worker blocked on a
// rate limit. Unlike the cumulative rate_limit_wait (worker-seconds, can
// exceed wall-clock under parallelism), this bucket is bounded by sync
// duration.
//
// Every reporter fires immediately after its wait ends — syncer-process
// sleeps exactly, connector-reported annotation waits approximately (at
// response receipt) — so each event is the interval [now-wait, now] and
// events arrive ordered by end time. Merging overlaps therefore reduces to a
// watermark: count only the part of the interval past the last covered
// instant. The watermark is in-memory only; across checkpoint/resume the
// first event after resume counts in full, which can only under-merge (never
// double-count) because the bucket itself persists in the token.
//
// Known bias: an annotation wait is end-anchored at response receipt, but the
// connector's sleep ended earlier in the RPC (marshal + transport + any
// post-sleep work follow it). The claimed interval shifts late by that
// amount, so under parallelism the merge can count wall time past another
// worker's watermark that was not actually blocked — an over-count bounded by
// the post-sleep RPC latency of that one report. The bucket stays bounded by
// elapsed sync time regardless.
func (s *syncer) recordRateLimitWallInterval(wait time.Duration) {
	if wait <= 0 || s.state == nil {
		return
	}
	s.rlWallMu.Lock()
	end := time.Now()
	start := end.Add(-wait)
	if start.Before(s.rlWallCoveredUntil) {
		start = s.rlWallCoveredUntil
	}
	if !end.After(start) {
		s.rlWallMu.Unlock()
		return
	}
	s.rlWallCoveredUntil = end
	// The state bucket accumulates whole milliseconds per call, so carry the
	// sub-millisecond remainder locally: overlapping parallel waits contribute
	// many tiny past-the-watermark slivers that would otherwise all truncate
	// to zero and systematically undercount the bucket.
	delta := end.Sub(start) + s.rlWallCarry
	whole := delta.Truncate(time.Millisecond)
	s.rlWallCarry = delta - whole
	// Flush outside rlWallMu: additions commute, and this keeps the wall
	// lock from nesting the state mutex.
	s.rlWallMu.Unlock()
	if whole > 0 {
		s.state.AddStepDuration("rate_limit_wait_wall", whole)
	}
}

// withRateLimitWaitObserver subscribes the syncer to every in-process sleep
// that happens below it: rate-limit gate sleeps (SDK client interceptor,
// hosted connector manager) land in rate_limit_wait, and the retryer's
// backoff sleeps land in retry_wait or rate_limit_wait per the event's Retry
// flag. This context observer is the single reporting channel for in-process
// waits (connector-process sleeps arrive separately, via the
// RateLimitWaitReport response annotation). The recordStats check happens at
// report time because the flag is only decided after the store is loaded,
// which is after the observer must already be on the context.
func (s *syncer) withRateLimitWaitObserver(ctx context.Context) context.Context {
	// Start the wall watermark at the top of the run: an end-anchored
	// interval (notably a connector-reported wait) must not count wall
	// time from before this sync process started waiting.
	s.rlWallMu.Lock()
	s.rlWallCoveredUntil = time.Now()
	s.rlWallCarry = 0
	s.rlWallMu.Unlock()
	return ratelimit.WithWaitObserver(ctx, func(ctx context.Context, ev ratelimit.WaitEvent) {
		if !s.recordStats {
			return
		}
		s.recordRetryWait(ctx, ev.Duration, !ev.Retry)
	})
}

func (s *syncer) parallelSync(
	ctx context.Context,
	runCtx context.Context,
	targetedResources []*v2.Resource,
) ([]error, error) {
	l := ctxzap.Extract(ctx)
	workerCtx, cancelWorkers := context.WithCancelCause(ctx)
	stopWorkers := context.AfterFunc(runCtx, func() {
		cancelWorkers(context.Cause(runCtx))
	})
	defer func() {
		stopWorkers()
		cancelWorkers(nil)
	}()

	// Retry backoff sleeps report through the context wait observer
	// installed at the top of Sync, like every other in-process sleep site.
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
			if tooManyWarnings(len(warnings), completedActionsCount) {
				return warnings, fmt.Errorf("%w: warnings: %v completed actions: %d", ErrTooManyWarnings, warnings, completedActionsCount)
			}
		}
		select {
		case <-runCtx.Done():
			err = context.Cause(runCtx)
			switch {
			case errors.Is(err, context.DeadlineExceeded):
				if s.recordStats {
					l.Info("sync run duration has expired, exiting sync early", s.syncSummaryFields(trace.SpanFromContext(ctx))...)
				} else {
					l.Info("sync run duration has expired, exiting sync early", zap.String("sync_id", s.syncID))
				}
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
			err = s.timedStep(SyncResourceTypesOp, func() error {
				return s.SyncResourceTypes(workerCtx, stateAction)
			})
			if !s.timedShouldWaitAndRetry(workerCtx, SyncResourceTypesOp, stateAction.ResourceTypeID, retryer, err) {
				return s.handleOperationError(ctx, runCtx, warnings, err)
			}
			continue

		case SyncResourcesOp:
			if stateAction.ResourceTypeID == "" && stateAction.ResourceID == "" {
				err = s.timedStep(SyncResourcesOp, func() error {
					return s.SyncResources(workerCtx, stateAction)
				})
				if !s.timedShouldWaitAndRetry(workerCtx, SyncResourcesOp, stateAction.ResourceTypeID, retryer, err) {
					return s.handleOperationError(ctx, runCtx, warnings, err)
				}
				continue
			}
			resourceActions := s.state.PeekMatchingActions(ctx, SyncResourcesOp)
			err = s.timedStep(SyncResourcesOp, func() error {
				w, syncErr := s.syncParallel(workerCtx, retryer, resourceActions, s.SyncResources)
				warnings = append(warnings, w...)
				return syncErr
			})
			if err != nil {
				return s.handleOperationError(ctx, runCtx, warnings, err)
			}
			continue

		case SyncTargetedResourceOp:
			targetedResourceActions := s.state.PeekMatchingActions(ctx, SyncTargetedResourceOp)
			err = s.timedStep(SyncTargetedResourceOp, func() error {
				w, syncErr := s.syncParallel(workerCtx, retryer, targetedResourceActions, s.SyncTargetedResource)
				warnings = append(warnings, w...)
				return syncErr
			})
			if err != nil {
				return s.handleOperationError(ctx, runCtx, warnings, err)
			}
			continue

		case SyncStaticEntitlementsOp:
			err = s.timedStep(SyncStaticEntitlementsOp, func() error {
				return s.SyncStaticEntitlements(workerCtx, stateAction)
			})
			if isWarning(ctx, err) {
				l.Warn("skipping sync static entitlements action", zap.Any("stateAction", stateAction), zap.Error(err))
				warnings = append(warnings, err)
				s.state.FinishAction(ctx, stateAction)
				continue
			}
			if !s.timedShouldWaitAndRetry(workerCtx, SyncStaticEntitlementsOp, stateAction.ResourceTypeID, retryer, err) {
				return s.handleOperationError(ctx, runCtx, warnings, err)
			}
			continue
		case SyncEntitlementsOp:
			if stateAction.ResourceTypeID == "" && stateAction.ResourceID == "" {
				err = s.timedStep(SyncEntitlementsOp, func() error {
					return s.SyncEntitlements(workerCtx, stateAction)
				})
				if isWarning(ctx, err) {
					l.Warn("skipping sync entitlement action", zap.Any("stateAction", stateAction), zap.Error(err))
					warnings = append(warnings, err)
					s.state.FinishAction(ctx, stateAction)
					continue
				}
				if !s.timedShouldWaitAndRetry(workerCtx, SyncEntitlementsOp, stateAction.ResourceTypeID, retryer, err) {
					return s.handleOperationError(ctx, runCtx, warnings, err)
				}
				continue
			}
			entitlementActions := s.state.PeekMatchingActions(ctx, SyncEntitlementsOp)
			err = s.timedStep(SyncEntitlementsOp, func() error {
				w, syncErr := s.syncParallel(workerCtx, retryer, entitlementActions, s.SyncEntitlements)
				warnings = append(warnings, w...)
				return syncErr
			})
			if err != nil {
				return s.handleOperationError(ctx, runCtx, warnings, err)
			}
			continue

		case SyncGrantsOp:
			if stateAction.ResourceTypeID == "" && stateAction.ResourceID == "" {
				err = s.timedStep(SyncGrantsOp, func() error {
					return s.SyncGrants(workerCtx, stateAction)
				})
				if isWarning(ctx, err) {
					l.Warn("skipping sync grant action", zap.Any("stateAction", stateAction), zap.Error(err))
					warnings = append(warnings, err)
					s.state.FinishAction(ctx, stateAction)
					continue
				}
				if !s.timedShouldWaitAndRetry(workerCtx, SyncGrantsOp, stateAction.ResourceTypeID, retryer, err) {
					return s.handleOperationError(ctx, runCtx, warnings, err)
				}
				continue
			}

			grantActions := s.state.PeekMatchingActions(ctx, SyncGrantsOp)
			err = s.timedStep(SyncGrantsOp, func() error {
				w, syncErr := s.syncParallel(workerCtx, retryer, grantActions, s.SyncGrants)
				warnings = append(warnings, w...)
				return syncErr
			})
			if err != nil {
				return s.handleOperationError(ctx, runCtx, warnings, err)
			}
			continue

		case SyncExternalResourcesOp:
			err = s.timedStep(SyncExternalResourcesOp, func() error {
				return s.SyncExternalResources(workerCtx, stateAction)
			})
			if !s.timedShouldWaitAndRetry(workerCtx, SyncExternalResourcesOp, stateAction.ResourceTypeID, retryer, err) {
				return s.handleOperationError(ctx, runCtx, warnings, err)
			}
			continue
		case SyncAssetsOp:
			err = s.timedStep(SyncAssetsOp, func() error {
				return s.SyncAssets(workerCtx, stateAction)
			})
			if !s.timedShouldWaitAndRetry(workerCtx, SyncAssetsOp, stateAction.ResourceTypeID, retryer, err) {
				return s.handleOperationError(ctx, runCtx, warnings, err)
			}
			continue

		case SyncGrantExpansionOp:
			// Mark the sync as supporting diff, but only if we're starting fresh.
			// If we're resuming (graph has edges or a page token), we may be continuing
			// from old code that didn't have this marker, so we must not set it.
			entitlementGraph := s.state.EntitlementGraph(ctx)
			isResumingExpansion := entitlementGraph.Loaded || len(entitlementGraph.Edges) > 0 || stateAction.PageToken != ""
			if !isResumingExpansion {
				if s.recordStats {
					l.Info("sync data collection complete", s.syncSummaryFields(trace.SpanFromContext(ctx))...)
				}
				if err := s.store.SyncMeta().MarkSyncSupportsDiff(ctx, s.syncID); err != nil {
					l.Error("failed to set supports_diff marker", zap.Error(err))
					return warnings, err
				}
			}

			if s.dontExpandGrants || !s.state.NeedsExpansion() {
				l.Debug("skipping grant expansion, no grants to expand")
				s.state.FinishAction(ctx, stateAction)
				continue
			}

			err = s.SyncGrantExpansion(workerCtx, stateAction)
			if !retryer.ShouldWaitAndRetry(ratelimit.WithWaitLabel(workerCtx, stateAction.ResourceTypeID), err) {
				return s.handleOperationError(ctx, runCtx, warnings, err)
			}
			continue
		default:
			return warnings, fmt.Errorf("unexpected sync step")
		}
	}
	return warnings, nil
}

func (s *syncer) handleOperationError(
	ctx context.Context,
	runCtx context.Context,
	warnings []error,
	batchErr error,
) ([]error, error) {
	if !errors.Is(context.Cause(runCtx), context.DeadlineExceeded) {
		return warnings, batchErr
	}
	// The run duration expired. The operation error is usually just the
	// resulting cancellation, but a genuine connector/store failure can
	// land in the same window — log it so expiry never masks a real bug.
	// The sync resumes from the checkpoint, so the work is retried either
	// way; only ErrSyncNotComplete is surfaced to keep the resumable
	// contract for callers.
	if batchErr != nil && !errors.Is(batchErr, context.Canceled) && !errors.Is(batchErr, context.DeadlineExceeded) {
		ctxzap.Extract(ctx).Error(
			"sync operation failed while run duration expired; exiting early for resume",
			zap.Error(batchErr),
		)
	}
	checkpointErr := s.Checkpoint(ctx, true)
	return warnings, errors.Join(checkpointErr, ErrSyncNotComplete)
}

func tooManyWarnings(warningCount int, completedActionsCount uint64) bool {
	return warningCount > 10 &&
		completedActionsCount > 0 &&
		float64(warningCount)/float64(completedActionsCount) > 0.1
}

type workerResult struct {
	warning error
	err     error
}

const maxSpawnedCursorsPerBatch = 100_000

// parallelActionQueue is a dynamically extensible worker queue. outstanding
// counts queued plus in-flight actions, so workers stop only after the whole
// fan-out drains. An in-flight action may enqueue siblings before completing;
// therefore outstanding cannot transiently reach zero between parent and child.
type parallelActionQueue struct {
	// mu guards every field below, including the fixed-size cursor digest set.
	mu          native_sync.Mutex
	cond        *native_sync.Cond
	actions     []*Action
	head        int
	outstanding int
	aborted     bool
	seen        map[parallelActionKey]struct{}
}

type parallelActionKey [sha256.Size]byte

func makeParallelActionKey(action *Action) parallelActionKey {
	hasher := sha256.New()
	header := [2]byte{byte(action.Op)}
	if action.TypeScoped {
		header[1] = 1
	}
	_, _ = hasher.Write(header[:])
	for _, value := range []string{
		action.ResourceTypeID,
		action.ResourceID,
		action.ParentResourceTypeID,
		action.ParentResourceID,
		action.PageToken,
	} {
		var length [8]byte
		binary.BigEndian.PutUint64(length[:], uint64(len(value)))
		_, _ = hasher.Write(length[:])
		_, _ = hasher.Write([]byte(value))
	}
	var key parallelActionKey
	hasher.Sum(key[:0])
	return key
}

func newParallelActionQueue(actions []*Action) *parallelActionQueue {
	q := &parallelActionQueue{
		actions:     append([]*Action(nil), actions...),
		outstanding: len(actions),
		seen:        make(map[parallelActionKey]struct{}, len(actions)),
	}
	for _, action := range actions {
		if action != nil {
			q.seen[makeParallelActionKey(action)] = struct{}{}
		}
	}
	q.cond = native_sync.NewCond(&q.mu)
	return q
}

func (q *parallelActionQueue) transition(
	batchOp ActionOp,
	parent *Action,
	nextPageToken string,
	childActions []Action,
	commit func() ([]*Action, error),
) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.aborted {
		return context.Canceled
	}

	keys := make(map[parallelActionKey]struct{}, len(childActions)+1)
	if nextPageToken != "" && parent != nil && parent.Op == batchOp {
		continuedParent := *parent
		continuedParent.PageToken = nextPageToken
		key := makeParallelActionKey(&continuedParent)
		if _, ok := q.seen[key]; ok {
			return fmt.Errorf(
				"duplicate or cyclic spawned cursor for op %s, resource type %q, resource %q, page token %q",
				parent.Op.String(),
				parent.ResourceTypeID,
				parent.ResourceID,
				nextPageToken,
			)
		}
		keys[key] = struct{}{}
	}
	for i := range childActions {
		child := &childActions[i]
		if child.Op != batchOp {
			continue
		}
		key := makeParallelActionKey(child)
		if _, ok := q.seen[key]; ok {
			return fmt.Errorf(
				"duplicate or cyclic spawned cursor for op %s, resource type %q, resource %q, page token %q",
				child.Op.String(),
				child.ResourceTypeID,
				child.ResourceID,
				child.PageToken,
			)
		}
		if _, ok := keys[key]; ok {
			return fmt.Errorf(
				"duplicate or cyclic spawned cursor for op %s, resource type %q, resource %q, page token %q",
				child.Op.String(),
				child.ResourceTypeID,
				child.ResourceID,
				child.PageToken,
			)
		}
		keys[key] = struct{}{}
	}
	if len(q.seen)+len(keys) > maxSpawnedCursorsPerBatch {
		return fmt.Errorf("spawned cursor batch exceeded the maximum of %d unique cursors", maxSpawnedCursorsPerBatch)
	}

	pushed, err := commit()
	if err != nil {
		return err
	}
	for key := range keys {
		q.seen[key] = struct{}{}
	}
	for _, action := range pushed {
		if action != nil && action.Op == batchOp {
			q.actions = append(q.actions, action)
			q.outstanding++
		}
	}
	q.cond.Broadcast()
	return nil
}

func (q *parallelActionQueue) next() (*Action, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for !q.aborted && q.head == len(q.actions) && q.outstanding > 0 {
		q.cond.Wait()
	}
	if q.aborted || q.outstanding == 0 {
		return nil, false
	}
	action := q.actions[q.head]
	q.actions[q.head] = nil
	q.head++
	if q.head == len(q.actions) {
		q.actions = nil
		q.head = 0
	} else if q.head >= 1024 && q.head*2 >= len(q.actions) {
		copy(q.actions, q.actions[q.head:])
		q.actions = q.actions[:len(q.actions)-q.head]
		q.head = 0
	}
	return action, true
}

func (q *parallelActionQueue) done() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.outstanding--
	if q.outstanding == 0 {
		q.cond.Broadcast()
	}
}

func (q *parallelActionQueue) abort() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.aborted = true
	q.cond.Broadcast()
}

func (s *syncer) setParallelActionTransitioner(
	transition func(context.Context, *Action, string, []Action) error,
) {
	s.parallelTransitionMu.Lock()
	defer s.parallelTransitionMu.Unlock()
	s.parallelActionTransitioner = transition
}

func (s *syncer) syncParallel(ctx context.Context, retryer *retry.Retryer, actions []*Action, f func(ctx context.Context, action *Action) error) ([]error, error) {
	l := ctxzap.Extract(ctx)
	l.Info("syncing in parallel", zap.Int("actions", len(actions)), zap.Int("workers", s.workerCount))

	// One bounded summary span per fan-out batch. The per-action work (f) starts
	// its own linked-root span, so this stays a handful of spans per sync rather
	// than one per resource/grant.
	op := ""
	batchOp := UnknownOp
	if len(actions) > 0 {
		batchOp = actions[0].Op
		op = batchOp.String()
	}
	ctx, span := tracer.Start(ctx, "syncer.syncParallel")
	span.SetAttributes(
		attribute.String("sync.op", op),
		attribute.Int("sync.action_count", len(actions)),
		attribute.Int("sync.worker_count", s.workerCount),
	)
	uotel.SetSyncIdentityAttrs(ctx, span)
	var batchErr error
	defer func() { uotel.EndSpanWithError(span, batchErr) }()

	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	queue := newParallelActionQueue(actions)
	s.setParallelActionTransitioner(func(
		transitionCtx context.Context,
		parent *Action,
		nextPageToken string,
		childActions []Action,
	) error {
		return queue.transition(batchOp, parent, nextPageToken, childActions, func() ([]*Action, error) {
			return s.transitionActionState(transitionCtx, parent, nextPageToken, childActions)
		})
	})
	defer s.setParallelActionTransitioner(nil)

	var resultsMu native_sync.Mutex
	var warnings []error
	var errs []error

	var wg native_sync.WaitGroup
	for i := 0; i < s.workerCount; i++ {
		wg.Go(func() {
			for {
				action, ok := queue.next()
				if !ok {
					return
				}
				r := s.syncOneAction(ctx, l, retryer, action, f)
				resultsMu.Lock()
				if r.warning != nil {
					warnings = append(warnings, r.warning)
				}
				if r.err != nil {
					errs = append(errs, r.err)
				}
				resultsMu.Unlock()
				queue.done()
				if r.err != nil {
					l.Error("cancelling context due to error in action", zap.Any("action", action), zap.Error(r.err))
					cancel(fmt.Errorf("cancelling context due to error in action %v: %w", action, r.err))
					queue.abort()
					return
				}
			}
		})
	}

	wg.Wait()

	batchErr = errors.Join(errs...)
	return warnings, batchErr
}

// syncOneAction processes a single action to completion,
// handling pagination by re-reading the action from state after each call.
func (s *syncer) syncOneAction(ctx context.Context, l *zap.Logger, retryer *retry.Retryer, action *Action, f func(ctx context.Context, action *Action) error) workerResult {
	for {
		err := f(ctx, action)
		if isWarning(ctx, err) {
			l.Warn("skipping sync action", zap.Any("action", action), zap.Error(err))
			s.state.FinishAction(ctx, action)
			return workerResult{warning: err}
		}
		if err != nil {
			if retryer.ShouldWaitAndRetry(ratelimit.WithWaitLabel(ctx, action.ResourceTypeID), err) {
				continue
			}

			return workerResult{err: err}
		}

		updated := s.state.GetAction(action.ID)
		if updated == nil {
			return workerResult{}
		}
		action = updated
	}
}
