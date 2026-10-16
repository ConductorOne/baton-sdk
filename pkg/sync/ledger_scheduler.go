package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"errors"
	"slices"
	"strings"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// persistenceError reports a failed action transition, checkpoint or page commit
// as Canceled when its context was canceled and Internal otherwise. The code comes
// from that failure alone, so a joined connector status cannot turn it into a
// warning or a retry. A deadline stays Internal: the retryer retries
// DeadlineExceeded, and a retry after a failed restart checkpoint sends the
// restarted listing's first request before its budget is durable
// (TestPageTokenRestartCheckpointDeadlineIsNotRetried).
type persistenceError struct {
	cause error
	code  codes.Code
}

func newPersistenceError(failure error, interrupted ...error) error {
	code := codes.Internal
	if errors.Is(failure, context.Canceled) {
		code = codes.Canceled
	}
	return persistenceError{cause: errors.Join(append(interrupted, failure)...), code: code}
}

func (e persistenceError) Error() string { return e.cause.Error() }
func (e persistenceError) Unwrap() error { return e.cause }
func (e persistenceError) GRPCStatus() *status.Status {
	return status.New(e.code, e.cause.Error())
}

type ledgerWorkerKey struct{}
type ledgerInvocationKey struct{}
type ledgerCommitKey struct{}

type ledgerInvocation struct {
	attempts              *ledgerAttempts
	action                *Action
	page                  *ledgerPage
	children              []Action
	afterCommit           []func()
	connectorObservations c1zstore.LedgerCounters
}

type ledgerTransitionCommit struct {
	commit  func() error
	warning bool
}

func ledgerIdentity(action *Action) c1zstore.LedgerActionIdentity {
	return c1zstore.LedgerActionIdentity{
		Op: action.Op.String(), ResourceTypeID: action.ResourceTypeID, ResourceID: action.ResourceID,
		ParentResourceTypeID: action.ParentResourceTypeID, ParentResourceID: action.ParentResourceID,
		PageToken: action.PageToken, TypeScoped: action.TypeScoped,
	}
}

func (i *ledgerInvocation) stage(action *Action, next string, children []Action) error {
	if action.ID != i.action.ID {
		return errors.New("ledger page transitioned a different action")
	}
	scheduled := make(map[string]bool)
	accepted := make([]Action, 0, len(children))
	for _, child := range children {
		key := ledgerSchedulingKey(child)
		if strings.HasPrefix(key, "resource:") {
			if scheduled[key] {
				continue
			}
			scheduled[key] = true
		}
		accepted = append(accepted, child)
	}
	children = accepted
	rows := make([]c1zstore.LedgerChild, 0, len(children))
	for index := range children {
		child := &children[index]
		if child.ID != "" {
			return errors.New("action ID must be empty for new actions")
		}
		rows = append(rows, c1zstore.LedgerChild{Identity: ledgerIdentity(child), Spawned: child.Spawned})
	}
	if err := i.page.transition(next, rows...); err != nil {
		return err
	}
	i.children = slices.Clone(children)
	return nil
}

func (s *syncer) invokeActionPage(ctx context.Context, action *Action, handler func(context.Context, *Action) error, allowWarning bool) error {
	request, err := providerPageAction(action)
	if err != nil {
		return err
	}
	if !s.ledgered {
		cause := handler(ctx, request)
		if cause == nil {
			return nil
		}
		next, restartErr := s.cfg.pageTokenRestartPolicy.next(ctx, request, cause)
		if restartErr != nil {
			if errors.Is(restartErr, ErrPageTokenRestartLimit) {
				if err := s.Checkpoint(ctx, true); err != nil {
					return newPersistenceError(err, restartErr)
				}
			}
			return restartErr
		}
		restarted := *action
		restarted.PageTokenRestarted = true
		if err := s.nextPageOrFinishAction(ctx, &restarted, next); err != nil {
			return newPersistenceError(err, cause)
		}
		if err := s.Checkpoint(ctx, true); err != nil {
			return newPersistenceError(err, cause)
		}
		logPageTokenRestart(ctx, request, cause)
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.ledger == nil {
		return errors.New("ledger runtime is not initialized")
	}
	if action.WorkID == 0 {
		return errors.New("ledger action has no pending work ID")
	}
	worker, _ := ctx.Value(ledgerWorkerKey{}).(int)
	if worker < 0 || worker >= int(c1zstore.TakeoverBucketWorker) {
		return errors.New("invalid ledger worker index")
	}
	workerIndex := uint32(worker)
	attempts, _ := ctx.Value(ledgerAttemptsKey{}).(*ledgerAttempts)
	if attempts == nil {
		attempts = &ledgerAttempts{}
	}
	attempts.selectPage(ledgerIdentity(action))
	invocation := &ledgerInvocation{action: action, attempts: attempts}
	var warning error
	var handlerFailure error
	var restartCause error
	transitionAction := action
	_, err = s.ledger.runPageWithCommit(ctx, workerIndex, ledgerIdentity(action), func(pageCtx context.Context, page *ledgerPage) error {
		invocation.page = page
		page.row.WorkID = action.WorkID
		page.row.WorkRevision = action.WorkRevision
		ledgerCollection(invocation)
		page.row.Spawned = action.Spawned
		page.row.TypeScopedPlanned = action.TypeScopedPlanned
		pageCtx = context.WithValue(pageCtx, ledgerInvocationKey{}, invocation)
		var err error
		switch {
		case s.testHooks.ledgerHandler != nil:
			err = s.testHooks.ledgerHandler(pageCtx, request, page)
		case action.Op == InitOp || action.Op == SyncResourceTypesOp || action.Op == SyncResourcesOp || action.Op == SyncTargetedResourceOp || action.Op == SyncEntitlementsOp ||
			action.Op == SyncGrantsOp || action.Op == SyncStaticEntitlementsOp || action.Op == SyncAssetsOp || action.Op == MaterializeStaticEntitlementsOp:
			err = handler(pageCtx, request)
		default:
			return errors.New("ledger production handlers are not integrated")
		}
		invocation.connectorObservations = attempts.addObservations(invocation.connectorObservations)
		if err != nil {
			if next, restartErr := s.cfg.pageTokenRestartPolicy.next(pageCtx, request, err); restartErr == nil {
				restartCause = err
				restarted := *action
				restarted.PageTokenRestarted = true
				transitionAction = &restarted
				err = invocation.stage(action, next, nil)
			} else {
				err = restartErr
			}
		}
		if err != nil {
			if !allowWarning || !isWarning(pageCtx, err) {
				handlerFailure = err
				return err
			}
			warning = err
			if page.transitions != 0 {
				return errors.New("ledger warning followed an action transition")
			}
			if err := invocation.stage(action, "", nil); err != nil {
				return err
			}
		}
		if page.transitions == 1 && page.row.NextPageToken == "" {
			if page.observations.Counters == nil {
				page.observations.Counters = make(map[string]uint64)
			}
			page.observations.Counters[ledgerCompletedActions]++
			page.observations.Counters[ledgerCompletedPrefix+action.Op.String()]++
			if warning != nil {
				page.observations.Counters[ledgerWarningsPrefix+action.Op.String()]++
			}
		}
		childKeys := make([]string, len(invocation.children))
		for i, child := range invocation.children {
			childKeys[i] = ledgerSchedulingKey(child)
		}
		if err := page.writer.SetPendingWork(s.pendingForAction(transitionAction), childKeys...); err != nil {
			return err
		}
		page.observations = addLedgerCounters(page.observations, invocation.connectorObservations)
		attempts.snapshot(&page.row)
		return nil
	}, func(page *ledgerPage, commit func() error) error {
		publish := func() error {
			if err := commit(); err != nil {
				return err
			}
			attempts.committed()
			if s.testHooks.ledgerCommitted != nil {
				s.testHooks.ledgerCommitted(page.row)
			}
			if page.row.TypeScopedPlanned {
				s.markTypeScopedPlanned(action)
			}
			for fact := range page.facts {
				s.run.setFact(fact)
			}
			return nil
		}
		commitCtx := context.WithValue(ctx, ledgerCommitKey{}, ledgerTransitionCommit{commit: publish, warning: warning != nil})
		return s.nextPageOrFinishAction(commitCtx, transitionAction, page.row.NextPageToken, invocation.children...)
	})
	if err != nil {
		if restartCause != nil {
			return newPersistenceError(err, restartCause)
		}
		if handlerFailure == nil && isWarning(ctx, err) {
			return newPersistenceError(err)
		}
		return err
	}
	s.publishLedgerConnectorObservations(invocation.connectorObservations)
	if restartCause != nil {
		logPageTokenRestart(ctx, request, restartCause)
	}
	for _, publish := range invocation.afterCommit {
		publish()
	}
	return warning
}

func ledgerActionFromIdentity(id c1zstore.LedgerActionIdentity) Action {
	return Action{Op: newActionOp(id.Op), ResourceTypeID: id.ResourceTypeID, ResourceID: id.ResourceID,
		ParentResourceTypeID: id.ParentResourceTypeID, ParentResourceID: id.ParentResourceID, PageToken: id.PageToken, TypeScoped: id.TypeScoped}
}
