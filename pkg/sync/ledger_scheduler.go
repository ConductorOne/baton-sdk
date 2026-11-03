package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"errors"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ledgerPageWriteError struct {
	cause error
}

func (e ledgerPageWriteError) Error() string { return e.cause.Error() }
func (e ledgerPageWriteError) Unwrap() error { return e.cause }
func (e ledgerPageWriteError) GRPCStatus() *status.Status {
	return status.New(codes.Internal, e.cause.Error())
}

type ledgerTransitionCommit struct {
	commit  func() error
	warning bool
	// The pending item continues at the next token even when it is "".
	restart bool
}

func ledgerIdentity(action *Action) c1zstore.LedgerActionIdentity {
	return c1zstore.LedgerActionIdentity{
		Op: action.Op.String(), ResourceTypeID: action.ResourceTypeID, ResourceID: action.ResourceID,
		ParentResourceTypeID: action.ParentResourceTypeID, ParentResourceID: action.ParentResourceID,
		PageToken: action.PageToken, TypeScoped: action.TypeScoped,
	}
}

func (s *syncer) invokeActionPage(
	ctx context.Context,
	action *Action,
	handler func(context.Context, *ledgerPage, *Action) error,
	allowWarning bool,
	worker int,
	attempts *ledgerAttempts,
	transition func(context.Context, ledgerTransitionCommit, *Action, string, []Action) error,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.ledger == nil {
		return errors.New("ledger runtime is not initialized")
	}
	if action.WorkID == 0 {
		return errors.New("ledger action has no pending work ID")
	}
	if worker < 0 || worker >= int(c1zstore.TakeoverBucketWorker) {
		return errors.New("invalid ledger worker index")
	}
	workerIndex := uint32(worker)
	if attempts == nil {
		attempts = &ledgerAttempts{}
	}
	attempts.selectPage(ledgerIdentity(action))
	var opened *ledgerPage
	var warning error
	var handlerFailure error
	_, err := s.ledger.runPageWithCommit(ctx, workerIndex, ledgerIdentity(action), func(pageCtx context.Context, page *ledgerPage) error {
		opened = page
		page.action = action
		page.attempts = attempts
		page.worker = worker
		page.row.WorkID = action.WorkID
		page.row.WorkRevision = action.WorkRevision
		ledgerCollection(page)
		page.row.Spawned = action.Spawned
		if action.TypeScopedPlanned {
			page.markTypeScopedPlanned()
		}
		var err error
		switch {
		case s.testHooks.ledgerHandler != nil:
			err = s.testHooks.ledgerHandler(pageCtx, page, action)
		case action.Op == InitOp || action.Op == SyncResourceTypesOp || action.Op == SyncResourcesOp || action.Op == SyncTargetedResourceOp || action.Op == SyncEntitlementsOp ||
			action.Op == SyncGrantsOp || action.Op == SyncStaticEntitlementsOp || action.Op == SyncAssetsOp || action.Op == MaterializeStaticEntitlementsOp:
			err = handler(pageCtx, page, action)
		default:
			return errors.New("ledger production handlers are not integrated")
		}
		page.connectorObservations = attempts.addObservations(page.connectorObservations)
		if err != nil {
			detail, present, classErr := classifyRestartRequest(action, err)
			switch {
			case classErr != nil:
				handlerFailure = classErr
				return classErr
			case present:
				if err := s.stageActionRestart(page, action, detail, err); err != nil {
					return err
				}
				ctxzap.Extract(pageCtx).Info("connector invalidated the action's page token; restarting the action", restartLogFields(action, detail)...)
			case !allowWarning || !isWarning(pageCtx, err):
				handlerFailure = err
				return err
			default:
				warning = err
				if page.transitions != 0 {
					return errors.New("ledger warning followed an action transition")
				}
				if err := page.stage(action, "", nil); err != nil {
					return err
				}
			}
		}
		if page.transitions == 1 && page.row.NextPageToken == "" && !page.row.Restarted {
			if page.observations.Counters == nil {
				page.observations.Counters = make(map[string]uint64)
			}
			page.observations.Counters[ledgerCompletedActions]++
			page.observations.Counters[ledgerCompletedPrefix+action.Op.String()]++
			if warning != nil {
				page.observations.Counters[ledgerWarningsPrefix+action.Op.String()]++
			}
		}
		childKeys := make([]string, len(page.children))
		for i, child := range page.children {
			childKeys[i] = ledgerSchedulingKey(child)
		}
		if err := page.writer.SetPendingWork(s.pendingForAction(action), childKeys...); err != nil {
			return err
		}
		page.addObservations(page.connectorObservations)
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
		pending := ledgerTransitionCommit{commit: publish, warning: warning != nil, restart: page.row.Restarted}
		if transition != nil {
			return transition(ctx, pending, action, page.row.NextPageToken, page.children)
		}
		return s.transitionActionState(ctx, action, page.row.NextPageToken, page.children, pending)
	})
	if err != nil {
		if handlerFailure == nil && isWarning(ctx, err) {
			return ledgerPageWriteError{cause: err}
		}
		var restart *restartRequest
		if opened != nil {
			restart = opened.restart
		}
		return wrapRestartCommitError(action, restart, err)
	}
	s.publishLedgerConnectorObservations(opened.connectorObservations)
	for _, publish := range opened.afterCommit {
		publish()
	}
	return warning
}

func ledgerActionFromIdentity(id c1zstore.LedgerActionIdentity) Action {
	return Action{Op: newActionOp(id.Op), ResourceTypeID: id.ResourceTypeID, ResourceID: id.ResourceID,
		ParentResourceTypeID: id.ParentResourceTypeID, ParentResourceID: id.ParentResourceID, PageToken: id.PageToken, TypeScoped: id.TypeScoped}
}
