package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"errors"
	"slices"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
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

type ledgerWorkerKey struct{}
type ledgerInvocationKey struct{}
type ledgerCommitKey struct{}
type ledgerReplayKey struct{}

type ledgerInvocation struct {
	action           *Action
	page             *ledgerPage
	children         []Action
	afterCommit      []func()
	resourceChildren bool
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
	if !s.ledgered {
		return handler(ctx, action)
	}
	if s.ledger == nil {
		return errors.New("ledger runtime is not initialized")
	}
	row, found, err := s.ledger.store.GetLedgerRow(ctx, ledgerIdentity(action))
	if err != nil {
		return ledgerPageWriteError{cause: err}
	}
	if found && row != nil && row.Identity == ledgerIdentity(action) {
		if row.Scrubbed {
			return errLedgerScrubbedUnfinished
		}
		children := make([]Action, 0, len(row.Children))
		for _, recorded := range row.Children {
			child := ledgerActionFromIdentity(recorded.Identity)
			if child.Op == UnknownOp {
				return errors.New("ledger row contains an unknown child operation")
			}
			child.Spawned = recorded.Spawned
			if child.Op == SyncResourcesOp && child.ParentResourceTypeID != "" && child.ParentResourceID != "" {
				s.childSchedule.recordIfNew(child.ResourceTypeID, child.ParentResourceTypeID, child.ParentResourceID)
			}
			children = append(children, child)
		}
		if row.TypeScopedPlanned {
			s.run.markTypeScopedPlanned(action.ID)
		}
		return s.nextPageOrFinishAction(context.WithValue(ctx, ledgerReplayKey{}, true), action, row.NextPageToken, children...)
	}
	worker, _ := ctx.Value(ledgerWorkerKey{}).(int)
	if worker < 0 || worker >= int(c1zstore.TakeoverBucketWorker) {
		return errors.New("invalid ledger worker index")
	}
	invocation := &ledgerInvocation{action: action}
	var warning error
	var handlerFailure error
	_, err = s.ledger.runPageWithCommit(ctx, uint32(worker), ledgerIdentity(action), func(pageCtx context.Context, page *ledgerPage) error {
		invocation.page = page
		page.row.Spawned = action.Spawned
		page.row.TypeScopedPlanned = action.TypeScopedPlanned
		pageCtx = context.WithValue(pageCtx, ledgerInvocationKey{}, invocation)
		var err error
		switch {
		case s.testHooks.ledgerHandler != nil:
			err = s.testHooks.ledgerHandler(pageCtx, action, page)
		case action.Op == InitOp || action.Op == SyncResourceTypesOp || action.Op == SyncResourcesOp || action.Op == SyncTargetedResourceOp || action.Op == SyncEntitlementsOp ||
			action.Op == SyncGrantsOp:
			err = handler(pageCtx, action)
		default:
			return errors.New("ledger production handlers are not integrated")
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
		return nil
	}, func(page *ledgerPage, commit func() error) error {
		if invocation.resourceChildren {
			s.childSchedule.mu.Lock()
			defer s.childSchedule.mu.Unlock()
			seen := make(map[string]bool)
			children := make([]Action, 0, len(invocation.children))
			rows := make([]c1zstore.LedgerChild, 0, len(invocation.children))
			for _, child := range invocation.children {
				key := childScheduleKey(child.ResourceTypeID, child.ParentResourceTypeID, child.ParentResourceID)
				if child.Op == SyncResourcesOp {
					if _, exists := s.childSchedule.m[key]; exists || seen[key] {
						continue
					}
					seen[key] = true
				}
				children = append(children, child)
				rows = append(rows, c1zstore.LedgerChild{Identity: ledgerIdentity(&child), Spawned: child.Spawned})
			}
			invocation.children, page.row.Children = children, rows
		}
		publish := func() error {
			if err := commit(); err != nil {
				return err
			}
			if invocation.resourceChildren {
				if s.childSchedule.m == nil {
					s.childSchedule.m = make(map[string]struct{})
				}
				for _, child := range invocation.children {
					if child.Op == SyncResourcesOp {
						s.childSchedule.m[childScheduleKey(child.ResourceTypeID, child.ParentResourceTypeID, child.ParentResourceID)] = struct{}{}
					}
				}
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
		return s.nextPageOrFinishAction(commitCtx, action, page.row.NextPageToken, invocation.children...)
	})
	if err != nil {
		if handlerFailure == nil && isWarning(ctx, err) {
			return ledgerPageWriteError{cause: err}
		}
		return err
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
