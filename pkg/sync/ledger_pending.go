package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

const pendingAdmissionWindow = 64

func (s *syncer) actionFromPending(work c1zstore.LedgerWork) (Action, error) {
	if work.SyncID != s.syncID {
		return Action{}, errors.New("pending work belongs to another sync")
	}
	action := ledgerActionFromIdentity(work.Action.Identity)
	if action.Op == UnknownOp {
		return Action{}, errors.New("pending work contains an unknown operation")
	}
	action.ID = fmt.Sprintf("%020d", work.ID)
	action.WorkID = work.ID
	action.WorkRevision = work.Revision
	action.WorkPageToken = work.Action.Identity.PageToken
	action.Spawned = work.Action.Spawned
	action.TypeScopedPlanned = work.TypeScopedPlanned
	return action, nil
}

func (s *syncer) pendingForAction(action *Action) c1zstore.LedgerWork {
	identity := ledgerIdentity(action)
	identity.PageToken = action.WorkPageToken
	return c1zstore.LedgerWork{SyncID: s.syncID, ID: action.WorkID, Revision: action.WorkRevision,
		Action: c1zstore.LedgerChild{Identity: identity, Spawned: action.Spawned}, TypeScopedPlanned: action.TypeScopedPlanned}
}

func (s *syncer) refreshPendingWindow(ctx context.Context) error {
	work, initialized, err := s.caps.pageLedger.PendingWork(ctx, 0, maxPeekActionsCount)
	if err != nil {
		return err
	}
	if !initialized && !s.run.hasFact(ledgerFactSealReady) {
		return errors.New("pending work is not initialized")
	}
	actions := make([]Action, 0, len(work))
	for _, item := range work {
		action, err := s.actionFromPending(item)
		if err != nil {
			return err
		}
		actions = append(actions, action)
	}
	s.run.mu.Lock()
	defer s.run.mu.Unlock()
	prior := s.run.actions
	s.run.actions = make(map[string]Action, len(actions))
	s.run.actionOrder = nil
	s.run.spawnedInFlight = make(map[string]Action)
	s.run.spawnedAdmitted = make(map[parallelActionKey]string)
	for i := len(actions) - 1; i >= 0; i-- {
		action := actions[i]
		if action.Op == SyncGrantExpansionOp || action.Op == SyncExternalResourcesOp {
			if old, ok := prior[action.ID]; ok && old.WorkRevision == action.WorkRevision {
				action.PageToken = old.PageToken
			}
		}
		s.run.actions[action.ID] = action
		s.run.actionOrder = append(s.run.actionOrder, action.ID)
		if action.Spawned {
			s.run.spawnedInFlight[action.ID] = action
		}
	}
	return nil
}

func (s *syncer) pendingRefill(ctx context.Context, op ActionOp, after *uint64) ([]*Action, error) {
	for {
		work, initialized, err := s.caps.pageLedger.PendingWorkAfter(ctx, *after, pendingAdmissionWindow)
		if err != nil {
			return nil, err
		}
		if !initialized {
			return nil, errors.New("pending queue disappeared during execution")
		}
		if len(work) == 0 {
			return nil, nil
		}
		actions := make([]*Action, 0, len(work))
		for _, item := range work {
			if item.ID <= *after {
				return nil, errors.New("pending work cursor did not advance")
			}
			*after = item.ID
			action, err := s.actionFromPending(item)
			if err != nil {
				return nil, err
			}
			if action.Op == op {
				actions = append(actions, &action)
			}
		}
		if len(actions) == 0 {
			continue
		}
		s.run.mu.Lock()
		for _, action := range actions {
			if _, exists := s.run.actions[action.ID]; exists {
				s.run.mu.Unlock()
				return nil, errors.New("pending work admitted twice")
			}
		}
		for _, action := range actions {
			s.run.actions[action.ID] = *action
			s.run.actionOrder = append(s.run.actionOrder, action.ID)
			if action.Spawned {
				s.run.spawnedInFlight[action.ID] = *action
			}
		}
		slices.Sort(s.run.actionOrder)
		s.run.mu.Unlock()
		return actions, nil
	}
}

func (s *syncer) publishPendingTransition(ctx context.Context, action *Action, next string, warning bool) {
	if warning {
		return
	}
	if next == "" {
		s.run.finishAction(ctx, action)
		s.recordListResourceCompletedThisRun(action)
		return
	}
	s.run.mu.Lock()
	defer s.run.mu.Unlock()
	updated := s.run.actions[action.ID]
	updated.PageToken = next
	updated.WorkPageToken = next
	updated.WorkRevision = action.WorkRevision + 1
	s.run.actions[action.ID] = updated
}

func ledgerSchedulingKey(action Action) string {
	if action.Spawned {
		return fmt.Sprintf("spawn:%x", makeParallelActionKey(&action))
	}
	if action.Op == SyncResourcesOp && action.ParentResourceTypeID != "" && action.ParentResourceID != "" {
		return "resource:" + childScheduleKey(action.ResourceTypeID, action.ParentResourceTypeID, action.ParentResourceID)
	}
	return ""
}

func pendingSeeds(actions []ledgerAction) []c1zstore.LedgerWork {
	work := make([]c1zstore.LedgerWork, 0, len(actions))
	for _, entry := range actions {
		action := ledgerActionFromIdentity(entry.identity)
		action.Spawned = entry.spawned
		if action.Op == SyncGrantExpansionOp {
			action.PageToken = ""
		}
		work = append(work, c1zstore.LedgerWork{Action: c1zstore.LedgerChild{Identity: ledgerIdentity(&action), Spawned: action.Spawned},
			TypeScopedPlanned: entry.typeScopedPlanned, SchedulingKey: ledgerSchedulingKey(action)})
	}
	return work
}

func (s *syncer) runPendingLocalStep(ctx context.Context, action *Action, handler func() error) error {
	if !s.ledgered {
		return handler()
	}
	s.run.mu.RLock()
	beforeTotal := s.run.completedActions
	beforeCount := s.run.actionCounts[action.Op.String()]
	s.run.mu.RUnlock()
	if err := handler(); err != nil {
		return err
	}
	if s.run.getAction(action.ID) != nil {
		return nil
	}
	counters := s.ledger.runCounterSnapshot()
	if counters.Counters == nil {
		counters.Counters = make(map[string]uint64)
	}
	counters.Counters[ledgerCompletedActions]++
	counters.Counters[ledgerCompletedPrefix+action.Op.String()]++
	if err := s.caps.pageLedger.CompletePendingWork(ctx, s.pendingForAction(action), s.ledger.runID, counters); err != nil {
		s.run.mu.Lock()
		s.run.completedActions = beforeTotal
		s.run.actionCounts[action.Op.String()] = beforeCount
		s.run.actions[action.ID] = *action
		s.run.actionOrder = append(s.run.actionOrder, action.ID)
		slices.Sort(s.run.actionOrder)
		if action.Spawned {
			s.run.spawnedInFlight[action.ID] = *action
		}
		s.run.mu.Unlock()
		return err
	}
	s.ledger.mu.Lock()
	s.ledger.localCompleted = maps.Clone(counters.Counters)
	s.ledger.mu.Unlock()
	return nil
}
