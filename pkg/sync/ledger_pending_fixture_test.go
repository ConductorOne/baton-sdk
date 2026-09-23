package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/stretchr/testify/require"
)

func seedLedgerTestRun(t *testing.T, s *syncer, selected *Action) {
	t.Helper()
	if !s.ledgered {
		return
	}
	pending, initialized, err := s.caps.pageLedger.PendingWork(t.Context(), 0, 100)
	require.NoError(t, err)
	if initialized {
		if selected != nil && selected.WorkID == 0 {
			for _, work := range pending {
				if work.Action.Identity == ledgerIdentity(selected) {
					mapped, err := s.actionFromPending(work)
					require.NoError(t, err)
					*selected = mapped
					return
				}
			}
			t.Fatal("test action was not persisted before execution")
		}
		return
	}
	if guarded, ok := s.caps.pageLedger.(*ledgerGuardedStore); ok {
		guarded.audit.mu.Lock()
		phase := guarded.audit.phase
		writers := guarded.audit.writers
		guarded.audit.mu.Unlock()
		require.NotEqual(t, ledgerWalk, phase, "fixture setup must precede the read-only resume assertion")
		require.Zero(t, writers)
		guarded.audit.enter(ledgerLifecycle)
		defer guarded.audit.enter(phase)
	}
	var roots []ledgerAction
	var selectedID uint64
	for i, id := range s.run.actionOrder {
		action := s.run.actions[id]
		roots = append(roots, ledgerAction{identity: ledgerIdentity(&action), spawned: action.Spawned, typeScopedPlanned: action.TypeScopedPlanned})
		if selected != nil && selected.ID == id {
			selectedID = uint64(i) + 1
		}
	}
	var facts []string
	for fact, established := range s.run.facts.established {
		if established {
			facts = append(facts, fact)
		}
	}
	require.NoError(t, s.caps.pageLedger.InitializePendingWork(t.Context(), pendingSeeds(roots), facts...))
	require.NoError(t, s.refreshPendingWindow(t.Context()))
	if selected != nil {
		work, _, err := s.caps.pageLedger.PendingWork(t.Context(), selectedID+1, 1)
		require.NoError(t, err)
		require.Len(t, work, 1)
		require.Equal(t, selectedID, work[0].ID)
		mapped, err := s.actionFromPending(work[0])
		require.NoError(t, err)
		*selected = mapped
	}
}

func invokeLedgerTestPage(t *testing.T, s *syncer, ctx context.Context, action *Action, handler func(context.Context, *Action) error, warning bool) error {
	t.Helper()
	seedLedgerTestRun(t, s, action)
	err := s.invokeActionPage(ctx, action, handler, warning)
	if err == nil && s.ledgered {
		return s.refreshPendingWindow(ctx)
	}
	return err
}

func runLedgerTestSync(t *testing.T, s *syncer, ctx, runCtx context.Context, targeted []*v2.Resource) ([]error, error) {
	t.Helper()
	seedLedgerTestRun(t, s, nil)
	return s.parallelSync(ctx, runCtx, targeted)
}

func hasLedgerScheduledChild(t *testing.T, s *syncer, child, parentType, parent string) bool {
	t.Helper()
	found, err := s.caps.pageLedger.HasScheduledWork(t.Context(), "resource:"+childScheduleKey(child, parentType, parent))
	require.NoError(t, err)
	return found
}

func restoreLedgerTestState(t *testing.T, s *syncer, ctx context.Context, resume ledgerResume, knownEmpty bool) error {
	t.Helper()
	if s.caps.pageLedger == nil {
		s.caps.pageLedger = s.ledger.store
	}
	pending, initialized, err := s.caps.pageLedger.PendingWork(ctx, 0, 100)
	if err != nil {
		return err
	}
	if !initialized {
		if err := s.caps.pageLedger.InitializePendingWork(ctx, pendingSeeds(resume.actions)); err != nil {
			return err
		}
		pending, _, err = s.caps.pageLedger.PendingWork(ctx, 0, 100)
		if err != nil {
			return err
		}
	}
	if len(pending) > 0 {
		s.syncID = pending[0].SyncID
	}
	return s.restoreLedgerState(ctx, resume, knownEmpty)
}
