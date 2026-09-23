package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

func ledgerListingFixtureRoots() []ledgerAction {
	return []ledgerAction{{identity: c1zstore.LedgerActionIdentity{Op: SyncResourceTypesOp.String()}}}
}

func runLedgerSchedulerFixture(t *testing.T, runtime *ledgerRuntime, roots []ledgerAction, workers uint32,
	handler func(context.Context, *syncer, *Action, *ledgerPage) error,
) error {
	t.Helper()
	s := &syncer{caps: storeCaps{pageLedger: runtime.store}, ledgered: true, ledger: runtime, run: newRunState(), stats: newRunStats(), cfg: syncConfig{workerCount: int(workers)}}
	pending, initialized, err := runtime.store.PendingWork(t.Context(), 0, 100)
	if err != nil {
		return err
	}
	if !initialized {
		err := func() error {
			if guarded, ok := runtime.store.(*ledgerGuardedStore); ok {
				guarded.audit.mu.Lock()
				phase := guarded.audit.phase
				guarded.audit.mu.Unlock()
				if phase == ledgerWalk {
					return errLedgerFixtureWrite
				}
				guarded.audit.enter(ledgerLifecycle)
				defer guarded.audit.enter(phase)
			}
			return runtime.store.InitializePendingWork(t.Context(), pendingSeeds(roots))
		}()
		if err != nil {
			return err
		}
		pending, _, err = runtime.store.PendingWork(t.Context(), 0, 100)
		if err != nil {
			return err
		}
	}
	if len(pending) > 0 {
		s.syncID = pending[0].SyncID
	}
	if err := s.restoreLedgerState(t.Context(), s.ledger.store, s.ledger.runID, false); err != nil {
		return err
	}
	s.testHooks.ledgerHandler = func(ctx context.Context, action *Action, page *ledgerPage) error {
		return handler(ctx, s, action, page)
	}
	_, err = s.parallelSync(t.Context(), t.Context(), nil)
	return err
}

func ledgerFixtureTransition(ctx context.Context, s *syncer, action *Action, next string, rows ...c1zstore.LedgerChild) error {
	children := make([]Action, 0, len(rows))
	for _, row := range rows {
		child := ledgerActionFromIdentity(row.Identity)
		child.Spawned = row.Spawned
		children = append(children, child)
	}
	return s.nextPageOrFinishAction(ctx, action, next, children...)
}
