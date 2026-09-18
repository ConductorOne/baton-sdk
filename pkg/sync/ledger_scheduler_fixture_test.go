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
	s := &syncer{ledgered: true, ledger: runtime, run: newRunState(), stats: newRunStats(), cfg: syncConfig{workerCount: int(workers)}}
	if err := s.restoreLedgerState(t.Context(), ledgerResume{actions: roots}, false); err != nil {
		return err
	}
	s.testHooks.ledgerHandler = func(ctx context.Context, action *Action, page *ledgerPage) error {
		return handler(ctx, s, action, page)
	}
	_, err := s.parallelSync(t.Context(), t.Context(), nil)
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
