package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"errors"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

func (s *syncer) prepareLedgerState(ctx context.Context, runID string, newSync bool) (bool, error) {
	ledger := s.caps.pageLedger
	if ledger == nil {
		return false, errors.New("ledger capability is missing")
	}
	finished, err := ledger.BoundSyncFinished(ctx)
	if err != nil {
		return false, err
	}
	facts, err := ledger.LedgerFacts(ctx)
	if err != nil {
		return false, err
	}
	_, discardPending := facts[c1zstore.LedgerFactDiscardOnSeal]
	if discardPending {
		finished = false
	}
	if len(facts) == 0 || len(facts) == 1 && discardPending {
		archive, err := ledger.GetArchivedLedgerReport(ctx)
		if err != nil {
			return false, err
		}
		if len(archive) > 0 {
			if err := ledger.RestoreLedgerArchive(ctx); err != nil {
				return false, err
			}
		}
	}
	resume, err := loadLedgerResume(ctx, s.store, ledger, runID)
	if err != nil {
		return false, err
	}
	if finished && (resume.sealReady || len(resume.actions) == 0) {
		if err := ledger.ClearLedgerRows(ctx, []string{ledgerFactSealReady, c1zstore.LedgerFactDiscardOnSeal}); err != nil {
			return false, err
		}
		resume.actions = []ledgerAction{{identity: c1zstore.LedgerActionIdentity{Op: InitOp.String()}}}
		resume.sealReady = false
	}
	if len(resume.actions) == 0 && !resume.sealReady {
		resume.actions = []ledgerAction{{identity: c1zstore.LedgerActionIdentity{Op: InitOp.String()}}}
	}
	s.ledger, err = newLedgerRuntime(ctx, ledger, runID)
	if err != nil {
		return false, err
	}
	if err := s.restoreLedgerState(ctx, resume, newSync); err != nil {
		return false, err
	}
	return resume.sealReady, nil
}
