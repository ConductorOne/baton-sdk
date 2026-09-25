package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"errors"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

type ledgerPreparation uint8

const (
	ledgerContinuePending ledgerPreparation = iota
	ledgerFinishSeal
	ledgerSeedPending
	ledgerProcessFinished
)

func (r ledgerResume) preparation(finished bool) ledgerPreparation {
	switch {
	case r.initialized:
		if r.sealReady {
			return ledgerFinishSeal
		}
		if finished && !r.hasPendingWork {
			return ledgerProcessFinished
		}
		return ledgerContinuePending
	case finished && (r.sealReady || len(r.actions) == 0):
		return ledgerProcessFinished
	case r.sealReady:
		return ledgerFinishSeal
	default:
		return ledgerSeedPending
	}
}

func (s *syncer) prepareLedgerState(ctx context.Context, runID string, newSync bool) error {
	ledger := s.caps.pageLedger
	if ledger == nil {
		return errors.New("ledger capability is missing")
	}
	finished, err := ledger.BoundSyncFinished(ctx)
	if err != nil {
		return err
	}
	facts, err := ledger.LedgerFacts(ctx)
	if err != nil {
		return err
	}
	_, discardPending := facts[c1zstore.LedgerFactDiscardOnSeal]
	finished = finished && !discardPending
	if len(facts) == 0 || len(facts) == 1 && discardPending {
		archive, err := ledger.GetArchivedLedgerReport(ctx)
		if err != nil {
			return err
		}
		if len(archive) > 0 {
			if err := ledger.RestoreLedgerArchive(ctx); err != nil {
				return err
			}
			facts, err = ledger.LedgerFacts(ctx)
			if err != nil {
				return err
			}
		}
	}
	resume, err := loadLedgerResume(ctx, s.store, ledger, runID, facts)
	if err != nil {
		return err
	}
	knownEmpty := newSync
	switch resume.preparation(finished) {
	case ledgerProcessFinished:
		if err := ledger.ClearLedgerRows(ctx, []string{ledgerFactSealReady, c1zstore.LedgerFactDiscardOnSeal, c1zstore.LedgerFactRetainTokens}); err != nil {
			return err
		}
		resume.actions = nil
		fallthrough
	case ledgerSeedPending:
		if len(resume.actions) == 0 {
			resume.actions = []ledgerAction{{identity: c1zstore.LedgerActionIdentity{Op: InitOp.String()}}}
		}
		if !knownEmpty && !finished && !discardPending {
			knownEmpty, err = ledger.BoundSyncUnstarted(ctx)
			if err != nil {
				return err
			}
		}
		var seedFacts []string
		if knownEmpty {
			seedFacts = append(seedFacts, ledgerFactIngestKnown)
		}
		if err := ledger.InitializePendingWork(ctx, pendingSeeds(resume.actions), seedFacts...); err != nil {
			return err
		}
	case ledgerContinuePending, ledgerFinishSeal:
	}
	if err := ledger.FoldLedgerCounters(ctx, runID); err != nil {
		return err
	}
	return s.restoreLedgerState(ctx, ledger, runID, knownEmpty)
}
