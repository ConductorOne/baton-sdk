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
		// A declaration means a pass is open; ended_at says nothing about it.
		// Drained work seals; pending work continues.
		if r.sealReady {
			return ledgerFinishSeal
		}
		return ledgerContinuePending
	case finished:
		// No declaration on a finished sync: whatever a legacy state decoded
		// to, the pass it described is over. The next pass starts from the
		// archive.
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
	resume, err := loadLedgerResume(ctx, s.store, ledger, runID, facts)
	if err != nil {
		return err
	}
	knownEmpty := newSync
	switch resume.preparation(finished) {
	case ledgerProcessFinished:
		seeds := pendingSeeds([]ledgerAction{{identity: c1zstore.LedgerActionIdentity{Op: InitOp.String()}}})
		if err := ledger.BeginPass(ctx, seeds, []string{ledgerFactSealReady, c1zstore.LedgerFactDiscardOnSeal, c1zstore.LedgerFactRetainTokens}); err != nil {
			return err
		}
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
