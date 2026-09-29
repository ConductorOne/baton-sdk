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

// The declaration's phase is the pass's state; ended_at is read only when
// there is no declaration.
func (r ledgerResume) preparation(finished bool) ledgerPreparation {
	switch r.phase {
	case c1zstore.LedgerQueueSealing:
		return ledgerFinishSeal
	case c1zstore.LedgerQueueCollecting:
		return ledgerContinuePending
	case c1zstore.LedgerQueueAbsent:
	}
	if finished {
		return ledgerProcessFinished
	}
	return ledgerSeedPending
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
	resume, err := loadLedgerResume(ctx, s.store, ledger, runID)
	if err != nil {
		return err
	}
	knownEmpty := newSync
	switch resume.preparation(finished) {
	case ledgerProcessFinished:
		seeds := pendingSeeds([]ledgerAction{{identity: c1zstore.LedgerActionIdentity{Op: InitOp.String()}}})
		if err := ledger.BeginPass(ctx, seeds, []string{c1zstore.LedgerFactDiscardOnSeal, c1zstore.LedgerFactRetainTokens}); err != nil {
			return err
		}
	case ledgerSeedPending:
		if len(resume.actions) == 0 {
			resume.actions = []ledgerAction{{identity: c1zstore.LedgerActionIdentity{Op: InitOp.String()}}}
		}
		if !knownEmpty {
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
