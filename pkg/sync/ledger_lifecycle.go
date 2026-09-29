package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"errors"
	"fmt"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// A resumer's expansion flags conflict with the pass's state (CO-039 §7).
var ErrLedgerStateConflict = errors.New("sync state conflicts with the requested expansion flags")

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
	case c1zstore.LedgerQueueCollecting, c1zstore.LedgerQueueExpanding:
		return ledgerContinuePending
	case c1zstore.LedgerQueueAbsent:
	}
	if finished {
		return ledgerProcessFinished
	}
	return ledgerSeedPending
}

// Returns the phase the pass was found in, before any write this call makes.
func (s *syncer) prepareLedgerState(ctx context.Context, runID string, newSync bool) (c1zstore.LedgerQueuePhase, error) {
	absent := c1zstore.LedgerQueueAbsent
	ledger := s.caps.pageLedger
	if ledger == nil {
		return absent, errors.New("ledger capability is missing")
	}
	state, err := ledger.State(ctx)
	if err != nil {
		return absent, err
	}
	if state.Token && state.Phase == c1zstore.LedgerQueueAbsent {
		// Decide on the token's own stack before takeover consumes it: a
		// refused request must leave the file as it found it, including
		// resumable by a baseline SDK.
		if err := s.legacyTokenFlagConflict(ctx, state.Finished); err != nil {
			return absent, err
		}
	}
	resume, err := loadLedgerResume(ctx, s.store, ledger, runID)
	if err != nil {
		return absent, err
	}
	if err := s.expansionFlagConflict(ctx, resume, state.Finished, newSync); err != nil {
		return absent, err
	}
	knownEmpty := newSync
	switch resume.preparation(state.Finished) {
	case ledgerProcessFinished:
		seeds := pendingSeeds([]ledgerAction{{identity: c1zstore.LedgerActionIdentity{Op: InitOp.String()}}})
		if err := ledger.BeginPass(ctx, seeds, []string{c1zstore.LedgerFactDiscardOnSeal, c1zstore.LedgerFactRetainTokens}); err != nil {
			return absent, err
		}
	case ledgerSeedPending:
		if len(resume.actions) == 0 {
			resume.actions = []ledgerAction{{identity: c1zstore.LedgerActionIdentity{Op: InitOp.String()}}}
		}
		if !knownEmpty {
			knownEmpty, err = ledger.BoundSyncUnstarted(ctx)
			if err != nil {
				return absent, err
			}
		}
		var seedFacts []string
		if knownEmpty {
			seedFacts = append(seedFacts, ledgerFactIngestKnown)
		}
		if err := ledger.BeginCollecting(ctx, pendingSeeds(resume.actions), seedFacts...); err != nil {
			return absent, err
		}
	case ledgerContinuePending, ledgerFinishSeal:
	}
	if err := ledger.FoldLedgerCounters(ctx, runID); err != nil {
		return absent, err
	}
	return resume.phase, s.restoreLedgerState(ctx, ledger, runID, knownEmpty)
}

// A legacy token is the pass's state before takeover: its stack is the queue.
// The same rule as expansionFlagConflict's Collecting case, read from the
// token so the refusal precedes the takeover write.
func (s *syncer) legacyTokenFlagConflict(ctx context.Context, finished bool) error {
	if !s.cfg.onlyExpandGrants {
		return nil
	}
	token, err := s.store.CurrentSyncStep(ctx)
	if err != nil {
		return fmt.Errorf("read legacy checkpoint: %w", err)
	}
	if token == "" {
		return nil
	}
	resume, _, _, err := decodeLedgerCheckpoint(token)
	if err != nil {
		return err
	}
	if len(resume.actions) == 0 {
		// An empty stack seeds Init, which plans the requested pass; a
		// finished baseline upload takes this path. Unfinished with nothing
		// queued is the unstarted case, decided after takeover on the seed.
		return nil
	}
	for _, action := range resume.actions {
		switch action.identity.Op {
		case InitOp.String():
			if !finished {
				return fmt.Errorf("%w: sync %s is a legacy checkpoint that has not collected; finish it before requesting expansion only", ErrLedgerStateConflict, s.syncID)
			}
		case SyncGrantExpansionOp.String():
		default:
			return fmt.Errorf("%w: sync %s is a legacy checkpoint mid-collection; finish it before requesting expansion only", ErrLedgerStateConflict, s.syncID)
		}
	}
	return nil
}

// The phase is the pass's commitment; a resumer's expansion flags are read
// against it (plan CO-039 §7). Refused before any write.
func (s *syncer) expansionFlagConflict(ctx context.Context, resume ledgerResume, finished, newSync bool) error {
	conflict := func(state, hint string) error {
		return fmt.Errorf("%w: sync %s is %s; %s", ErrLedgerStateConflict, s.syncID, state, hint)
	}
	switch resume.phase {
	case c1zstore.LedgerQueueExpanding:
		if s.cfg.dontExpandGrants {
			return conflict("expanding", "the pass committed to expansion; resume without dont-expand-grants to finish it")
		}
	case c1zstore.LedgerQueueCollecting:
		if !s.cfg.onlyExpandGrants {
			return nil
		}
		// Collection work still queued is the conflict: C1 expands through an
		// empty connector, and running those entries against it would seal a
		// truncated sync. The Init seed alone on an unfinished sync means
		// nothing was collected. The expansion step alone, or a drained
		// queue, is a completed collection.
		pending, _, err := s.caps.pageLedger.PendingWork(ctx, 0, maxPeekActionsCount)
		if err != nil {
			return err
		}
		for _, work := range pending {
			switch work.Action.Identity.Op {
			case InitOp.String():
				if !finished {
					return conflict("unstarted", "nothing has been collected under this sync ID")
				}
			case SyncGrantExpansionOp.String():
			default:
				return conflict("collecting", "the collection is incomplete; finish it before requesting expansion only")
			}
		}
	case c1zstore.LedgerQueueAbsent:
		if s.cfg.onlyExpandGrants && !finished && !newSync {
			return conflict("unstarted", "nothing has been collected under this sync ID")
		}
	case c1zstore.LedgerQueueSealing:
	}
	return nil
}
