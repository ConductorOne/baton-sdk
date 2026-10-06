package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// A resumer's flags conflict with the pass's state (CO-039 §7).
var ErrLedgerStateConflict = errors.New("sync state conflicts with the requested flags")

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
	firstOptions, err := s.encodeLedgerReportOptions(runID)
	if err != nil {
		return absent, err
	}
	resume, err := loadLedgerResume(ctx, s.store, ledger, runID, firstOptions)
	if err != nil {
		return absent, err
	}
	facts, err := s.flagConflict(ctx, resume, state.Finished, newSync)
	if err != nil {
		return absent, err
	}
	knownEmpty := newSync
	switch resume.preparation(state.Finished) {
	case ledgerProcessFinished:
		if !s.cfg.onlyExpandGrants {
			return absent, fmt.Errorf("%w: sync %s is finished; %s", ErrLedgerStateConflict, s.syncID, finishedSyncHint)
		}
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
		seedFacts := map[string]string{}
		if knownEmpty {
			seedFacts[ledgerFactIngestKnown] = ""
		}
		if legacyStackHasCollection(resume.actions) {
			// A frontier's stack seeded here is a plan committing now, as at
			// takeover; a record already on the file is the earlier plan's.
			existing, err := ledger.LedgerFacts(ctx)
			if err != nil {
				return absent, err
			}
			if _, recorded := existing[c1zstore.LedgerFactFirstReportOptions]; !recorded {
				seedFacts[c1zstore.LedgerFactFirstReportOptions] = firstOptions
			}
		}
		if err := ledger.BeginCollecting(ctx, pendingSeeds(resume.actions), seedFacts); err != nil {
			return absent, err
		}
	case ledgerContinuePending, ledgerFinishSeal:
	}
	if err := ledger.FoldLedgerCounters(ctx, runID); err != nil {
		return absent, err
	}
	return resume.phase, s.restoreLedgerStateWithFacts(ctx, ledger, runID, knownEmpty, facts)
}

const finishedSyncHint = "a finished sync accepts only-expand-grants; start a new sync to collect again"

// A legacy token is the pass's state before takeover: its stack is the queue.
// The same rules as flagConflict's Collecting case, read from the
// token so the refusal precedes the takeover write.
func (s *syncer) legacyTokenFlagConflict(ctx context.Context, finished bool) error {
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
	// An empty stack seeds Init, which plans the requested pass; on a
	// finished sync that is a baseline upload, and the pass may only be
	// expansion. Unfinished with nothing queued is the unstarted case,
	// decided after takeover on the seed.
	collectionQueued := false
	for _, action := range resume.actions {
		switch action.identity.Op {
		case InitOp.String():
			if !finished && s.cfg.onlyExpandGrants {
				return fmt.Errorf("%w: sync %s is a legacy checkpoint that has not collected; finish it before requesting expansion only", ErrLedgerStateConflict, s.syncID)
			}
		case SyncGrantExpansionOp.String(), SyncExternalResourcesOp.String():
		default:
			collectionQueued = true
		}
	}
	if collectionQueued && s.cfg.onlyExpandGrants {
		return fmt.Errorf("%w: sync %s is a legacy checkpoint mid-collection; finish it before requesting expansion only", ErrLedgerStateConflict, s.syncID)
	}
	if finished && !collectionQueued && !s.cfg.onlyExpandGrants {
		return fmt.Errorf("%w: sync %s is a finished legacy checkpoint; %s", ErrLedgerStateConflict, s.syncID, finishedSyncHint)
	}
	return nil
}

// The phase is the pass's commitment; a resumer's flags are read against it
// (plan CO-039 §7). Refused before any write. Returns the fact set when the
// check read it, so the restore that follows does not read it again; the
// writes between them touch no facts.
func (s *syncer) flagConflict(ctx context.Context, resume ledgerResume, finished, newSync bool) (map[string]string, error) {
	conflict := func(state, hint string) error {
		return fmt.Errorf("%w: sync %s is %s; %s", ErrLedgerStateConflict, s.syncID, state, hint)
	}
	switch resume.phase {
	case c1zstore.LedgerQueueExpanding:
		if s.cfg.dontExpandGrants {
			return nil, conflict("expanding", "the pass committed to expansion; resume without dont-expand-grants to finish it")
		}
	case c1zstore.LedgerQueueCollecting:
		// Collection work still queued conflicts with only-expand: C1 expands
		// through an empty connector, and running those entries against it
		// would seal a truncated sync. The Init seed alone on an unfinished
		// sync means nothing was collected; on a finished sync it is an
		// expansion pass that has not planned yet, and only an expansion
		// resumer may plan it. The expansion step and the external import
		// call no connector, so alone or with a drained queue they are a
		// completed collection.
		pending, _, err := s.caps.pageLedger.PendingWork(ctx, 0, maxPeekActionsCount)
		if err != nil {
			return nil, err
		}
		initQueued, collectionQueued := false, false
		for _, work := range pending {
			switch work.Action.Identity.Op {
			case InitOp.String():
				initQueued = true
				if !finished && s.cfg.onlyExpandGrants {
					return nil, conflict("unstarted", "nothing has been collected under this sync ID")
				}
			case SyncGrantExpansionOp.String(), SyncExternalResourcesOp.String():
			default:
				collectionQueued = true
			}
		}
		if collectionQueued && s.cfg.onlyExpandGrants {
			return nil, conflict("collecting", "the collection is incomplete; finish it before requesting expansion only")
		}
		if initQueued && finished && !collectionQueued && !s.cfg.onlyExpandGrants {
			return nil, conflict("finished", finishedSyncHint)
		}
		if collectionQueued {
			return s.collectionFlagConflict(ctx)
		}
	case c1zstore.LedgerQueueAbsent:
		if s.cfg.onlyExpandGrants && !finished && !newSync {
			return nil, conflict("unstarted", "nothing has been collected under this sync ID")
		}
	case c1zstore.LedgerQueueSealing:
	}
	return nil, nil
}

// The collection flags are the pass's from the commit that planned under
// them: the Init page, or the takeover that adopted a legacy stack. An
// attempt that finds none has nothing to compare against and continues.
func (s *syncer) collectionFlagConflict(ctx context.Context) (map[string]string, error) {
	facts, err := s.caps.pageLedger.LedgerFacts(ctx)
	if err != nil {
		return nil, err
	}
	recorded, ok := facts[c1zstore.LedgerFactFirstReportOptions]
	if !ok {
		return facts, nil
	}
	var first c1zstore.LedgerReportOptions
	if err := json.Unmarshal([]byte(recorded), &first); err != nil {
		return nil, fmt.Errorf("decode first report options: %w", err)
	}
	diffs := collectionFlagDifferences(first.Requested, s.requestedOptions())
	if len(diffs) == 0 {
		return facts, nil
	}
	return nil, fmt.Errorf("%w: sync %s is collecting under different flags (%s); resume with the flags the collection started with",
		ErrLedgerStateConflict, s.syncID, strings.Join(diffs, ", "))
}
