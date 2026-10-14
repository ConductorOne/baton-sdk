package pebble

import (
	"context"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/internal/rawdb"
)

// ErrNothingToRollBack is returned when the sync has grants but none carries
// Sources, so expansion left nothing to remove.
var ErrNothingToRollBack = errors.New("pebble: no grant carries expansion sources; nothing to roll back")

// ErrRollbackIncompleteCollection is returned for an unfinished sync whose
// collection is not known to be complete. Rolling it back and sealing would
// stamp ended_at on partial data.
var ErrRollbackIncompleteCollection = errors.New("pebble: refusing to roll back expansion before collection completes")

const rollbackExpansionRoundRows = 10_000

// RollbackExpansionResult counts what RollbackExpansion changed, or would
// change on a dry run. SuspectConnectorSourced counts grants with Sources, no
// self-source, and no GrantImmutable; they are in GrantsDeleted unless
// preserved, in which case they are in SuspectPreserved.
type RollbackExpansionResult struct {
	GrantsDeleted           int
	SourcesCleared          int
	SuspectConnectorSourced int
	SuspectPreserved        int
}

// RollbackExpansion restores syncID's grants to their pre-expansion shape,
// classifying each by its Sources the way dotc1z's SQLite RollbackExpansion
// does: a grant whose Sources lack a self-source is deleted, and a grant whose
// Sources include one has them cleared. A pending pass in Expanding or
// Sealing, or any pending pass on a finished sync, is dropped so a replay
// starts a fresh pass. The caller seals: grant indexes, digests, and stats are
// stale until the next EndSync.
func (e *Engine) RollbackExpansion(ctx context.Context, syncID string, dryRun, preserveSuspect bool) (*RollbackExpansionResult, error) {
	record, err := e.GetSyncRunRecord(ctx, syncID)
	if err != nil {
		return nil, fmt.Errorf("pebble: rollback could not load sync %q: %w", syncID, err)
	}
	finished := record.GetEndedAt() != nil
	if !finished && record.GetSyncToken() != "" {
		return nil, fmt.Errorf("%w: sync %q holds a legacy checkpoint", ErrRollbackIncompleteCollection, syncID)
	}
	_, phase, err := e.ledger.PendingWork(ctx, 0, 1)
	if err != nil {
		return nil, fmt.Errorf("pebble: rollback could not read pending work: %w", err)
	}
	if !finished && (phase == c1zstore.LedgerQueueAbsent || phase == c1zstore.LedgerQueueCollecting) {
		return nil, fmt.Errorf("%w: sync %q is unfinished with pending work %s", ErrRollbackIncompleteCollection, syncID, phase)
	}
	if phase != c1zstore.LedgerQueueAbsent {
		ctxzap.Extract(ctx).Warn("pebble: rollback found a pending pass; a write drops it and a replay starts a new one",
			zap.String("sync_id", syncID),
			zap.Stringer("phase", phase),
			zap.Bool("finished", finished),
			zap.Bool("dry_run", dryRun),
		)
	}

	res := &RollbackExpansionResult{}
	var scanned int
	if dryRun {
		scanned, err = e.rollbackExpansionRounds(ctx, false, preserveSuspect, res)
	} else {
		if err := e.SetCurrentSync(ctx, syncID); err != nil {
			return nil, err
		}
		if phase != c1zstore.LedgerQueueAbsent {
			if err := e.ledger.Drop(ctx); err != nil {
				return nil, fmt.Errorf("pebble: rollback could not drop pending pass: %w", err)
			}
		}
		scanned, err = e.rollbackExpansionRounds(ctx, true, preserveSuspect, res)
	}
	if err != nil {
		return nil, fmt.Errorf("pebble: rollback failed: %w", err)
	}
	if scanned > 0 && res.GrantsDeleted+res.SourcesCleared+res.SuspectPreserved == 0 {
		return nil, fmt.Errorf("%w: sync %q", ErrNothingToRollBack, syncID)
	}
	return res, nil
}

// rollbackExpansionRounds commits each round's deletes and clears when write
// is set, and returns the number of grants scanned.
func (e *Engine) rollbackExpansionRounds(ctx context.Context, write, preserveSuspect bool, res *RollbackExpansionResult) (int, error) {
	lower := encodeGrantPrefix()
	upper := upperBoundOf(lower)
	scanned := 0
	for lower != nil {
		if err := ctx.Err(); err != nil {
			return scanned, err
		}
		var next []byte
		var n int
		var err error
		if !write {
			next, n, err = e.rollbackExpansionRound(lower, upper, nil, preserveSuspect, res)
		} else {
			err = e.withWrite(func() error {
				if err := e.requireCurrentSync(); err != nil {
					return err
				}
				batch := e.db.NewRecordBatch()
				defer batch.Close()
				var roundErr error
				next, n, roundErr = e.rollbackExpansionRound(lower, upper, batch, preserveSuspect, res)
				if roundErr != nil {
					return roundErr
				}
				if batch.Empty() {
					return nil
				}
				return batch.Commit(writeOpts(e.opts.durability))
			})
		}
		if err != nil {
			return scanned, err
		}
		scanned += n
		lower = next
	}
	return scanned, nil
}

// rollbackExpansionRound classifies up to rollbackExpansionRoundRows grants
// in [lower, upper), staging each delete or clear into batch when batch is
// non-nil. The iterator is closed on return, before the caller commits.
// Returns the next round's lower bound, or nil when the range is exhausted.
func (e *Engine) rollbackExpansionRound(
	lower, upper []byte,
	batch *rawdb.RecordBatch,
	preserveSuspect bool,
	res *RollbackExpansionResult,
) ([]byte, int, error) {
	iter, err := e.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return nil, 0, err
	}
	next, n, err := rollbackExpansionRows(iter, batch, preserveSuspect, res)
	return next, n, errors.Join(err, iter.Close())
}

func rollbackExpansionRows(
	iter *pebble.Iterator,
	batch *rawdb.RecordBatch,
	preserveSuspect bool,
	res *RollbackExpansionResult,
) ([]byte, int, error) {
	var n int
	var err error
	var facts []grantSourceFact
	var record v3.GrantRecord
	var valScratch []byte
	for valid := iter.First(); valid; valid = iter.Next() {
		if n == rollbackExpansionRoundRows {
			return append([]byte(nil), iter.Key()...), n, nil
		}
		n++
		key, val := iter.Key(), iter.Value()
		var isImmutable bool
		isImmutable, facts, err = scanGrantContentFactsRawBytes(val, facts[:0])
		if err != nil {
			return nil, n, fmt.Errorf("grant %x: %w", key, err)
		}
		if len(facts) == 0 {
			continue
		}
		var ownEntitlementID string
		_, _, ownEntitlementID, _, _, _, err = scanGrantIndexFieldsRaw(val)
		if err != nil {
			return nil, n, fmt.Errorf("grant %x: %w", key, err)
		}
		hasSelf := false
		for _, f := range facts {
			if string(f.key) == ownEntitlementID {
				hasSelf = true
				break
			}
		}

		if hasSelf {
			res.SourcesCleared++
			if batch == nil {
				continue
			}
			record.Reset()
			if err := unmarshalRecord(val, &record); err != nil {
				return nil, n, fmt.Errorf("grant %x: %w", key, err)
			}
			record.SetSources(nil)
			valScratch, err = marshalRecordAppend(valScratch[:0], &record)
			if err != nil {
				return nil, n, fmt.Errorf("grant %x: %w", key, err)
			}
			if err := batch.StageGrantPutDeferred(key, valScratch, val, record.GetNeedsExpansion()); err != nil {
				return nil, n, err
			}
			continue
		}

		suspect := !isImmutable
		if suspect {
			res.SuspectConnectorSourced++
			if preserveSuspect {
				res.SuspectPreserved++
				continue
			}
		}
		res.GrantsDeleted++
		if batch != nil {
			if err := batch.StageGrantDelete(key, val); err != nil {
				return nil, n, err
			}
		}
	}
	return nil, n, iter.Error()
}
