package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

// completeLedgerLocalWork removes a finished local phase's pending entry and
// writes this attempt's run bucket with the completion counted, in one batch.
// The in-memory counters advance only after the store accepted the batch.
func (s *syncer) completeLedgerLocalWork(ctx context.Context, work c1zstore.LedgerWork, op ActionOp) error {
	candidate := s.stats.attemptLedgerCounters()
	candidate.Counters[ledgerCompletedActions]++
	candidate.Counters[ledgerCompletedPrefix+op.String()]++
	if err := s.ledger.store.CompletePendingWork(ctx, work, s.ledger.runID, candidate); err != nil {
		return err
	}
	s.stats.addAttemptCounter(ledgerCompletedActions, 1)
	s.stats.addAttemptCounter(ledgerCompletedPrefix+op.String(), 1)
	return nil
}

func (s *syncer) checkpointLedgerOnStop(ctx context.Context) {
	if s.ledger == nil {
		return
	}
	counters := s.stats.attemptLedgerCounters()
	if counters.IsZero() {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), stopCheckpointTimeout)
	defer cancel()
	if err := s.ledger.flushRunCounters(ctx, counters); err != nil {
		ctxzap.Extract(ctx).Error("error persisting run accounting while stopping sync", zap.Error(err))
		return
	}
	if s.testHooks.ledgerStop != nil {
		s.testHooks.ledgerStop(ctx)
	}
}
