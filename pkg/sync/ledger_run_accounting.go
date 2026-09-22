package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"time"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

func (s *syncer) recordRunStepDuration(bucket string, duration time.Duration) {
	s.stats.addStepDuration(bucket, duration)
	if s.ledgered && s.ledger != nil {
		s.ledger.runObservations.addStepDuration(bucket, duration)
	}
}

func (r *ledgerRuntime) runCounterSnapshot() c1zstore.LedgerCounters {
	counters := c1zstore.LedgerCounters{StepDurationsMs: r.runObservations.stepDurations(), SessionCalls: make(map[string]c1zstore.CallStat)}
	for method, stat := range r.runObservations.sessionStoreStats() {
		counters.SessionCalls[method] = c1zstore.CallStat{Count: stat.Count, TotalMs: stat.TotalMs, MaxMs: stat.MaxMs, Errors: stat.Errors, Timeouts: stat.Timeouts}
	}
	return counters
}

func (s *syncer) checkpointLedgerOnStop(ctx context.Context) {
	if s.ledger == nil {
		return
	}
	counters := s.ledger.runCounterSnapshot()
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

func (s *syncer) terminalLedgerCounters() c1zstore.LedgerCounters {
	counters := s.ledger.runCounterSnapshot()
	key := ledgerCompletedPrefix + SyncGrantExpansionOp.String()
	completed := s.run.getActionCount(SyncGrantExpansionOp).CompletedCount
	prior := s.ledger.prior.Counters[key]
	if completed > prior {
		counters.Counters = map[string]uint64{ledgerCompletedActions: completed - prior, key: completed - prior}
	}
	return counters
}
