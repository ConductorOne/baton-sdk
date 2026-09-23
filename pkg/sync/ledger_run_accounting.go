package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"sync"
	"time"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

func (s *syncer) recordRunStepDuration(bucket string, duration time.Duration) {
	s.stats.addStepDuration(bucket, duration)
	if s.ledgered && s.ledger != nil {
		s.ledger.accounting.addStepDuration(bucket, duration)
	}
}

type ledgerRunAccounting struct {
	mu       sync.Mutex
	counters c1zstore.LedgerCounters
}

func (a *ledgerRunAccounting) addStepDuration(bucket string, duration time.Duration) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.counters.StepDurationsMs == nil {
		a.counters.StepDurationsMs = make(map[string]int64)
	}
	a.counters.StepDurationsMs[bucket] += duration.Milliseconds()
}

func (a *ledgerRunAccounting) recordSessionOp(op string, elapsed time.Duration, opErr error, timedOut bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.counters.SessionCalls == nil {
		a.counters.SessionCalls = make(map[string]c1zstore.CallStat)
	}
	add := c1zstore.CallStat{Count: 1, TotalMs: elapsed.Milliseconds(), MaxMs: elapsed.Milliseconds()}
	if opErr != nil {
		add.Errors = 1
		if timedOut {
			add.Timeouts = 1
		}
	}
	stat := a.counters.SessionCalls[op]
	stat.Add(add)
	a.counters.SessionCalls[op] = stat
}

func (a *ledgerRunAccounting) snapshot() c1zstore.LedgerCounters {
	a.mu.Lock()
	defer a.mu.Unlock()
	return cloneLedgerCounters(a.counters)
}

func (r *ledgerRuntime) completeLocalWork(ctx context.Context, work c1zstore.LedgerWork, op ActionOp) error {
	candidate := r.accounting.snapshot()
	if candidate.Counters == nil {
		candidate.Counters = make(map[string]uint64)
	}
	candidate.Counters[ledgerCompletedActions]++
	candidate.Counters[ledgerCompletedPrefix+op.String()]++
	if err := r.store.CompletePendingWork(ctx, work, r.runID, candidate); err != nil {
		return err
	}
	r.accounting.mu.Lock()
	r.accounting.counters.Counters = candidate.Counters
	r.accounting.mu.Unlock()
	return nil
}

func (s *syncer) checkpointLedgerOnStop(ctx context.Context) {
	if s.ledger == nil {
		return
	}
	counters := s.ledger.accounting.snapshot()
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
