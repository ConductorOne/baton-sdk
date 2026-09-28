package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"time"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/ratelimit"
)

type ledgerAttemptsKey struct{}

// ledgerAttempts is owned by one goroutine: the worker whose context carries
// it (syncParallel mints one per worker) or the coordinator (parallelSync
// mints one for serial steps). Every mutator runs on that goroutine, including
// recordWait, which ratelimit.ObserveWait invokes from the goroutine that
// slept — retry.Retryer.ShouldWaitAndRetry and the unary client interceptor,
// neither of which hands the context to another goroutine. No mutex;
// TestLedgerWaitObservationsStayWithWorker pins the ownership.
type ledgerAttempts struct {
	identity                    c1zstore.LedgerActionIdentity
	observations                c1zstore.LedgerCounters
	attempts, errors            uint64
	retryWait, rateLimitWait    time.Duration
	connectorTime, reportedWait time.Duration
}

func withLedgerAttempts(ctx context.Context) context.Context {
	return context.WithValue(ctx, ledgerAttemptsKey{}, &ledgerAttempts{})
}

func (a *ledgerAttempts) selectPage(id c1zstore.LedgerActionIdentity) {
	if a.identity != id {
		a.identity = id
		a.attempts, a.errors, a.retryWait, a.rateLimitWait = 0, 0, 0, 0
		a.connectorTime, a.reportedWait = 0, 0
		a.observations = c1zstore.LedgerCounters{}
	}
}

func (a *ledgerAttempts) recordCall(elapsed time.Duration) {
	a.attempts++
	a.connectorTime += elapsed
}

func (a *ledgerAttempts) recordReportedWait(wait time.Duration) {
	a.reportedWait += wait
}

func (a *ledgerAttempts) recordError(err error) {
	if err == nil {
		return
	}
	a.errors++
}

func (a *ledgerAttempts) recordWait(ev ratelimit.WaitEvent) {
	if ev.Retry {
		a.retryWait += ev.Duration
	} else {
		a.rateLimitWait += ev.Duration
	}
}

func (a *ledgerAttempts) snapshot(row *c1zstore.LedgerRow) {
	row.ObservationsRecorded = true
	if a.attempts > 0 {
		row.ConnectorDuration, row.WaitDuration = a.connectorTime, a.reportedWait
	}
	row.ConnectorAttempts, row.ConnectorErrors = a.attempts, a.errors
	row.SDKRetryWaitDuration, row.SDKRateLimitWaitDuration = a.retryWait, a.rateLimitWait
}

func (a *ledgerAttempts) committed() {
	a.attempts, a.errors, a.retryWait, a.rateLimitWait = 0, 0, 0, 0
	a.connectorTime, a.reportedWait = 0, 0
	a.observations = c1zstore.LedgerCounters{}
}

func recordLedgerConnectorError(invocation *ledgerInvocation, err error) {
	if invocation.attempts != nil {
		invocation.attempts.recordError(err)
	}
}

func (a *ledgerAttempts) addObservations(observations c1zstore.LedgerCounters) c1zstore.LedgerCounters {
	a.observations = addLedgerCounters(a.observations, observations)
	return cloneLedgerCounters(a.observations)
}

func (s *syncer) publishLedgerConnectorObservations(observations c1zstore.LedgerCounters) {
	for method, stat := range observations.ConnectorCalls {
		s.stats.mergeConnectorCallStat(method, ConnectorCallStat{Count: stat.Count, TotalMs: stat.TotalMs, MaxMs: stat.MaxMs})
	}
	for method, stat := range observations.SessionCalls {
		s.stats.mergeSessionStat(method, SessionStoreStat{
			Count: stat.Count, Errors: stat.Errors, Timeouts: stat.Timeouts, TotalMs: stat.TotalMs, MaxMs: stat.MaxMs,
		})
	}
	for bucket, ms := range observations.StepDurationsMs {
		s.stats.mergeStepDuration(bucket, time.Duration(ms)*time.Millisecond)
	}
}
