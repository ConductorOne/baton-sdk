package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	native_sync "sync"
	"time"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/ratelimit"
)

type ledgerAttemptsKey struct{}

type ledgerAttempts struct {
	mu                          native_sync.Mutex
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
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.identity != id {
		a.identity = id
		a.attempts, a.errors, a.retryWait, a.rateLimitWait = 0, 0, 0, 0
		a.connectorTime, a.reportedWait = 0, 0
		a.observations = c1zstore.LedgerCounters{}
	}
}

func (a *ledgerAttempts) recordCall(elapsed time.Duration) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.attempts++
	a.connectorTime += elapsed
}

func (a *ledgerAttempts) recordReportedWait(wait time.Duration) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.reportedWait += wait
}

func (a *ledgerAttempts) recordError(err error) {
	if err == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.errors++
}

func (a *ledgerAttempts) recordWait(ev ratelimit.WaitEvent) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if ev.Retry {
		a.retryWait += ev.Duration
	} else {
		a.rateLimitWait += ev.Duration
	}
}

func (a *ledgerAttempts) snapshot(row *c1zstore.LedgerRow) {
	a.mu.Lock()
	defer a.mu.Unlock()
	row.ObservationsRecorded = true
	if a.attempts > 0 {
		row.ConnectorDuration, row.WaitDuration = a.connectorTime, a.reportedWait
	}
	row.ConnectorAttempts, row.ConnectorErrors = a.attempts, a.errors
	row.SDKRetryWaitDuration, row.SDKRateLimitWaitDuration = a.retryWait, a.rateLimitWait
}

func (a *ledgerAttempts) committed() {
	a.mu.Lock()
	defer a.mu.Unlock()
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
	a.mu.Lock()
	defer a.mu.Unlock()
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
		s.stats.addStepDuration(bucket, time.Duration(ms)*time.Millisecond)
	}
}
