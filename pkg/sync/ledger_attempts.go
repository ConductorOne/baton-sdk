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
	mu                       native_sync.Mutex
	identity                 c1zstore.LedgerActionIdentity
	attempts, errors         uint64
	retryWait, rateLimitWait time.Duration
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
	}
}

func (a *ledgerAttempts) recordCall() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.attempts++
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
	row.ConnectorAttempts, row.ConnectorErrors = a.attempts, a.errors
	row.SDKRetryWaitDuration, row.SDKRateLimitWaitDuration = a.retryWait, a.rateLimitWait
}

func (a *ledgerAttempts) committed() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.attempts, a.errors, a.retryWait, a.rateLimitWait = 0, 0, 0, 0
}

func recordLedgerConnectorError(invocation *ledgerInvocation, err error) {
	if invocation.attempts != nil {
		invocation.attempts.recordError(err)
	}
}
