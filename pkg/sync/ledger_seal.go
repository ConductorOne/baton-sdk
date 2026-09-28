package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"errors"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

const ledgerTerminalOp = "sync-terminal-v1"
const ledgerFactSealReady = "sync.seal_ready"

func (r *ledgerRuntime) prepareSeal(ctx context.Context, runCounters c1zstore.LedgerCounters, facts ...string) error {
	pending, phase, err := r.store.PendingWork(ctx, 0, 1)
	if err != nil {
		return err
	}
	if len(pending) > 0 {
		return errors.New("cannot prepare seal with pending work")
	}
	stored, err := r.store.LedgerFacts(ctx)
	if err != nil {
		return err
	}
	_, ready := stored[ledgerFactSealReady]
	if phase == c1zstore.LedgerQueueAbsent {
		_, discarding := stored[c1zstore.LedgerFactDiscardOnSeal]
		if !discarding || !ready {
			return errors.New("cannot prepare seal without pending-work declaration")
		}
	}
	if ready {
		// The terminal page exists from a prior attempt. This attempt's own
		// accounting (invariants, cleanup) still has to reach its bucket.
		if runCounters.IsZero() {
			return nil
		}
		return r.store.PutCounterBucket(ctx, r.runID, c1zstore.RunBucketWorker, runCounters)
	}
	page := r.store.BeginPage()
	defer page.Discard()
	for _, fact := range facts {
		if err := page.SetFact(fact); err != nil {
			return err
		}
	}
	if err := page.SetFact(ledgerFactSealReady); err != nil {
		return err
	}
	if err := page.SetCounterBucket(r.runID, c1zstore.RunBucketWorker, runCounters); err != nil {
		return err
	}
	id := c1zstore.LedgerActionIdentity{Op: ledgerTerminalOp}
	return page.Commit(c1zstore.WithOpenPage(ctx), id, &c1zstore.LedgerRow{Identity: id, Attempt: r.runID})
}

func (r *ledgerRuntime) seal(ctx context.Context) error {
	facts, err := r.store.LedgerFacts(ctx)
	if err != nil {
		return err
	}
	if _, ready := facts[ledgerFactSealReady]; !ready {
		return errors.New("ledger seal requires terminal page")
	}
	counters, err := r.store.LedgerCounters(ctx)
	if err != nil {
		return err
	}
	return r.store.EndSyncWithStats(ctx, c1zstore.LedgerSyncStats(facts, counters))
}
