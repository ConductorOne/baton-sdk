package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"errors"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

const ledgerTerminalOp = "sync-terminal-v1"

func (r *ledgerRuntime) prepareSeal(ctx context.Context, runCounters c1zstore.LedgerCounters, facts ...string) error {
	pending, phase, err := r.store.PendingWork(ctx, 0, 1)
	if err != nil {
		return err
	}
	switch phase {
	case c1zstore.LedgerQueueAbsent:
		return errors.New("cannot prepare seal without pending-work declaration")
	case c1zstore.LedgerQueueSealing:
		// The terminal page exists from a prior attempt. This attempt's own
		// accounting (invariants, cleanup) still has to reach its bucket.
		if runCounters.IsZero() {
			return nil
		}
		return r.store.PutCounterBucket(ctx, r.runID, c1zstore.RunBucketWorker, runCounters)
	case c1zstore.LedgerQueueCollecting, c1zstore.LedgerQueueExpanding:
	}
	if len(pending) > 0 {
		return errors.New("cannot prepare seal with pending work")
	}
	page := r.store.BeginPage()
	defer page.Discard()
	for _, fact := range facts {
		if err := page.SetFact(fact); err != nil {
			return err
		}
	}
	if err := page.SetTerminal(); err != nil {
		return err
	}
	if err := page.SetCounterBucket(r.runID, c1zstore.RunBucketWorker, runCounters); err != nil {
		return err
	}
	id := c1zstore.LedgerActionIdentity{Op: ledgerTerminalOp}
	return page.Commit(c1zstore.WithOpenPage(ctx), id, &c1zstore.LedgerRow{Identity: id, Attempt: r.runID})
}

func (r *ledgerRuntime) seal(ctx context.Context) error {
	_, phase, err := r.store.PendingWork(ctx, 0, 1)
	if err != nil {
		return err
	}
	if phase != c1zstore.LedgerQueueSealing {
		return errors.New("ledger seal requires terminal page")
	}
	facts, err := r.store.LedgerFacts(ctx)
	if err != nil {
		return err
	}
	counters, err := r.store.LedgerCounters(ctx)
	if err != nil {
		return err
	}
	return r.store.Seal(ctx, c1zstore.LedgerSyncStats(facts, counters))
}
