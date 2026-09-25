package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"errors"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

func (r *ledgerRuntime) flushRunCounters(ctx context.Context, counters c1zstore.LedgerCounters) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.active) != 0 {
		return errors.New("cannot flush ledger run accounting with active pages")
	}
	if r.closing {
		return errors.New("cannot flush ledger run accounting after terminal page")
	}
	return r.store.PutCounterBucket(ctx, r.runID, c1zstore.RunBucketWorker, counters)
}
