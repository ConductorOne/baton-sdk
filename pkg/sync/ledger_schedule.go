package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// Called by the coordinator after every worker has been joined.
func (r *ledgerRuntime) flushRunCounters(ctx context.Context, counters c1zstore.LedgerCounters) error {
	return r.store.PutCounterBucket(ctx, r.runID, c1zstore.RunBucketWorker, counters)
}
