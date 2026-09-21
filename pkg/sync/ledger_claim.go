package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

func (r *ledgerRuntime) claimPage(ctx context.Context, id c1zstore.LedgerActionIdentity) (func(), error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		r.mu.Lock()
		if r.claims == nil {
			r.claims = make(map[c1zstore.LedgerActionIdentity]chan struct{})
		}
		done, busy := r.claims[id]
		if !busy {
			done = make(chan struct{})
			r.claims[id] = done
			r.mu.Unlock()
			return func() { r.mu.Lock(); delete(r.claims, id); close(done); r.mu.Unlock() }, nil
		}
		r.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-done:
		}
	}
}
