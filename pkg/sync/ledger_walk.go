package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"errors"
	"slices"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

var errLedgerScrubbedUnfinished = errors.New("unfinished ledger contains scrubbed rows without seal-ready proof")

const ledgerFactSealReady = "sync.seal_ready"

type ledgerAction struct {
	identity          c1zstore.LedgerActionIdentity
	spawned           bool
	typeScopedPlanned bool
}

func (r *ledgerRuntime) walk(ctx context.Context, roots []ledgerAction) ([]ledgerAction, error) {
	return r.walkWithSeen(ctx, roots, make(map[c1zstore.LedgerActionIdentity]bool))
}

func (r *ledgerRuntime) walkWithSeen(ctx context.Context, roots []ledgerAction, seen map[c1zstore.LedgerActionIdentity]bool) ([]ledgerAction, error) {
	r.mu.Lock()
	_, ready := r.facts[ledgerFactSealReady]
	r.mu.Unlock()
	if ready {
		return nil, nil
	}
	stack := slices.Clone(roots)
	var pending []ledgerAction
	for len(stack) != 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		action := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if seen[action.identity] {
			continue
		}
		seen[action.identity] = true
		row, found, err := r.store.GetLedgerRow(ctx, action.identity)
		if err != nil {
			return nil, err
		}
		if found && row != nil && row.Scrubbed {
			return nil, errLedgerScrubbedUnfinished
		}
		if !found || row == nil || row.Identity != action.identity {
			pending = append(pending, action)
			continue
		}
		if row.NextPageToken != "" {
			next := action
			next.identity.PageToken = row.NextPageToken
			next.spawned = row.Spawned
			next.typeScopedPlanned = row.TypeScopedPlanned
			stack = append(stack, next)
		}
		for _, child := range row.Children {
			stack = append(stack, ledgerAction{identity: child.Identity, spawned: child.Spawned})
		}
	}
	slices.Reverse(pending)
	return pending, nil
}
