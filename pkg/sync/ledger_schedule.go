package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"errors"
	"fmt"
	native_sync "sync"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

type ledgerPageHandler func(context.Context, ledgerAction, *ledgerPage) error

type ledgerPageResult struct {
	action ledgerAction
	row    *c1zstore.LedgerRow
	err    error
}

func (r *ledgerRuntime) execute(ctx context.Context, roots []ledgerAction, workers uint32, handler ledgerPageHandler) error {
	if workers == 0 || workers >= c1zstore.TakeoverBucketWorker {
		return errors.New("invalid ledger worker count")
	}
	if handler == nil {
		return errors.New("ledger handler is nil")
	}
	admitted := make(map[c1zstore.LedgerActionIdentity]bool)
	pending, err := r.walkWithSeen(ctx, roots, admitted)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan ledgerAction)
	results := make(chan ledgerPageResult, workers)
	var joined native_sync.WaitGroup
	for worker := range workers {
		joined.Go(func() {
			for action := range jobs {
				row, err := r.runPage(ctx, worker, action.identity, func(ctx context.Context, page *ledgerPage) error {
					page.row.Spawned = action.spawned
					page.row.TypeScopedPlanned = action.typeScopedPlanned
					if err := handler(ctx, action, page); err != nil {
						return err
					}
					if page.row.NextPageToken == action.identity.PageToken && page.row.NextPageToken != "" {
						return errors.New("ledger page returned its own pagination token")
					}
					if page.row.NextPageToken == "" && !action.spawned {
						if page.observations.Counters == nil {
							page.observations.Counters = make(map[string]uint64)
						}
						page.observations.Counters[ledgerCompletedActions]++
						page.observations.Counters[ledgerCompletedPrefix+action.identity.Op]++
					}
					return nil
				})
				results <- ledgerPageResult{action: action, row: row, err: err}
			}
		})
	}
	defer func() { close(jobs); joined.Wait() }()
	active := 0
	for len(pending) != 0 || active != 0 {
		var send chan ledgerAction
		var next ledgerAction
		if len(pending) != 0 && err == nil {
			send = jobs
			next = pending[len(pending)-1]
		}
		select {
		case send <- next:
			pending = pending[:len(pending)-1]
			active++
		case result := <-results:
			active--
			if result.err != nil && err == nil {
				err = result.err
				cancel()
				pending = nil
			}
			if err != nil {
				continue
			}
			var discovered []ledgerAction
			if result.row.NextPageToken != "" {
				next := result.action
				next.identity.PageToken = result.row.NextPageToken
				next.typeScopedPlanned = result.row.TypeScopedPlanned
				if admitted[next.identity] {
					err = fmt.Errorf("ledger pagination revisited an admitted identity for %s", next.identity.Op)
					cancel()
					pending = nil
					continue
				}
				discovered = append(discovered, next)
			}
			for _, child := range result.row.Children {
				if admitted[child.Identity] {
					continue
				}
				discovered = append(discovered, ledgerAction{identity: child.Identity, spawned: child.Spawned})
			}
			var additional []ledgerAction
			additional, err = r.walkWithSeen(ctx, discovered, admitted)
			if err != nil {
				cancel()
				pending = nil
				continue
			}
			pending = append(pending, additional...)
		case <-ctx.Done():
			if err == nil {
				err = ctx.Err()
				pending = nil
			}
			if active != 0 {
				<-results
				active--
			}
		}
	}
	return err
}

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

func ledgerInitialActions() []ledgerAction {
	return []ledgerAction{{identity: c1zstore.LedgerActionIdentity{Op: InitOp.String()}}}
}
