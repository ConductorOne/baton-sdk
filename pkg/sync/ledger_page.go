package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

var (
	errLedgerPageTransition = errors.New("ledger page must transition exactly once")
	errLedgerWorkerIndex    = errors.New("ledger worker index outside the attempt's slots")
)

// ledgerRuntime is one attempt's state. workers[i] is the cumulative counter
// bucket the store blind-writes for (runID, i); it is touched only by the
// goroutine currently holding worker index i — a syncParallel worker for the
// life of its batch, the coordinator for index 0 while no workers are alive —
// and it keeps its total across batches because indexes restart at 0 in each
// one. No mutex: batches do not overlap, so no two goroutines hold one index.
type ledgerRuntime struct {
	store   c1zstore.PageLedgerStore
	runID   string
	workers []c1zstore.LedgerCounters
}

func newLedgerRuntime(store c1zstore.PageLedgerStore, runID string, workerCount int) (*ledgerRuntime, error) {
	if store == nil || runID == "" {
		return nil, errors.New("ledger runtime requires a store and attempt id")
	}
	if workerCount < 1 {
		return nil, errors.New("ledger runtime requires at least one worker slot")
	}
	return &ledgerRuntime{store: store, runID: runID, workers: make([]c1zstore.LedgerCounters, workerCount)}, nil
}

type ledgerPage struct {
	writer       c1zstore.PageWriter
	row          c1zstore.LedgerRow
	transitions  int
	facts        map[string]string
	observations c1zstore.LedgerCounters
}

func (p *ledgerPage) transition(next string, children ...c1zstore.LedgerChild) error {
	p.transitions++
	if p.transitions != 1 {
		return errLedgerPageTransition
	}
	p.row.NextPageToken = next
	p.row.Children = slices.Clone(children)
	return nil
}

func (p *ledgerPage) setFact(name string) error {
	if err := p.writer.SetFact(name); err != nil {
		return err
	}
	p.facts[name] = ""
	return nil
}

func (p *ledgerPage) setFactValue(name, value string) error {
	if err := p.writer.SetFactValue(name, value); err != nil {
		return err
	}
	p.facts[name] = value
	return nil
}

func (p *ledgerPage) hasFact(name string) bool {
	_, found := p.facts[name]
	return found
}

func (r *ledgerRuntime) runPage(
	ctx context.Context,
	worker uint32,
	id c1zstore.LedgerActionIdentity,
	handler func(context.Context, *ledgerPage) error,
) (*c1zstore.LedgerRow, error) {
	return r.runPageWithCommit(ctx, worker, id, handler, nil)
}

func (r *ledgerRuntime) runPageWithCommit(
	ctx context.Context,
	worker uint32,
	id c1zstore.LedgerActionIdentity,
	handler func(context.Context, *ledgerPage) error,
	transition func(*ledgerPage, func() error) error,
) (*c1zstore.LedgerRow, error) {
	if worker >= c1zstore.TakeoverBucketWorker {
		return nil, errors.New("reserved ledger worker index")
	}
	if handler == nil {
		return nil, errors.New("ledger page handler is nil")
	}
	if int(worker) >= len(r.workers) {
		return nil, fmt.Errorf("%w: %d of %d", errLedgerWorkerIndex, worker, len(r.workers))
	}
	previous := cloneLedgerCounters(r.workers[worker])
	page := &ledgerPage{
		writer: r.store.BeginPage(),
		row:    c1zstore.LedgerRow{Identity: id, Attempt: r.runID}, facts: make(map[string]string),
	}
	defer page.writer.Discard()
	ctx = c1zstore.WithOpenPage(ctx)
	if err := handler(ctx, page); err != nil {
		return nil, err
	}
	if page.transitions != 1 {
		return nil, errLedgerPageTransition
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	candidate := addLedgerCounters(previous, page.observations)
	if err := page.writer.SetCounterBucket(r.runID, worker, candidate); err != nil {
		return nil, err
	}
	committed := false
	publish := func() error {
		if committed {
			return errors.New("ledger page committed twice")
		}
		if err := page.writer.Commit(ctx, id, &page.row); err != nil {
			return fmt.Errorf("commit ledger page: %w", err)
		}
		r.workers[worker] = candidate
		committed = true
		return nil
	}
	var err error
	if transition == nil {
		err = publish()
	} else {
		err = transition(page, publish)
	}
	if err != nil {
		return nil, err
	}
	if !committed {
		return nil, errors.New("ledger transition did not commit its page")
	}
	row := page.row
	row.Children = slices.Clone(row.Children)
	return &row, nil
}

func cloneLedgerCounters(c c1zstore.LedgerCounters) c1zstore.LedgerCounters {
	return c1zstore.LedgerCounters{
		Counters: maps.Clone(c.Counters), Flags: c.Flags, ConnectorCalls: maps.Clone(c.ConnectorCalls),
		StepDurationsMs: maps.Clone(c.StepDurationsMs), SessionCalls: maps.Clone(c.SessionCalls),
	}
}

func addLedgerCounters(a, b c1zstore.LedgerCounters) c1zstore.LedgerCounters {
	out := cloneLedgerCounters(a)
	if out.Counters == nil {
		out.Counters = make(map[string]uint64)
	}
	if out.ConnectorCalls == nil {
		out.ConnectorCalls = make(map[string]c1zstore.CallStat)
	}
	if out.StepDurationsMs == nil {
		out.StepDurationsMs = make(map[string]int64)
	}
	if out.SessionCalls == nil {
		out.SessionCalls = make(map[string]c1zstore.CallStat)
	}
	for key, value := range b.Counters {
		out.Counters[key] += value
	}
	out.Flags |= b.Flags
	for key, value := range b.StepDurationsMs {
		out.StepDurationsMs[key] += value
	}
	for key, value := range b.ConnectorCalls {
		previous := out.ConnectorCalls[key]
		previous.Add(value)
		out.ConnectorCalls[key] = previous
	}
	for key, value := range b.SessionCalls {
		previous := out.SessionCalls[key]
		previous.Add(value)
		out.SessionCalls[key] = previous
	}
	return out
}
