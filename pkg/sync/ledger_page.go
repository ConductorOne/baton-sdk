package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	native_sync "sync"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

var (
	errLedgerPageTransition = errors.New("ledger page must transition exactly once")
	errLedgerWorkerBusy     = errors.New("ledger worker or page is already active")
)

type ledgerRuntime struct {
	localCompleted  map[string]uint64
	store           c1zstore.PageLedgerStore
	runObservations *runStats
	prepareMu       native_sync.Mutex
	beforePage      func(context.Context) error
	prepared        bool
	runID           string
	mu              native_sync.Mutex
	commitMu        native_sync.Mutex
	closing         bool
	facts           map[string]string
	prior           c1zstore.LedgerCounters
	workers         map[uint32]c1zstore.LedgerCounters
	active          map[uint32]bool
}

func newLedgerRuntime(ctx context.Context, store c1zstore.PageLedgerStore, runID string) (*ledgerRuntime, error) {
	if store == nil || runID == "" {
		return nil, errors.New("ledger runtime requires a store and attempt id")
	}
	facts, err := store.LedgerFacts(ctx)
	if err != nil {
		return nil, err
	}
	prior, err := store.LedgerCounters(ctx)
	if err != nil {
		return nil, err
	}
	if facts == nil {
		facts = make(map[string]string)
	}
	return &ledgerRuntime{
		store: store, runID: runID, runObservations: newRunStats(), facts: maps.Clone(facts), prior: cloneLedgerCounters(prior),
		workers: make(map[uint32]c1zstore.LedgerCounters), active: make(map[uint32]bool),
	}, nil
}

type ledgerPage struct {
	writer       c1zstore.PageWriter
	runtime      *ledgerRuntime
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
	if _, found := p.facts[name]; found {
		return true
	}
	p.runtime.mu.Lock()
	defer p.runtime.mu.Unlock()
	_, found := p.runtime.facts[name]
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
	r.mu.Lock()
	if r.closing || r.active[worker] {
		r.mu.Unlock()
		return nil, errLedgerWorkerBusy
	}
	r.active[worker] = true
	previous := cloneLedgerCounters(r.workers[worker])
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		delete(r.active, worker)
		r.mu.Unlock()
	}()
	page := &ledgerPage{
		writer: r.store.BeginPage(), runtime: r,
		row: c1zstore.LedgerRow{Identity: id, Attempt: r.runID}, facts: make(map[string]string),
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
		r.commitMu.Lock()
		defer r.commitMu.Unlock()
		if err := page.writer.Commit(ctx, id, &page.row); err != nil {
			return fmt.Errorf("commit ledger page: %w", err)
		}
		r.mu.Lock()
		r.workers[worker] = candidate
		maps.Copy(r.facts, page.facts)
		r.mu.Unlock()
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
