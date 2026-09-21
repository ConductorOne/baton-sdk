package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"errors"
	"iter"
	"strconv"
	"strings"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/sync/expand"
)

const ledgerFactExpansionComplete = "sync.expansion_complete"

const ledgerExpansionCursorPrefix = "ledger-expansion:"

var errLedgerExpansionStopped = errors.New("ledger expansion stopped")

type ledgerExpansionStream struct {
	next     func() ([]*v2.Grant, error, bool)
	stop     func()
	expected string
	graph    *expand.EntitlementGraph
	drops    *expand.DroppedEdgeStats
}

type ledgerExpansionStore struct {
	expand.ExpanderStore
	yield func([]*v2.Grant, error) bool
}

func (s ledgerExpansionStore) StoreExpandedGrants(_ context.Context, grants ...*v2.Grant) error {
	if !s.yield(grants, nil) {
		return errLedgerExpansionStopped
	}
	return nil
}

func (s *syncer) stopLedgerExpansion() {
	if s.ledgerExpansion != nil {
		s.ledgerExpansion.stop()
		s.ledgerExpansion = nil
	}
}

func (s *syncer) syncLedgerExpansion(ctx context.Context, action *Action) error {
	invocation, ok := ctx.Value(ledgerInvocationKey{}).(*ledgerInvocation)
	if !ok {
		return errors.New("expansion ledger handler requires an open page")
	}
	start := time.Now()
	defer func() { invocation.page.row.PageDuration = time.Since(start) }()
	if s.cfg.dontExpandGrants || !s.run.hasFact(factNeedsExpansion) {
		return s.nextPageOrFinishAction(ctx, action, "")
	}
	sequence := uint64(0)
	if strings.HasPrefix(action.PageToken, ledgerExpansionCursorPrefix) {
		var err error
		sequence, err = strconv.ParseUint(strings.TrimPrefix(action.PageToken, ledgerExpansionCursorPrefix), 10, 64)
		if err != nil || sequence == ^uint64(0) {
			return errors.New("invalid ledger expansion cursor")
		}
	}
	if stream := s.ledgerExpansion; stream != nil && stream.expected != action.PageToken {
		s.stopLedgerExpansion()
	}
	if s.ledgerExpansion == nil {
		graph, drops, err := s.buildLedgerExpansionGraph(ctx)
		if err != nil {
			return err
		}
		next, stop := iter.Pull2(func(yield func([]*v2.Grant, error) bool) {
			expander := expand.NewExpander(ledgerExpansionStore{ExpanderStore: s.expanderStore(), yield: yield}, graph)
			expander.SetDropStats(drops)
			if err := expander.RunTopologicalMergeProjection(ctx); err != nil && !errors.Is(err, errLedgerExpansionStopped) {
				yield(nil, err)
			}
		})
		s.ledgerExpansion = &ledgerExpansionStream{next: next, stop: stop, expected: action.PageToken, graph: graph, drops: drops}
	}
	stream := s.ledgerExpansion
	grants, err, more := stream.next()
	if err != nil {
		return err
	}
	if !more {
		if err := invocation.page.setFact(ledgerFactExpansionComplete); err != nil {
			return err
		}
		invocation.afterCommit = append(invocation.afterCommit, func() {
			s.graph.restore(stream.graph)
			s.expandDropStats = stream.drops
			stream.drops.LogSummary(ctx)
			s.stopLedgerExpansion()
		})
		return s.nextPageOrFinishAction(ctx, action, "")
	}
	if err := invocation.page.writer.StoreExpandedGrants(ctx, grants...); err != nil {
		return err
	}
	next := ledgerExpansionCursorPrefix + strconv.FormatUint(sequence+1, 10)
	invocation.afterCommit = append(invocation.afterCommit, func() { stream.expected = next })
	return s.nextPageOrFinishAction(ctx, action, next)
}
