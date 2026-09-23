package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"errors"
	"maps"
	"strings"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

func (s *syncer) restoreLedgerState(ctx context.Context, resume ledgerResume, knownEmpty bool) error {
	if s.testHooks.ledgerWalk != nil {
		s.testHooks.ledgerWalk(true)
		defer s.testHooks.ledgerWalk(false)
	}
	if s.ledger == nil {
		return errors.New("ledger runtime is not initialized")
	}
	facts, err := s.ledger.store.LedgerFacts(ctx)
	if err != nil {
		return err
	}
	counters, err := s.ledger.store.LedgerCounters(ctx)
	if err != nil {
		return err
	}
	work, initialized, err := s.ledger.store.PendingWork(ctx, 0, maxPeekActionsCount)
	if err != nil {
		return err
	}
	if !initialized {
		_, ready := facts[ledgerFactSealReady]
		_, discarding := facts[c1zstore.LedgerFactDiscardOnSeal]
		if !ready || !discarding {
			return errors.New("pending work is not initialized")
		}
	}
	run := newRunState()
	for i := len(work) - 1; i >= 0; i-- {
		action, err := s.actionFromPending(work[i])
		if err != nil {
			return err
		}
		run.actions[action.ID] = action
		run.actionOrder = append(run.actionOrder, action.ID)
		if action.Spawned {
			run.spawnedInFlight[action.ID] = action
		}
	}

	for fact := range facts {
		run.setFact(fact)
	}
	run.completedActions = counters.Counters[ledgerCompletedActions]
	for key, value := range counters.Counters {
		if op, ok := strings.CutPrefix(key, ledgerCompletedPrefix); ok {
			count := run.actionCounts[op]
			count.CompletedCount = value
			run.actionCounts[op] = count
		}
		if op, ok := strings.CutPrefix(key, ledgerWarningsPrefix); ok {
			count := run.actionCounts[op]
			count.WarningCount = value
			run.actionCounts[op] = count
		}
	}
	stats := newRunStats()
	stats.stepDurationsMs = maps.Clone(counters.StepDurationsMs)
	if stats.stepDurationsMs == nil {
		stats.stepDurationsMs = make(map[string]int64)
	}
	for method, stat := range counters.ConnectorCalls {
		stats.connectorCalls[method] = &ConnectorCallStat{Count: stat.Count, TotalMs: stat.TotalMs, MaxMs: stat.MaxMs}
	}
	for method, stat := range counters.SessionCalls {
		stats.sessionOps[method] = &SessionStoreStat{Count: stat.Count, TotalMs: stat.TotalMs, MaxMs: stat.MaxMs, Errors: stat.Errors, Timeouts: stat.Timeouts}
	}
	if quality := ledgerSyncStats(facts, counters).IngestQuality; quality != nil {
		converted := IngestQualityCheckpoint(*quality)
		stats.setIngestQuality(&converted)
	}
	quality := stats.ingestQuality()
	if knownEmpty {
		quality = &IngestQualityCheckpoint{}
		stats.setIngestQuality(quality)
	} else if quality == nil {
		quality = &IngestQualityCheckpoint{SourceCacheReplayBlocked: true, ReasonFlags: ingestQualityReasonUnknownPriorCheckpoint}
	}
	graph := newExpansionGraph()
	graph.restore(resume.graph)
	s.run = run
	s.stats = stats
	s.graph = graph
	s.ingestFilterStats.restore(quality)
	s.listResourceActionsCompletedThisRun.Store(0)
	return nil
}
