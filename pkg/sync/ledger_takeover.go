package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/sync/expand"
)

const (
	ledgerFactIngestKnown   = "sync.ingest_known"
	ledgerFactIngestBlocked = "sync.ingest_blocked"
	ledgerCompletedActions  = "actions.completed"
	ledgerCompletedPrefix   = "actions.completed."
	ledgerWarningsPrefix    = "actions.warnings."
)

type ledgerResume struct {
	actions   []ledgerAction
	graph     *expand.EntitlementGraph
	sealReady bool
}

func loadLedgerResume(ctx context.Context, store c1zstore.Store, ledger c1zstore.PageLedgerStore, runID string) (ledgerResume, error) {
	if runID == "" {
		return ledgerResume{}, errors.New("ledger takeover requires an attempt id")
	}
	state, err := store.CurrentSyncStep(ctx)
	if err != nil {
		return ledgerResume{}, fmt.Errorf("read legacy checkpoint: %w", err)
	}
	frontier, found, err := ledger.LedgerFrontier(ctx)
	if err != nil {
		return ledgerResume{}, fmt.Errorf("read ledger frontier: %w", err)
	}
	if state != "" && found {
		return ledgerResume{}, errors.New("legacy checkpoint conflicts with ledger frontier")
	}
	facts, err := ledger.LedgerFacts(ctx)
	if err != nil {
		return ledgerResume{}, err
	}
	if _, ready := facts[ledgerFactSealReady]; ready {
		if state != "" {
			return ledgerResume{}, errors.New("legacy checkpoint conflicts with seal-ready ledger")
		}
		return ledgerResume{sealReady: true}, nil
	}
	if state == "" {
		if found {
			if frontier == nil || frontier.State == "" {
				return ledgerResume{}, errors.New("ledger frontier has no readable state")
			}
			state = frontier.State
		}
		resume, _, _, err := decodeLedgerCheckpoint(state)
		return resume, err
	}
	resume, importedFacts, counters, err := decodeLedgerCheckpoint(state)
	if err != nil {
		return ledgerResume{}, err
	}
	prior, err := ledger.LedgerCounters(ctx)
	if err != nil {
		return ledgerResume{}, err
	}
	if !prior.IsZero() {
		counters = c1zstore.LedgerCounters{}
	}
	moved, err := ledger.TakeoverToken(ctx, runID, importedFacts, counters)
	if err != nil {
		return ledgerResume{}, fmt.Errorf("take over legacy checkpoint: %w", err)
	}
	if moved == "" {
		frontier, found, err := ledger.LedgerFrontier(ctx)
		if err != nil {
			return ledgerResume{}, err
		}
		if !found || frontier == nil || frontier.State == "" {
			return ledgerResume{}, errors.New("takeover returned no state and no frontier")
		}
		resumed, _, _, err := decodeLedgerCheckpoint(frontier.State)
		return resumed, err
	}
	if moved != state {
		return ledgerResume{}, errors.New("legacy checkpoint changed during takeover")
	}
	return resume, nil
}

func decodeLedgerCheckpoint(state string) (ledgerResume, []string, c1zstore.LedgerCounters, error) {
	parts, err := unmarshalToken(state)
	if err != nil {
		return ledgerResume{}, nil, c1zstore.LedgerCounters{}, fmt.Errorf("invalid ledger resume state: %w", err)
	}
	resume := ledgerResume{graph: parts.graph}
	for _, key := range parts.run.actionOrder {
		action, found := parts.run.actions[key]
		if !found || action.Op == UnknownOp {
			return ledgerResume{}, nil, c1zstore.LedgerCounters{}, fmt.Errorf("invalid ledger resume action %q", key)
		}
		resume.actions = append(resume.actions, ledgerAction{
			identity: c1zstore.LedgerActionIdentity{
				Op: action.Op.String(), ResourceTypeID: action.ResourceTypeID, ResourceID: action.ResourceID,
				ParentResourceTypeID: action.ParentResourceTypeID, ParentResourceID: action.ParentResourceID,
				PageToken: action.PageToken, TypeScoped: action.TypeScoped,
			}, spawned: action.Spawned, typeScopedPlanned: action.TypeScopedPlanned,
		})
	}
	facts := make([]string, 0, len(parts.run.facts.established)+2)
	for key, value := range parts.run.facts.established {
		if value {
			facts = append(facts, key)
		}
	}
	counters := c1zstore.LedgerCounters{
		Counters: make(map[string]uint64), ConnectorCalls: make(map[string]c1zstore.CallStat),
		StepDurationsMs: maps.Clone(parts.stats.stepDurationsMs), SessionCalls: make(map[string]c1zstore.CallStat),
	}
	if parts.run.completedActions != 0 {
		counters.Counters[ledgerCompletedActions] = parts.run.completedActions
	}
	for op, count := range parts.run.actionCounts {
		if count.CompletedCount != 0 {
			counters.Counters[ledgerCompletedPrefix+op] = count.CompletedCount
		}
		if count.WarningCount != 0 {
			counters.Counters[ledgerWarningsPrefix+op] = count.WarningCount
		}
	}
	for op, stat := range parts.stats.connectorCalls {
		if stat != nil {
			counters.ConnectorCalls[op] = c1zstore.CallStat{Count: stat.Count, TotalMs: stat.TotalMs, MaxMs: stat.MaxMs}
		}
	}
	for op, stat := range parts.stats.sessionOps {
		if stat != nil {
			counters.SessionCalls[op] = c1zstore.CallStat{Count: stat.Count, TotalMs: stat.TotalMs, MaxMs: stat.MaxMs, Errors: stat.Errors, Timeouts: stat.Timeouts}
		}
	}
	if state != "" {
		quality := parts.stats.ingest
		if quality == nil {
			facts = append(facts, ledgerFactIngestBlocked)
			counters.Flags |= ingestQualityReasonUnknownPriorCheckpoint
		} else {
			facts = append(facts, ledgerFactIngestKnown)
			if quality.SourceCacheReplayBlocked {
				facts = append(facts, ledgerFactIngestBlocked)
			}
			counters = addLedgerCounters(counters, ledgerIngestCounters(quality))
		}
	}
	slices.Sort(facts)
	return resume, facts, counters, nil
}

func ledgerIngestCounters(q *IngestQualityCheckpoint) c1zstore.LedgerCounters {
	counters := map[string]uint64{
		"ingest.entitlements_dropped":             q.EntitlementsDropped,
		"ingest.grants_dropped":                   q.GrantsDropped,
		"ingest.grant_resources_dropped":          q.GrantResourcesDropped,
		"ingest.expansion_resource_types_dropped": q.ExpansionResourceTypesDropped,
		"ingest.expansions_dropped":               q.ExpansionsDropped,
		"ingest.invalid_resource_types_observed":  q.InvalidResourceTypesObserved,
		"ingest.invalid_resources_observed":       q.InvalidResourcesObserved,
		"ingest.invalid_entitlements_observed":    q.InvalidEntitlementsObserved,
	}
	for key, value := range counters {
		if value == 0 {
			delete(counters, key)
		}
	}
	return c1zstore.LedgerCounters{Counters: counters, Flags: q.ReasonFlags}
}

func beginLedgerRuntime(ctx context.Context, store c1zstore.Store, ledger c1zstore.PageLedgerStore, runID string) (*ledgerRuntime, ledgerResume, error) {
	finished, err := ledger.BoundSyncFinished(ctx)
	if err != nil {
		return nil, ledgerResume{}, err
	}
	var resume ledgerResume
	if finished {
		if err := ledger.DropLedger(ctx); err != nil {
			return nil, ledgerResume{}, err
		}
		resume.actions = ledgerInitialActions()
	} else {
		resume, err = loadLedgerResume(ctx, store, ledger, runID)
		if err != nil {
			return nil, ledgerResume{}, err
		}
	}
	runtime, err := newLedgerRuntime(ctx, ledger, runID)
	return runtime, resume, err
}
