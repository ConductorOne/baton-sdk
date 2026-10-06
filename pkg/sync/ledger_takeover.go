package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

const (
	ledgerFactIngestKnown   = c1zstore.LedgerFactIngestKnown
	ledgerFactIngestBlocked = c1zstore.LedgerFactIngestBlocked
	ledgerCompletedActions  = c1zstore.LedgerCompletedActions
	ledgerCompletedPrefix   = "actions.completed."
	ledgerWarningsPrefix    = "actions.warnings."
)

type ledgerAction struct {
	identity          c1zstore.LedgerActionIdentity
	spawned           bool
	typeScopedPlanned bool
}

type ledgerResume struct {
	phase          c1zstore.LedgerQueuePhase
	hasPendingWork bool
	actions        []ledgerAction
}

// firstOptions is the attempt's encoded options, recorded as the pass's when
// the token's stack already carries collection work: the takeover is the
// commit that adopts that plan, as the Init page is for a plan of its own.
func loadLedgerResume(ctx context.Context, store c1zstore.Store, ledger c1zstore.PageLedgerStore, runID, firstOptions string) (ledgerResume, error) {
	if runID == "" {
		return ledgerResume{}, errors.New("ledger takeover requires an attempt id")
	}
	pending, phase, err := ledger.PendingWork(ctx, 0, 1)
	if err != nil {
		return ledgerResume{}, err
	}
	state, err := store.CurrentSyncStep(ctx)
	if err != nil {
		return ledgerResume{}, fmt.Errorf("read legacy checkpoint: %w", err)
	}
	if phase != c1zstore.LedgerQueueAbsent {
		if state != "" {
			return ledgerResume{}, errors.New("legacy checkpoint conflicts with pending work")
		}
		if phase == c1zstore.LedgerQueueSealing && len(pending) != 0 {
			return ledgerResume{}, errors.New("sealing declaration has pending work")
		}
		return ledgerResume{phase: phase, hasPendingWork: len(pending) != 0}, nil
	}
	frontier, found, err := ledger.LedgerFrontier(ctx)
	if err != nil {
		return ledgerResume{}, err
	}
	if state != "" && found {
		return ledgerResume{}, errors.New("legacy checkpoint conflicts with ledger frontier")
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
	if len(resume.actions) == 0 {
		resume.actions = []ledgerAction{{identity: c1zstore.LedgerActionIdentity{Op: InitOp.String()}}}
	}
	prior, err := ledger.LedgerCounters(ctx)
	if err != nil {
		return ledgerResume{}, err
	}
	if !prior.IsZero() {
		counters = c1zstore.LedgerCounters{}
	}
	facts := make(map[string]string, len(importedFacts)+1)
	for _, fact := range importedFacts {
		facts[fact] = ""
	}
	if firstOptions != "" && legacyStackHasCollection(resume.actions) {
		facts[c1zstore.LedgerFactFirstReportOptions] = firstOptions
	}
	// Collecting even when the stack is exactly the expansion step: the
	// token cannot say whether expansion ran (baseline SDKs clear its
	// cursor and keep no graph), and the baseline resumer treats that
	// stack as a step still to take, so dont-expand-grants skips it and
	// seals. Taking it over as Expanding would refuse that resume.
	moved, err := ledger.BeginFromToken(ctx, runID, state, facts, counters, pendingSeeds(resume.actions), c1zstore.LedgerQueueCollecting)
	if err != nil {
		return ledgerResume{}, fmt.Errorf("take over legacy checkpoint: %w", err)
	}
	if moved != "" && moved != state {
		return ledgerResume{}, errors.New("legacy checkpoint changed during takeover")
	}
	pending, phase, err = ledger.PendingWork(ctx, 0, 1)
	if err != nil {
		return ledgerResume{}, err
	}
	if phase == c1zstore.LedgerQueueAbsent {
		return ledgerResume{}, errors.New("takeover returned without pending work state")
	}
	return ledgerResume{phase: phase, hasPendingWork: len(pending) != 0}, nil
}

// The same reading as flagConflict's collectionQueued: the Init seed, the
// expansion step and the external import are not collection.
func legacyStackHasCollection(actions []ledgerAction) bool {
	for _, action := range actions {
		switch action.identity.Op {
		case InitOp.String(), SyncGrantExpansionOp.String(), SyncExternalResourcesOp.String():
		default:
			return true
		}
	}
	return false
}

func decodeLedgerCheckpoint(state string) (ledgerResume, []string, c1zstore.LedgerCounters, error) {
	parts, err := unmarshalToken(state)
	if err != nil {
		return ledgerResume{}, nil, c1zstore.LedgerCounters{}, fmt.Errorf("invalid ledger resume state: %w", err)
	}
	resume := ledgerResume{}
	for _, key := range parts.run.actionOrder {
		action, found := parts.run.actions[key]
		if !found || action.Op == UnknownOp || action.Op == MaterializeStaticEntitlementsOp {
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
