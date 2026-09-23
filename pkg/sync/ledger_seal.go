package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"errors"
	"maps"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

const ledgerTerminalOp = "sync-terminal-v1"
const ledgerFactSealReady = "sync.seal_ready"

func (r *ledgerRuntime) prepareSeal(ctx context.Context, runCounters c1zstore.LedgerCounters, facts ...string) error {
	return r.prepareSealWithOptions(ctx, runCounters, nil, facts...)
}

func (s *syncer) prepareLedgerSeal(ctx context.Context, counters c1zstore.LedgerCounters, facts ...string) error {
	return s.ledger.prepareSealWithOptions(ctx, counters, func(page *ledgerPage) error { return s.stageLedgerReportOptions(&ledgerInvocation{page: page}) }, facts...)
}

func (r *ledgerRuntime) prepareSealWithOptions(ctx context.Context, runCounters c1zstore.LedgerCounters, options func(*ledgerPage) error, facts ...string) error {
	pending, initialized, err := r.store.PendingWork(ctx, 0, 1)
	if err != nil {
		return err
	}
	if len(pending) > 0 {
		return errors.New("cannot prepare seal with pending work")
	}
	if !initialized {
		r.mu.Lock()
		_, discarding := r.facts[c1zstore.LedgerFactDiscardOnSeal]
		_, ready := r.facts[ledgerFactSealReady]
		r.mu.Unlock()
		if !discarding || !ready {
			return errors.New("cannot prepare seal without pending-work declaration")
		}
	}

	r.mu.Lock()
	if len(r.active) != 0 {
		r.mu.Unlock()
		return errors.New("cannot prepare ledger seal with active pages")
	}
	r.closing = true
	_, ready := r.facts[ledgerFactSealReady]
	r.mu.Unlock()
	if ready {
		return nil
	}
	page := r.store.BeginPage()
	defer page.Discard()
	if options != nil {
		if err := options(&ledgerPage{writer: page, runtime: r, facts: make(map[string]string)}); err != nil {
			return err
		}
	}
	for _, fact := range facts {
		if err := page.SetFact(fact); err != nil {
			return err
		}
	}
	if err := page.SetFact(ledgerFactSealReady); err != nil {
		return err
	}
	if err := page.SetCounterBucket(r.runID, c1zstore.RunBucketWorker, runCounters); err != nil {
		return err
	}
	id := c1zstore.LedgerActionIdentity{Op: ledgerTerminalOp}
	if err := page.Commit(c1zstore.WithOpenPage(ctx), id, &c1zstore.LedgerRow{Identity: id, Attempt: r.runID}); err != nil {
		return err
	}
	r.mu.Lock()
	r.facts[ledgerFactSealReady] = ""
	r.mu.Unlock()
	return nil
}

func (r *ledgerRuntime) seal(ctx context.Context) error {
	facts, err := r.store.LedgerFacts(ctx)
	if err != nil {
		return err
	}
	if _, ready := facts[ledgerFactSealReady]; !ready {
		return errors.New("ledger seal requires terminal page")
	}
	counters, err := r.store.LedgerCounters(ctx)
	if err != nil {
		return err
	}
	return r.store.EndSyncWithStats(ctx, ledgerSyncStats(facts, counters))
}

func ledgerSyncStats(facts map[string]string, counters c1zstore.LedgerCounters) c1zstore.SyncStats {
	_, blocked := facts[ledgerFactIngestBlocked]
	stats := c1zstore.SyncStats{
		Run: c1zstore.RunStats{
			CompletedActions:   counters.Counters[ledgerCompletedActions],
			StepDurationsMs:    maps.Clone(counters.StepDurationsMs),
			ConnectorCallStats: maps.Clone(counters.ConnectorCalls),
			SessionStoreStats:  maps.Clone(counters.SessionCalls),
		},
		IngestQuality: &c1zstore.IngestQuality{
			SourceCacheReplayBlocked: blocked, ReasonFlags: counters.Flags,
			EntitlementsDropped:           counters.Counters["ingest.entitlements_dropped"],
			GrantsDropped:                 counters.Counters["ingest.grants_dropped"],
			GrantResourcesDropped:         counters.Counters["ingest.grant_resources_dropped"],
			ExpansionResourceTypesDropped: counters.Counters["ingest.expansion_resource_types_dropped"],
			ExpansionsDropped:             counters.Counters["ingest.expansions_dropped"],
			InvalidResourceTypesObserved:  counters.Counters["ingest.invalid_resource_types_observed"],
			InvalidResourcesObserved:      counters.Counters["ingest.invalid_resources_observed"],
			InvalidEntitlementsObserved:   counters.Counters["ingest.invalid_entitlements_observed"],
		},
	}
	if _, known := facts[ledgerFactIngestKnown]; !known && !blocked {
		stats.IngestQuality = nil
	}
	return stats
}
