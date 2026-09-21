package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"encoding/json"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

func (s *syncer) stageLedgerReportOptions(invocation *ledgerInvocation) error {
	page := invocation.page
	key := c1zstore.LedgerFactReportOptionsPrefix + s.ledger.runID
	if page.hasFact(key) {
		return nil
	}
	hasFact := func(fact string) bool {
		return page.hasFact(fact) || s.run != nil && s.run.hasFact(fact)
	}
	cfg := s.cfg
	options := c1zstore.LedgerReportOptions{
		Attempt:                            s.ledger.runID,
		EffectiveSkipGrants:                hasFact(factShouldSkipGrants),
		EffectiveSkipEntitlementsAndGrants: hasFact(factShouldSkipEntitlementsAndGrants),
		Requested: c1zstore.LedgerRequestedOptions{
			SyncType: string(cfg.syncType), ResourceTypes: cfg.syncResourceTypes, WorkerCount: cfg.workerCount, RunDurationMs: cfg.runDuration.Milliseconds(),
			LedgerDebug: s.ledgerDebug, RetainLedgerTokens: cfg.retainLedgerTokens,
			SkipFullSync: cfg.skipFullSync, SkipGrants: cfg.skipGrants, SkipEntitlementsAndGrants: cfg.skipEntitlementsAndGrants,
			OnlyExpandGrants: cfg.onlyExpandGrants, DontExpandGrants: cfg.dontExpandGrants, PreserveEntitlementGraph: cfg.preserveEntitlementGraph,
			FailFastInvariants: cfg.failFastInvariants, ExternalSourceConfigured: s.externalResourceReader != nil,
			ExternalEntitlementIDFilter: cfg.externalResourceEntitlementIdFilter,
			PreviousSourceConfigured:    cfg.previousSyncC1ZPath != "", PreviousSourceOptional: cfg.previousSyncC1ZPathOptional,
		},
	}
	for _, target := range cfg.targetedSyncResources {
		options.Requested.Targets = append(options.Requested.Targets, c1zstore.LedgerReportTarget{
			ResourceTypeID: target.GetId().GetResourceType(), ResourceID: target.GetId().GetResource(),
			ParentResourceTypeID: target.GetParentResourceId().GetResourceType(), ParentResourceID: target.GetParentResourceId().GetResource(),
		})
	}
	for _, trait := range cfg.externalResourceTraits {
		options.Requested.ExternalResourceTraits = append(options.Requested.ExternalResourceTraits, trait.String())
	}
	data, err := json.Marshal(options)
	if err != nil {
		return err
	}
	if err := page.setFactValue(key, string(data)); err != nil {
		return err
	}
	return page.setFactValue(c1zstore.LedgerFactReportOptions, string(data))
}
