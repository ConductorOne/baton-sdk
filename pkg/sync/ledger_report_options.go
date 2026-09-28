package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"encoding/json"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// putLedgerReportOptions records the attempt's options once, before its first
// page, as a lifecycle write. Effective skip flags come from the request as
// well as facts because on a fresh sync Init has not yet turned the request
// into facts.
func (s *syncer) putLedgerReportOptions(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	hasFact := func(fact string) bool { return s.run != nil && s.run.hasFact(fact) }
	cfg := s.cfg
	options := c1zstore.LedgerReportOptions{
		Attempt:                            s.ledger.runID,
		EffectiveLedgerDebug:               s.ledgerDebug,
		EffectiveRetainLedgerTokens:        cfg.retainLedgerTokens || hasFact(c1zstore.LedgerFactRetainTokens),
		EffectiveSkipGrants:                cfg.skipGrants || hasFact(factShouldSkipGrants),
		EffectiveSkipEntitlementsAndGrants: cfg.skipEntitlementsAndGrants || hasFact(factShouldSkipEntitlementsAndGrants),
		Requested: c1zstore.LedgerRequestedOptions{
			SyncType: string(cfg.syncType), ResourceTypes: cfg.syncResourceTypes, WorkerCount: cfg.workerCount, RunDurationMs: cfg.runDuration.Milliseconds(),
			LedgerDebug: cfg.ledgerDebug, RetainLedgerTokens: cfg.retainLedgerTokens,
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
	facts := map[string]string{c1zstore.LedgerFactReportOptions: string(data)}
	if !hasFact(c1zstore.LedgerFactFirstReportOptions) {
		facts[c1zstore.LedgerFactFirstReportOptions] = string(data)
	}
	return s.caps.pageLedger.PutLedgerFacts(ctx, facts)
}
