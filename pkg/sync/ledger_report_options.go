package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"encoding/json"
	"slices"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// putLedgerReportOptions records the attempt's options once, before its first
// page, as a lifecycle write. The pass's first options are the Init page's
// (recordFirstReportOptions). Effective skip flags come from the request as
// well as facts because on a fresh sync Init has not yet turned the request
// into facts.
func (s *syncer) putLedgerReportOptions(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	data, err := s.encodeLedgerReportOptions(s.ledger.runID)
	if err != nil {
		return err
	}
	return s.caps.pageLedger.PutLedgerFacts(ctx, map[string]string{c1zstore.LedgerFactReportOptions: data})
}

// recordFirstReportOptions stages the pass's options on the Init page, so the
// record and the plan it describes commit together: an attempt that dies
// before Init leaves no record for the next attempt to be compared against. A
// pass that already has them (an expansion pass over a finished collection)
// keeps the collection's.
func (s *syncer) recordFirstReportOptions(page *ledgerPage) error {
	if (s.run != nil && s.run.hasFact(c1zstore.LedgerFactFirstReportOptions)) || page.hasFact(c1zstore.LedgerFactFirstReportOptions) {
		return nil
	}
	data, err := s.encodeLedgerReportOptions(s.ledger.runID)
	if err != nil {
		return err
	}
	return page.setFactValue(c1zstore.LedgerFactFirstReportOptions, data)
}

func (s *syncer) encodeLedgerReportOptions(attempt string) (string, error) {
	hasFact := func(fact string) bool { return s.run != nil && s.run.hasFact(fact) }
	cfg := s.cfg
	options := c1zstore.LedgerReportOptions{
		Attempt:                            attempt,
		EffectiveLedgerDebug:               s.ledgerDebug,
		EffectiveRetainLedgerTokens:        cfg.retainLedgerTokens || hasFact(c1zstore.LedgerFactRetainTokens),
		EffectiveSkipGrants:                cfg.skipGrants || hasFact(factShouldSkipGrants),
		EffectiveSkipEntitlementsAndGrants: cfg.skipEntitlementsAndGrants || hasFact(factShouldSkipEntitlementsAndGrants),
		Requested:                          s.requestedOptions(),
	}
	data, err := json.Marshal(options)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (s *syncer) requestedOptions() c1zstore.LedgerRequestedOptions {
	cfg := s.cfg
	requested := c1zstore.LedgerRequestedOptions{
		SyncType: string(cfg.syncType), ResourceTypes: cfg.syncResourceTypes, WorkerCount: cfg.workerCount, RunDurationMs: cfg.runDuration.Milliseconds(),
		LedgerDebug: cfg.ledgerDebug, RetainLedgerTokens: cfg.retainLedgerTokens,
		SkipFullSync: cfg.skipFullSync, SkipGrants: cfg.skipGrants, SkipEntitlementsAndGrants: cfg.skipEntitlementsAndGrants,
		OnlyExpandGrants: cfg.onlyExpandGrants, DontExpandGrants: cfg.dontExpandGrants, PreserveEntitlementGraph: cfg.preserveEntitlementGraph,
		FailFastInvariants: cfg.failFastInvariants, ExternalSourceConfigured: s.externalResourceReader != nil,
		ExternalEntitlementIDFilter: cfg.externalResourceEntitlementIdFilter,
		PreviousSourceConfigured:    cfg.previousSyncC1ZPath != "", PreviousSourceOptional: cfg.previousSyncC1ZPathOptional,
	}
	for _, target := range cfg.targetedSyncResources {
		requested.Targets = append(requested.Targets, c1zstore.LedgerReportTarget{
			ResourceTypeID: target.GetId().GetResourceType(), ResourceID: target.GetId().GetResource(),
			ParentResourceTypeID: target.GetParentResourceId().GetResourceType(), ParentResourceID: target.GetParentResourceId().GetResource(),
		})
	}
	for _, trait := range cfg.externalResourceTraits {
		requested.ExternalResourceTraits = append(requested.ExternalResourceTraits, trait.String())
	}
	return requested
}

// The collection flags: what a collection pass fetches. Locked from the
// pass's first page; an expansion pass does not read them.
func collectionFlagDifferences(want, got c1zstore.LedgerRequestedOptions) []string {
	var diffs []string
	if want.SyncType != got.SyncType {
		diffs = append(diffs, "sync_type")
	}
	if want.SkipEntitlementsAndGrants != got.SkipEntitlementsAndGrants {
		diffs = append(diffs, "skip_entitlements_and_grants")
	}
	if want.SkipGrants != got.SkipGrants {
		diffs = append(diffs, "skip_grants")
	}
	if !sameStringSet(want.ResourceTypes, got.ResourceTypes) {
		diffs = append(diffs, "resource_types")
	}
	if !sameTargetSet(want.Targets, got.Targets) {
		diffs = append(diffs, "targets")
	}
	if want.ExternalSourceConfigured != got.ExternalSourceConfigured {
		diffs = append(diffs, "external_source_configured")
	}
	if !sameStringSet(want.ExternalResourceTraits, got.ExternalResourceTraits) {
		diffs = append(diffs, "external_resource_traits")
	}
	if want.ExternalEntitlementIDFilter != got.ExternalEntitlementIDFilter {
		diffs = append(diffs, "external_entitlement_id_filter")
	}
	return diffs
}

func sameStringSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	a, b = slices.Clone(a), slices.Clone(b)
	slices.Sort(a)
	slices.Sort(b)
	return slices.Equal(a, b)
}

func sameTargetSet(a, b []c1zstore.LedgerReportTarget) bool {
	if len(a) != len(b) {
		return false
	}
	seen := make(map[c1zstore.LedgerReportTarget]int, len(a))
	for _, target := range a {
		seen[target]++
	}
	for _, target := range b {
		if seen[target] == 0 {
			return false
		}
		seen[target]--
	}
	return true
}
