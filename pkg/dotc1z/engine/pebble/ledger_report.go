package pebble

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"math"
	"math/bits"

	"github.com/cockroachdb/pebble/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/internal/rawdb"
)

type ledgerReportAttempts struct {
	Pages           uint64 `json:"pages_with_observations"`
	Calls           uint64 `json:"connector_attempts"`
	Errors          uint64 `json:"connector_errors"`
	RetryWaitMs     uint64 `json:"sdk_retry_wait_sum_ms"`
	RateLimitWaitMs uint64 `json:"sdk_rate_limit_wait_sum_ms"`
}

func (a *ledgerReportAttempts) add(other ledgerReportAttempts) {
	a.Pages += other.Pages
	a.Calls += other.Calls
	a.Errors += other.Errors
	a.RetryWaitMs += other.RetryWaitMs
	a.RateLimitWaitMs += other.RateLimitWaitMs
}

type ledgerReportWrites struct {
	ResourceTypes uint64 `json:"resource_types"`
	Resources     uint64 `json:"resources"`
	Entitlements  uint64 `json:"entitlements"`
	Grants        uint64 `json:"grants"`
}

func (w *ledgerReportWrites) add(other ledgerReportWrites) {
	w.ResourceTypes += other.ResourceTypes
	w.Resources += other.Resources
	w.Entitlements += other.Entitlements
	w.Grants += other.Grants
}

type ledgerReportCollection struct {
	Collection                                                   c1zstore.LedgerCollectionStats
	CollectionPages                                              uint64
	Attempts                                                     ledgerReportAttempts
	Writes                                                       ledgerReportWrites
	Scope                                                        c1zstore.LedgerActionIdentity
	Pages, Written, ZeroWritePages, TerminalPages                uint64
	PageMs, ConnectorMs, ReportedWaitMs, MaxConnectorMs          uint64
	Continuations, Children, PaginationUnknownPages, Collections uint64
	ConnectorPageMedian, ConnectorPageP95                        ledgerReportInterval
	WrittenPerPage                                               float64
	PagesPerThousandWrites, ConnectorMsPerThousandWrites         *float64
}

type ledgerReportSummary struct {
	PhaseElapsedMs                                        map[string]uint64
	References                                            *ledgerReferenceStats
	Options                                               *ledgerReportOptionSummary
	OptionSnapshots                                       uint64
	Collection                                            c1zstore.LedgerCollectionStats
	CollectionPages                                       uint64
	Attempts                                              ledgerReportAttempts
	Writes                                                ledgerReportWrites
	GrantsDisabled, EntitlementsDisabled                  *bool
	Written, ConnectorMs, ReportedWaitMs                  uint64
	Pages, Collections, LedgerKeysScanned, OperationTypes uint64
	Continuations, Children, PaginationUnknownPages       uint64
	TopOperationTypes                                     []ledgerReportCollection
	Top                                                   []ledgerReportCollection
}

func ledgerReport(ctx context.Context, e *Engine, emit func(ledgerReportCollection)) (ledgerReportSummary, error) {
	lo, hi := rawdb.LedgerBounds()
	iter, err := e.db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return ledgerReportSummary{}, err
	}
	summary, scanErr := ledgerReportScan(ctx, iter, emit, nil)
	return summary, errors.Join(scanErr, iter.Close())
}

type ledgerReportInterval struct {
	LowerMs, UpperMs uint64
	Available        bool
}

type ledgerReportHistogram struct {
	buckets [65]uint64
	count   uint64
}

func (h *ledgerReportHistogram) add(ms uint64) {
	h.buckets[bits.Len64(ms)]++
	h.count++
}

func (h ledgerReportHistogram) quantile(percent uint64) ledgerReportInterval {
	if h.count == 0 {
		return ledgerReportInterval{}
	}
	rank := (h.count/100)*percent + (h.count%100*percent+99)/100
	var seen uint64
	for bucket, count := range h.buckets {
		seen += count
		if seen < rank {
			continue
		}
		out := ledgerReportInterval{Available: true}
		if bucket > 0 {
			out.LowerMs = uint64(1) << (bucket - 1)
			out.UpperMs = math.MaxUint64
			if bucket < 64 {
				out.UpperMs = (uint64(1) << bucket) - 1
			}
		}
		return out
	}
	panic("invalid report histogram")
}

func ledgerReportRankBefore(a, b ledgerReportCollection) bool {
	if a.ConnectorMs != b.ConnectorMs {
		return a.ConnectorMs > b.ConnectorMs
	}
	x, y := a.Scope, b.Scope
	order := cmp.Or(cmp.Compare(x.Op, y.Op), cmp.Compare(x.ResourceTypeID, y.ResourceTypeID),
		cmp.Compare(x.ResourceID, y.ResourceID), cmp.Compare(x.ParentResourceTypeID, y.ParentResourceTypeID),
		cmp.Compare(x.ParentResourceID, y.ParentResourceID))
	if order != 0 {
		return order < 0
	}
	return !x.TypeScoped && y.TypeScoped
}

func ledgerReportShare(part, total uint64) *float64 {
	if total == 0 {
		return nil
	}
	share := 100 * float64(part) / float64(total)
	return &share
}

func renderLedgerReport(report ledgerReportSummary) ([]byte, error) {
	interval := func(v ledgerReportInterval) any {
		if !v.Available {
			return nil
		}
		return map[string]uint64{"lower_ms": v.LowerMs, "upper_ms": v.UpperMs}
	}
	convert := func(collections []ledgerReportCollection) []map[string]any {
		top := make([]map[string]any, 0, len(collections))
		for _, c := range collections {
			top = append(top, map[string]any{
				"scope": map[string]any{
					"operation": c.Scope.Op, "resource_type_id": c.Scope.ResourceTypeID, "resource_id": c.Scope.ResourceID,
					"parent_resource_type_id": c.Scope.ParentResourceTypeID, "parent_resource_id": c.Scope.ParentResourceID, "type_scoped": c.Scope.TypeScoped,
				},
				"pages": c.Pages, "record_writes": c.Written, "record_writes_by_family": c.Writes,
				"attempt_observations":    c.Attempts,
				"collection_observations": c.Collection, "pages_with_collection_observations": c.CollectionPages, "zero_write_pages": c.ZeroWritePages, "terminal_pages": c.TerminalPages,
				"recorded_continuations": c.Continuations, "recorded_children": c.Children, "pagination_unknown_pages": c.PaginationUnknownPages, "collections": c.Collections,
				"page_duration_sum_ms": c.PageMs, "connector_duration_sum_ms": c.ConnectorMs, "connector_duration_share_pct": ledgerReportShare(c.ConnectorMs, report.ConnectorMs),
				"reported_rate_limit_wait_sum_ms": c.ReportedWaitMs, "connector_page_duration_max_ms": c.MaxConnectorMs,
				"connector_page_duration_p50": interval(c.ConnectorPageMedian), "connector_page_duration_p95": interval(c.ConnectorPageP95),
				"record_writes_per_page": c.WrittenPerPage, "pages_per_1000_record_writes": c.PagesPerThousandWrites,
				"connector_ms_per_1000_record_writes": c.ConnectorMsPerThousandWrites,
			})
		}
		return top
	}
	return json.Marshal(map[string]any{
		"recorded_sync_phase_elapsed_ms": report.PhaseElapsedMs,
		"latest_attempt_options":         report.Options, "option_snapshots": report.OptionSnapshots,
		"schema_version": 2, "grants_disabled": report.GrantsDisabled, "entitlements_disabled": report.EntitlementsDisabled,
		"pages": report.Pages, "collections": report.Collections, "record_writes": report.Written, "record_writes_by_family": report.Writes,
		"attempt_observations":    report.Attempts,
		"collection_observations": report.Collection, "pages_with_collection_observations": report.CollectionPages, "ledger_keys_scanned": report.LedgerKeysScanned,
		"reference_validation_performed": report.References != nil, "reference_checks": report.References,
		"recorded_continuations": report.Continuations, "recorded_children": report.Children, "pagination_unknown_pages": report.PaginationUnknownPages,
		"connector_duration_sum_ms": report.ConnectorMs, "reported_rate_limit_wait_sum_ms": report.ReportedWaitMs,
		"top_collections_by_connector_ms": convert(report.Top),
		"operation_type_groups":           report.OperationTypes, "top_operation_types_by_connector_ms": convert(report.TopOperationTypes),
		"collections_omitted_from_top":           report.Collections - min(report.Collections, uint64(len(report.Top))),
		"operation_type_groups_omitted_from_top": report.OperationTypes - min(report.OperationTypes, uint64(len(report.TopOperationTypes))),
	})
}

func addReportCollection(dst *c1zstore.LedgerCollectionStats, src c1zstore.LedgerCollectionStats) {
	dst.ListResponses += src.ListResponses
	dst.EmptyListResponses += src.EmptyListResponses
	dst.EmptyListResponsesWithContinuation += src.EmptyListResponsesWithContinuation
	dst.ResourceTypesReceived += src.ResourceTypesReceived
	dst.ResourcesReceived += src.ResourcesReceived
	dst.EntitlementsReceived += src.EntitlementsReceived
	dst.GrantsReceived += src.GrantsReceived
	dst.ResourceTypesExcludedBySelection += src.ResourceTypesExcludedBySelection
	dst.EntitlementsExcludedByType += src.EntitlementsExcludedByType
	dst.GrantsExcludedByType += src.GrantsExcludedByType
	dst.DerivedResourcesExcludedByType += src.DerivedResourcesExcludedByType
	dst.ResourceTypesExcludedInvalid += src.ResourceTypesExcludedInvalid
	dst.ResourcesExcludedInvalid += src.ResourcesExcludedInvalid
	dst.EntitlementsExcludedInvalid += src.EntitlementsExcludedInvalid
}

func (e *Engine) GenerateLedgerReport(ctx context.Context) ([]byte, error) {
	report, err := ledgerReport(ctx, e, nil)
	if err != nil {
		return nil, err
	}
	if report.Options != nil && (report.Options.EffectiveLedgerDebug || report.Options.Requested.LedgerDebug) {
		report.References, err = e.validateLedgerReferences(ctx)
		if err != nil {
			return nil, err
		}
	}
	return renderLedgerReport(report)
}
