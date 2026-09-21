package pebble

import (
	"cmp"
	"encoding/json"
	"math"
	"math/bits"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

type reportPrototypeInterval struct {
	LowerMs, UpperMs uint64
	Available        bool
}

type reportPrototypeHistogram struct {
	buckets [65]uint64
	count   uint64
}

func (h *reportPrototypeHistogram) add(ms uint64) {
	h.buckets[bits.Len64(ms)]++
	h.count++
}

func (h reportPrototypeHistogram) quantile(percent uint64) reportPrototypeInterval {
	if h.count == 0 {
		return reportPrototypeInterval{}
	}
	rank := (h.count/100)*percent + (h.count%100*percent+99)/100
	var seen uint64
	for bucket, count := range h.buckets {
		seen += count
		if seen < rank {
			continue
		}
		out := reportPrototypeInterval{Available: true}
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

func reportPrototypeRankBefore(a, b reportPrototypeCollection) bool {
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

func reportPrototypeShare(part, total uint64) *float64 {
	if total == 0 {
		return nil
	}
	share := 100 * float64(part) / float64(total)
	return &share
}

func renderReportPrototype(report reportPrototypeSummary) ([]byte, error) {
	interval := func(v reportPrototypeInterval) any {
		if !v.Available {
			return nil
		}
		return map[string]uint64{"lower_ms": v.LowerMs, "upper_ms": v.UpperMs}
	}
	convert := func(collections []reportPrototypeCollection) []map[string]any {
		top := make([]map[string]any, 0, len(collections))
		for _, c := range collections {
			top = append(top, map[string]any{
				"scope": map[string]any{
					"operation": c.Scope.Op, "resource_type_id": c.Scope.ResourceTypeID, "resource_id": c.Scope.ResourceID,
					"parent_resource_type_id": c.Scope.ParentResourceTypeID, "parent_resource_id": c.Scope.ParentResourceID, "type_scoped": c.Scope.TypeScoped,
				},
				"pages": c.Pages, "record_writes": c.Written, "record_writes_by_family": c.Writes,
				"attempt_observations": c.Attempts, "zero_write_pages": c.ZeroWritePages, "terminal_pages": c.TerminalPages,
				"recorded_continuations": c.Continuations, "recorded_children": c.Children, "pagination_unknown_pages": c.PaginationUnknownPages, "collections": c.Collections,
				"page_duration_sum_ms": c.PageMs, "connector_duration_sum_ms": c.ConnectorMs, "connector_duration_share_pct": reportPrototypeShare(c.ConnectorMs, report.ConnectorMs),
				"reported_rate_limit_wait_sum_ms": c.ReportedWaitMs, "connector_page_duration_max_ms": c.MaxConnectorMs,
				"connector_page_duration_p50": interval(c.ConnectorPageMedian), "connector_page_duration_p95": interval(c.ConnectorPageP95),
				"record_writes_per_page": c.WrittenPerPage, "pages_per_1000_record_writes": c.PagesPerThousandWrites,
				"connector_ms_per_1000_record_writes": c.ConnectorMsPerThousandWrites,
			})
		}
		return top
	}
	return json.Marshal(map[string]any{
		"schema_version": 2, "grants_disabled": report.GrantsDisabled, "entitlements_disabled": report.EntitlementsDisabled,
		"pages": report.Pages, "collections": report.Collections, "record_writes": report.Written, "record_writes_by_family": report.Writes,
		"attempt_observations": report.Attempts, "ledger_keys_scanned": report.LedgerKeysScanned,
		"reference_validation_performed": false, "missing_continuation_references": nil, "missing_child_references": nil,
		"recorded_continuations": report.Continuations, "recorded_children": report.Children, "pagination_unknown_pages": report.PaginationUnknownPages,
		"connector_duration_sum_ms": report.ConnectorMs, "reported_rate_limit_wait_sum_ms": report.ReportedWaitMs,
		"top_collections_by_connector_ms": convert(report.Top),
		"operation_type_groups":           report.OperationTypes, "top_operation_types_by_connector_ms": convert(report.TopOperationTypes),
		"collections_omitted_from_top":           report.Collections - min(report.Collections, uint64(len(report.Top))),
		"operation_type_groups_omitted_from_top": report.OperationTypes - min(report.OperationTypes, uint64(len(report.TopOperationTypes))),
	})
}

func exportReportPrototype(report reportPrototypeSummary) error {
	dir := os.Getenv("LEDGER_REPORT_OUTPUT_DIR")
	if dir == "" {
		return nil
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	data, err := renderReportPrototype(report)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "stats.json"), append(data, '\n'), 0o600)
}

func TestLedgerReportHistogram(t *testing.T) {
	var h reportPrototypeHistogram
	require.False(t, h.quantile(50).Available)
	for _, ms := range []uint64{0, 1, 2, 3, 4, 200, 4000, math.MaxUint64} {
		h.add(ms)
	}
	require.Equal(t, reportPrototypeInterval{2, 3, true}, h.quantile(50))
	require.Equal(t, reportPrototypeInterval{uint64(1) << 63, math.MaxUint64, true}, h.quantile(95))
	var pair reportPrototypeHistogram
	pair.add(200)
	pair.add(4000)
	require.Equal(t, reportPrototypeInterval{128, 255, true}, pair.quantile(50))
	require.Equal(t, reportPrototypeInterval{2048, 4095, true}, pair.quantile(95))
}

func TestLedgerReportRendering(t *testing.T) {
	report := reportPrototypeSummary{ConnectorMs: 1000, Collections: 20}
	c := reportPrototypeCollection{ConnectorMs: 250}
	c.Scope.ResourceID = "resource-with-\"quotes\""
	c.Scope.PageToken = "TOKEN-MUST-NOT-BE-EXPORTED"
	report.Top = []reportPrototypeCollection{c}
	first, err := renderReportPrototype(report)
	require.NoError(t, err)
	second, err := renderReportPrototype(report)
	require.NoError(t, err)
	require.Equal(t, first, second)
	var decoded map[string]any
	require.NoError(t, json.Unmarshal(first, &decoded))
	require.Nil(t, decoded["grants_disabled"])
	require.NotContains(t, string(first), "page_token")
	require.NotContains(t, string(first), c.Scope.PageToken)
	top := decoded["top_collections_by_connector_ms"].([]any)[0].(map[string]any)
	require.Equal(t, 25.0, top["connector_duration_share_pct"])
	require.Nil(t, top["pages_per_1000_record_writes"])
	require.Nil(t, top["connector_ms_per_1000_record_writes"])
	require.Nil(t, top["connector_page_duration_p50"])
	require.Equal(t, c.Scope.ResourceID, top["scope"].(map[string]any)["resource_id"])
	require.Nil(t, reportPrototypeShare(0, 0))
	require.NotContains(t, string(first), "Outcome")
}

func TestLedgerReportRank(t *testing.T) {
	a := reportPrototypeCollection{ConnectorMs: 100}
	b := reportPrototypeCollection{ConnectorMs: 200}
	a.Scope.ResourceID = "a"
	b.Scope.ResourceID = "b"
	require.True(t, reportPrototypeRankBefore(b, a))
	b.ConnectorMs = 100
	require.True(t, reportPrototypeRankBefore(a, b))
	require.False(t, reportPrototypeRankBefore(b, a))
	b.Scope = a.Scope
	b.Scope.TypeScoped = true
	require.True(t, reportPrototypeRankBefore(a, b))
	require.False(t, reportPrototypeRankBefore(a, a))
}
