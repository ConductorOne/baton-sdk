package pebble

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLedgerReportHistogram(t *testing.T) {
	var h ledgerReportHistogram
	require.False(t, h.quantile(50).Available)
	for _, ms := range []uint64{0, 1, 2, 3, 4, 200, 4000, math.MaxUint64} {
		h.add(ms)
	}
	require.Equal(t, ledgerReportInterval{2, 3, true}, h.quantile(50))
	require.Equal(t, ledgerReportInterval{uint64(1) << 63, math.MaxUint64, true}, h.quantile(95))
	var pair ledgerReportHistogram
	pair.add(200)
	pair.add(4000)
	require.Equal(t, ledgerReportInterval{128, 255, true}, pair.quantile(50))
	require.Equal(t, ledgerReportInterval{2048, 4095, true}, pair.quantile(95))
}

func TestLedgerReportRendering(t *testing.T) {
	report := ledgerReportSummary{ConnectorMs: 1000, Collections: 20}
	c := ledgerReportCollection{ConnectorMs: 250}
	c.Scope.ResourceID = "resource-with-\"quotes\""
	c.Scope.PageToken = "TOKEN-MUST-NOT-BE-EXPORTED"
	report.Top = []ledgerReportCollection{c}
	first, err := renderLedgerReport(report)
	require.NoError(t, err)
	second, err := renderLedgerReport(report)
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
	require.Nil(t, ledgerReportShare(0, 0))
	require.NotContains(t, string(first), "Outcome")
}

func TestLedgerReportRank(t *testing.T) {
	a := ledgerReportCollection{ConnectorMs: 100}
	b := ledgerReportCollection{ConnectorMs: 200}
	a.Scope.ResourceID = "a"
	b.Scope.ResourceID = "b"
	require.True(t, ledgerReportRankBefore(b, a))
	b.ConnectorMs = 100
	require.True(t, ledgerReportRankBefore(a, b))
	require.False(t, ledgerReportRankBefore(b, a))
	b.Scope = a.Scope
	b.Scope.TypeScoped = true
	require.True(t, ledgerReportRankBefore(a, b))
	require.False(t, ledgerReportRankBefore(a, a))
}
