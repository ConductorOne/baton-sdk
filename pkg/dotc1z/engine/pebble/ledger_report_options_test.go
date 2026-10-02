package pebble

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLedgerReportOptionsBoundedProjection(t *testing.T) {
	input := `{"attempt":"run","secret":"must-not-appear","requested":{"worker_count":4,"skip_grants":true,"secret":"must-not-appear","resource_types":[` +
		strings.Repeat(`"type",`, 99999) +
		`"last"],"targets":[{"resource_id":"resource","secret":"must-not-appear"}]}}`
	var summary ledgerReportOptionSummary
	require.NoError(t, json.Unmarshal([]byte(input), &summary))
	require.EqualValues(t, 100000, summary.Requested.ResourceTypes.Total)
	require.Len(t, summary.Requested.ResourceTypes.Values, 16)
	require.Nil(t, summary.Requested.LedgerRequestedOptions.ResourceTypes)
	require.Nil(t, summary.LedgerReportOptions.Requested.ResourceTypes)
	require.Equal(t, 4, summary.Requested.WorkerCount)
	require.True(t, summary.Requested.SkipGrants)
	encoded, err := json.Marshal(summary)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "must-not-appear")
	require.Less(t, len(encoded), 4000)
}

func TestLedgerReportOptionsRejectMalformedScope(t *testing.T) {
	for _, input := range []string{`{"requested":{"resource_types":{}}}`, `{"requested":{"targets":[1]}}`} {
		var summary ledgerReportOptionSummary
		require.Error(t, json.Unmarshal([]byte(input), &summary))
	}
}
