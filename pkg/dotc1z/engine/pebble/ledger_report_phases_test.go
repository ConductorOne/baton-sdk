package pebble

import (
	"encoding/json"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
)

func TestLedgerReportRecordedPhaseElapsed(t *testing.T) {
	e, _ := newTestEngine(t)
	_, err := e.StartNewSync(t.Context(), connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	first := map[string]int64{"list-resources": 100, "list-grants": 20}
	for n := range 2000 {
		first[fmt.Sprintf("retry_wait:label-%d", n)] = 999
	}
	require.NoError(t, e.Ledger().PutCounterBucket(t.Context(), "first", c1zstore.RunBucketWorker, c1zstore.LedgerCounters{StepDurationsMs: first}))
	second := c1zstore.LedgerCounters{StepDurationsMs: map[string]int64{"list-resources": 50}}
	require.NoError(t, e.Ledger().PutCounterBucket(t.Context(), "second", c1zstore.RunBucketWorker, second))
	require.NoError(t, e.Ledger().PutCounterBucket(t.Context(), "second", c1zstore.RunBucketWorker, second))
	id := c1zstore.LedgerActionIdentity{Op: "list-resources"}
	require.NoError(t, e.Ledger().BeginPage().Commit(t.Context(), id, &c1zstore.LedgerRow{PageDuration: 5 * time.Second}))
	report, err := e.GenerateLedgerReport(t.Context())
	require.NoError(t, err)
	var result struct {
		Phases map[string]uint64 `json:"recorded_sync_phase_elapsed_ms"`
	}
	require.NoError(t, json.Unmarshal(report, &result))
	require.Equal(t, map[string]uint64{"list-resources": 150, "list-grants": 20}, result.Phases)
	require.NotContains(t, string(report), "label-")
}

func TestLedgerReportPhaseProjectionRejectsInvalidValues(t *testing.T) {
	encode := func(value uint64) []byte {
		entry := protowire.AppendTag(nil, 1, protowire.BytesType)
		entry = protowire.AppendString(entry, "list-resources")
		entry = protowire.AppendTag(entry, 2, protowire.VarintType)
		entry = protowire.AppendVarint(entry, value)
		return protowire.AppendBytes(protowire.AppendTag(nil, 4, protowire.BytesType), entry)
	}
	_, err := ledgerReportPhaseDurations(encode(math.MaxUint64))
	require.ErrorContains(t, err, "negative")
	_, err = ledgerReportPhaseDurations(protowire.AppendTag(nil, 4, protowire.BytesType))
	require.Error(t, err)
	report := ledgerReportSummary{}
	require.NoError(t, report.addPhaseDurations(encode(math.MaxInt64)))
	require.ErrorContains(t, report.addPhaseDurations(encode(1)), "overflow")
}
