package c1zstore

import (
	"math"
	"testing"

	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/stretchr/testify/require"
)

func TestListPageWindowStatsOverlays(t *testing.T) {
	const checkpointJSON = `{"counters":{"list_page_window.sampled":2,"list_page_window.sampled:group":2}}`
	reader := &reader_v2.SyncStats{}
	ApplySyncTokenStats(reader, checkpointJSON)
	require.EqualValues(t, 2, reader.GetCounters()["list_page_window.sampled"])
	storage := &v3.SyncStatsRecord{}
	ApplySyncTokenStatsRecord(storage, checkpointJSON)
	require.EqualValues(t, 2, storage.GetCounters()["list_page_window.sampled:group"])
	stats := LedgerSyncStats(nil, LedgerCounters{Counters: map[string]uint64{"list_page_window.sampled": 3}})
	require.EqualValues(t, 3, stats.Run.Counters["list_page_window.sampled"])
	require.Nil(t, ReaderCounters(nil))
	require.Equal(t, map[string]int64{"large": math.MaxInt64, "small": 2}, ReaderCounters(map[string]uint64{"large": math.MaxUint64, "small": 2}))
}

func TestLedgerSyncStatsPublishesOnlyListPageWindowCounters(t *testing.T) {
	for _, sampled := range []bool{false, true} {
		input := map[string]uint64{
			LedgerCompletedActions:          17,
			"actions.completed.list-grants": 4,
			"ingest.entitlements_dropped":   3,
			"list_page_windowish.sampled":   8,
			"list_page_window":              9,
		}
		want := map[string]uint64{}
		if sampled {
			input["list_page_window.sampled"] = 2
			input["list_page_window.sampled:group"] = 2
			want["list_page_window.sampled"] = 2
			want["list_page_window.sampled:group"] = 2
		}
		stats := LedgerSyncStats(map[string]string{LedgerFactIngestKnown: "true"}, LedgerCounters{Counters: input})
		require.Equal(t, want, stats.Run.Counters)
		require.EqualValues(t, 17, stats.Run.CompletedActions)
		require.NotNil(t, stats.IngestQuality)
		require.EqualValues(t, 3, stats.IngestQuality.EntitlementsDropped)
		require.EqualValues(t, 17, input[LedgerCompletedActions])
		require.EqualValues(t, 3, input["ingest.entitlements_dropped"])
	}
}
