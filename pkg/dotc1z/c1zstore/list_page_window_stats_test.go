package c1zstore

import (
	"encoding/json"
	"math"
	"testing"

	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
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

func TestSampledFromCounters(t *testing.T) {
	require.False(t, SampledFromCounters(nil))
	require.False(t, SampledFromCounters(map[string]uint64{"list_page_window.sampled": 0}))
	require.False(t, SampledFromCounters(map[string]uint64{"list_page_window.sampled:group": 1}))
	require.True(t, SampledFromCounters(map[string]uint64{"list_page_window.sampled": 1}))
}

func TestSampledSyncIsNotAReplaySource(t *testing.T) {
	for _, tc := range []struct {
		kind                       connectorstore.SyncType
		compacted, sampled, usable bool
	}{
		{connectorstore.SyncTypeFull, false, false, true},
		{connectorstore.SyncTypeFull, true, false, false},
		{connectorstore.SyncTypeFull, false, true, false},
		{connectorstore.SyncTypeFull, true, true, false},
		{connectorstore.SyncTypePartial, false, false, false},
		{connectorstore.SyncTypePartial, false, true, false},
		{connectorstore.SyncTypeResourcesOnly, false, false, false},
		{connectorstore.SyncTypeResourcesOnly, false, true, false},
	} {
		run := SyncRun{Type: tc.kind, Compacted: tc.compacted, Sampled: tc.sampled}
		require.Equal(t, tc.usable, run.UsableAsReplaySource(), "%+v", tc)
	}
}

func TestPendingWorkWindowPagesJSONCompatibility(t *testing.T) {
	var legacy LedgerWork
	require.NoError(t, json.Unmarshal([]byte(`{"sync_id":"sync","id":1,"revision":0,"action":{}}`), &legacy))
	require.Zero(t, legacy.WindowPages)
	encoded, err := json.Marshal(legacy)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "window_pages")
	var current LedgerWork
	require.NoError(t, json.Unmarshal([]byte(`{"sync_id":"sync","id":1,"revision":0,"action":{},"window_pages":2}`), &current))
	require.EqualValues(t, 2, current.WindowPages)
	encoded, err = json.Marshal(current)
	require.NoError(t, err)
	var restored LedgerWork
	require.NoError(t, json.Unmarshal(encoded, &restored))
	require.Equal(t, current, restored)
}
