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
