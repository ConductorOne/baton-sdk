package c1zstore

import (
	"testing"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/stretchr/testify/require"
)

func TestApplySyncTokenStatsRecordPreservesIngestQualityPresence(t *testing.T) {
	for _, tc := range []struct {
		name       string
		checkpoint string
		want       *v3.IngestQualityStats
	}{
		{
			name:       "clean",
			checkpoint: `{"version":1,"ingest_quality":{}}`,
			want:       &v3.IngestQualityStats{},
		},
		{
			name:       "blocked",
			checkpoint: `{"version":1,"ingest_quality":{"source_cache_replay_blocked":true,"grants_dropped":3,"reason_flags":2}}`,
			want: v3.IngestQualityStats_builder{
				SourceCacheReplayBlocked: true,
				GrantsDropped:            3,
				ReasonFlags:              2,
			}.Build(),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := &v3.SyncStatsRecord{}
			ApplySyncTokenStatsRecord(rec, tc.checkpoint)
			require.Equal(t, tc.want, rec.GetIngestQuality())
		})
	}
}

func TestSourceCacheReplayEligibleFailsClosed(t *testing.T) {
	require.False(t, SourceCacheReplayEligible(nil))
	require.False(t, SourceCacheReplayEligible(&v3.SyncStatsRecord{}))
	require.True(t, SourceCacheReplayEligible(v3.SyncStatsRecord_builder{
		IngestQuality: &v3.IngestQualityStats{},
	}.Build()))
	require.False(t, SourceCacheReplayEligible(v3.SyncStatsRecord_builder{
		IngestQuality: v3.IngestQualityStats_builder{SourceCacheReplayBlocked: true}.Build(),
	}.Build()))
}
