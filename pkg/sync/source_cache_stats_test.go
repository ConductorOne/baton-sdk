package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/logging"
)

// TestSourceCacheStats_TokenRoundTrip pins the resume property: the
// counters live in the checkpointed sync token, so a suspended warm sync
// keeps its replay/bounce counts across Marshal → Unmarshal.
func TestSourceCacheStats_TokenRoundTrip(t *testing.T) {
	st := newState()
	require.NoError(t, st.Unmarshal(""))

	st.AddSourceCacheStats(SourceCacheStats{
		ScopesReplayed: 3,
		ScopesStamped:  2,
		RowsReplayed:   map[string]int64{"grants": 40, "resources": 5},
		OverlayRows:    4,
		TombstoneIDs:   2,
		LookupBounces:  6,
		LookupBouncesByOp: map[string]int64{
			"sync-grants-for-resource": 6,
		},
	})
	// Second merge accumulates.
	st.AddSourceCacheStats(SourceCacheStats{
		ScopesReplayed:    1,
		RowsReplayed:      map[string]int64{"grants": 10},
		LookupBouncesByOp: map[string]int64{"sync-resources": 1},
	})

	token, err := st.Marshal()
	require.NoError(t, err)

	resumed := newState()
	require.NoError(t, resumed.Unmarshal(token))
	got := resumed.SourceCacheStatsSnapshot()
	require.NotNil(t, got, "stats must survive the token round trip")
	require.Equal(t, int64(4), got.ScopesReplayed)
	require.Equal(t, int64(2), got.ScopesStamped)
	require.Equal(t, int64(50), got.RowsReplayed["grants"])
	require.Equal(t, int64(5), got.RowsReplayed["resources"])
	require.Equal(t, int64(4), got.OverlayRows)
	require.Equal(t, int64(2), got.TombstoneIDs)
	require.Equal(t, int64(6), got.LookupBounces)
	require.Equal(t, int64(6), got.LookupBouncesByOp["sync-grants-for-resource"])
	require.Equal(t, int64(1), got.LookupBouncesByOp["sync-resources"])

	// Resumed state keeps accumulating on top of the restored counters.
	resumed.AddSourceCacheStats(SourceCacheStats{ScopesReplayed: 1})
	require.Equal(t, int64(5), resumed.SourceCacheStatsSnapshot().ScopesReplayed)

	// A fresh (empty-input) state reports nothing.
	fresh := newState()
	require.NoError(t, fresh.Unmarshal(""))
	require.Nil(t, fresh.SourceCacheStatsSnapshot())
}

// TestSourceCacheStats_EndToEnd runs a cold sync then a warm
// continuation sync and asserts the recorded counters match the known
// shape: 5 groups → 5 stamped scopes cold; 5 replayed scopes, 5 bounces
// (one ask per group page), 10 rows replayed, zero cold pages warm.
func TestSourceCacheStats_EndToEnd(t *testing.T) {
	ctx, err := logging.Init(context.Background())
	require.NoError(t, err)
	tmpDir := t.TempDir()
	mc := newContinuationMockConnector(t, 5)

	runWithStats := func(path, prevPath string) *SourceCacheStats {
		store, err := dotc1z.NewStore(ctx, path,
			dotc1z.WithEngine(c1zstore.EnginePebble),
			dotc1z.WithTmpDir(tmpDir),
		)
		require.NoError(t, err)
		opts := []SyncOpt{WithConnectorStore(store), WithTmpDir(tmpDir)}
		if prevPath != "" {
			opts = append(opts, WithPreviousSyncC1ZPath(prevPath))
		}
		sc, err := NewSyncer(ctx, mc, opts...)
		require.NoError(t, err)
		require.NoError(t, sc.Sync(ctx))
		impl, ok := sc.(*syncer)
		require.True(t, ok)
		stats := impl.state.SourceCacheStatsSnapshot()
		require.NoError(t, sc.Close(ctx))
		return stats
	}

	sync1 := filepath.Join(tmpDir, "sync1.c1z")
	cold := runWithStats(sync1, "")
	require.NotNil(t, cold)
	require.Equal(t, int64(5), cold.ScopesStamped, "cold: every group page stamps a scope")
	require.Zero(t, cold.ScopesReplayed)
	require.Zero(t, cold.LookupBounces, "no offer on a cold sync, so no bounces")

	warm := runWithStats(filepath.Join(tmpDir, "sync2.c1z"), sync1)
	require.NotNil(t, warm)
	require.Equal(t, int64(5), warm.ScopesReplayed, "warm: every group page replays")
	require.Zero(t, warm.ScopesStamped, "no cold pages on the warm sync")
	require.Equal(t, int64(10), warm.RowsReplayed["grants"], "5 groups × 2 member grants replayed")
	require.Equal(t, int64(5), warm.LookupBounces, "one ask bounce per group page")
	require.Equal(t, int64(5), warm.LookupRequestsBounced)
	require.Equal(t, int64(5), warm.LookupScopesAsked)
	require.Equal(t, int64(5), warm.LookupAnsweredFound)
	require.Equal(t, int64(5), warm.LookupBouncesByOp["sync-grants-for-resource"])
	require.Zero(t, warm.LookupCapFailures)
}
