package dotc1z

import (
	"context"
	"os"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/stretchr/testify/require"
)

func benchmarkStats(b *testing.B, syncID string, expectedStats map[string]int64) {
	c1zPath := cloneSyncBenchTestdataPath(syncID)
	info, err := os.Stat(c1zPath)
	if os.IsNotExist(err) {
		b.Skipf("testdata file not found: %s", c1zPath)
	}
	require.NoError(b, err)

	// Do not use b.Context() here. Doing so causes the benchmark to run slower.
	// The SQL library's interruptOnDone() is called if ctx.Done() is not nil.
	ctx := context.Background()

	c1f, err := newC1ZFile(ctx, c1zPath)
	require.NoError(b, err)
	defer c1f.Close(ctx)

	err = c1f.SetSyncID(ctx, syncID)
	require.NoError(b, err)

	b.SetBytes(info.Size())
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		stats, err := c1f.Stats(ctx, connectorstore.SyncTypeAny, syncID)
		require.NoError(b, err)
		require.Equal(b, expectedStats, stats)
	}

	err = c1f.Close(ctx)
	require.NoError(b, err)
}

func BenchmarkStatsSmall(b *testing.B) {
	expectedStats := map[string]int64{
		"entitlements":   334,
		"grants":         418389,
		"group":          17,
		"project":        100,
		"resource_types": 4,
		"role":           100,
		"user":           35000,
	}
	benchmarkStats(b, "36zGvJw3uxU1QMJKU2yPVQ1hBOC", expectedStats)
}

func BenchmarkStatsMedium(b *testing.B) {
	expectedStats := map[string]int64{
		"entitlements":   334,
		"grants":         537189,
		"group":          17,
		"project":        100,
		"resource_types": 4,
		"role":           100,
		"user":           45000,
	}
	benchmarkStats(b, "36zM46KKuaBq0wjSSvKh5o0350y", expectedStats)
}
