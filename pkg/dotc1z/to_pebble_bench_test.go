package dotc1z_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/dotc1z"
)

func toPebbleBenchTestdataPath(syncID string) string {
	_, filename, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(filename), "..", "sync", "expand", "testdata", fmt.Sprintf("sync.%s.expanded", syncID))
}

// benchmarkToPebble converts an existing expanded SQLite fixture into a v3/Pebble
// .c1z, measuring end-to-end conversion throughput. It reports per-stage row
// counts and durations as custom metrics so a single run shows where time lands.
func benchmarkToPebble(b *testing.B, syncID string) {
	c1zPath := toPebbleBenchTestdataPath(syncID)
	info, err := os.Stat(c1zPath)
	if os.IsNotExist(err) {
		b.Skipf("testdata file not found: %s", c1zPath)
	}
	require.NoError(b, err)

	// Do not use b.Context() here. Doing so causes the benchmark to run slower.
	// The SQL library's interruptOnDone() is called if ctx.Done() is not nil.
	ctx := context.Background()

	tempDir := b.TempDir()
	sourcePath := filepath.Join(tempDir, fmt.Sprintf("source.%s.c1z", syncID))
	srcData, err := os.ReadFile(c1zPath)
	require.NoError(b, err)
	require.NoError(b, os.WriteFile(sourcePath, srcData, 0600)) // #nosec G703 -- sourcePath is created under b.TempDir in this benchmark.

	c1f, err := dotc1z.NewC1ZFile(ctx, sourcePath, dotc1z.WithTmpDir(tempDir))
	require.NoError(b, err)
	defer func() { require.NoError(b, c1f.Close(ctx)) }()

	require.NoError(b, c1f.SetSyncID(ctx, syncID))
	require.NoError(b, c1f.EndSync(ctx))

	b.SetBytes(info.Size())
	b.ReportAllocs()
	b.ResetTimer()

	var stats *dotc1z.ConvertStats
	for i := 0; i < b.N; i++ {
		outPath := filepath.Join(tempDir, fmt.Sprintf("pebble-%d.c1z", i))

		b.StartTimer()
		stats, err = c1f.ToPebble(ctx, outPath, syncID)
		b.StopTimer()
		require.NoError(b, err)

		require.NoError(b, os.Remove(outPath))
	}

	if stats != nil {
		b.ReportMetric(float64(stats.Resources.Rows), "resources")
		b.ReportMetric(float64(stats.Entitlements.Rows), "entitlements")
		b.ReportMetric(float64(stats.Grants.Rows), "grants")
		b.ReportMetric(float64(stats.Assets.Rows), "assets")
		b.ReportMetric(stats.Resources.Duration.Seconds()*1000, "resources_ms")
		b.ReportMetric(stats.Entitlements.Duration.Seconds()*1000, "entitlements_ms")
		b.ReportMetric(stats.Grants.Duration.Seconds()*1000, "grants_ms")
	}
}

func BenchmarkToPebbleSmall(b *testing.B) {
	benchmarkToPebble(b, "36zGvJw3uxU1QMJKU2yPVQ1hBOC")
}

func BenchmarkToPebbleSmallMedium(b *testing.B) {
	benchmarkToPebble(b, "36zM46KKuaBq0wjSSvKh5o0350y")
}
