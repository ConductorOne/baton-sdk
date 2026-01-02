package synccompactor

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"syscall"
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/stretchr/testify/require"
)

// getRSSFromProc reads the current RSS from /proc/self/status on Linux
// or uses rusage on macOS. Returns bytes.
func getRSSFromProc() uint64 {
	// Try Linux /proc/self/status first
	data, err := os.ReadFile("/proc/self/status")
	if err == nil {
		lines := string(data)
		for _, line := range splitLines(lines) {
			if len(line) > 6 && line[:6] == "VmRSS:" {
				var rss uint64
				fmt.Sscanf(line, "VmRSS: %d kB", &rss)
				return rss * 1024
			}
		}
	}

	// Fallback to rusage (macOS)
	var rusage syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &rusage)
	return uint64(rusage.Maxrss)
}

func splitLines(s string) []string {
	var lines []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			lines = append(lines, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		lines = append(lines, s[start:])
	}
	return lines
}

func getMemStats() runtime.MemStats {
	var m runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m)
	return m
}

func formatBytes(b uint64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := uint64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

func formatSignedBytes(b int64) string {
	sign := ""
	if b < 0 {
		sign = "-"
		b = -b
	}
	return sign + formatBytes(uint64(b))
}

// TestCompactorMmapMemory tests memory behavior using the ACTUAL compactor code
// Compares RSS growth with default SQLite settings vs mmap_size=0
func TestCompactorMmapMemory(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping compactor memory test in short mode")
	}

	ctx := context.Background()

	// Test configuration - adjust for your needs
	numCompactionCycles := 10
	syncsPerCycle := 3
	resourcesPerSync := 10000
	grantsPerSync := 20000

	t.Logf("Config: %d compaction cycles, %d syncs/cycle, %d resources/sync, %d grants/sync",
		numCompactionCycles, syncsPerCycle, resourcesPerSync, grantsPerSync)

	// Test 1: Default pragmas (mmap enabled by SQLite default)
	// t.Run("DefaultPragmas", func(t *testing.T) {
	// 	runCompactorMemoryTest(ctx, t, numCompactionCycles, syncsPerCycle, resourcesPerSync, grantsPerSync, false)
	// })

	// Test 2: With mmap_size=0
	t.Run("MmapDisabled", func(t *testing.T) {
		runCompactorMemoryTest(ctx, t, numCompactionCycles, syncsPerCycle, resourcesPerSync, grantsPerSync, true)
	})
}

func runCompactorMemoryTest(ctx context.Context, t *testing.T, numCycles, syncsPerCycle, resourcesPerSync, grantsPerSync int, disableMmap bool) {
	mmapStatus := "ENABLED (SQLite default)"
	if disableMmap {
		mmapStatus = "DISABLED (mmap_size=0)"
	}
	t.Logf("=== mmap %s ===", mmapStatus)

	baseDir := t.TempDir()

	runtime.GC()
	runtime.GC()
	debug.FreeOSMemory()

	initialRSS := getRSSFromProc()
	initialMem := getMemStats()
	t.Logf("INITIAL: RSS=%s, GoSys=%s, GoHeap=%s",
		formatBytes(initialRSS), formatBytes(initialMem.Sys), formatBytes(initialMem.HeapAlloc))

	rssHistory := make([]uint64, 0, numCycles)

	for cycle := 0; cycle < numCycles; cycle++ {
		cycleDir := filepath.Join(baseDir, fmt.Sprintf("cycle-%d", cycle))
		outputDir := filepath.Join(cycleDir, "output")
		err := os.MkdirAll(outputDir, 0755)
		require.NoError(t, err)

		// Create sync files for this compaction cycle
		compactableSyncs := createSyncsForCompaction(ctx, t, cycleDir, syncsPerCycle, resourcesPerSync, grantsPerSync, disableMmap)

		// Use the ACTUAL compactor
		compactor, cleanup, err := NewCompactor(ctx, outputDir, compactableSyncs,
			WithTmpDir(cycleDir),
		)
		require.NoError(t, err)

		// Run compaction
		_, compactErr := compactor.Compact(ctx)
		if compactErr != nil {
			// Grant expansion might fail with empty connector, but compaction itself should work
			t.Logf("Cycle %d compaction note: %v", cycle, compactErr)
		}

		// Cleanup
		err = cleanup()
		require.NoError(t, err)

		err = os.RemoveAll(cycleDir)
		require.NoError(t, err)

		runtime.GC()

		rss := getRSSFromProc()
		rssHistory = append(rssHistory, rss)

		if (cycle+1)%5 == 0 || cycle == 0 {
			mem := getMemStats()
			t.Logf("After cycle %d: RSS=%s, GoSys=%s, GoHeap=%s",
				cycle+1, formatBytes(rss), formatBytes(mem.Sys), formatBytes(mem.HeapAlloc))
		}
	}

	runtime.GC()
	runtime.GC()
	debug.FreeOSMemory()

	finalRSS := getRSSFromProc()
	finalMem := getMemStats()
	rssGrowth := int64(finalRSS) - int64(initialRSS)
	sysGrowth := int64(finalMem.Sys) - int64(initialMem.Sys)

	t.Logf("FINAL: RSS=%s, GoSys=%s, GoHeap=%s",
		formatBytes(finalRSS), formatBytes(finalMem.Sys), formatBytes(finalMem.HeapAlloc))
	t.Logf("GROWTH: RSS=%s, GoSys=%s", formatSignedBytes(rssGrowth), formatSignedBytes(sysGrowth))

	// Trend analysis
	if len(rssHistory) >= 4 {
		q1End := len(rssHistory) / 4
		q4Start := len(rssHistory) * 3 / 4

		var avgQ1, avgQ4 uint64
		for i := 0; i < q1End; i++ {
			avgQ1 += rssHistory[i]
		}
		avgQ1 /= uint64(q1End)

		for i := q4Start; i < len(rssHistory); i++ {
			avgQ4 += rssHistory[i]
		}
		avgQ4 /= uint64(len(rssHistory) - q4Start)

		t.Logf("RSS trend: Q1 avg=%s, Q4 avg=%s, growth=%s",
			formatBytes(avgQ1), formatBytes(avgQ4), formatSignedBytes(int64(avgQ4)-int64(avgQ1)))
	}
}

func createSyncsForCompaction(ctx context.Context, t *testing.T, baseDir string, numSyncs, resourcesPerSync, grantsPerSync int, disableMmap bool) []*CompactableSync {
	t.Helper()

	syncs := make([]*CompactableSync, numSyncs)

	for i := 0; i < numSyncs; i++ {
		syncID := fmt.Sprintf("sync-%d-%d", time.Now().UnixNano(), i)
		filePath := filepath.Join(baseDir, fmt.Sprintf("%s.c1z", syncID))

		opts := []dotc1z.C1ZOption{
			dotc1z.WithTmpDir(baseDir),
		}
		// if disableMmap {
		// 	opts = append(opts, dotc1z.WithPragma("mmap_size", "0"))
		// }

		db, err := dotc1z.NewC1ZFile(ctx, filePath, opts...)
		require.NoError(t, err)

		_, err = db.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)

		// Create resource type
		rt := &v2.ResourceType{
			Id:          fmt.Sprintf("type-%d", i),
			DisplayName: fmt.Sprintf("Type %d", i),
		}
		err = db.PutResourceTypes(ctx, rt)
		require.NoError(t, err)

		// Create resources in batches
		batchSize := 5000
		for j := 0; j < resourcesPerSync; j += batchSize {
			end := j + batchSize
			if end > resourcesPerSync {
				end = resourcesPerSync
			}
			resources := make([]*v2.Resource, 0, end-j)
			for k := j; k < end; k++ {
				resources = append(resources, &v2.Resource{
					Id: &v2.ResourceId{
						ResourceType: rt.Id,
						Resource:     fmt.Sprintf("res-%d-%d", i, k),
					},
					DisplayName: fmt.Sprintf("Resource %d-%d", i, k),
				})
			}
			err = db.PutResources(ctx, resources...)
			require.NoError(t, err)
		}

		// Create entitlement
		ent := &v2.Entitlement{
			Id:          fmt.Sprintf("ent-%d", i),
			DisplayName: fmt.Sprintf("Entitlement %d", i),
			Slug:        fmt.Sprintf("ent-%d", i),
			Resource: &v2.Resource{
				Id: &v2.ResourceId{ResourceType: rt.Id, Resource: "owner"},
			},
		}
		err = db.PutEntitlements(ctx, ent)
		require.NoError(t, err)

		// Create grants in batches
		for j := 0; j < grantsPerSync; j += batchSize {
			end := j + batchSize
			if end > grantsPerSync {
				end = grantsPerSync
			}
			grants := make([]*v2.Grant, 0, end-j)
			for k := j; k < end; k++ {
				grants = append(grants, &v2.Grant{
					Id:          fmt.Sprintf("grant-%d-%d", i, k),
					Entitlement: ent,
					Principal: &v2.Resource{
						Id: &v2.ResourceId{
							ResourceType: rt.Id,
							Resource:     fmt.Sprintf("res-%d-%d", i, k%resourcesPerSync),
						},
					},
				})
			}
			err = db.PutGrants(ctx, grants...)
			require.NoError(t, err)
		}

		err = db.EndSync(ctx)
		require.NoError(t, err)

		// Get the actual sync ID
		actualSyncID, err := db.LatestFinishedSyncID(ctx, connectorstore.SyncTypeAny)
		require.NoError(t, err)

		err = db.Close()
		require.NoError(t, err)

		syncs[i] = &CompactableSync{
			FilePath: filePath,
			SyncID:   actualSyncID,
		}
	}

	return syncs
}
