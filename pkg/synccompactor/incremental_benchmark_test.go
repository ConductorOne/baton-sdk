package synccompactor

import (
	"context"
	"runtime"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

func BenchmarkIncrementalCompactionHappy(b *testing.B) {
	for _, incremental := range []bool{true, false} {
		name := "full"
		if incremental {
			name = "incremental"
		}
		b.Run(name, func(b *testing.B) {
			benchmarkIncrementalCompaction(b, incremental, false)
		})
	}
}

func BenchmarkIncrementalCompactionDecline(b *testing.B) {
	for _, requested := range []bool{true, false} {
		name := "full"
		if requested {
			name = "incremental-requested-declined"
		}
		b.Run(name, func(b *testing.B) {
			benchmarkIncrementalCompaction(b, requested, true)
		})
	}
}

func benchmarkIncrementalCompaction(b *testing.B, requestIncremental, declining bool) {
	ctx := context.Background()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		inputDir := b.TempDir()
		var entries []*CompactableSync
		if declining {
			entries = buildSpecChangeFixtures(b, ctx, inputDir, false, true)
		} else {
			entries = buildIncrementalFixtures(b, ctx, inputDir)
		}
		options := []Option{WithTmpDir(b.TempDir()), WithEngine(c1zstore.EnginePebble)}
		if requestIncremental {
			options = append(options, WithIncrementalExpansion())
		}
		compactor, cleanup, err := NewCompactor(ctx, b.TempDir(), entries, options...)
		if err != nil {
			b.Fatal(err)
		}
		runtime.GC()
		var before runtime.MemStats
		runtime.ReadMemStats(&before)
		b.StartTimer()
		_, err = compactor.Compact(ctx)
		b.StopTimer()
		if err != nil {
			b.Fatal(err)
		}
		if err := cleanup(); err != nil {
			b.Fatal(err)
		}
		reportCompactorHeapDelta(b, &before)
	}
}

func reportCompactorHeapDelta(b *testing.B, before *runtime.MemStats) {
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	if after.TotalAlloc >= before.TotalAlloc {
		b.ReportMetric(float64(after.TotalAlloc-before.TotalAlloc), "total-alloc-bytes/op")
	}
	if after.HeapInuse >= before.HeapInuse {
		b.ReportMetric(float64(after.HeapInuse-before.HeapInuse), "heap-inuse-delta-bytes/op")
	}
}
