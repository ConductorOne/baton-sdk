package pebble

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

func BenchmarkSourceCacheReplayResources(b *testing.B) {
	for _, rows := range []int{1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("rows-%d", rows), func(b *testing.B) {
			ctx := context.Background()
			base := b.TempDir()
			prev, err := Open(ctx, filepath.Join(base, "previous"))
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = prev.Close() })
			prevAdapter := NewAdapter(prev)
			if _, err := prevAdapter.StartNewSync(ctx, connectorstore.SyncTypeFull, ""); err != nil {
				b.Fatal(err)
			}
			const writeChunk = 1_000
			for start := 0; start < rows; start += writeChunk {
				end := min(start+writeChunk, rows)
				records := make([]*v3.ResourceRecord, 0, end-start)
				for i := start; i < end; i++ {
					records = append(records, v3.ResourceRecord_builder{
						ResourceTypeId: "user",
						ResourceId:     fmt.Sprintf("user-%08d", i),
						SourceScopeKey: "benchmark-scope",
					}.Build())
				}
				if err := prev.PutResourceRecords(ctx, records...); err != nil {
					b.Fatal(err)
				}
			}

			b.ReportAllocs()
			b.SetBytes(int64(rows))
			b.ReportMetric(float64(rows), "rows/op")
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				currentPath := filepath.Join(base, fmt.Sprintf("current-%d", i))
				current, err := Open(ctx, currentPath)
				if err != nil {
					b.Fatal(err)
				}
				currentAdapter := NewAdapter(current)
				if _, err := currentAdapter.StartNewSync(ctx, connectorstore.SyncTypeFull, ""); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()

				result, err := current.ReplaySourceCacheResources(ctx, prev, "benchmark-scope")
				if err != nil {
					b.Fatal(err)
				}
				if result.Rows != int64(rows) {
					b.Fatalf("replayed %d rows, want %d", result.Rows, rows)
				}

				b.StopTimer()
				if err := current.Close(); err != nil {
					b.Fatal(err)
				}
				if err := os.RemoveAll(currentPath); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
			}
		})
	}
}
