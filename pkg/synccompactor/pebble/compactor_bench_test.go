package pebble

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/segmentio/ksuid"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	enginepkg "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
)

// BenchmarkCompactionFlow measures the cross-engine Compact path —
// the load-bearing primitive behind c1's compaction workflow.
// Source has N grants under one sync_id; the compactor builds an
// SST per record-type bucket and IngestAndExcise's into dst.
//
// We sweep two scales (10k and 100k grants) and report the
// per-grant compaction cost. Output metrics: ns/op, grants/op,
// and B/op.
//
// Run with:
//
//	go test -tags=baton_lambda_support,batonsdkv2 \
//	    -bench=BenchmarkCompactionFlow -benchtime=1x \
//	    ./pkg/synccompactor/pebble
func BenchmarkCompactionFlow(b *testing.B) {
	for _, n := range []int{10_000, 100_000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			b.ReportMetric(float64(n), "grants/op")

			// Pre-seed N grants into a fresh source engine per
			// iteration so the benchmark doesn't reuse a hot LSM
			// state between runs. Each iter: new src + new dst.
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				src, _ := benchNewEngine(b, fmt.Sprintf("src-%d", i))
				dst, _ := benchNewEngine(b, fmt.Sprintf("dst-%d", i))
				syncID := ksuid.New().String()
				ctx := context.Background()
				if err := src.MarkFreshSync(syncID); err != nil {
					b.Fatalf("MarkFreshSync src: %v", err)
				}
				if err := dst.MarkFreshSync(syncID); err != nil {
					b.Fatalf("MarkFreshSync dst: %v", err)
				}
				records := make([]*v3.GrantRecord, 0, n)
				for j := 0; j < n; j++ {
					records = append(records, benchGrant(syncID, j))
				}
				if err := src.PutGrantRecords(ctx, records...); err != nil {
					b.Fatalf("PutGrantRecords: %v", err)
				}
				if err := src.EndFreshSync(ctx); err != nil {
					b.Fatalf("EndFreshSync: %v", err)
				}

				comp, err := NewCompactor(dst, b.TempDir())
				if err != nil {
					b.Fatalf("NewCompactor: %v", err)
				}

				b.StartTimer()
				if err := comp.Compact(ctx, src, syncID); err != nil {
					b.Fatalf("Compact: %v", err)
				}
				b.StopTimer()
			}
		})
	}
}

func benchNewEngine(b *testing.B, name string) (*enginepkg.Engine, string) {
	b.Helper()
	root := b.TempDir()
	dir := root + "/" + name
	e, err := enginepkg.Open(context.Background(), dir)
	if err != nil {
		b.Fatalf("Open %s: %v", name, err)
	}
	b.Cleanup(func() { _ = e.Close() })
	return e, dir
}

func benchGrant(syncID string, i int) *v3.GrantRecord {
	return v3.GrantRecord_builder{
		ExternalId: "grant-" + strconv.Itoa(i),
		Entitlement: v3.EntitlementRef_builder{
			ResourceTypeId: "app",
			ResourceId:     "github",
			EntitlementId:  "ent-" + strconv.Itoa(i%50),
		}.Build(),
		Principal: v3.PrincipalRef_builder{
			ResourceTypeId: "user",
			ResourceId:     "u-" + strconv.Itoa(i%1000),
		}.Build(),
	}.Build()
}
