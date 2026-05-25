package pebble

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
)

func benchmarkGrants(n int) []*v2.Grant {
	grants := make([]*v2.Grant, 0, n)
	for i := 0; i < n; i++ {
		grants = append(grants, mkV2Grant(
			fmt.Sprintf("grant-%08d", i),
			fmt.Sprintf("entitlement-%04d", i%100),
			"user",
			fmt.Sprintf("user-%08d", i%1000),
		))
	}
	return grants
}

func prepareRegisteredC1Z(b *testing.B, n int, engine dotc1z.Engine) (string, string) {
	b.Helper()
	ctx := context.Background()
	if engine == dotc1z.EnginePebble {
		if err := Register(); err != nil {
			b.Fatalf("Register: %v", err)
		}
	}
	path := fmt.Sprintf("%s/%s-sync.c1z", b.TempDir(), engine)
	store, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(engine))
	if err != nil {
		b.Fatalf("NewStore: %v", err)
	}
	syncID, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	if err != nil {
		b.Fatalf("StartNewSync: %v", err)
	}
	if err := store.PutGrants(ctx, benchmarkGrants(n)...); err != nil {
		b.Fatalf("PutGrants: %v", err)
	}
	if err := store.EndSync(ctx); err != nil {
		b.Fatalf("EndSync: %v", err)
	}
	if err := store.Close(ctx); err != nil {
		b.Fatalf("Close: %v", err)
	}
	return path, syncID
}

func benchmarkRegisteredWritePack(b *testing.B, engine dotc1z.Engine, n int) {
	ctx := context.Background()
	root := b.TempDir()
	grants := benchmarkGrants(n)
	if engine == dotc1z.EnginePebble {
		if err := Register(); err != nil {
			b.Fatalf("Register: %v", err)
		}
	}
	b.ReportAllocs()
	b.ReportMetric(float64(n), "grants/op")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		path := fmt.Sprintf("%s/%s-sync-%06d.c1z", root, engine, i)
		store, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(engine))
		if err != nil {
			b.Fatalf("NewStore: %v", err)
		}
		if _, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, ""); err != nil {
			b.Fatalf("StartNewSync: %v", err)
		}
		if err := store.PutGrants(ctx, grants...); err != nil {
			b.Fatalf("PutGrants: %v", err)
		}
		if err := store.EndSync(ctx); err != nil {
			b.Fatalf("EndSync: %v", err)
		}
		if err := store.Close(ctx); err != nil {
			b.Fatalf("Close: %v", err)
		}
	}
}

func benchmarkRegisteredUnpackReadGrants(b *testing.B, engine dotc1z.Engine, n int) {
	ctx := context.Background()
	path, syncID := prepareRegisteredC1Z(b, n, engine)
	b.ReportAllocs()
	b.ReportMetric(float64(n), "grants/op")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store, err := dotc1z.NewStore(ctx, path, dotc1z.WithReadOnly(true))
		if err != nil {
			b.Fatalf("NewStore read-only: %v", err)
		}
		if err := store.SetCurrentSync(ctx, syncID); err != nil {
			b.Fatalf("SetCurrentSync: %v", err)
		}
		resp, err := store.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
		if err != nil {
			b.Fatalf("ListGrants: %v", err)
		}
		if got := len(resp.GetList()); got != n {
			b.Fatalf("ListGrants count = %d, want %d", got, n)
		}
		if err := store.Close(ctx); err != nil {
			b.Fatalf("Close: %v", err)
		}
	}
}

func BenchmarkPebbleAdapterWriteGrant(b *testing.B) {
	ctx := context.Background()
	a := newAdapter(b)
	if _, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, ""); err != nil {
		b.Fatalf("StartNewSync: %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := a.PutGrants(ctx, mkV2Grant(
			fmt.Sprintf("grant-%08d", i),
			fmt.Sprintf("entitlement-%04d", i%100),
			"user",
			fmt.Sprintf("user-%08d", i%1000),
		)); err != nil {
			b.Fatalf("PutGrants: %v", err)
		}
	}
	b.StopTimer()
	if err := a.Close(ctx); err != nil {
		b.Fatalf("Close: %v", err)
	}
}

func benchmarkRegisteredWriteGrant(b *testing.B, engine dotc1z.Engine) {
	ctx := context.Background()
	if engine == dotc1z.EnginePebble {
		if err := Register(); err != nil {
			b.Fatalf("Register: %v", err)
		}
	}
	path := fmt.Sprintf("%s/%s-sync.c1z", b.TempDir(), engine)
	store, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(engine))
	if err != nil {
		b.Fatalf("NewStore: %v", err)
	}
	if _, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, ""); err != nil {
		b.Fatalf("StartNewSync: %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := store.PutGrants(ctx, mkV2Grant(
			fmt.Sprintf("grant-%08d", i),
			fmt.Sprintf("entitlement-%04d", i%100),
			"user",
			fmt.Sprintf("user-%08d", i%1000),
		)); err != nil {
			b.Fatalf("PutGrants: %v", err)
		}
	}
	b.StopTimer()
	if err := store.Close(ctx); err != nil {
		b.Fatalf("Close: %v", err)
	}
}

func BenchmarkRegisteredPebbleWriteGrant(b *testing.B) {
	benchmarkRegisteredWriteGrant(b, dotc1z.EnginePebble)
}

func BenchmarkRegisteredSQLiteWriteGrant(b *testing.B) {
	benchmarkRegisteredWriteGrant(b, dotc1z.EngineSQLite)
}

// writePackScales is the grant-count grid the bench sweeps. Small
// sizes catch fixed-cost regressions (startup, encode). The 100k/1M
// sizes catch the LSM-vs-B-tree scaling curve where SQLite's per-row
// cost grows nonlinearly and Pebble's stays flat. Filter via
// `-bench='/grants=100000$'` or set BATONSDK_BENCH_SCALES=100,1000.
var writePackScales = []int{100, 1000, 10000, 100000, 1000000}

func grantsScales() []int {
	if env := os.Getenv("BATONSDK_BENCH_SCALES"); env != "" {
		out := []int{}
		for _, s := range strings.Split(env, ",") {
			n, err := strconv.Atoi(strings.TrimSpace(s))
			if err == nil && n > 0 {
				out = append(out, n)
			}
		}
		if len(out) > 0 {
			return out
		}
	}
	if testing.Short() {
		return []int{100, 1000, 10000}
	}
	return writePackScales
}

func BenchmarkRegisteredPebbleWritePack(b *testing.B) {
	for _, n := range grantsScales() {
		b.Run(fmt.Sprintf("grants=%d", n), func(b *testing.B) {
			benchmarkRegisteredWritePack(b, dotc1z.EnginePebble, n)
		})
	}
}

func BenchmarkRegisteredSQLiteWritePack(b *testing.B) {
	for _, n := range grantsScales() {
		b.Run(fmt.Sprintf("grants=%d", n), func(b *testing.B) {
			benchmarkRegisteredWritePack(b, dotc1z.EngineSQLite, n)
		})
	}
}

func BenchmarkRegisteredPebbleUnpackReadGrants(b *testing.B) {
	for _, n := range grantsScales() {
		b.Run(fmt.Sprintf("grants=%d", n), func(b *testing.B) {
			benchmarkRegisteredUnpackReadGrants(b, dotc1z.EnginePebble, n)
		})
	}
}

func BenchmarkRegisteredSQLiteUnpackReadGrants(b *testing.B) {
	for _, n := range grantsScales() {
		b.Run(fmt.Sprintf("grants=%d", n), func(b *testing.B) {
			benchmarkRegisteredUnpackReadGrants(b, dotc1z.EngineSQLite, n)
		})
	}
}

func BenchmarkExternalC1ZOpenAndList(b *testing.B) {
	path := os.Getenv("BATONSDK_BENCH_C1Z")
	if path == "" {
		b.Skip("set BATONSDK_BENCH_C1Z to a baton-demo-generated sync.c1z to run this benchmark")
	}
	ctx := context.Background()
	if err := Register(); err != nil {
		b.Fatalf("Register: %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store, err := dotc1z.NewStore(ctx, path, dotc1z.WithReadOnly(true))
		if err != nil {
			b.Fatalf("NewStore: %v", err)
		}
		latest, ok := store.(connectorstore.LatestFinishedSyncIDFetcher)
		if !ok {
			b.Fatalf("store %T does not expose LatestFinishedSyncIDFetcher", store)
		}
		syncID, err := latest.LatestFinishedSyncID(ctx, connectorstore.SyncTypeAny)
		if err != nil {
			b.Fatalf("LatestFinishedSyncID: %v", err)
		}
		if syncID == "" {
			b.Fatalf("no finished sync in %s", path)
		}
		if err := store.SetCurrentSync(ctx, syncID); err != nil {
			b.Fatalf("SetCurrentSync: %v", err)
		}
		if _, err := store.ListResourceTypes(ctx, v2.ResourceTypesServiceListResourceTypesRequest_builder{}.Build()); err != nil {
			b.Fatalf("ListResourceTypes: %v", err)
		}
		if _, err := store.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{}.Build()); err != nil {
			b.Fatalf("ListResources: %v", err)
		}
		if _, err := store.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{}.Build()); err != nil {
			b.Fatalf("ListEntitlements: %v", err)
		}
		if _, err := store.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build()); err != nil {
			b.Fatalf("ListGrants: %v", err)
		}
		if err := store.Close(ctx); err != nil {
			b.Fatalf("Close: %v", err)
		}
	}
}
