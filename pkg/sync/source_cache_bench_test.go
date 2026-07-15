package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

// Source-cache path benchmarks: the write-side cost of scope stamping
// (inline by_source_scope index + manifest writes + the seal-time I6
// scan) and the replay path (engine copy + the replay-time count check),
// against an unstamped cold sync of the same rows as the baseline.
//
//	go test -bench=BenchmarkSourceCacheSync -benchtime=1x -count=3 \
//	    -run='^$' ./pkg/sync/
//
// Shape: nGroups groups × membersPerGroup member grants, one grants
// scope per group (the per-resource conditional-request shape), users
// listed unstamped — a realistic mixed sync. The warm variant replays
// every scope (all 304s): the maximum-replay / maximum-count-check case.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/logging"
)

const (
	benchGroups          = 500
	benchUsers           = 5_000
	benchMembersPerGroup = 50 // 500 × 50 = 25k grants, 500 scopes
)

func buildSourceCacheBenchConnector(b *testing.B, ctx context.Context, stamped bool) *sourceCacheMockConnector {
	b.Helper()
	mc := newSourceCacheMockConnector()
	users := make([]string, benchUsers)
	for i := range benchUsers {
		u, err := mc.AddUser(ctx, fmt.Sprintf("u-%05d", i))
		if err != nil {
			b.Fatalf("AddUser: %v", err)
		}
		users[i] = u.GetId().GetResource()
		_ = users[i]
	}
	userRes := mc.resourceDB[userResourceType.GetId()]
	for gi := range benchGroups {
		g, _, err := mc.AddGroup(ctx, fmt.Sprintf("g-%04d", gi))
		if err != nil {
			b.Fatalf("AddGroup: %v", err)
		}
		for m := range benchMembersPerGroup {
			mc.AddGroupMember(ctx, g, userRes[(gi*benchMembersPerGroup+m)%len(userRes)])
		}
		if stamped {
			mc.etagByResource[g.GetId().GetResource()] = "v1"
		}
	}
	return mc
}

func benchOneSync(b *testing.B, ctx context.Context, cc *sourceCacheMockConnector, path, prevPath, tmpDir string) {
	b.Helper()
	store, err := dotc1z.NewStore(ctx, path,
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithTmpDir(tmpDir),
	)
	if err != nil {
		b.Fatalf("NewStore: %v", err)
	}
	opts := []SyncOpt{WithConnectorStore(store), WithTmpDir(tmpDir)}
	if prevPath != "" {
		opts = append(opts, WithPreviousSyncC1ZPath(prevPath))
	}
	syncer, nerr := NewSyncer(ctx, cc, opts...)
	if nerr != nil {
		b.Fatalf("NewSyncer: %v", nerr)
	}
	if err := syncer.Sync(ctx); err != nil {
		b.Fatalf("Sync: %v", err)
	}
	if err := syncer.Close(ctx); err != nil {
		b.Fatalf("Close: %v", err)
	}
}

func BenchmarkSourceCacheSync(b *testing.B) {
	ctx, err := logging.Init(context.Background(), logging.WithLogLevel("error"))
	if err != nil {
		b.Fatalf("logging.Init: %v", err)
	}

	b.Run("cold_unstamped", func(b *testing.B) {
		mc := buildSourceCacheBenchConnector(b, ctx, false)
		tmpDir := b.TempDir()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			start := time.Now()
			benchOneSync(b, ctx, mc, filepath.Join(tmpDir, fmt.Sprintf("cold-un-%d.c1z", i)), "", tmpDir)
			b.ReportMetric(float64(time.Since(start).Milliseconds()), "ms/sync")
		}
	})

	b.Run("cold_stamped", func(b *testing.B) {
		mc := buildSourceCacheBenchConnector(b, ctx, true)
		tmpDir := b.TempDir()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			start := time.Now()
			path := filepath.Join(tmpDir, fmt.Sprintf("cold-st-%d.c1z", i))
			benchOneSync(b, ctx, mc, path, "", tmpDir)
			b.ReportMetric(float64(time.Since(start).Milliseconds()), "ms/sync")
			if i == 0 {
				if fi, err := os.Stat(path); err == nil {
					b.ReportMetric(float64(fi.Size())/(1<<20), "MB/c1z")
				}
			}
		}
	})

	b.Run("warm_replay", func(b *testing.B) {
		mc := buildSourceCacheBenchConnector(b, ctx, true)
		tmpDir := b.TempDir()
		prevPath := filepath.Join(tmpDir, "prev.c1z")
		benchOneSync(b, ctx, mc, prevPath, "", tmpDir) // seed: fresh fetch, stamps + manifest
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			start := time.Now()
			benchOneSync(b, ctx, mc, filepath.Join(tmpDir, fmt.Sprintf("warm-%d.c1z", i)), prevPath, tmpDir)
			b.ReportMetric(float64(time.Since(start).Milliseconds()), "ms/sync")
		}
	})
}
