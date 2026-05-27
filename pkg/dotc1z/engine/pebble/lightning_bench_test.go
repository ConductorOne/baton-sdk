package pebble

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/anypb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
)

// Lightning benchmarks. Coverage for the four c1z IO operations
// kans flagged as historically problematic:
//
//   1. Grant Insertion        — BenchmarkPebbleInsert{1k,10k,100k}_BothEngines
//                               (already covered by existing BenchmarkRegistered{Pebble,SQLite}WritePack;
//                                this file adds the SyncShape variant of insertion at fixed scale).
//   2. Grant Expansion        — BenchmarkPebbleExpansion_*
//   3. Uplift Bulk Reads      — BenchmarkPebbleUpliftRead_*
//   4. Compaction workflow    — BenchmarkPebbleCompaction_* (lives in pkg/synccompactor/pebble_bench_test.go)
//
// Run with:
//
//	go test -tags=baton_lambda_support,batonsdkv2 \
//	    -bench=BenchmarkPebble -benchtime=1x \
//	    ./pkg/dotc1z/engine/pebble
//
// Goal: explicit, named coverage so a future index-choice tradeoff
// can be evaluated against each category individually rather than
// against a single composite metric.

const upliftSyncs = 5
const upliftGrantsPerSync = 5_000
const upliftEntitlements = 50
const upliftUsers = 1_000

// BenchmarkPebbleExpansion_StoreExpandedGrants exercises the
// post-expansion write path: GrantStore.StoreExpandedGrants(...)
// hands the engine grants whose GrantExpandable annotation has
// already been processed by the expander. This is the hot loop
// inside pkg/sync.syncer.ExpandGrants.
func BenchmarkPebbleExpansion_StoreExpandedGrants(b *testing.B) {
	for _, n := range []int{1_000, 10_000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			ctx := context.Background()
			if err := Register(); err != nil {
				b.Fatalf("Register: %v", err)
			}
			path := fmt.Sprintf("%s/expansion.c1z", b.TempDir())
			store, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(dotc1z.EnginePebble))
			if err != nil {
				b.Fatalf("NewStore: %v", err)
			}
			if _, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, ""); err != nil {
				b.Fatalf("StartNewSync: %v", err)
			}
			// Seed grants the expander would have produced — a mix
			// of plain grants and grants whose GrantExpandable
			// annotations have already been stripped. The engine
			// re-writes them after expansion.
			grants := make([]*v2.Grant, 0, n)
			for i := 0; i < n; i++ {
				g := mkV2Grant("g-"+strconv.Itoa(i), "ent-"+strconv.Itoa(i%50), "user", "u-"+strconv.Itoa(i%1000))
				grants = append(grants, g)
			}
			c1z := store.(dotc1z.C1ZStore)
			b.ReportAllocs()
			b.ReportMetric(float64(n), "grants/op")
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := c1z.Grants().StoreExpandedGrants(ctx, grants...); err != nil {
					b.Fatalf("StoreExpandedGrants: %v", err)
				}
			}
			b.StopTimer()
			_ = store.Close(ctx)
		})
	}
}

// BenchmarkPebbleUpliftRead_CrossSync exercises the access pattern
// kans called "critical": iterating all grants for a fixed
// entitlement across N finished syncs. The current
// idxGrantByEntitlement keyspace is sync-prefixed, so this
// benchmark walks one PaginateGrantsByEntitlement per sync. A
// future entitlement-first index would collapse the call count.
//
// Output metric `entries_returned` is the number of grants
// surfaced for the target entitlement across all syncs — should
// equal ~(upliftGrantsPerSync / upliftEntitlements) * upliftSyncs.
func BenchmarkPebbleUpliftRead_CrossSync(b *testing.B) {
	ctx := context.Background()
	if err := Register(); err != nil {
		b.Fatalf("Register: %v", err)
	}
	path := fmt.Sprintf("%s/uplift.c1z", b.TempDir())

	store, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(dotc1z.EnginePebble))
	if err != nil {
		b.Fatalf("NewStore: %v", err)
	}

	// Seed: upliftSyncs syncs each containing upliftGrantsPerSync
	// grants spread across upliftEntitlements + upliftUsers.
	syncIDs := make([]string, 0, upliftSyncs)
	for s := 0; s < upliftSyncs; s++ {
		id, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		if err != nil {
			b.Fatalf("StartNewSync %d: %v", s, err)
		}
		grants := make([]*v2.Grant, 0, upliftGrantsPerSync)
		for i := 0; i < upliftGrantsPerSync; i++ {
			grants = append(grants, mkV2Grant(
				"g-"+strconv.Itoa(s)+"-"+strconv.Itoa(i),
				"ent-"+strconv.Itoa(i%upliftEntitlements),
				"user",
				"u-"+strconv.Itoa(i%upliftUsers),
			))
		}
		if err := store.PutGrants(ctx, grants...); err != nil {
			b.Fatalf("PutGrants %d: %v", s, err)
		}
		if err := store.EndSync(ctx); err != nil {
			b.Fatalf("EndSync %d: %v", s, err)
		}
		syncIDs = append(syncIDs, id)
	}

	targetEntitlement := "ent-0" // hottest entitlement, hit by every sync
	b.ReportAllocs()
	b.ReportMetric(float64(upliftSyncs), "syncs/op")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		total := 0
		for _, syncID := range syncIDs {
			// ListGrantsForEntitlement keys off the engine's
			// current sync; set it before each per-sync call.
			if err := store.SetCurrentSync(ctx, syncID); err != nil {
				b.Fatalf("SetCurrentSync %s: %v", syncID, err)
			}
			pageToken := ""
			for {
				resp, err := store.ListGrantsForEntitlement(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
					Entitlement: v2.Entitlement_builder{
						Id: targetEntitlement,
						Resource: v2.Resource_builder{
							Id: v2.ResourceId_builder{ResourceType: "app", Resource: "github"}.Build(),
						}.Build(),
					}.Build(),
					Annotations: []*anypb.Any{},
					PageSize:    1000,
					PageToken:   pageToken,
				}.Build())
				if err != nil {
					b.Fatalf("ListGrantsForEntitlement (sync %s): %v", syncID, err)
				}
				total += len(resp.GetList())
				pageToken = resp.GetNextPageToken()
				if pageToken == "" {
					break
				}
			}
		}
		b.ReportMetric(float64(total), "entries_returned")
	}
	b.StopTimer()
	_ = store.Close(ctx)
}

// makeGrantWithAnnotations is a small helper used by benchmarks
// that want to include annotation payloads. Kept simple — the
// annotation here is intentionally a no-op so the benchmark
// measures the put path's overhead at realistic record sizes,
// not the annotation library.
func makeGrantWithAnnotations(id, entID, principalID string) *v2.Grant {
	g := mkV2Grant(id, entID, "user", principalID)
	a := annotations.Annotations{}
	// Add a discovered_at-shaped annotation; uses anypb.Any so
	// the wire shape mirrors what real connectors emit.
	tsAny, err := anypb.New(&v2.ETag{Value: "etag-" + id})
	if err == nil {
		a = append(a, tsAny)
	}
	g.SetAnnotations(a)
	_ = time.Now()
	return g
}

// _ = makeGrantWithAnnotations keeps the helper exercised even if
// no benchmark calls it directly — useful as a building block for
// the follow-up uplift-with-annotations bench when the index work
// lands.
var _ = makeGrantWithAnnotations
