package sync

import (
	"context"
	"fmt"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/sourcecache"
)

// BenchmarkSourceCachePageOps pins the per-page cost source-cache
// orchestration adds to the collection handlers' hot path (one call per
// list-response page, before the page's row puts).
//
// The disabled cells are the cost EVERY production page pays while no
// connector declares SourceCacheCapability: two annotation Picks over the
// page's annotation slice. The enabled cells price the parse-and-gate work
// for opted-in connectors. All cells are pure CPU — no store I/O happens
// until beforeUpserts/afterUpserts, whose costs are engine-level and
// benchmarked in pkg/dotc1z/engine/pebble (BenchmarkSourceCacheReplay*,
// BenchmarkScopeIngest*).
func BenchmarkSourceCachePageOps(b *testing.B) {
	ctx := context.Background()
	newBenchSyncer := func(capable bool) *syncer {
		s := &syncer{state: newState(), syncType: connectorstore.SyncTypeFull}
		if capable {
			s.sourceCacheCapability = v2.SourceCacheCapability_builder{
				Mode:            v2.SourceCacheCapability_MODE_READ_WRITE,
				CacheGeneration: "gen-1",
			}.Build()
			s.sourceCacheStore = &fakeSourceCacheStore{}
		}
		return s
	}
	record := v2.SourceCacheRecord_builder{
		ScopeKey:       "grants:team-1",
		CacheValidator: "etag-1",
	}.Build()
	unrelated := annotations.New(&v2.ETag{}, &v2.RateLimitDescription{})

	cells := []struct {
		name    string
		capable bool
		annos   annotations.Annotations
	}{
		{"disabled-no-annotations", false, nil},
		{"disabled-unrelated-annotations", false, unrelated},
		{"enabled-no-annotations", true, nil},
		{"enabled-record-annotation", true, annotations.New(record)},
	}
	for _, cell := range cells {
		b.Run(cell.name, func(b *testing.B) {
			s := newBenchSyncer(cell.capable)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ops, err := s.sourceCachePageOps(ctx, sourcecache.RowKindGrants, cell.annos, 100)
				if err != nil {
					b.Fatal(err)
				}
				_ = ops
			}
		})
	}
}

// BenchmarkSourceCacheScopeLocks pins the retained per-scope replay lock
// cost (CO-6b-003): one mutex per distinct replay-annotated (rowKind,
// scopeKey), never reclaimed for the syncer's lifetime — the same
// cardinality as the provenance sets, whose serialized cost curve
// BenchmarkStateMarshalSourceCacheSets pins. new-scopes prices the
// first-touch allocation; existing-scope is the steady-state per-page cost.
func BenchmarkSourceCacheScopeLocks(b *testing.B) {
	b.Run("new-scopes", func(b *testing.B) {
		s := &syncer{}
		keys := make([]string, b.N)
		for i := range keys {
			keys[i] = fmt.Sprintf("grants:team-%d", i)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			s.sourceCacheScopeLock(sourcecache.RowKindGrants, keys[i])
		}
	})
	b.Run("existing-scope", func(b *testing.B) {
		s := &syncer{}
		s.sourceCacheScopeLock(sourcecache.RowKindGrants, "grants:team-1")
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			s.sourceCacheScopeLock(sourcecache.RowKindGrants, "grants:team-1")
		}
	})
}

// BenchmarkStateMarshalSourceCacheSets pins the checkpoint-serialization
// cost curve of the source-cache provenance sets (hit map with validators +
// replayed set), which are re-serialized into the sync token on EVERY
// checkpoint. Scope keys here are HashScope-sized (64 hex chars) and
// validators ETag-sized. The curve is the documented bound on
// state.sourceCacheHits: a connector whose scope count makes this material
// should move the sets to sidecar persistence before adopting source cache
// at that scale.
func BenchmarkStateMarshalSourceCacheSets(b *testing.B) {
	for _, scopes := range []int{0, 1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("scopes-%d", scopes), func(b *testing.B) {
			st := newState()
			for i := 0; i < scopes; i++ {
				scope := sourcecache.HashScope(fmt.Sprintf("grants:team-%d", i))
				st.RecordSourceCacheHit(sourcecache.RowKindGrants, scope, fmt.Sprintf("W/\"etag-%d\"", i))
				st.MarkSourceCacheReplayed(sourcecache.RowKindGrants, scope)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				token, err := st.Marshal()
				if err != nil {
					b.Fatal(err)
				}
				if i == 0 {
					b.ReportMetric(float64(len(token)), "token-bytes")
				}
			}
		})
	}
}
