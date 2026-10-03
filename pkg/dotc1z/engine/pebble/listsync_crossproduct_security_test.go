package pebble

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"

	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// TestSecurity_ListSyncsBoundedAgainstHostileSyncRunCardinality guards the
// one-sync-per-file contract on the ListSyncs read path.
//
// Finding: pkg/dotc1z/engine/pebble/adapter_reader.go:ListSyncs:syncStatsForRun.computeSyncStats-cross-product
//
// The engine's contract (keys.go) is that a v3 pebble c1z holds exactly ONE
// sync-run record at the 2-byte fixed key; engine writers only ever Set that
// key. Pre-fix, ListSyncs walked the entire typeSyncRun key RANGE and called
// syncStatsForRun per row; a hostile LSM planting K extra rows under the
// prefix (each missing the single stats sidecar) made one ListSyncs RPC cost
// K x O(N) — K full-keyspace stats scans with K attacker-chosen up to the
// 10,000-row page cap and N the artifact's row count. The c1 sync worker
// calls ListSyncs with an empty request after every completed sync, so each
// hostile upload burned that work inside the shared worker.
//
// Post-fix, stats computation is gated to the canonical fixed key and runs
// at most once per RPC, so a hostile call must cost approximately the same
// as a clean one regardless of planted cardinality.
func TestSecurity_ListSyncsBoundedAgainstHostileSyncRunCardinality(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)

	// A real sync with real data rows (N grants).
	if _, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, ""); err != nil {
		t.Fatalf("StartNewSync: %v", err)
	}
	const grantsN = 5_000
	grants := make([]*v3.GrantRecord, 0, grantsN)
	for i := range grantsN {
		grants = append(grants, (&v3.GrantRecord_builder{
			Entitlement: v3.EntitlementRef_builder{
				ResourceTypeId: "app",
				ResourceId:     fmt.Sprintf("app-%04d", i%16),
				EntitlementId:  fmt.Sprintf("ent-%04d:member", i%64),
			}.Build(),
			Principal: v3.PrincipalRef_builder{
				ResourceTypeId: "user",
				ResourceId:     fmt.Sprintf("user-%06d", i),
			}.Build(),
			ExternalId: fmt.Sprintf("g-%06d", i),
		}).Build())
	}
	if err := e.PutGrantRecords(ctx, grants...); err != nil {
		t.Fatalf("PutGrantRecords: %v", err)
	}

	db := e.db.UnsafeForTesting()
	plant := func(k int) {
		t.Helper()
		for i := range k {
			rec := (&v3.SyncRunRecord_builder{
				SyncId: fmt.Sprintf("bogus-%06d", i),
				Type:   v3.SyncType_SYNC_TYPE_FULL,
			}).Build()
			val, err := marshalRecord(rec)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			key := append([]byte{versionV3, typeSyncRun, 0x00}, []byte(fmt.Sprintf("%08d", i))...)
			if err := db.Set(key, val, pebble.NoSync); err != nil {
				t.Fatalf("plant: %v", err)
			}
		}
	}

	// Measure the clean call first (one canonical sync row; at most one
	// stats computation).
	cleanStart := time.Now()
	if _, err := e.ListSyncs(ctx, (&reader_v2.SyncsReaderServiceListSyncsRequest_builder{}).Build()); err != nil {
		t.Fatalf("ListSyncs clean: %v", err)
	}
	cleanDur := time.Since(cleanStart)

	// Plant K hostile rows and re-measure. Pre-fix the hostile call was
	// ~K times the clean call (measured 19.7x-103.7x at K=20 across
	// machines); post-fix it must stay within a small constant factor.
	const bogusK = 20
	plant(bogusK)

	hostileStart := time.Now()
	resp, err := e.ListSyncs(ctx, (&reader_v2.SyncsReaderServiceListSyncsRequest_builder{}).Build())
	hostileDur := time.Since(hostileStart)
	if err != nil {
		t.Fatalf("ListSyncs: %v", err)
	}
	ratio := float64(hostileDur) / float64(cleanDur)
	t.Logf("ListSyncs rows=%d grants=%d bogus=%d clean=%s hostile=%s ratio=%.1fx",
		len(resp.GetSyncs()), grantsN, bogusK, cleanDur, hostileDur, ratio)

	if ratio > 5.0 {
		t.Fatalf("ListSyncs cost scales with hostile sync-run cardinality: hostile/clean = %.1fx (K=%d planted rows); per-row stats fallback must be gated to the canonical fixed key", ratio, bogusK)
	}
}
