package pebble

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/segmentio/ksuid"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/codec"
)

// Diff-strategy benchmark — trie (diffGrants) vs full-scan (diffGrantsFullScan).
//
// Fixture shape: N entitlements × M grants in base, plus a small addition set in
// applied (diffBenchDirtyEnts existing ents each gain diffBenchAddPerDirty grants,
// plus diffBenchNewEnts brand-new entitlements). The fixture is scale-independent:
// only N×M changes between scale levels; the addition count stays fixed.
//
// Scales (grant counts in base):
//
//	10K  — 50 ents × 200 grants, depth-0 trees.  Always runs (also with -short).
//	1M   — 1 000 ents × 1 000 grants, depth-1 trees.  Default.
//	50M  — 5 000 ents × 10 000 grants, depth-1 trees.  Set BATONSDK_BENCH_DIFF_LONG=1.
//	        WARNING: seeding 2×50M grants takes ~20 min. Run overnight or on CI.
//
// Run examples:
//
//	# default (1M)
//	go test -run=^$ -bench=BenchmarkDiffGrants -benchtime=1x ./pkg/dotc1z/engine/pebble
//
//	# quick smoke (10K)
//	go test -run=^$ -bench=BenchmarkDiffGrants -benchtime=1x -short ./pkg/dotc1z/engine/pebble
//
//	# full suite including 50M
//	BATONSDK_BENCH_DIFF_LONG=1 go test -run=^$ -bench=BenchmarkDiffGrants -benchtime=1x \
//	    ./pkg/dotc1z/engine/pebble
const (
	// additions injected into applied — same count at every scale.
	diffBenchDirtyEnts    = 5  // existing entitlements that gain new grants in applied
	diffBenchAddPerDirty  = 20 // new grants per dirty entitlement
	diffBenchNewEnts      = 5  // brand-new entitlements present only in applied
	diffBenchGrantsNewEnt = 20 // grants per new entitlement

	diffBenchTotalAdded = diffBenchDirtyEnts*diffBenchAddPerDirty +
		diffBenchNewEnts*diffBenchGrantsNewEnt // = 200
)

type diffScale struct {
	tag          string
	ents         int
	grantsPerEnt int
}

// diffBenchScales returns the scale levels to run.
//   - -short  → 10K only (fast smoke).
//   - default → 1M.
//   - BATONSDK_BENCH_DIFF_LONG=1 → 1M + 50M.
func diffBenchScales() []diffScale {
	if testing.Short() {
		return []diffScale{{tag: "10K", ents: 50, grantsPerEnt: 200}}
	}
	scales := []diffScale{{tag: "1M", ents: 1_000, grantsPerEnt: 1_000}}
	if os.Getenv("BATONSDK_BENCH_DIFF_LONG") != "" {
		scales = append(scales, diffScale{tag: "50M", ents: 5_000, grantsPerEnt: 10_000})
	}
	return scales
}

// seedGrantDiffBench builds the base and applied syncs. Seeding is the
// expensive part; call this once per benchmark sub-function before the loop.
//
// Grants are PUT entitlement-by-entitlement to keep peak memory per call at
// O(grantsPerEnt) rather than O(ents×grantsPerEnt).
// newAdapterNoSync opens an adapter with DurabilityNoSync so the 200
// per-diff grant writes don't each pay a WAL fsync.
func newAdapterNoSync(t testing.TB) *Adapter {
	t.Helper()
	e, _ := newTestEngine(t, WithDurability(DurabilityNoSync))
	return NewAdapter(e)
}

func seedGrantDiffBench(b *testing.B, a *Adapter, ents, grantsPerEnt int) (baseSyncID, appliedSyncID string) {
	b.Helper()
	ctx := context.Background()

	baseEnts := make([]*v2.Entitlement, ents)
	for i := range baseEnts {
		baseEnts[i] = mkV2Ent(fmt.Sprintf("ent-%04d", i))
	}

	// Base sync ----------------------------------------------------------------
	var err error
	baseSyncID, err = a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	if err != nil {
		b.Fatalf("StartNewSync(base): %v", err)
	}
	if err := a.PutEntitlements(ctx, baseEnts...); err != nil {
		b.Fatalf("PutEntitlements(base): %v", err)
	}
	for e := range ents {
		entID := fmt.Sprintf("ent-%04d", e)
		batch := make([]*v2.Grant, grantsPerEnt)
		for g := range batch {
			batch[g] = mkV2Grant(
				fmt.Sprintf("%s:g%06d", entID, g),
				entID, "user", fmt.Sprintf("u%08d", g),
			)
		}
		if err := a.PutGrants(ctx, batch...); err != nil {
			b.Fatalf("PutGrants(base ent %d): %v", e, err)
		}
	}
	if err := a.EndSync(ctx); err != nil {
		b.Fatalf("EndSync(base): %v", err)
	}

	// Applied sync -------------------------------------------------------------
	appliedSyncID, err = a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	if err != nil {
		b.Fatalf("StartNewSync(applied): %v", err)
	}
	appliedEnts := make([]*v2.Entitlement, 0, ents+diffBenchNewEnts)
	appliedEnts = append(appliedEnts, baseEnts...)
	for i := range diffBenchNewEnts {
		appliedEnts = append(appliedEnts, mkV2Ent(fmt.Sprintf("new-ent-%04d", i)))
	}
	if err := a.PutEntitlements(ctx, appliedEnts...); err != nil {
		b.Fatalf("PutEntitlements(applied): %v", err)
	}

	// Existing entitlements: identical to base, plus additions in the first few.
	for e := range ents {
		entID := fmt.Sprintf("ent-%04d", e)
		cap := grantsPerEnt
		if e < diffBenchDirtyEnts {
			cap += diffBenchAddPerDirty
		}
		batch := make([]*v2.Grant, 0, cap)
		for g := range grantsPerEnt {
			batch = append(batch, mkV2Grant(
				fmt.Sprintf("%s:g%06d", entID, g),
				entID, "user", fmt.Sprintf("u%08d", g),
			))
		}
		if e < diffBenchDirtyEnts {
			for g := range diffBenchAddPerDirty {
				batch = append(batch, mkV2Grant(
					fmt.Sprintf("%s:added%04d", entID, g),
					entID, "user", fmt.Sprintf("added-u%d-%08d", e, g),
				))
			}
		}
		if err := a.PutGrants(ctx, batch...); err != nil {
			b.Fatalf("PutGrants(applied ent %d): %v", e, err)
		}
	}

	// Brand-new entitlements present only in applied.
	for i := range diffBenchNewEnts {
		entID := fmt.Sprintf("new-ent-%04d", i)
		batch := make([]*v2.Grant, diffBenchGrantsNewEnt)
		for g := range batch {
			batch[g] = mkV2Grant(
				fmt.Sprintf("%s:g%06d", entID, g),
				entID, "user", fmt.Sprintf("new-ent-u%d-%08d", i, g),
			)
		}
		if err := a.PutGrants(ctx, batch...); err != nil {
			b.Fatalf("PutGrants(applied new-ent %d): %v", i, err)
		}
	}

	if err := a.EndSync(ctx); err != nil {
		b.Fatalf("EndSync(applied): %v", err)
	}
	return baseSyncID, appliedSyncID
}

// runDiffGrantsBench is the shared body for the two diff benchmarks.
func runDiffGrantsBench(b *testing.B, diffFn func(context.Context, *Adapter, []byte, []byte, string, string, string) error) {
	b.Helper()
	for _, sc := range diffBenchScales() {
		b.Run(sc.tag, func(b *testing.B) {
			a := newAdapterNoSync(b)
			ctx := context.Background()
			baseSyncID, appliedSyncID := seedGrantDiffBench(b, a, sc.ents, sc.grantsPerEnt)

			baseBytes, err := codec.EncodeSyncID(baseSyncID)
			if err != nil {
				b.Fatalf("encode base: %v", err)
			}
			appliedBytes, err := codec.EncodeSyncID(appliedSyncID)
			if err != nil {
				b.Fatalf("encode applied: %v", err)
			}

			b.ReportMetric(float64(sc.ents*sc.grantsPerEnt), "base_grants")
			b.ReportMetric(float64(diffBenchTotalAdded), "expected_additions")

			for b.Loop() {
				diffID := ksuid.New().String()
				if err := diffFn(ctx, a, baseBytes, appliedBytes, baseSyncID, appliedSyncID, diffID); err != nil {
					b.Fatalf("diff: %v", err)
				}
			}
		})
	}
}

// fullScanAdapter wraps diffGrantsFullScan to match the diffGrants signature.
func fullScanAdapter(ctx context.Context, a *Adapter, baseBytes, appliedBytes []byte, _, _, diffID string) error {
	return diffGrantsFullScan(ctx, a, baseBytes, appliedBytes, diffID)
}

// BenchmarkDiffGrants_Trie / BenchmarkDiffGrants_FullScan use DurabilityNoSync
// so that the 200 per-diff grant writes don't each pay a WAL fsync. The writes
// still happen (churn is present), but sync latency is excluded from the
// measurement. This isolates the read/comparison cost of each strategy.
func BenchmarkDiffGrants_Trie(b *testing.B)     { runDiffGrantsBench(b, diffGrants) }
func BenchmarkDiffGrants_FullScan(b *testing.B) { runDiffGrantsBench(b, fullScanAdapter) }
