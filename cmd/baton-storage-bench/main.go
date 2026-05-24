//go:build batonsdkv2

// Command baton-storage-bench is the canonical benchmark harness for
// the v3 Pebble storage engine (RFC 0004 §6, Stack 5).
//
// Targets the goal table from RFC §6:
//
//	G1  Bulk write grants from a single sync_run
//	G2  Random Get by primary key
//	G3  List by entitlement
//	G4  List by principal
//	G5  Full-sync scan
//	G6  Cold open + Checkpoint
//	G7  Cross-engine compaction (delegated to synccompactor/pebble)
//	G8  Memory ceiling under burst
//	G9  Save → Open → Read roundtrip
//	G10 Engine open count for the multi-engine ReaderCache
//
// Stack 5 MVP implements G1–G5 (the read/write hot path). G6–G10 land
// alongside Stack 5 follow-up commits as the canonical fixture and
// cmd/c1z-bench expand.
//
// Usage:
//
//	# generate a fixture first:
//	baton-fixture-gen -out /tmp/fix-1m -grants 1000000 \
//	    -entitlements 1000 -principals 50000
//
//	# then bench:
//	baton-storage-bench -input /tmp/fix-1m -sync-id ... -bench G3 -duration 30s
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	enginepkg "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
)

func main() {
	input := flag.String("input", "", "input Pebble directory (built by baton-fixture-gen)")
	syncID := flag.String("sync-id", "", "sync_id to bench against")
	benchName := flag.String("bench", "G3", "benchmark to run: G2|G3|G4|G5")
	duration := flag.Duration("duration", 10*time.Second, "minimum total benchmark duration")
	entitlements := flag.Int("entitlements", 100, "matches the fixture's -entitlements")
	principals := flag.Int("principals", 1000, "matches the fixture's -principals")
	flag.Parse()

	if *input == "" {
		log.Fatal("baton-storage-bench: -input is required")
	}
	if *syncID == "" {
		log.Fatal("baton-storage-bench: -sync-id is required")
	}

	ctx := context.Background()
	e, err := enginepkg.Open(ctx, *input, enginepkg.WithReadOnly(true))
	if err != nil {
		log.Fatalf("open engine: %v", err)
	}
	defer e.Close()
	if err := e.SetCurrentSync(*syncID); err != nil {
		_ = e.Close()
		fmt.Fprintf(os.Stderr, "SetCurrentSync: %v\n", err)
		os.Exit(1) //nolint:gocritic // close already invoked above
	}

	switch *benchName {
	case "G2":
		runG2(ctx, e, *syncID, *duration)
	case "G3":
		runG3(ctx, e, *syncID, *entitlements, *duration)
	case "G4":
		runG4(ctx, e, *syncID, *principals, *duration)
	case "G5":
		runG5(ctx, e, *syncID, *duration)
	default:
		log.Fatalf("unknown bench %q (G2|G3|G4|G5 supported)", *benchName)
	}
}

// G2 — random Get by primary key.
func runG2(ctx context.Context, e *enginepkg.Engine, syncID string, dur time.Duration) {
	rng := rand.New(rand.NewSource(1)) //nolint:gosec // deterministic bench seed
	deadline := time.Now().Add(dur)
	var ops, hits, misses int64

	start := time.Now()
	for time.Now().Before(deadline) {
		ext := fmt.Sprintf("grant-%010d", rng.Intn(1_000_000))
		_, err := e.GetGrantRecord(ctx, syncID, ext)
		ops++
		if err == nil {
			hits++
		} else {
			misses++
		}
	}
	elapsed := time.Since(start)
	report("G2 Get-by-PK", elapsed, ops, hits, misses)
}

// G3 — list by entitlement.
func runG3(ctx context.Context, e *enginepkg.Engine, syncID string, numEnt int, dur time.Duration) {
	rng := rand.New(rand.NewSource(2)) //nolint:gosec // deterministic bench seed
	deadline := time.Now().Add(dur)
	var ops, rows int64

	start := time.Now()
	for time.Now().Before(deadline) {
		entID := fmt.Sprintf("ent-%06d", rng.Intn(numEnt))
		var got int
		err := e.IterateGrantsByEntitlement(ctx, syncID, entID, func(*v3.GrantRecord) bool {
			got++
			return true
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "iter error: %v\n", err)
			os.Exit(1)
		}
		ops++
		rows += int64(got)
	}
	elapsed := time.Since(start)
	report("G3 ListByEntitlement", elapsed, ops, rows, 0)
}

// G4 — list by principal.
func runG4(ctx context.Context, e *enginepkg.Engine, syncID string, numPrinc int, dur time.Duration) {
	rng := rand.New(rand.NewSource(3)) //nolint:gosec // deterministic bench seed
	deadline := time.Now().Add(dur)
	var ops, rows int64

	start := time.Now()
	for time.Now().Before(deadline) {
		pid := fmt.Sprintf("user-%08d", rng.Intn(numPrinc))
		var got int
		err := e.IterateGrantsByPrincipal(ctx, syncID, "user", pid, func(*v3.GrantRecord) bool {
			got++
			return true
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "iter error: %v\n", err)
			os.Exit(1)
		}
		ops++
		rows += int64(got)
	}
	elapsed := time.Since(start)
	report("G4 ListByPrincipal", elapsed, ops, rows, 0)
}

// G5 — full-sync scan.
func runG5(ctx context.Context, e *enginepkg.Engine, syncID string, dur time.Duration) {
	deadline := time.Now().Add(dur)
	var ops, rows int64

	start := time.Now()
	for time.Now().Before(deadline) {
		var got int
		err := e.IterateGrantsBySync(ctx, syncID, func(*v3.GrantRecord) bool {
			got++
			return true
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "iter error: %v\n", err)
			os.Exit(1)
		}
		ops++
		rows += int64(got)
	}
	elapsed := time.Since(start)
	report("G5 FullSyncScan", elapsed, ops, rows, 0)
}

func report(name string, elapsed time.Duration, ops, a, b int64) {
	opsPerSec := float64(ops) / elapsed.Seconds()
	nsPerOp := float64(elapsed.Nanoseconds()) / float64(ops)
	fmt.Fprintf(os.Stdout, "%s: %d ops in %v  %.0f ops/s  %.0f ns/op  a=%d b=%d\n",
		name, ops, elapsed.Round(time.Millisecond), opsPerSec, nsPerOp, a, b)
}
