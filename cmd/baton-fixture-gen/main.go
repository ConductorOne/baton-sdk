//go:build batonsdkv2

// Command baton-fixture-gen generates synthetic hyper-scale c1z3
// fixtures for the storage-engine benchmark + equivalence harnesses
// (RFC 0004 Stack 5).
//
// Usage:
//
//	baton-fixture-gen \
//	    -out /tmp/fixture-1m-grants \
//	    -grants 1000000 \
//	    -entitlements 1000 \
//	    -principals 50000 \
//	    -sync-id 00000000000000000000000001
//
// The output is a fresh Pebble directory containing one sync's worth
// of GrantRecord rows + indexes. Run baton-storage-bench against this
// fixture to time the canonical reader workloads.
//
// Determinism: with the same -seed the same fixture is produced
// byte-for-byte (modulo Pebble's compaction non-determinism, which
// affects file layout but not logical content).
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	enginepkg "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
)

func main() {
	out := flag.String("out", "", "destination directory (must not exist)")
	grants := flag.Int("grants", 100_000, "total number of grants to generate")
	entitlements := flag.Int("entitlements", 100, "number of distinct entitlements")
	principals := flag.Int("principals", 1_000, "number of distinct principals")
	syncID := flag.String("sync-id", "", "sync ID to use (default: 25-char KSUID)")
	seed := flag.Int64("seed", 1, "PRNG seed for deterministic generation")
	flag.Parse()

	if *out == "" {
		log.Fatal("baton-fixture-gen: -out is required")
	}
	if _, err := os.Stat(*out); err == nil {
		log.Fatalf("baton-fixture-gen: %s already exists", *out)
	}

	if *syncID == "" {
		*syncID = "2N7tQT4Yx8z" + strconv.FormatInt(time.Now().Unix(), 10)
		// Pad/truncate to 27 chars (KSUID string length).
		if len(*syncID) > 27 {
			*syncID = (*syncID)[:27]
		}
		for len(*syncID) < 27 {
			*syncID += "0"
		}
	}

	if err := run(*out, *syncID, *grants, *entitlements, *principals, *seed); err != nil {
		log.Fatalf("baton-fixture-gen: %v", err)
	}
}

func run(outDir, syncID string, totalGrants, numEnt, numPrinc int, seed int64) error {
	ctx := context.Background()
	e, err := enginepkg.Open(ctx, outDir)
	if err != nil {
		return fmt.Errorf("open engine: %w", err)
	}
	defer e.Close()

	if err := e.SetCurrentSync(syncID); err != nil {
		return fmt.Errorf("SetCurrentSync: %w", err)
	}

	rng := rand.New(rand.NewSource(seed)) //nolint:gosec // deterministic fixture generation; not security-sensitive
	progressEvery := totalGrants / 20
	if progressEvery == 0 {
		progressEvery = 1
	}

	start := time.Now()
	for i := 0; i < totalGrants; i++ {
		entID := fmt.Sprintf("ent-%06d", rng.Intn(numEnt))
		principalID := fmt.Sprintf("user-%08d", rng.Intn(numPrinc))
		ext := fmt.Sprintf("grant-%010d", i)

		r := v3.GrantRecord_builder{
			SyncId:     syncID,
			ExternalId: ext,
			Entitlement: v3.EntitlementRef_builder{
				ResourceTypeId: "app",
				ResourceId:     fmt.Sprintf("app-%d", rng.Intn(10)),
				EntitlementId:  entID,
			}.Build(),
			Principal: v3.PrincipalRef_builder{
				ResourceTypeId: "user",
				ResourceId:     principalID,
			}.Build(),
		}.Build()

		if err := e.PutGrantRecord(ctx, r); err != nil {
			return fmt.Errorf("PutGrantRecord[%d]: %w", i, err)
		}

		if i%progressEvery == 0 {
			elapsed := time.Since(start)
			rps := float64(i) / elapsed.Seconds()
			fmt.Fprintf(os.Stderr, "[%5.1f%%] %d/%d grants  %.0f r/s  elapsed=%v\n",
				100*float64(i)/float64(totalGrants), i, totalGrants, rps, elapsed.Round(time.Millisecond))
		}
	}

	elapsed := time.Since(start)
	rps := float64(totalGrants) / elapsed.Seconds()
	fmt.Fprintf(os.Stderr, "done: %d grants in %v (%.0f r/s)\n",
		totalGrants, elapsed.Round(time.Millisecond), rps)
	return nil
}
