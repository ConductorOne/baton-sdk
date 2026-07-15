package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

// Soak sweep for the replay-equivalence harness: 20 additional seeds
// beyond TestSourceCache_ReplayEquivalence's fixed five, alternating
// worker counts and continuation topology. Gated behind BATON_SOAK so
// ordinary CI runs don't pay ~20x harness cost.
//
// TEMPORARY VERIFICATION FILE — safe to delete; nothing references it.

import (
	"fmt"
	"os"
	"testing"
)

func TestSourceCache_ReplayEquivalenceSoak(t *testing.T) {
	if os.Getenv("BATON_SOAK") == "" {
		t.Skip("set BATON_SOAK=1 to run the seed soak")
	}
	for seed := int64(6); seed <= 25; seed++ {
		workers := 0
		if seed%2 == 0 {
			workers = 4
		}
		continuation := seed%3 == 0
		name := fmt.Sprintf("workers=%d/seed=%d/continuation=%v", workers, seed, continuation)
		t.Run(name, func(t *testing.T) {
			runReplayEquivalenceScenario(t, workers, seed, continuation)
		})
	}
}
