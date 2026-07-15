//go:build race

package dotc1z

// raceEnabled reports whether this test binary was built with -race.
// sync.Pool intentionally randomizes item retention under the race
// detector (Put may discard), so tests asserting pool REUSE are
// structurally flaky there and must skip that assertion.
const raceEnabled = true
