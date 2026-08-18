//go:build baton_lockchecks || race

package pebble

// writeBarrierOwnerChecks turns on the deadlock-shape checks in
// write_barrier_owner.go and admission.go: barrier re-entrancy, waiting
// on your own write or pinned read, and lifecycle transitions from
// inside a write body. A compile-time constant, pebble's own invariants
// pattern, so
// the disabled build carries none of the bookkeeping — knowing which
// goroutine holds what means formatting a runtime stack (~2µs against a
// ~7µs grant write), which is the wrong trade everywhere the checks are
// not wanted: production binaries, and benchmarks, whose numbers would
// otherwise measure the check instead of the write and only on the
// Pebble side of every Pebble-vs-SQLite comparison.
//
// The race tag arms them for free in any `-race` invocation — cmd/go
// sets it automatically — so the race-based Makefile targets need no
// opt-in. Everything else gets them from -tags=baton_lockchecks, which
// `make test` and the CI workflows supply; TestLockChecksCompiledIn and
// TestLockChecksSuppliedByTestInvocations exist to make forgetting that
// loud.
const writeBarrierOwnerChecks = true
