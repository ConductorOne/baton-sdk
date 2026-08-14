//go:build !baton_lockchecks && !race

package pebble

// The deadlock-shape checks are compiled out: every gated branch in
// write_barrier_owner.go and admission.go is dead code under this
// constant and the tracking costs nothing. See lock_checks_enabled.go
// for what the checks are and which invocations arm them.
const writeBarrierOwnerChecks = false
