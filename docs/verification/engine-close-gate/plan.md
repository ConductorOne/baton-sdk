# Verification plan: pebble engine close gate (admission) — review-closure stage

Stage of the `kans/engine-deadlock-fixes` series (PR #1088). Earlier commits
on the branch introduced the read pin, the write-side close drain, the
owned-mutex diagnostics, and consolidated seven concurrency fields into one
`admission` type. This stage closes the findings of a three-reviewer code
pass over that consolidated state. Proportionality per BUG_CATCHING §"keep
the machinery proportional": this packet covers the delta, not a re-plan of
the whole subsystem; the enduring enforcement lives in the repository tests
named below, which survive this document.

## Contracts under verification

- C1 (gate soundness): no code path reads the engine's `db` handle without
  holding gate admission — a read pin, an admitted write, lifecycle
  machinery that is itself the admission, Open-time code before the engine
  is shared, or the merge surface's documented exclusion.
- C2 (drain correctness): `Close` waits for every admitted operation and
  runs the teardown exactly once; an admission attempt concurrent with the
  closing flip either completes against a live handle or is refused with
  `ErrEngineClosing`. Draining must tolerate concurrent enter attempts
  (the `sync.WaitGroup` Add-vs-Wait misuse is structurally excluded:
  the gate counts under a mutex and signals on a condition variable).
- C3 (lifecycle transitions): all five transitions (StartNewSync,
  ResumeSync, SetCurrentSync, CheckpointSync, EndSync) run as admitted
  writes, assert the write barrier is not held before taking
  `lifecycleMu`, and validate state under the lock (no check-then-bind
  TOCTOU against a concurrent wipe).
- C4 (no leaked admissions): every pin's release is deferred; an early
  return cannot strand a counter and wedge Close.
- C5 (bounded pins): every pinned iterator scan checks `ctx.Err()` per
  iteration, including the seek-driven distinct-referent shape.
- C6 (armed builds): correctness-focused Make targets compile the
  deadlock-shape checks in, either via `-race` or `baton_lockchecks`;
  measurement targets stay unarmed, stated per target.

## Coverage model

Mechanical, not sampled:

- C1: `TestBareHandleAccessIsGateCovered` (AST) enumerates every `e.db` /
  `*.e.db` selector in the package and requires a withWrite literal or a
  justified allowlist entry. `TestAdmissionUsedOnlyThroughItsMethods` pins
  the gate's internals to `admission.go`.
- C2: `admission_test.go` unit-hammers the gate directly
  (`TestAdmissionDrainWritesToleratesConcurrentEnters`,
  `TestAdmissionEnterNeverTripsDrainingWaitGroup`);
  `TestCloseWaitsForInFlightAdmission` white-boxes the flip/enter race;
  `TestConcurrentCloseWithPaginatedReads` and the write-side reentry tests
  hammer the engine end to end under `-race`.
- C3: `TestLifecycleMuTakersAreTransitionsOnly` (AST) enforces the
  transition set and the assert-before-lock ordering by token position.
- C4: `TestPinnedReadsDeferTheirRelease` (AST) covers every `pinRead`
  call site.
- C5: `TestScanReadsArePinned` (AST) covers both iterator-loop shapes
  (`iter.Valid()` conditions and seek-driven bool conditions).
- C6: reviewed target-by-target in the Makefile; the arming policy is
  written beside the targets.

## Closure

Closure for this stage is: the six meta/unit instruments above pass, the
package suite passes under `-race -tags=baton_lockchecks`, and each
instrument has been shown live by mutation (see evidence.md). Deferred
beyond this stage: object-tied leases for the merge surface (documented
exclusion instead — see merge_surface.go header), and any redesign of the
bare-ID lookup's O(all grants) fallback (pre-existing, documented).
