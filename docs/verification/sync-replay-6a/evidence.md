# Sync Replay Phase 6a Verification Evidence

Plan: [`plan.md`](plan.md)

Branch evidence revision: `257eccd6`

## Signoff scope

Phase 6a storage and dotc1z capability behavior is verified except where this
record explicitly marks a criterion incomplete, excluded, sampled, or deferred.
Syncer/checkpoint orchestration, compatibility matching/gating, connector
continuation/RPC behavior, invalidation policy, compacted/non-FULL eligibility,
compactor integration, and post-replay ingest-invariant evaluation remain outside
this stage.

## Criterion evidence

- C01–C04 — verified across ordinary and conditional/newer resources,
  entitlements, and grants:
  `TestVerificationSourceScopeMutationTransitions`,
  `TestVerificationSourceScopeMutationAtomicity`,
  `TestVerificationIfNewerSourceScopeTransitions`,
  `TestVerificationMalformedGrantDeleteCleansSourceScopeIndex`, and
  `TestVerificationSourceScopeAuditorMutationAdequacy`.
- C05–C09 — verified by the all-kind manifest partition/overwrite matrix,
  selection/prefix isolation tests, direct typed-materialization differential, and
  committed replay idempotency coverage. P2 uses documented symmetry and
  representative operation models rather than a literal Cartesian expansion.
- C10–C12 — verified by all-kind replay commit failure/retry, terminal-manifest
  failure, the 10,001-row chunk/error/cancel/read-failure hard-reopen test,
  deterministic batch high-water telemetry, bounded scoped-tombstone retry, and
  the 1k/10k/100k benchmark. Benchmark results are measured sampling.
- C13–C14 — verified by direct typed-materialization differential, distinct
  entitlement structural identities, and grant source/expansion side-state
  regressions.
- C15–C17 — verified by all-kind canonical tombstone idempotency,
  grant-principal/resource-ID tests, combined selector ordering, and bounded
  interrupted scoped-delete retry.
- C18 — verified by `TestVerificationResetRemovesSourceCacheFamilies`.
- C19 — evidence incomplete at the scoped bulk boundary. Scoped v2 bulk input is
  not representable and is an executable exclusion. Existing broad bulk-import
  tests cover unscoped finish, abort, failure, and dedupe behavior.
- C20 — verified: normal Pebble stores expose the complete optional capability and
  SQLite exposes clean capability absence.
- C21 — measured schedule sampling under `-race`; no bounded schedule closure is
  claimed.
- C22 — verified for the critical source-scope biconditional mutants and supported
  by actual pre-fix red tests for manifest authorization, invalidation, aliasing,
  occupied-destination replacement, tombstone atomicity, empty validation, and
  stale-index classes. This does not claim one synthetic mutant for every prose
  example in the frozen catalog.
- C23–C27 — verified by source-envelope SHA-256, all-kind encoded timestamp
  preservation and forward replay, manifest failure, deterministic per-row source
  errors, cancellation, hard reopen, and retry. Fuzz/soak evidence is not claimed.
- C28–C29 — verified for the bounded hostile-scope corpus and independent ordered
  overlay/tombstone model. Random hostile-input fuzzing remains measured sampling,
  not closure.
- C30 — explicitly excluded: `SourceCacheCompatRecord` is schema-only in Phase 6a;
  no compatibility-family writer/key exists without implementing deferred matching
  behavior.
- C31–C33 — verified for all row kinds: occupied-scope replacement,
  missing/orphan/wrong-scope/malformed source rejection, and second-hop replay.
- C34 — explicitly deferred to syncer annotation/orchestration; the storage
  capability cannot receive the transitional annotation shape.
- C35 — verified for unsupported SQLite, corrupt envelopes, and sealed read-only
  Pebble sources. Unsealed, compacted/non-FULL, and compatibility eligibility are
  deferred because the Phase 6a capability exposes no eligibility predicate.
- C36 — verified by canonical/principal union and cross-page ordering.
- C37 — verified using a cloned read-only artifact as a replay source.
- C38 — verified at defined seams by exact iteration before seal and persisted
  sidecar statistics after seal.
- C39–C43 — verified for wrong-kind misses, invalidated rejection,
  prefix-neighbor isolation, atomic mixed tombstone rejection, exact-handle
  self-replay, same-path replay, and symlink aliases.

## Executable exclusions

`TestVerificationPhase6aExecutableExclusions` records:

- scoped bulk input unavailable at the Phase 6a API boundary;
- compatibility record lifecycle unavailable without deferred compatibility
  behavior;
- transitional `overlay=false` annotation behavior owned by syncer orchestration;
- compacted/non-FULL and compatibility eligibility deferred;
- unsealed-source policy deferred to the production previous-artifact lifecycle
  owner.

An exclusion is not a behavioral pass.

## Evidence commands

The final feature evidence includes:

```text
make lint
go test ./pkg/sourcecache ./pkg/dotc1z/engine/pebble ./pkg/dotc1z
go test -race ./pkg/dotc1z ./pkg/dotc1z/engine/pebble -run '^TestVerification' -count=1
go test ./...
```

After CO-003, the broad run overlapped another broad test process. Every package
except `pkg/sync` passed; `pkg/sync` alone hit its 10-minute package timeout.
The uncontended rerun passed:

```text
go test ./pkg/sync -count=1
```

The same repository-wide suite had passed before CO-003, and CO-003 does not touch
`pkg/sync`.

## Performance evidence

The replay benchmark sampled 1,000, 10,000, and 100,000-row scopes. Time and
allocation growth remained approximately linear. Positive source-integrity
preflight increased the 100,000-row sample because the current manifest has no row
count or digest; the separate scope-count change order addresses the resulting
many-scope O(S·N) cost.

Deterministic tests, not the benchmark, establish the 10,000-operation replay,
replacement, and scoped-tombstone batch bounds.

## Process corrections

- The first completion report incorrectly treated “every criterion accounted for”
  as full closure. The criterion map above now names partial evidence and exclusions.
- Repository lint was initially omitted from signoff; `make lint` subsequently
  found and drove correction of 21 issues.
- A post-closure review found unbounded scoped tombstone batches; CO-003 added the
  bounded implementation and interrupted-retry evidence.
