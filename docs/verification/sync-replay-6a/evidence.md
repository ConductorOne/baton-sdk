# Sync Replay Phase 6a Verification Evidence

Plan: [`plan.md`](plan.md)

Verified implementation and instrument state: current uncommitted working tree
based on `1e8cc1b3`. Replace this line with the final commit SHA before repository
signoff.

Included change orders: CO-003a and CO-005 through CO-010b.

## Signoff scope

Phase 6a storage and dotc1z capability behavior is verified except where this
record explicitly marks a criterion incomplete, excluded, sampled, or deferred.
Syncer/checkpoint orchestration, compatibility matching/gating, connector
continuation/RPC behavior, invalidation policy, compacted/non-FULL eligibility,
compactor integration, and post-replay ingest-invariant evaluation remain outside
this stage.

## Criterion evidence

- C01 — **verified**. Scoped/unscoped resources, entitlements, grants, and
  synthesized paths are checked by `TestVerificationSourceScopeMutationTransitions`
  and the typed-operation coverage suite. Oracle: raw row stamp plus O4.
- C02 — **verified**. The full absent/A/B/unscoped/delete transition table, ordinary
  and IfNewer paths, and malformed all-kind deletes pass
  `TestVerificationSourceScopeMutationTransitions`,
  `TestVerificationIfNewerSourceScopeTransitions`, and
  `TestVerificationMalformedAllKindDeleteCleansSourceScopeIndex`. CO-009 additionally
  consumes every sibling empty-keyspace proof on conservative rebind.
- C03 — **verified**. `TestVerificationSourceScopeMutationAtomicity` injects the
  shared typed-commit failure for fresh puts, moves, and deletes for all kinds and
  compares the incumbent row/index state.
- C04 — **verified**. Typed-operation descriptor coverage, direct-materialization
  index comparisons, grant expansion regressions, and all-kind malformed cleanup
  cover the obligations actually owned by each row kind. O4 is independently
  validated by `TestVerificationSourceScopeAuditorMutationAdequacy`; CO-009's
  colliding writes prove stale fast-path state cannot bypass old-index cleanup.
- C05 — **verified**. `TestVerificationManifestPartitionAndOverwriteMatrix` covers
  all kind/scope overwrite cells; C25/C40 supply 0/1/many states. Row cardinality
  is reduced because manifest write uses one shared `(kind,scope)` choke point and
  does not inspect rows.
- C06 — **verified**. All-kind direct differential, hostile-scope, and bidirectional
  prefix-neighbor tests assert exact selected scope/kind sets and decoy survival.
- C07 — **verified**. `TestVerificationReplayRejectsStampedRowsWithoutManifest`
  covers all kinds with occupied destinations and full before/after engine digests.
- C08 — **verified** at the CO-002 reduced cells. Replay/direct primary and maintained
  index families are compared for every kind; overlay identity cases separately
  cover distinct and colliding entitlement identities.
- C09 — **verified**. Sequential replay and tombstone retries converge, including
  occupied replacement, duplicate/unknown tombstones, and interrupted-prefix retry.
- C10 — **verified**. Production-size resources exercise each 10,000-row boundary;
  `TestVerificationReplayCommittedPrefixRetryAllKinds` exercises a committed cut,
  hard reopen, and retry for every kind; manifest and public dirty-lifecycle
  failures are separate instruments. CO-009 adds the distinct destructive-clear
  commit loop and consumes proof state before returning a later error.
- C11 — **verified**. Replay, overlay, and tombstone outcomes for every kind are
  byte-compared across an immediate hard reopen; zero-row replacement,
  failure-prefix persistence, clone state, timestamps, manifests, and final stats
  add public/lifecycle coverage. CO-010 proves that zero-primary-row defensive
  index healing also survives the public close/reopen lifecycle; CO-010a proves
  concurrent Close cannot checkpoint between public dirty marking and engine entry.
- C12 — **verified** structurally and by measured sampling. Replay and scoped-delete
  high-water hooks pin bounded batches; the 1k/10k/100k benchmark is linear sampling,
  not exhaustive performance proof.
- C13 — **verified**. Occupied all-kind replacement and both entitlement identity
  cases compare direct/replay outcomes and assert stale-index absence. CO-009
  plants an armed proof after occupied replacement, fails after a committed clear
  prefix, and requires the surviving colliding identity to clean its old index.
- C14 — **verified** at descriptor-selected owned fields. Direct/replay encoded-row
  comparisons and grant source/expansion tests cover the maintained side state;
  timestamp provenance is handled by C26.
- C15 — **verified**. Canonical tombstone success, duplicate, unknown, repeated, and
  mixed-rejection cells cover all row kinds with primary/index postconditions.
- C16 — **verified**. Grant principal and resource-ID scoped selectors delete exact
  matches and preserve scope/type decoys; entitlement principal selectors are the
  invalid C42 cell. CO-010 covers orphan-only healing for both supported selector
  families and the grant-external-ID variant.
- C17 — **verified**. `TestVerificationCanonicalTombstoneIdempotencyMatrix` and
  scoped-delete retry cover duplicate, repeated, and unknown selectors.
- C18 — **verified**. `TestVerificationResetRemovesSourceCacheFamilies` asserts every
  new source-index and manifest family is absent after populated-to-empty reuse.
- C19 — **explicitly excluded** for scoped input; unscoped finish/abort/failure and
  dedupe remain covered by the broad bulk-import suite. The executable exclusion
  records that Phase 6a bulk input cannot represent source scope or manifests.
  CO-009 nevertheless verifies that successful ingest invalidates all three
  sibling empty-keyspace proofs.
- C20 — **verified**. Normal Pebble open exposes the complete capability; SQLite
  exposes clean absence and fails direct replay loudly.
- C21 — **verified at its declared measured level**. Duplicate concurrent replay
  converges under the race command and matches a separately direct-materialized
  semantic row/manifest/index snapshot; no bounded schedule claim is made.
- C22 — **verified for the in-scope catalog**. Biconditional and manifest
  reconcilers have physical planted violations; auxiliary validators reject
  timestamp swaps, over-limit batches, stale counters, premature manifests, source
  digest changes, and prefix-neighbor deletion. Error, corruption, invalidation,
  dirty-merge, and forward-stamp cases use actual injected or pre-fix red paths.
  CO-007 defers the wrong-page-order mutant with C29. CO-009 asserts the proof bit
  was armed before each lifecycle transition and uses a colliding-write O4 oracle;
  CO-010 plants physical orphan indexes and observes live and durable healing.
  CO-010a proves mutation lock ownership and that Close reached the competing lock
  attempt before releasing the engine-entry seam.
- C23 — **verified**. Full semantic key/value snapshots cover all-kind engine
  outcomes and corrupt-source rejection.
  `TestVerificationSourceArtifactDigestAllKindOutcomes` additionally compares the
  sealed c1z SHA-256 after success, injected post-commit wrapper failure,
  cancellation, and retry for every kind.
- C24 — **verified**. `auditTerminalManifestReconciliation` checks physical
  manifest-cell identity, replayable ownership for every stamped all-kind scope,
  and valid zero-row manifests together with O4. Independent expected row counts
  cover all-kind replay, overlay, tombstone, and failed-manifest outcomes; the
  missing-owner mutant goes red.
- C25 — **verified for the Phase 6a API boundary**. Every kind loudly rejects empty
  creation and replacement, raw storage remains absent/unchanged, and the prior
  completed entry survives. CO-010b additionally proves rejection leaves a clean
  resumed wrapper clean and retains validation precedence after Close. Page-level
  transitional empty validators are an executable orchestration exclusion.
- C26 — **verified under CO-006**. Independent all-kind source, overlay, and current
  manifest-write sentinels are checked across replay, replacement retry, and reopen.
- C27 — **verified**. Production-size resources pin the real 10,000-row boundary;
  every kind additionally sweeps deterministic read-error and cancellation cuts
  before the first row, inside chunks, at chunk boundaries, and before final commit,
  preserving error identity, O4, source snapshots, and retry convergence. CO-009
  separately injects the destructive-clear loop after one landed batch.
- C28 — **verified at bounded corpus plus measured sampling**. The deterministic
  corpus covers empty, oversized, separator, prefix, embedded-NUL, Unicode,
  max-length, malformed-ID, duplicate-ID, and invalid-kind input. All kinds cover
  shared scope validation plus byte-exact replay/tombstones for opaque empty, NUL,
  normalization-neighbor, and long IDs; grant sampling supplements the shared
  tuple codec. Invalid UTF-8 and an empty-resource canonical BID are executable
  unrepresentability cells under CO-008.
- C29 — **deferred** by CO-007. No in-scope Phase 6a component owns page ordering;
  directly calling storage methods in a chosen order is not evidence for this
  criterion.
- C30 — **explicitly excluded**. `SourceCacheCompatRecord` is schema-only in Phase
  6a; no Pebble compatibility family exists to exercise without implementing
  deferred matching behavior.
- C31 — **verified**. Occupied destinations for all kinds are replaced exactly;
  decoy scopes survive and source-scope, grant-principal, and resource-parent stale
  indexes are explicitly counted. CO-009 verifies committed clear accounting and
  proof invalidation before a later clear error.
- C32 — **verified**. The all-kind missing/orphan/wrong-scope/malformed matrix seeds
  the exact destination scope and compares complete source/destination snapshots;
  manifest-only zero-row scopes remain valid.
- C33 — **verified**. Every kind completes zero-row, populated, overlay, and
  tombstoned two-hop replay through the public capability. B and C both match an
  independently direct-materialized canonical row/manifest/index snapshot, and the
  B source digest is unchanged by the second hop.
- C34 — **explicitly excluded**. The storage capability cannot receive the
  transitional page annotation; ownership is assigned to deferred syncer
  orchestration.
- C35 — **verified where representable; explicitly excluded elsewhere**. Unsupported
  SQLite rejects against an occupied destination with an unchanged digest; corrupt
  envelope and sealed read-only source cells fail/pass as declared. Unsealed,
  compacted/non-FULL, and compatibility eligibility require the deferred lifecycle
  owner and predicate.
- C36 — **verified for storage composition, not ordering**. Grant and resource tests
  include canonical/principal overlap, selector-only matches, and survivors;
  entitlement principal selection is rejected by C42. C29 ordering is not claimed.
- C37 — **verified**. `CloneSync` and `CopyIsolateSync` preserve complete all-kind
  semantic/obligation snapshots, manifests, and source artifact digest; each
  read-only result replays through the public capability. `GenerateSyncDiff` is an
  executable partial-sync exclusion.
- C38 — **verified**. Exact resource/entitlement/grant stats are asserted after
  occupied replacement, overlay, tombstones, retry, seal, and hard reopen.
- C39 — **verified**. All six ordered pairs of distinct row kinds use the same scope
  bytes, fail closed, and preserve complete source/destination snapshots.
- C40 — **verified**. All kinds at 0/1/many rows treat invalidated entries as misses,
  reject replay, and preserve occupied destination and source digests; valid and
  absent D26 cells are covered by C06/C07.
- C41 — **verified**. All kinds exercise both `foo`/`foobar` directions and both
  embedded-NUL directions through lookup, occupied replacement, replay, and
  applicable tombstones.
- C42 — **verified**. Canonical selectors cover every kind, principal selectors
  cover resources/grants, entitlement principal selection rejects, and mixed
  valid+invalid or ambiguous batches preserve full before/after state atomically.
- C43 — **verified**. Exact writable handle, two opens of the same path, and symlink
  aliases reject before mutation. Immediate occupied-destination/source digests
  remain unchanged, and subsequent valid replay proves non-poisoning.

## Executable exclusions

`TestVerificationPhase6aExecutableExclusions` records:

- scoped bulk input unavailable at the Phase 6a API boundary;
- page-level transitional empty validators unavailable at the storage boundary;
- invalid UTF-8 protobuf IDs and an empty-resource canonical BID are
  unrepresentable while the underlying opaque resource row remains replayable;
- compatibility record lifecycle unavailable without deferred compatibility
  behavior;
- transitional `overlay=false` annotation behavior owned by syncer orchestration;
- page upsert/tombstone ordering owned by deferred syncer orchestration;
- `GenerateSyncDiff` produces a partial sync rather than a standalone full-sync
  replay source;
- compacted/non-FULL and compatibility eligibility deferred;
- unsealed-source policy deferred to the production previous-artifact lifecycle
  owner.

An exclusion is not a behavioral pass.

## Independent implementation-obligation review

A reader independent of the implementation and instrument author audited resource
ownership, typed batches, proof/cache state, distinct commit loops, public dirty
lifecycle, bulk ownership, reset/open/close/checkpoint, sidecars, and FileOps. The
criterion-form addendum is appended to `plan.md`.

The initial focused implementation review passed with zero new HIGH findings and
found one LOW defensive persistence defect: orphan-only source-scope index healing
committed inside the engine while public wrappers remained clean because zero
primary rows were deleted. CO-010 added the failing-first public lifecycle
instrument and correction.

The required re-review of that correction found a HIGH atomicity defect between
public dirty marking and entering the engine writer barrier: concurrent `Close`
could checkpoint between the persistence claim and a later successful mutation.
CO-010a serializes every public source-cache mutation with `Close` across that
entire handoff and adds a deterministic close-attempt/lock-ownership/reopen oracle.
Re-review of CO-010a passed the zero-HIGH gate and found one LOW validation-order
regression: an empty manifest validator entered the lifecycle boundary before the
engine rejected it. CO-010b restores wrapper-owned input validation before dirty
state or lifecycle checks. Final re-review found no remaining HIGH, MEDIUM, or LOW
implementation findings and **passed the focused implementation-review gate**.

The review also recorded these non-defect limits:

- fold barrier ordering and entitlement-cache invalidation belong to deferred
  compactor integration;
- scoped rows/manifests remain unrepresentable through bulk input;
- many-scope source preflight remains the sampled performance limitation assigned
  to CO-004.

## Evidence commands

The final feature evidence includes:

```text
make lint
go test ./pkg/sourcecache ./pkg/dotc1z/engine/pebble ./pkg/dotc1z
go test -race ./pkg/dotc1z ./pkg/dotc1z/engine/pebble -run '^TestVerification' -count=1
go test ./...
```

The current uncommitted working tree was rerun after CO-010b and the final clean
implementation re-review on 2026-07-29. All of these commands passed:

```text
make lint
go test ./pkg/sourcecache ./pkg/dotc1z/engine/pebble/internal/rawdb ./pkg/dotc1z/engine/pebble ./pkg/dotc1z -count=1
go test -race ./pkg/dotc1z ./pkg/dotc1z/engine/pebble -run '^TestVerification' -count=1
go test -p 1 ./... -count=1
```

The addendum's replay-versus-close scheduling gap was then closed by a deterministic
commit-barrier instrument; both ordinary and race runs passed:

```text
go test ./pkg/dotc1z/engine/pebble -run '^TestVerificationReplayWriteBarrierDrainsBeforeClose$' -count=1
go test -race ./pkg/dotc1z/engine/pebble -run '^TestVerificationReplayWriteBarrierDrainsBeforeClose$' -count=1
```

CO-010a's public handoff and CO-010's orphan-healing lifecycle instruments passed
ordinary and race runs:

```text
go test ./pkg/dotc1z -run '^TestVerification(OrphanScopeIndexHealingPersistsAfterReopen|SourceCacheMutationHandoffToConcurrentClose)$' -count=1
go test -race ./pkg/dotc1z -run '^TestVerification(OrphanScopeIndexHealingPersistsAfterReopen|SourceCacheMutationHandoffToConcurrentClose)$' -count=1
```

CO-010b's validation-order regression passed together with the adjacent lifecycle
instruments:

```text
go test ./pkg/dotc1z -run '^TestVerification(EmptyValidatorDoesNotPublishManifest|OrphanScopeIndexHealingPersistsAfterReopen|SourceCacheMutationHandoffToConcurrentClose)$' -count=1
```

Two default-parallel broad runs failed only
`pkg/dotc1z.TestC1ZConcurrentClose`, whose SQLite WAL-close assertion is outside
the Phase 6a Pebble/source-cache delta and is sensitive to repository-wide package
load. Ten isolated repetitions passed, as did the complete serial-package broad
suite above:

```text
go test ./pkg/dotc1z -run '^TestC1ZConcurrentClose$' -count=10
```

This is a repository-signoff flake disclosure, not passing evidence for the
default-parallel command.

The scope-isolation fuzz command previously passed on the CO-005 through CO-008
working tree:

```text
go test ./pkg/dotc1z -run '^$' -fuzz '^FuzzVerificationScopeEncodingIsolation$' -fuzztime=10s
```

After CO-003, the broad run overlapped another broad test process. Every package
except `pkg/sync` passed; `pkg/sync` alone hit its 10-minute package timeout.
The uncontended rerun passed:

```text
go test ./pkg/sync -count=1
```

The same repository-wide suite had passed before CO-003, and CO-003 does not touch
`pkg/sync`.

CO-003a evidence passed without committing the working tree:

```text
make lint
go test ./pkg/dotc1z/engine/pebble -run '^TestVerificationScopedDeleteBatch(BoundAndInterruptedRetry|FinalCloseOwnership)$' -count=1
go test -race ./pkg/dotc1z/engine/pebble -run '^TestVerificationScopedDeleteBatch(BoundAndInterruptedRetry|FinalCloseOwnership)$' -count=1
```

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
- Focused review of CO-003 then found a stale pointer retained after returning the
  final Pebble batch to its process-global pool. CO-003a makes ownership
  nil-or-exclusive and adds an explicit final-close lifecycle assertion.
- A later implementation read found stale empty-keyspace proofs at rebind,
  bulk-finish, and committed-clear failure boundaries. CO-009 treats derived state
  as a proof, crosses the lifecycle with every sibling family, and uses a
  premise-armed colliding write rather than relying on durable key auditors.
- The implementation-obligation addendum was then generated and independently
  reviewed as a required deliverable rather than received piecemeal after closure.
  Its initial gate passed with zero new HIGH findings; its one LOW orphan-healing
  persistence finding became failing-first CO-010 evidence.
- Required re-review of CO-010 found a HIGH mutation-to-close handoff defect in the
  correction's pre-boundary dirty-marking approach. CO-010a moves the persistence
  claim and engine mutation into one store-close critical section and adds a
  deterministic concurrent-close oracle.
- Re-review of CO-010a passed the zero-HIGH gate and found one LOW validation-order
  regression. CO-010b validates empty manifest validators before dirty/lifecycle
  entry and pins clean-state and post-Close error precedence.
