# Sync Replay Phase 6a Verification Evidence

Plan: [`plan.md`](plan.md)

Closure implementation and instrument state:
`c913dbc174644d7bff10449597f57d6c8cbbdba3`. The CO-012a final gates below were
rerun against that committed SHA on 2026-07-30. Two independent final-code
re-reviews accepted closure with the explicit limitations below.

Included change orders: CO-003, CO-003a, CO-005 through CO-010b, and CO-011
through CO-013.

CO-013's final remediation is committed at
`8f507e1606c92e87bbb4f3966a4600989df90809`. The predecessor `c28887c9` passed
the broad final gates on 2026-07-31, after which three independent
implementation/evidence audits returned two REJECT verdicts and one ACCEPT WITH
EXPLICIT LIMITATIONS. They found a missing compacted projection in
`ListSyncRuns`, a vacuous post-expansion grant-index fixture, and stale
criterion/exclusion text. Commit `8f507e16` corrects those findings. Two fresh,
independent focused reviews both returned ACCEPT WITH EXPLICIT LIMITATIONS, and
the remediation's affected, lint, and race gates pass. Phase 6a is closed at
`8f507e16` with the explicit limitations retained below.

## Signoff scope

Phase 6a is signed off at `8f507e16` with explicit limitations. This record marks
incomplete, excluded, sampled, and deferred criteria rather than converting them
into behavioral passes.
Syncer/checkpoint orchestration, compatibility matching/gating, connector
continuation/RPC behavior, and post-replay ingest-invariant evaluation remain
outside this stage. CO-013 implements the previously deferred compacted/non-FULL
source eligibility and compactor source-cache invalidation policy.

The repeated final-code audits reopened closure, leading to CO-012 and CO-012a.
The corrected implementation rejects unfinished sources, propagates owned
source-close errors, removes unpublished run files on close failure, and narrows
the evidence claims. Final gates pass and both independent re-reviews accepted the
result with explicit limitations.

## CO-013 remediation evidence

The working-tree remediation:

- releases grant `Get` closers on marshal failure through per-record immediate
  defers, with invalid UTF-8 forcing both exact branches;
- persists and exports the compacted marker, then rejects partial and compacted
  sources in both syncer selection and public replay before destination mutation;
- drops manifests only for fold and drops manifests plus source-scope indexes for
  k-way/overlay, including a second finalization after grant expansion; and
- projects the durable compacted bit into the v3 envelope's sync-run summary, so
  `ReadManifestHeader` exposes it without payload unpack or zstd decode; and
- preserves that bit in both public sync-run projections, so generic metadata
  consumers cannot misclassify compacted FULL runs as replay-eligible; and
- registers the new typed-batch commit point and plants its exact failure,
  requiring atomic preservation of manifests and indexes.

The exact committed candidate `c28887c9` passed:

```text
make lint
buf lint
go test -race ./pkg/dotc1z ./pkg/dotc1z/engine/pebble ./pkg/synccompactor/pebble ./pkg/synccompactor -run '<closure instruments>' -count=1
go test -p 1 ./... -count=1
go test -timeout 30m ./pkg/dotc1z -run '^TestModelRandomizedSourceCacheLifecycle$' -count=20
go test -timeout 30m ./pkg/sourcecache ./pkg/dotc1z/engine/pebble ./pkg/dotc1z -coverprofile=/tmp/sync-replay-6a-co013.cover -count=1
go tool cover -func=/tmp/sync-replay-6a-co013.cover
```

The profile reported 70.5% combined statement coverage. The first model-soak
invocation shared the machine with the race and serial gates and hit Go's default
10-minute process timeout while still progressing; the serial rerun with the
30-minute closure allowance passed.

Fold's invalidation is one `NoSync` range tombstone and does not enumerate
manifests or delete source-scope indexes. A five-sample, one-iteration comparison
of the existing skewed 10-sync fold benchmark against detached HEAD used:

```text
go test ./pkg/synccompactor -run '^$' \
  -bench '^BenchmarkCompactorSQLiteVsPebbleSkewed/syncs=10/pebble_fold$' \
  -benchtime=1x -count=5 -benchmem
```

The working-tree median was 0.881 s and 42,719 allocs versus detached HEAD at
1.115 s and 42,520 allocs. The one-iteration timing is noisy and is not claimed as
a speedup; it shows no fold-time or allocation explosion, while the code shape
proves invalidation cost is independent of base row count.

Post-gate independent review found that the compaction invalidation test populated
only the resource family, so it did not prove the second post-expansion
invalidation removed a restaged grant scope index. The corrected fixture now
contains scoped resource, entitlement, and expandable-grant rows and asserts all
three source-scope index families after real expansion. The same remediation adds
the missing `ListSyncRuns` compacted projection and reconciles C35/exclusions
below.

The complete affected suites and lint passed against the tracked remediation
content:

```text
go test ./pkg/dotc1z/engine/pebble ./pkg/sync ./pkg/synccompactor/...
make lint
```

At exact commit `8f507e16`, the focused race gates also passed:

```text
go test -race ./pkg/dotc1z/engine/pebble \
  -run '^(TestManifestSyncRunProjection|TestVerificationPhase6aExecutableExclusions)$' -count=1
go test -race ./pkg/sync ./pkg/synccompactor \
  -run '^(TestPreviousSyncC1ZPathEnforcesReplayEligibility|TestCompactPebbleInvalidatesSourceCacheReplayState)$' -count=1
```

The two independent focused re-reviews found no production regression. One
accepted with no findings; the other retained LOW limitations that the
post-expansion restage proof is contract-backed rather than observed between the
expansion and final invalidation calls, and that this evidence-only signoff update
must name the final SHA. The SHA is pinned above; the instrument proves a real
expanded collision (`Sources` populated) and final absence of all three
source-scope index families.

## CO-014/CO-015/CO-016 remediation evidence

The working-tree remediation:

- adds presence-tracked `row_count` to `SourceCacheEntryRecord`; EndSync's
  finalize recounts stamped primaries per `(row_kind, scope)` from the primary
  keyspace (never the index) and seals the counts before the `ended_at` stamp;
  rebinding a completed sync durably clears counts (read-only opens skip the
  clear) so an unpublished rebound store stays fail-closed until it reseals —
  the strip commits in bounded pages (intermediate pages NoSync, the final
  page synced, which persists the WAL prefix covering every earlier one) so
  memory and batch size are bounded by the page rather than the manifest,
  with a partially stripped crash image fail-closed in both directions
  (bind never returned, so no mutation was admitted: stripped entries are
  hard preflight errors, still-counted entries remain accurate witnesses) —
  and manifest writes reject unknown row kinds at the engine (the seal pass
  hard-errors on kinds it cannot count and entries are individually
  undeletable, so an unvalidated kind would make the artifact unsealable);
- replaces the O(S·N) preflight primary scan with a scope-bounded index walk
  validated entry-by-entry against primary stamps and, in aggregate, against
  the sealed count; a replay-eligible entry without a count is a hard error,
  as is a cardinality mismatch, each refusing before destination mutation;
- stages a durable per-scope poison marker in the same record batch as any
  mutation that removes a row from another scope's stamped set — cross-scope
  restamps, stamp-clearing unscoped overwrites, and deletes acting outside
  the row's scope (record batches carry an acting scope, defaulting to
  unscoped, so maintenance deletes including external-principal
  reconciliation poison conservatively); scoped tombstone paths and replay
  destination clearing act as their own scope and never self-poison;
- rejects poisoned scopes at replay preflight before destination mutation,
  treats them as lookup misses in `LookupSourceCacheEntry`, and logs poison
  staging post-commit with scope, kind, and cause — marker staging dedups
  per batch, and the engine's observer dedups the WARNING per
  `(row_kind, scope_key)` per open, so a persistently mis-partitioned
  connector (the reconciliation shape included) logs once per poisoned
  scope per artifact rather than once per 10k-row chunk;
- keeps poison markers inside the source-cache family bounds so compaction
  replay-state invalidation range-deletes them with the manifests; and
- bounds the canonical-ID tombstone paths with the same chunked commits as
  the scoped tombstone paths and threads the acting scope through
  `SourceCacheStore.DeleteSourceCacheRows`.

The instruments passed focused runs in the working tree:

```text
go test ./pkg/dotc1z/engine/pebble -run '^Test(VerificationPoisonCrossScopeRestampOrders|VerificationPoisonAllKindRestampAndUnscopedDelete|VerificationPoisonCrossScopeCanonicalTombstone|VerificationPoisonScopedTombstonesDoNotSelfPoison|VerificationPoisonReconciliationShapeRefusesReplay|VerificationPoisonSurvivesReopenAndInvalidationDropsIt|VerificationPoisonEventsAreLogged|VerificationSealedCountPreflightGates)$' -count=1
go test ./pkg/dotc1z -run '^TestVerificationPoisonedScopeIsLookupMiss$' -count=1
```

The poison instruments were written failing-first against the pre-poison
tree: every refusal case then completed as a silent replay of the shrunken
row set, confirming that restamps and deletes leave index, stamps, and
sealed count self-consistent with the damaged partition and only a marker
staged at mutation time can catch the loss. The reconciliation-shaped
instrument pins the CO-016 boundary: unscoped identity-deletes of a scope's
placeholder grants poison exactly the grant scope while the same sync's
resource and entitlement scopes stay replayable. A three-model independent
review (plus verification of each finding) added two direct pins the first
pass left transitive: a failed batch commit delivers no poison event and no
durable marker (the telemetry test's injected-commit-failure leg), and a
same-scope A→A rewrite does not false-positive poison (replay asserted
after the rewrite in the mutation-transitions test).

The CO-004 closure-gate benchmark holds total grant rows fixed at 4096 and
varies the partition into 1, 16, and 256 scopes, replaying every scope:

```text
go test ./pkg/dotc1z/engine/pebble -run '^$' -bench '^BenchmarkReplayPreflightScopeScaling$' -benchtime=1x -count=3
```

Medians were 52.7 ms (1 scope), 81.3 ms (16), and 70.9 ms (256): flat from
16 to 256 scopes, where the deleted O(S·N) preflight would have rescanned
all primaries once per scope (~16x the preflight work). The step from 1 to
16 is fixed per-scope replay overhead (batch mint and manifest round
trips), not primary-scan growth. The independent biconditional auditor
remains count-unaware.

### PR-review remediation round (post-landing incremental review)

The incremental CI review of the CO-014/CO-015 commit confirmed the O(S·N)
preflight resolved and raised four suggestions; one older open thread was
also picked up. All five landed together:

- **Per-chunk lookup invalidation.** `DeleteEntitlementRecords` bumped the
  bare-id lookup generation only at function exit, but chunk commits mutate
  the keyspace mid-loop and `entitlementIdentitiesForExternalID` takes only
  `entIDLookupMu` — a concurrent lookup between a landed chunk and return
  served a cached map listing deleted rows. `sourceCacheDeleteBatch` now
  carries a post-commit hook that bumps as each chunk lands, and the replay
  destination clear bumps per landed entitlement chunk for the same reason.
  Pinned deterministically by
  `TestVerificationEntitlementDeleteBumpsLookupGenPerChunk`: chunk 2's
  pre-commit hook runs strictly after chunk 1 committed and must already
  observe a bumped generation.
- **Committed progress rides replay errors.** The three `ReplaySourceCache*`
  functions returned an empty result with any error even after bounded
  intermediate batches landed. On error the result now reports rows whose
  commits landed (matching the scoped-delete siblings); `NeedsExpansion`
  accumulates at stage time and may overreport a never-committed row, which
  is the safe direction (expansion is idempotent and add-only). Pinned by
  the committed-prefix retry test asserting `Rows == 2` alongside the
  injected error.
- **Paged manifest rewrites.** The seal previously retained every decoded
  manifest entry across the primary scan and the rebind clear committed the
  whole manifest as one batch — both unbounded in the scope-count dimension
  CO-3 bounded everywhere else. The seal now runs three passes (validate
  kinds, count, stream the rewrites in bounded pages over a snapshot
  iterator) holding only the counts map, which is the output and thus an
  irreducible O(scopes-with-stamped-rows); the clear pages at the same
  bound with the final page synced (see the strip bullet above for the
  crash argument).
- **Observer-level poison log dedup.** Marker staging dedups per batch, but
  batches re-mint per chunk, so the staging-level "logs once" claim held
  only within one batch. The engine's poison observer now dedups warnings
  per `(row_kind, scope_key)` per open; the durable marker stays idempotent.
  Pinned by the telemetry test's new cross-batch leg: a later batch
  re-poisoning the same scope lands the marker but logs nothing new.
- **Seal/clear cost-contract benchmarks.** EndSync's seal scan and the
  rebind clear had no enforcing benchmark (the replay benchmarks
  deliberately seal outside their timed regions).
  `BenchmarkSourceCacheSealRowCounts` times EndSync against grant-row count
  (82/113/186 ms at 2048/8192/32768 rows, `-benchtime=1x`, Apple M1 —
  linear incremental cost over fixed EndSync overhead) and
  `BenchmarkSourceCacheClearRowCounts` times `SetCurrentSync` against
  manifest size (7.3/11.1/8.3 ms at 16/256/4096 scopes — flat,
  fsync-dominated).

```text
go test ./pkg/dotc1z/engine/pebble -run '^Test(VerificationEntitlementDeleteBumpsLookupGenPerChunk|VerificationReplayCommittedPrefixRetryAllKinds|VerificationPoisonEventsAreLogged|VerificationScopedDeleteBatchBoundAndInterruptedRetry|VerificationSealedCountPreflightGates)$' -count=1
go test ./pkg/dotc1z/engine/pebble -run '^$' -bench '^BenchmarkSourceCache(Seal|Clear)RowCounts$' -benchtime=1x -count=1
```

Two review suggestions were assessed and declined: softening the batch-leak
ledger to warn-only in production (deliberate design — it matches pebble's
own always-on iterator accounting, and the store orders `save()` before
teardown so a ledger error is post-save diagnostics, not a discard
verdict), and the chaos-corpora tiering callout (a PR-description note, not
a code change).

A second incremental review confirmed all five fixes landed, independently
verified the paged rewrites safe (snapshot iterators, error-path closes,
the final-page sync argument), and flagged the round's own residue, fixed
here:

- the public `ReplaySourceCache` wrapper zeroed the engine's
  committed-progress result on error, making the new contract unreachable
  from the only surface replay orchestration uses and inconsistent with the
  delete siblings' interface docs — the result now rides the error through
  the wrapper (including the post-replay test-seam path), and the interface
  doc states the contract;
- the entitlement replay COPY loop deferred its lookup-generation bump to
  function exit — the same mid-loop stale-window the round closed for the
  delete paths and the clear half of the very same function. Each landed
  chunk now bumps, pinned by
  `TestVerificationEntitlementReplayBumpsLookupGenPerChunk` (the replay
  twin of the delete-side gen test). The pre-existing comment justifying
  exit-only bumping ("bumping earlier would let a build record the fresh
  generation against partially-replayed state") was wrong — readers load
  the generation BEFORE taking `entIDLookupMu`, so a build racing any bump
  records the older generation and rebuilds next lookup; the comment was
  corrected; and
- the poison-log dedup set was the one scope-scale allocation the round
  left unbounded; it is now capped (4096 distinct `(kind, scope)` pairs per
  open, one-time suppression notice past the cap, already-seen scopes stay
  deduplicated) — a diagnostics-only cache, with the durable markers still
  recording every poisoned scope. The cap's three branches are pinned by
  `TestVerificationPoisonLogSuppressionCap` via a test seam that shrinks
  the bound to 1: past it, unseen scopes stop logging behind exactly one
  notice, seen scopes still dedup silently, and every poisoned scope's
  durable marker lands regardless.

## Criterion evidence

- C01 — **verified**. Scoped/unscoped resources, entitlements, grants, and
  synthesized paths are checked by `TestVerificationSourceScopeMutationTransitions`
  and the typed-operation coverage suite. Oracle: raw row stamp plus O4.
- C02 — **verified**. The full absent/A/B/unscoped/delete transition table and
  fail-closed malformed all-kind deletes pass
  `TestVerificationSourceScopeMutationTransitions` and
  `TestVerificationMalformedAllKindDeleteFailsClosed`. (The IfNewer conditional
  put path and its transitions test were removed with diff sync support.)
  CO-009 additionally consumes every sibling empty-keyspace proof on
  conservative rebind.
- C03 — **verified**. `TestVerificationSourceScopeMutationAtomicity` injects the
  shared typed-commit failure for fresh puts, moves, and deletes for all kinds and
  compares the incumbent row/index state.
- C04 — **verified**. `TestVerificationDescriptorClosedReplayAndDirectMaterialization`
  freezes the exact top-level field set for every D11 record, populates every field
  with a non-default sentinel (no current exemptions), and requires byte-semantic
  replay/direct equality plus every derived index key. Existing typed-operation,
  malformed-cleanup, digest/expansion, and O4 mutation-adequacy tests own the
  transition-specific obligations.
- C05 — **verified**. `TestVerificationManifestPartitionAndOverwriteMatrix` covers
  all kind/scope overwrite cells; C25/C40 supply 0/1/many states. Row cardinality
  is reduced because manifest write uses one shared `(kind,scope)` choke point and
  does not inspect rows.
- C06 — **verified**. All-kind direct differential, hostile-scope, and bidirectional
  prefix-neighbor tests assert exact selected scope/kind sets and decoy survival.
- C07 — **verified**. `TestVerificationReplayRejectsStampedRowsWithoutManifest`
  covers all kinds with occupied destinations and full before/after engine digests.
- C08 — **verified at the declared reduced P2 cells**. The descriptor-closed
  differential compares every field, generated index key, index-backed query
  projection, and normalized manifest semantics for all kinds after replay and
  repeated overlay versus direct materialization. Existing occupied/decoy and
  distinct/colliding entitlement identity tests own the remaining reduced P2
  representatives.
- C09 — **verified**. Replay and tombstone retries converge for the directed
  occupied, duplicate/unknown, and interrupted-prefix cells. The descriptor-closed
  differential applies representative all-kind overlays twice, compares them to
  one direct application, and rejects stale changed index keys.
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
- C14 — **verified**. The explicit D11 descriptor registry fails on an unclassified
  schema field, populates every current field, and overlays all kinds while
  preserving unchanged side state. Resource parent and grant expansion/index
  changes exercise stale-index cleanup; entitlement overlay exercises non-indexed
  payload replacement. Timestamp provenance remains independently owned by C26.
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
- C22 — **partial; verified for the planted mutants listed here, not the full
  frozen catalog**. Biconditional and manifest
  reconcilers have physical planted violations; auxiliary validators reject
  timestamp swaps, over-limit batches, stale counters, premature manifests, source
  digest changes, and prefix-neighbor deletion. The source-cache model now retains
  basic row/manifest cell mutants and the batch-leak oracle retains one mutant per
  family. The descriptor differential retains a dirty-destination wrong-merge
  mutant.
  Corrupt-source and `invalidated=true` matrices plant physical rejected inputs;
  the terminal-manifest missing-owner mutant and two-hop replay own lost forward
  stamps. The all-kind terminal-iterator failure cut is C27 behavioral evidence,
  not a planted swallowed-error mutant. That mutant remains incomplete here, and
  CO-007 explicitly defers wrong page order with C29.
  CO-009 asserts the proof bit
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
- C27 — **verified**. Production-size resources pin the real 10,000-row boundary,
  and every kind sweeps callback read failures and cancellation cuts before the
  first row, inside chunks, at boundaries, and before final commit. A dedicated
  terminal-iterator seam executes at the source-copy loop's production terminal
  disposition, after iteration and before final commit; every kind preserves the
  committed prefix, O4, source snapshot, error identity, and convergent retry.
  Preflight, destructive-clear, and scoped-delete loops check `Iterator.Error`
  inline but do not claim a dedicated deterministic terminal-error sweep. CO-009
  separately injects a destructive-clear commit failure after one landed batch.
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
- C35 — **verified for supported format, durable finished state, FULL/non-compacted
  eligibility, and corrupt input; explicitly excluded for compatibility
  matching**. Unsupported SQLite, unfinished, non-FULL, and compacted all-kind
  Pebble sources reject before destination mutation with unchanged source and
  occupied-destination digests; corrupt envelopes fail at open, and durably
  finished (`ended_at`) read-only FULL sources replay. Live and reopened unfinished
  sources are both rejected. This policy is enforced by the public
  `SourceCacheStore` wrapper and previous-artifact syncer selection; direct engine
  replay primitives are lower-level and bypass it. Cross-version compatibility
  matching remains deferred.
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
- cross-version compatibility eligibility deferred; compacted/non-FULL rejection
  is verified by CO-013;
An exclusion is not a behavioral pass.

## Structural-coverage triage

The closing profile was used as navigation for targeted review of the Phase 6a
delta. The retained ledger records four actionable findings:

- F1, HIGH — same-identity replay overwrite from a foreign scope had no prior
  caller. `TestVerificationReplayOverwriteCleansForeignScopeIndex` now covers all
  kinds, verifies the old source-scope index is gone, and uses a stale-scope delete
  as the behavioral over-delete oracle.
- F2, HIGH — scoped grant-ID tombstones lacked a non-selected survivor.
  `TestVerificationScopedGrantIDDeletePreservesNonTombstonedRows` pins exact subset
  deletion, survivor preservation, returned committed count, and idempotence.
- F3, MEDIUM — post-Close public source-cache mutations had no caller.
  `TestVerificationClosedStoreRejectsSourceCacheMutations` covers every public
  mutation and requires `ErrEngineClosing` before dirty marking/engine entry.
- F8, LOW — disabled-source-cache `NoopLookup` had no caller.
  `TestNoopLookupAlwaysMissesCleanly` requires a clean miss so connectors fall back
  to a cold fetch.

No finding is accepted solely because a line executed. Each item above has a
behavioral oracle and risk disposition. The profile produced no additional
actionable changed-branch finding beyond F1/F2/F3/F8 at the superseded candidate.
The CO-012a profile rerun at `c913dbc1` again reported 70.4% combined statements;
the new durable-finished-source helper was 83.3% covered and its success plus
unfinished rejection branches have behavioral tests. The retained F1/F2/F3/F8
ledger records the actionable findings from review; it is not a reproducible
per-branch disposition inventory, and the percentage remains navigation only.

The closure-candidate profile rerun passed at 82.4% statements for
`pkg/sourcecache`, 70.3% for `pkg/dotc1z/engine/pebble`, and 70.5% for
`pkg/dotc1z`. These percentages are navigation signals, not evidence:

```text
go test ./pkg/sourcecache ./pkg/dotc1z/engine/pebble ./pkg/dotc1z -coverprofile=/tmp/sync-replay-6a.cover -count=1
go tool cover -func=/tmp/sync-replay-6a.cover
```

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
state or lifecycle checks. That focused re-review was against the pre-CO-011 code
and is superseded for final signoff.

The first two independent final-code evidence audits found no new source-cache
product correctness defect, but rejected closure because provenance and several
instruments overclaimed their actual coverage. CO-011 through CO-011c record the
newly exposed batch-ownership, compactor-lifecycle, commit-seam, and stateful-model
obligations.

The repeated audits then disagreed on severity but agreed signoff was not ready.
One accepted with explicit limitations; the other rejected because the public
primitive replayed unfinished sources. Both found source-store Close errors were
discarded and structural/commit-point claims were too broad. CO-012 enforces durable
finished-source state, propagates owned source-close errors, and narrows the claims.
Unit tests inject synthetic source-handle close failures and verify joined identity,
complete handle traversal, and async cleanup. They do not inject an actual
source-store close failure through every top-level fold/rebuild/overlay publication
path; those paths are implementation-reviewed rather than end-to-end fault-proven.
The re-review also found a completed-run ownership gap on source-close failure.
CO-012a removes the unpublished run before returning the joined error and narrows
C22 and structural-coverage claims to their reproducible evidence. The final Grok
and Sol re-reviews found no HIGH or MEDIUM correctness/signoff blocker and both
returned **ACCEPT WITH EXPLICIT LIMITATIONS**. The remaining LOW test gap for run
removal failure is covered by
`TestFinishChunkRunFileJoinsRemovalFailure`.

The review also recorded these non-defect limits:

- C22 remains partial: no planted swallowed-terminal-error mutant and page order
  remains deferred with C29;
- actual source-store close failures are not end-to-end fault-injected through
  every top-level publication path;
- structural coverage is a navigation profile plus the named F1/F2/F3/F8 ledger,
  not a reproducible per-branch disposition artifact;
- finished-source enforcement belongs to the public `SourceCacheStore`; direct
  engine replay primitives bypass that policy;
- direct non-batch durability sites remain explicit follow-up debt;
- fold barrier ordering and entitlement-cache invalidation belong to deferred
  compactor integration;
- scoped rows/manifests remain unrepresentable through bulk input;
- many-scope source preflight was the sampled performance limitation assigned
  to CO-004; it has since landed in this PR per CO-014, with the counted
  preflight, CO-015 poison, and CO-016 boundary evidence recorded in the
  CO-014/CO-015/CO-016 remediation evidence section above.

## Evidence commands

The post-audit closure-candidate working tree passed the affected-package suites:

```text
go test ./pkg/sourcecache ./pkg/dotc1z/engine/pebble/internal/rawdb ./pkg/dotc1z/engine/pebble ./pkg/dotc1z ./pkg/synccompactor/pebble ./pkg/synccompactor -count=1
```

The corrected instruments passed focused runs:

```text
go test ./pkg/dotc1z/engine/pebble -run '^Test(CommitPointsHaveFailureSeams|FailureSeamsAreExercised|ResourceLeakRideAlongAdequacy|VerificationReplayCommittedPrefixRetryAllKinds|VerificationDescriptorClosedReplayAndDirectMaterialization)$' -count=1
go test ./pkg/dotc1z -run '^Test(SourceCacheModelOracleMutationAdequacy|ModelRandomizedSourceCacheLifecycle)$' -count=1
go test ./pkg/synccompactor/pebble -run '^Test(OverlayFoldBatchLifecycleFailureCuts|OverlayRestartCommitFailureReleasesBatches|MergeFoldCommitFailureRetryConvergesAndClosesCleanly)$' -count=1
go test ./pkg/synccompactor -run '^TestJoinCompactorCloseError$' -count=1
```

The repository lint gate and race-enabled closure instruments passed:

```text
make lint
go test -race ./pkg/dotc1z ./pkg/dotc1z/engine/pebble ./pkg/synccompactor/pebble -run '^Test(Verification|ModelRandomizedSourceCacheLifecycle|SourceCacheModelOracleMutationAdequacy|ResourceLeakRideAlongAdequacy|CommitPointsHaveFailureSeams|OverlayFoldBatchLifecycleFailureCuts|OverlayRestartCommitFailureReleasesBatches|MergeFoldCommitFailureRetryConvergesAndClosesCleanly)' -count=1
```

One-sample compactor measurements after family-batch accounting and close/remint
were 1,590,408 B / 5,058 allocs at 10k rows and 1,561,840 B / 5,061 allocs at
100k rows. This is measured allocation sampling, not a latency or regression proof:

```text
go test ./pkg/synccompactor/pebble -run '^$' -bench '^BenchmarkCompactionFlow$' -benchtime=1x -benchmem -count=1
```

CO-012a's committed implementation `c913dbc1` passed its focused lifecycle and
affected compactor suites:

```text
go test ./pkg/dotc1z -run '^TestVerification' -count=1
go test ./pkg/dotc1z -run '^Test(ModelRandomizedSourceCacheLifecycle|SourceCacheModelOracleMutationAdequacy|VerificationReplayRejectsUnfinishedSourceAllKinds)$' -count=1
go test ./pkg/synccompactor/pebble ./pkg/synccompactor -count=1
```

It then passed the final lint, race, model-soak, structural-profile, and
repository-wide gates:

```text
make lint
go test -race ./pkg/dotc1z ./pkg/dotc1z/engine/pebble ./pkg/synccompactor/pebble ./pkg/synccompactor -run '^Test(Verification|ModelRandomizedSourceCacheLifecycle|SourceCacheModelOracleMutationAdequacy|ResourceLeakRideAlongAdequacy|CommitPointsHaveFailureSeams|OverlayFoldBatchLifecycleFailureCuts|OverlayRestartCommitFailureReleasesBatches|MergeFoldCommitFailureRetryConvergesAndClosesCleanly|CloseSourceHandlesJoinsErrors|SourceChunkCloseAsyncPropagatesCloseAndRemovesDirectory|FinishChunkRunFile.*|Join.*CloseError)' -count=1
go test -p 1 ./... -count=1
go test ./pkg/dotc1z -run '^TestModelRandomizedSourceCacheLifecycle$' -count=20
go test ./pkg/sourcecache ./pkg/dotc1z/engine/pebble ./pkg/dotc1z -coverprofile=/tmp/sync-replay-6a-co012a.cover -count=1
go tool cover -func=/tmp/sync-replay-6a-co012a.cover
```

The final CO-012a profile reported 70.4% combined statement coverage. The serial
repository gate, 20-run model soak, and all race instruments passed.

The superseded committed closure candidate
`097f064e2ad2c35017d87f43e3836ff474a6f503` passed the following final gates on
2026-07-30:

```text
make lint
go test ./pkg/sourcecache ./pkg/dotc1z/engine/pebble/internal/rawdb ./pkg/dotc1z/engine/pebble ./pkg/dotc1z ./pkg/synccompactor/pebble ./pkg/synccompactor -count=1
go test -race ./pkg/dotc1z ./pkg/dotc1z/engine/pebble ./pkg/synccompactor/pebble -run '^Test(Verification|ModelRandomizedSourceCacheLifecycle|SourceCacheModelOracleMutationAdequacy|ResourceLeakRideAlongAdequacy|CommitPointsHaveFailureSeams|OverlayFoldBatchLifecycleFailureCuts|OverlayRestartCommitFailureReleasesBatches|MergeFoldCommitFailureRetryConvergesAndClosesCleanly)' -count=1
go test ./pkg/dotc1z -run '^TestModelRandomizedSourceCacheLifecycle$' -count=20
go test ./pkg/sourcecache ./pkg/dotc1z/engine/pebble ./pkg/dotc1z -coverprofile=/tmp/sync-replay-6a-final.cover -count=1
go tool cover -func=/tmp/sync-replay-6a-final.cover
go test -p 1 ./... -count=1
```

The structural profile reported 70.4% combined statement coverage. The 20-run
model soak and serial-package repository suite both passed. These commands ran
from a clean tracked tree; the unrelated untracked
`digest-endsync-bench.txt` artifact was not part of the candidate.

Historical pre-CO-011 evidence: the then-uncommitted working tree was rerun after
CO-010b and the clean implementation re-review on 2026-07-29. All of these commands
passed:

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

### Ordinary-ingest cost audit and the source-scope obligation gate

A post-closure algorithmic audit of the ORDINARY (unscoped) write paths found
one material regression the replay-focused benchmarks had not covered:
`PutEntitlementRecords` gained a per-row read-before-write whose only purpose
is feeding the by_source_scope overwrite cleanup — a value-derived index
obligation entitlements never had before. Grants and resources were measured
unchanged: their prior-value reads predate this work (grant overwrite probe,
resource by_parent cleanup) and the added scope field scans skip
length-delimited payloads in O(1) per field header.

A/B evidence (`scope_ingest_bench_test.go`, 200k rows in 10k batches, same
bench file on both trees, interleaved runs on an M1 laptop):

- Before the gate: entitlement ingest floor 735ms vs 505ms on main
  (~+1µs/row, the added `db.Get`); grants and resources within noise.
- After the gate: entitlement ingest 513–599ms interleaved against main's
  516–585ms — the regression is eliminated; grants and resources remain
  within noise. End-to-end sync-shape and full-syncer benchmarks show no
  measurable branch delta.

The fix is `rawdb.sourceScopeMayExist`, the same shape as the existing
`grantDigestsPresent` gate: false certifies that no by_source_scope index
entry exists, so record ops skip every obligation that exists to maintain
entries (prior-value scope scans, delete-side cleanup, and the entitlement
read-before-write). Transitions: probed at Open with bounded seeks over the
three index families, armed inside `stageSourceScopeChange` the moment a
stamped record is staged (self-healing; the new value is always scanned),
armed unconditionally by `NewFoldBatch` (the fold copies borrowed scope-index
keys the typed ops never see), disarmed only by `ResetForNewSync`'s family
excision. Replay-state invalidation deliberately leaves the gate armed:
stale-true only costs performance, false-with-entries is the one unsound
state. Stamped primaries without entries (the rebuild-compaction output
shape) correctly reopen unarmed — nothing the gate guards exists to maintain.

Verification: `source_scope_gate_verification_test.go` pins the lifecycle
(fresh-unarmed, unscoped-writes-stay-unarmed, stamped-write-arms per record
kind, reset-disarms), the Open probe in both directions per kind, the fold
surface (arm at mint plus end-to-end maintenance of an actually-borrowed
entry), the bulk-import Finish re-probe, the post-invalidation write shapes,
and a behavioral proof that the unarmed path never consults the prior value
(a planted undecodable prior value is accepted unarmed and rejected armed).

Two independent reviews of the gate change (Grok, Sol; no shared context)
each returned zero reachable HIGH findings and converged on one MEDIUM
latent hole: `BulkSyncImport.Finish` ingests SSTs whose grant index family
set already includes by_source_scope (`grantIndexKeys` emits scope entries
for stamped records), without arming the gate — today's v2 translators never
stamp, so the state was unreachable in-tree, but the writer surface permits
it. Remediation: Finish re-probes the gate inside the same write-barrier
closure that burns the fresh-empty proofs; the arming obligation is
documented on the rawdb ingest family and the index-migration registry
(probe runs before migrations, so a scope-backfilling migration must arm or
re-probe itself); the reviews' secondary finding — the test file's
completeness overclaim — was corrected by scoping the header claim and
adding the per-kind, fold borrowed-entry, and bulk-Finish transition tests
above.

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
- Performance review had gated fold, replay, and compaction but not ORDINARY
  ingest, which silently carried the new scope-index obligation cost on every
  unscoped write. The ingest-cost audit above found and removed the entitlement
  per-row read via the sourceScopeMayExist gate. Correction adopted: end-to-end
  sync ingest benchmarks (per-family ingest plus the sync-shape and full-syncer
  benches) are part of the critical performance path for any change that touches
  record write obligations.
