# Sync Replay Phase 6b Verification Evidence

Plan: [`plan.md`](plan.md)

Scope: source-cache replay orchestration — capability parsing, lookup
install/teardown and delivery, compat record lifecycle and byte-match
gating, fresh-page recording, replay handling with provenance enforcement,
the warm/cold `ErrReplayIntegrity` taxonomy, the CO-017 fold fence
(rider A), and the chaos suites that observe all of it. Gates were run on
2026-08-27 against the working tree; the committed SHA is recorded at
commit time. Included change orders: CO-6b-001, CO-6b-002, CO-6b-003,
CO-6b-004, CO-6b-005, CO-6b-006
(post-review remediation; see Independent reviews).

## Gates

Re-run in full after the post-review remediation (CO-6b-003):

```text
make lint                                                    -> 0 issues
go test -count=1 -timeout=25m ./...                          -> pass 876s
go test -race -count=1 -timeout=15m \
  -run '^TestChaosSourceCache' ./pkg/sync                    -> ok 227.5s
go test -race -count=1 -timeout=10m \
  -run '^(TestSourceCacheReplayVerdictTaxonomy|TestChaosSourceCacheReplayWithoutHitFailsCold|TestChaosSourceCacheCompatDriftOnResume|TestPreviousSyncC1ZPathEnforcesReplayEligibility|TestChaosSourceCacheDriftedResumeRejectsRestoredReplay|TestChaosSourceCacheDuplicateReplayCursorsParallel|TestChaosSourceCacheUnsupportedShapesBlockReplaySeed|TestSourceCacheLookupDeliverabilityProbe|TestSourceCacheReplayOncePerScopeIsAtomic)$' \
  ./pkg/sync                                                 -> ok 49.1s
go test -race -count=1 \
  -run '^TestVerificationReplayVerdictSentinelIdentity$' \
  ./pkg/dotc1z/engine/pebble                                 -> ok 5.3s
make chaos-check                                             -> pass
BATON_TEST_NIGHTLY=1 go test -count=1 \
  -run '^TestChaosSourceCacheGenerationalLongChain$' \
  ./pkg/sync                                                 -> pass 4.5s
```

Note: under the whole-repo run, `pkg/dotc1z` needs more than Go's default
10-minute per-package timeout when sharing the machine with every other
package's tests (it passes alone in ~190s); the full-suite gate above runs
with `-timeout=25m` for that reason.

Re-run after the third re-review remediation (CO-6b-006):

```text
make lint                                                    -> 0 issues
go test -count=1 -timeout=20m ./pkg/sync                     -> ok 479s
go test -count=1 -timeout=25m ./...                          -> pass
go test -race -count=1 -timeout=10m \
  -run '^(TestSourceCacheReplayOncePerScopeIsAtomic|TestSourceCacheRecordPageParksBehindReplayCopy|TestChaosSourceCacheDuplicateReplayCursorsParallel|TestChaosSourceCacheDriftedResumeRejectsRestoredReplay)$' \
  ./pkg/sync                                                 -> ok 5.6s
make chaos-check                                             -> pass
```

Mutation checks re-run at this SHA: warm gate deleted -> chaos + unit
instruments fail; record-page early return restored -> park instrument
fails; withdrawal block disabled -> capability-withdrawn cell fails;
scope mutex removed and probe assert disabled -> their CO-6b-005
instruments fail (verified last round, instruments unchanged).

## Performance evidence

The orchestration's hot-path addition is one `sourceCachePageOps` call per
list-response page (pure CPU; no store I/O until an annotation is honored).
`BenchmarkSourceCachePageOps` (`pkg/sync/source_cache_pageops_bench_test.go`,
Apple M1):

```text
disabled-no-annotations            88.8 ns/op   256 B/op   2 allocs/op
disabled-unrelated-annotations    110.9 ns/op   256 B/op   2 allocs/op
enabled-no-annotations             94.5 ns/op   256 B/op   2 allocs/op
enabled-record-annotation         290.2 ns/op   344 B/op   5 allocs/op
```

The disabled cells are what EVERY production page pays while no connector
declares the capability: ~100ns and two small allocations per page (a page
is typically 100+ rows, each costing orders of magnitude more to decode and
write). Non-capable syncs additionally pay one type-assert and at most one
compat-record point read per sync attempt; the per-scope lock map, the
provenance sets, and the produce-guard row scans are only touched by
annotated pages of capable connectors.

Engine-level costs carried over from the 6a work and re-measured on this
branch: `BenchmarkSourceCacheReplayResources` copies 100k rows in ~0.46s
(~217k rows/s, ~210 B allocated per row — the bounded-batch contract also
pinned by `source_cache_replay_bounds_verification_test.go`); 10k rows in
~54ms. Scoped-ingest A/B (`BenchmarkScopeIngest*`) and preflight scaling
(`BenchmarkReplayPreflightScopeScaling`) cover the stamp-and-seal side. A
warm replay therefore costs milliseconds against the seconds-to-minutes a
cold refetch of the same scope costs upstream.

Checkpoint cost of the provenance sets (unbounded in scope count, so the
curve is the documented bound — see `state.sourceCacheHits`):
`BenchmarkStateMarshalSourceCacheSets` (Apple M1, hit map with validators
plus replayed set, HashScope-sized keys): 1k scopes → ~0.5ms marshal,
~151KB token; 10k → ~10ms, ~1.5MB; 100k → ~102ms, ~15MB. Immaterial at
realistic scope counts; a connector at whale scale should move the sets to
sidecar persistence (like the entitlement graph) before adopting.

## Criterion closures

Every entry names its instrument(s) and the observed outcome. All
instruments pass under `-race`.

### R1 — Lookup lifecycle

Instruments: `TestChaosSourceCacheGateMatrix` (OR1 events),
`TestChaosSourceCacheWarmAcrossTransports`,
`TestChaosSourceCacheLookupTeardown` (`pkg/sync/chaos_source_cache_gate_test.go`).

- The capability is parsed per attempt; the warm-baseline cell observes a
  warm consult with the previous validator, every degrade cell observes a
  cold consult (miss), never an error surfaced to the connector.
- `requireSourceCacheEvents` asserts on EVERY event in EVERY suite that
  `SyncOpAttrs.Lookup` arrived non-nil (`NoopLookup` substitutes when
  unset) and that no lookup error reached the connector.
- Transport coverage: the warm consult contract holds on both in-process
  transports (direct client and in-process gRPC) — both are chaos-harness
  clients, which wire the lookup setter themselves. NO production
  transport wires the setter in this phase; the subprocess transport and
  the absence of a wired production path are a registered exclusion
  (CO-6b-001, see limitations). The syncer probes deliverability
  (`sourcecache.LookupDeliverabilityProbe`) and keeps the consume side
  cold — never logging warm — when the transport cannot deliver.
  Instrumented by `TestSourceCacheLookupDeliverabilityProbe`
  (mutation-verified: disabling the probe type-assert fails the
  undeliverable cell), and the probe is compile-pinned on
  `internal/connector.connectorClient` so a method rename cannot silently
  sever it from the syncer's type assertion.
- Teardown: `TestChaosSourceCacheLookupTeardown` drives a consult through
  the connector-held lookup AFTER sync exit and observes a miss — the
  teardown cleared connector-visible state (`SetSourceCache(nil)` on every
  exit path, including cancellation, via `context.WithoutCancel`).
- Delivery sites: `pkg/connectorbuilder/resource_syncer.go` populates
  `SyncOpAttrs.Lookup` at all four sites; consults are observed at the
  three scoped kinds (resources, entitlements, grants — including the
  ForResourceType variants). The static-entitlements site receives the
  lookup identically but its kind is a registered exclusion (B10).

### R2 — Gate decision table

Instruments: `TestChaosSourceCacheGateMatrix` (install-time gates G6/G7 +
quality), `TestPreviousSyncC1ZPathEnforcesReplayEligibility`
(`pkg/sync/pebble_etag_replay_test.go`, NewSyncer gates G1–G5).

- Chaos cells: `warm-baseline`, `no-previous-artifact`,
  `capability-absent`, `capability-disabled`,
  `compat-cache-generation-mismatch`, `compat-config-fingerprint-mismatch`,
  `compat-selection-fingerprint-mismatch`, `compat-record-absent`,
  `quality-blocked-previous`. Each degrade cell produces exactly a cold
  sync (OR1 miss events) with the artifact converging to cold truth.
- NewSyncer cells: `pebble-full-synced` (reader installed),
  `pebble-full-no-quality-stats` (G4 fail-closed), `pebble-partial` (G3),
  `pebble-compacted-full` (G3 fold-dominates), `sqlite-full` (G2),
  `pebble-quality-blocked` (G4 reason flag),
  `pebble-fence-stripped-old-fold-shape` and `pebble-fence-foreign-witness`
  (G5). Strict `WithPreviousSyncC1ZPath` on an unusable file still fails
  NewSyncer loudly (`TestOptionalPreviousSyncC1ZPath_SoftFails`).

### R3 — Fresh-page semantics

Instrument: `TestChaosSourceCacheFreshPageSemantics`
(`pkg/sync/chaos_source_cache_collection_test.go`).

- `multi-page-round`: a recording round split across wire pages stamps
  every page's rows and publishes one entry with the shared validator.
- `same-page-put-then-tombstone`: within-page order is upserts before
  deletes (6a C29's orchestration half) — the tombstoned row dies, the
  other survives stamped.
- `empty-validator-no-entry-misses-next-sync`: rows stamp, no manifest
  entry publishes, and the NEXT sync's consult misses and refetches
  (6a C25's semantics at orchestration level).
- `static-entitlement-annotation-ignored`: a `SourceCacheRecord` on a
  static-entitlements response is ignored with a warn; no entry, no
  stamps, sync stays green (registered exclusion B10).
- Zero-row scope entries are additionally pinned by the collection
  suite's zero-row replay cell (OR3 asserts the entry with zero stamps).
- Unreplayable shapes block the seed (CO-6b-003):
  `TestChaosSourceCacheUnsupportedShapesBlockReplaySeed`
  (`pkg/sync/source_cache_orchestration_test.go`) — a record-annotated
  resources page whose rows declare child resource types, and a grants
  page carrying `InsertResourceGrants`, complete green but seal the
  artifact `source_cache_replay_blocked`
  (`source_cache_shape_unsupported`), so the next sync's G4 gate refuses
  it: replaying such a scope would silently lose the derived rows.

### R4 — Compat lifecycle (closes 6a C30)

Instruments: `TestSourceCacheCompatRecordLifecycle`
(`pkg/dotc1z/engine/pebble/source_cache_compat_lifecycle_test.go`),
`TestChaosSourceCacheCompatDriftOnResume`
(`pkg/sync/source_cache_orchestration_test.go`).

- Engine lifecycle (replaces the `C30-compatibility-record-lifecycle`
  exclusion): round-trip forces the singleton id; replay-state
  invalidation and replacement-sync reset both wipe the record; the
  cleanup range list covers the compat key; leak accounting rides along in
  every cell.
- Record bytes: the gate matrix's compat cells (R2/R5) read the stored
  record back through the oracle and match it field-for-field against the
  capability that produced it.
- Resume idempotence: `TestChaosSourceCacheInterruptResume`'s warm-resume
  cells re-run the install sequence over the checkpointed store with an
  unchanged capability — the existing record verifies silently and the
  resume stays warm.
- Drift-on-resume (plan B4): the capability's cache generation changes
  between an interrupted attempt and its resume. The resume completes
  green, every consult goes cold despite a fully eligible previous
  artifact, the ORIGINAL compat record is preserved (never overwritten),
  and the artifact seals with `source_cache_replay_blocked` set so it
  cannot seed the next generation.

### R5 — Compat byte-match

Instrument: `TestChaosSourceCacheGateMatrix` compat cells (within R2).

- Each mismatching field degrades in isolation: cache generation, config
  fingerprint, and selection fingerprint cells each flip exactly one field.
- Missing record degrades (`compat-record-absent`).
- Empty-matches-only-empty: the warm baseline's selection fingerprint is
  empty on both sides (unrestricted syncs) and matches; the
  selection-fingerprint cell declares an explicit selection against the
  stored empty value and degrades — both directions of the rule.

### R6 — Quality consume gate

Instruments: gate matrix `quality-blocked-previous` cell;
`TestPreviousSyncC1ZPathEnforcesReplayEligibility`
(`pebble-quality-blocked`, `pebble-full-no-quality-stats`);
`TestChaosSourceCacheGenerationalQualityLossBlocksC`
(`pkg/sync/chaos_source_cache_generational_test.go`).

- The generational cell is the end-to-end shape the criterion names: a
  chaos run trips a real ingest-filter drop, seals green with the
  replay-blocked reason, is offered as the previous source, and the next
  generation degrades to cold (OR1) rather than replaying lossy rows.
- Stats-absent artifacts fail closed (G4 conservatism cell).

### R7 — Replay semantics (closes 6a C29 at orchestration)

Instrument: `TestChaosSourceCacheCollectionSemantics`
(`pkg/sync/chaos_source_cache_collection_test.go`).

- Frozen page order (copy → upserts → tombstones) and cross-page order:
  delta-overlay cells apply upserts and tombstones over the replayed base
  and converge to the cold truth of the current epoch (OR2 full-proto
  fingerprint against an independent cold baseline).
- Copy dedup per scope: the duplicate-annotation cell proves the
  replacement copy runs once (a second copy would wipe overlay upserts,
  which OR2 catches).
- `overlay=false` pages carrying rows: applied as overlay upserts with a
  warn (transitional tolerance pinning 6a C34's contract).
- Validator publish on both paths (record wins over replay; zero-row
  scopes still publish); cross-kind non-aliasing (same scope key on
  different RPC kinds stays partitioned) — OR3 manifest/stamp snapshots.

### R8 — Provenance enforcement

Instruments: `TestChaosSourceCacheReplayWithoutHitFailsCold`,
`TestChaosSourceCacheDriftedResumeRejectsRestoredReplay`
(`pkg/sync/source_cache_orchestration_test.go`);
`TestChaosSourceCacheOrderingAdversary/spawned-duplicate-replay-cursors`;
`TestChaosSourceCacheDuplicateReplayCursorsParallel`;
`TestChaosSourceCacheInterruptResume`; `TestChaosSourceCacheLookupTeardown`.

- Replay-while-cold fails loudly: a connector emitting `SourceCacheReplay`
  with no this-sync lookup hit (cold sync, `NoopLookup`) fails the sync
  with `ErrReplayIntegrity`, cold verdict, correct row kind and scope key
  via `errors.As` — never a silent degrade (OR4).
- Planning-call → spawned-cursor handoff: the ordering suite's spawned
  cell consults at the root and honors replay annotations on spawned
  cursors (single copy, OR2 equivalence).
- Hit/replayed sets survive interrupt/resume: the resume suite checkpoints
  after every page (`checkpointInterval = 0`) and resumes with a NEW
  syncer instance over the checkpointed state — the cross-process-shaped
  resume the criterion requires. The replayed-set guard skips the
  replacement copy on the re-walked page. Durability granularity
  (CO-6b-006): provenance recorded during a dispatch batch becomes
  durable at the checkpoint atop the NEXT loop iteration; hits recorded
  in the batch that crashes are lost with the batch, whose actions
  re-run from the pre-batch checkpoint and re-consult on resume
  (at-least-once, safe in both warm and cold resume permutations).
- Restored hits do not outrank the attempt's own gates (CO-6b-003;
  premise corrected under CO-6b-006 after the third re-review proved the
  original two-team scenario never checkpointed its hit): the
  drifted-resume instrument spans two dispatch batches (102 grants
  actions against the batch cap of 100), captures the surviving
  checkpoint, and asserts IN-BAND that it contains both the batch-1 hit
  and the pending batch-2 replay-carrier action. The resume under a
  drifted capability restores that hit-set, re-serves the replay — and
  the warm gate (`sourceCacheWarm`, set only on actual warm install)
  rejects it with a cold `ErrReplayIntegrity` instead of copying rows
  from an artifact the resume's gates just refused. Mutation-verified:
  deleting the warm gate lets the restored hit and the still-eligible
  seed base drive the copy, and the test fails.
- Hits bind to the base they came from (CO-6b-004): each recorded hit
  carries the validator the lookup returned, and the copy runs only if
  the CURRENT replay base's manifest entry byte-matches it — a previous
  artifact swapped between attempts for a gate-identical sibling
  (identical compat key) fails cold instead of importing rows the
  connector never revalidated. Cells: `swapped-base-validator-mismatch`,
  `swapped-base-entry-missing`, `base-entry-read-failure`,
  `base-without-entry-surface`.
- Once-per-scope copy is atomic under parallelism (CO-6b-003): the
  overlap instrument `TestSourceCacheReplayOncePerScopeIsAtomic` holds
  one replay copy mid-flight in the store and drives a second
  `beforeUpserts` for the same scope — with the lock the second parks
  and takes the already-replayed skip; exactly one copy lands
  (mutation-verified: removing the scope mutex fails it). The
  end-to-end shape is `TestChaosSourceCacheDuplicateReplayCursorsParallel`
  (duplicate replay annotations at `WithWorkerCount(4)`, OR2 against the
  cold baseline); note the chaos test's spawned cursors are admitted
  after the parent page marked the scope, so the overlap instrument —
  not the chaos test — is what pins the race itself.
- Record-only pages serialize against in-flight copies (CO-6b-006):
  every scoped page — not just replay pages — holds the scope lock from
  `beforeUpserts` through `afterUpserts`, so a record page's row puts,
  tombstones, and manifest publish cannot interleave with another
  action's REPLACEMENT copy for the same scope (which deletes the
  scope's rows before copying the base — silent wipe of the fresh rows,
  or a validator published over an incomplete scope). Error paths
  between the two calls release through an idempotent `release()`
  deferred at all three handler call sites, so a failed page cannot
  leak the lock and deadlock its own retry. Instrument
  `TestSourceCacheRecordPageParksBehindReplayCopy`, mutation-verified
  against restoring the record-page early return.
- Post-sync teardown: R1's teardown instrument.

### R9 — Verdict taxonomy

Instruments: `TestSourceCacheReplayVerdictTaxonomy`
(`pkg/sync/source_cache_orchestration_test.go`) — a bounded enumeration of
every failure path named in plan B7, driven through the real page-ops
pipeline against a fake store — and
`TestVerificationReplayVerdictSentinelIdentity`
(`pkg/dotc1z/engine/pebble/source_scope_verification_test.go`):

- Cold: unparsable record annotation, unparsable replay annotation,
  duplicate same-type annotations on one page, invalid replay scope key,
  invalid record scope key (over-length), two scopes on one page,
  principal tombstones on an entitlements page, malformed (non-BID)
  resource tombstones, replay on a store without the source-cache
  surface, replay without a this-sync hit, replay with a hit but no
  previous artifact, replay while the attempt is not warm (drifted/cold
  resume), source-side replay-copy failure.
- Warm: destination-commit replay-copy failure
  (`dotc1z.ErrSourceCacheReplayDestination`), cancellation mid-replay
  (ERROR CHAIN only — ambient `ctx.Err()` no longer promotes, so a
  sibling action's failure cannot launder a genuine integrity error into
  warm), canonical-tombstone failure, principal-tombstone failure,
  manifest-publish failure, row-put failure on an annotated page
  (`wrapPageRowPutError`; the helper's classification is unit-pinned —
  its wiring at the three collection-handler put sites is code-reviewed
  but has no store-fault instrument, a known verification gap recorded
  under limitations).
- Identity (OR4): every cell asserts `errors.Is(…, ErrReplayIntegrity)`
  and `errors.As` → verdict/row-kind/scope through an extra `fmt.Errorf`
  wrap, and that the underlying cause stays reachable via `Unwrap`.
- Engine-side sentinel identity (closes the review's overfit finding):
  the engine instrument injects failures at the REAL commit and read
  sites (the enumerated commit-point seams) and asserts a
  destination-commit failure arrives carrying
  `ErrReplayDestinationCommit` while a source-side read failure arrives
  without it — deleting the engine's wrapping can no longer pass the
  suite.

### R10 — CO-017 fence (rider A)

Instrument: `TestPreviousSyncC1ZPathEnforcesReplayEligibility` fence cells.

- New saves carry the witness: the fence-stripped cell first asserts the
  freshly synced artifact's envelope manifest equals
  `sourcecache.MaterializationPolicyGeneration` (the strip would otherwise
  be a vacuous no-op).
- Old-fold shape: payload intact (manifest entries, indexes, compat record
  survive; `compacted` false), witness absent — rejected by the fence
  alone (`pebble-fence-stripped-old-fold-shape`).
- Witness mismatch: a foreign generation (future bump) is rejected the
  same way — the fence is exact-match, never presence-only
  (`pebble-fence-foreign-witness`).

### R11 — Interruption/resume

Instruments: `TestChaosSourceCacheInterruptResume`,
`TestChaosSourceCacheReplayStripsExpanderSources`
(`pkg/sync/chaos_source_cache_resume_test.go`).

- Cut enumeration: crashes before the warm root, after the replay copy,
  after overlay pages, and around manifest publish all resume and converge
  to the cold baseline (OR2), with no manifest entry vouching for an
  incomplete scope (OR3 checked at every seal).
- Withdrawn-at-resume: a previous artifact that disappears between attempt
  and resume degrades the resume to cold and still converges.
- Resume granularity is pinned as CO-6b-002: interrupted paginated actions
  restart from their ROOT page token (at-least-once page processing); the
  checkpointed hit/replayed sets act as idempotency guards, asserted
  explicitly per cell (`grantsTraceHas` on served tokens, exact re-consult
  counts).
- Expansion composition: the expansion-enabled scenario proves
  expander-written `Sources` on direct grants are stripped at replay copy
  and recomputed, while connector-set `Sources` survive byte-for-byte;
  OR3 stamp counts exclude the expander-created grant (derived rows stay
  unscoped — registered boundary B10).

### R12 — Ordering/pagination adversary

Instrument: `TestChaosSourceCacheOrderingAdversary`
(`pkg/sync/chaos_source_cache_order_test.go`).

- `lost-response-reconsults`: a warm root answered but lost in flight is
  re-consulted on retry (the warm branch is not burned by an unlanded
  response); the replay round completes on attempt two.
- `epoch-drift-between-retries`: the upstream moves between attempts; the
  retry's consult re-decides (stale → fresh fetch) and the artifact equals
  the DRIFT epoch's cold truth, never a blend.
- `spawned-duplicate-replay-cursors`: duplicate replay annotations across
  spawned cursors produce exactly one copy; the last replay page's
  validator wins the manifest entry.
- `TestChaosSourceCacheDuplicateReplayCursorsParallel` re-runs the
  duplicate-cursors shape at `WithWorkerCount(4)` (CO-6b-003): the
  per-scope lock serializes the decide-copy-mark sequence, exactly one
  copy lands, and OR2/OR3 hold under `-race`.
- Every cell closes with OR2 against the answering epoch's independent
  cold baseline and OR3 on the final manifest/stamp state.

### R13 — Generational steady state (first-class)

Instruments: `TestChaosSourceCacheGenerationalSteadyState`,
`TestChaosSourceCacheGenerationalResumedBSeedsC`,
`TestChaosSourceCacheGenerationalQualityLossBlocksC` (per-PR), and
`TestChaosSourceCacheGenerationalLongChain` (nightly, six generations)
(`pkg/sync/chaos_source_cache_generational_test.go`).

- OR5(a) hit-rate parity: all four scopes (etag-style resources,
  delta-style entitlements, etag-style grants, delta-style grants) consult
  warm in B and again in C — no per-scope delta.
- OR5(b) zero fresh fetches for unchanged scopes: every C consult takes
  the warm branch; no fresh-fetch page is served.
- OR5(c) manifest parity: every scope entry in B's artifact is present in
  C's (OR3 snapshots per generation), zero-row scopes included.
- OR5(d) validator provenance: delta-style validators rotate once per
  generation; etag-style validators carry unchanged. The long chain holds
  all four properties across six hops and still equals the cold truth.
- Adversarial: an interrupted-and-resumed B still seeds a warm C (routine
  resumes are not chain breaks); a B with genuine quality loss blocks C
  loudly with the reason in the trace, and C's artifact converges cold.
- This suite observes the producer half of replay: a replayed scope that
  failed to republish its entry would alternate hit/miss per generation —
  the steady-state warm-event assertions would catch it in B→C.

### R14 — Degradation composition

Instruments: `TestChaosSourceCacheStaleValidatorFetchesFresh`,
`TestChaosSourceCachePoisonedScopeColdInsideWarmSync`
(`pkg/sync/chaos_source_cache_stale_test.go`).

- Stale validators (upstream changed between generations) consult, fail to
  match, fresh-fetch, and converge to the cold baseline — warm machinery
  never replays stale rows.
- A poisoned scope (CO-015/CO-016 shape: a cross-scope tombstone poisons
  the stamped scope) reads as a lookup miss inside an otherwise-warm sync
  and cold-fetches, while sibling scopes stay warm — the graceful
  composition the CO-016 documentation in `pkg/sourcecache/sourcecache.go`
  describes.

## Closure-rule items

- The two skipped ETag replay tests were REMOVED from
  `pkg/sync/pebble_etag_replay_test.go` with a pointer comment; their
  intent is subsumed by R7/R13 instruments (validator carry: gate matrix +
  generational; row carry-forward: collection + generational OR2).
- The 6a `C30-compatibility-record-lifecycle` exclusion is replaced by
  `TestSourceCacheCompatRecordLifecycle`; the 6a
  `C25-transitional-empty-overlay-validator`,
  `C34-transitional-overlay-annotation`, and `source-compatibility-policy`
  exclusions are closed by the R3/R7/R2 instruments named above and
  removed from `pkg/dotc1z/engine/pebble/source_cache_exclusions_verification_test.go`
  with pointer comments.
- The Phase 6b exclusions registry
  (`pkg/sync/source_cache_exclusions_verification_test.go`) registers:
  lambda ask/answer continuation (deferred to 6c), subprocess transport
  lookup delivery (CO-6b-001), resource-targeted sync and event feeds
  (B10); and names the instruments for the static-entitlement and
  derived-row boundaries.
- The `NOT YET WIRED` block in `pkg/sourcecache/sourcecache.go` is
  rewritten as current behavior (wiring, CO-6b-001 boundary, CO-016
  composition) — after R1–R9 instruments passed, and corrected
  post-review to state the delivery limitation honestly (no wired
  production transport; deliverability probe keeps unwired paths cold).
- Tiering: `make chaos-check` gained named source-cache representatives
  (gate matrix, collection, interrupt/resume, generational steady state,
  compat drift, replay-without-hit, drifted-resume warm gate, parallel
  duplicate cursors, unsupported shapes); `make chaos-full-check` runs
  every `TestChaos(Connector|SourceCache)` suite nightly-tier; the
  six-generation chain requires `BATON_TEST_NIGHTLY=1`.

## Change orders

- **CO-6b-001** — lookup delivery is in-process only (subprocess-wrapped
  connectors observe `NoopLookup` and sync cold; the ask/answer
  continuation in 6c is the cross-process mechanism). Amended
  post-review: NO production transport wires the setter in this phase —
  in-tree delivery is the chaos harness's clients only — and the syncer's
  deliverability probe keeps unwired transports cold rather than logging
  warm. Registered as an executable exclusion; R1 transport coverage is
  direct + in-process gRPC via those clients.
- **CO-6b-002** — interrupted paginated actions restart from their root
  page token (at-least-once page processing); the checkpointed provenance
  sets are idempotency guards, not mid-chain resume state. Pinned by the
  resume suite's per-cell trace assertions.
- **CO-6b-003** — post-review contract tightening: untargeted-FULL-only
  sync-shape gate (with artifact blocking when produce state predates a
  capability/shape change on resume), the `sourceCacheWarm` provenance
  gate in replay enforcement, per-scope atomic decide-copy-mark,
  unreplayable-shape produce guards (child resource types,
  `InsertResourceGrants`), and verdict-classification tightening
  (error-chain-only cancellation, row-put wrapping, duplicate
  annotations, BID-validated resource tombstones). Full mechanism and
  instrument list in `plan.md`.
- **CO-6b-004** — hit-validator binding: recorded hits carry the
  validator the lookup returned, and the replay copy requires the
  current base's manifest entry to byte-match it, closing the
  swapped-previous-artifact hole the eligibility gates cannot see
  (identical compat keys). Checkpoint hit shape changed to
  scope → validator. Full entry in `plan.md`.
- **CO-6b-005** — re-review remediation: mutation-adequate instruments
  for the deliverability probe and the per-scope replay lock, the probe
  interface promoted to `pkg/sourcecache` with a compile pin on the
  production client, the capability-withdrawal-on-resume cell, and the
  lock-map/scope-set cost benchmarks. Full entry in `plan.md`.
- **CO-6b-006** — third re-review remediation: the drifted-resume
  instrument's premise corrected (batch-split scenario with in-band
  checkpoint assertions; the warm gate is now genuinely
  mutation-verified end to end), record-only pages hold the scope lock
  through `afterUpserts` (closing the residual copy-versus-record
  interleaving race), and the sync-token schema fence (reshaped hit map
  takes a new JSON key; versioned tokens that fail to parse error
  loudly instead of silently downgrading to the v0 format and dropping
  the action stack) with round-trip and fence tests. Full entry in
  `plan.md`.

## Explicit limitations

- Lookup delivery: NO production transport wires the lookup setter this
  phase (CO-6b-001) — the standard runner path spawns the connector as a
  subprocess and has no backchannel, and no production in-process
  constructor wires the setter either. Delivery is verified on the chaos
  harness's direct and in-process gRPC clients; production syncs run the
  produce side (stamping, validator publish) but consume cold, with the
  deliverability probe preventing a false warm log.
- Lambda ask/answer continuation (`SourceCacheLookupOffer/Ask/Answers`)
  is unimplemented and deferred to Phase 6c.
- `overlay=false` replay pages carrying rows are tolerated with a warn and
  overlay semantics (transitional; hardening to an error is future work).
- CO-016 (non-destructive external-resource reconciliation) is NOT
  implemented; connectors using `ExternalResourceMatch*` annotations have
  their placeholder-grant scopes poisoned every sync and cold-fetch those
  scopes inside warm syncs, as documented in `pkg/sourcecache`.
- OR5(b) is observed through consult decisions and served-page shapes,
  not a dedicated connector-side fetch counter.
- `wrapPageRowPutError`'s warm classification is unit-pinned, but its
  wiring at the three collection-handler put sites has no store-fault
  instrument (the chaos store has no put-failure injection seam);
  deleting a call site would not fail a test. Known verification gap,
  carried to 6c alongside the runner ladder that consumes the verdicts.
- R12 corpus breadth beyond the named adversarial cells and the
  six-generation chain run at nightly tier, not per-PR.

## Independent reviews

Three independent model code reviews ran against the committed phase
implementation (closure rules). Verdicts: two REJECT, one ACCEPT WITH
LIMITATIONS. Every load-bearing finding was verified against the code,
fixed, and instrumented; the remediation is CO-6b-003 plus the CO-6b-001
amendment. All fixes landed with their instruments before signoff.

Consolidated findings and dispositions:

1. **Lookup delivery never wired on any production path** (BLOCKER, two
   reviews). `SetSourceCacheSetter` had no callers, so the production
   client's setter was always nil while `installSourceCacheLookup` logged
   "installed (warm)" — the feature was inert in production with logs
   asserting otherwise. Fixed: `sourceCacheLookupDeliverable` probe;
   consume side stays cold and never logs warm when the transport cannot
   deliver; CO-6b-001, the exclusions registry, `pkg/sourcecache` docs,
   and this document's claims corrected (the original "production
   in-process path is covered" claim was false). Wiring a production
   transport is Phase 6c scope alongside the ask/answer continuation.
2. **Checkpoint-restored provenance re-authorized replay on cold/drifted
   resume** (BLOCKER). `beforeUpserts` trusted the checkpointed hit-set
   plus `previousSyncReader` and never consulted the attempt's own gate
   outcome (`sourceCacheWarm` was a dead field). Fixed: the warm gate is
   enforced in `beforeUpserts`; instrument
   `TestChaosSourceCacheDriftedResumeRejectsRestoredReplay`.
3. **Non-atomic once-per-scope replay guard** (MAJOR, all three reviews).
   Check-then-act between `SourceCacheReplayed` and
   `MarkSourceCacheReplayed` duplicated replacement copies at
   `WithWorkerCount > 1`, able to resurrect replaced overlay rows.
   Fixed: per-`(rowKind, scopeKey)` mutex around decide-copy-mark;
   instruments `TestSourceCacheReplayOncePerScopeIsAtomic` (overlapping
   `beforeUpserts` calls for one scope, mutation-verified against mutex
   removal) and `TestChaosSourceCacheDuplicateReplayCursorsParallel`
   (end-to-end shape at worker count 4).
4. **Replayed resource pages never schedule child-resource discovery**
   (MAJOR, two reviews). Child discovery runs over a page's own rows and
   replay pages return zero rows, so replaying a parent scope silently
   dropped all child resources. Fixed as a produce-side guard: pages
   declaring child resource types (or grants pages carrying
   `InsertResourceGrants`) block the artifact as a future replay source;
   instrument `TestChaosSourceCacheUnsupportedShapesBlockReplaySeed`.
5. **Fail-open verdicts and classification gaps** (MINOR, multiple).
   Ambient `ctx.Err()` promoted any replay-copy failure to warm; row-put
   failures on annotated pages carried no verdict; duplicate same-type
   annotations were silently first-match; targeted/partial syncs were not
   excluded from source-cache handling; capability withdrawal across a
   resume left stale produce state trusted. All fixed under CO-6b-003
   with taxonomy cells.
6. **Overfit sentinel test** (MINOR). The syncer taxonomy synthesized the
   destination-commit sentinel by hand, so deleting the engine's wrapping
   would have passed. Fixed: `TestVerificationReplayVerdictSentinelIdentity`
   injects at the engine's real commit/read seams and asserts sentinel
   identity both ways.

Findings reviewed and NOT acted on: none load-bearing — the remaining
reviewer notes (stale comments, log ordering) were fixed inline as part
of the batches above.

### Re-review rounds (post-remediation)

Three independent re-reviews audited the CO-6b-003 remediation; all
three REJECTED, on the shared ground that fixes claimed closed lacked
mutation-adequate instruments (reverting the fix left the suite green).
The third also ran an adversarial pass with mutation testing and found
two genuinely new defects. Dispositions:

1. **Vacuous instruments for the probe and the per-scope mutex** (rounds
   one and two) — closed under CO-6b-005:
   `TestSourceCacheLookupDeliverabilityProbe` and
   `TestSourceCacheReplayOncePerScopeIsAtomic`, both mutation-verified,
   plus the compile pin on the production client and the
   capability-withdrawal cell (which also kills the third round's
   surviving withdrawal-block mutant).
2. **Drifted-resume instrument premise false** (third round, confirmed
   by mutation on HEAD: the chaos test passed with the warm gate
   deleted because no surviving checkpoint ever contained the hit) —
   closed under CO-6b-006 with the batch-split scenario and in-band
   premise assertions; the R8 claim this document previously made for
   the old scenario was withdrawn and replaced. The unit cell had
   independently killed the mutation throughout; the chaos instrument
   now does too.
3. **Record-only pages bypass the scope lock** (third round, N1, MAJOR)
   — a real residual race of the same class the mutex closed; fixed and
   instrumented under CO-6b-006.
4. **Token schema change without a version fence** (third round, N5) —
   real data-loss hazard (v1-token parse failure silently downgraded to
   the v0 format, dropping the action stack); fixed and instrumented
   under CO-6b-006 before any release carried the reshaped field.
5. **Provenance-set round-trip untested** (third round, N3) — closed:
   `TestSyncerTokenSourceCacheSetsRoundTrip`; the marshal cost curve was
   already pinned under CO-6b-004.
6. **Retained lock map unreclaimed/undocumented** (third round, N4) —
   already closed under CO-6b-005 (documentation and
   `BenchmarkSourceCacheScopeLocks`); cardinality note updated for the
   CO-6b-006 lock-scope extension (one mutex per scoped page's scope,
   not per replayed scope).
7. **Produce-guard limitations noted, not blocking** (third round): the
   unsupported-shape block is artifact-wide (per-sync stats, one
   hierarchical resource type blocks every scope), and it fires even for
   child types excluded from the sync — both conservative in the safe
   direction; recorded here as accepted behavior.

### PR review round (automated, PR #1112)

Zero blocking findings, four suggestions; dispositions (CO-6b-004):

1. **Hit not bound to its artifact** (load-bearing) — a previous artifact
   swapped between attempts for a gate-identical sibling could satisfy a
   checkpointed hit and import unrevalidated rows. Fixed: hits record the
   lookup's validator; `beforeUpserts` requires the current base's
   manifest entry to byte-match before the copy; four cold cells added.
2. **Provenance-set checkpoint cost unbounded** — cost curve documented on
   the field and pinned by `BenchmarkStateMarshalSourceCacheSets`;
   sidecar persistence named as the whale-scale escape hatch. Not moved
   to sidecar in this phase.
3. **Cold verdict has no consumer; degraded mid-sync resumes fail
   deterministically** — operational contract documented on
   `beforeUpserts` (loud repeated failure until the caller's retry policy
   abandons the unfinished sync; 6c's ladder automates the cold
   fallback). No behavior change.
4. **Private backend paths in `docs/rfcs/0010-sqlite-conversion-only.md`**
   — scrubbed to generic descriptions per the public-repo content
   guidelines; exact call sites stay in the internal ticket.
