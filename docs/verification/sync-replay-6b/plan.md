# Sync Replay Phase 6b Verification Plan — Orchestration

Status: frozen behavioral baseline, with append-only change orders below.

This plan was frozen before writing any Phase 6b orchestration code. The
pre-existing surfaces it builds on (Phase 6a storage machinery, the annotation
protos, the chaos harness, the syncer's existing eligibility gate) were read
first; the behavior specified here for the NEW orchestration is preregistered
and implementation-blind with respect to that new code. Deviations discovered
during implementation are logged as change orders, never silently absorbed.

## Guardrails and risk verdict

- Inputs used for the frozen core: `docs/BUG_CATCHING.md`,
  `docs/REVIEW_CHECKLIST.md`, `docs/verification/sync-replay-6a/plan.md`
  (especially CO-013..CO-017 and deferred criteria C29/C30),
  `docs/verification/sync-replay-6a/evidence.md` (signoff scope),
  `pkg/sourcecache/sourcecache.go` (the connector-facing contract, present
  tense by design), `proto/c1/connector/v2/annotation_source_cache.proto`,
  `proto/c1/storage/v3/records.proto`, `proto/c1/c1z/v3/manifest.proto`, the
  Phase 6b plan's PR 2 section, and the existing seams in `pkg/sync/syncer.go`,
  `pkg/connectorbuilder/`, `internal/connector/connector.go`,
  `pkg/dotc1z/source_cache.go`, and `internal/chaosconnector/`.
- Phase 6b is HIGH risk: failures are silent (a warm sync that quietly loses a
  scope's rows is green), combinatorial (capability × provenance × compat ×
  quality × page shape × interruption), and have no single-run oracle (chain
  continuity is only observable across three or more generations).
- Replay is Pebble-only by design (`docs/rfcs/0010-sqlite-conversion-only.md`).
  Non-Pebble previous artifacts degrade to cold inputs through the existing
  `NewSyncer` gate; nothing SQLite-side is built or verified beyond that
  degradation.
- Store-level semantics (copy exactness, scope isolation, tombstone unions,
  source immutability, poison behavior) were closed in Phase 6a and are NOT
  re-verified here; 6b verifies the orchestration that drives them and the
  sync-level equivalences only 6b can make observable.
- Observed baseline fact: the chaos-level eligibility-gate matrix described in
  the Phase 6b plan's PR 1 did not land as such on `main` (PR #1045 absorbed
  the corpora differently; `TestPreviousSyncC1ZPathEnforcesReplayEligibility`
  is a plain table test). The 6b gate-matrix suite therefore builds its own
  artifact-provenance helpers and is the first chaos-level instrument for the
  gate.
- Out of scope: everything in 6c (runner cold-retry, spare promotion, lambda
  continuation plumbing, CLI audit command), PR 7 drops, CO-016 reconciliation
  rework, incremental-expansion Stage A/B.

## Frozen behavioral contract

### B1 — Capability parsing and lookup install/teardown

- The syncer parses `SourceCacheCapability` from the `Validate` response
  annotations at the start of every sync attempt, including resumes.
- Absent capability or any mode other than `MODE_READ_WRITE` means every
  source-cache annotation on every page is ignored for the whole sync (proto
  contract). No stamping, no manifest writes, no compat record.
- The syncer delivers the lookup to the connector via
  `sourcecache.SetLookup.SetSourceCache` at sync start and delivers `nil` at
  sync end (all exits, including error paths), so a late RPC cannot read stale
  state. Delivery mirrors the session-store pattern:
  `internal/connector.connectorClient` forwards to a builder-side setter;
  `pkg/connectorbuilder`'s builder implements `sourcecache.SetLookup` and
  populates a new `resource.SyncOpAttrs.Lookup` field at all four list sites
  (`ListResources`, `ListStaticEntitlements`, `ListEntitlements`,
  `ListGrants`). When no lookup has been delivered (or `nil` was delivered),
  the builder populates `sourcecache.NoopLookup{}` — connectors never see a
  nil Lookup.
- The installed warm lookup is a syncer-owned wrapper over the previous
  store's `LookupSourceCacheEntry` that records every HIT `(row_kind,
  scope_key)` into the sync's durable provenance set (B5). Lookup read errors
  that leave fresh fetch available are returned as misses, not errors
  (package contract). Poisoned scopes already read as misses at the store
  layer; orchestration treats every miss as "fetch cold", never an error.

### B2 — Warm/cold gate decision table

A warm lookup (backed by the previous artifact) is installed if and only if
ALL of the following hold; otherwise `NoopLookup` is installed and the sync
proceeds cold with a structured warn naming the first failing condition:

| # | Condition | Where evaluated |
|---|-----------|-----------------|
| G1 | A previous artifact was supplied and opens read-only | `NewSyncer` (exists) |
| G2 | Previous artifact is Pebble-engine | `NewSyncer` (exists) |
| G3 | Latest finished sync is FULL and not compacted (`UsableAsReplaySource`) | `NewSyncer` (exists) |
| G4 | Previous artifact's latest usable run has ingest-quality stats present and `source_cache_replay_blocked == false` (`c1zstore.SourceCacheReplayEligible`) | `NewSyncer` (new) |
| G5 | Previous artifact's envelope manifest carries the materialization witness equal to `sourcecache.MaterializationPolicyGeneration` (CO-017 fence, B8) | `NewSyncer` (new) |
| G6 | Connector declared `SourceCacheCapability` `MODE_READ_WRITE` on this sync's Validate response | `Sync`, post-Validate (new) |
| G7 | Previous artifact's stored `SourceCacheCompatRecord` byte-matches the current sync's computed compat key on all four fields (B4) | `Sync`, post-Validate (new) |

- Strict-open failures (explicit `WithPreviousSyncC1ZPath` that cannot be
  opened) remain hard errors, as today. Every other failure of G1–G7 is
  optional-degrade: warn with the reason (quality degradation includes the
  block reason flags), install `NoopLookup`, proceed cold.
- Degradation applies to the CONSUME side only. When capability is
  `MODE_READ_WRITE`, the produce side (stamping, manifest publish, compat
  record — B3/B4) runs regardless of G1–G5/G7, because a cold sync by a
  capable connector must still seed the next sync (generation A seeds B).
- A lossy B must never seed C: G4 is evaluated against the same run that G3
  selected.

### B3 — Fresh-page handling (`SourceCacheRecord`)

Honored on `ListResources`, `ListEntitlements`, and `ListGrants` responses
when capability is `MODE_READ_WRITE`. Row kind is determined by the RPC,
never the annotation.

- Scope key is validated (`sourcecache.ValidateScopeKey`); an invalid scope
  key on an annotated page fails the sync loudly (cold verdict, B7) — the
  connector asked for caching semantics the SDK cannot key.
- The page's connector-emitted rows are stamped by wrapping the store puts in
  `sourcecache.WithScope(ctx, scope_key)`. Only the annotated page's rows are
  stamped: SDK-derived writes (expansion output, external-principal
  reconciliation writes, targeted-sync injections) and rows from other pages
  are not.
- Tombstones apply AFTER the page's rows commit (proto contract, closes the
  orchestration half of 6a C29's within-page order):
  `deleted_ids` → canonical-ID tombstones (`DeleteSourceCacheRows`);
  `deleted_principal_ids` → principal-scoped tombstones (grants: every grant
  in scope whose principal id matches; resources: resource rows in scope by
  resource id). `deleted_principal_ids` on an entitlements page is a loud
  sync failure (cold verdict): the proto defines no semantics for it.
- Manifest publish: when a page's `cache_validator` is non-empty, the syncer
  writes the scope's manifest entry (`PutSourceCacheEntry`) after that page's
  rows and tombstones complete. A zero-row page with a non-empty validator
  still publishes (200-with-zero-rows contract). Interim pages with empty
  validators publish nothing; a scope whose round never supplies a non-empty
  validator gets no entry and is a miss next sync (6a C25's transitional
  semantics, at orchestration level).
- `SourceCacheRecord` on `ListStaticEntitlements` responses is ignored with a
  warn: static entitlements stay unscoped (registered exclusion).

### B4 — Compat record lifecycle (closes 6a C30)

- When capability is `MODE_READ_WRITE`, the syncer writes the singleton
  `SourceCacheCompatRecord` (id `"compat"`) into the current store at sync
  start, after Validate and before the first list action. Fields:
  - `connector_cache_generation` — from the capability, verbatim.
  - `connector_config_fingerprint` — from the capability, verbatim.
  - `sdk_materialization_generation` — the new exported constant
    `sourcecache.MaterializationPolicyGeneration`. Bumped when the SDK
    changes how it materializes replayed rows (e.g. the expander-source
    stripping rule); a bump colds every pre-bump artifact.
  - `sync_selection_fingerprint` — lowercase-hex sha256 of the canonical
    string `v1|types=<comma-joined sorted resource-type filter>|skipEG=<bool>|skipG=<bool>`
    over the sync's selection shape (empty filter list canonicalizes to an
    empty segment). Any selection change colds the next sync.
- Byte-match gating (G7): all four fields of the previous artifact's stored
  record must equal the current sync's computed values byte-for-byte. A
  missing record on a previous artifact that is otherwise usable is a
  mismatch (degrade). Empty strings match only empty strings.
- Resume: the record is recomputed on every attempt. If a record already
  exists and matches, the write is an idempotent no-op. If it exists and
  DIFFERS (connector or config changed between resume attempts), the sync's
  cached rows are mixed-generation: the original record is left in place,
  the warm lookup (if any) degrades to `NoopLookup` for the remainder, and
  the artifact is marked replay-blocked through the ingest-quality reason
  flags so it cannot seed the next sync. The sync itself proceeds.
- Storage lifecycle: the record lives in the source-cache key family, so the
  fold compactor's `InvalidateSourceCacheReplayState` and the
  rebuild/overlay compactors' family drops remove it with the rest of the
  replay state; cleanup/reset/leak accounting include it (this is the
  executable closure of the skipped `C30-compatibility-record-lifecycle`
  cell).

### B5 — Replay handling (`SourceCacheReplay`) and provenance

Honored on `ListResources`, `ListEntitlements`, and `ListGrants` responses.
Row kind from the RPC. Ordering per page is frozen as: replay copy → page
upserts (stamped with the scope) → page tombstones (canonical ∪ principal,
after upserts). Cross-page ordering follows page arrival order exactly;
cross-page re-adds after a tombstone are the connector's responsibility
(proto contract).

- Same-sync provenance: a replay is honored only for a `(row_kind,
  scope_key)` present in this sync's lookup hit-set. The hit-set is recorded
  by the warm lookup wrapper (B1) and persisted in the sync's checkpoint
  token state, restored on resume — a planning call may batch-resolve scopes
  and hand verdicts to sibling cursors via `EnqueuePageTokens` page tokens,
  and those cursors may run after an interrupt/resume in a different
  process. A replay for a scope NOT in the hit-set (including any replay
  while the lookup is `NoopLookup`) fails the sync loudly with a cold
  verdict: the connector skipped row generation, there is nothing to fall
  back to.
- Replay copy runs once per `(row_kind, scope_key)` per sync. The
  replayed-scope set is persisted in the checkpoint token state alongside
  the hit-set. A later `SourceCacheReplay` for an already-replayed scope
  skips the copy (idempotent under duplicate pages and lost-response
  retries) and applies the page's upserts/tombstones normally.
- `overlay == false` with rows on the page: transitional tolerance — warn
  and apply the rows as overlay upserts (pins 6a C34's shape at
  orchestration level; the proto documents this hardening to an error
  later).
- Validator publish: if `SourceCacheReplay.cache_validator` is non-empty,
  the manifest entry is published after that page's operations complete;
  otherwise publish is deferred to a later `SourceCacheRecord` page whose
  `cache_validator` is non-empty (B3 rule). A replayed scope whose round
  never publishes a validator has no entry and is a miss next sync — the
  replay itself remains valid (rows are correct; only cacheability is
  lost).
- Checkpoint durability: hit-set and replayed-set mutations are recorded in
  the token state under the syncer's existing state lock and travel with
  the normal checkpoint cadence. Both sets are monotone within a sync, so
  a checkpoint cut between a lookup and its page commit is harmless: the
  worst case re-records a hit or re-runs an idempotent copy.

### B6 — Quality consume-side gate

- G4 of the decision table: `NewSyncer` reads the ingest-quality stats of
  the run selected by G3 and requires `c1zstore.SourceCacheReplayEligible`
  (stats present AND `source_cache_replay_blocked == false`). Absent stats
  degrade (pre-6b artifacts and unknown-checkpoint conservatism), blocked
  stats degrade with the reason flags in the warn.
- The produce side is untouched: `ingestFilterStats` continues to decide
  `SourceCacheReplayBlocked`, with one addition — the compat-drift-on-resume
  reason (B4).

### B7 — Warm/cold `ErrReplayIntegrity` taxonomy

- A new exported error surface in `pkg/sync`: `ErrReplayIntegrity`, carrying
  a verdict (`Warm` or `Cold`), the row kind, and the scope key, reachable
  via `errors.Is`/`errors.As` from the sync error chain. 6c's runner
  consumes it; 6b attaches it at every orchestration replay failure path.
- Frozen classification:
  - **Cold** (discard the spare, cold-retry): provenance violation (replay
    for an unknown scope, replay while cold); invalid annotation shapes
    (invalid scope key, principal tombstones on entitlements); source-side
    integrity failures from the store's replay preflight (poisoned scope,
    ineligible/inconsistent source, count mismatch) — the previous artifact
    or the connector's replay decision cannot be trusted.
  - **Warm** (retryable with replay still armed): destination-side failures
    after a sound source verdict — replay-copy commit errors past
    preflight, overlay upsert/tombstone write errors, manifest publish
    errors, and interruption/cancellation mid-replay. The replay decision
    remains valid; a retry may succeed warm.
  - Ambiguous store errors default to cold (fail-closed: a wrong cold
    wastes a fetch; a wrong warm can loop on a poisoned artifact).

### B8 — CO-017 cross-version fold fence (Rider A decision)

- Hazard (from 6a CO-017): an OLDER pebble3 SDK's fold byte-copies a scoped
  artifact and rewrites primaries while leaving the source-cache manifest,
  poison markers, scope indexes, and any byte-copied compat record intact,
  sets no `compacted` stamp, and invalidates nothing — a stale-but-eligible
  replay source. A byte-copied payload record can never be the fence.
- **Frozen decision: a save-time materialization witness in the v3 envelope
  manifest.** The current engine writes a new `C1ZManifestV3` field carrying
  `sourcecache.MaterializationPolicyGeneration` on EVERY envelope save. The
  eligibility gate (G5) requires the previous artifact's witness to equal
  the current constant before installing a warm lookup.
- Why this fences: every committed old-SDK write session — fold included —
  must rewrite the c1z envelope, and it rebuilds the manifest from its own
  proto descriptors, which do not know the field. The witness is therefore
  necessarily ABSENT from any old-fold output, and the old fold cannot
  forge it. The payload compat record's `sdk_materialization_generation`
  (B4) is still byte-matched, but the envelope witness is the fence. This
  instantiates CO-017's "fold-disturbance witness" candidate; the engine
  name stays `pebble3`, so old SDKs retain read access to scoped artifacts.
- Collateral (accepted): ANY old-SDK write session against a scoped
  artifact drops the witness and colds the chain — correct, since any
  old-SDK write makes the source-cache state untrustworthy.
- Pinned by a test that simulates the old-fold shape: scoped artifact →
  envelope rewritten without the witness while the payload (manifest
  entries, indexes, counts, compat record) survives byte-identical and
  `compacted` stays false → the gate must degrade cold with the fence
  reason observable.

### B9 — CO-016 graceful degrade (Rider B, no implementation)

- Connectors using `ExternalResourceMatch*` annotations get their
  placeholder-grant scopes poisoned every sync by design (destructive
  reconciliation). Orchestration inherits the correct behavior with no new
  code: poison ⇒ store-level lookup miss ⇒ `NoopLookup`-equivalent verdict
  for that scope ⇒ cold fetch, which is pre-replay behavior. The rewritten
  `sourcecache.go` docs and this plan say so explicitly; the non-destructive
  reconciliation rework is a separate future PR.

### B10 — Explicit exclusions (registered)

- Lambda ask/answer continuation: `SourceCacheLookupOffer` is never attached
  to requests, `SourceCacheLookupAsk`/`SourceCacheLookupAnswers` are never
  consumed/produced by 6b orchestration. Deferred to 6c; registered in the
  executable exclusions registry.
- Static entitlements stay unscoped (B3); derived rows (expansion output,
  reconciliation writes) stay unscoped (B3).
- `ResourceTargetedSyncer.Get` and event feeds carry no source-cache
  semantics.

## Oracles

- **OR1, gate/trace conformance**: for every gate-matrix cell, the observed
  outcome (warm vs cold lookup, hit/miss trace events, degradation reason)
  equals the decision-table row. Instrument: chaos trace lookup events plus
  gate logs.
- **OR2, cold-baseline differential**: a warm run's final logical content
  equals an independent cold baseline of the same final epoch
  (`CompareLogicalContent` full-proto fingerprints). The sync-level
  analogue of 6a's O2.
- **OR3, manifest/stamp observation**: after a sync, every expected scope
  has exactly the expected manifest entry (validator value, zero-row scopes
  included) and annotated pages' rows carry the expected stamps; unexpected
  scopes have none. Instrument: oracle reads over the produced artifact.
- **OR4, error-identity oracle**: failure cells surface `ErrReplayIntegrity`
  with the frozen verdict via `errors.As`, and loud-failure cells fail the
  sync rather than degrade.
- **OR5, generational parity**: across A→B→C against an unchanged upstream:
  (a) per-scope lookup hit-rate parity between B and C — any delta is the
  finding regardless of cause; (b) zero fresh fetches in C for unchanged
  scopes (connector-side fetch counters); (c) manifest parity — every scope
  entry in B's artifact present in C's, zero-row scopes included;
  (d) validator provenance — delta-style validators rotate every
  generation, etag-style carry unchanged.
- **OR6, checkpoint/resume conformance**: interruption cuts around every
  replay stage resume to OR2 equivalence with no manifest entry claiming a
  validator over an incomplete scope; the provenance/replayed sets restored
  from the checkpoint behave identically to uninterrupted state.

## Criteria

Each criterion names its oracle(s) and coverage level. R7 closes 6a C29 at
orchestration level; R4 closes 6a C30.

- **R1 Lookup lifecycle** — capability parsed each attempt; warm/Noop
  installed per B2; `SetSourceCache(nil)` on every exit; `SyncOpAttrs.Lookup`
  populated at all four builder sites, `NoopLookup` when unset; a late call
  after teardown observes no stale lookup. Oracle: OR1 + unit assertions.
  Coverage: bounded (all four sites; direct and in-memory-gRPC chaos
  transports).
- **R2 Gate decision table** — every reachable G1–G7 cell produces exactly
  the frozen outcome, warm or degrade-with-reason; strict-open stays hard.
  Oracle: OR1. Coverage: bounded matrix — capability
  {absent, disabled, read-write} × previous source {none, SQLite,
  unfinished, non-FULL, compacted, quality-blocked, fence-stripped,
  usable} × compat {match, each-field-mismatch, record-absent}, pruned to
  reachable combinations with every gate condition falsified at least once
  in isolation.
- **R3 Fresh-page semantics** — stamping exactness (only annotated pages'
  rows), manifest publish on non-empty validator including zero-row scopes,
  per-page tombstones after puts, empty-validator rounds publish nothing,
  static-entitlement annotations ignored with warn. Oracle: OR3 + OR2.
  Coverage: bounded over row kinds × {single-page, multi-page,
  zero-row, tombstoned} cells.
- **R4 Compat lifecycle (closes C30)** — record written at the frozen time
  with the frozen fields; resume idempotence; drift-on-resume blocks replay
  and degrades per B4; fold/rebuild/cleanup lifecycle inclusion. Oracle:
  OR3 (record bytes) + OR1 + lifecycle assertions. Coverage: bounded.
- **R5 Compat byte-match** — any single-field mismatch degrades; empty
  matches only empty; missing record degrades. Oracle: OR1. Coverage:
  bounded per-field cells (inside the R2 matrix).
- **R6 Quality consume gate** — blocked or stats-absent previous artifact
  degrades with reason; eligible artifact passes; the run inspected is the
  run G3 selected; a chaos run that trips an ingest-filter drop, seals, and
  is offered as the previous source degrades with the block reason in the
  trace. Oracle: OR1. Coverage: bounded.
- **R7 Replay semantics (closes C29 at orchestration)** — frozen page order
  (copy → upserts → tombstones), cross-page order preserved, copy dedup per
  scope, `overlay=false` tolerance, validator publish on either path,
  cross-kind non-aliasing driven by RPC kind. Oracle: OR2 + OR3.
  Coverage: bounded over kinds × {pure replay, replacement, delta overlay +
  tombstones, zero-row, same scope across kinds} — the collection-semantics
  range.
- **R8 Provenance enforcement** — replay without a this-sync hit fails
  loudly cold (including replay-while-cold); planning-call → spawned-cursor
  handoff accepted; hit-set and replayed-set survive interrupt/resume;
  post-sync teardown clears connector-visible lookup. Oracle: OR4 + OR6 +
  OR1. Coverage: bounded, including at least one cross-process-shaped
  resume (new syncer instance over the checkpointed state).
- **R9 Verdict taxonomy** — every orchestration replay failure path carries
  the frozen warm/cold classification, surfaced via `errors.As`. Oracle:
  OR4. Coverage: bounded enumeration of every failure path named in B7.
- **R10 CO-017 fence** — new saves carry the witness; the simulated
  old-fold shape (payload intact, witness absent, `compacted` false)
  degrades cold with the fence reason; witness mismatch (future bump)
  degrades. Oracle: OR1 + artifact inspection. Coverage: bounded.
- **R11 Interruption/resume** — cuts after replay copy, after overlay,
  after tombstones, and around manifest publish resume and converge to the
  cold baseline; a cold resume (previous artifact withdrawn at resume)
  converges too; one expansion-enabled scenario proves expander-written
  Sources on direct grants are stripped at copy and recomputed while
  connector-set Sources survive byte-for-byte. Oracle: OR6 + OR2 + OR3.
  Coverage: bounded cut enumeration on a deterministic scenario.
- **R12 Ordering/pagination adversary** — duplicate pages, duplicate
  cursors, lost-response retries, and epoch drift between retries preserve
  OR2 equivalence and single-copy semantics. Oracle: OR2 + OR3 + trace.
  Coverage: bounded adversarial cells; corpus breadth is nightly.
- **R13 Generational steady state (first-class; own closure entry)** —
  A cold → B warm → C warm against a deterministic unchanged upstream:
  OR5(a–d) in full; adversarial cells: B interrupted-and-resumed still
  seeds C (the conservative replay-blocked default must not convert
  routine resumes into chain breaks); B with genuine quality loss blocks C
  loudly (trace shows the reason), never silently. This suite is the only
  instrument observing replay's producer half — a replayed scope that
  fails to republish its manifest entry alternates hit/miss per generation
  with green syncs, and only this criterion catches it. It is not folded
  into R7's range. Oracle: OR5 + OR1 + OR2. Coverage: bounded
  three-generation chain per-PR; wider corpora nightly.
- **R14 Degradation composition** — poisoned scopes (CO-016 shape) read as
  misses and cold-fetch inside an otherwise-warm sync; stale validators
  (upstream changed between B and C) produce fresh fetches, not replays,
  and the artifact converges to the cold baseline. Oracle: OR2 + OR1.
  Coverage: bounded.

## Closure rules

- Every criterion above needs a closure entry in `evidence.md` naming its
  instrument(s) and observed outcome; R13 gets its own entry, never merged
  into R7's.
- The two skipped ETag tests in `pkg/sync/pebble_etag_replay_test.go` are
  replaced by the suites above (their intent — validator carry across syncs
  and previous-sync row carry-forward — is subsumed by R7/R13); the
  `C30-compatibility-record-lifecycle` exclusion is un-skipped/replaced per
  R4; the exclusions registry drops orchestration-deferred entries and adds
  the ask/answer continuation, static-entitlement, and derived-row
  exclusions (B10).
- The `NOT YET WIRED` block in `pkg/sourcecache/sourcecache.go` is rewritten
  as current behavior once — and only once — R1–R9 instruments pass.
- Tiering: deterministic single-scenario suites run per-PR; large corpora
  (R12 breadth, extra R13 generations) go nightly via
  `BATON_TEST_EXTRA`/`BATON_TEST_NIGHTLY`; every new suite family keeps at
  least a smoke slice in per-PR CI, and `make chaos-check` gains named
  representatives.
- Gates before signoff: full suites, `make lint`, focused `-race` on the new
  orchestration and chaos suites, `make chaos-check`, then three
  independent model code reviews with findings verified before fixes.

## Baseline placement map (pre-implementation)

- Gate + lookup install: `pkg/sync/syncer.go` — `NewSyncer` previous-artifact
  gate (G4/G5 additions), `Sync` post-Validate (G6/G7, install), sync-end
  teardown; hit-set/replayed-set in `pkg/sync/state.go` token state.
- Delivery: `internal/connector/connector.go` (`connectorClient` forwarder,
  mirroring `SetSessionStore`); `pkg/connectorbuilder/connectorbuilder.go`
  (builder implements `sourcecache.SetLookup`);
  `pkg/connectorbuilder/resource_syncer.go` (4 `SyncOpAttrs` sites);
  `pkg/types/resource/resource.go` (`SyncOpAttrs.Lookup`).
- Page handling: `pkg/sync/syncer.go` — `syncResources`,
  `syncEntitlementsForResource`, `syncGrantsForResource`, static-entitlement
  path (ignore+warn).
- Compat record: new key under the source-cache family in
  `pkg/dotc1z/engine/pebble/internal/rawdb/` + engine/store accessors in
  `pkg/dotc1z/`; writer in the syncer.
- Fence witness: `proto/c1/c1z/v3/manifest.proto` field +
  `pkg/dotc1z/engine/pebble/manifest.go` (write) + gate read via the
  manifest header path.
- Verdicts: `pkg/sync` error types near `ingest_invariants.go`'s ladder.
- Harness: `internal/chaosconnector/` builder capability emission, page
  model `(kind, scope, validator, mode, tombstones)`, real lookup from
  `SyncOpAttrs.Lookup` with planning-handoff shape, trace lookup events,
  oracle manifest/stamp/fetch-count observations.
- Suites: `pkg/sync/chaos_source_cache_*_test.go`.

## Change-order log

(Append-only. Deviations from the frozen contract discovered during
implementation are recorded here as CO-6b-NNN entries with mechanism, reason,
and verification delta.)

### CO-6b-001 — Lookup delivery is in-process only

- Type: scope boundary discovered during implementation; no contract change
  for in-process connectors.
- Mechanism: B1 specifies delivery mirroring the session-store pattern
  (`internal/connector.connectorClient` forwards to a builder-side setter).
  The session-store pattern works because the wrapper and the builder share
  a process in the in-process configurations. When the runner spawns the
  connector as a SUBPROCESS, `SetSourceCache(lookup Lookup)` cannot cross
  the process boundary — there is no RPC backchannel for an interface
  value, and building one (a reverse-direction lookup service) is new
  protocol surface out of 6b's scope.
- Behavior: subprocess-wrapped connectors observe `NoopLookup` for the
  whole sync and serve cold, which is the frozen degrade default (B2's
  optional-degrade). Delivery requires the transport that constructs the
  connector client to wire a live in-process setter behind
  `SetSourceCache`; in-tree, only the chaos harness's clients (direct and
  in-process gRPC) do. There is NO wired production path in this phase —
  `internal/connector.NewWrapper` never wires the setter (amended per
  independent review; the original entry overclaimed "the production
  in-process path").
- Deliverability probe (amendment, CO-6b-003 remediation): a client can
  satisfy `sourcecache.SetLookup` structurally while wrapping a transport
  that cannot forward an interface value. The syncer probes the optional
  `sourceCacheLookupDeliverable` interface before install and keeps the
  consume side cold when the transport cannot deliver, so an unwired
  `SetLookup` is never logged or treated as warm. The produce side
  (stamping, validator publish) still runs so such syncs seed future warm
  syncs.
- Verification delta: R1's transport coverage is direct + in-process gRPC
  via the chaos harness clients ONLY; the subprocess boundary and the
  absence of a wired production path are registered in the Phase 6b
  executable exclusions registry
  (`pkg/sync/source_cache_exclusions_verification_test.go`). The lambda
  ask/answer continuation (6c) is the planned mechanism for cross-process
  lookups.

### CO-6b-002 — Resume granularity: interrupted actions restart at their root

- Type: discovered behavior pinned as contract; no code change.
- Mechanism: the syncer's checkpoint restores the action stack, and an
  interrupted paginated action re-executes from its ROOT page token —
  mid-chain wire tokens are not resume points. Replay-relevant page
  processing is therefore at-least-once across a resume: the restarted
  root re-consults the lookup, and the checkpoint-durable hit/replayed
  sets act as idempotency guards (the re-walked replay pages skip the
  replacement copy) rather than as mid-chain resume state.
- Reason: OR6 as frozen said the restored sets "behave identically to
  uninterrupted state"; the discovered granularity satisfies that oracle
  but makes one of its motivating shapes (a replay annotation resumed
  mid-chain with no fresh consult) unreachable in practice.
- Verification delta: `TestChaosSourceCacheInterruptResume` pins the
  granularity explicitly (the withdrawn-at-resume cell asserts the warm
  chain is never revisited; the warm-resume cell asserts exactly one
  re-consult) alongside OR2 convergence for every cut × resume shape.

### CO-6b-003 — Review remediation: shape gates, warm provenance gate, copy atomicity

- Type: contract tightening in response to three independent correctness
  reviews (two REJECT, one ACCEPT WITH LIMITATIONS); each item closes a
  silent-wrong-data or fail-open path the frozen contract implied but the
  first implementation did not enforce.
- Sync-shape gate: source-cache handling is untargeted-FULL-sync only.
  Partial, resources-only, and targeted syncs collect less than the full
  inventory, so their pages must not stamp rows or publish validators and
  their lookup stays cold — a warm replay copy would import a whole
  scope's previous rows into a store that never selected them
  (`sourceCacheEnabled`). A resume attempt whose capability is withdrawn
  or whose shape stops qualifying, over a store that already carries
  produce state from a prior attempt, blocks the artifact as a future
  replay source (same mixed-generation hazard as compat drift;
  fail-closed on compat-read errors).
- Warm provenance gate: `beforeUpserts` consults the attempt-scoped
  `sourceCacheWarm` flag (set only after a deliverable warm lookup is
  actually installed) before any replay copy. A checkpointed hit-set
  restored into a cold or compat-drifted resume no longer re-authorizes
  replay; the annotation fails cold `ErrReplayIntegrity` (plan B4/B5
  enforcement the original implementation left as a dead field).
- Copy atomicity: the decide-copy-mark replay sequence holds a per-
  `(rowKind, scopeKey)` mutex, closing the check-then-act window that
  duplicated replacement copies (and could resurrect replaced overlay
  rows) at `WithWorkerCount > 1`.
- Unreplayable-shape produce guard: a record-annotated resources page
  whose rows declare child resource types, and a grants page carrying
  `InsertResourceGrants`, mark the artifact replay-blocked
  (`ingestQualityReasonSourceCacheShapeUnsupported`) — replaying such a
  scope would silently lose the derived rows because replay copies only
  the scope's own partition. `EnqueuePageTokens` on an annotated
  resources page warns loudly as a registered boundary.
- Verdict-classification tightening: ambient `ctx.Err()` no longer
  promotes replay-copy failures to warm (only the error chain
  classifies); row-put failures on annotated pages wrap as warm
  `ErrReplayIntegrity`; duplicate same-type source-cache annotations on
  one page fail cold; resource tombstones must parse as Baton resource
  BIDs before deletion.
- Verification delta: new instruments
  `TestChaosSourceCacheDriftedResumeRejectsRestoredReplay` (warm gate on
  drifted resume), `TestChaosSourceCacheDuplicateReplayCursorsParallel`
  (copy atomicity at worker count 4),
  `TestChaosSourceCacheUnsupportedShapesBlockReplaySeed` (produce
  guards), `TestVerificationReplayVerdictSentinelIdentity` (engine-level
  proof that real destination-commit failures carry the warm sentinel and
  source-side failures do not), plus taxonomy cells for the sync-shape
  gates, duplicate annotations, put-failure wrapping, non-BID tombstones,
  and materialization-only compat mismatch.

### CO-6b-004 — Hit-validator binding: a replay copies only the base its hit came from

- Type: contract tightening from the PR-review round (automated reviewer,
  zero blocking findings; this was its load-bearing suggestion).
- Gap: the same-sync provenance rule recorded hits as bare
  `(rowKind, scopeKey)` pairs, and no consume gate identifies WHICH
  artifact a hit came from — two artifacts from the same connector and
  config carry byte-identical compat keys, so a previous artifact swapped
  between attempts (service-mode spare replaced by a rollback/restore)
  passes G1–G7 while its rows for a scope may predate the state the
  connector actually revalidated. A checkpointed hit would then authorize
  a replacement copy from a base the connector never consulted: silently
  stale rows in a green sync.
- Mechanism: the hit map now records the validator the lookup returned
  (`row kind → scope key → validator`, checkpoint shape changed
  accordingly), and `beforeUpserts` requires the CURRENT replay base's
  manifest entry to byte-match the recorded validator before the copy
  runs. Mismatch, absent entry, read failure, and a base without the
  entry surface are all cold `ErrReplayIntegrity` — the copy's provenance
  cannot be established, fail-closed. Validator equality is the right
  binding: it is exactly the value the connector's replay verdict was
  computed against.
- Also recorded from the same round: the stuck-resume operational
  contract (a cold verdict inside a checkpointed cursor fails every
  resume deterministically until the caller's retry policy abandons the
  unfinished sync; 6c's ladder automates the cold fallback) is documented
  on `beforeUpserts`; the provenance sets' checkpoint cost curve is
  documented on the state field and pinned by
  `BenchmarkStateMarshalSourceCacheSets` (~151KB token / ~0.5ms at 1k
  scopes, ~15MB / ~102ms at 100k — sidecar persistence is the escape
  hatch at whale scale).
- Verification delta: taxonomy cells `swapped-base-validator-mismatch`,
  `swapped-base-entry-missing`, `base-entry-read-failure`,
  `base-without-entry-surface` (provenance-cold-paths); every existing
  warm replay instrument now passes THROUGH the binding check against
  real artifacts, so deleting the check cannot pass the chaos suites
  vacuously — but the mismatch cells are what fail if the comparison is
  removed.

### CO-6b-005 — Re-review remediation: mutation-adequate instruments and the probe pin

- Type: verification hardening from the second independent-review round
  (two reviewers re-audited the CO-6b-003 remediation; both accepted the
  warm-gate and produce-guard closures and rejected two instruments as
  vacuous — reverting the fix they claim to pin left the suite green).
- Deliverability probe (closes the re-review's finding on the original
  BLOCKER): the probe interface is promoted to
  `sourcecache.LookupDeliverabilityProbe` so the production client can
  compile-pin it (`var _` in `internal/connector`) — a method rename can
  no longer silently sever the probe from the syncer's type assertion.
  New instrument `TestSourceCacheLookupDeliverabilityProbe`: a
  structurally-satisfied `SetLookup` client whose probe reports
  undeliverable receives NO delivery and the sync stays cold; deliverable
  and probe-less clients receive the (possibly nil) lookup plus teardown.
  Mutation-verified: disabling the probe type-assert fails the
  undeliverable cell.
- Per-scope lock (closes the re-review's finding on the TOCTOU MAJOR):
  the parallel-cursors chaos test admits its spawned duplicates only
  after the parent page has marked the scope replayed, so it serializes
  on the already-replayed skip, not on decide-copy-mark. New instrument
  `TestSourceCacheReplayOncePerScopeIsAtomic` holds one copy mid-flight
  in a blocking store and overlaps a second `beforeUpserts` for the same
  scope: with the mutex the second parks and skips; without it the
  second reaches the store inside the observation window.
  Mutation-verified: removing the lock fails the test.
- Capability withdrawal on resume now has its cell: the compat-drift
  resume suite gained `capability-withdrawn` (attempt 1 warm under
  gen-1, resume declares nothing) — the stale produce state blocks the
  artifact and the resume completes cold.
- Cost pins requested by the re-review: `BenchmarkSourceCacheScopeLocks`
  (per-scope mutex: ~326ns/157B first touch, ~42ns steady state;
  entries retained for the syncer's one-sync lifetime, cardinality equal
  to the provenance sets) alongside CO-6b-004's
  `BenchmarkStateMarshalSourceCacheSets`.
- Honesty deltas: `pkg/sourcecache` seedability claim now carries the
  quality-block exception; the row-put wrapper's call-site wiring is
  recorded as an explicit verification gap (no store-fault injection
  seam this phase) rather than claimed as covered.

### CO-6b-006 — Third re-review remediation: corrected warm-gate premise, record-page lock, token schema fence

- Type: bug fixes and verification hardening from the third independent
  re-review (adversarial pass with mutation testing against `b617d6d8`).
- Warm-gate chaos instrument premise was FALSE (review finding, confirmed
  by mutation on HEAD): in the two-team drifted-resume scenario the
  consult and the crash sat in the same dispatch batch, and provenance
  recorded during a batch becomes durable only at the checkpoint atop the
  NEXT loop iteration — so no surviving checkpoint ever contained the
  hit, and the resume rejected on the ordinary no-hit gate while the
  warm gate went untested (the unit cell still killed the mutation; the
  chaos instrument did not). Corrected: the scenario now spans two
  dispatch batches (102 per-resource grants actions against
  `maxPeekActionsCount` = 100, resource ids chosen for the store's
  lexicographic drain order), and the test CAPTURES the surviving
  checkpoint and asserts both premise halves in-band — the hit is in it
  and the carrier's replay action is still pending. Mutation-verified:
  deleting the warm gate now fails the chaos instrument (the restored
  hit plus the still-eligible seed base let the copy proceed).
  This also settles the durability question the review raised: hits DO
  checkpoint at batch boundaries; hits recorded in the batch that
  crashes are lost and their actions re-run from the pre-batch
  checkpoint, re-consulting on resume (at-least-once, safe in both warm
  and cold resume permutations).
- Record-only pages now hold the scope lock (review finding N1, MAJOR):
  `beforeUpserts` returned before locking when the page carried no
  replay annotation, so a record page's row puts, tombstones, and
  manifest publish could interleave with another action's in-flight
  REPLACEMENT copy for the same scope — the copy deletes the scope's
  rows before copying the base, silently wiping the fresh rows or
  publishing a validator over an incomplete scope. Fixed: every scoped
  page acquires the scope lock in `beforeUpserts` and releases it in
  `afterUpserts`, with an idempotent `release()` deferred at all three
  handler call sites so error paths between the two cannot leak the
  lock (a leaked lock would deadlock the action's own retry). New
  instrument `TestSourceCacheRecordPageParksBehindReplayCopy`
  (mutation-verified: restoring the early return fails it inside the
  observation window); the once-per-scope instrument updated for the
  extended lock lifetime.
- Sync-token schema fence (review finding N5, process-blocking):
  CO-6b-004 changed the checkpointed hit map's value shape under the
  SAME JSON key (`source_cache_hits`), and the token unmarshal fell back
  to the v0 format on ANY parse error — a token written by the previous
  commit and read by the new code would misparse as v0 and silently
  drop the v1 action stack. Fixed twice over: the reshaped map takes a
  NEW key (`source_cache_hit_validators`; the retired key is ignored,
  degrading legacy hits to loud cold replays, never corruption), and
  `state.Unmarshal` no longer falls back to v0 when the input declares a
  version AND fails to parse as v1 — such tokens now fail loudly. (A
  token that parses cleanly but carries an unrecognized version still
  takes the v0 fallback: that is the pre-version token format's
  compatibility path, unchanged. The fence targets the misparse hazard —
  a same-key shape change breaking the v1 parse — not version-unknown
  inputs; wording narrowed per round-4 review.) Instruments:
  `TestSyncerTokenSourceCacheSetsRoundTrip` (review finding N3 — the
  provenance sets round-trip Marshal → Unmarshal exactly) and
  `TestSyncerTokenSchemaFence` (retired key ignored, versioned parse
  failure loud, version-less v0 fallback preserved).
- Mutation adequacy re-verified this round: warm gate (chaos + unit),
  record-page lock, capability-withdrawal block (its cell from
  CO-6b-005 kills disabling the block — closing the review's surviving
  mutant), once-per-scope mutex, deliverability probe.

### CO-6b-007 — Round-4 remediation: held-lock ride-along, coverage triage, witness pin

- Type: verification hardening from the fourth independent-review round
  (three reviewers against `c33f698e`; all seven CO-6b-006 closure claims
  independently confirmed mutation-adequate by all three).
- Held-lock ride-along (closes the round's one new MAJOR — the deferred
  `release()` backstop CO-6b-006 introduced had no instrument, and the
  evidence misattributed its coverage to the record-page parking test,
  which only exercises `afterUpserts`): the chaos fixture itself now
  registers a sync-end cleanup that walks `sourceCacheScopeLocks` and
  fails any test that ends with a scope lock still held. This is the
  ladder climb the recurrence rule prescribes (third uninstrumented
  resource-release obligation on this branch): every present and future
  chaos suite evaluates the invariant, including call sites added later.
  The loud-failure suite additionally grew one cell per collection
  handler (resources, entitlements, grants — previously grants-only), so
  each handler's `defer scOps.release()` has a killing scenario.
  Mutation-verified per site: removing any one handler's defer fails
  that handler's cell at the ride-along assertion.
- Structural-coverage triage (closes the round's process MAJOR): the
  handbook's coverage-driven step for HIGH changes — profile across all
  changed packages, intersect with the diff, disposition every uncovered
  changed block — is now recorded in evidence.md, with three new unit
  instruments for the boundary branches the triage judged worth real
  tests (unparsable capability, warm-lookup input validation, record on
  a surfaceless store).
- Witness compile pin (MINOR): the G5 fence's inline anonymous interface
  is promoted to `sourcecache.MaterializationWitnessReader` with a
  `var _` pin on the pebble store — same discipline as the CO-6b-005
  probe promotion; a method rename is now a build break, not a
  behavioral-test catch.
- Probe assertion honesty (MINOR): the syncer now asserts the exported
  `sourcecache.LookupDeliverabilityProbe` directly; the private duplicate
  interface it actually used (making the compile-pin claim an
  overstatement) is deleted.
- Excluded permutation registered (MINOR): a connector whose emitted
  resource-type list shrinks between generations without a
  `cache_generation` bump can replay rows the fresh-ingest filter would
  drop; registered as an executable exclusion with its containment
  rationale (connector-side capability-contract violation; warn-class
  ingest invariants surface the dangling references) rather than gated.
- Wording deltas (LOW): the token fence's claim narrowed to
  parse-failure-only (a cleanly-parsed unknown version still takes the
  v0 fallback — the pre-version compatibility path); the performance
  evidence now states the extended lock-hold contract for annotated
  pages; the scope-lock doc comment updated to the extended cardinality.
- Operational note kept as documentation (LOW, review N-5): a genuinely
  corrupt same-version token now fails every resume attempt loudly
  rather than degrading to redone work — same operational contract as
  the cold-verdict retry-livelock note on `beforeUpserts` (the caller's
  retry policy abandons the token and a fresh sync starts cold); an
  in-tree abandon-the-token path is Phase 6c runner-ladder scope.

### CO-6b-008 — Record-round grounding: the verdict-flip phantom union, witnessed and fixed

- Type: live defect, predicted by the formal model (walker calibration
  scenario 1, tc1c flavor — `formal/walker/CALIBRATION.md`), witnessed
  against the shipped syncer, fixed.
- Premise (the verdict-flip path): a warm round's replay copy commits,
  the sync crashes before the round's validator publishes (a round runs
  inside one batch, so no checkpoint can intervene — CO-6b-002's
  granularity), upstream moves between attempts, and the resume's
  consult misses — the connector serves a fresh RECORD round. The
  record round's upserts landed on the crashed attempt's copied debris;
  rows departed upstream sealed under the fresh validator, which the
  NEXT sync's consult validates clean and replays forward (the
  non-self-healing direction).
- Fix: a record round is a replacement listing, so before its first
  write to a scope this attempt, a partition holding rows that no
  completed round published (no manifest entry this sync) is cleared.
  Store surface `ClearSourceCacheScope` (the replay unit's clear leg
  standalone, bounded batches, idempotent); orchestration
  `groundRecordScope` under the scope lock; attempt-local grounded set
  (deliberately volatile — a resume re-decides from durable facts) so
  replay and record pages of one round never re-ground over each other.
  Published entries exempt the scope, preserving multi-action
  accumulation into shared scopes.
- Witness/regression: `TestChaosSourceCacheRecordFlipOverReplayDebris`
  (fails against the pre-fix code with the union sealed; passes with
  the fix) pins both the outcome (content oracle: the departed row is
  absent) and the mechanism (the resumed attempt's trace shows the
  grounding clear with no replay copy). Trace-visible: record rounds
  now emit a real clear — "replacement rounds clear first", previously
  granted structurally by the oracle's renderer, now witnessed
  (fixtures `cold_record_sync.jsonl`,
  `warm_replay_sync_record_flip.jsonl`).
- Scope note: the model's verified V-ATOMIC/V-OVERLAY-UNIT fix family
  targets designs with durable marker suppression; this code base heals
  by re-execution (root restart + idempotent re-copy, CO-6b-002), so
  grounding was the missing piece, not unit-mode commit.

### CO-6b-009 — Session store across resume: hazard analysis; resume-clear REJECTED

- Type: hazard analysis with contractual pins. A mechanical fix
  (wholesale namespace clear on a participating resume,
  `groundSessionStoreOnResume`) was briefly shipped and REVERTED —
  see the rejection rationale below before reintroducing anything
  like it.
- Premise (verified in code, not model-derived): connector
  session-store writes are durable in the artifact, keyed by sync id,
  and commit OUTSIDE the checkpoint mechanism (`SessionSet` commits its
  own batch; `CheckpointSync` is a separate write). After a crash the
  resumed attempt's cursor rolls back to the last checkpoint while
  every session write survives — the resumed attempt inherits the dead
  attempt's session state wholesale, and the connector cannot detect
  the resume (its process restarted; sessions are the only surviving
  state).
- The two-sided hazard:
  - Writes from BEYOND the restored cursor survive into work that
    re-runs: the re-run window can observe its own dead attempt's
    "future" writes, so session-based once-only decisions (dedup,
    "already handled" markers) silently drop work.
  - Under the protocol, session caches derived from replay-era rounds
    can feed rounds whose rows a resume re-grounds (CO-6b-008 clears
    the row partition; nothing re-validates session state derived from
    it).
- Why the resume-clear was rejected: resume restores the action queue
  from the checkpoint and COMPLETED actions never re-run, so a
  wholesale clear destroys session values whose producing work will
  not execute again (accumulate-then-consume: an index built during a
  completed action, consumed by a later one, is unrecoverable). The
  clear also rewrote the session contract to match the fix
  ("attempt-scoped") rather than fixing to the contract.
- Correct mechanical fix (future work, not scheduled): checkpoint-
  consistent sessions — session mutations land in a volatile overlay,
  reads merge overlay-over-durable, and the overlay flushes in the
  SAME batch as the checkpoint write. Crash then restores sessions to
  exactly the checkpoint's state: future-writes vanish (hazard a),
  completed-action caches persist, and the re-run window regenerates
  its own writes as the work re-runs. (Epoch-tagging entries and
  purging on resume cannot undo deletes/overwrites without a value
  journal, which converges to the overlay.) Candidate for RFC 0011
  scope, where session reads become stamped observation points.
- Current stance — contractual, pinned in `pkg/sourcecache` and
  `pkg/session/README.md`: session use must be safe under
  at-least-once re-execution with prior state present (no once-only
  decisions); replay/record verdicts must come from upstream evidence,
  never session-cached answers; session state built while generating
  rows is silently partial for replayed scopes.
- Witness: `TestChaosSourceCacheSessionPersistsAcrossResume` — a probe
  key planted in the interrupted sync's session namespace survives the
  resume with AND without the source-cache capability, pinning the
  persistence semantics and standing guard against reintroducing a
  resume-time clear.
