# Sync Replay Phase 6a Verification Plan

Status: frozen implementation-blind baseline, with append-only change orders below.

This plan was derived before inspecting the Phase 6a implementation or its tests.
The placement map was added only after the criteria, oracles, coverage model, and
closure rules were frozen.

This repository copy was added retroactively when verification plans became
first-class repository artifacts. Its preregistration provenance is the original
plan record at
https://gist.github.com/kans/12e274d4de4bdff4e3a84cd6958498e0; unlike future
plans, its first repository commit is not the freeze point.

## Guardrails and risk verdict

- Inputs used for the frozen core: `docs/BUG_CATCHING.md`, the supplied Phase 6a
  contract/boundary, `proto/c1/connector/v2/annotation_source_cache.proto`,
  `proto/c1/storage/v3/records.proto`, and
  `proto/c1/connectorapi/baton/v1/source_cache.proto`, each read from `main` with
  `git show main:<path>`.
- Amendment 1 adds eight implementation-blind criteria supplied after the initial
  freeze: source immutability, manifest reconciliation, empty-validator
  transitions, timestamps, iterator/cancellation failures, hostile encodings,
  page ordering, and the compatibility-family lifecycle boundary. The amended
  core is frozen again without inspecting Phase 6a implementation or tests.
- Amendment 2 adds eight more implementation-blind gaps: occupied-destination pure
  replay, inconsistent sources, forward cacheability, transitional non-overlay
  rows, unsupported/unusable sources, combined tombstones, FileOps/clone
  disposition, and live counters. The core is frozen again before any Phase 6a
  implementation inspection.
- Amendment 3 is the final expansion: wrong-kind hits, invalidated entries,
  first-class prefix neighbors, illegal tombstone combinations, and
  replay-from-self. The amended verification space is now closed to further
  preregistration changes unless the Phase 6a contract itself changes.
- Phase 6a is HIGH risk: failures can be silent and durable; behavior is a product
  of row kind, scope, prior/current artifact state, mutation path, retries, faults,
  and volume; direct correctness requires a constructed two-artifact oracle.
- Pebble is the executable target. SQLite is checked only for explicit, clean
  absence of the optional capability; no new cross-engine parity requirement is
  introduced.
- Deferred and excluded from 6a closure: syncer/checkpoint orchestration,
  compatibility matching, connector continuation/RPC behavior, the policy that
  decides when to set `invalidated`, compacted/non-FULL source eligibility,
  compactor integration, and post-replay ingest-invariant evaluation. Phase 6a
  must consume an already-invalidated entry as non-replayable, but must not invent
  invalidation or ingest-invariant policy.
- CO-013 later changed the Phase 6a contract and supersedes the compacted/non-FULL
  eligibility and compactor-invalidation portions of this historical frozen
  boundary. Compatibility matching and the other listed orchestration remain
  deferred.

## Frozen core: stage claims

- S1 — Connector-written resources, entitlements, and grants persist the exact
  connector-supplied `source_scope_key`; unscoped and derived rows remain unscoped.
- S2 — Every relevant typed rawdb primary-row put, overwrite, and delete atomically
  maintains the matching source-scope index and all pre-existing row obligations.
- S3 — A durable manifest entry is maintained per `(row_kind, scope_key)`,
  including zero-row scopes, without cross-kind or cross-scope aliasing.
- S4 — Replay copies exactly one selected `(scope, row_kind)` from a previous
  Pebble artifact into the current sync.
- S5 — Replay is scope-isolated, retry-safe, durable across reopen, bounded-memory,
  and semantically equivalent to materializing the same final upstream state
  directly.
- S6 — Overlay replacement handles base-only, current-only, and colliding
  identities; obsolete index entries disappear and required store-managed side
  state survives according to the direct-materialization oracle.
- S7 — Canonical-ID and contract-valid principal tombstones remove only selected
  in-scope rows and every obligation owned by those rows; absent/repeated
  tombstones are harmless.
- S8 — reset/cleanup, reuse, abort, reopen, and bulk import neither leak nor omit
  the new record/index families.
- S9 — The dotc1z store exposes an explicit source-cache capability; Pebble
  satisfies it and unsupported engines degrade by capability absence, not partial
  behavior.
- S10 — The previous/source artifact is immutable under successful, failed,
  cancelled, repeated, and concurrent replay attempts.
- S11 — Terminal manifest state reconciles with scoped rows, while zero-row scopes
  remain representable; empty validators are transitional and never become a
  false completed claim.
- S12 — Overlay page rows are upserted before that page’s tombstones, with
  deterministic ordering across pages.
- S13 — Compatibility key families obey Phase 6a lifecycle ownership even though
  compatibility matching/gating semantics remain deferred.
- S14 — Pure replay has replacement semantics for an already-populated destination
  scope: its terminal row set is exactly the source scope, not a union with
  destination-only rows.
- S15 — A structurally inconsistent or unusable source fails closed before
  publishing a replay result; unsupported engines never degrade into an empty or
  partial replay.
- S16 — A successfully replayed current artifact is itself a valid future replay
  source: stamps, indexes, manifest, and validator survive a second replay hop.
- S17 — Canonical and principal tombstones on one page compose as a union after
  that page’s upserts, deleting a matching row once without collateral damage.
- S18 — Clone/FileOps and live counters have an executable preserve/recompute
  contract or an explicit executable exclusion; neither is left implicit.
- S19 — Scope identity is the exact pair `(row_kind, scope_key)`; same-key/wrong-kind
  and prefix-neighbor entries never alias.
- S20 — Already-invalidated manifest entries are non-replayable and produce no
  destination mutation, independent of the deferred policy that created the flag.
- S21 — Illegal tombstone selector/kind combinations and replay-from-self are
  rejected before mutation and leave no residue.

## Frozen core: failure properties and oracles

- O1, semantic snapshot: canonicalize all primary records, schema-declared fields,
  manifest entries, and results of every applicable public/index-backed query;
  ignore physical LSM order and file bytes.
- O2, direct-materialization differential: build artifact A by
  replay/overlay/tombstones and artifact B by directly writing the intended final
  rows; require O1(A) = O1(B).
- O3, scope-isolation digest: snapshot target scope, decoy scope, same scope in
  another row kind, and unscoped rows before/after; only the selected partition may
  change.
- O4, obligation auditor: for every primary row, require exactly one matching
  source-scope index entry iff its stamp is non-empty; require every source-scope
  index entry to resolve to a matching primary. Also verify all pre-existing schema
  indexes and store-managed obligations for touched rows.
- O5, crash/retry dichotomy: after an injected failure and hard reopen, state is
  either the prior complete state or a self-consistent committed prefix whose exact
  same operation can be retried to O2 equivalence; no manifest may claim a
  completed validator over an incomplete scope.
- O6, lifecycle leak audit: enumerate all keys after reset/cleanup/abort/reuse and
  require every remaining key to be engine-global or owned by the live sync on an
  explicit allowlist.
- O7, bounded-memory oracle: deterministic test telemetry proves rows are consumed
  and committed in bounded chunks with a fixed maximum live batch independent of
  scope cardinality; benchmarks measure the time/allocation curve but do not
  establish closure alone.
- O8, capability oracle: compile-time interface assertions plus a runtime
  store-open/type-capability check; SQLite’s disposition is explicit capability
  absence.
- O9, source immutability: compare source semantic snapshot and stable
  file/directory content digests before and after every replay outcome; reopening
  the source must return the identical snapshot.
- O10, manifest reconciliation: every terminal manifest entry resolves to exactly
  the scoped rows for its `(kind, scope)`, including an explicitly valid zero-row
  set; every terminal stamped scope has exactly one manifest owner. Transitional
  empty-validator states are checked separately and cannot satisfy terminal
  reconciliation.
- O11, timestamp oracle: use distinct sentinel timestamps for source rows, current
  overlay rows, and current manifest writes. Base-only replay preserves source row
  timestamps; current overlay replacements follow the normal direct-write
  timestamp; manifest timestamps equal the current entry supplied/written, never
  an accidental source-row or wall-clock value.
- O12, ordered-operation model: replay a recorded sequence of page upserts and
  tombstones through a tiny independent map model, then compare O1/O4; within a
  page, upserts precede tombstones.
- O13, source-integrity preflight: audit source primary↔scope-index biconditional
  and stamped-scope→manifest ownership before trusting it. Stamped rows without a
  manifest and dangling/wrong-scope indexes are corruption; a manifest with no rows
  remains a valid zero-row scope.
- O14, two-hop oracle: materialize A, replay A→B, then use B as the sole source for
  B→C; require O1(B)=O1(C), successful validator/manifest lookup in B, O9(A/B), and
  O2 equivalence to direct materialization.
- O15, stats/counter oracle: at every documented readable/stable seam, compare
  stored/live counters to counts derived from O1. If counters are intentionally
  undefined until seal/recompute, calls before that seam must have an explicit
  tested disposition and the first defined seam must reconcile exactly.
- O16, qualified-lookup oracle: resolve by exact `(kind, scope bytes)` and record
  whether the cell is found, invalidated/miss, wrong-kind/miss, or absent/miss; no
  fallback to another kind or prefix is permitted.
- O17, residue-free rejection: snapshot source/destination semantics and key
  digests, invoke an invalid combination, require the declared error identity, then
  prove both snapshots unchanged and a subsequent valid call succeeds.

## Frozen core: coverage dimensions and cross-product

- D1 row kind: resources, entitlements, grants.
- D2 mutation family: put-new, overwrite, delete; include all relevant typed
  variants (including conditional/newer and grant inline/deferred paths), with
  non-primary repair operations explicitly disposed as applicable or N/A.
- D3 prior stamp: row absent, unscoped, scope A, scope B.
- D4 requested transition: unscoped→A, A→A, A→B, A→unscoped, each prior
  state→delete; include malformed old value where structural identity still
  permits cleanup.
- D5 artifact partition: target `(A, kind K)`, decoy scope B, same scope A under
  kind J≠K, and unscoped rows.
- D6 overlay identity state: base-only, current-only, same identity in both, same
  public ID but different structural identity where legal, and empty scope.
- D7 overlay data change: unchanged, payload changed without indexed-field change,
  indexed-field changed, scope changed, and store-managed side state present.
- D8 tombstone: none, matching canonical ID, unknown ID, duplicate ID; plus
  principal selector for resources and grants as defined by the schema contract.
- D9 lifecycle/failure point: clean call, repeat call, fail before first commit,
  fail after each committed chunk, final-manifest failure, hard reopen then retry,
  reset, bulk-finish, bulk-abort.
- D10 cardinality: 0, 1, chunk-boundary−1, boundary, boundary+1, multi-chunk, and
  whale benchmark size.
- D11 obligations: every field and index declared for ResourceRecord,
  EntitlementRecord, GrantRecord, and SourceCacheEntryRecord; exemptions must be
  explicit and reasoned.
- D12 scheduling: sequential duplicate and two concurrent identical invocations
  under `-race`.
- D13 manifest/validator state: entry absent, old non-empty, incoming empty,
  incoming non-empty, zero-row completed scope, and interrupted/transitional scope.
- D14 timestamp provenance: source sentinel, current-overlay sentinel,
  current-manifest sentinel, and zero/unset where legal.
- D15 source-input termination: clean EOF, iterator error before first row, error at
  each chunk boundary/interior, cancellation before start, cancellation between
  chunks, and cancellation before manifest commit.
- D16 hostile encoding corpus: empty (explicit reject/allow disposition), NUL and
  tuple-escape bytes, Unicode/non-normalized pairs, prefix-related scopes, maximum
  contract length, IDs containing separator bytes, malformed values with
  structurally valid keys, and duplicate inputs.
- D17 page order: upsert only, canonical tombstone only, principal tombstone only,
  both tombstone classes, same-ID upsert plus either/both tombstone classes on one
  page, tombstone then later-page re-add, and re-add then later-page tombstone.
- D18 compatibility family: absent, populated, malformed/unreadable, reset, bulk
  finish/abort/failure, and reopen. No compatibility-match decision is asserted in
  6a.
- D19 destination target-scope state before pure replay: empty, exact prior partial
  copy, destination-only rows, colliding changed rows, and mixed target/decoy rows.
- D20 source integrity: healthy zero-row manifest, healthy populated scope, stamped
  rows without manifest, dangling source index, wrong-scope index, malformed indexed
  row, and manifest-only zero-row scope.
- D21 source usability: valid Pebble, SQLite/non-Pebble, unreadable/corrupt
  artifact, unfinished/unsealed artifact, and otherwise ineligible source state;
  compatibility-key mismatch is excluded with gating.
- D22 replay annotation shape: overlay false with no rows, overlay false with
  emitted rows, and overlay true with emitted/no emitted rows.
- D23 FileOps surface: clone/copy whole artifact, clone selected sync if applicable,
  read-only source, reopen clone, and unsupported operation.
- D24 stats phase: before replay, after each replay/overlay/tombstone call, after
  retry, after seal/recompute, and after reopen.
- D25 qualified scope relation: exact same kind/key, same key wrong kind, prefix
  neighbor `foo`/`foobar` in both directions, embedded-NUL neighbor, and absent key.
- D26 manifest replayability: `invalidated=false`, `invalidated=true`, and absent
  entry.
- D27 tombstone validity: canonical selector on each kind, principal selector on
  resources/grants, principal selector on entitlements, mixed valid+invalid
  selectors, and selector fields on a non-applicable operation.
- D28 source/destination identity: distinct read-only source, same artifact path
  opened twice, exact same engine/store handle, and source path alias to destination.

Mechanize the space as four exhaustive sub-products plus measured scale/schedule
supplements:

- P1 mutation closure = D1 × every applicable D2 × all legal D3→D4 transitions ×
  D11. Every cell is a table row or an excluded row with a reason.
- P2 replay/overlay closure = D1 × D5 × D6 × D7, reduced only by documented
  symmetry. Every deterministic cell is judged by O2, O3, and O4.
- P3 tombstone closure = D1 × applicable D8 × target/decoy membership ×
  first/repeated application. Principal tombstones are N/A for entitlements unless
  the contract is changed.
- P4 lifecycle/fault closure = each new key family × every applicable D9 state,
  including zero-row manifests.
- P5 manifest/input closure = D1 × D13 × applicable D15, judged by O5/O10.
- P6 ordering/encoding closure = D1 × applicable D16 × D17; deterministic corpus
  cells are exhaustive, fuzzed encodings are measured sampling.
- P7 compatibility lifecycle closure = D18 × each lifecycle surface introduced or
  touched in 6a, excluding match/gating semantics.
- P8 occupied-destination/source closure = D1 × D19 × D20 × applicable D21, with
  corruption/usability cells expected to fail before destination mutation.
- P9 forward/lifecycle closure = D1 × {one-hop,two-hop} × D23 × D24, with
  unsupported FileOps/stats phases represented by executable exclusions.
- P10 qualified-input closure = D1 × D25 × D26 × applicable D27, with all
  miss/reject cells judged by O16/O17.
- D10 whale cells and randomized D12 schedules are measured sampling. Boundary
  cells and sequential duplicate cells remain part of bounded closure.

## Frozen core: criteria, oracle, and coverage level

- C01 Scope stamping — each connector-written kind stores the exact scope and
  unscoped/derived rows do not acquire one. Oracle: O1 + O4. Coverage: bounded
  closure over D1 and scoped/unscoped cases.
- C02 Mutation transition completeness — every relevant typed mutation correctly
  creates, preserves, moves, or removes its source index. Oracle: O4 against P1.
  Coverage: bounded exhaustive table.
- C03 Mutation atomicity — an injected failed commit exposes neither a primary-only
  nor index-only transition and preserves all old obligations. Oracle: before/after
  O1 + O4. Coverage: bounded failure sweep over every P1 mutation family.
- C04 Existing obligation preservation — scope maintenance never corrupts
  parent/resource, entitlement/resource, grant
  principal/entitlement/needs-expansion, digest, deferred-index, or schema-field
  obligations. Oracle: O1 + O4 with D11 descriptor closure. Coverage: bounded
  descriptor/table closure.
- C05 Manifest persistence — exact `(kind, scope, validator)` entries persist for
  zero, one, and many rows and overwrite only the selected entry. Oracle: O1 + O3.
  Coverage: bounded D1 × cardinality {0,1,many} × {new,overwrite}.
- C06 Replay selection/isolation — replay copies all and only target-scope rows of
  the selected kind. Oracle: O3 + exact expected IDs. Coverage: bounded D1 × D5.
- C07 Missing-scope failure — replay of an unknown scope fails loudly and leaves
  current state unchanged. Oracle: error identity plus before/after O1. Coverage:
  bounded for all D1.
- C08 Direct-materialization equivalence — replay-only and every overlay cell
  produce the same semantic store as direct final-state writes. Oracle: O2,
  including all index-backed query projections and manifest. Coverage: bounded P2
  plus supplemental generated sampling.
- C09 Sequential retry convergence — replay, overlay, and tombstones applied twice
  equal one application with no duplicate keys. Oracle: O1 + O4 + O2. Coverage:
  bounded over P2/P3 representative-complete cells.
- C10 Interrupted retry convergence — fail after every chunk and at final manifest
  persistence, reopen, retry, and converge without a false durable claim. Oracle:
  O5 + O2. Coverage: bounded exhaustive cut sweep for a deterministic multi-chunk
  workload.
- C11 Reopen durability — successful replay and manifest/index state survive
  close/reopen unchanged. Oracle: O1 + O4 before/after reopen. Coverage: bounded for
  D1 and replay/overlay/tombstone outcomes.
- C12 Bounded memory — maximum live replay batch does not grow with total scope
  size; runtime curve remains linear or better in rows. Oracle: O7. Coverage:
  structural/bounded closure for batch high-water; measured sampling for whale
  benchmark.
- C13 Overlay identity replacement — base-only/current-only/colliding identities
  resolve exactly like direct materialization and stale indexes disappear. Oracle:
  O2 + O4. Coverage: bounded P2.
- C14 Overlay side-state preservation — every required store-managed field survives
  replacement, while connector-owned changed fields match direct materialization.
  Oracle: schema-descriptor field comparison with explicit exemption list + O2.
  Coverage: bounded descriptor closure across D1/D7.
- C15 Canonical-ID tombstones — each valid tombstone removes only matching in-scope
  rows and every associated index/side obligation. Oracle: O2 + O3 + O4. Coverage:
  bounded P3 for all D1.
- C16 Principal tombstones — grant tombstones remove every grant in the scope for
  that principal; resource tombstones remove scoped rows with that resource ID
  across resource types; decoys survive. Oracle: exact expected sets + O3/O4.
  Coverage: bounded valid principal-selector cross-product.
- C17 Tombstone idempotency — unknown, repeated, and duplicate tombstones are no-ops
  after the intended first deletion. Oracle: before/after O1 + O4. Coverage:
  bounded P3.
- C18 Reset/cleanup/reuse — no source index or manifest from sync N survives
  replacement by sync N+1, including a zero-row N+1. Oracle: O6. Coverage: bounded
  over every new family and populated/empty replacement.
- C19 Bulk import — finish produces complete source indexes/manifests for every
  scoped shape the import API can represent; unscoped input manufactures none;
  abort/failure ingests none; duplicate folding follows an explicit deterministic
  scope policy. Oracle: O1 + O4 + O6. Coverage: bounded D1 × {scoped if
  representable,unscoped} × {finish,abort,failure,duplicate}; any unrepresentable
  scoped cell is recorded as an API-boundary exclusion, not silently skipped.
- C20 Capability exposure — production Pebble stores satisfy the capability’s exact
  method set through the normal dotc1z open path; SQLite’s unsupported disposition
  is clean and explicit. Oracle: O8. Coverage: structural closure plus bounded
  engine registry cells.
- C21 Concurrent duplicate safety — two identical concurrent invocations either
  both succeed or one returns a declared conflict, but final state equals one
  direct materialization. Oracle: O2 + O4 under `-race`. Coverage: measured schedule
  sampling; no bounded schedule closure claimed.
- C22 Instrument mutation adequacy — planted primary/index mismatch, cross-scope
  copy, stale index, premature manifest, unbounded batch, source mutation, manifest
  orphan, timestamp swap, swallowed iterator error, dirty-destination wrong merge,
  accepted corrupt source, replay of `invalidated=true`, lost forward stamp,
  prefix-neighbor delete, wrong page order, and stale counter each make the owning
  harness fail. Oracle: expected red tests or test-only mutants. Coverage: bounded
  over every listed oracle bug class; the catalog must grow whenever a new oracle
  is added.
- C23 Source-artifact immutability — replay never writes, repairs, stamps, compacts,
  or otherwise changes the previous artifact under any outcome. Oracle: O9.
  Coverage: bounded over D1 × {success,failure,cancel,retry}; concurrent scheduling
  remains measured sampling.
- C24 Manifest-to-row reconciliation — terminal manifests and scoped rows satisfy
  O10, including exact zero-row entries and no stamped orphan scope. Oracle: O10 +
  O4. Coverage: bounded P5 terminal-state cells.
- C25 Empty-validator transitions — an empty record validator does not
  create/replace a completed manifest; an empty replay validator is only
  transitional when a later overlay page supplies the final non-empty validator;
  failure before that point leaves no false terminal claim. Oracle: explicit D13
  state-transition table + O5/O10. Coverage: bounded exhaustive D13 transitions for
  D1.
- C26 Timestamp semantics — replayed base rows, overlay replacements, and manifest
  entries carry timestamps from the explicitly declared provenance in O11. Oracle:
  sentinel equality plus O2. Coverage: bounded D1 × applicable D14 ×
  {replay,overlay,retry,reopen}.
- C27 Iterator/cancellation safety — source iterator errors and cancellation at
  every deterministic cut return the original error identity, preserve O4, do not
  publish a false manifest, leave the source immutable, and retry to O2. Oracle: O5
  + O9 + O10. Coverage: bounded exhaustive D15 sweep for a deterministic
  multi-chunk scope.
- C28 Hostile scope/input encodings — byte-distinct scope keys and IDs remain
  distinct through stamping, indexing, replay, tombstones, and manifest lookup;
  invalid empty/oversized/malformed inputs follow one explicit loud disposition
  without mutation. Oracle: O1/O3/O4 over D16. Coverage: bounded hostile corpus plus
  measured fuzz sampling.
- C29 Page upsert/tombstone ordering — same-page upsert then tombstone ends absent;
  cross-page outcomes follow page order exactly and remain retry-safe. Oracle: O12
  + O2. Coverage: bounded D1 × applicable D17, with invalid selector/kind cells
  explicitly excluded.
- C30 Compatibility-family lifecycle boundary — compatibility records/families are
  included in reset, cleanup, reopen, bulk finish/abort/failure, and leak accounting
  as applicable, without asserting deferred eligibility/matching behavior. Oracle:
  O6 over P7. Coverage: bounded lifecycle closure; compatibility gating remains
  explicitly deferred.
- C31 Occupied-destination pure replay — non-overlay replay replaces the selected
  destination scope so its terminal rows equal the source scope exactly;
  destination-only rows and obsolete indexes disappear, while decoy scopes/kinds
  remain unchanged. Oracle: O2/O3/O4. Coverage: bounded D1 × D19, including
  retry-partial destination state.
- C32 Inconsistent previous artifact — stamped rows without a manifest,
  dangling/wrong-scope source indexes, and malformed indexed rows fail with an
  integrity error before destination mutation; manifest-only zero-row scopes remain
  valid. Oracle: O13 + before/after O1 + O9. Coverage: bounded D1 × D20.
- C33 Forward cacheability — replay output retains complete stamps, source indexes,
  and a matching manifest/validator and can serve as the only source for a second
  replay hop. Oracle: O14. Coverage: bounded D1 ×
  {zero-row,populated,overlay,tombstoned} two-hop cells.
- C34 Transitional `overlay=false` with emitted rows — pin the proto’s current
  transitional behavior: if Phase 6a’s API can receive this shape, replay the base
  then apply emitted rows with overlay semantics and page-order tombstones;
  otherwise provide an executable API-boundary exclusion assigning it to syncer
  orchestration. Oracle: O12/O2/O10 or the executable exclusion. Coverage: bounded
  D1 × D22.
- C35 Unsupported/unusable previous source — non-Pebble, unreadable,
  unfinished/unsealed, and otherwise unusable artifacts fail closed with a stable
  error and leave destination/source unchanged. Oracle: error identity + O1/O9.
  Coverage: bounded D21; compatibility mismatch remains deferred.
- C36 Combined same-page tombstones — canonical and principal tombstones union
  after upserts; a row matching either selector is deleted once, including a row
  upserted on that page, while nonmatching scope/principal/ID decoys survive. Oracle:
  O12/O3/O4. Coverage: bounded applicable D1 × combined D17 cells.
- C37 Clone/FileOps disposition — any FileOps path that can produce a future replay
  source preserves a self-consistent set of scoped rows, source indexes, manifests,
  compatibility-family lifecycle state, and source immutability across reopen;
  every non-applicable path has an executable exclusion. Oracle: O1/O4/O6/O9/O10
  plus two-hop use where supported. Coverage: bounded D23.
- C38 Stats/counter coherence — replay, replacement, overlay, and tombstones do not
  leave defined live/stored counters stale; if counters are defined only after
  seal/recompute, that exemption is explicit and the first defined result equals a
  store-derived count across retry/reopen. Oracle: O15. Coverage: bounded D1 × D24 ×
  {replay,overlay,tombstone,retry}.
- C39 Wrong-kind scope hit — if the previous artifact has scope S for one kind but
  not the requested kind, replay follows the explicit per-`(kind,scope)` miss policy
  and never copies the other kind. For the direct replay primitive, the miss fails
  closed and leaves both artifacts unchanged. Oracle: O16/O17 + O3. Coverage:
  bounded ordered pairs of distinct D1 kinds using the same scope bytes.
- C40 Invalidated previous entry — a manifest with `invalidated=true` is treated as
  non-replayable/miss even when matching stamped rows remain; direct replay fails
  closed without destination mutation. Oracle: O16/O17 + O9. Coverage: bounded D1 ×
  D26 × {zero rows,one row,many rows}.
- C41 Prefix-neighbor isolation — operations on `foo` never read, copy, replace,
  tombstone, or delete `foobar`, and vice versa; this is a named regression cell
  rather than only part of the hostile corpus. Oracle: O3/O4/O16. Coverage: bounded
  D1 × both D25 prefix directions × {replay,replace,tombstone,manifest lookup}.
- C42 Illegal tombstone combinations — `deleted_principal_ids` on entitlements and
  every other selector/kind combination outside the proto contract are rejected
  atomically; a mixed valid+invalid request applies nothing. Oracle: O17. Coverage:
  bounded exhaustive D1 × D27 validity table.
- C43 Replay-from-self — the same writable handle, same artifact path, or aliased
  source/destination is rejected before iteration or write, without deadlock,
  source mutation, partial destination state, or poisoned subsequent use. Oracle:
  O17 with a bounded timeout only as a liveness guard. Coverage: bounded D28
  identity cells; filesystem alias forms unsupported by the platform are explicit
  exclusions.

## Frozen core: closure criteria

- Every C01–C43 criterion has an executable artifact, an evidence command/result,
  and the stated coverage level; any missing current-stage mapping blocks 6a
  closure.
- P1–P10 contain every cell, with executable expected outcomes or explicit reasoned
  exclusions; no implicit N/A cells.
- All bounded/exhaustive and structural checks pass, including reopen and
  planted-violation adequacy checks.
- Source immutability, terminal manifest reconciliation, timestamp provenance, and
  iterator/cancellation cut sweeps pass for all three row kinds.
- Occupied-destination replacement, corrupt/unusable-source rejection, and two-hop
  forward-cacheability pass for all three kinds; Clone/FileOps and counter
  exclusions are executable, not prose-only.
- Wrong-kind, invalidated, prefix-neighbor, illegal-tombstone, and replay-from-self
  cells pass with residue-free failure where required.
- `go test -race` passes for affected Pebble/dotc1z packages and the concurrent
  duplicate test; this is race evidence, not schedule closure.
- The deterministic batch high-water assertion passes at all boundary sizes. Whale
  benchmark results record rows/s, bytes/op, allocs/op, and the observed curve;
  regressions are compared against `main`. Benchmark/fuzz/soak evidence remains
  measured sampling.
- No claim depends on raw file-byte equality, SQLite parity, syncer orchestration,
  compatibility gating, continuation/RPC, invalidation, or compactor behavior.

## Frozen core: required instruments

- I1 semantic snapshot + obligation/leak auditor implementing O1/O3/O4/O6.
- I2 table-generated P1 typed-mutation transition/atomicity suite.
- I3 two-artifact replay harness with independent direct-materialization model
  implementing O2.
- I4 overlay/tombstone generator implementing P2/P3, with deterministic seeds
  retained on failure.
- I5 per-chunk/final-manifest failure and hard-reopen sweep implementing O5, with
  self-terminating completeness evidence.
- I6 reset/reuse/bulk-import lifecycle harness.
- I7 compile/runtime capability conformance checks.
- I8 deterministic batch high-water seam plus whale benchmark.
- I9 concurrent duplicate test under the race detector.
- I10 mutation-adequacy tests for every C22 oracle bug class.
- I11 source-artifact semantic/content digest guard.
- I12 manifest reconciler plus exhaustive empty-validator transition table.
- I13 sentinel-timestamp fixtures and provenance comparator.
- I14 failing iterator and cancellation-at-cut harness.
- I15 hostile encoding corpus plus deterministic fuzz target.
- I16 page operation-log model for upsert/tombstone order.
- I17 compatibility-family lifecycle registry/auditor, scoped strictly to storage
  ownership.
- I18 occupied-destination and corrupt-source fixture builder with source-integrity
  preflight oracle.
- I19 two-hop A→B→C forward-cacheability harness.
- I20 unsupported/unusable source corpus spanning engine format, corruption, and
  unfinished state.
- I21 FileOps/clone replay-source conformance harness with executable exclusion
  registry.
- I22 store-derived stats/counter reconciler across operation phases.
- I23 exact `(kind,scope)` lookup matrix with invalidated-entry and first-class
  prefix-neighbor fixtures.
- I24 tombstone selector-validity dispatch table and atomic rejection harness.
- I25 source/destination alias detector harness, including timeout-guarded
  same-handle calls.

## Baseline `main` placement map

- I1 shared Pebble helpers: `pkg/dotc1z/engine/pebble`.
- I2 typed mutation transitions: adjacent to typed record-operation coverage.
- I3/I4 two-artifact replay and overlay/tombstone models: Pebble replay tests using
  two real engine lifecycles and an independent direct model.
- I5/I14 failure and cancellation sweeps: Pebble replay failure tests, with every
  new seam registered in `obligations_on_failure_test.go`.
- I6 reset/reuse/bulk lifecycle: cleanup and bulk-import suites.
- I7 capability conformance: `pkg/dotc1z`.
- I8 batch high-water and benchmarks:
  `pkg/dotc1z/engine/pebble/source_cache_replay_bench_test.go`.
- I9 concurrent duplicate behavior: replay integration suite under `-race`.
- I10 mutation adequacy: beside each owning harness.
- I11/I13 source digest and timestamp sentinels: shared Pebble test helpers.
- I12 manifest reconciliation: source-cache manifest tests.
- I15 hostile encoding: source-scope encoding tests.
- I16 page ordering model: source-cache overlay-order tests.
- I17 compatibility lifecycle: cleanup and bulk lifecycle suites, without matching
  semantics.
- I18/I20 source integrity and usability: replay-integrity and capability tests.
- I19 two-hop replay: replay integration tests through the public capability.
- I21 clone/FileOps: clone-sync tests and engine adapter tests where applicable.
- I22 stats/counters: source-cache stats tests using stable sidecar seams.
- I23 exact qualified lookup: manifest and replay-integrity suites.
- I24 tombstone selector validity: overlay-order tests.
- I25 source/destination alias detection: replay-integrity tests.

## Change-order log

### CO-001 — Replay-boundary ownership clarification

- Type: clarification.
- Source: initial failing verification cells exposed an ambiguity between the
  direct engine primitive, the public source-cache capability, and deferred syncer
  orchestration.
- Resolution: `SourceCacheStore.ReplaySourceCache` authorizes replay from an exact,
  non-invalidated manifest. Engine replay validates the selected source
  primary↔index biconditional and performs replacement/copy. The caller owns
  overlay/tombstones and terminal manifest publication.
- Verification delta: missing/wrong-kind/invalidated manifest rejection and
  all-kind source-integrity preflight.

### CO-002 — Coverage-reduction clarification

- Type: clarification.
- Source: literal review of the frozen P1–P10 language after initial signoff.
- Resolution: account for the full dimensions, but close implementation-equivalent
  cells through documented row-kind symmetry, shared typed rawdb choke points, and
  representative operation models. Unrepresentable cells remain executable
  exclusions; fuzzing, randomized schedules, and benchmarks remain measured
  sampling.
- Verification delta: all-kind corrupt-source matrix, realistic artifact aliases,
  corrupt-envelope rejection, and remaining typed-path assertions.

### CO-003 — Scoped tombstone batch bound

- Type: correction.
- Source: post-verification review found scoped principal/resource tombstone scans
  staged O(scope size) operations into one RecordBatch.
- Contract delta: no new external behavior; bounded-memory enforcement is extended
  from replay/replacement to adjacent source-cache scope scans.
- Verification delta: all three scoped tombstone paths prove bounded commits,
  accurately reported committed progress on interruption, primary/index agreement,
  and convergent retry. The public wrapper marks the store dirty whenever committed
  progress is returned, including an error path.

### CO-004 — Scope-count replay optimization

- Type: extension/optimization.
- Source: current source-integrity preflight is bounded-memory but scans all
  primaries of a row kind for every replayed scope.
- PR placement: separate stacked follow-up PR based on Phase 6a.
- Status: open action item; Phase 6a correctness closure remains valid, but
  many-scope replay performance is not closed until this change order has
  evidence.
- Action: add an optional primary-derived `row_count` to each sealed
  `(row_kind, scope_key)` manifest entry. Fuse count collection into EndSync's
  existing primary scans where available, cover deferred/stashed scan paths,
  and clear counts before a completed sync is rebound for mutation.
- Replay contract: when `row_count` is present, retain scoped
  index→primary/stamp validation and require its cardinality to equal the
  sealed count before destination mutation; when absent, preserve the current
  full primary↔index preflight for legacy artifacts. Count zero means a proven
  empty scope, not unknown.
- Verification delta: optional sealed manifest row counts, legacy fallback,
  rebind invalidation, counted corruption checks, and many-scope benchmarks.
- Closure gate: all three row kinds prove primary-derived count reconciliation,
  zero-row and second-hop behavior, missing/orphan/wrong-scope/malformed-source
  rejection in counted and legacy modes, failure/retry without a false count,
  read-only reopen/clone preservation, and stale-count removal on rebind. A
  fixed-total-row benchmark varying the number of replayed scopes must compare
  counted and legacy manifests and demonstrate removal of the O(S·N) primary
  scan; the independent biconditional auditor remains count-unaware.

### CO-003a — Pooled batch ownership correction

- Type: correction to CO-003.
- Source: focused review of the new bounded-delete helper found that its final
  commit closed a Pebble batch but retained the pointer for deferred cleanup.
  Pebble returns closed batches to a process-global pool, so a second close could
  act on a batch another goroutine had acquired.
- Contract delta: the helper owns either one live batch or nil. Every close
  relinquishes the pointer immediately, and deferred cleanup is nil-safe and
  idempotent.
- Verification delta:
  `TestVerificationScopedDeleteBatchFinalCloseOwnership` asserts that final commit
  clears ownership and repeated deferred cleanup cannot touch the pooled object.

### CO-005 — Public replay persistence-lifecycle correction

- Type: correction.
- Source: focused implementation-obligation review found that the public replay
  wrapper marked the store dirty only after a successful replay reporting one or
  more copied rows. Replacement can commit destination clearing with zero copied
  rows, or commit clearing/bounded copy chunks before a later error.
- Contract delta: no new external behavior. Once manifest authorization succeeds,
  the public wrapper marks dirty before crossing the engine mutation boundary so
  `Close` persists every committed prefix.
- Verification delta: a resumed occupied destination is replayed from a valid
  zero-row source and through an injected failure after committed clearing. Both
  cases require dirty state and verify the exact durable result after close/reopen.

### CO-006 — Timestamp-oracle correction

- Type: correction.
- Source: C26/O11 described a current manifest timestamp as both “supplied/written”
  and never a wall-clock value, but the Phase 6a manifest API supplies only a
  validator; the engine assigns `discovered_at` at the current manifest write.
- Resolution: source rows use an explicit source sentinel and replay preserves it
  byte-for-byte. A normal current overlay uses an explicit overlay sentinel.
  A current manifest write must fall within the before/after bounds of that write,
  differ from both row sentinels, and survive reopen unchanged. This corrects an
  unrepresentable oracle cell; it does not add a product behavior.
- Verification delta: all three row kinds exercise source replay, colliding current
  overlay, retry replacement, current manifest publication, and hard reopen with
  the three independently observable timestamp provenances.

### CO-007 — Page-ordering ownership clarification

- Type: clarification.
- Source: CO-001 assigns overlay/tombstone invocation to the caller, while C29's
  page-order claim requires a component that owns page scheduling. No in-scope
  Phase 6a component owns that ordering decision; syncer orchestration is deferred.
- Resolution: C29 is deferred to the orchestration phase and cannot be closed by
  directly invoking storage methods in a chosen order. C36 remains in scope for
  storage composition: overlapping canonical/principal selectors form a
  duplicate-safe union, and later explicit calls deterministically re-add or
  remove identities.
- Verification delta: the storage test includes a row matched by both selector
  classes and explicitly disclaims page-order closure. Future C29 evidence must
  invoke the orchestration owner.

### CO-008 — Opaque row-ID validity clarification

- Type: clarification.
- Source: C28 grouped empty/oversized/malformed “inputs” without distinguishing
  source-cache-owned scope/selector validation from the base typed writers' opaque
  connector row IDs.
- Resolution: Phase 6a validates row kind, scope key, and structured resource BID
  selectors. Representable connector row-ID strings remain opaque storage data,
  including empty, embedded-NUL, normalization-neighbor, and long values; Phase 6a
  must preserve and isolate them rather than invent a new base-row validity policy.
  Invalid UTF-8 is not representable in protobuf string fields. Malformed resource
  BIDs and structurally ambiguous entitlement/grant selectors retain their loud,
  residue-free C42 dispositions.
- Verification delta: all row kinds replay and tombstone the representable hostile
  ID corpus byte-exactly with neighbor survival; invalid UTF-8 is executable
  unrepresentability evidence.

### CO-009 — Derived fast-path proof lifecycle correction

- Type: correction.
- Source: post-closure implementation review found that sync rebind and bulk-import
  finish invalidated the resource/grant empty-keyspace proofs but not the
  entitlement proof, while a destructive replay clear that failed after a
  committed batch left the proof armed for every row kind.
- Contract delta: no new external behavior. Derived state that authorizes
  read-before-write omission is a proof: every transition that falsifies its
  premise must consume it at the committed cut or conservatively before mutation.
- Verification delta:
  `TestVerificationBindCurrentSyncDisarmsAllFastPathProofs`,
  `TestVerificationBulkImportFinishDisarmsAllFastPathProofs`, and
  `TestVerificationReplayClearCommittedPrefixDisarmsFastPath` assert the armed
  premise, all sibling kinds, distinct clear-loop commits, and a colliding write
  whose old-index cleanup requires the slow path.

### CO-010 — Orphan-only scoped-index healing persistence correction

- Type: correction.
- Source: the independent implementation-obligation review of CO-009 found that
  scoped tombstone loops could commit deletion of orphan source-scope indexes
  while reporting zero deleted primary rows. Public wrappers marked dirty only
  for positive row counts, so `Close` discarded the healing.
- Contract delta: no new row-deletion behavior. A public mutating boundary must
  persist committed defensive cleanup even when its result count is zero.
- Verification delta:
  `TestVerificationOrphanScopeIndexHealingPersistsAfterReopen` creates the
  otherwise-unrepresentable orphan state, exercises resource-ID, grant-principal,
  and grant-external-ID public paths, proves live healing reports zero primary
  deletions, and requires the healed index state after close/reopen.

### CO-010a — Public mutation-to-close atomic handoff correction

- Type: correction to CO-010 and the existing public replay dirty lifecycle.
- Source: focused re-review of the CO-010 fix found that marking dirty before an
  engine call was still not atomic with entering the engine writer barrier.
  Concurrent `Close` could checkpoint after dirty marking but before the mutation
  entered, then discard a later successful mutation.
- Contract delta: every public source-cache mutation owns the store close mutex
  from conservative dirty marking through completion of its engine call. `Close`
  therefore checkpoint-saves either before the mutation starts or after it
  completes, never between its persistence claim and facts.
- Verification delta:
  `TestVerificationSourceCacheMutationHandoffToConcurrentClose` blocks a public
  mutation immediately before engine entry, proves it owns the close mutex, proves
  `Close` has reached its lock attempt, then requires the successful mutation in
  the reopened artifact. `TestVerificationReplayWriteBarrierDrainsBeforeClose`
  separately pins the engine-level established-writer drain.

### CO-010b — Manifest validation-order correction

- Type: correction to CO-010a.
- Source: focused re-review found that the new public mutation boundary marked a
  clean store dirty before the engine rejected an empty manifest validator and
  changed error precedence after Close.
- Contract delta: validate all wrapper-owned inputs before entering the mutating
  lifecycle boundary. Rejection does not dirty or rewrite an artifact and retains
  its validation error independently of store lifecycle state.
- Verification delta: the clean/closed-store cell in
  `TestVerificationEmptyValidatorDoesNotPublishManifest` asserts the empty-validator
  error, unchanged clean dirty state, and validation precedence after Close.

### CO-011 — Closing-instrument and runtime-obligation expansion

- Type: additive verification change order with production ride-along accounting.
- Source: the post-CO-010b closing round applied the handbook's commit-point,
  resource-leak, stateful-model, and structural-coverage instruments. Those
  instruments exposed obligations not present in the prior addendum.
- Contract delta: no source-cache API behavior changes. Every rawdb family batch
  is an owned resource from mint until its first `Close`; `Engine.Close` reports
  any outstanding record, session, digest, or fold batch while still closing the
  underlying database. Batch `Close` is idempotent.
- Verification delta: a mutation-adequacy suite plants leaks for every independent
  family counter plus Pebble iterators/Get closers, checks clean and double-close
  lifecycles, and turns every ordinary engine/compactor fixture into a ride-along
  leak oracle. A deterministic source-cache reference model supplements the
  directed matrix; it is measured exploration, not bounded closure.
- Cost delta: accounting adds one atomic increment and decrement per batch
  lifecycle, not per row. The asymptotic cost remains O(batches), and closing
  committed overlay batches restores Pebble pooling instead of retaining every
  completed chunk.

### CO-011a — Overlay fold-batch ownership correction

- Type: production correction found by CO-011's resource-leak oracle.
- Source: `overlayBucketRawWriter.flush` committed each primary/index pair and
  replaced both pointers without closing the committed batches. Pebble does not
  release a batch on `Commit`, so every successful chunk leaked two pooled
  batches until process exit.
- Contract delta: a successful flush closes both committed batches before
  reminting. Commit failures, cancellation, restart discard, and final cleanup
  retain one clear owner and release it exactly once. An index-commit failure
  after a primary commit makes the unpublished destination disposable; no caller
  may publish that intermediate artifact.
- Verification delta: the compactor's single `commitFoldBatch` choke point accepts
  an explicit per-call failure argument used only by focused tests; production
  passes `nil` and carries no mutable hook state. It exercises primary failure,
  index failure after primary success, cancellation with pending writes, successful
  close/remint, restart discard/remint, final cleanup, and merge failure/retry.
  Every cut requires a clean destination `Close`.

### CO-011b — Commit-seam reachability and model reproducibility correction

- Type: verification-instrument correction.
- Source: independent evidence audit found that the original commit-point
  meta-test accepted a seam name without proving that the concrete batch type
  invoked it, omitted synccompactor, and misrouted ordinary typed commits through
  an engine-only hook. The randomized model also iterated maps while claiming
  exact seed reproduction.
- Contract delta: none.
- Verification delta: typed record commits retain their rawdb pre-commit seam. The
  registry scans engine/rawdb and synccompactor batch commits; compactor fold
  commits reduce to one helper with an explicit test-only failure argument rather
  than mutable engine/rawdb hook fields. Digest/session sites retain explicit
  errorfs-backed follow-up dispositions. The model sorts every map-derived choice
  before seeded shuffling and its row/manifest cell oracle has planted
  missing/extra/wrong-state mutants.
- Remaining limitation: this registry covers batch `Commit` calls. Direct
  set/delete/ingest/excise/flush durability points remain governed by their
  purpose-named rawdb choke points, errorfs/crash suites, and the explicit
  obligations below; it must not be cited as enumeration of every durable write.

### CO-011c — Descriptor closure and terminal-iterator error correction

- Type: verification-instrument correction.
- Source: independent evidence audit found that C04/C14 used representative
  populated records without a schema-drift gate, C09 lacked an all-kind
  overlay-twice differential, and C27 injected callback failures rather than the
  terminal `Iterator.Error()` disposition.
- Contract delta: none.
- Verification delta:
  `TestVerificationDescriptorClosedReplayAndDirectMaterialization` freezes every
  top-level D11 field by descriptor name, has no current exemptions, fills every
  field with a non-default sentinel, compares replay/direct and repeated-overlay
  semantics for all kinds, verifies generated index keys, and rejects stale changed
  keys. A separate replay iterator hook executes at the production terminal-error
  check after iteration and before final commit; all kinds require exact error
  identity, committed-prefix accounting, O4, source immutability, and convergent
  retry.

### CO-012 — Final independent-review lifecycle and evidence correction

- Type: production correction plus verification-claim correction.
- Source: the repeated independent final-code audits rejected signoff because the
  public replay capability accepted an unfinished previous artifact, compactor
  source-close errors were log-only, and several structural/commit-seam statements
  exceeded what their instruments proved.
- Contract delta: C35's unfinished-source cell is enforced at the public storage
  capability. A previous Pebble artifact must contain a durably finished sync run
  before destination mutation begins; the in-memory `IsSealed` flag is not evidence
  because it intentionally resets on reopen. At CO-012, compacted/non-FULL and
  compatibility eligibility remained deferred; CO-013 supersedes the first two
  cells while compatibility matching remains deferred.
- Production delta: `ReplaySourceCache` validates durable finished state after
  manifest authorization and before `beginSourceCacheMutation`. K-way chunk cleanup
  joins every owned source-handle close error while still scheduling asynchronous
  directory removal. Fold and rebuild source-store close errors join the operation
  result and therefore prevent publication.
- Verification delta:
  `TestVerificationReplayRejectsUnfinishedSourceAllKinds` requires rejection before
  engine mutation with unchanged occupied destination and source digests. Legitimate
  public replay fixtures now finish their source syncs. Source-handle close tests
  require all handles to close, preserve every error identity, and retain async
  cleanup. C27's dedicated terminal-error cut is explicitly limited to the
  all-kind source-copy loop; preflight, destination-clear, and scoped-delete loops
  retain inline `Iterator.Error` checks without a claimed deterministic terminal
  failure sweep. C22 remains partial because this behavioral cut is not a planted
  swallowed-error mutant and page ordering remains deferred with C29. The
  batch-commit registry is drift prevention for missing dispositions, not proof
  that every registered call site independently executed. Source-close unit tests
  cover joined error identity and cleanup, while actual store-close failure through
  every top-level publication path remains implementation-reviewed rather than
  end-to-end fault-injected.

### CO-012a — Close-error run-file ownership and evidence narrowing

- Type: production ownership correction plus evidence correction.
- Source: final independent re-review found that a completed k-way run file could
  be returned with a later source-close error; callers discarded the run value and
  therefore lost the path needed for direct cleanup.
- Contract delta: none. A run file is unpublished until both build and owned-source
  cleanup succeed. A close error removes a successfully built run before returning
  the joined error.
- Verification delta:
  `TestFinishChunkRunFileRemovesUnpublishedRunAfterCloseFailure` plants the exact
  build-success/close-failure cut and requires error identity plus filesystem
  absence. C22 is explicitly partial rather than verified for its full frozen
  mutant catalog; structural coverage is a navigation profile plus the named
  F1/F2/F3/F8 ledger, not a complete per-branch disposition artifact. A separate
  all-kind test closes unfinished sources, reopens them read-only, and proves the
  durable `ended_at` guard rejects before destination mutation. Finished-source
  enforcement belongs to the public `SourceCacheStore` capability; direct engine
  replay primitives remain lower-level and bypass that wrapper policy.

### CO-013 — Compaction eligibility, cache invalidation, and Get-closer ownership

- Type: production correctness correction plus deferred eligibility-policy
  implementation.
- Source: an independent post-closure implementation review found two concrete
  obligations: compactors could preserve or synthesize source-cache state whose
  validators did not describe the merged winners, and two grant overwrite paths
  retained successful `Get` closers across protobuf marshaling without releasing
  them when marshaling failed.
- Contract delta: a replay source must be a finished FULL connector sync and must
  not carry the durable `compacted` marker. Both syncer previous-artifact selection
  and public replay enforce the same predicate before replay mutation.
- Production delta: fold marks its output compacted and range-deletes only source
  manifests, retaining inherited source-scope indexes to preserve fold's bounded
  write cost. K-way and overlay mark outputs compacted and range-delete manifests
  plus all three source-scope index families after winner materialization and
  again after grant expansion, because expansion may restage grant indexes.
  Range tombstones keep invalidation O(1) in output row count and commit with
  `NoSync`, matching fold batches: artifact publication still waits for Close's
  checkpoint/fsync, so invalidation adds no standalone fsync. The v3 envelope's
  header-only `SyncRunSummary` projects `compacted`, allowing eligibility
  introspection without payload unpack or zstd decode. Grant IfNewer and expanded-
  grant overwrite loops now put each successful `Get` in a per-record function
  with an immediate deferred close. Source-scope maintenance adds a point `Get`
  to entitlement overwrites once the first-call fresh-sync proof is unavailable,
  and grant/resource writes scan old/new protobuf values for scope transitions.
  These are constant-factor connector-write costs, not complexity changes; CO-013
  records them explicitly but does not claim a dedicated throughput benchmark.
- Verification delta:
  `TestGrantMarshalFailureReleasesExistingRowCloser` reaches both marshal-failure
  branches with invalid UTF-8 after an existing-row `Get`; the package-wide
  `Engine.Close` oracle requires clean version references.
  `TestCompactPebbleInvalidatesSourceCacheReplayState` exercises fold, k-way, and
  overlay through grant expansion and checks the exact retained/dropped keyspaces
  plus the durable compacted marker.
  `TestVerificationReplayRejectsIneligibleFinishedSourceAllKinds` and
  `TestPreviousSyncC1ZPathEnforcesReplayEligibility` cover direct public replay and
  syncer selection. `TestInvalidateSourceCacheReplayStateCommitFailureIsAtomic`
  plants the exact typed-batch commit failure and requires both manifest and index
  families to remain unchanged. `TestManifestSyncRunProjection` and compactor
  output checks require the compacted bit from `ReadManifestHeader`, which reads
  no payload bytes.

## Post-freeze implementation-obligation addendum

This addendum was produced by a reader independent of the implementation and
instrument author. It records implementation-derived obligations the behavioral
contract could not name. Each entry states mechanism/owner; invariant; failure
premise; oracle; and instrument or remaining boundary.

1. **Engine lifecycle barrier (Engine)** — every writer joins `writeWG`, serializes
   through `writeMu`, rechecks closing/sealed state, and completes before DB/cache
   teardown. Race close/checkpoint/seal against writes; require no post-seal commit,
   panic, or retained DB use. Existing engine lifecycle tests cover the barrier;
   `TestVerificationReplayWriteBarrierDrainsBeforeClose` additionally blocks replay
   at its commit seam and requires `Close` to wait for the active writer.
2. **Get closers and iterators (calling engine/rawdb method)** — every successful
   acquisition closes on success, error, cancellation, malformed value, and early
   yield. Reopen/race and injected malformed/cancel paths are the oracle. All-kind
   malformed-delete, replay-cut, and hard-reopen tests found no remaining leak.
3. **Typed record batches (rawdb)** — primary mutation, old-index cleanup, new
   indexes, source-scope transition, and digest invalidation commit atomically.
   Stage/commit failure must preserve the complete prior key snapshot and O4.
   `TestVerificationSourceScopeMutationAtomicity` and typed-operation coverage own
   this obligation.
4. **Grant index regimes (rawdb/EndSync)** — inline writes maintain principal and
   expansion indexes; deferred writes arm a durable rebuild marker before rows and
   clear it only after rebuild. Marker/row/rebuild cuts require flag/key agreement
   and complete sealed indexes; deferred-marker and failed-mutation tests cover it.
5. **Malformed source-scope cleanup (rawdb)** — malformed old values trigger
   identity-derived scans that remove all matching source indexes atomically.
   `TestVerificationMalformedAllKindDeleteCleansSourceScopeIndex` requires primary
   and stale-index absence.
6. **Fresh-sync proof state (Engine)** — `MarkFreshSync` establishes each family
   emptiness proof; first mutation and every falsifying lifecycle transition
   consume it. The oracle is an armed-premise assertion followed by a colliding
   write and O4. CO-009 supplies bind, bulk-finish, and committed-clear evidence.
   Bind/bulk assert the proof state directly: bind has no other mutation, while
   bulk input cannot represent a prior scoped obligation for a behavioral
   collision. The committed-clear cell supplies the all-kind colliding-write
   oracle for the shared proof-consumption mechanism.
7. **Entitlement bare-ID cache (Engine)** — generation advances after every landed
   entitlement mutation, so a racing rebuild from an older generation remains
   stale. Lookup-invalidation tests cover writes and replay; fold callers remain
   conventionally responsible and compactor integration is deferred.
8. **Replay source preflight (replay Engine)** — prove the selected source
   primary↔scope-index biconditional before any destination mutation and retain
   source ownership of all closers/iterators. Corrupt-source matrices require
   unchanged source and destination snapshots.
9. **Destructive replay clear loop (Engine)** — bounded batches report only landed
   deletions and invalidate family proof/cache state even if a later commit fails.
   CO-009 covers every kind with a committed clear cut, colliding write, and retry;
   public zero-row and failure persistence tests cover close/reopen representatives.
   Public persistence is reduced across kinds because one wrapper marks dirty
   before dispatch; the kind-specific engine loops remain all-kind evidence.
10. **Replay copy loops (Engine)** — each family stages primary and maintained
    indexes together; committed rows, not staged/result rows, control proof/cache
    invalidation. Read, malformed, intermediate/final commit, and cancellation cuts
    require O2/O4, source immutability, reopen durability, and convergent retry.
11. **Scoped tombstone loops (Engine/public wrapper)** — batch ownership is
    nil-or-exclusive, commits are bounded, result counts include only committed
    primary deletes, and committed orphan cleanup still persists. Bounded-retry,
    final-close ownership, and CO-010 close/reopen tests cover the obligation.
12. **Manifest mutation and dirty lifecycle (public `pebbleStore`)** — public replay
    and all source-cache mutations own `closeMu` from conservative dirty marking
    through engine completion; terminal claims publish only after their owned
    facts. CO-010a deterministically pins the pre-engine Close handoff. Zero-row
    replacement, post-commit error, manifest failure, and hard-reopen tests cover
    the remaining persistence cuts.
13. **Bulk-import ownership (BulkSyncImport)** — Finish/Abort own SST writers,
    sorters, shards, files, goroutines, and staging teardown; successful ingest
    invalidates all affected proof/cache state. The bulk suite and abort-after-close
    test cover representable data; scoped bulk input remains unrepresentable.
14. **Reset/open/close/checkpoint (Engine/store)** — reset removes data/sidecars
    while preserving engine-global metadata; open restores durable proof state;
    close flushes before release; checkpoint orders flush, snapshot, then WAL
    truncation. Reset, MemFS, errorfs, checkpoint-WAL, and close tests own this.
15. **Sidecars, markers, and counters (Engine/rawdb)** — digest/build-pending and
    deferred-index state are correctness proofs; computed stats are derived state.
    Failure after establishment must yield fallback/missing-not-stale state and
    marker agreement. Existing digest/marker crash tests cover correctness;
    attempted-versus-committed expansion telemetry has no Phase 6a semantic effect.
16. **Clone/copy/fold ownership (FileOps/synccompactor)** — clone owns checkpoint,
    destination engine, temporary output, and rename; failure removes temporary
    output. Clone/copy tests require semantic equivalence, source immutability, and
    read-only replay. Fold barrier ordering and lookup invalidation remain with the
    explicitly deferred compactor integration owner.
17. **Family-batch accounting (rawdb/Engine)** — every `NewRecordBatch`,
    `NewSessionBatch`, `NewDigestBatch`, and `NewFoldBatch` increments exactly one
    family counter; first `Close` decrements it even after commit failure; later
    closes are no-ops. `DB.Close` joins the leak error with Pebble's close error so
    teardown still occurs. Clean, double-close, and one planted leak per family are
    the executable oracle.
18. **Overlay/fold batch rotation (synccompactor)** — the current batch remains
    exclusively owned through commit, cancellation, or error. Success closes before
    remint; restart closes before remint; final cleanup closes the current handles.
    The primary/index pair is not an atomic commit: failure of the second commit
    abandons the unpublished destination artifact. Fold failure-cut tests and clean
    destination Close own this obligation.
19. **Compactor lifecycle error propagation (synccompactor)** — source/destination
    close errors, including the family-batch leak signal, are correctness outcomes,
    not log-only telemetry. Test and benchmark fixtures assert clean Close; the
    top-level compactor joins deferred destination-close errors with its operation
    result.
20. **Commit-point dispositions (engine/rawdb/synccompactor)** — typed record
    commits retain the rawdb seam; compactor fold commits reduce to one helper whose
    explicit failure argument exercises exact ownership paths without mutable
    production hook state. Digest/session batch commits are visible follow-up debt
    with errorfs coverage, not exact-site claims. Direct writes, ingests, excises,
    flushes, and durability fences require separate §5.12 dispositions; the batch
    registry is not evidence for those sites.
21. **Stateful-model scope (dotc1z tests)** — a printed seed must reproduce the same
    choices independent of Go map order, every oracle dimension claimed by the
    model needs a planted mutant, and probabilistic operation occurrence is measured
    sampling. Directed tests continue to own failure cuts, exact index projections,
    and bounded matrix closure.

The initial independent review reported zero new HIGH findings. Its focused
re-review of CO-010 found the HIGH handoff defect recorded by CO-010a; re-review of
that correction passed the zero-HIGH gate and found the LOW validation-order issue
recorded by CO-010b. The later final-code evidence audit superseded that provisional
gate: it found no new source-cache product defect, but reopened closure because the
evidence provenance, commit-seam claims, descriptor closure, iterator-error
instrument, and structural-coverage disposition were incomplete. The listed
deferred integration boundaries are not Phase 6a closure claims.
