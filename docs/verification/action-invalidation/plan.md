# Action invalidation: coverage-driven verification plan

Change under verification: a connector-signaled restart of a pending-work
item. A connector whose page token has expired returns a gRPC status error
carrying an `InvalidateAction` detail. The ledgered syncer commits that as a
page: the work item moves to the restart token at the next revision, no
records or children land, the sync is marked replay-blocked, and the
action's next connector call carries the restart token. Without this, an
expired token fails the attempt, the pending item keeps the token, and
every resume fails identically.

Frozen at `87abf5ed` (`main`), before implementation. Contract sources, in
precedence order: this plan's §1; the pending-work contract in
`c1zstore.LedgerQueue` and `c1zstore.PageWriter` (`pkg/dotc1z/c1zstore/ledger.go`);
`docs/tasks/sound-syncs-solutions-brief.md` §3 (page ledger); the proto doc
comments on `LedgerRow` and `LedgerActionIdentity` in
`proto/c1/storage/v3/records.proto`.

## Scope boundary and stages

This plan spans three staged PRs. Stage 1 is due now. Stages 2 and 3 are
named so their criteria can be deferred rather than omitted.

- **Stage 1 (this PR).** The error detail and its helpers; the restart
  transition on a non-spawned work item; per-item and per-sync
  invalidation caps; replay-blocked marking; counters, summary, and
  ledger-report fields. No row deletion. Spawned work items refuse the
  signal.
- **Stage 2 (deferred).** Spawned chains: honoring a non-empty restart
  token on a `Spawned` item, cancelling the chain's descendant work items,
  releasing their scheduling claims, resetting in-process dedup.
- **Stage 3 (deferred; gated on source-cache replay orchestration).**
  Scoped unwind: post-order deletion of the chain's scope-stamped rows
  under a durable intent marker, replacing the replay-blocked fallback
  when every page of the chain was scoped.

The non-ledgered syncer path (`s.ledgered == false`) never restarts; its
one behavior is S11.

## 0. Guardrails, risk verdict, and authorship

### 0.1 Authorship

Written before implementation, by an author who had read
`ledger_scheduler.go:runLedgerAction`, `ledger_pending.go`,
`pebble/pending_work.go:stageWorkTransition`, `pebble/page_unit.go:Commit`,
`pebble/ledger_report_scan.go`, `retry.go`, and the Lambda transport's
status round-trip while settling the design. The reading-order bias is
real: it is easy to state criteria as "the transition branch does X". §1
and §5 state claims as observables — keys present after reopen, the token
the connector receives on its next call, the error the caller gets — and
never as code paths.

### 0.2 Risk verdict and routing

Escape axes (`docs/REVIEW_CHECKLIST.md`):

- Silence: a restart that commits records, a restart row the report reads
  as a finished action, a resume that hands the connector the expired
  token after the restart committed, or a cap that never fires all produce
  a well-formed `.c1z` and a sync that reports success. Silent.
- Durability: the pending item and the ledger row are the recovery
  authority for the next attempt, which may be a different process and SDK
  version. A new row field and new pending-item fields ship in the
  artifact. Durable.
- Uncontrolled dimensions: crash timing around the restart commit;
  transport (in-process, subprocess, Lambda); whether the connector wraps
  the status error; the attempt that resumes.
- Consumer distance: connectors in other repositories emit the signal;
  the hosted runner interprets the terminal error; the ledger report and
  stats sidecar are read by tooling.

Verdict: **HIGH**. Silent and durable with a version-pair dimension. The
default path is touched: the pending-work transition runs for every page
whether or not the branch is taken, and the error classification at the
page boundary changes for every connector error. The step-up pipeline
applies.

Routing: Pass 2 (checkpoint and resume), Pass 4 (classification and
budgets), Pass 3 (permutation over op × code × cut × transport), and Pass
5 (invariant gating: the invalidation page writes no records) are the heavy
passes. Pass 1, Pass 6, and Pass 7 are selected; Pass 6 states no cost
change on the page commit path.

### 0.3 Decisions frozen here

These are design decisions the implementation must not revisit without a
change order.

- The signal is an error, not an annotation. Old SDKs see an ordinary
  failed page. No request-side capability marker.
- The detail is honored only when the status code is `FailedPrecondition`
  or `Aborted`. A detail on any other code is a classification error that
  fails the sync; it is never silently honored and never silently ignored.
  Rationale: `retry.ShouldWaitAndRetry` retries `Unavailable` and
  `DeadlineExceeded`; `isWarning` completes `NotFound` actions with a
  warning. The detail must not ride a code that another layer already
  owns.
- Stage 1 honors the detail on non-spawned work items for the paginated
  list ops: `SyncResourceTypesOp`, `SyncResourcesOp`, `SyncEntitlementsOp`,
  `SyncGrantsOp`, `SyncStaticEntitlementsOp`. On a `Spawned` item or any
  other op the detail is a classification error.
- The restart token is honored as given on a non-spawned item. `""` is the
  common case. The SDK does not interpret it.
- The invalidation commits as a page: one batch carrying the row and the
  pending-item transition. Nothing else in that batch: no records, no
  children, no facts set after the connector call. Facts set by the
  handler before the connector call may land; they are monotone sync flags
  and the restarted page sets them again.
- Replay is blocked for the sync on the first invalidation, with a new
  ingest-quality reason flag. Stage 3 narrows this.
- Caps: per work item and per sync, both configurable with defaults named
  in OQ-1. On the per-item cap the sync fails with the connector's
  original error wrapped and the invalidation history attached; the
  pending item is left at the expired token with its count at the cap, so
  a resume makes exactly one connector call and fails again. On the
  per-sync budget the same, for whichever item trips it. The cap error
  is non-retryable at the task layer (S6): a sync that trips a cap is
  finished, not paused. Retention is untouched; the artifact stays.
- Counting: an invalidation page is not a completed action and does not
  advance progress. It increments `ledger.action_invalidations` and
  `ledger.action_invalidations.<op>`.
- Translating a provider's expired-token response into the signal is the
  connector's job. The SDK's `uhttp` maps HTTP 400 and 410 to
  `InvalidArgument` today, so the provider error reaches the syncer as
  `InvalidArgument` with no detail and fails the page as it does now. A
  connector that wants the restart emits the helper's error instead.
  `InvalidArgument` carrying the detail is a classification error like any
  other non-honoring code. The SDK does not match provider message text.

### 0.4 Reference case

Production logs for the Microsoft Entra connector, 2026-10-11 to
2026-10-26 (the retention window), anonymized. This is what the plan must
fix; the cells below are the ones it must exercise first.

Microsoft Graph returns two shapes for an expired pagination token:

- HTTP 400, `The specified page token value has expired and can no longer
  be included in your request.` Seen on `groups/{id}/members` and
  `servicePrincipals/{id}/appRoleAssignedTo` during grant sync, and on a
  root resource list.
- HTTP 410, `Skip token has expired. Restart pagination from the first
  page.` Seen only on the audit-log and sign-in event feeds, where the
  connector already clears the token and retries from the window's filter
  dates. Not a sync path; listed so the implementer does not key the
  detector to one status.

Both arrive at the syncer as `InvalidArgument` through `uhttp`.

Scale over the window: about 1,180 failed sync attempts across 7 production
connectors in 6 tenants. By phase: grants for `enterprise_application`
resources 789 attempts over 4 resources; grants for `group` resources 352
over 8; root resource listing 36 (plus 3 in a non-production environment).

The resume loop the plan describes is observed, not inferred. Per
connector and resource, the same pending item fails on every resume: one
enterprise application failed 673 consecutive attempts over 5.5 days at
roughly one attempt per 12 minutes; one group 152 attempts over 31 hours;
eleven further streaks of 9 to 69 attempts. Every attempt logs `c1z-sync:
syncer.Sync() returned error, preserved c1z checkpoint`, so the checkpoint
being preserved is the one holding the dead token. Streaks end only on
external intervention.

Mapping to this plan: the dominant cell is `SyncGrants` per-resource,
mid-chain (D2), non-spawned, restart token `""`, through the subprocess or
Lambda transport. The secondary cell is `SyncResources` per-type,
mid-chain. The connector-side detector is out of this repository; stage 1
is complete here when those two cells pass P2, P3, and P4 end to end
through I4 configured to the Graph shapes (§7).

## 1. Frozen core: stage claims (S#)

Stated on the store, the artifact, and the connector call sequence.

- **S1 Signal shape.** A status error with an `InvalidateAction` detail
  constructed by the SDK helper has code `FailedPrecondition`. The
  extractor finds the detail on a bare status error, on a status error
  wrapped with `%w`, and on a status error that crossed the Lambda
  transport; it returns the restart token and reason unchanged.
- **S2 Classification is total.** For every (code, detail present, op,
  spawned) cell the outcome is exactly one of: honored; classification
  error naming the cell; unchanged legacy handling (retry, warning, or
  failure as today). No cell is honored by accident and no cell changes
  legacy handling when the detail is absent.
- **S3 Atomic restart.** After an honored invalidation, either all of the
  following are present after reopen or none is: a ledger row for
  `(identity at the expired token, workID, revision r)` marked restarted
  and recording the restart token; the pending item at revision `r+1`
  with its identity token equal to the restart token, revision `r`
  appended to its invalidation list, and every other field (`SyncID`,
  `SchedulingKey`, `Spawned`, `TypeScopedPlanned`, the rest of the
  identity) equal to its value before; the invalidation counters in the
  worker's bucket. No record of any kind, no child work item, and no
  scheduling claim is present from that page. A restart whose token is
  `""` is a restart, not a finish: the pending item is present, not
  deleted.
- **S4 Next call carries the restart token.** In the same attempt and on
  resume by a different process, the connector's next call for that work
  item carries the restart token when S3 committed, and the expired token
  when it did not. Never a third value. A takeover by another attempt
  observes the same.
- **S5 Equal work after restart.** Children the restarted chain pushes
  whose scheduling key is already claimed are not pushed again; children
  whose key is new are pushed once. After the chain completes, no two
  pending or completed work items share a scheduling key.
- **S6 Caps bound the loop.** With per-item cap `k`: `k` invalidations
  honor; the `(k+1)`th fails the sync with the original error wrapped and
  leaves the pending item unchanged; a resume calls the connector once for
  that item and fails identically. The per-sync budget is the fold of
  `ledger.action_invalidations` across every worker bucket, read inside
  the restart batch under the engine write lock, so it is durable across
  attempts and two workers cannot both read `budget-1`. Both caps appear
  in the terminal error with the invalidation list (the revisions in the
  pending item) and in the sync summary. Both cap errors carry the
  sentinel `ErrActionInvalidationCapExceeded`, and
  `c1api.classifySyncError` joins `ErrTaskNonRetryable` onto any error
  that `errors.Is` the sentinel, the same way it does for
  `ErrIngestInvariantViolated`. The task manager reports the sync as
  failed and non-retryable; the runner does not resume it. The artifact
  is preserved (RFC 0009: retention is a storage question; loop
  prevention is retryability's job). The one-call-per-resume behavior
  above is the fallback for a runner that resumes anyway.
- **S7 Replay blocked, durably.** After the first honored invalidation the
  sealed artifact's stats sidecar carries `SourceCacheReplayBlocked = true`
  with the new reason flag set, and the `ledgerFactIngestBlocked` fact is
  present. A crash after the invalidation commit and before seal preserves
  this through resume.
- **S8 Report and summary.** The ledger report reads a restarted row as
  non-terminal; its action and page counts exclude invalidation pages
  from completions and include them under invalidations by op. The sync
  summary carries total and per-op invalidation counts.
- **S9 Scrub.** After seal without retain, the restart token on a restarted
  row is absent from every SST and WAL and its hash is present; with
  retain it is present. Identity compare on a scrubbed restarted row
  behaves as on any scrubbed row.
- **S10 Cost.** The page commit path for a page without an invalidation
  does the same number of batch operations as before; the restart branch
  adds one pending-item read and write already paid by the continuation
  branch. No new index, no new iterator.
- **S11 Legacy path cannot restart and says so.** With `s.ledgered ==
  false` (a store with no page ledger, which is a sqlite c1z), a status
  error carrying the detail fails the sync with the connector's code and
  text unchanged, joined with `ErrActionInvalidationUnsupported`;
  `classifySyncError` marks it non-retryable. An error without the detail
  is returned exactly as today. No restart is attempted: there is no
  pending-work item to move.

## 2. Failure properties and oracles (O#)

- **O1 Durable-state oracle.** Close (or `CrashClone`) and reopen; read the
  pending item by ID, the ledger row by `(identity, workID, revision)`, the
  worker's counter bucket, and a raw iterator over the record families
  with the sync's key prefix. Presence and values after reopen are the
  oracle, never process state.
- **O2 Connector call-sequence oracle.** A recording connector captures
  every `List*` request's page token per action identity. The sequence
  `[..., T_expired, R, ...]` versus `[..., T_expired, T_expired, ...]` is
  the observable for S4; the count of calls after the cap is the
  observable for S6.
- **O3 Classification table.** Table-driven: for each cell of D4 × D5 × D1
  × D3, the returned error's class (honored → no error from the page;
  classification error → a sentinel the test matches with `errors.Is`;
  legacy → the unchanged error). The table is the oracle; each row must
  also fail on a planted wrong outcome (one planted violation per class).
- **O4 Crash dichotomy.** For a crash at each cut in D6 and reopen, O1
  shows exactly one of the two S3 images. No single-run oracle; instrument
  I2 sweeps the cuts.
- **O5 Transport differential.** The same connector behind the in-process
  client, the subprocess gRPC client, and the Lambda transport; O2 and O3
  outcomes must be identical across the three.
- **O6 Scheduling-key uniqueness.** After the restarted chain completes,
  iterate pending-work history and scheduling claims; assert no duplicate
  key and that every child tuple pass 2 re-pushed either has one claim
  (from pass 1) or one new work item, never both.
- **O7 Sidecar and fact oracle.** `ReadSyncStatsRecord` on the sealed
  artifact and `LedgerFacts` before seal.
- **O8 Report projection.** `ledgerReportProject` over a restarted row
  returns `terminal = false` and excludes the row from `TerminalPages`;
  the report's counts match an independent fold the test computes from
  the rows it wrote. The hard case is a restarted row whose restart token
  is `""`: today's projection reads `terminal = !hasNext`, and after
  scrub `terminal = hashPresent && hashEmpty`, so both paths read it as
  finished unless the restart mark is consulted first.
- **O9 Byte-absence oracle.** `checkpointNeedleHits` over SSTs and WALs for
  a planted restart-token needle: zero after seal without retain, non-zero
  with retain (control).
- **O10 Static-upstream differential.** The same corpus synced twice, once
  with a connector that invalidates once mid-list and once that never does;
  the record families must be byte-equal (ledger families excluded). With
  an upstream deletion between the two passes, the deleted row is present
  in the invalidating run and absent in the control — the accepted
  over-retention, asserted so the behavior is documented, not discovered.
- **O11 Cost.** Batch-operation count per page commit, with and without the
  change, on the no-invalidation path. Equal.

## 3. Coverage dimensions and cross-product (D#, P#)

### 3.1 Axes

- **D1 Op variant (10, over 7 ops):** `SyncResourceTypes`;
  `SyncResources` per-type and child-under-parent; `SyncEntitlements`
  per-resource and type-scoped; `SyncGrants` per-resource and type-scoped;
  `SyncStaticEntitlements` per-type — these eight are the honoring
  variants, each a paginated connector call; `SyncTargetedResource`
  (non-paginated `GetResource`, must reject); `SyncGrantExpansion` (no
  connector call, must reject). The `SyncResources`, `SyncEntitlements`,
  and `SyncGrants` roots read the store and make no connector call, so
  the signal cannot arise there; they are not cells.
- **D2 Page position (2):** first page (identity token `""`); mid-chain.
- **D3 Spawned (2):** no; yes (rejects in stage 1).
- **D4 Status code (8):** `FailedPrecondition`, `Aborted`, `Unavailable`,
  `DeadlineExceeded`, `NotFound`, `Unknown`, `Internal`,
  `InvalidArgument`. The last is the code the reference case arrives
  under today (§0.4); its detail-absent cell is the current failure and
  its detail-present cell is a classification error.
- **D5 Detail (3):** absent; present with `""` restart token; present with
  a non-empty restart token.
- **D6 Cut point (5):** F1 the invalidation page's batch returns an error;
  F2 crash after the batch commits, before in-memory publish; F3 crash
  after publish, before the next page; F4 crash during the first page of
  the restarted chain; F5 no crash.
- **D7 Resumer (4):** same attempt; new attempt, same process identity;
  takeover by another attempt; the stale attempt committing a
  continuation or a second restart after a takeover restart. The last is
  the losing side of the fence: after a restart to `""` the pending
  identity at `r+1` equals a first-page identity, so only the revision
  fences it; the stale commit must get `ErrStaleLedgerWork` and leave no
  row, record, or child.
- **D8 Transport (3):** in-process; subprocess gRPC; Lambda.
- **D9 Wrapping (2):** bare status error; wrapped with `%w` once by the
  connector. The SDK wraps again in `collectLedgerGrants`, so the
  extractor sees one or two layers; both values here are in addition to
  that wrap.
- **D10 Invalidation count (5):** 0 → 1; `k-1` → `k`; `k` → `k+1`; sync
  budget boundary with two items in one attempt; sync budget boundary
  with two items invalidating concurrently under two or more workers.
- **D11 Seal (2):** scrub; retain.
- **D12 Syncer path (2):** ledgered; legacy.

### 3.2 Sub-products and cell counts

- **P1 Classification** = D1 × D3 × D4 × D5 × D9: 10 × 2 × 8 × 3 × 2 =
  960 cells, realized as one table (O3). Reductions: R1, D9 collapses for
  detail-absent cells (wrapping cannot change a missing detail): −160. R2,
  D1 collapses across the eight honoring variants for detail-absent cells
  on every code except `NotFound`: legacy handling of `NotFound` depends
  on the op's `allowWarning` (`invokeActionPage` warns only when it is
  set, and it is set for entitlements, grants, and static entitlements
  but not for resource types or resources), so those cells stay. −98
  after R1. Expected after reductions: 702 table rows. The expected-class
  function takes the op. The test enumerates the axes and reports its own
  count; a mismatch with 702 is a finding.
- **P2 Restart durability** = honoring variants of D1 (8) × D2 × D5
  present (2) × D6 × D7: 8 × 2 × 2 × 5 × 4 = 640 cells. The restart-token
  axis is in P2 because `""` is the reference case and collides with the
  engine's finish branch (`stageWorkTransition` deletes the pending item
  when the row's next token is `""`); a restart to `""` must be
  distinguishable by the row's restart mark at every cut. Reduction: D7
  collapses for F5 (no crash, no resumer): −96. 544 cells, swept by I2.
- **P3 Transport** = D8 × D5 (detail present only) × D9: 3 × 2 × 2 = 12
  cells, O5.
- **P4 Caps** = D10 × D7 (same attempt, resumed): 10 cells, plus one cell
  where the restart token equals the expired token (O1 shows `r+1`; the
  cap fires on schedule): 11 cells, O2 + O1. Elsewhere test restart
  tokens differ from `T_expired`, so O2's `[T, R]` is distinguishable
  from `[T, T]`.
- **P5 Seal** = D11 × {restarted row present}: 2 cells, O9.
- **P6 Equal work** = honoring variants that push children
  (`SyncResources` per-type and child-under-parent; the others push
  children only through `EnqueuePageTokens`, which an error response
  cannot carry) × D2: 4 cells, O6 and O10.
- **P7 Legacy** = D12 legacy × D4 × D5 present: 16 cells, O3 (unchanged
  error).
- **P8 Reference case** = the two §0.4 cells (`SyncGrants` per-resource
  and `SyncResources` per-type, mid-chain, non-spawned, restart token
  `""`) × D8: 6 cells. The chaos connector runs in Graph mode (I4): the
  listed collection is larger than the pages a token survives, the
  provider error is the 400 text, and the connector-side translation to
  the helper's error is applied inside the chaos connector. O1, O2, O10.

### 3.3 What must hold per cell

- P1: the outcome class matches the frozen decision table in §0.3; the
  classification error names the op, code, and spawned flag.
- P2: at F1 the pending item is unchanged and the row is absent; at F2–F4
  O1 shows the committed image and O2 shows the restart token on the next
  call; at F5 the same. Under takeover (D7) the taking attempt sees the
  committed image and never the expired token after commit.
- P3: outcome identical to the in-process cell.
- P4: as S6. The terminal error wraps the original with `%w`; the
  invalidation history in the error is the pending item's invalidation
  list, so it survives resume by another process without a row scan.
- P5: as S9.
- P6: as S5 and O10.
- P7: as S11; the detail-absent controls return the handler's error
  unchanged (same pointer or `errors.Is`-equal, same code, same text).
- P8: the sync completes; O2 shows `[..., T_expired, "", ...]` for the
  item and the full collection's records are present after reopen; with
  the Graph-mode translation disabled, the same scenario fails the sync
  with `InvalidArgument` and the pending item still holds `T_expired`
  (today's behavior, as the control).

### 3.4 Cells this plan excludes, with reason

- A non-empty restart token on a `Spawned` item: stage 2. The stage 1
  behavior (classification error) is covered in P1.
- Deletion of rows written before the restart: stage 3. O10's
  over-retention assertion documents the stage 1 behavior.
- Scope poisoning or scope-key recording: stage 3.
- The restart token being a token the chain already visited this attempt
  (a connector that restarts into its own history): the per-item cap
  bounds it; the `R == T_expired` cell in P4 is the degenerate case; no
  further cell.
- Concurrent invalidations on two work items: the per-sync budget is
  shared state (S6), so this is not excluded; it is the concurrent D10
  cell in P4, run under `-race`.

### 3.5 Non-single-run oracles and their instruments

- O4 (crash dichotomy): I2.
- O5 (transport): I3.
- O11 (cost): I6.

## 4. Criteria ids and coverage levels

Levels: **bounded** (every cell of the named sub-product executed),
**sampled** (named cells executed; closure not claimable), **structural**
(type system or meta-test makes the violation unrepresentable or
unreachable).

## 5. Criteria (C#)

| id | claim | oracle | cells | level | stage |
|---|---|---|---|---|---|
| C01 | helper emits `FailedPrecondition` with the detail | O3 | 1 | bounded | 1 |
| C02 | extractor finds the detail bare, wrapped, and post-Lambda | O3, O5 | D9 × D8 | bounded | 1 |
| C03 | classification table total over P1 | O3 | P1 (702) | bounded | 1 |
| C04 | detail-absent cells unchanged from today | O3 | P1 absent rows | bounded | 1 |
| C05 | spawned item rejects the detail | O3 | D3 yes × honoring ops | bounded | 1 |
| C06 | invalidation page writes no records, children, claims | O1 | P2 F5 | bounded | 1 |
| C07 | restart row present, marked, records the token | O1 | P2 F2–F5 | bounded | 1 |
| C08 | pending item: whole `LedgerWork` equal before/after except revision `r+1`, token `R`, and `r` appended to the invalidation list | O1 | P2 F2–F5 | bounded | 1 |
| C09 | counters in the worker bucket | O1 | P2 F5 | bounded | 1 |
| C10 | crash dichotomy at every cut | O4 | P2 (544) | bounded | 1 |
| C11 | next call carries `R` after commit, `T_expired` before | O2 | P2 × D7 | bounded | 1 |
| C12 | takeover observes the committed image; the stale attempt's commit gets `ErrStaleLedgerWork` and writes nothing | O2, O1 | P2 D7 takeover and stale | bounded | 1 |
| C13 | restarted chain re-pushes claimed children zero times | O6 | P6 | bounded | 1 |
| C14 | static-upstream differential byte-equal | O10 | P6 | bounded | 1 |
| C15 | over-retention present and asserted | O10 | P6, one cell | bounded | 1 |
| C16 | per-item cap fails with wrapped original | O2, O1 | P4 | bounded | 1 |
| C17 | post-cap resume calls connector once and fails | O2 | P4 resumed | bounded | 1 |
| C33 | `classifySyncError` joins `ErrTaskNonRetryable` on the cap error and on the legacy unsupported error; a `FailedPrecondition` without either sentinel stays retryable; `ShouldDiscardSyncArtifact` is false for all three | O3 | 4 | bounded | 1 |
| C18 | per-sync budget across two items, in one attempt, across a resume, and concurrently under two workers | O2, O1 | P4 budget cells | bounded | 1 |
| C19 | replay blocked flag and fact, durable across crash | O7 | P2 F2 + seal | bounded | 1 |
| C20 | report reads restarted row as non-terminal and excludes it from `TerminalPages` | O8 | honoring ops × D5 present × D11 (32) | bounded | 1 |
| C21 | report and summary counts match independent fold | O8 | P2 F5 | bounded | 1 |
| C22 | scrub removes the restart token; retain keeps it | O9 | P5 | bounded | 1 |
| C23 | identity compare on scrubbed restarted row | O1 | 1 | bounded | 1 |
| C24 | no-invalidation page commit cost unchanged | O11 | 1 | sampled | 1 |
| C25 | legacy path: detail-absent unchanged; detail-present keeps code and text and carries `ErrActionInvalidationUnsupported` | O3 | P7 + 8 detail-absent controls | bounded | 1 |
| C26 | transport differential | O5 | P3 | bounded | 1 |
| C27 | formal model: restart transition preserves no-skip | I7 | model | bounded | 1 |
| C32 | reference case completes end to end; control fails as today | O1, O2, O10 | P8 | bounded | 1 |
| C28 | spawned chain restart with non-empty token | — | — | deferred | 2 |
| C29 | descendant cancel and claim release | — | — | deferred | 2 |
| C30 | scoped unwind under intent marker | — | — | deferred | 3 |
| C31 | shared-scope detection falls back to poison | — | — | deferred | 3 |

## 6. Closure criteria

Stage 1 closes when every C01–C27, C32, and C33 is **verified to stated coverage** in
`evidence.md` with the artifact, assertion, revision, and command named;
the decorrelated implementation read and the independent evidence audit
against the final code report zero new findings; I2 and I5 soak clean at
the final revision; and `go test -race ./pkg/sync/... ./pkg/dotc1z/...`
passes. C28–C31 limit whole-plan closure and are not stage 1 gaps.

## 7. Required instruments (I#)

- **I1 Classification table test.** One table-driven test over P1 with the
  cell enumerated mechanically (nested loops over the axes, not a
  hand-written list), the expected class from a function of the cell that
  encodes §0.3, and a planted-violation mode that flips one expected class
  and must fail.
- **I2 Crash sweep.** The existing page-ledger crash harness extended with
  the five D6 cuts around the invalidation commit, over the eight honoring
  variants and both page positions, reopened by a fresh process identity and by
  a takeover attempt. Validated by a planted violation that commits the
  pending transition in a separate batch from the row.
- **I3 Transport harness.** The chaos connector behind each of the three
  transports with the same scenario; `baton-compat-harness` already hosts
  the Lambda shape.
- **I4 Chaos mutation `expire-page-token`.** A `Mutation` in
  `internal/chaosconnector/mutation.go` that, for a configured action and
  page, returns the detail error instead of the response, with a knob for
  the code and the restart token. A Graph mode makes the token expire by
  age rather than by page index: a token older than a configured lifetime
  is rejected with the §0.4 400 text as an `InvalidArgument` status error,
  and a translation switch converts that into the helper's error the way
  a connector would. Used by I1, I2, I3, I5, and P8.
- **I5 Soak.** The scheduler soak with I4 enabled at random pages and
  random crash cuts, many seeds, under `-race`; oracle O6 plus O1 at the
  end of every run.
- **I6 Cost probe.** Batch-operation count per page commit before and after,
  on the no-invalidation path. A count, not a timing.
- **I7 Model.** `formal/MODEL_SPEC.md` gains a restart transition on the
  pending-work machine: same work ID, revision `+1`, token replaced, no
  fold events. Property: no page is skipped-not-rerun across any
  interleaving of restart and crash. The model's change-order log records
  it.
- **I8 Report fold.** A test that writes rows including restarted ones and
  asserts the report's counts against a fold computed from the rows.

## 8. Placement map

- Proto: `proto/c1/connector/v2/` new detail message; buf lint and breaking
  gates.
- Helpers: `pkg/connectorbuilder` or a new leaf package; unit tests for
  C01, C02.
- Engine: `pkg/dotc1z/engine/pebble/pending_work.go` transition;
  `ledger.go` scrub; `ledger_report_scan.go` projection. Tests for C06–C10,
  C20–C23 live beside them over the real store lifecycle.
- Store: `c1zstore.LedgerWork` and `LedgerRow` fields.
- Syncer: `pkg/sync/ledger_scheduler.go` intercept; `ledger_pending.go`
  publish; counters and summary. Tests for C03–C05, C11–C19, C25 in
  `pkg/sync`.
- Chaos: `internal/chaosconnector/mutation.go` (I4); soak (I5).
- Model: `formal/MODEL_SPEC.md` (I7).
- Task layer: `pkg/tasks/c1api/full_sync.go` `classifySyncError` (C33).

## 9. Implementation-obligation addendum

Appended after implementation inspection and before evidence is classified.
Inventory, per the checklist: new fields and their lifecycle (scrub, report
projection, JSON compatibility of `LedgerWork` with items written by the
previous SDK); any new counter and where it folds; any closer, iterator, or
batch acquired on the restart branch; the exact commit site and its six
answers (§5.12). Nothing here may revise §1.

Inventory at the implementation under review (uncommitted on top of
`76264ae5`):

- **New fields.** `LedgerRow.restarted` (proto field 26; `LedgerRow.Restarted`
  in `c1zstore`); the restart token rides `next_page_token` (field 2), so
  `scrubTokens` and the hash logic are unchanged and `TestLedgerScrubRestartToken`
  shows the token is scrubbed or retained with the rest. `LedgerWork.Invalidations
  []uint64` (`json:"invalidations,omitempty"`); the previous SDK's decoder
  drops the unknown key (OQ-4), and the field is absent from the JSON when
  empty, so an item that never restarted encodes byte-for-byte as before.
  `ActionCount.InvalidationCount` is `json:"-"`: it is restored from the
  ledger counters by `restoreLedgerState`, so it is not on the checkpoint
  token (`TestGoldenTokenFieldCoverage` enforces that every token field has
  a recorded fixture; this one has none because it is not a token field).
- **New counters.** `ledger.action_invalidations` and
  `ledger.action_invalidations.<op>` in the worker bucket, staged by
  `stageActionRestart`. The per-sync fold is `Ledger.counterSplit`, read
  under the engine write lock inside the restart commit. The engine
  requires the staged bucket's `ledger.action_invalidations` to equal the
  committed value plus one (`stageRestart`), so a page that forgot the
  counter is refused with `ErrPageUnitRestart` rather than under-counted.
- **Resources acquired on the restart branch.** One pebble iterator over
  the counter bounds in `counterSplit`, closed by `defer`. No new batch:
  the restart rides the page batch. No new closer in `pkg/sync`.
- **Commit site.** `pageUnit.Commit` → `requireRestartShape` (refuses a
  restart page carrying records, children, claims, or a terminal mark, and
  a `SetRestart`/`row.Restarted` disagreement) → `stageWorkTransition` →
  `stageRestart` (phase must be collecting; spawned refused; per-item cap;
  bucket fold; per-sync cap; `Invalidations = append(…, Revision)`;
  `Revision++`; `PageToken = row.NextPageToken`; `stagePendingWork`). The
  revision fence (`ErrStaleLedgerWork`) runs before `stageRestart`, so a
  stale attempt's restart is refused the same way as its continuation.
- **No-invalidation path.** `stageWorkTransition` gains `if restart != nil`;
  `pageUnit.Commit` gains `requireRestartShape`, which reads in-memory
  fields only. No key is read or written that was not before. C24 is
  classified by this inspection; no operation-count instrument was built
  because there is no pre-change binary to count against in the same test.
- **Syncer intercept.** `invokeActionPage`: handler error →
  `classifyActionInvalidation` → `stageActionRestart` (requires
  `page.transitions == 0`; stages the transition with the restart token;
  marks the row; clears children and after-commit publishers; `SetRestart`;
  counters; `ReasonFlags` bit 7; facts `sync.ingest_known` and
  `sync.ingest_blocked`). Publication: `publishPendingTransition` treats a
  restart to `""` as a continuation, not a finish; the completed-action
  counters skip restarted pages. `wrapInvalidationCommitError` maps the
  engine's `InvalidationCapError` to `ActionInvalidationCapError`, whose
  `Unwrap() []error` exposes the sentinel, the cap detail, and the
  connector's status.
- **Legacy path.** `wrapUnsupportedInvalidation` wraps with `%w` on both
  sides, so `errors.Is` finds the sentinel and `status.FromError` still
  finds the connector's code and text.
- **Defaults.** `defaultInvalidationPerItem = 5`, `defaultInvalidationPerSync
  = 100` (OQ-1); `WithActionInvalidationCaps` overrides them.

## 10. Open questions (OQ-#) with settling checks

- **OQ-1 Cap defaults.** Settled by the owner after implementation: 5 per
  item, 100 per sync (`defaultInvalidationPerItem`,
  `defaultInvalidationPerSync`; `WithActionInvalidationCaps` overrides).
  Per item: the number of restarts one pending work item may take within
  one sync before the next expiry fails the sync (S6). Per sync: total
  restarts across all items; 100 is set so that a tenant whose many large
  collections each expire once does not trip it, while a connector that
  misfires on every page still does. The tests parameterize the cap and
  assert boundary behavior, not the number. Evidence weighed (§0.4): the
  observed streaks are hundreds of identical attempts on one item, so an
  uncapped restart would turn each into a restart loop rather than a
  failure loop. A collection whose pagination cannot finish inside the
  token lifetime under rate limiting will trip any per-item cap; the cap
  exists to make that a terminal error with the history attached, not to
  make it succeed. A per-item cap of `k` means such a resource fails on
  its `(k+1)`th expiry within one sync.
- **OQ-2 `Aborted`.** Settled during implementation. `pkg/` attaches no
  meaning to `Aborted` on connector calls (its only uses are
  `IsSyncPreservable` and the credential provider), so both codes are
  honored (`honorsInvalidationCode`); the helper emits `FailedPrecondition`.
- **OQ-3 Pre-call facts.** Settled during implementation. The honoring
  handlers (`collectLedgerResources`, `collectLedgerResourceTypes`,
  `collectLedgerEntitlements`, `collectLedgerGrants`,
  `collectLedgerStaticEntitlements`) set no fact before the connector call
  returns; the facts they set afterwards are behind the error return. Only
  `stageActionRestart`'s own facts land in the restart batch, and both
  (`sync.ingest_known`, `sync.ingest_blocked`) are monotone. The same read
  found CO-006.
- **OQ-4 `LedgerWork` JSON skew.** Settled (CO-004). `LedgerWork` decodes
  with `encoding/json`, which drops unknown keys, and `invalidations` is
  `omitempty`, so an item that never restarted encodes as before. No stamp
  bump.
- **OQ-5 Hosted runner on the cap error.** Settled (CO-003). Retention
  by error code is ruled out by RFC 0009; `ShouldDiscardSyncArtifact`
  answers only to `ErrArtifactUnusable` from `Close()`. The cap error
  therefore stops the loop through retryability: `classifySyncError`
  marks it `ErrTaskNonRetryable` by sentinel (C33). The artifact is
  preserved. Starting a fresh sync after the failure is the schedule's
  job, not this change's.
- **OQ-6 Type-scoped roots.** Whether a type-scoped `SyncEntitlements` or
  `SyncGrants` page can carry `EnqueuePageTokens` and an invalidation in
  the same response. An error carries no annotations, so no; recorded
  here so the reviewer does not re-derive it.

## 11. Change-order log (CO-###)

The plan is frozen at `87abf5ed`.

- **CO-001 (2026-10-26, before implementation).** Production log review
  of the Entra connector. Added §0.4 (reference case), the §0.3 bullet on
  connector-side translation, `InvalidArgument` to D4 (P1 602 → 688, P7
  14 → 16, C03 cells), P8 and C32 (reference case end to end with a
  control), the I4 Graph mode, and the evidence note under OQ-1. No S#
  claim changed. Reason: the plan named the mechanism but not the error
  shape or the cells production actually hits; the first attempt to find
  the error in logs missed it because the text lives in an attribute
  free-text search does not scan.
- **CO-002 (2026-10-26, before implementation).** Independent review of
  the plan against the scheduler, engine, retry, and report code. S3 now
  names every pending-item field that must be preserved, stores the
  invalidation history as a revision list in the pending item (a count
  cannot reproduce the history after resume), and states that restart to
  `""` is not a finish. S6 freezes the per-sync budget as a fold of the
  worker buckets read under the engine write lock inside the restart
  batch, and records that the cap error is preservable so the cap ends
  the restart loop rather than the attempt streak. P2 gains the
  restart-token axis (`""` collides with the finish branch and the
  report's `terminal = !hasNext`; 208 → 544). R2 no longer collapses
  `NotFound` (legacy handling depends on `allowWarning`; P1 688 → 702).
  D7 gains the stale-attempt side of the fence; D10 gains the concurrent
  budget cell; P4 gains `R == T_expired` (8 → 11). C08 compares the whole
  `LedgerWork`; C12, C18, C20 widened; O8 names the `""` projection case.
  D9 notes the SDK's own `%w` wrap. OQ-5 partly settled. No S# claim
  reversed; S3 and S6 are sharpened, not changed in direction.
- **CO-003 (2026-10-26, before implementation).** Owner decision: a
  work item that trips its invalidation cap must end the sync, not leave
  it resuming forever. CO-002's S6 text said the cap "ends the restart
  loop, not the attempt streak"; that was a bounded loop inside an
  unbounded one and is withdrawn. S6 now routes the cap error through
  retryability (`ErrActionInvalidationCapExceeded` → `ErrTaskNonRetryable`
  in `classifySyncError`), per RFC 0009's split between retention and
  loop prevention. §0.3 caps bullet, OQ-5, C33, and §6 updated.
  Placement: `pkg/tasks/c1api/full_sync.go` joins the §8 map.
- **CO-004 (2026-10-26, during implementation).** Owner question: a c1z
  built before this change. Pebble c1zs need nothing (the pending item is
  rewritten in the new shape on restart; an older SDK resuming it drops
  the unknown field). A sqlite c1z has no page ledger, so S11 as frozen
  left it in today's resume loop under a different code. Owner decision:
  non-retryable. S11 now joins `ErrActionInvalidationUnsupported` onto
  the detail-carrying error; C33 and C25 widened; scope boundary
  reworded. Code and text of the connector's error are unchanged, so no
  other S# moves.
- **CO-005 (2026-10-26, during implementation).** I7 and C27 rest on a
  premise that does not hold: `formal/MODEL_SPEC.md` models the legacy
  action stack and checkpoint token (MSyncAttempt, MStore) and has no
  pending-work machine, no work ID, and no revision; neither does any P
  source under `formal/`. Adding "a restart transition on the pending-work
  machine" means modeling the page ledger first, which is a separate
  piece of work. C27 is reclassified **not assessed, follow-up**: the
  no-skip property across restart and crash is discharged in stage 1 by
  the engine crash dichotomy (`TestPendingWorkRestartCrashDichotomy`), the
  process-crash dichotomy on the public path
  (`TestActionInvalidationCrashProcess`), and the stale-attempt fence
  (`TestPendingWorkRestartFencesStaleAttempt`); the model obligation moves
  to whole-plan closure alongside C28–C31. §6 is not edited: it already
  lists C27 among the stage 1 criteria, and this entry records that it
  does not close in stage 1 and why.
- **CO-006 (2026-10-26, during implementation).** D1 says the roots of the
  per-type ops make no connector call. The static-entitlements root does:
  `syncLedgerStaticEntitlements` with no resource type paginates
  `ListResourceTypes` through `listLedgerStaticResourceTypes` on a token of
  its own, not the action's. A detail from that call, classified by op
  alone, would restart the root at a token the root never reads.
  `honorsActionInvalidation` now takes the action: resource types are
  honored; resources, entitlements, grants, and static entitlements are
  honored only with a resource type set. D1's root sentence is corrected
  by this entry; the honoring variants are unchanged. P1 gains a root axis
  (the table enumerates `ResourceTypeID` in `{"", "group"}`), and the
  classification error names the resource type. S2's class for a root
  detail is the classification error, which is where an undeclared cell
  belongs. No other S# moves.
