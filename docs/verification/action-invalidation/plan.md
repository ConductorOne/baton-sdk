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
  owns. The classification error therefore reports its own status
  (`Internal`), not the connector's: `invokeActionPage`'s callers run
  `isWarning` and `ShouldWaitAndRetry` on what it returns, and the
  connector's code must not reach them (CO-010).
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
  spawned, from the action's own list call) cell the outcome is exactly
  one of: honored; classification error naming the cell; unchanged legacy
  handling (retry, warning, or failure as today). No cell is honored by
  accident and no cell changes legacy handling when the detail is absent.
  A detail on any other connector call the handler makes (the grants
  handler's `GetResource`) is a classification error (CO-009).
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
  failed and non-retryable; the runner does not resume it. The syncer
  does not discard the artifact (RFC 0009: retention is a storage
  question; loop prevention is retryability's job); the hosted full-sync
  task removes its temp c1z on every failure as it did before this
  change. The one-call-per-resume behavior above is the fallback for a
  runner that resumes anyway.
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
- **S11 Legacy path is untouched.** With `s.ledgered == false` (a store
  with no page ledger, which is a sqlite c1z), `invokeActionPage` returns
  the handler's error as it did before this change, detail or not. No
  restart is attempted and no sentinel is joined; the sqlite write path
  is being removed on its own branch (CO-008).

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
| C03 | classification table total over P1, with the own-list-call axis | O3 | P1 (702) × 2 | bounded | 1 |
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
| C25 | legacy path: error returned unchanged (explicitly excluded, CO-008) | O3 | — | — | 0 |
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
- **CO-007 (2026-10-26, PR review).** Automated review of #1196: the
  legacy path joined `ErrActionInvalidationUnsupported` onto every
  detail-carrying error, skipping the code, op, root, and spawned checks
  the ledgered path applies, so a detail on `Unavailable` was retryable
  on pebble and non-retryable on sqlite. `wrapUnsupportedInvalidation` now
  runs `classifyActionInvalidation` first and returns its misclassification
  error; the sentinel is joined only where the ledgered path would
  restart. S11 reads accordingly; C25 gains the misclassified cell. The
  same review noted that an SDK predating this change rewrites a pending
  item without `invalidations`, which resets the per-item cap for a sync
  resumed across SDK versions; recorded in the PR's rollout notes, not
  tested (such an SDK cannot restart, so the reset cannot compound).
- **CO-008 (2026-10-26, PR review).** Owner decision: the sqlite write
  path is being removed on a separate branch expected to land within
  days, so this change does nothing on the legacy path. CO-004's
  `ErrActionInvalidationUnsupported`, `wrapUnsupportedInvalidation`, the
  `classifySyncError` arm, and `TestActionInvalidationLegacyStoreUnsupported`
  are deleted; CO-007's first half is moot. S11 now says the legacy path
  returns the handler's error unchanged; C25 is explicitly excluded; C33
  covers the cap sentinel only. A sqlite c1z under a connector that
  returns the detail behaves as it does on main today.
- **CO-009 (2026-10-26, PR review).** Independent review of #1196 found
  that `invokeActionPage` classified every handler error against the
  outer action. The grants handler calls `GetResource` for related
  resources after `ListGrants` returned and before any grant is staged,
  so a detail from that call was honored: the grants action restarted at
  a token it never paged on, five times, then failed at the per-item cap
  (confirmed by planting). Each of the five list call sites now wraps its
  error in `actionListCallError`; `classifyActionInvalidation` honors a
  detail only under that mark. S2 and C03 gain the own-list-call axis;
  `TestActionInvalidationNestedConnectorCallMisclassified` is the end to
  end case. The same review noted that the hosted full-sync task removes
  its temp c1z on every failure (pre-existing); S6's retention sentence
  now says so.
- **CO-010 (2026-10-27, PR review).** Automated review of a61d84de: the
  classification error wrapped the connector's status with `%w`, so
  `status.Code` on it was the connector's code. `syncOneAction` and the
  root cases in `parallel_syncer.go` run `isWarning` and
  `ShouldWaitAndRetry` on `invokeActionPage`'s return, so a detail on
  `NotFound` finished the action with a warning and the sync succeeded
  (confirmed by planting: the sync ended in "too many warnings" instead
  of the classification error), and a detail on `Unavailable` or
  `DeadlineExceeded` was retried. `misclassifiedInvalidationError` now
  reports `Internal` from its own `GRPCStatus` and exposes the sentinel
  and the connector's error through `Unwrap`. The table asserts
  `Internal`, `!isWarning`, and `!ShouldWaitAndRetry` on every
  misclassified cell; `TestActionInvalidationMisclassifiedNotFoundFailsNotWarns`
  is the end to end case on grants, the op whose caller allows warnings.
- **CO-011 (2026-10-27, independent review).** Two more places dropped
  the detail before `classifyActionInvalidation` saw it:
  `getLedgerResourceFromConnector` returned `(nil, nil)` on `NotFound`
  and `Unimplemented` without looking for the detail, so a detail on
  either code from the grants handler's nested `GetResource` was skipped
  silently; the static-entitlements handler's `prefixError` text match
  finished the action before marking the error. Both now check for the
  detail first and let it through to the classifier. `InvalidateActionFromError`
  and `ErrInvalidateAction.GRPCStatus` dereferenced a typed-nil
  `*ErrInvalidateAction`; both now treat it as absent. The proto comment
  on `reason` said it reached the ledger; it is logged only, and the
  comment says so. Test comments: `TestActionInvalidationRestartsAction`
  no longer claims C13 (its fixture has no child work) and states the
  `T2` exception to equality with the uninterrupted run.
- **CO-012 (2026-10-27, independent review).** `IsSyncPreservable`
  (frozen by RFC 0009; older runners branch on it) classifies by gRPC
  code: the cap error exposes the connector's `FailedPrecondition` or
  `Aborted`, so it is preservable there, and such a runner resumes and
  makes one call that fails the same way (S6's fallback); the
  classification error reports `Internal`, so it is not preservable
  there, and a runner that still discards on false drops the partial
  artifact, as it does today for any `Internal`. The frozen surface is
  not changed; the PR's rollout notes state both. Engine test gap:
  `TestPendingWorkRestartRefusesPayload/children` fed scheduling keys
  without row children, which the arity check in `stageWorkTransition`
  would also refuse, so deleting the shape check in `requireRestartShape`
  left the test green. Added `children-aligned` (row children matching
  the keys) and `terminal` (`SetTerminal` with `SetRestart`); both fail
  with the shape line planted out. C06 reads accordingly.
- **CO-013 (2026-10-27, independent review).** Three gaps closed with
  tests. Honored restart end to end was exercised only through
  `ListResources`, so deleting `markActionListCall` at any of the other
  four list call sites left the suite green;
  `TestActionInvalidationRestartsEveryHonoringOp` restarts each of the
  five ops through its own handler against a differential store oracle
  (planted: the entitlements mark removed fails that op's case). The
  restart-shape refusal exercised one record buffer; it now covers all
  six (`resource-types`, `resources`, `entitlements`, `grants`,
  `grant-deletes`, `assets`). No test raced two writers on one item;
  `TestPendingWorkRestartRaceOnOneItem` commits eight restarts of the
  same item at the same revision concurrently and requires exactly one
  commit, `ErrStaleLedgerWork` for the rest, and one invalidation
  recorded (passes 20× under `-race`). The chaos `EffectInvalidateAction`
  comment said `Code` "when set" replaces the helper's code; zero is
  unset, so it reads "when non-zero and not FailedPrecondition".
- **CO-014 (2026-10-27, automated review).** `ffa20d66` deleted
  `notFoundDetailGrantsConnector` and
  `TestActionInvalidationMisclassifiedNotFoundFailsNotWarns` by accident
  while replacing the adjacent nested-connector block; C04 and CO-010
  kept citing the test. Restored unchanged from the parent commit; it
  passes against the current classifier.
- **CO-015 (2026-10-27, resume path).** Every resume test stopped at or
  before the restarted chain's first page. Added: a stop after that page
  committed, resumed at the page reached with the invalidation list intact
  (`TestActionInvalidationResumeMidRestartedChain`); the per-sync budget
  spent in one attempt and enforced in the next on the count restored
  from the ledger (`TestActionInvalidationPerSyncBudgetAcrossResume`);
  C13 with a `ChildResourceType` parent, in the same attempt and across a
  resume (`TestActionInvalidationRestartedChainPushesChildrenOnce`),
  which also showed that the ledger scheduling claim is the only child
  dedupe on the ledgered path; C15 with a connector that deletes the
  first page's resource at the expiry
  (`TestActionInvalidationOverRetention`); and an in-process I2 over the
  five ops × both restart tokens × commit-failed / after-commit, each
  resumed by a new syncer (`TestActionInvalidationAbortAtRestartSweep`,
  twenty cells). `ledgerRestartCrashStore.cut` now returns an error so a
  cut can stand in for the commit failing. Plants: zeroing the count
  restore fails the ten after-commit cells; disabling `HasScheduledWork`
  fails both C13 cases; dropping other buckets in `counterSplit` fails
  the budget-across-resume test.
- **CO-016 (2026-10-27).** The child-under-parent variant of
  `SyncResources` had no restart cell of its own; its identity carries the
  parent, and a restart to `""` makes the `r+1` identity equal the
  first-page identity. `TestActionInvalidationRestartsChildUnderParent`
  expires the child list under one parent and requires the child to run
  again from the start, counted once, with every child resource stored
  and no pending work. The two remaining I2 gaps are closed by
  construction and recorded as such in the evidence: a process kill
  cannot vary by op (one batch), and a takeover resumer is a
  configuration the product does not run.
- **CO-017 (2026-10-27, human review).** Five findings. (1) `uhttp`
  caches every 200 GET for an hour by default, so a restart to `""`
  would replay page one from cache and walk the cached chain back to
  the expired token until the per-item cap failed the sync.
  `connectorbuilder` now clears the caches when a list call's error
  carries the detail (`clearHTTPCachesOnInvalidation`, four call sites;
  `TestListCallInvalidationClearsHTTPCaches` drives a real `uhttp`
  client against an `httptest` server per op, with a control error that
  keeps the cache; planted per call site). The end-to-end case is
  `TestActionInvalidationThroughHTTPCache`: a built connector listing
  through `uhttp` over in-memory gRPC against an `httptest` provider
  that expires its page-two token; the sync completes, page one is
  fetched twice, and with the clear disabled the run ends at the
  per-item cap after five cached replays. (2) `InvalidateActionFromError`
  found the typed error anywhere in the tree, while a gRPC server
  serializes only the first status carrier, so an error behind
  `uhttp.WrapErrors` or `errors.Join(status, typed)` restarted in a
  connector's test and not in deployment. The helper now reads
  `status.FromError`, the same first carrier
  (`TestInvalidateActionFromErrorMatchesWire`); the godoc and proto
  comment say to return it bare or under `%w` only. (3) The reference
  scan in `ledger_report_references.go` read a restarted row with
  restart token `""` as terminal and never checked its `r+1`
  continuation; it now checks every restarted row
  (`TestPendingWorkReferencesCheckRestartedRowContinuation`, planted).
  (4) The cap error's message carried the sentinel text twice;
  `ErrActionInvalidationCapExceeded` is now an alias of
  `c1zstore.ErrInvalidationCapExceeded` and `ActionInvalidationCapError`
  prints `e.Cap` once. `InvalidationCapError.Observed` counted the
  refused page for the per-sync scope and not for the per-item scope;
  both now count committed restarts. (5) Godoc states when the restart
  is not honored. Declined: folding `counterSplit` into
  `countersExcept`, which also builds the call-stat and duration maps
  and would do more work under the write lock, not less.
  Why the plan missed (1) and (2): D9 listed only shapes (bare, `%w`)
  on which in-process and wire agree, so the P3 transport differential
  could not disagree with itself; and the reference case is modeled by
  the chaos connector, which fabricates pages in memory, so no cell had
  `uhttp` between the connector and the provider. Both are axes the
  model lacked, not cells it skipped.
- **CO-018 (2026-10-27, naming).** The feature spoke three dialects for
  one concept (invalidate action, restart, invalidation). Everything a
  connector author or operator sees now uses the engine's word, restart.
  The plan, evidence, and test function names keep "action invalidation"
  as the historical name of this obligation set; read the identifiers
  they cite through this table.

  | Before | After |
  | --- | --- |
  | proto `InvalidateAction` (`action_invalidation.proto`) | `RestartAction` (`action_restart.proto`) |
  | `pagination.ErrInvalidateAction` | `pagination.RestartActionError` |
  | `pagination.NewErrInvalidateAction` | `pagination.NewRestartActionError` |
  | `pagination.InvalidateActionFromError` | `pagination.RestartActionFromError` |
  | `sync.ErrActionInvalidationCapExceeded` | `sync.ErrRestartLimitExceeded` (alias of the c1zstore sentinel) |
  | `sync.ActionInvalidationCapError{Cap}` | `sync.RestartLimitError{Limit}` |
  | `sync.ErrActionInvalidationMisclassified` | `sync.ErrRestartRejected` |
  | `sync.WithActionInvalidationCaps(perItem, perSync)` | `sync.WithRestartLimits(perAction, perSync)` |
  | `c1zstore.ErrInvalidationCapExceeded` | `c1zstore.ErrRestartLimitExceeded` |
  | `c1zstore.InvalidationCapError{PerItem bool, Cap, Observed, Invalidations}` | `c1zstore.RestartLimitError{Scope RestartScope, Limit, Count, Restarts}` |
  | `c1zstore.RestartCaps{PerItem, PerSync}` | `c1zstore.RestartLimits{PerAction, PerSync}` |
  | `c1zstore.LedgerWork.Invalidations` (json `invalidations`) | `Restarts` (json `restarts`) |
  | counter `LedgerActionInvalidations` = `ledger.action_invalidations` | `LedgerActionRestarts` = `ledger.action_restarts` |
  | chaos `EffectInvalidateAction` = `invalidate-action` | `EffectRestartAction` = `restart-action` |
  | files `pkg/sync/ledger_invalidation*.go`, `pkg/pagination/invalidate_action*.go` | `ledger_restart*.go`, `restart_action*.go` |

  Unchanged: `SetRestart`, `LedgerRow.Restarted`, report `restart_pages`.
  No behavior changed; the persisted counter key and work-item JSON field
  are renamed because nothing has shipped with the old names.
- **CO-019 (2026-10-27, post-merge).** CO-017 (2) made the detail's
  wire form a correctness requirement on the connector author: behind
  `uhttp.WrapErrors` or `errors.Join(status, typed)` the restart was
  lost, and with an `Unavailable` wrapper the syncer retried the dead
  token until the task deadline. The repair moves to where the error
  becomes a gRPC status. `pagination.SurfaceRestartAction(err)` returns
  `err` unchanged when `RestartActionFromError` already sees the detail,
  and otherwise, when `errors.As` finds a `RestartActionError` anywhere
  in the chain, an error whose status is FailedPrecondition with the
  detail and `err`'s full message, unwrapping to `err`. The four list
  handlers in `connectorbuilder` call it before `RecordTaskFailure` and
  the cache clear. `RestartActionFromError` and `classifyRestartRequest`
  are unchanged, so the in-process and wire answers still agree; the
  restart's own code wins over the outer code because a
  `RestartActionError` has no other reading. Tests:
  `TestSurfaceRestartAction` (unchanged set, surfaced set, idempotent);
  `TestListCallInvalidationClearsHTTPCaches` and
  `TestActionInvalidationThroughHTTPCache` now run four shapes (bare,
  `%w`, `uhttp.WrapErrors(Unavailable, …)`, `errors.Join(Internal, …)`)
  through the builder and real gRPC. Planted (the four calls removed):
  both behind-status shapes fail the builder test on every op, the
  joined shape fails the sync, and the `WrapErrors` shape does not
  finish inside a 40s test budget. A hand-rolled gRPC server outside
  `connectorbuilder` still has to call `SurfaceRestartAction` or return
  the error bare; the godoc says so.
