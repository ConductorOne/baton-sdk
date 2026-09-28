# Syncer ledger implementation

The [frozen plan](plan.md) and its change orders define the observable contract.
The [pending-work design](pending-work.md) defines the current recovery algorithm.
Original briefs and intermediate designs are retained in the
[pre-trim implementation record](https://github.com/ConductorOne/baton-sdk/blob/fda242827a2e177e7f459d802502fd8f4f8fbe2b/docs/verification/syncer-on-ledger/implementation.md).

## Execution and durability

Pebble uses the ledger unconditionally; SQLite retains checkpoints. Attachment
resolves capabilities once and rejects disagreement with engine metadata.

The existing scheduler runs collection handlers against PageWriter. A page commit
publishes records/indexes, facts, the worker's cumulative counter bucket, completed
report history and the pending-work transition together. An action must transition
exactly once; errors discard its staged page. In-memory progress follows commit.

Pending work has independent IDs/revisions. Equal request arguments are distinct
executions. Resume loads at most100 stack entries; the existing worker queue admits
new children in windows of64. Completed history is not traversed for recovery.
Resource-child scheduling relations live in an index rather than a growing map.
Startup constructs the runtime and scheduler from the same final read after any
required migration or seed. Prior counter folds and legacy graph payloads are
not retained in the runtime. Lifecycle preparation classifies the next operation
once; the engine still independently checks its mutation preconditions.

Attempt accounting owns cumulative run-level observations and local-phase counts.
Live stats retain historical totals for logging; they are never written as a new
attempt bucket. Response annotations are decoded before commit and the captured
values feed post-commit live updates.

Legacy V0/V1/V2 tokens are parsed before the store atomically clears the token,
saves migration provenance and seeds pending actions/facts/counters. Pending work
then becomes authoritative. Stale compaction provenance is dropped. Local expansion
rebuilds its transient graph rather than importing inline checkpoint graph state.

Expansion and external import keep their ordinary handlers and optimized store
capabilities. Only whole-phase completion removes their pending item. A crash may
repeat local processing; there is no transaction buffering those phases. The
known upstream external-import annotation replay issue remains a separate fix.

Static-entitlement responses capture templates in pending arguments. Each local
materialization page covers one resource listing page, preserving overwrite order.
This bounds staged output, not arbitrary connector response or template bytes.

## Lifecycle and reporting

Binding a finished sync preserves its identity, data and lifecycle metadata.
New requested processing clears old history and applies current diagnostic flags;
an unfinished processing declaration prevents a failed seal being mistaken for
another finished binding. Missing queue state is not an empty queue.

Default sealing saves the mechanical report/options, drops history and purges
its physical token residue once. Explicit ledger debug mode (independent of logging verbosity) retains scrubbed history and enables
indexed reference checks; explicit token retention requires debug mode and warns.
Unfinished resumes honor durable retention. Report-generation failure saves an unavailable result and still discards
history. Recovery-archive write failure prevents sealing and remains retryable. Final cleanup removes the token-free completion declaration only after
seal succeeds. Already-finished low-level reseal and durable disposal recovery
are separate from permission to finish an uninitialized collection.

The report aggregates writes, collection counts/exclusions, empty responses,
pagination, connector latency, observed waits, retry attempts/errors, phase time
and non-secret effective options. The scanner skips unneeded protobuf fields and
keeps bounded top lists. Debug references use exact work IDs/revisions and indexed
lookups; default reporting is one forward scan. No AI-generated interpretation is
part of the artifact or logs.

Ingest quality's behavior-controlling knowledge is durable fact state; numeric
observations are counters. Missing legacy knowledge remains unknown. Fresh and
resumed pages use the engine's existing NoSync policy; a crash comparison uses
the pages present in the durable image, not every returned commit.

## Verification

[evidence.md](evidence.md) records current checks and conservative per-criterion
status, with links to preserved tests/mutants and uncovered cells. The write hook
rejects unexplained writes during pages and writes during pending-state restore;
explicit session-state bypasses and lifecycle writes have their own checks.
[README.md](README.md) contains performance results and reproduction commands.

## Bounded attempt metadata (CO-030)

Before constructing the resumed runtime, fold old counter buckets into one fixed
bucket in a synced batch. Delete only the folded keys; broad overlapping range
tombstones made repeated preparation unnecessarily expensive. The current attempt's buckets remain untouched, including
on a repeated preparation call. Skip the write when only the folded bucket and
current attempt are present. This preserves cumulative worker replacement (C22,
C28) and bounds future resume work by workers and counter labels, not attempts
(C46). Classification of previously empty state precedes this lifecycle write.

Store first/latest options at two fixed fact keys, preserving first across finished
same-ID processing. Stage them under the existing page-commit mutex so concurrent
workers cannot replace first. Publish a runtime flag only
after the page with latest options commits, so failed pages cannot suppress their
retry's options. Reports and option lookup expose only these two snapshots (C50).

Implement storage with failure/crash/scale tests first, then connect the syncer and
replace attempt-indexed option snapshots. Check the migration, cold-resume,
quality and disposal suites before publishing. This does not bound connector
response sizes, selected scope lists or completed diagnostic history.

## Combined historical migration check

Have eb63f1b5 itself collect two resource types, four resources, four entitlements,
and two of four grants using four workers. Stop only after both grant actions
reach their second page, then save the real checkpoint. Also produce the complete
old-SDK artifact as an independent data/index/digest reference. The current SDK
must finish an uncut migration and process-death cuts before/after takeover,
before/after a grant page, and before/after the terminal page. Resume with one or
four workers. Reject any connector call for pre-checkpoint completed phases or
committed grant pages, and verify the complete saved artifact after reopen.

Compare collected data/indexes/digest with the completed historical artifact;
compare committed accounting with uncut migration of the same checkpoint and
explicit imported totals plus remaining work. Normalize durations only for this
accounting comparison. The old SDK's two deliberately canceled calls are already
part of the checkpoint and must not be confused with newly committed page calls.
These cases combine C24–C28 with C04/C16/C31; they do not claim arbitrary storage
I/O failure or real power-loss coverage. No binary fixture is committed.

### CO-031 implementation obligation

Separate plain ending from checked completion in the existing engine finalizer.
Plain ending preserves the entire ledger and its format declaration, even if a
previous completion attempt set disposal facts. It still builds indexes, writes
ended_at, persists statistics and flushes/detaches. No new durable intent marker
is needed: before ended_at the unchanged recovery state remains unfinished;
after it the same recovery state remains available to explicit binding.
Move the pure ledger-to-SyncStats conversion into c1zstore so the syncer and
plain engine ending use identical accounting. Preserve completion guards and
cleanup for EndSyncWithStats. Test the current refusal before removing it, then
exercise cold reopen, stamp failure/retry and reset. No SQLite path changes.

### CO-033 service-mode rollback instrument

Use NewConnectorRunner in daemon mode with its real C1 task manager, authenticated
local TLS/gRPC client, heartbeat loop, full-sync handler and streaming upload.
Only C1's token/task API and connector endpoint data are simulated. Compile the
same child connector fixture on current and historical SDK revisions. The parent
keeps one API endpoint and persistent directory while replacing child processes.
Kill after the second resource request proves the first page has returned; inspect
the surviving current-engine database to establish committed ledger work. Also
exercise a reported connector error. With spare retention enabled, first finish
a new-SDK task to seed the spare. Requeue the same task after replacement and
require a validated upload, successful FinishTask and another accepted task.
Keep the cross-version build opt-in; no production injection hooks or retry
policy changes. Results must distinguish SDK daemon behavior from C1 workflow
redelivery, which the local API controls.

### CO-034 ended-empty rebind

Carry the existing bounded pending lookup's nonempty result into lifecycle
selection. An ended, empty binding uses the existing finished-processing branch:
clear prior history and processing policy, then seed Init with current options.
Pending bindings resume, and unfinished empty bindings proceed to sealing. Change
ClearLedgerRows to refuse actual pending work rather than any initialization
marker, checking under the write lock. Its existing synced batch removes the old
queue declaration with history; existing failure/crash cuts cover that boundary.
Validate the public unexpanded upload/host expansion sequence before the fix,
then rerun lifecycle, pending, seal, compactor and race checks. Keep expansion's
existing handler and storage capabilities unchanged.

### CO-035 seal-to-expansion handoff

After recovery preparation, an expansion-only request with no pending actions
finishes the previous pass before entering the existing finished-rebind path.
Keep the previous request's option snapshot while preparing that seal; do not
persist an expansion-complete graph for work this invocation has not performed.
After successful seal/report archival, rebind the same sync ID, restore the
current request's retention settings and initialize the requested pass with a
fresh accounting attempt. A failed old seal never starts new work. A failed
handoff never returns success. Initialization makes the next pass nonempty, so
this handoff cannot recurse repeatedly under the store contract. Final cleanup
and the success log belong to the completed requested pass only.

Explicit expansion-only calls after a drained pass may redo deterministic
expansion. Tests must count both executed passes honestly rather than suppress
new work to keep counters equal to a one-call reference. Pending-work recovery
continues to avoid reseeding while an expansion item is present.

### CO-037 implementation

Storage. The work-state value gains a phase byte: `{version 2, lastID, phase}`,
phase ∈ {collecting, sealing}. Version-1 values are rejected as invalid; no
released file carries one. `PendingWork`/`PendingWorkAfter` return
`LedgerQueuePhase` ∈ {absent, collecting, sealing}. `PageWriter.SetQueueSealing`
stages the terminal transition; `pageUnit.Commit` validates it under `writeMu`
before staging anything: phase is `collecting`, the pending range is empty, the
row has no continuation and no children. The terminal page carries the run
bucket, terminal facts and the phase in one batch, as today minus `seal_ready`.

`endSync` with stats requires phase `sealing`, or `ended_at` set with no
declaration (engine-level reseal). Finalize order: deferred indexes and manifest
counts as today; token-bearing disposal (rows, pending, frontier, scheduling) or
scrub in retained mode; residue purge; clear the in-flight stamp; then one
`RecordBatch` with the archive value, deletion of the remaining family (default:
facts, counters, declaration, policy fact; retained: declaration only), and the
sync-run record with `ended_at`, committed `pebble.Sync`; then `PersistSyncStats`
and `FinishSync`. Delete the post-stamp block, `sealDiscardsRows` gating in
`endSync`, the `pendingOnly` logic, the marker-clear hooks and their `SealCost`
share. The archive stager is a `RecordBatch` method for the engine-meta key; the
sync-run record already has one. Report generation failure still records
`unavailable` in the archive.

`BeginPass(ctx, seeds, clearFacts)` replaces `ClearLedgerRows` and
`RestoreLedgerArchive`: under `lifecycleMu` and the write lock, require
`ended_at` and no declaration; read the archive (same sync ID, or `Compacted`
record); stamp in-flight; one synced batch restores archived facts except
`clearFacts`, stages the archived counters as the `"archived"` takeover bucket,
deletes rows, scheduling relations and the frontier, stages the seeds and the
declaration at phase `collecting`. `InitializePendingWork` keeps its role for
never-started syncs and refuses when `ended_at` is set. `Ledger.Drop`,
`ResetForNewSync`, `scopedRanges` and the raw capability inventory are unchanged
in coverage; the phase byte lives in the existing key.

Syncer. `ledgerResume` carries `phase`, `hasPendingWork` and whether a legacy
token exists. `preparation` is a switch on phase; `ended_at` is read only in the
absent case. `prepareLedgerState` loses the fact-count and `discardPending`
heuristics and the restore call. `loadLedgerResume` takes over a token only when
the declaration is absent and errors when one is present. `restoreLedgerState`,
`refreshPendingWindow` and `prepareSealWithOptions` require a non-absent phase and
drop their `seal_ready`/`discard` special cases; `prepareSealWithOptions` calls
`SetQueueSealing` on the terminal page and returns early when the phase is already
`sealing`. `seal` requires phase `sealing`. `finishPreviousRequest` is unchanged
in effect: it seals the drained pass, rebinds, and the recursive call finds
absent + `ended_at` and begins the requested pass. `c1z.discard_ledger_on_seal`
stays in `terminalFacts` as the policy input to finalize.

Tests. Add (a), (b), (c) from the change order as public-API tests using the
existing crash fixture and raw snapshot helper; record each failing at 0bd0e5fb
in evidence.md before the fix. Re-run every disposal, seal, rebind, early-end,
takeover and compactor fold suite. Update tests that assert `seal_ready`, the
marker hooks, `ClearLedgerRows` or `RestoreLedgerArchive` directly to assert the
phase and the post-stamp snapshot instead. Extend the legacy-artifact tooling
with a completed unexpanded baseline artifact for (e); both directions opt-in.

Commit sequence, each building and passing alone: (1) phase byte, terminal
transition and phase-returning reads, engine tests; (2) single stamp batch and
removal of the post-stamp block, engine crash cuts and (c); (3) `BeginPass`,
engine tests; (4) syncer classifier and removals, sync suites, (a) and (b);
(5) cross-version tooling and (e); (6) evidence and doc updates. Storage commits
land before any syncer change so the syncer never targets two contracts.

Not changed: page commit and pending-work ID allocation, the scheduler, the
takeover token format, report content, retained-mode scrubbing, residue purge,
old-host readability of default-mode sealed files, and the SQLite store and
token path (no file under `pkg/dotc1z/*.go` except `pebble_store.go`, no hunk in
`pkg/sync` outside `ledger_*.go` or an `s.ledgered` fork). Test doubles in
`pkg/sync` that implement `PageLedgerStore` gain the new methods.

### CO-036 implementation

Separate connector observations from page effects. Fold each handler attempt's connector/session/wait counters into the existing retry accumulator, then include that aggregate in the successful page bucket and publish the same aggregate to live stats after commit. Keep only aggregate maps, never callbacks or responses per retry. Clear on commit or identity change. Record wall-wait intervals when observed, rather than reconstructing their timestamps at commit. Preserve page-only callbacks for records and ingest effects. Add the independent three-attempt reproducer, strengthen exact field and next-page assertions, demonstrate failure before correction, then run sync and race checks. Commit correction and evidence separately from this brief.
