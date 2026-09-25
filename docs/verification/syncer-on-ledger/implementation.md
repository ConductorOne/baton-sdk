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
