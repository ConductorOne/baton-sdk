# Syncer on the page ledger: current implementation

This brief describes the current design. The original brief at `644c26cf` and
all subsequent design notes through `0d87cd4c` are preserved in the
[historical brief](https://github.com/ConductorOne/baton-sdk/blob/3b35887e02365b3b3f84ed1a54d2be8b622a0d4f/docs/verification/syncer-on-ledger/implementation.md).
Numbered sections cited by the frozen plan refer to that historical brief.
The frozen [plan](plan.md), including its change orders, is unchanged.
[evidence.md](evidence.md) records criterion status; implementation is not proof.

## Runtime and persistence

| Decision | Implementation | Criteria |
| --- | --- | --- |
| Select persistence once when attaching the store | `setStore` uses engine metadata and capabilities resolved in `store_caps`; Pebble requires the ledger, SQLite rejects it, other engines fail | C01–C03, C44–C45 |
| Keep the existing scheduler | `parallel_syncer` retains operation ordering, workers, retries and warning policy; `ledger_scheduler` wraps a scheduled action's persistence | C05, C13–C15, C20, C23, C39–C40 |
| Commit one page as a unit | `ledger_page` owns a PageWriter, private facts, observations and cumulative worker bucket; exactly one transition is required; other exits discard | C04–C08, C17–C23 |
| Publish only committed progress | `invokeActionPage` commits before publishing the transition, facts and child-scheduling entries; duplicate identity claims serialize | C05, C09, C13–C15, C20 |
| Reconstruct pending actions without writes | `ledger_walk` follows committed next-page and child identities, suppresses repeats and returns missing actions to the existing scheduler | C09–C12, C16, C46 |
| Preserve legacy progress | `ledger_takeover` decodes accepted V0/V1/V2 state before migration; empty token plus frontier reuses that frontier; counters import only when absent | C24–C30 |
| Seal only after terminal proof | An atomic terminal page stores seal-ready facts and final run accounting; durable counters/facts supply EndSyncWithStats | C11, C22, C31–C32 |
| Preserve finished-sync lifecycle | Binding keeps sync identity and completion metadata; requested processing restores archived facts/accounting and clears old page rows without starting a new sync | C33–C36 |
| Save useful stats before disposal | engine seal archives mechanical JSON, discards rows and purges once; the syncer logs the saved report; archival failure retains scrubbed history | C31, C35, C50 |

The syncer's `ledgerRuntime` owns transactions and accounting, not a second work
queue or worker pool. Production entry uses `s.parallelSync`. The test scheduler
adapter also calls that implementation.

Collection handler copies use PageWriter for records, assets and full-identity
deletes. Expansion uses the normal evaluator and store adapter outside pages. Read-through/staged-grant support lives in the storage page
implementation. External import and matching use main's normal batches outside page execution
(CO-023); the known main replay defect is a separate fix. Pure planning/filtering helpers are
shared where allowed by CO-011; the SQLite token writer is retained for CXE-1311.

## Contract decisions

- **Scrubbed unfinished rows:** a durable terminal proof permits finishing seal.
  Without proof, return a diagnostic without walking them as completed pagination.
  CO-004 accepts refusal of this unproven state; no new seal-marker API was needed.
- **Retention:** default completion saves the report/options and drops the ledger.
  WithLedgerDebug retains it and enables indexed reference checks. Tokens are
  scrubbed unless WithRetainLedgerTokens is explicitly enabled with debug.
  Durable declarations carry effective retention across processes.
- **Rollback:** the SQLite CLI stays unchanged. Pebble deferred expansion uses the
  existing syncer entry with WithSyncID and WithOnlyExpandGrants, per CO-006/010.
- **Ingest quality:** behavior-controlling known/blocked values are facts; numeric
  filtering observations are counters. Missing legacy knowledge stays unknown.
- **Worker facts:** each page stages its facts privately and publishes them after
  commit. Shared runState retains its lock and token-path semantics; replay can
  suppress completion increments through the reviewed shared helper.
- **Takeover:** preserve supported action identities, tokens, facts, accounting and
  graph semantics. Do not invent absent history. Discard stale compaction
  provenance; the compactor owns the new artifact's sidecar (CO-007).
- **Durability:** fresh and resumed record/page writes use the engine's existing
  NoSync behavior. Crash comparisons use the pages present in the recovered image,
  not an assumption that every returned commit survived (CO-005/012).
- **Auxiliary writes:** strict test hooks check direct writes during pages and
  prohibit writes during the resume walk. Session mutations and invalidating prior
  verification register explicit reasons; lifecycle writes have separate tests.

## Report and cost

The report groups committed pages/writes by operation and resource type, records
received/excluded counts with known reasons, empty responses, pagination, SDK
retry attempts and observed waits, connector timing and recorded phase elapsed
time. It preserves effective non-secret options. It does not infer missing
upstream data from an empty response or count pages reused during resume.

The default projection scans the ledger with bounded groups/examples and bounded
working state. Debug validation decodes each source row once and uses indexed,
key-only lookups for references; it does not rescan or decode targets per edge.
The compact archive preserves facts/accounting needed after default disposal.

The public benchmark uses deterministic zero-latency connector responses and
compares baseline `eb63f1b5` against fresh/resumed ledger Sync. CO-019 accepts the
current machine. At ten million records, the latest single-worker repeat is
111.5s ledger versus 81.0s token. Earlier four-worker samples were 58.5s versus
55.4s. These are individual samples. Collection performance is accepted under CO-020;
the original full C49 measurement matrix has not been completed. Profiles locate extra work in storage point lookups. A Bloom-filter
experiment reduced the repeated ledger sample to 104.1s but enlarged its final
file from 6.4MB to 19.0MB; it is not adopted. The report itself took about 12ms.

## Verification and remaining work

The [criterion index](evidence.md) retains each test/mutant claim and its limits.
The artifact preserves the original candidate-test mapping, exact execution
history, full cost samples and statement-coverage output. Green component tests
do not close public failure products. C01's 16 attachment cells are verified;
remaining crash products, complete logical/index differentials, structural
coverage, final independent audit/soak and C49 measurements are not all closed.

Keep the full sync, focused ledger race, Pebble, compactor and lint gates from the
plan. New correctness claims need a failing planted defect before closure. The
source inventory records targeted review, not an independent final review.

## PR-size cleanup sequence

1. Preserve the complete verification directory and old benchmark sources in a
   separate evidence branch; verify its remote commit and every original document
   blob before removing local copies. Keep plan.md byte-for-byte unchanged.
2. Replace this historical design journal with the current brief. Retain the
   criterion index and current measurements in the PR; link historical execution
   notes and raw output through a pinned artifact commit.
3. Remove the superseded synthetic benchmark driver. Move its still-used commit
   timing helpers into the public benchmark and remove its unused timing state.
   Make the runner select the public path without an obsolete alternate mode.
4. Audit the production additions for a second scheduler, unused integration code
   and unnecessary copies. Keep behavior-critical handler copies until a separate
   change can remove the token path; do not introduce another persistence dispatch
   abstraction to reduce the line count.
5. Verify fresh/resumed public benchmark output and baseline output with a small
   interleaved run, plus full sync and lint. Check archive hashes, document links,
   Python syntax and final diff totals. Commit evidence relocation separately from
   benchmark-source removal; each source commit must build on its own.

This cleanup does not change the persistence contract or promote any incomplete
criterion. It removes review noise and an obsolete measurement path.

## Expansion correction (CO-021)

First add a regression proving the current wrapper hides layer capabilities and
creates expansion rows. Restore the existing scheduler's direct expansion call
and the unchanged SyncGrantExpansion body. Delete the iterator wrapper, synthetic
expansion cursors and PageWriter expanded-grant extension, together with tests
whose only requirement was that extension. Retain main's bulk-write tests.

Collection rows remain the authority for collected work. Expansion stays pending
in that frontier until terminal completion, so any stop before terminal proof
rebuilds/replays it. No expansion completion counter is persisted on stop; count
its in-memory completion once in the terminal run bucket, relative to restored
prior accounting. Timing still uses run accounting. Preserve optional graph
reconstruction after a terminal-proof restart solely for the graph sidecar.

Replace lifecycle fixtures that fake expansion pages with ordinary pending
collection work, and add actual public expansion-only interruption/reopen cases.
Assert layer Begin/Add/Finish reach the original adapter, no expansion ledger row
exists, collection is not called on replay, final grants equal a clean run and
completed-action accounting does not grow on an interrupted pass. Include skip,
read failure and optional preserved-graph paths. Reject the old wrapper on the
new fixture before deleting it; plant skipped replay and terminal double-count
errors against the final checks. Run sync/expander/storage/compactor suites,
focused ledger race checks and broad lint. Production and test removal land in
one building commit; evidence records limits without inflating coverage claims.

Debug reference checks must distinguish expansion's phase-frontier entry from a
collection-page reference. Add a public archived-report assertion that expansion
produces no missing-child diagnostic; first demonstrate it fails after removing
expansion pages. Exempt only expansion children from row lookup, retaining checks
for collection children and continuations. Run the existing missing-reference and
fan-in tests to guard against accidentally disabling the checker.

## One disposal purge (CO-022)

The terminal page declares discard-on-seal for default completion. During
EndSyncWithStats, archive the current bound run before deleting its ledger,
keep the in-flight version stamp until normal seal completion, then purge the
marked residue once. Debug retention continues to scrub/purge. On archival error,
warn and retain scrubbed rows; post-seal report access may retry archival but
must not trigger another disposal purge. Reuse a matching archive when rows
were already deleted so a seal retry cannot overwrite it with an empty report.

Restore a matching discard archive for an unfinished run when its ledger is
empty. Reject restoration of a different run's archive into an unfinished sync.
Finished continuation still retains its existing archive rules. Clear the discard
declaration when starting requested processing over a finished binding. Keep the
post-seal ArchiveLedgerReport API for report access; it returns the saved report
after default disposal. Add failure cuts through the existing archive test hook.

First demonstrate a counted default-completion fixture sees two purge invocations
on the current path. Then change seal/disposal together with consumer fixtures.
Test retained/debug/token-retention modes, archive-write failure, durable crash
images before/after archive and deletion, purge failure and ended-at failure.
No shortcut may leave credential-bearing SST residue in a finished artifact.

External import is investigated separately: compare repeated main-path execution
against one clean run with fixed source data and matching options. Preserve
matching annotations, import-before-match and replacement-before-delete ordering.
No external runtime change is part of the disposal correction.

## External processing correction (CO-023)

Dispatch SyncExternalResources directly, remove its ledger fork and copied
import/matching/deletion handlers. Remove the staged grant iterator and staged
resource/entitlement deletion extensions that only served this transaction.
Keep original bulk deletion and matching code intact. Generalize terminal local-
phase accounting to external processing and expansion; stop accounting must not
claim either phase completed durably. Debug reference checks exclude both phase
entries. Their work may rerun before terminal completion, as requested; the
pre-existing expansion-annotation replay defect is being fixed independently.

A source-reader probe checks that the first imported grant page is already in the
destination before the second is requested. It must fail on the transaction path.
Migrate parity and chaos tests to direct store writes, remove transaction-only
fixtures, and keep comparisons with main's normal one-pass behavior. Run the sync,
Pebble, compactor and focused race suites plus lint before committing this removal.
Then implement the previously specified single-purge disposal change separately.

The token-free discard declaration stays live through finalization, then is
deleted without another purge. It keeps the token/plain-EndSync gates closed and
marks a pending seal even when the sync record still has a prior finished
processing timestamp. Recovery restores the archive when only this declaration
remains. Unreadable archives cannot remove that live recovery declaration.

## Final recovery audit

Extend public process-crash coverage beyond resources: paginate resource types,
resources, static entitlements, entitlements and grants; terminate before and
after a populated continuation-page commit, with one/four workers and WAL/flushed
images. Compare primary records, secondary indexes and digests against an
uninterrupted public Sync, using deterministic time rather than discarding record
timestamps. Check committed completion/connector accounting at the seal consumer,
and verify that default disposal leaves the saved report and no live ledger.
Qualify the comparison by deleting a recovered grant or index entry in the test
image; the comparison must reject each mutation. This extends C04/C16/C31 evidence;
it does not replace the remaining targeted-resource, asset or takeover products.

Reconcile lifecycle/deletion tests with their current branches, and inspect shared
scheduler/lifecycle diffs against main. Record executed checks and remaining gaps
separately; do not promote an entire product from one passing fixture.

The finished-continuation audit also checks the single-purge requirement across
ClearRows and the following seal. ClearRows keeps its synced deletion and durable
pending-cleanup marker, but defers physical compaction to seal. The old immediate
purge must fail a test that expects no purge before seal and one purge afterward;
the saved checkpoint must contain neither the old nor the new page token. Existing
ClearRows durable crash images must retain the pending marker until a later seal.

## Empty-start quality recovery (CO-024)

Add BoundSyncUnstarted to the ledger capability. Pebble checks the bound sync
record (unfinished, empty checkpoint), absence of an archive, and emptiness of
three key ranges covering primary records/assets, indexes/counters, and
digests/source-cache/ledger state. Session records and format metadata do not
count as collection progress. The query seeks once per range, does not iterate
records, and writes nothing. Missing bindings or read errors cannot establish a
clean start.

When restore has no facts or accounting and the bound sync is unfinished, use
that query to recognize an empty start. Quality starts clean in memory and is
saved with Init as before. Token migration, known/blocked quality and finished
processing retain their existing paths. No checkpoint parsing or schema change
is needed. First restore the strict quality assertion in the public crash test
and observe its failure; verify the storage query independently, then connect
it to restore and run the differential and legacy/finished-state guards.

## Final verification and independent review

Treat missing bound-run records as a normal absence in the empty-state query;
exercise absent/mismatched records with empty/populated files. Verify public Sync
still rejects invalid bindings without collection or key changes, against the
baseline as well as the ledger branch.

Add a test-only constructor for the existing Pebble store adapter over a supplied
engine. External tests can then run the public syncer on CrashableMem without a
production option or replacement store implementation. Capture durable-only images
before/after collection commits, both with an entirely unflushed run and with a
flushed prefix. Include targeted and asset work, then check recovered records,
indexes, digests, stats consumers and report/disposal state against uninterrupted
runs. Ordinary-process cuts remain a separate test axis.

After these checks and evidence reconciliation, request separate final-code reviews
from different model families. Reviewers receive the contract, current change
orders and code, but not each other's findings. Reproduce findings before fixes;
run affected checks again and obtain follow-up review where behavior changes.

## Bounded static materialization (CO-025)

Append a ledger-only materialization operation. The remote static handler records
one child per returned template. Its internal cursor contains a version, a hash
of the full parent identity, the template ordinal, deterministic protobuf bytes,
and a stored-resource cursor. Identity distinguishes identical templates within
and across remote pages. This token is never passed to a connector and remains
under normal token scrubbing/disposal. No template facts or storage fields are added.

Reverse the children for the existing stack and execute materialization serially:
all resource chunks of template zero precede template one, and all children precede
the parent's next remote page. A materialization page reads one resource page,
generates its entitlements using the existing transformation, and atomically
records its next cursor. Its completion counts as an action; only the parent
records remote received/attempt/wait metrics. Add the finite operation to phase
reporting. Reject materialization operations in legacy checkpoint tokens because
this SDK can only persist them through ledger rows.

First plant the staging-bound regression using two resources and a one-resource
reader page. Then test duplicate template identities and overwrite order, cold
resume after a later materialization commit failure, no refetch of committed
remote pages, internal-cursor validation, and existing handler output parity.
Extend public crash images and credential scrub fixtures to the new child operation.
