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
| Save useful stats before disposal | `ledger_report` archives and logs mechanical JSON; default completion drops rows after archival; archival failure warns and retains them | C31, C35, C50 |

The syncer's `ledgerRuntime` owns transactions and accounting, not a second work
queue or worker pool. Production entry uses `s.parallelSync`. The test scheduler
adapter also calls that implementation.

Collection handler copies use PageWriter for records, assets and full-identity
deletes. Expansion uses the normal evaluator and store adapter outside pages. Read-through/staged-grant support lives in the storage page
implementation. External import and matching share a page so failure does not
preserve a stale external-source answer. Pure planning/filtering helpers are
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
