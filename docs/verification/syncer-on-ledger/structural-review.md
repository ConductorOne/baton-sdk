# Structural review

Branch `matt.kaniaris/CXE-1358/syncer-ledger-plan`, head `5dd28b62`, base `main` at `eb63f1b5`.
RULE 1: landed CO-037, CO-038, CO-039; pending none.

## Part One — reconstruction

Written from the production diff and the files it touches. Doc comments were read as claims, not as authority.

### States

Durable state is one bound sync in the Pebble file (keys carry no sync id). Read the pass with `Engine.LedgerState` (`adapter.go:1275`): phase from the work-state key, `Finished` from `ended_at`, `Token` from `sync_token`.

| State | How to read it |
| --- | --- |
| Unstarted | Phase absent (no work-state key, `pending_work.go:41`), `ended_at` nil, `sync_token` empty, and `BoundSyncUnstarted` (`ledger_empty.go:12`) true: no archive, no keys in the record/index/digest spans it scans. |
| Legacy checkpoint | Phase absent and `sync_token` nonempty. A frontier key (`LedgerFrontierKey`, kind `0x03`) means a takeover already copied that token. |
| Collecting | Work-state value: version `2`, last allocated id, phase byte `1` (`pending_work.go:35-57`). Pending entries are kind `0x04`. |
| Expanding | Same key, phase byte `3`. |
| Sealing | Phase byte `2`. The terminal page (`op` `sync-terminal-v1`, `ledger_seal.go:10`) is what moved it there, and that page is refused unless the pending range is empty (`page_unit.go:90-96`). |
| Sealed | `ended_at` set. `StageLedgerSeal` deletes the work-state key (`records.go:706`), so a later `workState` reads absent. Engine-meta `ledger-archive` holds facts, counters, and a report (`ledger_archive.go:18-34`). The in-flight keyspace stamp is put back to version 2 before `ended_at` (`keyspace_version.go:30-34`, `adapter.go:489-492`). |
| Follow-on collecting | A sealed sync with phase absent and no token, after `BeginPass`: collecting declaration, fact `c1z.pass.follow_on`, seeds (`ledger.go:715-818`). |

Disposal is a fact, not a phase. `c1z.discard_ledger_on_seal` present: seal drops rows, pending, scheduling, frontier, then facts and counters. Absent: those stay; only the declaration is deleted (`records.go:664-709`). `c1z.retain_tokens` present, or `Ledger.retainTokens`: seal does not scrub page tokens (`ledger.go:143-156`).

In memory, for one process attempt: `syncer.ledgered` is set in `setStore` when the engine is Pebble and the store implements `PageLedgerStore` (`syncer.go:3871-3885`). `ledgerRuntime` holds the attempt id (`rand.Text`, not a lifecycle marker) and `workers[i]`, the cumulative counter total blind-written for `(runID, i)` (`ledger_page.go:18-37`). `runState` is a projection of at most 100 pending entries (`maxPeekActionsCount`, `run_state.go:298`), plus facts and completion counts copied out of the store (`ledger_restore.go:12-96`). When `ledgered`, `Checkpoint` writes nothing (`syncer.go:479-481`). `runStats` keeps restored totals and a separate attempt-only map that feeds the run bucket (`run_stats.go:66-71`). The expansion graph is process memory, written to a sidecar only when `preserveEntitlementGraph` (`ledger_sync.go:67-84`).

### Transitions

Each named function is one synced batch, except seal.

- Absent + token → Collecting: `BeginCollectingFromToken` → `Ledger.takeover` (`ledger.go:573`). One batch: clear `sync_token`, write frontier, seed pending as Collecting, facts, takeover counter bucket `0xFFFFFFFE`, stamp in-flight.
- Absent + unfinished + no token → Collecting: `BeginCollecting` (`pending_work.go:80`). Refuses `ended_at`, a leftover token, or existing rows. Already-initialized phase is a no-op (`:102`).
- Absent + finished → Collecting: `BeginPass` (`ledger.go:715`). Holds `lifecycleMu`. Wipes rows, scheduling, frontier; restores archive facts minus `clearFacts`; restores archived counters only when the family has no bucket.
- Collecting → Expanding: `BeginExpanding` (`pending_work.go:143`). Phase byte only. Requires Collecting and exactly one pending entry whose op is `grant-expansion`. Caller is `parallelSync` when expansion is not skipped (`parallel_syncer.go:409-417`).
- A collecting page: `ledgerRuntime.runPageWithCommit` (`ledger_page.go:88`) then `pageUnit.Commit` (`page_unit.go:446`). The syncer unit is one `transition` and one `Commit`: records, the row, facts, the worker's whole counter bucket, and the pending update (delete, or revision+1 and children).
- A local step (grant expansion, external resources): `CompletePendingWork` (`pending_work.go:396`). Deletes the entry and writes the run bucket `0xFFFFFFFF`. No page row. `runPendingLocalStep` (`ledger_pending.go:168`).
- Collecting or Expanding → Sealing: `prepareSeal` (`ledger_seal.go:12`) via `SetTerminal`. If already Sealing, only `PutCounterBucket` of this attempt's run counters.
- Sealing → Sealed: `Seal` → `EndSyncWithStats` (`pebble_store.go:494`, `adapter.go:278`). Not one batch: index build, source-cache counts, archive, optional discard, optional scrub, optional compaction, clear the in-flight stamp, then one `StageLedgerSeal` batch (archive, drop the declaration, drop facts and counters when discarding, set `ended_at`).

`prepareLedgerState` (`ledger_lifecycle.go:40`) picks among continue, finish-seal, seed, and follow-on from phase, and from `ended_at` only when phase is absent (`:23-36`).

### Owners

- Ledger family and the sync-run record: engine `writeMu`. Takeover, `BeginPass`, and `endSync` also hold `lifecycleMu` (`engine.go:59-77`).
- `workers[i]`: the goroutine whose context carries `ledgerWorkerKey` `i` (`parallel_syncer.go:880`). The coordinator uses index 0 when the key is absent (`ledger_scheduler.go:95`, zero value). `syncParallel` waits before returning (`:920`), so a coordinator page and a worker page on index 0 do not overlap in that loop. No lock; the comment at `ledger_page.go:18-23` is the claim.
- `run.actions`: coordinator (`refreshPendingWindow`) and a worker (`pendingRefill`), both under `run.mu`. After commit, `publishPendingTransition` updates the same map. Pending keys are the queue. For `grant-expansion` and `list-external-resources`, a matching revision copies `PageToken` off the previous in-memory action (`ledger_pending.go:63-66`).
- `run.facts`: copied from `LedgerFacts` at restore; page commit copies `page.facts` in (`ledger_scheduler.go:172-174`). `hasFact` reads the copy.
- Attempt run-bucket: coordinator, via `flushRunCounters`, `prepareSeal`, and `CompletePendingWork`. Page observations go into the worker bucket inside the page batch.
- `Ledger.retainTokens`: set from config at the start of `syncLedger` (`ledger_sync.go:26`); every page commit re-stages `c1z.retain_tokens` if the bool is set (`page_unit.go:564`). Seal reads the bool or the fact.
- Session writes during a page bypass the page gate (`ledger_session.go:12-14`).

### Invariants the code checks

- A page calls `transition` once (`ledger_page.go:48-52`, `:114`) and `Commit` once (`:125-146`).
- `stagePhaseLocked` refuses pages in Sealing, and refuses a non-terminal page in Expanding (`page_unit.go:66-78`). `stageWorkTransition` repeats the phase check (`pending_work.go:313-324`).
- Terminal page: declaration exists, queue empty, no continuation, children, or work (`page_unit.go:81-96`).
- `BeginExpanding`: Collecting, exactly one `grant-expansion` entry (`pending_work.go:155-163`).
- Pending commit matches sync id, work id, revision, and identity, or `ErrStaleLedgerWork` (`pending_work.go:275`).
- A legacy token and a live declaration are an error; a token and a frontier are an error; a Sealing declaration with pending entries is an error (`ledger_takeover.go:45-59`).
- Expansion flags are checked against phase before any write in `prepareLedgerState` (`ledger_lifecycle.go:54`, `:91`).
- A key whose stored identity does not match the lookup reads as absent (`ledger.go:86-100`, `:474`).
- A page begun under a different current sync is refused (`page_unit.go:609`).
- Worker index must be inside the attempt's slots and below `0xFFFFFFFE` (`ledger_page.go:95-102`).

### Decisions, and how many places use the same fact

1. `syncer.ledgered` — token checkpoint versus no-op, `Sync` versus `syncLedger`, page invocation versus the handler, local-step completion, stop accounting, session wrapper, `BeginExpanding` versus `MarkSyncSupportsDiff`, parallel refill. Set once in `setStore`. Branched in `syncer.go` (Checkpoint, Sync, nextPage, transition, each `Sync*` method, loadStore), `parallel_syncer.go` (eight sites), `initial_actions.go`, `ingest_invariants.go`, `ledger_scheduler.go`, `ledger_pending.go`.
2. Work-state phase — `prepareLedgerState`, `expansionFlagConflict`, `parallelSync` (resume and whether to call `BeginExpanding`), `prepareSeal`, `seal`, `stagePhaseLocked`, `stageWorkTransition`, `endSync` (`adapter.go:301-308`). Eight functions.
3. `ended_at` — `LedgerState.Finished`, `expansionFlagConflict`, `BeginCollecting` refuses it, `BeginPass` requires it, `endSync` allows absent+finished as a reseal, `BoundSyncUnstarted` treats it as started. Six.
4. `dontExpandGrants` / `onlyExpandGrants`, read with phase and `factNeedsExpansion` — `expansionFlagConflict`, `parallelSync` skip, `syncLedger`'s `finishPreviousRequest`, `initialActions`. Four.
5. Token retention — `cfg.retainLedgerTokens`, `syncer.ledgerDebug`, fact `c1z.retain_tokens`, `Ledger.retainTokens`. Seal scrubs from the bool or the fact; `prepareSeal` writes `c1z.discard_ledger_on_seal` from `!ledgerDebug` (`ledger_sync.go:96-98`). Four representations. Discard itself has one writer (`prepareSeal`) and one reader (`sealDiscardsRows`).
6. Ingest provenance — `newSync`, else `BoundSyncUnstarted`, else a nil quality becomes blocked (`ledger_restore.go:80-85`). The Init page also writes `c1z` ingest-known when quality is non-nil (`initial_actions.go:58-61`). Three.

### Not recoverable

1. Whether `syncOneAction`'s warning path (`parallel_syncer.go:937-940`) and the warning branch inside `invokeActionPage` (`ledger_scheduler.go:126-138`) are one completion or two. Both run on a warning. I cannot tell from the code which write is the completion. Would need to be told which one is the transition.
2. What of the phase is still true after seal, once the declaration is gone. The archive keeps facts and counters (`ledger_archive.go:18-29`); it does not store the phase byte. A retained seal also leaves pending keys with no work-state key (`records.go:689-709` deletes the declaration only). I cannot tell whether those leftover pending keys are still a queue. Would need to be told whether phase-absent plus leftover pending keys is sealed or corrupt.
3. Who may call `PutLedgerFacts` and `PutCounterBucket` outside a page, relative to the phase machine. Both exist on `LedgerAccounting` and do not read the phase (`ledger.go:653-704`, `c1zstore/ledger.go:298-308`). `prepareSeal` and `flushRunCounters` use the counter write; `putLedgerReportOptions` uses the fact write (`ledger_report_options.go:53`). I cannot tell whether those writes are part of a state or a side channel the machine does not own.

Four items. Closure note from the prompt: more than three "not recoverable" items is not closable by fixing rows.


## Part Two — intended model

The brief has a state table: plan CO-039 §1 (`docs/verification/syncer-on-ledger/plan.md`).

Read in the required order.

`AGENTS.md` "Surface the decisions review cannot see" and "Verified or assumed": a lifecycle decision is a claim written before the code; the implementation brief holds the ownership table and the state inventory; a claim about code is cited from a read in this session or marked assumed. `AGENTS.md:13-58`.

State table, plan CO-039 §1 (`plan.md:1334-1341`). Durable states, read from the declaration and `ended_at`:

| State | Declaration | `ended_at` | Store accepts |
| --- | --- | --- | --- |
| Unstarted | absent | unset | `BeginCollecting`, `BeginCollectingFromToken` |
| Collecting | collecting | any | page commits; `CompletePendingWork`; `BeginExpanding`; terminal page |
| Expanding | expanding | any | `CompletePendingWork` of the expansion entry; `PutCounterBucket`; terminal page. No page commits |
| Sealing | sealing | any | `PutCounterBucket`, `PutLedgerFacts`, `EndSyncWithStats` |
| Sealed | absent | set | `BeginPass`; `EndSyncWithStats` as an engine-level reseal |

Collecting and Expanding with an empty queue are drained. The syncer reads the queue; the store does not encode drained as a phase.

Transitions, CO-039 §2 (`plan.md:1342-1349`): each one synced batch, validated under `writeMu`. Unstarted → Collecting by seed or takeover. Collecting → Expanding by `BeginExpanding` when the expansion action is first picked up. Collecting or Expanding → Sealing by the terminal page. Sealing → Sealed by the stamp batch (CO-037). Sealed → Collecting by `BeginPass`, which stages `c1z.pass.follow_on`. Skipped expansion never enters Expanding: the expansion entry completes through `CompletePendingWork` in Collecting.

CO-039 §5 (`plan.md:1352`): options facts are written only when the attempt enters at collecting or expanding. An attempt that enters at sealing writes its run bucket and nothing else. No transition, no write to the fact family.

CO-039 §7 (`plan.md:1354`): expansion flags are refused against the phase before any write. `onlyExpandGrants` conflicts with Collecting that still has a collection entry, with Collecting on an unfinished sync whose queue is the Init seed, and with Unstarted under a caller-supplied sync ID and no legacy token. `dontExpandGrants` conflicts with Expanding.

CO-037 §1 and §4 (`plan.md:1305-1308`): the declaration's phase is the one record of pass state. Absent plus `ended_at` is sealed. A retained seal keeps scrubbed rows, facts, and counters, and no declaration. A default-mode seal leaves an empty ledger family.

Ownership table, `implementation.md:181-198`. Coordinator owns index 0 between batches; worker i owns index i during a batch; batches do not overlap. Attempt option snapshot and the verification-marker clear belong to the coordinator. Known facts are shared under `run.mu`. Attempt accounting is shared under `stats.mu`.

State inventory, `implementation.md:205-230`. One row per durable question. The declaration answers whether a pass is open and which phase it is in. `supports_diff` is named as marking the same collection-complete event for `rollback-expansion`, with the token path still needing it. `c1z.retain_tokens` is written by page commit and takeover and read at finalize.

CO-039 implementation (`implementation.md:257-270`): `putLedgerReportOptions` runs only when the resume phase is collecting or expanding. The `finishPreviousRequest` gate stays. A fresh expansion pickup stamps `supports_diff`, then calls `BeginExpanding`.

CO-038 (`plan.md:1372-1379`, superseded in part by CO-039): the option snapshot is one coordinator write. CO-039 supersedes "an attempt that commits no page still records its options" and the note that the sealing attempt's retention flag decides disposal. The terminal page's facts decide; the sealing attempt writes none (`plan.md:1358`).

Requester brief §4 (`docs/tasks/syncer-on-ledger-brief.md:170-188`): one boolean set in `setStore` is the only path predicate. Where the ledger path meets existing code, the existing code gets one `if s.ledgered` fork and the body below is untouched. Share only helpers that never touch the store.

Registries. `pkg/sync/sync_primitives_meta_test.go:40-66`: no entry is marked "predates". `pkg/dotc1z/engine/pebble/sync_primitives_meta_test.go:23-51`: every listed engine primitive is marked "predates". `testSeams.sealCost` (`test_seams.go`, added field) is not in that map. Not checked: whether the walker visits it.

## Part Three — the diff

Six findings. Not closed.

1. `PutLedgerFacts` and `PutCounterBucket` commit under `writeMu` and do not read the phase (`pkg/dotc1z/engine/pebble/ledger.go:653`, `:674`). / CO-039 §1 lists `PutLedgerFacts` for Sealing only, and `PutCounterBucket` for Expanding and Sealing (`plan.md:1339-1340`). The verification column treats an unlisted event as a refusal (`plan.md:1360`). / bug class: a fact or a counter bucket can land in Unstarted or Collecting, which one phase check on those two methods would reject. / `ledger.go:653`, `ledger.go:674`; syncer callers `ledger_report_options.go:53`, `ledger_schedule.go:11`, `ledger_seal.go:26`. / Refuse each unless the phase is one the table lists for that method.

2. `planRootEntitlementActions` and `planRootGrantActions` call `store.ListResources` and are used by the token handlers and the ledger collectors (`root_planning.go:32`, `syncer.go:2083`, `syncer.go:2610`, `ledger_entitlements.go:33`, `ledger_grants.go:38`). `filterGrantExpansionTypes` and `filterFreshGrants` were edited in place so both paths pass a stats pointer (`ingest_filter.go:233`, `:288`). / Requester brief §4: share only helpers that never touch the store; CO-037: a `pkg/sync` hunk outside `ledger_*.go` is an `s.ledgered` fork (`plan.md:1325`, `syncer-on-ledger-brief.md:177-184`). / change tax: planning and grant filtering now have one body for both engines. A ledger-only change edits the SQLite path, and CXE-1311 cannot delete a ledger copy that was never separate. / `root_planning.go:11`, `root_planning.go:61`, `ingest_filter.go:233-365`. / Put the store-reading planners back behind the ledger collectors and leave the token handlers on their own copies.

3. `syncLedger` writes options when the phase `prepareLedgerState` returned is not Sealing, and skips them when `finishPreviousRequest` is set (`ledger_sync.go:36-41`). That returned phase is the phase before `BeginCollecting` or `BeginPass` (`ledger_lifecycle.go:39`, `:86`), so a fresh sync and a follow-on pass return Absent and still write options. / CO-039 §5 and the implementation section say the write happens only when the resume phase is collecting or expanding (`plan.md:1352`, `implementation.md:267-269`). The implementation also says the `finishPreviousRequest` gate stays, which skips the write on a drained collecting pass that will still commit the terminal page. / change tax: the next edit that matches the brief's phase list ("only collecting or expanding") drops the snapshot on every new pass, because the value in `phase` is still Absent. The gate and the phase list cannot both be followed without this being written down. / `ledger_sync.go:31-41`, `ledger_lifecycle.go:39`, `ledger_lifecycle.go:58-86`. / Return the phase after the seed or `BeginPass`, and name the drained-collecting skip in the same sentence as §5.

4. On an absent declaration, `onlyExpandGrants` is refused when `newSync` is false and `ended_at` is unset (`ledger_lifecycle.go:124-127`). `newSync` is false when the caller passed a sync ID (`syncer.go:850-855`) and when the store resumed the latest unfinished sync with an empty ID (`syncer.go:862`, `adapter.go:177-183`). / CO-039 §7 names that refusal for Unstarted under a caller-supplied sync ID and no legacy token (`plan.md:1354`). / change tax: the check remembers `newSync`, not the caller's ID. Tightening it to the caller's ID changes the latest-unfinished resume; leaving it refuses expansion-only on a sync that died before `BeginCollecting` and was not opened by `WithSyncID`. / `ledger_lifecycle.go:124-127`, `syncer.go:843-856`. / Decide the refusal on the fact §7 names, and record the latest-unfinished cell if it is the same rule.

5. A retained seal deletes the work-state key and keeps pending keys, scheduling keys, and the frontier. `StageLedgerDisposeTokens` runs only when the discard fact is set (`adapter.go:423-437`, `records.go:664-679`). `StageLedgerSeal` with `retained` true deletes facts and counters only in the discard branch and always deletes the declaration (`records.go:692-709`). `PendingWork` returns no entries when the phase is absent, without reading those keys (`pending_work.go:201-203`). / CO-037 §4: a retained seal keeps scrubbed rows, facts, and counters, and no declaration (`plan.md:1308`). CO-039 Sealed is absent plus `ended_at` (`plan.md:1340`). The inventory's question for pending keys is "what work remains" (`implementation.md:214`). / read cost: a reader of a retained sealed file must be told that kinds `0x04`, `0x06`, and `0x03` can still hold bytes and are not a pass. They need it at the first scan of those prefixes. `BeginPass` deletes them in the seed batch (`ledger.go:801`), which is the other place that has to stay true. / `adapter.go:450-463`, `records.go:692-709`, `pending_work.go:201-203`. / Delete pending, scheduling, and frontier in the retained stamp batch, with the declaration.

6. `LedgerLifecycle`'s methods, read without their comments, are `State`, `BeginCollecting`, `BeginCollectingFromToken`, `BeginExpanding`, `Seal`, `BeginPass` (`c1zstore/ledger.go:258-277`). The names yield Unstarted → Collecting → Expanding, then `Seal`, then `BeginPass` back to Collecting. They do not yield Sealing. The edge into Sealing is `PageWriter.SetTerminal` (`c1zstore/ledger.go:231`), and `endSync` accepts that call only from Sealing or from absent plus `ended_at` (`adapter.go:301-308`). / CO-039 §1–§2: Sealing is a state, entered by the terminal page from Collecting or Expanding, and `EndSyncWithStats` runs from Sealing (`plan.md:1339-1347`). / read cost: a reader of `LedgerLifecycle` must be told that `Seal` is refused unless a page has already moved the declaration to Sealing. They need it before calling `Seal` from Expanding. / `c1zstore/ledger.go:248-277`, `page_unit.go:66-97`. / One finding for this interface: the Sealing edge is a method whose name is the phase it enters.

Part One lists three not-recoverable items. The line under them says "Four". The count is three, which is not "more than three". Items 2 and 3 are findings 5 and 1. Item 1 is the warning path, handed to correctness below; it is not a state in the table.

## Part Four — appendix

Verdicts: justified, misplaced, duplicate, unexplained, consolidate, split. Rows are recorded. Findings 2 and 5 cite rows below.

### A. Fields

| ID | Field | Owner, lifetime | Verdict |
| --- | --- | --- | --- |
| A-syncer.ledgered | `syncer.go:150` | `setStore`, process. True iff engine is Pebble and `pageLedger` is non-nil (`syncer.go:3879-3885`) | justified |
| A-syncer.ledgerDebug | `syncer.go:151` | coordinator, attempt. Set from config, from the retain fact, and cleared on the follow-on recursion (`ledger_sync.go:32-35`, `:117`) | duplicate of `cfg.ledgerDebug` plus `c1z.retain_tokens` |
| A-syncer.ledger | `syncer.go:152` | coordinator allocates; workers call `runPage` on it. One attempt | justified |
| A-ledgerRuntime.store | `ledger_page.go:25` | the attempt's `PageLedgerStore` | justified |
| A-ledgerRuntime.runID | `ledger_page.go:26` | attempt id, `rand.Text`, not a lifecycle marker | justified |
| A-ledgerRuntime.workers | `ledger_page.go:27` | holder of index i; attempt; no mutex (`ledger_page.go:18-23`) | justified, matches the ownership table |
| A-runStats.attemptStepDurationsMs | `run_stats.go:69` | attempt, `stats.mu` | justified |
| A-runStats.attemptSessionOps | `run_stats.go:70` | attempt, `stats.mu` | justified |
| A-runStats.attemptCounters | `run_stats.go:71` | attempt, `stats.mu` | justified |
| A-syncConfig.retainLedgerTokens | `config.go` added field | caller option, immutable after `NewSyncer` | justified |
| A-syncConfig.ledgerDebug | `config.go` added field | caller option | justified |
| A-storeCaps.pageLedger | `store_caps.go` | resolved once in `resolveStoreCaps` | justified |
| A-storeCaps.writeHook | `store_caps.go` | resolved once | justified |
| A-Ledger.e | `ledger.go:25` | the engine, process | justified |
| A-Ledger.retainTokens | `ledger.go:26` | process memory, set at attempt start (`ledger_sync.go:26`); seal reads it before the fact (`ledger.go:143-146`) | duplicate of `c1z.retain_tokens`. Appendix only |
| A-Ledger.mismatches | `ledger.go:27` | process counter of identity mismatches | justified |
| A-Ledger.inFlight | `ledger.go:29` | mirrors the keyspace stamp (`keyspace_version.go:77`) | justified |
| A-Engine.ledger | `engine.go:181` | embedded, process | justified |
| A-testSeams.sealCost | `test_seams.go` | tests, one finalize. Ownership table says tests, primitive none | justified |
| A-testSeams.ledgerBeginPassHook | `test_seams.go` | tests | justified |
| A-testSeams.ledgerBeginExpandingHook | `test_seams.go` | tests | justified |
| A-testSeams.ledgerArchiveHook | `test_seams.go` | tests | justified |
| A-syncTestHooks.ledgerCommitted | `hooks.go:22` | tests | justified |
| A-syncTestHooks.ledgerStop | `hooks.go:23` | tests | justified |
| A-syncTestHooks.ledgerWalk | `hooks.go:24` | tests | justified |
| A-syncTestHooks.ledgerHandler | `hooks.go:25` | tests | justified |
| A-ledgerPage | `ledger_page.go:40-46` | one page, one goroutine. `transitions`, `facts`, `observations` | justified |
| A-ledgerInvocation | `ledger_scheduler.go:28-35` | one page call | justified |
| A-ledgerResume.phase | `ledger_takeover.go:27-31` | classification input, the phase before this attempt's writes | justified |
| A-runState | `run_state.go:184-226` | pre-existing fields. When `ledgered`, a projection of at most 100 pending entries (`ledger_restore.go:28`, `run_state.go:298`), rebuilt by `refreshPendingWindow` | duplicate of pending keys, as the ownership table's in-memory stack |
| A-Engine other fields | `engine.go:36-192` except `ledger` | read this session. Predate this branch's syncer work. Owners are the comments on those fields | justified |

`A-Ledger.retainTokens` is a second record for whether seal scrubs tokens. It stays in the appendix. The inventory names the fact as the finalize reader (`implementation.md:221`). The cost of the bool winning is a scrub outcome, which this pass does not analyze.

### B. Primitives the registries do not mark "predates"

Sync registry (`sync_primitives_meta_test.go:40-63`). None of these strings say "predates".

| ID | Declaration | Interleaving |
| --- | --- | --- |
| B-runState.mu | `run_state.go:185` | workers transition and finish; coordinator reads `current` |
| B-runStats.mu | `run_stats.go:55` | workers merge stats; coordinator reads summaries and flushes the attempt maps |
| B-parallelActionQueue.mu | registry sentence | N workers |
| B-parallelActionQueue.cond | registry sentence | workers blocked in `next` |
| B-syncer.syncParallel.resultsMu | function-local | workers append; `syncParallel` reads after `wg.Wait` (`parallel_syncer.go:920`) |
| B-syncer.parallelTransitionMu | `syncer.go:212` | coordinator sets the transitioner; workers read it |
| B-syncer.rlWallMu | `syncer.go:220` | wait observers on workers |
| B-syncer.listResourceActionsCompletedThisRun | `syncer.go:209` | workers increment; coordinator reads the warning ratio |
| B-syncMap.m | registry sentence | workers |
| B-childScheduleSet.mu | registry sentence | token path only, per the registry sentence |
| B-queueAudit.mu | registry sentence | test workers; nil in production |
| B-ingestFilterStats.* | twelve atomic fields in the registry | workers' `afterCommit`; coordinator reads at seal |

Engine registry entries all say "predates" (`sync_primitives_meta_test.go:24-51` in the pebble package). No engine row in this section.

`testSeams.sealCost` is an `atomic.Pointer` and is not in `enginePrimitiveRegistry`. Two goroutines: none. The ownership table assigns it to tests.

### C. Durable keys and facts

| ID | Record | Question | Writer | Readers | Other answer |
| --- | --- | --- | --- | --- | --- |
| C-work-state | ledger kind `0x05` | phase | `BeginCollecting`, `BeginPass`, `BeginExpanding`, terminal page, child allocation; deleted by `StageLedgerSeal` | `State`, resume, flag check, page refusal, `endSync` | none for the phase. `ended_at` answers finished, not phase |
| C-pending | kind `0x04` | what work remains | page commit, seed, takeover, `CompletePendingWork` | `PendingWork` when phase is not absent | after a retained seal, the keys can remain while `PendingWork` returns empty. Finding 5 |
| C-scheduling | kind `0x06` | child already scheduled | page commit, seed | `stageWorkTransition`, invariant I4 | in-memory `childScheduleSet` on the token path |
| C-rows | kind `0x00` | diagnostic history | page commit | report, `GetLedgerRow` | none |
| C-discard-fact | `c1z.discard_ledger_on_seal` | disposal | terminal page (`ledger_seal.go:34-37`) | `sealDiscardsRows` | none |
| C-retain-fact | `c1z.retain_tokens` | scrub | page commit if `retainTokens` is set (`page_unit.go:564`); takeover | `sealScrubsTokens` after the bool (`ledger.go:143`) | `Ledger.retainTokens` |
| C-follow-on | `c1z.pass.follow_on` | pass began on a sealed sync | `BeginPass` (`ledger.go:804`) | archive link. Not fully read: `buildLedgerArchiveLocked` past `ledger_archive.go:120` | not checked past that line |
| C-options | `c1z.report.first_options`, `latest_options` | which options ran | `PutLedgerFacts` from `putLedgerReportOptions` | report. Not checked: the report reader line | finding 3 for when the write runs |
| C-ingest-facts | `sync.ingest_known`, `sync.ingest_blocked` | replay eligibility | seed, Init page, terminal page | restore (`ledger_restore.go:80-85`) | nil quality becomes blocked |
| C-counters | kind `0x02` | cumulative accounting | page bucket, `PutCounterBucket`, fold, `BeginPass` `"archived"` bucket | `LedgerCounters` | `runStats` attempt maps are the in-memory source of the run bucket |
| C-frontier | kind `0x03` | takeover provenance | takeover | `loadLedgerResume` conflicts if a token and a frontier both exist (`ledger_takeover.go:58`) | the token, consumed in that batch |
| C-archive | engine-meta `ledger-archive` | report plus sealed facts and counters | stamp batch; also the discard batch | `BeginPass` | none |
| C-residue | engine-meta `ledger_residue_pending` | compaction owed | drop, discard | finalize | none |
| C-stamp | engine-meta `keyspace_version` value 3 | v2 readers refuse | first ledger write (`ledger.go:411-418`) | open (`keyspace_version.go:77`) | `Ledger.inFlight` |
| C-ended-at | sync-run `ended_at` | finished | stamp batch | `LedgerState.Finished` | the declaration is absent in that same batch |

### D. Two containers

| ID | Site | Verdict |
| --- | --- | --- |
| D-ledger_pending.go:63 | expansion and external-resource `PageToken` copied from the previous in-memory action when the revision matches | duplicate of whatever cursor the handler uses. Handed to correctness; not analyzed |
| D-ledger_scheduler.go:172 | `page.facts` copied into `run.facts` after commit | duplicate of ledger facts. The ownership table names the in-memory side |
| D-ledger_page.go:131 | `workers[i]` updated only after `Commit` returns | justified. The bucket is the durable copy |
| D-ledger.go:143 | `retainTokens` consulted before the fact | duplicate. See A-Ledger.retainTokens |
| D-root_planning.go:11 | one planner writes actions for both paths | split. Finding 2 |

### E. Interface and test-hook methods

| ID | Method | Production caller |
| --- | --- | --- |
| E-LedgerLifecycle.State | `c1zstore/ledger.go:259` | `prepareLedgerState` `ledger_lifecycle.go:46` |
| E-LedgerLifecycle.BeginCollecting | `:262` | `prepareLedgerState` `:78`; `skipLedgerSync` `ledger_sync.go:161` |
| E-LedgerLifecycle.BeginCollectingFromToken | `:265` | `loadLedgerResume` `ledger_takeover.go:85` |
| E-LedgerLifecycle.BeginExpanding | `:267` | `parallelSync` `parallel_syncer.go:415` |
| E-LedgerLifecycle.Seal | `:271` | `ledgerRuntime.seal` `ledger_seal.go:65` |
| E-LedgerLifecycle.BeginPass | `:277` | `prepareLedgerState` `ledger_lifecycle.go:61` |
| E-LedgerQueue.CompletePendingWork | `:291` | `completeLedgerLocalWork` `ledger_run_accounting.go:18` |
| E-LedgerAccounting.PutLedgerFacts | `:308` | `putLedgerReportOptions` `ledger_report_options.go:53` |
| E-LedgerAccounting.PutCounterBucket | `:305` | `flushRunCounters` `ledger_schedule.go:11`; `prepareSeal` `ledger_seal.go:26` |
| E-LedgerAccounting.FoldLedgerCounters | `:303` | `prepareLedgerState` `ledger_lifecycle.go:83` |
| E-LedgerArchive.BoundSyncUnstarted | `:323` | `prepareLedgerState` `ledger_lifecycle.go:69` |
| E-PageWriter.SetTerminal | `:231` | `prepareSeal` `ledger_seal.go:39` |
| E-syncTestHooks.ledgerHandler | `hooks.go:25` | `invokeActionPage` `ledger_scheduler.go:118`, nil in production |
| E-syncTestHooks.ledgerCommitted | `:22` | `ledger_scheduler.go:165`, nil in production |
| E-syncTestHooks.ledgerWalk | `:24` | `ledger_restore.go:13`, nil in production |
| E-syncTestHooks.ledgerStop | `:23` | `ledger_run_accounting.go:40`, nil in production |
| E-testSeams.ledgerBeginExpandingHook | `test_seams.go` | `BeginExpanding` `pending_work.go:173` |
| E-testSeams.ledgerBeginPassHook | `test_seams.go` | `BeginPass` `ledger.go:810` |
| E-testSeams.ledgerArchiveHook | `test_seams.go` | `adapter.go:444` and `:482` |

### F. Hunks outside the allotted files

CO-037 allots `pkg/dotc1z/engine/pebble`, `pebble_store.go`, and `pkg/sync` hunks in `ledger_*.go` or inside `s.ledgered` (`plan.md:1325`).

| ID | File | Verdict |
| --- | --- | --- |
| F-root_planning.go | new file, both paths, calls the store | split. Finding 2 |
| F-ingest_filter.go | token-path filter functions gained a stats parameter | split. Finding 2 |
| F-syncer.go | `s.ledgered` forks plus `planRoot*` calls in the token handlers | the forks are justified; the `planRoot*` calls are finding 2 |
| F-parallel_syncer.go | `s.ledgered` forks | justified |
| F-initial_actions.go | fork inside `initializeAction` | justified |
| F-ingest_invariants.go | `s.ledgered` branch at `:634` | justified. Body of the branch not re-read this session past the grep |
| F-run_state.go | `maxPeekActionsCount` used by the ledger window; field list pre-exists | justified |
| F-run_stats.go | attempt maps added on the shared struct | justified. Token marshal of those fields: not checked |
| F-config.go | two option fields | justified |
| F-hooks.go | four hook fields | justified |
| F-store_caps.go | `pageLedger`, `writeHook` | justified |
| F-queue_audit.go | two audit event constants | justified. Test recorder |
| F-artifact_retention.go | package comment only | justified |
| F-c1zstore/ledger.go | contract | justified |
| F-proto | `LedgerCollectionStats` and row fields | justified |
| F-pb validate | generated | not checked as a hand edit |

### G. Counts against base

Base counts were not enumerated field-by-field. Deltas taken from the diff hunks read this session:

| ID | What | Delta |
| --- | --- | --- |
| G-syncer | fields | +3: `ledgered`, `ledgerDebug`, `ledger` |
| G-ledgerRuntime | fields | new struct, 3 |
| G-runStats | fields | +3 attempt maps |
| G-syncConfig | fields | +2 |
| G-storeCaps | fields | +2 |
| G-test-hooks | `syncTestHooks` + `testSeams` | +4 and +4 (`sealCost` plus three hooks) |
| G-keyspace kinds | | not checked. `keyspace.go` diff is 31 lines; kind bytes `0x00`–`0x06` are in the current file (`keyspace.go:303-311`) and were not diffed line by line |
| G-fact constants | | `LedgerFactDiscardOnSeal` and `LedgerFactFollowOnPass` are added lines in `c1zstore/ledger.go`. `LedgerFactRetainTokens`: not checked against base |
| G-interface methods | current `PageLedgerStore` | 23 methods, counted from `c1zstore/ledger.go:258-330`. Base count not checked |
| G-proto | | `LedgerCollectionStats` added (14 fields). On `LedgerRow`, added lines include `work_id`, `work_revision`, `observations_recorded`, `connector_attempts`, `connector_errors`, `sdk_retry_wait_ms`, `sdk_rate_limit_wait_ms`. Base field count not checked |

### H. Comments and names

Appendix only.

| ID | Site | Verdict |
| --- | --- | --- |
| H-ledger_lifecycle.go:11 | comment names "CO-039 §7" as the owner of the flag rule | unexplained. The check is the code; the comment sends the reader to the plan |
| H-ledger_lifecycle.go:89 | "plan CO-039 §7" on `expansionFlagConflict` | unexplained, same |
| H-ledger_page.go:18 | owner of `workers[i]`, and that batches do not overlap | justified. The type cannot carry the exclusion |
| H-c1zstore/ledger.go:114 | phase comment states what each value accepts | justified, and it is the contract finding 6 says the method names do not carry |
| H-parallel_syncer.go:409 | says the ledger path does not write `supports_diff` | justified as a description of this function. It disagrees with `implementation.md:259-263`, which is the claims list, not a comment defect |
| H-diction | banned words in `ledger_*.go` and pebble `ledger*.go` | none found |

### Claims in the brief's tables the code contradicts

- CO-039 §1 accept column versus `PutLedgerFacts` / `PutCounterBucket` with no phase check (`ledger.go:653`, `:674` versus `plan.md:1338-1340`).
- CO-039 §5 "only when the resume phase is collecting or expanding" versus `ledger_sync.go:38`, which writes unless the pre-transition phase is Sealing (`ledger_lifecycle.go:86`).
- CO-037 §4 retained contents versus pending, scheduling, and frontier keys surviving the retained stamp (`records.go:692-709`, `adapter.go:450`).
- CO-039 §7 "caller-supplied sync ID" versus `newSync` (`ledger_lifecycle.go:126`, `syncer.go:850-862`).
- Requester brief §4 and CO-037's fork rule versus `root_planning.go` and `ingest_filter.go`.
- `implementation.md:259-263` says the expansion pickup stamps `supports_diff` and then calls `BeginExpanding`. `parallel_syncer.go:409-417` calls `BeginExpanding` and does not call `MarkSyncSupportsDiff`.

### Handed to correctness

- `parallel_syncer.go:937` — `syncOneAction` calls `finishActionWithWarning` after `invokeActionPage` has already staged a warning completion.
- `syncer.go:2560` — entitlement-graph pagination calls `run.nextPage` on the in-memory action.
- `parallel_syncer.go:409` — the ledger expansion pickup does not call `MarkSyncSupportsDiff`.

