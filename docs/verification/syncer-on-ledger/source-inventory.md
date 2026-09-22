# Targeted source audit at a3863c3d

Baseline: eb63f1b5. This inventory records the inspected integration boundaries;
it is not a complete branch-to-cell manifest or an independent review. The
changed-file statement inventory remains executed-coverage-d891604d.json.

| Boundary | Change inspected | Criteria | Evidence and limits |
| --- | --- | --- | --- |
| Attachment: syncer.setStore, loadStore, NewSyncer, store_caps | One ledger boolean set from engine/capability agreement; errors retained across SyncOpts; PageLedgerStore and WriteHookStore assertions occur in store_caps | C01–C03, C44 | Engine/capability matrix, file-path attachment and guard tests pass. No ledger-selection option exists. Metadata's older recordStats check remains separate. |
| Scheduler: parallel_syncer, initial_actions, ledger_scheduler | Existing phase switch and worker scheduler invoke page execution; Init action construction is shared; ledger warning/commit handling and stop accounting fork on the ledger boolean | C05, C13–C15, C20, C40, C45 | ExistingScheduler and initial-action baseline tests, public process cases and full sync suite pass. CO-011 permits these shared changes. This audit does not replace every scheduling product. |
| Token and run state | token.go and run_stats.go have no diff. Checkpoint has an inserted ledger fork. runState retains its mutex, stack and facts; transitionActionWithCompletion lets replay suppress completion increments, while old callers pass true | C03, C20, C24, C45 | Token corpus, restoration tests and public takeover totals pass. This is a reviewed shared change, not a claim that all SQLite-executed lines are literally unchanged. |
| Shared observations: ingest_filter and recordRunStepDuration | Filtering accepts a destination accumulator; existing callers use the original accumulator, ledger callers use page-local observations. Duration recording still updates original stats and additionally updates ledger observations when enabled | C17–C23, C45, C50 | Filter, retry/session and failed-page tests pass. Helpers retain read-only type checks; no new SQLite write mechanism is introduced. |
| Handler entry points in syncer | Ledger forks precede collection handlers; expansion is excluded by CO-021 | C04–C09, C36–C42 | Handler-specific tests plus public resource crash tests exist. Other handler families are not covered by the new public process fixture. |
| Lifecycle: ledger_lifecycle, ledger_takeover, ledger_restore, ledger_sync, ledger_seal | Resume uses committed rows/frontier and facts; terminal proof precedes sealing; finished continuation preserves binding lifecycle and accepts requested processing options | C10–C12, C24–C36 | Public crash/takeover tests cover selected process cuts. Internal stamp-only and physical-loss consumer products remain incomplete. |
| Report: ledger_report, ledger_report_options and storage report/archive files | Archive precedes default disposal; debug retains history; requested options stay immutable and effective debug policy is stored separately | C31, C35, C44, C50 | Public report policy, archive crash, debug reference and option tests pass. Reference checking uses key-only target lookups. |
| Hooks and auxiliary writes | New sync hooks live on syncTestHooks. Registered production page bypasses are session mutations and clearing prior ingest verification | C37, C38, C44 | Strict page hook, mutation-surface recorder and session failure/reopen tests pass. Process death immediately around each session method and verification-clear boundary remains incomplete. |
| Artifact preservation | artifact_retention.go changes only its package lint suppression | C03, C45 | No preservation decision or error classification changes in this file. |

No production file changed in the two public process-test increments after
0618510e. Searching the production ledger files finds no capability assertions
outside store_caps; references to the resolved capability are operations or
missing-capability errors, not an alternate engine-selection path. The debug and
retention options affect report policy, not whether Pebble uses the ledger.

Remaining structural work: a complete changed-branch-to-criterion inventory,
qualification of that inventory against planted omissions, and final independent
audit/soak. C44/C45/C48 remain evidence incomplete.

## PR-size audit at 0d87cd4c

The baseline-to-PR production additions are 6,981 lines in 61 Go files:

| Area | Added lines | Disposition |
| --- | ---: | --- |
| Generated protobufs | 1,064 | Generated from the 23 added proto lines; retained with the schema |
| Sync handlers and planning | 2,405 | Live PageWriter handlers plus shared initial planning; their token counterparts remain for SQLite |
| Sync persistence, lifecycle and integration | 1,687 | Atomic page publication, resume, takeover, accounting and existing-scheduler integration |
| Storage report and archive | 1,133 | Report projection, debug reference checks and state needed after default disposal |
| Storage page/support | 692 | Atomic assets, deletion/expanded-grant support, capability methods and seal measurement |

Inspected ledger_scheduler, ledger_page, ledger_sync, ledger_walk, lifecycle and
handler entry points. Production ledger code calls the existing parallelSync;
it creates no worker goroutines. The channel in ledger_claim serializes duplicate
page identities and does not dispatch work. ledger_schedule contains the live
run-counter flush guard, not another scheduler. The retained test adapter also
calls parallelSync; it is used by correctness/crash fixtures.

Removed the obsolete TestLedgerCostRuntime synthetic measurement path and its
alternate runner mode. The public benchmark retains its commit observer directly;
the unused sealing flag and intermediate measurement-store wrapper are removed.
Historical driver source/results are available in the pinned evidence artifact.
No correctness fixture or production behavior is removed by this cleanup.

The handler copies are the largest handwritten category. Their writes and
post-commit publication differ from the token handlers; deleting them now would
require another persistence refactor or removal of SQLite writes. The latter is
CXE-1311. Existing shared planning/filter helpers remain shared under CO-011.
This audit did not find another independent executor or an unused production
integration path to delete; it does not establish that every remaining line is
minimal. The remaining structural/coverage obligations above are still open.

CO-021 supersedes this audit's expansion-page entries. The expansion iterator,
PageWriter expansion method and dedicated expanded-record staging are removed.
Expansion calls the original handler/store adapter directly. Only terminal
completion accounting and optional preserved-graph recovery interact with ledger
lifecycle state; collection write atomicity remains unchanged.

CO-023 restores external import/matching to main's direct store batches and removes
the staged grant merge and staged resource/entitlement deletion APIs. CO-022 moves
default archival/disposal into seal, eliminating the scrub-then-drop sequence.
The matching unfinished archive remains recovery authority after row deletion;
retained/debug ledgers still use the existing scrub policy.


## Final recovery boundary audit after 38b87b01

This is a direct source inspection and targeted execution, not an independent
review by another reviewer or closure of every coverage product.

| Boundary | Current behavior and test evidence |
| --- | --- |
| Page publication | `invokeActionPage` stages the transition and observations; `runPageWithCommit` publishes only after the batch succeeds. Handler failures discard; storage errors cannot become connector NotFound warnings. ExistingScheduler commit-failure, warning, independent-error and fact-publication tests exercise these distinctions. |
| Resume walk | Public family crash fixtures bracket the walk with the write recorder and exact raw snapshots. The walk returns pending work to the existing scheduler; it has no replacement worker pool. Recorded completion is not incremented again. |
| Fresh sync without surviving Init | The known-quality fact is part of Init. If all initial pages disappear, restore uses the same UnknownPriorCheckpoint fallback as the token path. This changes the sealed quality flags relative to uninterrupted execution; C16 records the unresolved equality claim. |
| Finished continuation | `startOrResumeSync` is unchanged from baseline. `prepareLedgerState` restores archived facts/accounting and clears rows only for finished processing without pending legacy work. The timestamp-based engine BoundSyncFinished API is unchanged. FinishedProcessingResumesWithoutReset, FinishedLegacyFrontierKeepsPendingWork and PublicFinishedContinuationAfterDisposal cover these branches. |
| ClearRows | Synced row/frontier/selected-fact deletion preserves records, counters and sync metadata. ClearRowsFailureCuts and ClearRowsCrashImages cover stamped/staged/committed boundaries; the latter uses durable-only VFS images. Physical purge is deferred to seal, with the cleanup marker retained across those images. |
| Archive and disposal | The archive is saved before history deletion. A token-free pending declaration survives through finalization, including retries over an older finished timestamp. DiscardDurableSealCuts covers eight cuts for fresh/finished bindings; batch failure and purge/stamp/marker retry tests cover error returns. |
| Archive failure | Sealing retains and scrubs history. Successful finalization still removes the disposal declaration; the fallback test now asserts this so later requested processing is not mistaken for a pending seal. |
| Stats overlay | FailedSealDropsItsStatsOverlay verifies that the next seal uses its own stats. Terminal consumer tests separately assert folded ledger facts/counters reach EndSyncWithStats. |
| Shared scheduler | The existing phase switch, queue, retry policy and worker dispatch remain. Ledger context and invocation wrapping are conditional; the token arm calls the original handler. InitialActionsBaseline and ExistingScheduler tests cover ordering, spawned completion, duplicate claims, warnings and independently failing workers. |
| Expansion/external processing | Both call main's ordinary handlers outside page transactions. Expansion's timing wrapper records elapsed time; it does not replace graph construction, write interfaces or retry behavior. External import retains its separately documented upstream replay limitation. |

The final audit runs the complete cut-enumeration fixture and the six fixed-seed
scheduler soaks explicitly; those tests are opt-in and a normal package-test pass
does not establish that they ran. Per-run results are recorded in evidence.md.
