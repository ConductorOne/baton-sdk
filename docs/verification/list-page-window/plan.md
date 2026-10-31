# List-page-window verification plan

## Contract

An opt-in syncer page window bounds successful resources, entitlements, and grants pages per action. Zero preserves existing behavior and token bytes. Static entitlements and planning actions are excluded. Each spawned cursor and each new walk starts at zero. Token checkpoints and durable ledger pending-work revisions retain the action count across process restarts.

The synchronous report includes operation, resource and parent identities, type-scoped/spawned markers, pages returned, configured window, and vendor termination. At the window a nonempty continuation is cleared and counted as sampled; an empty continuation is reported as vendor-terminated and is not sampled. Earlier termination emits no report. Handlers run outside scheduler and run-state locks and must support concurrent workers.

Sampling counters survive both token-based and ledger-based EndSync and are readable through the additive SyncStats counters map. The CLI and local runner propagate the option. The unreleased annotation and its helpers are removed; compatibility is checked against main.

Call `sync.WithListPageWindow(2, handler)` or explicitly pass the hidden `--list-page-window 2` connector flag. The flag stays hidden until downstream marker checks are complete. `SyncRun.Sampled` is the durable sample identity; `SyncRun.Stats.Counters["list_page_window.sampled"]` and `list_page_window.sampled:<resource_type_id>` expose totals. The result is a sample, not a complete snapshot.

## Frozen coverage criteria

- W1: cross product of serial, parallel, and ledger transitions with top-level resources, parented resources with same-ID/different-type parents, per-resource entitlements, per-resource grants, and type-scoped grants with spawned cursors.
- W2: each cell covers sampled (five vendor pages/window two), terminated at window, terminated before window, and disabled window. Assert call counts, report identity and termination, sampled counters, and stored second-page data.
- W3: static entitlements remain unbounded; siblings have independent windows; rewalking one identity reports again.
- W4: disabled/default action serialization is byte-identical; token checkpoint and cold ledger reopen retain the remaining page window.
- W5: handlers run outside locks; unsuccessful transitions do not invoke handlers.
- W6: token overlay and ledger seal expose counters; CLI-to-local-runner propagation is exercised.
- W7: generated artifacts are fresh; Buf lint, format, and breaking against main pass; scoped tests, race checks, and repository CI pass.

## Risk and cost

Opt-in sampling changes durable completeness reporting and interacts with scheduling and restart, so verification uses the high-risk process. The page-window check adds constant work per successful list page and one action-local uint32; it adds no identity map or growing per-page state. Existing default-path collection and expansion algorithms are unchanged.

## Scope exclusions

No annotation transport, connector-side policy, production default change, or downstream upload/uplift gate is included. Release publication and merge remain owner actions.

## Implementation obligations (contract corrections, same scope)

- The ledger calls the pagination funnel both to stage a page and to commit it. Count only during staging; use page-atomic counters and the existing after-commit callback to avoid double counting and reporting failed writes (W1, W5, W6).
- Root planners share collection operation values. Empty resource-type identities must remain excluded (W2, W3).
- Storage SyncStatsRecord needs its own additive counters map; reader-only generation cannot persist the map. Preserve it through token overlay, ledger overlay, and storage-to-reader conversion. Finished-sync metadata loads the existing cached sidecar without scanning rows (W6).
- Token-mode cumulative counters are separate from existing attempt-only counters, so unrelated default token bytes remain unchanged (W4).
- The supported local storage engine is Pebble. Token-mode matrix fixtures wrap that store with the existing non-file store interface; explicit assertions prove which transition mode actually executes (W1).
- Pending-work refresh retains counts for the same in-process action revision; durable rows preserve the count across process restart (W4).

## Pre-release correction: portable counters and optional stats

- Publish only `list_page_window.` counter keys from the ledger, matching token-mode output; no sampling means an empty published counters map, including window zero. Keep internal ledger completion/ingestion accounting unchanged (W6).
- A corrupt optional stats sidecar must not fail finished-sync identity lookup. Log the failure and return the run with nil Stats; a corrupt-sidecar regression covers both full-sync and any-type lookups (W6).
- Add a filter-removal mutation check to the existing matrix. A separate sampled-run sync-fact follow-up remains deferred; the durable run-record marker and downstream consumption sequence are specified below.

## Owner-feedback correction: durable sample identity and restart bounds

- M1: add an orthogonal sampled boolean to storage and reader sync-run records and the exported SyncRun. Derive it from observed sampled counters at EndSync, never merely from configuring a window, and commit it atomically with ended_at in token and ledger modes.
- M2: sampled runs are not replay sources and cannot be compactor inputs. Keep sync type and unrelated ingestion policy unchanged. Hide the sampling CLI flag until downstream marker checks are complete; external upload/uplift enforcement is a separate follow-up.
- M3: persist each action's successful page count through ledger row and pending-work revisions, including connector-signaled restart preservation. A cold reopen resumes the same remaining window rather than granting another one.
- M4: prove token/ledger positive and vendor-terminated-negative marker cases, storage/reader conversions, replay and compactor refusal, same-batch finish durability, pending-work serialization compatibility, and cold reopen after page one with exactly two pages remaining in a window of three.
- M5: preserve all token golden bytes. Run affected suites, race regressions, additive Buf checks, generation freshness, and repository CI.

These pre-release changes provide durable sample identity and exact resume bounds. A downstream upload/uplift gate remains outside this SDK change.

## Simplicity-review correction

- Keep the sampled verdict only in the authoritative run record and reader projection. Do not add manifest fields or change existing header fast-path selection; every merge opens the source payload before reading its records.
- Use one selected-sync metadata helper at Compact, MergeInto, and openSourceChunk. MergeInto validates its existing base as an input as well as incoming sources. Preserve the raw merge API's metadata-free legacy behavior.
- Do not extend pre-ledger token takeover: real takeover inputs predate page-window counts. Durable ledger row/pending-work persistence is the supported restart path.
