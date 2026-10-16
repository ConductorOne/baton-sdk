# Pagination restart evidence

Plan revision: 1 plus corrections CO1, CO2 and CO3. Implementation:
`ec11ddaaf730f0a68c73e5aa23d2f9e269841354`, based on v0.40.0.
The behavioral plan was committed before the new runtime implementation. Existing
SDK code had been inspected during host diagnosis. Test-only additions in
`c1c80cd7` strengthen the independent audit's coverage findings. CO2 and CO3
change runtime code; their sections below record that evidence.

## Implementation-obligation addendum

- M01: A versioned cursor envelope carries the action's restart generation and
  provider token. Decode before the connector call; keep the envelope as the ledger
  request identity. Reject malformed metadata without echoing tokens.
- M02: Restart transitions leave the same action pending with a nonempty encoded
  continuation. An empty provider cursor inside that envelope means restart, not
  completion. The next real provider response controls completion.
- M03: Legacy restart transitions force `Checkpoint` before another provider call.
  Serialize checkpoint writers and timestamps, including concurrent worker restarts.
  Persistence errors retain the provider cause. The host's existing close/upload
  remains the publication fence for a remotely resumed artifact; no new guarantee
  is made for unpublished work lost with a killed host.
- M04: Pebble stages the restart through its existing page writer and pending-work
  CAS. Its failed-commit path must discard the candidate generation and observations.
  Retrying first-page requests under a new encoded generation must bypass previously
  committed page replay while retaining the old records.
- M05: Completed/warning counters change only on actual action completion. Failed
  provider calls remain error observations; restart commits contain no successful
  list response. Collected-row observations can include repeated reads.
- M06: Existing provider tokens and default-disabled calls stay unchanged. Metadata
  decodes even with new restarts disabled. Spawned cursors and internal store pages
  are not restarted because their seed is not the provider's empty initial cursor.
- M07: No iterator, raw store capability, deletion, external session, or new storage
  keyspace is introduced. The existing page commit and legacy checkpoint own writes.
- M08: SDK state conflict remains a host whole-sync reset. Expiry-budget exhaustion
  becomes an ordinary failed action; C1 retains the budget and its existing bounded
  whole-sync attempt fallback. No connector IDs or provider strings enter SDK policy.

## Criterion-to-evidence record

| ID | Status and instantiated coverage | Premise and oracle |
| --- | --- | --- |
| P01 | Verified to stated coverage. `TestPageTokenRestartProviderRPCFamilies`: five provider list operations x legacy/Pebble; policy table covers excluded operations, spawned cursors, empty cursors, canceled context, unrelated provider errors and disabled policies. | Each actual SDK handler receives one failing provider RPC with `provider-next`; its action remains pending at count 1 with an empty provider cursor. Excluded cases use the same tagged provider error as the eligible case, so the provider-only guard cannot mask them. `TestPageTokenRestartIgnoresStoreErrors` returns the same sentinel from GetResource and requires no restart/checkpoint. |
| P02 | Verified to stated coverage. `TestPageTokenRestartBudgetSurvivesResume`: SQLite/Pebble x 1/3 workers, plus cold clients, changed worker count, identical checkpoint copies and a separate-process resumer. | The first attempt reaches exactly three first-page calls and the typed limit error. Each cold resumer must retain the same sync ID, return the limit error, and issue zero new first-page calls. The child validates these assertions before its required exit code 73. |
| P03 | Verified to stated coverage. `TestPageTokenRestartKeepsCompletedWork` and `TestPageTokenRestartBudgetsAreIndependent`, SQLite/Pebble x 1/3 workers. | Read back the original sync ID after close/reopen and compare all four independently named grant IDs, including distinct abandoned-only and replacement memberships. Resource listings run once per type; the healthy group starts once. A later fresh full sync excludes the abandoned-only member. Independent failing groups consume one and two restarts respectively and both complete. |
| P04 | Verified to stated coverage. `TestPageTokenRestartCheckpointFailure` injects legacy write failure on both restart and limit exits. `TestPageTokenRestartLedgerCommitFailure` injects Pebble commit failure. `TestPageTokenRestartCheckpointBeforeRequest` reads the accepted SQLite checkpoint from inside the next handler. | No second connector call follows failed persistence; both provider and storage causes survive. Before the restarted handler proceeds, the stored token must already contain one marked action with count 1 and empty raw cursor. Pebble's complete raw snapshot stays unchanged and outstanding writers return to zero. Retrying advances only to generation/count 1, not completion. This validates ordering at the SDK write boundary, not physical disk corruption. |
| P05 | Verified to stated coverage. Codec malformed/version/count/generation/base64 table, unmarked-token table with policy enabled/disabled, token golden corpus and `TestPageTokenRestartCancellationCauseRedactsCursor`. | The provider token round-trips exactly and the original Action is unchanged. Invalid marked metadata returns sanitized errors. Reserved-looking unmarked tokens reach the handler unchanged. A sibling returning context.Cause cannot expose the action cursor. The added golden fixture pins the new JSON field; CO2 moves the unknown-version fixture to version 4. |
| P06 | Verified to stated coverage. Real Pebble cells in the completion/resume matrix. | The provider call log contains the first-page request again; its next-page response succeeds only after that real request, so replaying the old first page cannot satisfy the oracle. Encoded generations remain ledger identities while RPCs receive raw tokens. The commit-failure test also rejects clearing a persisted discriminator under the current pending-work revision. |
| P07 | Verified to stated reduced coverage. `TestPageTokenRestartInterruptedFirstPage`, `TestPageTokenRestartCanceledBeforeCommit`, cold checkpoint clones and separate-process resumption. | Source cancellation immediately after a durable restart resumes and completes, including when new restarts are disabled; reopened content must contain all four expected grants, including the abandoned-only member. Actual context cancellation at SetPendingWork is positively armed, preserves both causes, leaves the full raw snapshot unchanged, releases the writer and retries at count 1. This covers representative restart boundaries, not every possible process-kill interleaving. |
| P08 | Verified to stated coverage. Independent-resource matrix and completion matrix, each SQLite/Pebble x 1/3 workers. | Resource-specific first-page counts are 2 and 3 under a shared limit of 2; sharing a global budget would fail. Completed sibling and resource-phase call counts remain unchanged. |
| P09 | Verified to stated coverage. Ledger commit test, actual provider-RPC family table and terminal-status table. | A committed restart has one pending work item and zero completed actions. All five actual RPC-family restart rows retain one ConnectorErrors observation, zero successful/empty list responses, zero completion and zero warnings. Actual completion increments once. Limits originating as NotFound, Unavailable, DeadlineExceeded or InvalidArgument retain the original error identity while becoming non-warning and non-retryable inside the SDK. |
| P10 | Verified to stated coverage. Existing golden corpus, marked-checkpoint disabled-policy matrix, and `TestPageTokenRestartLegacyTakeover`. | A marked legacy fixture is stored, consumed through loadLedgerResume/BeginFromToken, and cleared from the legacy slot. Real Pebble close/reopen and fresh runtime restore retain raw cursor `next` and count 1. Supporting readers decode marked state with a zero restart limit. Older readers remain explicitly outside the enabled-feature compatibility claim. |
| P11 | Evidence incomplete. Verified to stated host-package coverage against runtime `ec11dda` only: C1 consumer tests plus its full affected Bazel package. CO2 and CO3 change runtime after that run. | Exact Graph expiry classification includes wrapping/joined cancellation; unrelated failures stay unchanged. Exhaustion does not reset the whole sync. Legacy callbacks preserve the 15-attempt failsafe. SQLite/Pebble x 1/3 workers complete the original sync, retain abandoned observations, then remove them during a later fresh sync. No live provider or production rollout is claimed. |
| P12 | Verified to stated reduced coverage. Full affected sync race suite, `TestPageTokenRestartCheckpointWritersSerialize`, codec and existing fanout checkpoint benchmarks. | Thirty-two simultaneous forced checkpoints commit exactly once each with maximum active writer count 1. Healthy cursor decoding has zero allocations. Stored restart metadata is O(cursor bytes); forced checkpoints retain the existing O(pending actions) serialization cost and add no scan over stored grants. Race/fuzz execution is measured sampling, not exhaustive schedule coverage. |

## Instrument validation and defects corrected

- Before runtime implementation, all four completion-matrix cells failed on the
  actual provider-expiry error. After enabling restart, the same cells passed.
- Replacing the restored restart count with zero failed the resume test on an
  invalid recovery continuation. Independently changing the limit guard from
  `>=` to `>` failed the same oracle with four first-page calls instead of three.
  Both mutations were restored before final validation.
- Independent review found unmarked-token reinterpretation, warning/retry status
  surviving limit exhaustion, and internal store errors consuming restart budget.
  New regressions reproduced each defect before correction.
- A failed restart-page commit originally lost the provider cause; the fault test
  failed on the missing error identity before the persistence wrapper corrected it.
- A controlled sibling cancellation originally exposed the action cursor through
  context.Cause. The redaction regression failed before the cancel cause was
  restricted to operation/resource identity.
- The broad suite rejected the new unpinned JSON discriminator; adding and
  registering the new token fixture corrected the field-coverage failure.
- The independent evidence audit found masked negative-policy cases, identical
  membership data across attempts, missing worker-count cells and insufficient
  ordering, accounting and resumed-content assertions. Tests now check each item.
  Removing policy exclusions fails the tagged-error table. Omitting the forced
  checkpoint fails the stored-token assertion inside the next handler. Removing
  the abandoned-only grant from actual readback fails the union comparison; this
  last mutation validates the content oracle, not production deletion code.
  All temporary mutations were restored before final validation.

## Executed commands

- `go test -mod=vendor -run '^(TestPageTokenRestart|TestGoldenToken)' -count=1 -coverprofile=coverage.out ./pkg/sync` passed. The final strengthened selector also passed under `-race` with a coverage profile in 5.451 seconds.
- `go test -json -p 4 -mod=vendor -tags=baton_lambda_support -count=1 -timeout=15m ./...` passed, including sync, store, compactor, CLI and harness packages. Optional environment-gated confidence suites retain their normal gates.
- `go test -json -p 4 -mod=vendor -race -count=1 -timeout=15m ./pkg/sync` passed in 215 seconds. Additional final test-only coverage passed under the affected restart/golden race selector.
- `make lint` passed with the CI-pinned golangci-lint v2.13.2 and Go 1.27.1. The environment's preinstalled v2.9.0 could not handle the module's Go version; building the pinned tool and using the module's declared toolchain resolved that tooling failure.
- `go test -mod=vendor -run '^$' -fuzz '^FuzzPageTokenRestartCodec$' -fuzztime=20s -parallel=2 ./pkg/sync` passed 13,841 executions. This is sampled codec coverage.
- `BenchmarkPageTokenRestartCodec` measured 0 allocations for unmarked input and 3 allocations for a decoded recovery cursor. Existing checkpoint fanout benchmarks ran through 100,000 pending actions. Wall-clock samples are not a production throughput claim.

## Structural coverage and review

The focused profile reports 100% statement coverage for checkpoint serialization,
cursor parsing/encoding, policy dispatch and typed recovery errors. New discriminator
write/restore/clear-refusal branches execute in the real-store lifecycle tests.
The audit identified two missing changed-branch instruments: invokeActionPage's
decoder-error return and legacy transition-error wrapper. Both now execute in
TestPageTokenRestartInvalidTransition; the latter retains both causes and performs
no checkpoint. Restart and exhaustion checkpoint failures have explicit fault cases.
Remaining uncovered preexisting runtime/worker and warning branches are not claims
about newly added recovery coverage.

The independent implementation read was clean after the three verified findings
were corrected, including the final cancellation-message change. The independent
evidence re-audit reported zero remaining findings after the strengthened tests.
That reviewer independently ran the final focused selector on Go 1.27.1 (1.030
seconds) and a coverage run (0.915 seconds), verified both previously missing error
branches executed, and inspected the host's policy wiring and assertions. Historical
mutation and repository-gate outcomes remain author-observed, not independently
replayed. The final all-package suite passed again after the test-only changes.

C1's full affected Bazel package, graph generation, authoritative vendor-content
gate, vendor consistency, module verification and dependency-license check passed.
The consumer ran against runtime commit ec11dda. CO2 and CO3 change runtime after
that run, so P11 stays evidence incomplete until the consumer reruns against the
final runtime commit. This record claims bounded fixture coverage and sampled concurrency/codec
coverage, not universal crash or version-pair closure.

## CO2: rollback format guards

This correction adds P13 from the committed CO2 plan. It supersedes the earlier
old-reader exclusion only for the bounded reader pair below. The runtime now
stamps marked serialized tokens as version 3, including mixed type-scoped stacks.
The recovery golden fixture is `v3_page_token_restart.json`. The hand-authored
unknown-version fixture moves to version 4 so it still tests the V0 fallback;
other existing fixture bytes are unchanged.

P13: **verified to stated bounded coverage**. `TestPageTokenRestartVersionStamp`
covers plain/type-scoped/spawned/planned/restarted/mixed markers, each with and
without the graph-omission rewrite, and requires exact round-trip bytes and
restored provider cursor/count. `TestPendingWorkRestartVersion` checks the raw
declaration version, failed-commit atomicity, close/reopen, continuation without
children, child allocation by recovered and plain siblings, expansion and sealing.
`TestPendingWorkRestartSeedVersion` covers initial seeding and legacy takeover.
The existing resume and golden suites continue to cover disabled-policy reading
and repeated cold resume on both engines.

`scripts/check-page-token-restart-rollback.sh` exports tokens and closed Pebble
databases through the current production writers, then reads them with the pinned
pre-recovery parent `47f5027e75e56872a9325a378badcaaab2718f4a`. Its old-reader
assertions require the recovered action stack to be discarded and the recovered
pending-work declaration to be rejected before returning any work. Plain tokens,
version-2 type-scoped tokens and plain queues remain readable. This checks the
actual older codec and engine, not an emulation of their version tests. It does
not claim a completed old-binary full sync or compatibility with every release.

Instrument validation: before the fix, the serialized stamp table failed with
versions 1/2 instead of 3; both initial-seed cells and the transition test failed
with pending-work version 2 instead of 3. After the fix, the unchanged rollback
script exits successfully. With `BATON_ROLLBACK_MUTATE=1`, lowering the exported
token guard to 1 and the declaration guard to 2 makes both recovered old-reader
cells fail while their plain baselines pass. The old token reader exposes the
wrapped cursor and the old queue reader returns work. Mutations affect only the
throwaway compatibility artifacts.

Execution against the final runtime and tests:

- `make test`: passed with CI's `baton_lambda_support` tag; 67 tested packages.
- `go test -race -tags=baton_lambda_support -count=1 -run 'Test(PageTokenRestart|PendingWork|Ledger.*(Phase|Terminal|Seal)|.*Token|GoldenToken)' ./pkg/sync ./pkg/dotc1z/engine/pebble`: passed.
- `go test -count=1 -coverprofile=coverage.out -run 'Test(PageTokenRestart|PendingWork|LedgerWorkStateRejectsForeignEncodings|Ledger.*(Phase|Terminal|Seal)|SyncerToken|GoldenToken)' ./pkg/sync ./pkg/dotc1z/engine/pebble`: passed.
- `GOTOOLCHAIN=go1.27.1 scripts/check-page-token-restart-rollback.sh`: passed; the same command with `BATON_ROLLBACK_MUTATE=1` failed at both required recovered-reader assertions.
- `GOTOOLCHAIN=go1.27.1 make lint`, with CI-pinned golangci-lint v2.13.2: passed, zero issues. The environment's Go 1.27.2 export format was incompatible with the lint loader, so lint uses the module's declared toolchain.
- `go test -run '^$' -fuzz '^FuzzPageTokenRestartCodec$' -fuzztime=20s -parallel=2 ./pkg/sync`: passed, 742 executions; sampled codec coverage.
- `bash -n scripts/check-page-token-restart-rollback.sh` and `git diff --check`: passed.

The first targeted run encountered an existing keyspace-reader test's transient
Pebble lock error. That test passed in isolation on both the unchanged parent and
the candidate, then passed in the candidate race and full-suite runs.

CO2 structural triage: the profile executes every new version choice, supported
version read, malformed declaration/phase rejection and successful declaration
write. The remaining uncovered blocks in the edited functions are inherited
validation and I/O-error returns: closed-store reads, invalid queue phases and
seeds, allocation/revision overflow, invalid children, and staging failures. These
are excluded from new fault-coverage claims; the extraction preserves their
predicates and propagation. Failed-batch atomicity has a direct new fault test.
No new keyspace, lock, closer, scan or commit point is added. Healthy page work
and worst-case checkpoint serialization complexity are unchanged; recovery adds
one constant-size declaration update in the existing page batch.

The independent reader inspected the final writers, readers, tests and script,
audited the before/after, race, lint, broad-suite and mutation evidence, and
reported no findings within this bounded correction. Historical release-pair,
full old-binary sync and exhaustive crash-schedule coverage remain excluded.

## CO3: review corrections

This correction follows the CO3 plan entry. A deep review of `02c5996c` found the
three defects below; each failing instrument was observed before its fix.

P14: **verified to stated coverage**. `TestProviderPageErrorKeepsStatus` sends a
`PermissionDenied` connector status through the five provider RPC families x
legacy/Pebble with no restart policy. The oracle is the status each call site
returns without `providerPageError`: the connector's own message for the four
direct calls, and the existing `sync-grants-for-resource` prefix for grants.
`TestProviderPageErrorStatusTransparent` compares `status.FromError` with and
without the wrapper for status, wrapped-status, plain and canceled causes, each
bare, `%w`-wrapped and joined ahead of a sibling status error. It also requires
`errors.Is` to reach the cause and `errors.As` to find `providerPageError`.

P13 extension: **verified to stated bounded coverage**. `TestPendingWorkRestartVersion`
now requires version 2 after `BeginExpanding` and after the terminal page.
`TestPendingWorkRestartVersionTerminalFromCollecting` seals straight from
Collecting and requires version 2. `TestPendingWorkRestartVersionMarkedExpansion`
keeps version 3 when the only pending entry is itself marked. The rollback script
exports a drained queue (restart committed, marked work completed, `BeginExpanding`)
and runs the pre-recovery reader's `StartNewSync` on every queue. The plain and
drained queues start a new sync, and the drained queue also resumes at its
expansion entry. The recovered queue still rejects resume and also rejects
`StartNewSync` with `invalid pending-work state`; `WithPageTokenRestarts` now
states that window.

Instrument validation:

- Before the fix, `TestProviderPageErrorKeepsStatus` failed in all eight
  direct-call cells with `rpc error: code = PermissionDenied desc = connector says
  no`; the grants cells passed.
- A first fix gave `providerPageError` a `GRPCStatus` method that returned nil for
  causes without a status. `TestProviderPageErrorStatusTransparent` failed in its
  plain/joined and canceled/joined cells: the nil status stopped `errors.As`
  before the sibling's status. The final fix returns `providerStatusPageError`
  only for causes that carry a status.
- Before the fix, both new version-2 expectations failed with version 3.
- `BATON_ROLLBACK_MUTATE=1` now also keeps the drained queue at version 3. Its
  old-reader cell fails, as do the recovered token and queue cells, while the
  plain baselines pass.

Execution against the final runtime and tests:

- `go test -json -p 4 -mod=vendor -tags=baton_lambda_support -count=1 -timeout=15m ./...`: passed; 67 tested packages, no failures.
- `go test -race -tags=baton_lambda_support -count=1 -run 'Test(PageTokenRestart|ProviderPageError|PendingWork|Ledger.*(Phase|Terminal|Seal)|.*Token|GoldenToken)' ./pkg/sync ./pkg/dotc1z/engine/pebble`: passed.
- `go test -count=1 -coverprofile=coverage.out -run 'Test(PageTokenRestart|ProviderPageError|PendingWork|LedgerWorkStateRejectsForeignEncodings|Ledger.*(Phase|Terminal|Seal)|SyncerToken|GoldenToken)' ./pkg/sync ./pkg/dotc1z/engine/pebble`: passed.
- `scripts/check-page-token-restart-rollback.sh` on Go 1.27.1: passed; with `BATON_ROLLBACK_MUTATE=1` it failed at the recovered token, recovered queue and drained queue cells.
- golangci-lint v2.13.2 built with Go 1.27.1, `run --timeout=3m`: passed, zero issues.
- `bash -n scripts/check-page-token-restart-rollback.sh` and `git diff --check`: passed.

The full-suite and race runs preceded a one-line declaration change in
`TestProviderPageErrorKeepsStatus` that lint required; that test passed again
afterward.

CO3 structural triage: the profile executes both `newProviderPageError` branches,
the `providerStatusPageError` methods, the `BeginExpanding` version choice in both
directions, and the terminal-page version write. No new keyspace, lock, scan or
commit point is added; the version byte rides the existing `BeginExpanding` and
terminal-page batches. `newProviderPageError` adds one `status.FromError` per
failed provider RPC; healthy pages are unchanged.

Not covered: no independent reader has audited CO3. P11 stays evidence incomplete
until the C1 consumer reruns against the final runtime commit. A pre-recovery SDK
still cannot resume or start a new sync over a Pebble file between an action's
first restart and the end of collection; CO3 documents that window and does not
remove it.
