# Additional report fields: code study

Later decisions in CO-017 narrow retry statistics to successfully committed
pages and omit resume-reuse counts. This is a design study, not report output or evidence that the fields are
implemented. Inspected the existing scheduler, current ledger handlers, retryer,
run accounting and report prototype. No execution behavior changes in this study.

| Proposed field | Existing evidence | Addition required | Recommendation |
| --- | --- | --- | --- |
| Output by record family | LedgerRow stores resource-type, resource, entitlement and grant write counts separately. The prototype currently combines them. | Keep those four counters separate in the projection and aggregates. | Include by operation/type and globally. No new row metadata or scan. |
| Connector-returned counts | Handlers have the response lists before validation/filtering. These lengths are not stored in the page row. | Capture list lengths at each response boundary and commit them with the page; keep family attribution explicit. | Include. O(1) per response when its list is already materialized. |
| Filtered/invalid counts | Resource/entitlement/grant handlers already track invalid records and selected-type exclusions in page counter observations; those fold into worker buckets. | Preserve useful reason/type attribution with the page or structured aggregate. | Reuse those observations. Do not infer reasons from returned minus written. |
| Zero-output pagination | Current report has zero-write pages, terminal pages and continuation counts. Tokens/hashes show whether a page declared more work. | Count zero-write-and-continuation pages; add true zero-response counts once returned counts exist. | Include as separate numeric fields; neither condition is a bug verdict. |
| Pages per collection | Ordered scope grouping already counts pages per collection. Type-scoped and parent-scoped identities remain separate. | Add aggregate distribution/max if desired, using fixed histograms. | Cheap; distinguish connector collection from internal planning pages first. |
| Retry/error counts | Syncer calls expose RPC status; retryer exposes decisions/waits. Current call observations are page-owned; failed pages discard them. | Observe error codes at RPC boundaries and retry decisions when another attempt is actually scheduled. Persist observed run accounting separately from successful-page totals. | Include observed SDK RPC errors and retries, not claimed upstream HTTP attempts. |
| Retry/backoff duration | Retryer reports completed or cancellation-shortened sleeps through the wait observer. Run accounting already saves waits, including resource-type labels. | Add operation attribution; distinguish rate-limit waits from other backoff. | Reuse actual wait observations, not planned delays. |
| Resume accounting | Restore walk and action wrapper recognize trusted rows; fresh page rows carry an attempt identity. | Count reused versus newly committed pages at distinct existing execution points; record attempt metadata. | Include, but do not infer reuse or total starts solely from final rows. |
| Elapsed phase durations | Existing timedStep wraps many coordinator phases. Expansion has a collection-complete boundary; finalization is explicit after the scheduler. | Define and instrument disjoint active collection/expansion/finalization intervals, including stops and modes that never expand. | Include. Keep attempt-active elapsed time separate from worker totals and time between processes. |
| Bounded warning examples | NotFound is the scheduler's tolerated warning. Targeted GetResource also handles NotFound/Unimplemented explicitly. Existing logs retain raw action/error objects. | Capture stable operation/type/resource/code fields at the outcome, with a fixed cap and omitted count. | Debug only; default retains counts. Do not export raw errors or action structs containing tokens. |

## Important distinctions

**Returned, derived, written and final records are different measures.** Grant
handling may derive resources and entitlements from response content (including
InsertResourceGrants). Multiple pages can write the same identity, and staged
puts/deletes can change the final set. Report connector-returned records, generated
records where instrumented, filtered records and write counts separately. Do not
advertise write counts as distinct final records or assume they form a conservation
equation with response lengths.

**“Empty” requires outcome and action-kind evidence.** A zero-write page can be a
planning action, a filtered response or a tolerated warning. Connector page/call
classification and an explicit outcome are needed before calling it an empty API
response. A true zero-response page with a continuation is useful pagination
telemetry but can be valid behavior.

**Count errors at the connector boundary.** An action can fail because of a store
write, not just a connector error. The scheduler's action error must not be counted
as an API failure. Current SDK retries accept Unavailable and DeadlineExceeded;
rate-limit details may select the wait. A completed wait alone does not prove a
retry RPC happened: cancellation can stop it. Retries inside the connector or its
HTTP client are invisible unless the connector reports them separately.

**Durability of observations differs from committed data.** Failed calls have no
committed page. Run-level diagnostics can be flushed on orderly stop or included
with a later commit, but a hard crash can lose observations not yet persisted.
Label them observed counts, not a complete audit of every attempted network call.
An attempt that dies before any durable record likewise cannot be recovered from
final page rows alone. LedgerRow.Replayed describes source-cache replay, not a
resume skipping already-committed work; do not repurpose it.

**Resume counts need one owner.** Both restoration and invokeActionPage can see
trusted rows. Counting every lookup would double-count reuse. Use the existing
walk's first-visit accounting and distinguish any later scheduler skips. Do not
add a report-sized identity set or another lookup pass for this metric.

**Elapsed phase time needs explicit boundaries.** The existing scheduler gives
useful boundaries without changing scheduling. Resource-only and expansion-only
runs, interrupted runs and resumed expansion need coverage. Sum active intervals
per phase across attempts; a separate start/end span may include offline time.
Keep retry/wait subsets labeled as overlapping phase time. Seal timing cannot be
part of a summary irrevocably finalized before seal finishes: record it afterward
for logs, and decide separately how the artifact receives that final duration.

**Cardinality remains a cost constraint.** Operation and stable status/reason
codes are bounded categories. Do not add unbounded maps keyed by resource IDs or
raw messages. Full resource-type breakdowns must use the planned streaming or
bounded representation; observing events cheaply does not make retaining every
dimension in memory free.

## Implementation order

1. Keep existing write counts by family; add zero-write continuation counts.
2. Preserve effective options and explicit page kind/outcome; capture returned
   and filtering counts in the existing loops.
3. Add elapsed phase timings and observed RPC-error/retry accounting, with clear
   stop/resume durability and operation attribution.
4. Add resume reuse/commit counts and attempt metadata without another report walk.
5. Debug-only bounded outcome examples and optional reference checks.

The inspection supports these fields, with the qualifications above. It does not
justify additional data scans or changes to retry/scheduling behavior. Numerical
checks and planted defects belong in the implementation brief before code changes.

## Code inspected

- pkg/sync/ledger_grants.go: connector response, InsertResourceGrants handling,
  derived records and stageLedgerFilterStats.
- pkg/sync/ledger_resources.go and ledger_entitlements.go: response filtering,
  invalid-record observations and selected-type exclusions.
- pkg/sync/ledger_resource_types.go: page-owned call and reported-wait observations.
- pkg/sync/parallel_syncer.go and pkg/retry/retry.go: timedStep, retry decisions,
  actual wait observation and transition into expansion.
- pkg/sync/ledger_walk.go, ledger_scheduler.go and ledger_run_accounting.go:
  trusted-row reuse, discarded page observations and run-bucket persistence.
- pkg/sync/ledger_sync.go: current uncommitted finalization integration.
- pkg/dotc1z/c1zstore/ledger.go and ledger_report_*_test.go: stored fields and
  current streaming report aggregation.
