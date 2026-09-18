# Collection report experiment

This is a feasibility experiment, not a shipped report. The examples below come
from synthetic pages committed through the real Pebble page writer. The durations
are deliberately supplied fixture values. No customer data is used.

## Example: what was recorded for each collection?

Request scope: not recorded in this fixture. Do not infer that all resource types
or grant collections were requested. Operation: grant collection; type: app.

| Resource | Pages | Grant writes | Recorded connector time | Reported rate-limit wait (inside connector time) | Recorded work |
| --- | ---: | ---: | ---: | ---: | --- |
| team-a | 2 | 5 | 4.20s | 3.00s | Both pagination pages present; terminal page present |
| team-b | 1 | 0 | 0.10s | 0 | Terminal page present; endpoint outcome unavailable |
| team-c | 1 | 1 | 0.50s | 0 | Continuation advertised; matching page absent |
| team-d | 1 | 0 | 0.05s | 0 | Child collection for team-e advertised; matching page absent |

Interpretation:

- team-a: reported rate-limit waiting accounts for 3.00 of its 4.20 seconds of
  recorded connector time. Its slower page took 4.00 seconds. This is cumulative
  connector time, not the elapsed critical path of a parallel sync.
- team-b: zero writes do not prove an empty endpoint. A tolerated warning can
  produce the same row. Filtering and non-connector control actions also need
  distinct treatment in a production report.
- team-c: the committed page declares more work for the same collection. The
  artifact does not record its completion. This is expected in an interrupted
  file; it needs explanation in a file claiming complete collection.
- team-d: the missing work is a separate child, not another pagination page.
  The report can point to team-e without reading grant records.

The prototype checks every recorded continuation and child reference using the
whole action identity. Parent scope and type-scoped collection remain distinct.
It does not prove root reachability, absence of cycles, or completeness of work
that the connector never declared. “All references resolve” is not a verdict
that a source system's data was completely collected.

## Request scope and intentional omissions

The existing init page saves should_skip_grants and
should_skip_entitlements_and_grants facts. Separate fixtures demonstrate:

| Saved evidence | Report interpretation |
| --- | --- |
| should_skip_grants | Grants disabled by saved sync flag |
| should_skip_entitlements_and_grants | Entitlements and grants disabled by saved sync flag |
| Neither, and no request manifest | Request not recorded; absence is not evidence that grants were enabled |

These are effective saved facts, not a verbatim command-line history. The current
ledger does not contain a complete, versioned request manifest. Resource-type
filters, selected targets, expansion-only mode and changes in options across
resume need an explicit representation. Record allowlisted effective options,
not raw argv, environment variables, paths or credentials.

The planner also skips individual resources or types based on connector
annotations. Those decisions do not currently produce a per-resource reason in
the ledger. Type-scoped grant collection may replace all individual group calls;
an absent per-group row must not be reported as missing work in that case.

## What needs recording for a trustworthy report

1. Effective requested scope, including which attempt supplied it and how a
   resumed run relates to the original scope. Missing metadata stays unknown.
2. Per-page outcome: successful connector response, tolerated warning, internal
   planning, or deliberate skip. A warning needs a stable reason/code, not an
   arbitrary error string that could expose response data.
3. Intentional collection exclusions and their reasons, including resource/type
   annotations and type-scoped collection. Prefer recording these alongside the
   planner's existing decisions; avoid reconstructing them by scanning records.
4. Connector-call count and response/filter counts if the report is to distinguish
   an empty response from all returned records being filtered or overwritten.

Current rows already support resource-level write counts, recorded timing,
pagination/child-reference checks and grouping by full scope. They do not support
an honest “all requested data collected” verdict by themselves.

## Cost experiment

The test-only aggregator streams groups in ledger key order, keeps the top ten
collections by connector time, and can emit full resource summaries incrementally.
It does not hold a map of every resource. Reference validation uses point reads
for each advertised continuation or child. That costs additional reads; this is
not merely a single sequential scan. It performs no record-family scans or writes.

Coverage-only prototype smoke measurements (before timing sections) on Linux arm64, Go 1.26.0, GOMAXPROCS 4. The environment
reports 16 logical CPUs; it is not an unloaded-machine qualification. Each fixture
was flushed to SSTs, then the report was measured three times. Caches were not
cleared. There are no adjacent grant records or large child lists in this cost
fixture. Population time is excluded. No output serialization is timed.

| Ledger pages | Pages per resource | Mean report time | Allocated bytes per report |
| ---: | ---: | ---: | ---: |
| 1,000 | 1 | 1.15ms | 0.94MB |
| 1,000 | 100 | 3.21ms | 1.00MB |
| 10,000 | 1 | 9.96ms | 9.22MB |
| 10,000 | 100 | 32.53ms | 9.79MB |
| 100,000 | 1 | 100.35ms | 92.04MB |
| 100,000 | 100 | 329.05ms | 97.79MB |

The 100-page chains check 99,000 continuations at 100,000 pages. Allocated bytes
are cumulative allocation, not peak live memory or RSS. Deserialization produces
allocation proportional to rows despite bounded retained aggregation state.

These numbers support continuing the experiment. They do not establish final
report cost: measure large fan-out, mixed record/ledger SSTs, cold reads, full
output generation and peak RSS. The initial prototype reported total and maximum connector time only. The timing
extension below adds bounded histogram quantiles and generated HTML/JSON.

## Decision so far

The promising deliverable is a report of requested scope, recorded collection
coverage, intentional exclusions, unexplained recorded work, output and timing.
The existing ledger provides much of the evidence but lacks outcome and scope
explanations. Retaining rows alone does not solve those gaps.

Do not change retention or scrubbing in this experiment. A later finished-sync
continuation currently clears collection rows; a durable collection report must
survive that lifecycle if it is to explain the original collection. Whether to
keep individual rows or only their report remains a separate decision after
agreeing on the useful drill-down.

## Mechanically generated timing artifact

The Go prototype now emits standalone report.html and report.json directly from
the aggregate result. The HTML is a fixed escaped template, with no generated
analysis text. It includes coverage evidence and the ten largest collections by
recorded connector time, ordered deterministically across equal totals.

Reproduce the synthetic artifact:

```sh
LEDGER_REPORT_OUTPUT_DIR=/tmp/ledger-generated-report GOTOOLCHAIN=go1.26.0 \
  go test -mod=vendor ./pkg/dotc1z/engine/pebble -run '^TestLedgerReportPrototype$' -count=1
```

The timing table includes share of all recorded connector time, reported waits,
maximum connector milliseconds per page, median/p95 intervals, writes per page,
and pages/connector milliseconds per 1,000 writes. Quantiles use nearest rank in
65 integer histogram buckets; only the current group's histogram is retained.
No list of individual page latencies is kept. Pages are not labeled API calls.
Rates for zero writes are unavailable. The JSON contains no page-token values.

In the generated fixture team-a has 86.60% of the 4,850 recorded connector
milliseconds, 3,000 reported wait milliseconds, 2.5 writes per page and 400 pages
per 1,000 writes. Its median falls in 128–255ms and p95 in 2,048–4,095ms; its
exact maximum is 4,000ms. Those intervals are intentionally not printed as exact
percentiles. These are fixture measurements, not real connector performance.

The final cost run includes scanning, reference checks, histogram/rate aggregation,
top-ten selection and HTML rendering. It excludes output file writes and JSON
serialization. Same warm-cache synthetic conditions as above; three iterations:

| Pages | Pages per resource | Mean time including HTML | Cumulative allocation | HTML size |
| ---: | ---: | ---: | ---: | ---: |
| 1,000 | 1 | 2.22ms | 1.23MB | 8.2KB |
| 1,000 | 100 | 3.95ms | 1.12MB | 8.3KB |
| 10,000 | 1 | 15.74ms | 11.11MB | 8.2KB |
| 10,000 | 100 | 34.78ms | 9.94MB | 8.3KB |
| 100,000 | 1 | 151.58ms | 109.76MB | 8.2KB |
| 100,000 | 100 | 341.32ms | 98.09MB | 8.3KB |

HTML whitespace was subsequently wrapped to meet repository line limits; that
format-only change was tested but not re-benchmarked. Sizes are from the measured
revision. This remains a feasibility result, not C49 qualification. The prototype
is still test-only and has no command for an arbitrary customer artifact. Full
resource drill-down, effective-request metadata, outcome/skip reasons, cold reads,
large fan-out and peak RSS remain outstanding.
