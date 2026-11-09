# List-page-window verification evidence

## Coverage

- W1/W2: `TestListPageWindowSyncMatrix` runs real syncs over one-worker and two-worker Pebble ledger modes. Main removed token-mode collection; the rebase migration is recorded in the plan addendum. All four scenarios (sampled, terminated at window, terminated early, disabled) cover top-level resources, two same-ID/different-type parents, per-resource entitlements/grants, and type-scoped grants with two nonempty-token siblings. Assertions cover calls, report identities, sampled counters, and stored final-page rows.
- W3: the matrix proves static entitlement pages remain unbounded and each sibling receives two pages independently. `TestListPageWindowActionCheckpointAndRewalk` proves two new actions for the same identity report twice.
- W4: legacy checkpoint round-trip preserves the live page count; `TestListPageWindowZeroPreservesSyncTokenBytes` compares whole serialized tokens with and without explicit zero. The pre-existing golden corpus plus a recorded window-pages-only fixture pin default compatibility and the retained Action wire field; token sampling counters are removed. `TestListPageWindowLedgerPreservesWindowAcrossReopen` verifies page one commits, a cold reopen restores one counted page, and exactly two more pages exhaust window three.
- W4 pending refresh: `TestListPageWindowPendingRefreshCarriesMatchingRevision` proves the page count survives a matching-revision refresh and is discarded for a different revision. Removing the refresh carry through a temporary Go overlay makes the matching-revision case fail (expected two pages, observed zero).
- W5: the report callback acquires the run-state lock. Failed transition and ledger-stage-then-failure tests assert no report or sampled counter, and restore the caller's page count. Ledger callbacks use the existing after-commit boundary, outside queue/run-state locks.
- W6: ledger conversion and signed-reader saturation have direct tests. The matrix reads sealed ledger stats. `TestListPageWindowCLIToFinishedSyncStats` re-executes a real connector CLI and its service subprocess, then reopens the resulting c1z and checks two stored resources and one sampled list. CLI negative/overflow values are rejected before constructing the connector.
- W6 portable keys: the matrix requires exactly the five sampling keys when sampled, and an empty counters map for every unsampled case with one or two workers, including window zero. `TestLedgerSyncStatsPublishesOnlyListPageWindowCounters` also verifies that filtering does not alter internal completion/ingestion accounting.
- W6 optional stats: `TestFinishedSyncMetadataIgnoresCorruptStatsSidecar` first proves a healthy cached counter is readable, then corrupts only the sidecar. Both full-sync and any-type lookups return the original finished identity with nil Stats and no error, and emit a warning.

## Observed checks

- Buf lint, format, and breaking against `origin/main`: pass. The removed annotation existed only on this unmerged branch; no annotation is removed from main's published protocol.
- `make protogen`: pass with repository-pinned remote generators.
- `go build -tags=baton_lambda_support ./...`: pass.
- Changed handwritten packages: CI-pinned golangci-lint v2.14.0 reports zero issues.
- Focused window tests: pass, including the subprocess CLI smoke and golden checkpoint assertions.
- Race-instrumented window matrix, CLI smoke, and counter tests: pass.
- Changed-package regression suites pass for syncer (92 seconds), c1zstore, Pebble engine, config, CLI, connectorrunner, local tasks, fields, annotations, and chaosconnector. The syncer rerun includes the required golden checkpoint fixture.

## Instrument check and source audit

A planted counter defect prevented the walk from reaching window two. The one-worker sampled matrix rejected it with five list calls instead of two. The source was restored and the positive matrix passed.

The window-pages-only fixture was recorded through the actual codec and registered in the existing corpus; no pre-existing fixture or coverage exclusion changed. The earlier counter-bearing fixture and its token-only counter producer tests were removed after main deleted non-ledger collection.

Removing only the ledger counter filter through a temporary Go overlay makes the window-zero ledger matrix fail: internal action/ingestion keys appear where the portable counters map must be empty. The overlay was removed and the unmodified matrix passed.

## Limits

Ledger pending-work revisions and cold ledger reopen preserve the same successful-page count and remaining window; the legacy token codec retains the Action field without sampling counters. Connector-signaled cursor restarts retain already-counted pages; rewound pages consume the remaining window and can reduce the sample of distinct rows. Older SDKs resuming a sampled run do not preserve window counts or stamp the sampled marker and counters. Chained `EnqueuePageTokens` spawns are bounded per spawn, not across the whole list.

Sampling does not establish dataset completeness. Vendor termination is reported separately and does not increment sampled counters. No connector annotation, connector-side policy, merge, deployment, or release tag is part of this change.

The sampling CLI flag is hidden until downstream consumers enforce the durable marker. Sampled artifacts remain local-only; production diff/upload is forbidden until the downstream upload/uplift gate consumes the marker on its SDK re-pin. A separate sampled-run sync-fact follow-up remains deferred.

## Owner-feedback marker and restart proof

- `TestSampledSyncMarkerAtSeal` covers stats-overlay, ledger-overlay, and ledger-recovery sealing with positive and zero sampled counters. It checks the payload record, exported metadata, sorted metadata, reader RPC, and replay eligibility. `TestSampledSyncMarkerSurvivesResealWithoutCounters` pins monotonic sample identity for follow-on passes through the supported stats-overlay seal.
- `TestSampledSyncMarkerStampDurability` checks an unfinished/unmarked record before the stamp and a crash image immediately after it. Both stats-overlay and ledger images contain ended_at, sampled, and prior page data before the optional stats sidecar exists.
- The replay refusal table covers resources, entitlements, and grants. Compactor tests refuse sampled bases and partials in fold, K-way, and overlay modes; raw Compact/MergeInto/K-way/overlay tests exercise the shared payload guard. A new selected complete sync remains eligible after an earlier sampled run.
- `TestPendingWorkPreservesWindowPages` checks row conversion, pending revisions, and connector-signaled restart preservation. `TestListPageWindowLedgerPreservesWindowAcrossReopen` closes after page one, reopens cold with count one, and needs exactly two more pages to exhaust window three; the sealed run is sampled.
- `TestListPageWindowCLIIsHidden` pins the interim hidden flag. The real CLI subprocess smoke still accepts the explicit flag and verifies the durable sampled verdict.
- Temporary Go overlays disabling the marker stamp, durable page-count copy, compactor refusal, and replay refusal are each rejected by their behavioral regression. The overlays were removed without modifying source files.

The final shape intentionally has no manifest sampled projection, presence fallback, high-level duplicate refusal sites, or token-to-ledger takeover extension. Existing header selection is unchanged; merge seams read the selected payload's authoritative record.

The simplified owner-feedback candidate passed the full affected suites (16 packages, with four protobuf/support packages having no tests), targeted race checks, scoped CI-pinned lint, additive Buf lint/format/breaking checks, and the full SDK build. The rebase subsequently replaced the unreleased counter-bearing token fixture with a window-pages-only recording. The pre-existing token corpus, manifest schema/projection, and token-takeover implementation remain unchanged.
