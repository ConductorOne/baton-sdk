# List-page-window verification evidence

## Coverage

- W1/W2: `TestListPageWindowSyncMatrix` runs real syncs over serial token, two-worker parallel token, and Pebble ledger modes. Every mode explicitly asserts its selected transition mode. All four scenarios (sampled, terminated at window, terminated early, disabled) cover top-level resources, two same-ID/different-type parents, per-resource entitlements/grants, and type-scoped grants with two nonempty-token siblings. Assertions cover calls, report identities, sampled counters, and stored final-page rows.
- W3: the matrix proves static entitlement pages remain unbounded and each sibling receives two pages independently. `TestListPageWindowActionCheckpointAndRewalk` proves two new actions for the same identity report twice.
- W4: checkpoint round-trip preserves the live page count and prior sampled counters; `TestListPageWindowZeroPreservesSyncTokenBytes` compares whole serialized tokens with and without explicit zero. The unchanged golden corpus plus a newly recorded fixture pin default compatibility and the additive wire names. `TestListPageWindowLedgerReconstructionStartsNewWindow` pins the documented restart limitation.
- W4 pending refresh: `TestListPageWindowPendingRefreshCarriesMatchingRevision` proves the page count survives a matching-revision refresh and is discarded for a different revision. Removing the refresh carry through a temporary Go overlay makes the matching-revision case fail (expected two pages, observed zero).
- W5: the serial callback acquires the run-state and transition locks. Failed transition and ledger-stage-then-failure tests assert no report or sampled counter, and restore the caller's page count. Source audit confirms ledger callbacks use the existing after-commit boundary, outside queue/run-state locks.
- W6: counter-only token overlays, ledger conversion, and signed-reader saturation have direct tests. The matrix reads both EndSync routes. `TestListPageWindowCLIToFinishedSyncStats` re-executes a real connector CLI and its service subprocess, then reopens the resulting c1z and checks two stored resources and one sampled list. CLI negative/overflow values are rejected before constructing the connector.

## Observed checks

- Buf lint, format, and breaking against `origin/main`: pass. The removed annotation existed only on this unmerged branch; no annotation is removed from main's published protocol.
- `make protogen`: pass with repository-pinned remote generators.
- `go build -tags=baton_lambda_support ./...`: pass.
- Changed handwritten packages: CI-pinned golangci-lint v2.14.0 reports zero issues.
- Focused window tests: pass, including the subprocess CLI smoke and golden checkpoint assertions.
- Race-instrumented window matrix, CLI smoke, and counter tests: pass.
- Changed-package regression suites pass for syncer (92 seconds), c1zstore, Pebble engine, config, CLI, connectorrunner, local tasks, fields, annotations, and chaosconnector. The syncer rerun includes the required golden checkpoint fixture.

## Instrument check and source audit

A planted counter defect prevented the walk from reaching window two. The serial sampled matrix rejected it with five list calls instead of two. The source was restored and the positive matrix passed. An independent read-only source audit found a counters-only-token classification defect; the parser guard and direct test now cover it. A bounded re-read reported no additional verified correctness findings.

The first full syncer run correctly rejected the new checkpoint fields because the golden corpus did not cover them. A fixture was recorded through the actual codec, registered in the existing corpus, and documented; no prior fixture or coverage exclusion changed.

## Limits

Ledger rows persist the next cursor but not the action's window count. A process restart can therefore return one additional window per restart. This is an explicit opt-in sampling limitation, not a claim of strict pagination bounds across ledger restarts. In-process pending refresh and token-based restart preserve the count. Connector-signaled cursor restarts also retain already-counted pages; rewound pages consume the remaining window and can reduce the sample of distinct rows.

Sampling does not establish dataset completeness. Vendor termination is reported separately and does not increment sampled counters. No connector annotation, connector-side policy, merge, deployment, or release tag is part of this change.
