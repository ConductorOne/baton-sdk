# Collection NotFound verification evidence

## Scope

The change makes a NotFound from root ListResources fail collection instead of committing a completed warning. A ListResources request with both parent type and parent ID retains the deleted-parent warning behavior. NotFound from object-scoped entitlement, grant, and targeted-resource operations is unchanged. No exported API or artifact/checkpoint format changes.

Current sync stores must expose the Pebble page ledger (`syncer.setStore`); SQLite is not an alternate active scheduler/store implementation on this base. The real-store instruments therefore use Pebble. There is no change to the cost curve: the additional classifier is constant-time with no allocation, scan, new state, or IO.

## Regression and instrument validation

Commands run with `CGO_ENABLED=0 GOMAXPROCS=4`, `-p 4 -mod=vendor -buildvcs=false -count=1`.

1. Before the production fix, the dedicated root-list test failed because NotFound returned a nil sync error. PermissionDenied and Unavailable remained errors, and valid empty collections remained successful. The initial fixture also exposed test-premise mistakes: the retry budget permits two Unavailable calls, and runState.current returns a copy. Those assertions/setup were corrected; they were not classified as product defects.
2. With the final fixtures, a controlled mutation removed the new action-scope guard, restoring the old warn-and-drop classification. Eight root NotFound cases failed (one/four workers × first/continuation pages × direct/wrapped errors), and the three real-store root NotFound cases failed (direct, gRPC, gRPC server fault). The guard was restored before final checks.
3. The final scoped run passed in 24.735s:

```sh
go test -p 4 -mod=vendor -buildvcs=false -count=1 \
  -coverprofile=/tmp/sdk-coverage.out \
  -run 'Test(RootResourceListErrorRetainsPendingPage|ChildResourceListNotFoundCompletesWithWarning|EmptyRootResourceListCompletes|ActionWarningScope|ChaosConnectorResourcesAndEntitlementsFaultMatrix|LedgerExistingSchedulerWarningCommitsAccounting|SpawnedCursorErrorCategoriesEndToEnd)$' ./pkg/sync
```

The coverage-profile destination is incidental. `isActionWarning` and `isWarning` both had 100% statement coverage. Overall package coverage was 42.5%; it is not claimed as whole-package closure. Both changed warning decisions were exercised for fatal root failures and allowed warnings. Existing unrelated branches in their enclosing functions are outside this change's coverage claim.

## Criterion evidence

| Criterion | Status | Instrument and exact claim |
| -- | -- | -- |
| NF-01 | Verified to stated coverage | Root scheduler fixture: NotFound propagates with the original identity/code and no warning. Real-store fault matrix: root collection failure aborts rather than seals successfully, over three transports. |
| NF-02 | Verified to stated coverage at d41c9976 | The parent-scoped NotFound fixture now seeds a surviving resource, verifies the single request and warning, and compares full logical entity contents before and after completion. This is an in-process scheduler fixture, not a real-store deleted-parent cold-resume test. |
| NF-03 | Verified to stated coverage at d41c9976 | The scheduler fixture asserts zero resources, entitlements, and grants. A separate public `Sync` fixture for one and four workers closes and reopens a Pebble artifact, verifies the run ended, retains declared resource types, and checks zero entity rows and the expected store identities. |
| NF-04 | Verified to stated coverage | Root error matrix covers NotFound, PermissionDenied, Unavailable, direct/wrapped, first/continuation, one/four workers. ActionWarningScope additionally covers nil error, malformed parent scope, child lists, entitlements, grants, targeted resources, and unspecified action. |
| NF-05 | Verified to stated coverage | Existing real-store fault matrix updated for root NotFound: verifies fault fired, unfinished persisted sync, close/reopen with fresh connector and store handle, same sync ID, eventual completion and full logical-content equality against an uninterrupted baseline. |
| NF-06 | Verified to stated coverage | Original implementation failure plus controlled guard-removal mutation. Final instruments failed in the eight root scheduler cases and all three root fault transports, then passed after guard restoration. |
| IM-01 | Verified to stated coverage | RootResourceListErrorRetainsPendingPage compares full raw ledger snapshots, requires the action remain pending, and asserts no warnings after the actual connector call. |
| IM-02 | Verified to stated coverage | Same test drives syncParallel, invokeActionPage and syncOneAction rather than manually sequencing transitions; original error identity and status survive both classification points. |
| IM-03 | Verified to stated coverage | ChildResourceListNotFoundCompletesWithWarning and LedgerExistingSchedulerWarningCommitsAccounting retain child completion and warning counts. |
| IM-04 | Verified to stated coverage | Root matrix covers both empty and nonempty checkpointed page tokens. Setup updates runState through nextPage, not a detached copy. |
| FL-01 | Explicitly excluded from this bug-fix PR | WithSyncResourceTypes explicitly selects listed types; ChildResourceType is attached to emitted parent resources, not a type-level dependency declaration. AWS Account Access Manager documents the role-only filter limitation in roles.go:139-156. A new dependency declaration and connector adoption are feature/API design work, not a demonstrated failure of an existing dependency contract. No speculative API is added. |

## Review selection and remaining gates

Selected review passes: edge cases, checkpoint/resume, permutation coverage, error classification, and invariant gating. The change adds no mutable production state, resource ownership, or scheduling algorithm; concurrency review is limited to preservation of existing one/four-worker behavior and the required race suite. No performance benchmark is needed for expansion/compaction/checkpoint/open-time cost curves because none changes.

Full repository tests, lint, build, race checks, independent evidence audit, and focused implementation review are recorded separately as they settle. Their completion is not implied by the scoped results above. No live provider API or deployed behavior was tested. No merge approval is claimed.

## Repository gate results at production commit d7b5eb2c

- `make test` with the repository's `baton_lambda_support` tag passed: 69 packages with tests, no failure markers.
- `make build` passed, including the embedded frontend; the resulting CLI's `--help` smoke passed.
- Full `make race-check` was run, not skipped. `pkg/sync` passed under race instrumentation in 220.513s. The whole gate failed in two unchanged packages: `engine/pebble` panicked in `TestErrorFSEndSyncWindowSweep` while rotating the WAL, and `sync/expand/scc` exceeded `TestNoHang_ManyTwoCycles`'s two-second deadline.
- The Pebble panic reproduced on pristine base b00aa1a567ebc95caa50d1b6ee85b7caf5fa2b32 with the same errorfs test and race instrumentation. The SCC test passed one isolated baseline run and a fixed five-run baseline sample; it failed an isolated PR run. The SCC package has no source changes in this PR. This timing result is not claimed as a passing full race gate or as a proven new product defect.
- Workstation golangci-lint 2.15 reported the unchanged `cap` loop variable in `connectorbuilder.go`. CI-pinned golangci-lint 2.14 reported the unchanged ignored `resp.Body.Close` in `uhttp/error_messages_test.go:892`; that exact pinned-version diagnostic also reproduced on the pristine base. Neither unrelated source file is modified here.
- Technical GitHub CI reached green on this production commit. This does not establish a clean local full race gate, human approval, or merge permission.

Independent criterion/evidence audit and focused implementation review remain separate from these execution results.

## Follow-up proof at d41c9976

With `CGO_ENABLED=0 GOMAXPROCS=4 GOFLAGS='-p=4 -mod=vendor -buildvcs=false'`, the CI-tagged scoped `pkg/sync` command passed in 22.918 seconds at this head:

```sh
go test -tags=baton_lambda_support -count=1 \
  -run 'Test(RootResourceListErrorRetainsPendingPage|ChildResourceListNotFoundCompletesWithWarning|EmptyRootResourceListCompletes|EmptyRootCollectionSyncCompletesArtifact|ActionWarningScope|ChaosConnectorResourcesAndEntitlementsFaultMatrix|LedgerExistingSchedulerWarningCommitsAccounting|SpawnedCursorErrorCategoriesEndToEnd)$' ./pkg/sync
```

This closes the NF-02/NF-03 fixture-content omissions identified by the earlier evidence audit, not the missing real-store deleted-parent warning case noted in PR review. Full repository gates and an independent re-audit of the final proof remain separate from this scoped run. The default root `NotFound` behavior break also requires a downstream rollout note; no format change or unchanged exported API removes that compatibility cost.

## Default-path behavior and release note

This change is an intentional behavior break, despite leaving exported Go signatures and artifact formats unchanged: a connector that returned `codes.NotFound` for a root `ListResources` operation and previously finished with a warning will now fail that sync. An unlicensed, disabled, or unavailable collection endpoint may trigger this path. A connector that has positive evidence of an empty collection should return an empty successful page; it should not translate an unknown failure into success.

Before upgrading a deployed connector to a release containing this fix, its owner should audit root collection `NotFound` handling and verify optional/unlicensed feature behavior with a connector-specific test. Stage SDK upgrades for affected connectors and watch sync failure counts and the newly surfaced NotFound responses. If failures appear, correct the connector's root-list contract and retry the sync; do not roll out the SDK as if this were an artifact-format-only change. Release sequencing and affected deployed connectors remain unverified here, so this note is guidance to the rollout owner, not an assertion that migration is complete.

## Repository gate results at d41c9976

- CI-tagged scoped `pkg/sync` controls passed as recorded above.
- CI-tagged full `make test` passed: no failed packages or panic markers in the retained output.
- `make build` passed, including the embedded frontend, and the newly built `dist/linux_arm64/baton --help` exited successfully.
- Local `make lint` failed on the unchanged `pkg/connectorbuilder/connectorbuilder.go:426` predeclared-identifier finding already recorded above. It is not a clean lint gate, and this change does not touch that package.
- An initial `make race-check` attempt failed before tests because `CGO_ENABLED=0` disables Go's race detector; the command was rerun with `CGO_ENABLED=1`. The full rerun failed in unchanged `pkg/dotc1z/engine/pebble`: `TestErrorFSEndSyncWindowSweep` panicked with `fatal error: sync: unlock of unlocked mutex`. This matches the failure previously reproduced on pristine base. The changed `pkg/sync` passed race instrumentation (244.106 seconds); `pkg/sync/expand/scc` passed in this run. The full race gate remains red.
- An independent final-proof evidence audit and focused implementation review remain pending; full test/build and changed-path race results do not constitute that review.
