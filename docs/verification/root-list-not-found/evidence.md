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
| NF-02 | Verified to stated coverage | ChildResourceListNotFoundCompletesWithWarning: populated parent type/ID, one connector call, nil error, one matching warning, completed action. Ledger warning accounting retains a parent-scoped case. |
| NF-03 | Verified to stated coverage | EmptyRootResourceListCompletes: one successful empty response, no warnings, completed action at one/four workers. |
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
