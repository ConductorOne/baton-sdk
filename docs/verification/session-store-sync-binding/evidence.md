# Session store sync binding: evidence

Stage 1: failing evidence on unchanged production code. Base `main` at
`613aeb2e`; tests at `0bae9598`. No production file differs from `main`.

## Commands

```bash
go test -race -count=1 -run 'TestSessionBinding' -v ./pkg/sync/
go test -race -count=1 -run TestGRPCSessionServerAttachWhileServing ./pkg/session/
go test -race -count=1 -timeout=45m \
  -skip 'TestSessionBindingOverlap|TestGRPCSessionServerAttachWhileServing' \
  ./pkg/session/... ./pkg/dotc1z/... ./pkg/sync/... ./pkg/connectorbuilder/... \
  ./pkg/tasks/... ./pkg/cli/... ./internal/connector/...
```

## Criteria

| ID | Status | Artifact | Cells |
| --- | --- | --- | --- |
| V1 | failed (expected) | `TestSessionBindingOverlap/*/V1_held_reads_own_writes`, `TestSessionBindingOverlapSkipSync` | 9 of 9 overlap cells, skip cell |
| V2 | failed (expected) | `.../V2_no_foreign_rows` | 9 of 9 |
| V3 | failed (expected) | `.../V3_held_cleanup_reaches_own_store`, `TestSessionBindingOverlapSkipSync` | 9 of 9, skip cell |
| V4 | failed (expected) | `.../V4_closed_sync_rejected` | 9 of 9 |
| V5 | failed (expected) | `.../V5_stats_attribution` | 4 Pebble cells without cache |
| V6 | failed (expected) under `-race`, 5 of 5 runs | `TestGRPCSessionServerAttachWhileServing` | n/a |
| V7 | verified at `0bae9598` | `TestSessionBindingSingleSync`, `TestSessionBindingResume`, `TestSessionBindingSkipFullSync`, `TestSessionBindingWithoutSessionSupport` | Pebble and SQLite each |
| V8 | deferred to stage 2 | | |
| V9 | deferred to stage 2 | | |
| V10 | deferred to stage 2 | | |
| V11 | baseline recorded; one failure on `main`, outside scope | suites listed in the plan | |

Reductions and exclusions:

- V5 excludes SQLite: the syncer records stats only on Pebble
  (`pkg/sync/syncer.go:944`). V5 excludes the connector-cache cell: cached
  reads never reach the parent, so `store.get` is lower by design.
- V5 asserts the runner's counts only when the runner succeeds; a failed sync
  has no completed stats to read.

## Instrument validation

- Setup checks held in every overlap cell: the runner's `Sync` result matched
  the cell's outcome, the held sync had not returned before its final
  release, the held sync succeeded, and the two sync IDs differ. No failure
  came from a setup check.
- Each criterion failed by the mechanism the plan predicts:
  - V1: the held sync's writes after the runner closed failed with
    `ErrEngineClosing` (`codes.Unavailable`) on Pebble and `ErrDbNotOpen`
    (`codes.Unknown`) on SQLite.
  - V2: the runner's c1z held the held sync's `mid` row, written while the
    runner was attached.
  - V3: the held sync's c1z still held its `pre` row after its `Cleanup`.
  - V4: a request for the closed runner's sync ID returned `codes.Internal`
    on Pebble (nil `*pebble.DB` in `SessionGet`, recovered by
    `RecoveryUnaryInterceptor`) and `codes.Unknown` on SQLite.
  - V5: the held sync's `store.set` was 1, expected 3; only its write before
    the runner attached was counted on its own syncer.
  - V6: the race detector reported the write at `session_server.go:31`
    (`SetSessionStore`) against the read at `session_server.go:35`
    (`Validate`).
- The overlap harness under `-race` reported no race (CO1). It is not an
  instrument for V6.

## V11 baseline

All listed packages passed under `-race` except `pkg/dotc1z`:
`TestC1ZConcurrentClose` reported a race between `C1File.validateDb`
(`c1file.go:1506`) and `C1File.closeRawDB` (`c1file.go:846`), 2 of 2 runs.
This branch does not touch `pkg/dotc1z`. PR CI does not run `-race`
(`.github/workflows/ci.yaml:45`). Stage 2 compares against this baseline.

# Stage 2

Fix and remaining criteria on top of `9b4350dd`. Change orders CO5–CO10 in
the plan apply; CO10 supersedes CO4.

## Commands

```bash
go test -race -count=1 -run 'TestSessionBinding' ./pkg/sync/
go test -race -count=1 -run 'TestGRPCSessionServer' ./pkg/session/
go test -race -count=1 -timeout=45m \
  ./pkg/session/... ./pkg/dotc1z/... ./pkg/sync/... ./pkg/connectorbuilder/... \
  ./pkg/tasks/... ./pkg/cli/... ./internal/connector/...
golangci-lint run ./pkg/session/... ./pkg/sync/... ./internal/connector/... ./pkg/types/sessions/...
go vet ./...
```

## Criteria

| ID | Status | Artifact |
| --- | --- | --- |
| V1–V4 | verified, 9 of 9 overlap cells and the skip cell | `TestSessionBindingOverlap`, `TestSessionBindingOverlapSkipSync` |
| V5 | verified, 4 Pebble cells without cache, `store.*` and `connector.*` | `.../V5_stats_attribution` (harness corrected, CO8) |
| V6 | verified under `-race` | `TestGRPCSessionServerAttachWhileServing`, now binding, unbinding, and serving at once |
| V7 | verified | unchanged tests, Pebble and SQLite |
| V8 | verified | `TestGRPCSessionServerUnbindDrains`, `TestGRPCSessionServerUnbindDrainTimeout`, `TestSessionBindingCloseDrainsInFlight` (both engines), `TestSessionBindingCloseClosesStoreWhenDrainFails` (both engines, CO11) |
| V9 | verified, all eight handlers | `TestGRPCSessionServerRejectsUnroutableRequests` |
| V10 | verified | `TestGRPCSessionServerBindRejects` |
| V11 | matches the baseline: every package passes except the pre-existing `TestC1ZConcurrentClose` race in `pkg/dotc1z` (`c1file.go:1506`), the only race reported | suites listed above |
| V12 | verified | `TestSessionBindingBindFailureFailsSync` |

## Instrument validation

Each mutation was applied alone to the fixed code; the listed tests failed
and the mutation was reverted.

| Mutation | Caught by |
| --- | --- |
| `acquire` skips `inflight.Add` | `UnbindDrains`, `UnbindDrainTimeout` |
| `unbind` leaves the binding in the map | `UnbindDrains`, `UnbindDrainTimeout`, `BindRejects` |
| unbound sync ID served by any bound store | `RejectsUnroutableRequests` (all eight handlers) |
| bind overwrites an existing binding | `BindRejects`, `UnbindDrains` |
| unbind without `sync.Once` | `BindRejects`, `UnbindDrains` |
| `Clear` rejects unbound sync IDs | `RejectsUnroutableRequests/Clear`, `UnbindDrains` |
| empty sync ID not rejected | `RejectsUnroutableRequests` (all eight handlers) |
| unbind does not wait | `UnbindDrains`, `UnbindDrainTimeout` |
| `acquire` reads the map without the lock | V6, data race in 3 of 3 runs |
| `Close` closes the store after a failed drain | `CloseKeepsStoreOpenWhenDrainFails/pebble` |
| `Close` does not wait for unbind | `CloseDrainsInFlight`, `CloseKeepsStoreOpenWhenDrainFails` (both engines) |
| no bind in `skipLedgerSync` | `BindFailureFailsSync/pebble-skip-full-sync` |
| bind error ignored in `Sync` | `BindFailureFailsSync/pebble`, `/sqlite` |
| no bind in `Sync` | V1, V5, V7, V8, V12 cells |
| `Close` never unbinds | V4 (all cells), V8, `TestSessionBindingResume` |
| bound store not instrumented | V5 (4 cells) |

`TestSessionBindingOverlapSkipSync` passes with the `skipLedgerSync` bind
removed: an unbound skip sync no longer disturbs the held sync. V12 is the
check for that site.

The first V11 run failed `TestGRPCSessionServerUnbindDrains` once: its probe
`Set` can land before the unbind goroutine removes the binding, and that
`Set` correctly reaches the store. The test now counts probes served before
the unbind. After the change it passed 900 runs under `-race` (`-count=300
-cpu 1,2,8`), and the `acquire`, `unbind`, and no-wait mutations still fail
it. `TestSessionBinding*` passed 5 of 5 runs under `-race`.

Test names in the table drop the `TestGRPCSessionServer` and
`TestSessionBinding` prefixes.

## CO10 rerun

After removing the old attach API: `go build ./...`, `go vet ./...`, and
`golangci-lint` on the changed packages are clean. Under `-race`,
`pkg/session`, `pkg/tasks/...`, `pkg/cli`, and the session tests in
`pkg/sync` pass. V6 still reports a data race in 3 of 3 runs with the
`acquire` lock removed.

## CO11 rerun

`TestSessionBinding*` pass under `-race`; `pkg/session`, `pkg/tasks/...`,
and `internal/connector` pass under `-race`; `golangci-lint` on the changed
packages is clean. Each mutation below was applied alone and caught:

| Mutation | Caught by |
| --- | --- |
| drain uses the finalize context instead of `sessionDrainTimeout` | `CloseClosesStoreWhenDrainFails` (both engines) |
| store left open when the drain fails | `CloseClosesStoreWhenDrainFails` (both engines) |
| drain error returned from `Close` | `CloseClosesStoreWhenDrainFails` (both engines) |
| `Close` does not wait for the drain | `CloseDrainsInFlight` (both engines) |

This supersedes the two stage 2 mutation rows that assumed `Close` leaves
the store open after a failed drain.

## CO12

`TestSessionBindingSequentialSyncs` passes 3 of 3 runs under `-race` on
both engines; `golangci-lint` on `pkg/sync` is clean. Each mutation below
was applied alone and caught on both engines:

| Mutation | Failing assertion |
| --- | --- |
| `Close` does not release the binding | raw request for a finished sync returns `Internal` |
| `MemorySessionCache` key omits the sync ID | syncs 2 and 3 read the previous sync's `k` before writing |
| server `Clear` does not call the bound store | `sync-0.c1z` holds session rows for its own sync ID |
