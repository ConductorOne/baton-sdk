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
