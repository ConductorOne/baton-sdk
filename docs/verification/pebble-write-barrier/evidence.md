# Evidence: one write lock for the Pebble engine

Against `plan.md` §5. Base `bce1fd57`; runs on darwin/arm64, Go 1.26.1.
Rebased onto `2d04d337` (#1133), which renamed `EndFreshSync` to
`FinishSync`; the pebble, dotc1z, and sync suites were rerun with `-race`
after the rebase and passed. Rows below use the name current at the time.

## Base (before the change)

`TestWriteBarrierSoak` at the base, 8 writers + checkpointer + sealer for
300ms, then `Close`:

- `go test -count=1`: no panic; writes=119375, seals=675575,
  **checkpoints=0**. `writeWG.Wait()` never observes zero under continuous
  writers, so `CheckpointTo` starves for the whole window. A second defect
  of the WaitGroup-as-barrier design, not named in the brief.
- `go test -race -count=10`: `WARNING: DATA RACE` on the first run — the
  race detector instruments `sync.WaitGroup` and reports `Wait`
  (`engine.go:967`, `CheckpointTo`) concurrent with `Add` (`engine.go:794`,
  `withWriteAllowSealed`). This is the misuse the brief describes, surfaced
  as a race report rather than the panic. 1 of 10 runs completed a
  checkpoint.

## After

| ID | Result |
| --- | --- |
| C1 | `TestWriteBarrierSoak`: `-race -count=3` and `-count=5` pass; per run writes≈500, checkpoints≈150, seals≈60 under `-race` (the checkpointer now gets the lock). No panic, no race, no hang |
| C2 | `TestCloseAndCheckpointWaitForWriter`: `Close` and `CheckpointTo` both parked for 50ms behind a holder, both returned after release. `-race -count=5` |
| C3 | `TestWritesAfterCloseReturnErrEngineClosing`: queued write lands or is refused; every entry point (`PutResourceTypeRecord`, `withWriteAllowSealed`, `CheckpointTo`, `Flush`, `CompactAllRanges`, `BeginSynthesizedGrantLayer`) returns `ErrEngineClosing` after `Close`; `Abort` and a second `Close` return nil. Planted violation: removing the `db == nil` check in `checkWritableAllowSealedLocked` → `nil pointer dereference` in the test. Restored |
| C4 | `TestDeleteGrantsByRefsCompletesBeforeSeal` (rewritten from `...StopsWhenSealedMidCall`, which pinned the old mid-call flip and now self-deadlocks by construction): seal requested at chunk boundary 2 does not return for 50ms, `IsSealed` stays false under the writer, all chunks commit, seal lands after. Existing `session_sealed_test.go`, `compaction_pause_lifecycle_test.go` pass in the suite |
| C5 | `checkpoint_wal_test.go`, `checkpoint_wal_envelope_test.go`, `synth_layer_session_test.go` pass; C7 covers the ingest statically |
| C6 | `-race` on the whole engine suite clean; `fast_path_proof_verification_test.go` updated to read the bits under `writeMu` |
| C7 | `TestWriteMuHolders` passes on the tree. Planted violations, each detected and restored: a bare `writeMu.Lock()` in a new method (rule 4, fatal, masks the rest so planted separately); `e.db.MetaSet` in `IsSealed` (rule 1); `e.clearCurrentSyncLocked()` in `IsSealed` (rule 3); `e.seal()` inside `EndFreshSync`'s `withWriteAllowSealed` closure (rule 2). The tree needed no allowlist beyond `merge_surface.go`: the 19 first-pass findings resolved to local closures invoked synchronously (`flush := func…`), Open-time functions (reached only from `Open`), and helpers every caller of which holds the lock — all three are now computed, not listed |
| C8 | `synth_layer_session_test.go` passes, including the worker-error cases (`setErr` via `e.synthLayer` directly) |
| C9 | engine suite `ok` 141.5s |
| C10 | `go test ./pkg/dotc1z/engine/pebble/` ok; `go test ./pkg/dotc1z/ ./pkg/synccompactor/...` ok (317s, 376s, 18s, 100s); `go test -race ./pkg/dotc1z/engine/pebble/` ok 142s; `golangci-lint run ./pkg/dotc1z/...` 0 issues |

## Not done

- Structural coverage over the changed files was not measured; the plan
  called for it. The changed branches are the lock acquisitions and the
  `db == nil` / `sealed` checks, each of which has a directed test above, but
  the uncovered-branch disposition step was skipped.
- `TestWriteMuHolders` is syntactic. It does not follow a mutation through a
  local alias of `e.db`, nor through a helper type that stores the engine
  and mutates from its own methods; `newSourceCacheDeleteBatch` and
  `newGrantDigestFold` are covered only because they are constructed under
  the lock.
- The compactor's `merge_surface.go` writes remain outside `writeMu` by
  design (fenced by call order in `pkg/synccompactor`). Unchanged by this
  branch and still the one documented bypass.
