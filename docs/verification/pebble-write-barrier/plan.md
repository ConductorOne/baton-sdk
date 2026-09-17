# Plan: one write lock for the Pebble engine

Branch `kans/pebble-write-barrier`, base `bce1fd57`. Frozen before
implementation; later changes are versioned change orders at the end.
`lock-inventory.md` beside this file is the input.

## 1. Problem

The engine answers "may this goroutine mutate the DB right now?" with seven
primitives: `writeWG`, `writeMu`, `closing`, `closeMu`, `sealed`, `sealMu`,
`checkpointMu`. Three more guard state that only lifecycle transitions
change and only writers read: `currentSyncMu`, `synthLayerMu`, and the
`sealed`/`sealMu` pair again. Each was added against one finding. The
`writeWG` one is wrong outright (`sync.WaitGroup` forbids `Add` concurrent
with `Wait`, and nothing orders them), and it has been redundant with
`writeMu` since the first Pebble commit (#874): every `Add` site except
`CompactAllRanges`/`Flush` also takes `writeMu`, and both `Wait` sites take
`writeMu` immediately after waiting.

## 2. Model

Two facts hold today and define the target:

- **F1. Engine mutations are single-writer.** `writeMu` already serialises
  every DB mutation the engine's own goroutines make, except the synth-layer
  worker's `IngestSSTs` (fenced by `checkpointMu`) and the compactor's raw
  `DB()` writes in another package (fenced by call order; out of scope).
- **F2. Lifecycle transitions are exclusive with writes.** Bind, seal,
  close, and the checkpoint window each want "no writer is mid-mutation and
  none can start."

Target: `writeMu` is the one lock for F1 and F2. Transitions take it; the
state they change (`db`, the sync binding, `sealed`, the synth-layer
session) is read by writers under it and published to lock-free readers as
one immutable snapshot. The engine's own goroutines never mutate the DB
outside `writeMu`. `lifecycleMu` stays: it serialises transitions against
each other across their read-check-write over the sync-run record, which is
a different obligation from excluding writers. `entIDLookupMu`, the two
stats stashes, `poisonLogMu`, and the compaction scheduler's lock are not
part of this question and are untouched.

## 3. Change

### 3.1 Delete

| Primitive | Replaced by |
| --- | --- |
| `writeWG` | `writeMu` (every former `Add` site already holds it or now does) |
| `closing`, `closeMu` | `e.db == nil` under `writeMu`; `Close` nils `db` under `writeMu`, second `Close` sees nil |
| `sealed`, `sealMu` | `syncBinding.sealed` in the snapshot; `seal`/`unseal` replace the snapshot under `writeMu` |
| `currentSyncMu` | snapshot for `id`/`fresh`; `fresh*Empty` become plain fields read and written under `writeMu` |
| `synthLayerMu` | `synthLayer` pointer read and written under `writeMu` (`Begin`/`Abort` take it; `Close` already holds it) |
| `checkpointMu` | gone once the synth worker no longer mutates the DB (3.3) |

### 3.2 Lifecycle snapshot

```go
type syncBinding struct { id []byte; fresh bool; sealed bool }
binding atomic.Pointer[syncBinding]   // never nil; replaced only under writeMu
```

`CurrentSyncID`, `IsFreshSync`, `IsSealed`, `requireCurrentSync`,
`currentSyncBytes` load it. `bindCurrentSync`, `MarkFreshSync`,
`clearCurrentSync`, `seal`, `unseal` take `writeMu` and replace it; each has
a `Locked` form for callers already holding `writeMu` (`FinishSync`'s
closure calls `clearCurrentSyncLocked`).

### 3.3 Barrier

- `withWriteAllowSealed`: `writeMu.Lock`; one check (`db == nil` →
  `ErrEngineClosing`, `readOnly` → error); run `fn`. `withWrite` adds the
  `sealed` check from the snapshot, once, under the lock. The pre-lock checks
  and the under-lock re-checks collapse to the single under-lock check.
- `Close`: `writeMu.Lock`; `if db == nil return nil`; abort synth layer
  (locked form); flush; `db.Close`; `db = nil`.
- `CheckpointTo`: `writeMu.Lock`; `if db == nil return ErrEngineClosing`;
  Flush→Checkpoint→truncate. No `Wait`, no `checkpointMu`.
- `CompactAllRanges`, `Flush`: `writeMu.Lock` then `checkWritable`. They
  exclude concurrent writers for their duration; neither has a caller on the
  sync path (inventory §1.5).
- Synth layer: the worker merges chunks to an SST and appends the path to
  `session.ready` under `segMu`. `Add`, `Finish` (under `writeMu`) take
  `ready` and `IngestSSTs` each path in order before their own work; `Finish`
  drains after `segWG.Wait`. `Abort` discards `ready` with the staging dir.
  Rows become visible at the next `Add` or at `Finish`; the documented
  contract (visible by `Finish`) is unchanged.
- `deleteGrantsByIdentities`: the per-chunk `sealed` check goes; `seal`
  cannot flip while the call holds `writeMu`. The per-chunk `ctx` check
  stays.

### 3.4 Order after the change

```
pebbleStore.closeMu → lifecycleMu → writeMu → leaves         (store-driven lifecycle)
pebbleStore.closeMu → writeMu                                (save → CheckpointTo, Engine.Close)
                      writeMu → {entIDLookupMu, computedStatsMu, deferredGrantStatsMu, poisonLogMu, segMu}
```

`lifecycleMu` is never taken under `writeMu` (unchanged). The synth worker
takes `segMu` only. No `Locked` function takes `writeMu`; no function
holding `writeMu` calls one that takes it.

### 3.5 Behaviour changes to name in the commit

- A manual `CompactAllRanges`/`Flush` blocks concurrent writers.
- `seal` waits for an in-flight write instead of letting it observe the
  flip mid-call.
- A write arriving during `Close`'s teardown blocks until teardown finishes,
  then returns `ErrEngineClosing`, instead of returning immediately.
- Likewise for `ErrEngineSealed`: `withWrite` had a lock-free `sealed` check
  before taking `writeMu`; now the check is under the lock only. A record
  write issued during `EndSync` parks behind `BuildDeferredGrantIndexes`
  (~1m45s on the whale fixture) and then gets the same refusal. No in-repo
  caller writes during `EndSync` (workers are joined first); an embedder's
  goroutine would. Record `Put*` paths on `pebbleStore` do not hold
  `closeMu` across the engine call, so the park does not propagate there.
  Not restored: the two-check shape is the one this change removes, and the
  refusal is unchanged.
- Synth-layer rows become visible at the next `Add`/`Finish` instead of when
  the worker's ingest lands.

## 4. Risk

Failure modes of the change: a deadlock (silent, schedule-dependent → two
escape axes → HIGH); a lost exclusion (a write landing inside the checkpoint
window is a WAL-only row the truncate discards → silent, durable → HIGH); a
lifecycle read that sees a torn state. Step-up applies. Remediation is
redeploy for the first, re-sync for the second.

## 5. Criteria

| ID | Property | Instrument | Coverage claim |
| --- | --- | --- | --- |
| C1 | No `sync.WaitGroup` misuse panic and no deadlock under concurrent writers, `CheckpointTo`, seal/unseal, and `Close` from arbitrary goroutines | `TestWriteBarrierSoak`: N writers, one checkpointer, one sealer, one closer, fixed window, `-race`, `-count` | measured sampling |
| C2 | `Close` and `CheckpointTo` do not proceed while a writer or `CompactAllRanges`/`Flush` is inside `writeMu` | directed: a closure parked on a channel inside `withWriteAllowSealed`; `Close`/`CheckpointTo` observed not returned before release, returned after | bounded |
| C3 | A write that starts after `Close` began returns `ErrEngineClosing`, never a nil-`db` panic | directed: write parked behind a `Close` that is itself parked behind a holder; planted violation: delete the `db == nil` check → nil deref | bounded |
| C4 | A write that starts after `seal` began returns `ErrEngineSealed`; sealed-lifecycle writes (`withWriteAllowSealed`) still pass | existing `session_sealed_test.go`, `compaction_pause_lifecycle_test.go`, `grant_batch_delete_test.go`; plus C1's sealer | bounded + sampled |
| C5 | No row commits inside `CheckpointTo`'s Flush→Checkpoint→truncate window; a synth-layer segment cannot land there | existing `checkpoint_wal_test.go`, `checkpoint_wal_envelope_test.go`, `synth_layer_session_test.go`; C7 statically | bounded (static) |
| C6 | Lifecycle readers (`CurrentSyncID`, `IsFreshSync`, `IsSealed`) never observe a torn (id, fresh, sealed) triple | by construction (one pointer swap); `-race` on the suite; `fast_path_proof_verification_test.go` updated to the snapshot | bounded |
| C7 | Every DB mutation the engine makes is inside `writeMu`: rawdb mutating calls appear only lexically inside a `withWrite*` closure, in a `*Locked` function, or in {`Close`, `CheckpointTo`, `CompactAllRanges`, `Flush`}; `*Locked` functions and the allowlisted holders are never called from a `writeMu` holder's transitive callees except each other; nothing that takes `writeMu` is reachable from a holder | `TestWriteMuHolders` (AST, non-test files); planted violations: a rawdb call outside a holder, a `withWrite` call inside a closure | bounded (static, in-package; method calls through other receivers not followed, stated in the test) |
| C8 | Synth-layer rows all present after `Finish`; `Abort` leaves already-ingested rows and no staging dir | existing `synth_layer_session_test.go` | bounded |
| C9 | Existing semantics: `ErrEngineClosing` code, read-only `CheckpointTo` copy path, `Close` idempotent, leak oracle at `Close` | existing package suite | bounded |
| C10 | Suites: `go test ./pkg/dotc1z/engine/pebble/`, `go test ./pkg/dotc1z/`, `go test -race ./pkg/dotc1z/engine/pebble/`, `golangci-lint run pkg/dotc1z/...` | repository gates | — |
| C11 | Cost: moving the synth-layer segment ingest from the worker onto the producer's `Add`/`Finish` under `writeMu` does not slow expansion measurably (`docs/BUG_CATCHING.md` §2 cost contract) | `TestRunWhalePebbleProjectionExpansion` on the whale seed, base vs branch, back-to-back on one machine; compare the expansion phase and an untouched phase (`EndSync` deferred index) as the noise reference | measured, one seed |

Structural coverage over changed files after C1–C9 run; every uncovered
changed branch dispositioned in `evidence.md`.

## 6. Reproducer honesty

C1's soak is also the reproducer for the original `writeWG` panic at
`bce1fd57`. The panic needs an `Add` from zero while a waiter is parked, a
window of a few instructions; the soak is run against the base first and its
result recorded whichever way it falls. Failing to trip it does not weaken
the fix, which removes the primitive rather than the window.

## 7. Change orders

None yet.
