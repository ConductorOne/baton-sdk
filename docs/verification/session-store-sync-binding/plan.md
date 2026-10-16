# Session store sync binding: verification plan

Written 2026-10-15. Frozen at its first commit; later changes append as change
orders at the bottom.

## Problem

In service mode a connector process runs one `GRPCSessionServer`
(`internal/connector/connector.go:281`) shared by every task. The server holds
one `store` field (`pkg/session/session_server.go:19`). Each syncer overwrites
it from `loadStore` (`pkg/sync/syncer.go:3909`). The runner executes up to
`task-concurrency` tasks at once (default 3, `pkg/field/defaults.go:25`), and a
partial sync is a `FullSyncType` task, so a partial sync started during a full
sync takes over the full sync's session traffic. Requests already carry the
right sync ID (`connectorbuilder` stamps `ActiveSyncId`); the server ignores it
when choosing a store, and neither engine's session store checks that it owns
the sync ID it is given.

Observed consequences, by code reading:

- The full sync's reads miss its own earlier writes (masked for up to 10
  minutes by the connector-side otter cache).
- The full sync's writes land in the partial sync's c1z under the full sync's
  ID and ride along in the partial sync's upload.
- The full sync's `store.*` session stats are recorded on the partial syncer.
- After the partial syncer closes, the full sync's session calls reach a closed
  store: Pebble dereferences a nil `*pebble.DB` (recovered to `codes.Internal`),
  SQLite returns a closed-database error.
- The full sync's `Cleanup` clears against the closed store, so its own early
  rows survive into its upload.
- `GRPCSessionServer.store` is written and read across goroutines without a
  lock.

## Risk routing

Silent (misses look like cache misses; misplaced rows are well-formed),
durable (rows inside an uploaded c1z), schedule-dependent (only under
overlapping tasks). HIGH. Default task concurrency exposes every
session-enabled service-mode connector.

## Scope facts

- The session store is sync-only by design. `connectorbuilder` reaches it only
  from the resource-syncer `List*` paths and `Cleanup`, all carrying
  `ActiveSyncId`.
- The syncer learns its sync ID at `pkg/sync/syncer.go:1004` (new, resumed,
  `WithSyncID`) and `pkg/sync/ledger_sync.go:158` (ledgered skip-full-sync).
  No session-bearing connector call precedes either.
- Lambda does not use `GRPCSessionServer`. Local one-shot uses it for one sync.
- No existing test exercises connector-side gRPC session client →
  `GRPCSessionServer` → syncer c1z.

## Contract

- **B1 Routing.** A session operation carrying sync ID X reaches the store of
  the syncer running sync X, independent of how many other syncs share the
  server, their start order, and whether they are running, finished, failed,
  or closed.
- **B2 Artifact isolation.** A c1z produced by a sync holds no session rows
  under any other sync's ID.
- **B3 Cleanup.** After a successful sync's connector `Cleanup`, its c1z holds
  no session rows under its own ID.
- **B4 Closed sync.** Once a syncer's `Close` returns, no session operation
  touches its store. A request for its sync ID gets a classified gRPC error
  (`FailedPrecondition` or `NotFound`), never `Internal` and never a panic.
- **B5 Drain.** `Close` does not close the store while a session operation for
  that sync is executing. The wait is bounded by the finalize context.
- **B6 Stats attribution.** `store.*` and `connector.*` session stats in a
  sync's stats count only that sync's operations.
- **B7 No fallback.** A request with an empty or unregistered sync ID is
  rejected; it is never served from another sync's store.
- **B8 Unique binding.** Binding a sync ID that is already bound is an error.
- **B9 Single-sync behavior preserved.** Local one-shot, resume of an
  unfinished sync, ledgered and non-ledgered skip-full-sync, and connectors
  without session support behave as before.
- **B10 Race-free.** `-race` reports nothing on the session server, syncer, and
  task packages under overlapping syncs.

## Coverage model

Overlap harness dimensions:

| Dimension | Values |
| --- | --- |
| Engine | Pebble (ledgered), SQLite (token path) |
| Start order | full then partial, partial then full |
| Other sync's outcome | success, failure (`Close` on the error path) |
| Connector-side cache | off (all cells), on (one cell) |

Plus one cell: ledgered skip-full-sync overlapping a full sync (second sync-ID
site).

Reduction: start order is covered for both engines because the bug is
order-sensitive (last attach wins). Cache-on is one cell because the cache
keys by sync ID and only masks reads; the cell exists to catch divergence
between cached and routed state, notably `Clear` invalidation.

## Criteria

| ID | Contract | Instrument | Stage |
| --- | --- | --- | --- |
| V1 | B1 | Overlap harness: interrupted sync reads back its own key after the other sync starts and after it closes | 1 (fails today) |
| V2 | B2 | Overlap harness: reopen each c1z, list session rows per sync ID | 1 (fails today) |
| V3 | B3 | Overlap harness: the interrupted sync's c1z has its rows before `Cleanup`, none after | 1 (fails today) |
| V4 | B4 | Overlap harness: raw gRPC request for the closed sync's ID returns a classified error | 1 (fails today) |
| V5 | B6 | Overlap harness: per-sync session stats equal that sync's own operation counts | 1 (fails today) |
| V6 | B10 | Overlap harness under `-race` | 1 (fails today) |
| V7 | B9 | Single sync through the real server: one-shot read/write/Cleanup, resume, skip-full-sync on both engines, nil session server | 1 (passes before and after) |
| V8 | B5 | Blocking store wrapper holds an in-flight op across `Close`; second case exceeds the finalize context | 2 (needs the binding) |
| V9 | B7 | Server rejects empty and unregistered sync IDs | 2 |
| V10 | B8 | Second bind of a bound sync ID fails | 2 |
| V11 | regression | Existing suites under `-race`: `pkg/session`, `pkg/dotc1z`, `pkg/sync`, `pkg/connectorbuilder`, `pkg/tasks/c1api`, `pkg/tasks/local`, `pkg/cli` | 1 and 2 |

Instrument validation for V1–V6: the harness asserts that the interleaving
happened (the other syncer's `Close` returned while the interrupted sync was
blocked in the connector). A failure caused by setup rather than by the
assertion under test invalidates the run. Oracles read the reopened c1z files
directly, not through the server under test.

## Out of scope

- `MemorySessionCache.Clear` returns before invalidating when the backing
  `Clear` fails.
- `Cleanup` calls `uhttp.ClearCaches`, emptying the connector's HTTP caches for
  every running sync.

## Change orders

### CO1 (clarification): V6 instrument

- Source: running the overlap harness under `-race` on `main`.
- Motivation: the harness reported no race. Every attach in it is ordered by
  channel handoffs, and the race detector treats socket and file I/O as
  synchronization, so the unsynchronized `GRPCSessionServer.store` is never
  observed as concurrent.
- Contract delta: none. B10 unchanged.
- Verification delta: V6 moves to `TestGRPCSessionServerAttachWhileServing`
  (`pkg/session/session_server_test.go`), which attaches and serves from two
  goroutines with no I/O between them. It has no assertion and fails only
  under `-race`; `make test` runs `-race`. It targets `SetSessionStore`; if
  the binding API replaces it, the test moves to the replacement.
- Affected criteria: V6.

### CO2 (clarification): observed closed-store errors

- Source: overlap harness output on `main`.
- Contract delta: none. The problem statement's closed-store consequences,
  as measured:
  - Pebble writes return `ErrEngineClosing`, which carries
    `codes.Unavailable` (`engine_stub.go:31`). By reading, not run: the
    syncer's retryer treats `Unavailable` as retryable
    (`pkg/retry/retry.go:61`), so a connector that returns the session error
    from a `List*` call would be retried against a store that will not
    reopen. B4's classified codes are not retryable.
  - Pebble reads skip the closing check and dereference the nil
    `*pebble.DB`; the recovery interceptor returns `codes.Internal`.
  - SQLite returns `ErrDbNotOpen` as `codes.Unknown`.
- Affected criteria: none.

### CO3 (correction to CO1)

- Source: `.github/workflows/ci.yaml:45`, `Makefile:111-113`,
  `.github/workflows/nightly.yaml`.
- CO1 said `make test` runs `-race`. PR CI runs `go test` without `-race`;
  `make race-check` and the nightly race shards run it. `pkg/session` is in
  the nightly `rest` shard. V6 is therefore enforced nightly, not per PR.
- Affected criteria: V6.

### CO4 (contract amendment): deprecated attach paths

- Source: `BUG_CATCHING.md` §5.8; `pkg/session` and `pkg/sync` are exported.
- Motivation: `GRPCSessionServer.SetSessionStore` and `sync.WithSessionStore`
  taking a `sessions.SetSessionStore` are public API.
- Contract delta:
  - `GRPCSessionServer.SetSessionStore` is deprecated and kept. A store set
    through it serves requests whose sync ID has no binding. A bound sync ID
    is always served by its binding. B7 holds whenever nothing has called the
    deprecated method; no in-repo code calls it.
  - `WithSessionStore` given a setter that does not implement
    `sessions.SessionStoreBinder` attaches through `SetSessionStore`, the
    previous behavior. Every in-repo setter implements the binder.
- Verification delta: `TestGRPCSessionServerDeprecatedSetSessionStore`.
- Affected criteria: V9.

### CO5 (contract clarification): `Clear` for an unbound sync ID

- Source: the existing `Clear` returned success when no store was set.
- Contract delta: B4 and B7 except `Clear`. `Clear` for an unbound sync ID
  returns success and touches no store. An empty sync ID is still rejected.
- Verification delta: the `Clear` case of
  `TestGRPCSessionServerRejectsUnroutableRequests`.
- Affected criteria: V9.

### CO6 (scope): `WithConnectorStore` syncers bind

- Source: the binding moved from `loadStore` to the two places the sync ID is
  set.
- Contract delta: a syncer built with `WithConnectorStore` and
  `WithSessionStore` now binds its store; before, it never attached one. No
  in-repo caller combines the two (`synccompactor`, `baton-compat-harness`,
  `baton rollback-expansion` use `WithConnectorStore` alone).
- Affected criteria: none.

### CO7 (decision): drain past the finalize context

- Source: B5 bounds the wait but does not say what `Close` does when the
  bound is hit. `dotc1z.FinalizeTimeout` is read once from the environment
  and has no test hook.
- Contract delta: when the drain does not finish before the finalize context
  ends, `Close` returns the error and does not close the store. The c1z is
  not finalized; a later `Close` closes it. The binding is already removed,
  so no new request reaches the store.
- Verification delta: V8's second case runs at two levels.
  `TestGRPCSessionServerUnbindDrainTimeout` drives a real deadline through
  the server. `TestSessionBindingCloseKeepsStoreOpenWhenDrainFails` gives the
  syncer an unbind that fails and checks the store still serves reads.
- Affected criteria: V8.

### CO8 (instrument correction): connector-side usage in the overlap harness

- Source: V5 after the fix failed on `connector.set` = 0 for the held sync.
- Motivation: `lazyCachingSessionStore.ensureSession` wraps the connector's
  backend in `NewInstrumentedSessionStore`, which produces the usage
  annotations behind `connector.*`. The harness skipped it, so every sync
  reported zero. Stage 1 V5 stopped at `store.set` and never reached the
  `connector.*` assertions.
- Verification delta: the harness wraps the backend the same way. Instrument
  revalidation: the stage 1 suite rerun with the corrected harness against
  unchanged production code fails V1–V5 with the same messages as stage 1.
- Affected criteria: V5.

### CO9 (contract addition): bind failure

- Contract delta: **B11.** A sync whose bind fails returns the error before
  any resource-syncer call, on both sync ID sites.
- Verification delta: **V12** `TestSessionBindingBindFailureFailsSync`
  (Pebble, SQLite, ledgered skip-full-sync).

### CO10 (contract amendment, supersedes CO4): old attach API removed

- Source: the only consumers that matter are this repo and the c1 monorepo.
  c1 at `4f77224cc4e` has no caller of `SetSessionStore`,
  `GRPCSessionServer`, the setter interfaces, or `sync.WithSessionStore`; it
  uses only the session client side (`NewGRPCSessionStore`,
  `MemorySessionCache`, `sessions.SessionStore`, `WithSyncID`,
  `WithPrefix`).
- Contract delta: removed `sessions.SetSessionStore`,
  `session.SetSessionStore`, `GRPCSessionServer.SetSessionStore`,
  `GRPCSessionServer.Validate`, and `connectorClient.SetSessionStore`.
  `sync.WithSessionStore` takes a `sessions.SessionStoreBinder`. The internal
  `SetSessionStoreSetter` is now `SessionServerSetter.SetSessionServer`. B7
  holds without exception.
- Verification delta: `TestGRPCSessionServerDeprecatedSetSessionStore` is
  removed. V6 binds, unbinds, and serves concurrently.
- Affected criteria: V6, V9.

### CO11 (contract amendment, supersedes CO7): bounded drain, then close

- Source: review of the branch. CO7 bounded the drain by
  `dotc1z.FinalizeTimeout` (1 hour), which caps the c1z commit in `Close`,
  not session requests against a local store. When that bound was hit, the
  store stayed open, and no production caller runs a second `Close`, so the
  store leaked for the life of the process. Closing after the drain would
  also have run `store.Close` on an expired context.
- Contract delta: **B5** is now: `Close` waits up to `sessionDrainTimeout`
  (30 seconds) for session requests already running against the sync's
  store, then closes the store. If requests are still running, `Close` logs
  a warning and does not return an error; those requests get an error from
  the closed store. The commit keeps the full finalize budget.
- Also from the review: `WithSessionStore` is now `WithSessionStoreBinder`;
  the unused internal `SessionServerSetter` interface is removed.
- Verification delta: `TestSessionBindingCloseKeepsStoreOpenWhenDrainFails`
  is replaced by `TestSessionBindingCloseClosesStoreWhenDrainFails`, which
  checks that `Close` succeeds, the drain context has a deadline no later
  than `sessionDrainTimeout`, and the c1z is committed.
- Affected criteria: V8.

### CO12 (verification addition): sequential syncs on one server

- Source: review question about a service-mode connector that finishes a
  task, idles, and starts the next. c1api creates a new temp c1z per task,
  so each task has a new sync ID, while the session server, the connector
  client, and the connector's `MemorySessionCache` live for the whole
  process. No criterion ran more than one completed sync through one
  server and one connector cache.
- Contract delta: none.
- Verification delta: `TestSessionBindingSequentialSyncs` runs three
  syncs one after another on one harness with the connector cache on, for
  both engines. Each sync reads `k` before writing it and must find
  nothing, then writes `k` and must read back its own value. Every finished
  sync ID must get a classified error from a raw request, and no c1z may
  hold session rows for any of the three sync IDs.
- Affected criteria: B2, B3, B5, B9.
