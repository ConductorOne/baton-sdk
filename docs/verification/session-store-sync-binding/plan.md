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

None.
