# Lock inventory: the sync path at `bce1fd57`

Every synchronization primitive live while a sync task runs, from the task
runner down to the Pebble engine. Primitives are grouped by layer; each entry
names what it guards, who acquires it, how long it is held, and what may be
held around it. Line numbers are at `bce1fd57` (`origin/main`).

Scope: `pkg/connectorrunner`, `pkg/tasks`, `pkg/uhttp`, `pkg/ratelimit`,
`internal/connector`, `pkg/sync` (with `expand`, `expand/scc`, `progresslog`),
`pkg/dotc1z` (store layer, `c1zstore`, `format/v3`), `pkg/synccompactor`,
`pkg/session`, and `pkg/dotc1z/engine/pebble` (with `internal/rawdb`).
`*_test.go` and `vendor/` are excluded except where a test enforces an order.

Method: `rg` over the non-test tree for `sync.{Mutex,RWMutex,WaitGroup,Once,
Cond,Map,Pool}`, `atomic.*`, `semaphore.`, `errgroup.`, `singleflight`,
`make(chan`, then a read of each acquire site.

## 1. Pebble engine (`pkg/dotc1z/engine/pebble`)

### 1.1 The write barrier and its neighbours

| Primitive | Type | Guards | Acquirers | Hold |
| --- | --- | --- | --- | --- |
| `Engine.writeWG` (`engine.go:98`) | `sync.WaitGroup` | in-flight writes; `Close` and `CheckpointTo` `Wait` on it | `Add(1)`: `withWriteAllowSealed` (`engine.go:794`, and via it every `withWrite` caller — 58 closure sites across 21 files), `CompactAllRanges` (`cleanup.go:166`), `Flush` (`cleanup.go:216`). `Wait`: `Close` (`engine.go:420`), `CheckpointTo` (`engine.go:967`) | For the whole write, including the closure body; for `CompactAllRanges`, across every `db.Compact` |
| `Engine.writeMu` (`engine.go:99`) | `sync.Mutex` | serialises record writes; `Close` teardown; `CheckpointTo` window | `withWriteAllowSealed` (`engine.go:801`), `Close` (`engine.go:434`), `CheckpointTo` (`engine.go:972`) | Whole closure body. Long holders: `BuildDeferredGrantIndexes` (`deferred_index.go:278`, O(grants) scan + `IngestAndExcise`), `deleteGrantsByIdentities` (`grants.go:1030`, one lock for the whole delete), bulk-import ingest, digest build, `ResetForNewSync` |
| `Engine.closing` (`engine.go:100`) | `atomic.Bool` | post-`Close` flag | `Store`: `Close` (`engine.go:419`). `Load`: `checkWritableAllowSealed` (`engine.go:752`), the re-check after each `Add` (`engine.go:798`, `cleanup.go:168`, `cleanup.go:218`), `CheckpointTo` (`engine.go:969`, `engine.go:978`) | — |
| `Engine.closeMu` (`engine.go:101`) | `sync.Mutex` | makes `Close` idempotent; serialises concurrent `Close` | `Close` (`engine.go:414`) | Whole of `Close`: `Wait`, `AbortSynthesizedGrantLayer`, `writeMu`, `db.Close` |
| `Engine.checkpointMu` (`engine.go:178`) | `sync.RWMutex` | `CheckpointTo`'s Flush→Checkpoint→WAL-truncate window against DB mutations that bypass `writeMu` | `Lock`: `CheckpointTo` (`engine.go:974`), under `writeMu`. `RLock`: the synth-layer worker's `ingestSynthLayerSegment` (`grants.go:586`), around one `IngestSSTs` | `CheckpointTo`: the whole window. Worker: one ingest |
| `Engine.lifecycleMu` (`engine.go:64`) | `sync.Mutex` | sync-lifecycle transitions (read-check-write over the sync-run record and `currentSync`) | `startNewSync` (`adapter.go:100`), `ResumeSync` (`:150`), `SetCurrentSync` (`:200`), `CurrentSyncStep` (`:221`), `CheckpointSync` (`:239`), `EndSync` (`:262`) | Held across engine writes: `ResetForNewSync`, `PutSyncRunRecord`, and the whole `endSyncFinalize` (deferred index build, stats sidecar, `EndFreshSync`) |
| `Engine.sealMu` (`engine.go:197`) | `sync.Mutex` | the (`sealed`, compactions-paused) pair transition | `seal` (`engine.go:524`), `unseal` (`:532`) | Two stores; leaf |
| `Engine.sealed` (`engine.go:192`) | `atomic.Bool` | post-`EndSync` state; record writes refuse | `Store` under `sealMu`. `Load`: `checkWritable` (`engine.go:742`), `withWrite` before and under `writeMu` (`engine.go:768`, `:778`), `IsSealed`, `deleteGrantsByIdentities` per chunk | — |
| `Engine.currentSyncMu` (`engine.go:74`) | `sync.RWMutex` | `currentSync`, `freshSync`, `fresh*Empty` | `bindCurrentSync` (`engine.go:467`), `MarkFreshSync` (`:558`), `clearCurrentSync` (`:576`), `IsFreshSync` (`:588`), `takeFresh*Empty` (`:634`, `:644`, `:654`), `EndFreshSync` (`:674`), `currentSyncBytes` (`:700`), `CurrentSyncID` (`:715`), `requireCurrentSync` (`:727`) | Field access; leaf. Taken under `lifecycleMu` and under `writeMu` (`requireCurrentSync` inside closures) |
| `Engine.synthLayerMu` (`engine.go:158`) | `sync.Mutex` | the `synthLayer` pointer only | `BeginSynthesizedGrantLayer` (`grants.go:499`), `loadSynthLayer` (`:514`), `takeSynthLayer` (`:523`) | Pointer handoff; leaf. Taken under `writeMu` (Add, Finish) and under `closeMu` (Close → Abort), and with nothing held (Abort from the driver) |
| `Engine.computedStatsMu` (`engine.go:108`) | `sync.Mutex` | `computedStats` map | `StashComputedSyncStats` (`sync_stats_sidecar.go:302`), `takeStashedSyncStats` (`:312`) | Map access; leaf |
| `Engine.deferredGrantStatsMu` (`engine.go:115`) | `sync.Mutex` | `deferredGrantStats` | `stashDeferredGrantStats` (`sync_stats_sidecar.go:243`), `takeDeferredGrantStats` (`:252`) | Field access; leaf |
| `Engine.entIDLookupMu` (`engine.go:208`) | `sync.Mutex` | lazy `entIDLookup` map and its generation | `entitlementIdentitiesForExternalID` (`lookup.go:85`) | Held across `buildEntitlementIDLookup`, an O(entitlements) keyspace scan. Leaf (the scan is reads only). Reached from `lookup.go:154`, `:177`, `:316` |
| `Engine.entIDLookupGen` (`engine.go:207`) | `atomic.Uint64` | invalidation counter | `noteEntitlementKeyspaceWrite` after every entitlement-keyspace mutation | — |
| `Engine.grantDigestBuildPending`, `grantDigestAbiStale` (`engine.go:134`, `:145`) | `atomic.Bool` | digest fail-safe flags | digest build arm/complete; read-only open; root getters | — |
| `Engine.expandedWrite*`, `synthesizedWrite*` (`engine.go:218-221`) | `atomic.Int64` | counters | expansion write paths | — |
| `poisonLogMu` (`engine.go:364`, local to `Open`) | `sync.Mutex` | poison-log dedup set | the poison observer closure, on the committing goroutine | Map access; leaf |

Lock order as it exists today, outermost first:

```
closeMu  →  writeWG.Wait  →  synthLayerMu (Abort)  →  writeMu                (Close)
            writeWG.Wait  →  writeMu  →  checkpointMu.Lock                    (CheckpointTo)
lifecycleMu  →  sealMu                                                        (seal / unseal / bindCurrentSync / MarkFreshSync)
lifecycleMu  →  writeWG.Add  →  writeMu  →  {currentSyncMu, synthLayerMu,
                                            entIDLookupMu, computedStatsMu,
                                            deferredGrantStatsMu, poisonLogMu} (lifecycle-driven writes)
             writeWG.Add  →  writeMu  →  the same leaf mutexes                 (record writes)
             checkpointMu.RLock                                                (synth-layer worker, holds nothing else)
```

`sealMu` is taken only under `lifecycleMu` (`adapter.go:287`, `:293`;
`engine.go:476`, `:567`), never under `writeMu`. Nothing holding `writeMu`
takes `lifecycleMu` or `closeMu`.
`checkpointMu` is only ever taken after `writeMu` (exclusive) or with nothing
held (shared), so the worker cannot participate in a cycle.

The `writeWG` `Add`/`Wait` ordering defect is the subject of this change; see
`plan.md`.

#### 1.1a After this branch

Deleted: `writeWG`, `closing`, `closeMu`, `checkpointMu`, `sealMu`, `sealed`,
`currentSyncMu`, `synthLayerMu`. Added: `Engine.binding`, an
`atomic.Pointer[syncBinding]` holding (`id`, `fresh`, `sealed`) as one
snapshot, replaced only under `writeMu` by `transitionLocked`.

| Primitive | Guards | Acquirers |
| --- | --- | --- |
| `writeMu` | every DB mutation the engine's goroutines make; `db` (nil after `Close`); `fresh*Empty`; `synthLayer`; the `binding` swap | `withWrite`, `withWriteAllowSealed`, `Close`, `CheckpointTo`, `CompactAllRanges`, `Flush`, `transition`, `setSealed`, `clearCurrentSync`, `AbortSynthesizedGrantLayer` — exactly this set, checked by `TestWriteMuHolders` |
| `binding` | lifecycle snapshot, lock-free reads | `Store` under `writeMu`; `Load` from `CurrentSyncID`, `IsFreshSync`, `IsSealed`, `requireCurrentSync`, `currentSyncBytes`, `checkWritableLocked` |
| `lifecycleMu`, `entIDLookupMu`, `computedStatsMu`, `deferredGrantStatsMu`, `poisonLogMu`, scheduler `mu` | unchanged | unchanged |
| `synthGrantLayerSession.segMu` | `segErr` and now `ready` (merged SST paths) | producer under `writeMu`; the worker, which no longer touches the DB |

```
pebbleStore.closeMu → lifecycleMu → writeMu → {entIDLookupMu, computedStatsMu,
                                               deferredGrantStatsMu, poisonLogMu, segMu}
pebbleStore.closeMu → writeMu                                    (save → CheckpointTo, Engine.Close)
                      segMu                                      (synth worker, holds nothing else)
```

### 1.2 Compaction scheduler (`compaction_scheduler.go`)

| Primitive | Type | Guards | Acquirers | Notes |
| --- | --- | --- | --- | --- |
| `pausableCompactionScheduler.mu` (`:32`) | struct-embedded `sync.Mutex` | `runningCompactions`, `unregistered`, `isGranting`, `lastAllowedWithoutPermission` | `Unregister`, `TrySchedule`, `Done`, `UpdateGetAllowedWithoutPermission`, `tryGrantLockedAndUnlock`, `periodicGranter` | Called from pebble's compaction goroutines and the granter goroutine. Released across `db.Schedule` inside the grant loop (`:162-177`) |
| `mu.isGrantingCond` (`:40`) | `*sync.Cond` on `mu` | waits out an in-flight grant loop | `Unregister`, `tryGrantLockedAndUnlock` | — |
| `paused`, `registered` (`:29-30`) | `atomic.Bool` | pause switch; register-once | `pause`/`resume` (under the engine's `sealMu`), `Register`/`Unregister` | `resume` pokes the granter with a non-blocking send |
| `stopPeriodicGranterCh`, `pokePeriodicGranterCh` (`:52-53`) | `chan struct{}` (unbuffered / cap 1) | granter lifecycle; wake-up | `Unregister` blocks on the stop send | — |

Goroutine: `periodicGranter` (`:184`), started in `Register`, joined by
`Unregister` (pebble calls it from `db.Close`).

### 1.3 Background workers the engine owns during a sync

| Owner | Primitives | Goroutines | Joined by |
| --- | --- | --- | --- |
| `synthGrantLayerSession` (`grants.go:420`) | `segCh chan synthLayerSegment` (cap 1, backpressure), `segWG`, `segMu` (guards `segErr`), `sortSem chan struct{}` (cap 2–4), `arenaFree` freelist channel | one merge+ingest worker (`grants.go:550`); per-chunk sorters via `spillSorter` | `FinishSynthesizedGrantLayer` (under `writeMu`) and `AbortSynthesizedGrantLayer` (no barrier) via `close(segCh)` + `segWG.Wait` |
| `grantRebuildTee` (`deferred_index.go:96`) | `ch chan rebuildBatch` (cap 4), `done chan struct{}`, `mu` (guards `err`) | `run` (`deferred_index.go:107`) | `finish`/`abort`, inside `BuildDeferredGrantIndexes` (under `writeMu`) |
| `spillSorter` (`bulk_import.go:912`) | `wg`, `chunkMu` (guards `chunks`, `firstErr`), shared `sem chan struct{}` bounding concurrent sorts, `spillArenaFreeList.ch` | one `sortAndWriteChunk` per cut chunk | `finalize`/`abort` |
| `BulkSyncImport` (`bulk_import.go:247`) | `mu` (shard registration and aggregation), `entitlementsSkippedMissingRefs`, `grantsSkippedMissingRefs` atomics; `Finish` merge `wg` (`:726`) | per-merge-unit goroutines at `Finish` | `wg.Wait` |
| `putGrants` encode fan-out (`grants.go:817-820`) | `wg`, `errMu`, `failed atomic.Bool` | `GOMAXPROCS` encoders, transient, inside a `withWrite` closure | `wg.Wait` before the batch is staged |
| `translateGrants` shards (`adapter.go:478`) | `wg` | shard translators, transient, before the engine write | `wg.Wait` |
| pools | `synthRecordsPool` (`grants_synth_encode.go:143`), `spillArenaPool`, `spillViewsPool` (`bulk_import.go:958-959`), `codec.reflectCache sync.Map` (`codec/registry.go:59`) | — | — |

The synth-layer worker is the only goroutine that mutates the DB outside
`writeMu`; it holds `checkpointMu.RLock` for the ingest and nothing else. An
`Add` holding `writeWG` + `writeMu` blocks on `segCh` when the worker is a
segment behind, so the worker must never need `writeMu` (`grants.go:572-580`).

### 1.4 rawdb (`internal/rawdb`)

| Primitive | Type | Guards |
| --- | --- | --- |
| `DB.deferredIdxPending` (`rawdb.go:61`) | `atomic.Bool` | mirror of the durable deferred-index marker |
| `DB.grantDigestsPresent` (`rawdb.go:70`) | `atomic.Bool` | digest-invalidation obligation gate |
| `DB.sourceScopeMayExist` (`rawdb.go:99`) | `atomic.Bool` | source-scope index obligation gate |
| `batchAccounting.{record,session,digest,fold}` (`rawdb.go:130-133`) | `atomic.Int64` | outstanding family batches; `DB.Close` reports a nonzero balance as a leak |

rawdb owns no mutex; callers arrive inside the engine barrier (`rawdb.go:17-24`).

### 1.5 Where the barrier is entered from outside the engine

`CompactAllRanges` and `Flush` have two production callers, neither on the
ordinary sync path: `pebbleStore.NormalizeForFixtureSave` (`pkg/dotc1z/pebble_store.go:383-386`,
benchmark fixture generation) and `compactPebbleFold` behind
`BATON_EXPERIMENTAL_FOLD_COMPACT=1` (`pkg/synccompactor/compactor_pebble.go:732`).
`CheckpointTo` is called by `pebbleStore.save` (`pkg/dotc1z/pebble_store.go:792`)
and by `CloneSync` (`adapter_clone_sync.go:95`). No caller inside the pebble
package reaches `Flush`, `CompactAllRanges`, or `CheckpointTo` from inside a
`withWrite` closure (`rg` over the non-test package: the only in-package call
sites are their own definitions and `adapter_clone_sync.go:95`, which runs at
top level before `dest.withWrite` on a different engine).

## 2. Store layer (`pkg/dotc1z`, `format/v3`, `synccompactor`, `session`)

`pkg/dotc1z/c1zstore`, `pkg/connectorstore`, and `pkg/sourcecache` hold no
synchronization primitives (`c1zstore/write_hook.go` uses context values only).

### 2.1 `pebbleStore` (`pkg/dotc1z/pebble_store.go`)

| Primitive | Type | Guards | Acquirers | Hold |
| --- | --- | --- | --- | --- |
| `pebbleStore.closeMu` (`:232`) | `sync.Mutex` | `closed`, `dirty`, `foldDeadBytes` | `Close` (`:885`, whole method), `CloseEngineOnly` (`:396`, unlocks before `Engine.Close`), `MarkDirty` (`:442`), `AddFoldDeadBytes` (`:457`), `StartNewSync`/`StartOrResumeSync` dirty stamp (`:481-493`), `NormalizeForFixtureSave` (`:430`), `beginSourceCacheMutation` (`source_cache.go:109-121`) | `Close` holds it across `save` → `Engine.CheckpointTo` (`:938`) and `Engine.Close` (`:919`). Source-cache mutations hold it across the engine write (`PutSourceCacheEntry`, `ReplaySourceCache*`, `DeleteSourceCacheRows*`, `DeleteSourceCacheGrantsByIDInScope`). Everything else is a field stamp |
| `pebbleStore.writeHookFn` (`:245`) | `atomic.Pointer[WriteHook]` | test-only write hook; nil in production | `writeHook` | — |

Order: `closeMu → Engine.{CheckpointTo, Close, Put*, Delete*}`. The ordinary
record write path is the reverse shape but not a cycle: `Engine.Put*` returns,
then `markDirty` takes `closeMu` briefly. `Flush`, `CompactAllRanges`,
`EndSync`, and `CheckpointSync` are never called under `closeMu`.

### 2.2 `C1File` (SQLite engine; `pkg/dotc1z/c1file.go`)

| Primitive | Type | Guards | Hold |
| --- | --- | --- | --- |
| `dbUpdated` (`:53`) | `atomic.Bool` | save-on-close flag | — |
| `closedMu` (`:59`) | `sync.Mutex` | `closed`; serialises finalize | Whole of `Close` (`:626`): deferred index build, WAL checkpoint, `closeRawDB`, `saveC1z` (zstd encode). Also `closeWithoutSave` (`convert_open.go:142`) |
| `cachedViewSyncMu` (`:79`) | `sync.Mutex` | `cachedViewSyncRun`, `cachedViewSyncErr` | Cache miss spans `getFinishedSync`/`getLatestUnfinishedSync` SQL (`:213`); invalidation is short |
| `slowQueryLogTimesMu` (`:84`) | `sync.Mutex` | `slowQueryLogTimes` | Map + log (`:157`) |

None of these nest with each other.

### 2.3 Everything else in the store layer

| Primitive | Location | Guards | Notes |
| --- | --- | --- | --- |
| `engineRegistry.mu` | `engine_registry.go:100` `sync.RWMutex` | `byEngine` | `register` Lock (`:135`), `driverForEngine` RLock (`:145`); map access only |
| `encoderPool`, `decoderPool`, `encoderPuts`, `decoderPuts` | `pool.go:36`, `:86`, `:20-21` | zstd coder pools and put counters | Get/Put around encode/decode; no user mutex |
| `decoder.initOnce` | `decoder.go:145` `sync.Once` | one-shot header peek + decoder construction | `ensureInit` (`:185`) |
| `convertGrants` lane locals | `to_pebble.go:912` `errMu`, `:914` `rowCount`, `:933` `laneWG`, `:961` `rawCh` (cap 2) | first error; row tally; lane lifecycle; reader→worker batches | Lanes spill into per-lane `BulkGrantShard`; the engine ingest is `bi.Finish` on the caller after `laneWG.Wait` (`:373`, `:1051`) |
| `prepareConnectorObjectRowsParallel.wg` | `sql_helpers.go:442` | ≤4 marshal workers writing disjoint slots | Joined at `:491`; DB insert after |
| `DecoderPool.mu` | `format/v3/envelope.go:383` | `idle`, `closed` | `get`/`put`/`Close`; short |
| `ExtractZstdTar` locals | `envelope.go:848-852` `jobs` chan, `wg`, `errMu` | 4 file-writer workers | Joined at `:947` |
| `extractIndexedZstd` locals | `format/v3/indexed.go:584-588` `jobs`, `wg`, `errMu`, `failed atomic.Bool` | ≤8 decode workers | Joined at `:627` |
| `decodedBudget.mu` | `indexed.go:356` | `remaining`, `limit` | `take` (`:366`) from the extract workers |
| `asyncRemover.wg` | `synccompactor/pebble/kway.go:258` | background `os.RemoveAll` | Engines closed before `remove` (`:245-251`); `wait` before the merge returns |
| `UsageCollector.mu` | `session/instrumented_session.go:62` | `kind`, `stats` | `record` (`:83`), `Annotation` (`:114`) |

No goroutine spawned in this layer calls `Engine.Put*`/`Delete*`; the fan-outs
are marshal, extract, and spill work joined before the engine call.

## 3. Syncer (`pkg/sync`, `expand`, `expand/scc`, `progresslog`)

### 3.1 Enforced order

`TestRunStatsLockOrder` (`lock_order_meta_test.go`) walks the package AST and
fails if any function other than `marshalToken` holds both `runState.mu` and
`runStats.mu`, or takes them in any order but `runState.mu` → `runStats.mu`
(`token.go:263-266`). It also fails on an unresolved `.mu.Lock`/`.RLock`
receiver so a new `mu` field cannot slip past the resolver.

### 3.2 Primitives

| Primitive | Type | Guards | Acquirers | Hold |
| --- | --- | --- | --- | --- |
| `runState.mu` (`run_state.go:163`) | `sync.RWMutex` | `actions`, `actionOrder`, `currentActionID`, `completedActions`, `actionCounts`, `facts`, `spawnedInFlight`, `spawnedAdmitted` | Lock: `seedInitAction`, `setFact`, `pushAction`, `markTypeScopedPlanned`, `transitionAction`, `finishAction*`, `nextPage`, `loadRunState` (`token.go:156`). RLock: `hasFact`, `current`, `getAction`, `peekMatchingActions`, `undrainedSpawnedCursors`, `completedActionsCount`, `getActionCount`, `marshalToken` (`token.go:263`) | Map/stack mutation and zap logging only; no store or connector I/O |
| `runStats.mu` (`run_stats.go:53`) | `sync.RWMutex` | `stepDurationsMs`, `connectorCalls`, `sessionOps`, `ingest`, `compaction` | `addStepDuration`, `recordConnectorCall`, `merge*`, `setIngestQuality`, `setCompaction`, getters, `loadRunStats` (`token.go:218`), `marshalToken` RLock (`token.go:265`) | Counter updates |
| `syncer.parallelTransitionMu` (`syncer.go:207`) | `sync.RWMutex` | `parallelActionTransitioner` pointer | `setParallelActionTransitioner` (`parallel_syncer.go:746`), `nextPageOrFinishAction` RLock (`syncer.go:761`) | Pointer install/read |
| `syncer.rlWallMu` (`syncer.go:214`) | `sync.Mutex` | `rlWallCoveredUntil`, `rlWallCarry` | `recordRateLimitWallInterval` (`parallel_syncer.go:83-103`) | Unlocks before `stats.addStepDuration` (`:101-106`) so it never nests `runStats.mu` |
| `syncer` `syncMap` caches (`syncer.go:190-197`) | `sync.Map` wrapper | `skipEGForResourceType`, `skipEntitlementsForResourceType`, `skipGrantsForResourceType`, `typeScopedGrantsForResourceType`, `typeScopedEntitlementsForResourceType`, `scheduledResourceTypes`, `resourceTypeTraits` | `shouldSkip*`, `resourceTypeCarries` (`type_scoped.go:25-38`), `scheduledResourceTypeExists` (`ingest_filter.go:129-151`) | Miss path does a store `GetResourceType` with no package mutex held |
| `ingestFilterStats` (`ingest_filter.go:26-37`) | `atomic.Uint64` ×9, `atomic.Bool` ×2 | ingest-quality counters, replay-block flags | `drop*`, `blockReplay`, `snapshot`, `restore`, `markKnown`; `observeInvalidConnectorData` (`syncer.go:1757`) | — |
| `syncer.listResourceActionsCompletedThisRun` (`syncer.go:204`) | `atomic.Uint64` | per-process finished `SyncResourcesOp` count | `recordListResourceCompletedThisRun` (`:795`); read at `parallel_syncer.go:187` | — |
| `parallelActionQueue.mu` + `cond` (`parallel_syncer.go:536-538`, `:589`) | `sync.Mutex` + `*sync.Cond` | `actions`, `head`, `outstanding`, `aborted`, `audit`, `auditBatch` | `attachAudit`, `transition`, `next` (blocks in `cond.Wait`), `done`, `abort` (`:600-740`) | `transition` holds `q.mu` across the commit callback → `run.transitionAction`, which takes `runState.mu` (`:641-698`, `syncer.go:772-782`). No store or connector work under `q.mu` |
| `syncParallel` locals (`parallel_syncer.go:798-802`) | `resultsMu sync.Mutex`, `wg sync.WaitGroup` | `warnings`, `errs`; worker lifecycle | workers after `syncOneAction` (`:811-818`); `wg.Wait` (`:842`) | — |
| `childScheduleSet.mu` (`ingest_invariants.go:184`) | `sync.Mutex` | `m` | `recordIfNew` (`:199`), `has` (`:213`) | Check-and-set; called before `nextPageOrFinishAction`, so not under `q.mu` |
| `queueAudit.mu` (`queue_audit.go:19`) | `sync.Mutex` | `events`, `batchSeq` | test hook only (`parallel_syncer.go:783-785`) | Under `q.mu` when live |
| `ProgressLog.mu` (`progresslog/progresslog.go:43`) | `sync.RWMutex` | counters and last-log timestamps | `Add*` (`:405-429`), `Log*Progress` (`:191-327`) | Releases before logging |
| `ProgressLog.expandMu` (`progresslog.go:57`) | `sync.Mutex` | `lastActionLog`, `dbSize`, expand-metric deltas | `SetDBSizeProvider` (`:143`), `LogExpandProgress` (`:349`) | Spans `dbSize.CurrentDBSizeBytes()` (`:379`), a directory walk; kept off `mu` so it does not serialise `Add*` |
| `DroppedEdgeStats.mu` (`expand/drop_stats.go:26`) | `sync.Mutex` | `sourceMissing`, `destinationMissing`, `seen`, `examples` | `record`, `LogSummary` | Short |
| `bfsMultiSource.wg` (`expand/scc/scc.go:433`) | `sync.WaitGroup` | per-BFS-level workers (`:449`) | `fixEntitlementGraphCycles` → `scc.CondenseFWBW` | Workers touch the bitset only, never the store |
| `bitset` CAS (`expand/scc/bitset.go:47-84`) | `atomic.LoadUint64`/`CompareAndSwapUint64` | `visited` bits shared by BFS workers | `testAndSetAtomic`, `clearAtomic` | — |
| `intSlicePool` (`scc.go:528`) | `sync.Pool` | degree slices for `trimSingletons` | driver goroutine | — |

Order, outermost first: `parallelActionQueue.mu → runState.mu`;
`runState.mu → runStats.mu` (in `marshalToken` only). `rlWallMu`,
`parallelTransitionMu`, `childScheduleSet.mu`, and the progress-log mutexes are
leaves.

`expansionGraph` has no mutex; the expansion phase is single-threaded
(`expansion_graph.go:19-23`). No channels, `sync.Once`, or `errgroup` in
non-test `pkg/sync`.

### 3.3 Goroutines and who writes the store

```
Sync (syncer.go:871)                                   main goroutine
└─ parallelSync (parallel_syncer.go:134)
   ├─ workerCtx = WithCancelCause(ctx); AfterFunc(runCtx → cancel)   (:140-147)
   ├─ loop: Checkpoint → store.CheckpointSync          main only, no workers live (:164)
   │        serial ops (SyncResourceTypes, expansion)  main
   │        fan-out ops → syncParallel (:779-842)
   │            N = workerCount workers, WaitGroup.Go  (:802-840)
   │            each: queue.next → SyncResources|Entitlements|Grants|Targeted → queue.done
   │            hard error: cancel(batchCtx) + queue.abort
   │            wg.Wait before returning to the loop   (:842)
   └─ SyncGrantExpansion                               main; scc BFS workers never touch the store
```

Workers write the store concurrently with each other: `SyncEntitlements` →
`store.PutEntitlements` (`syncer.go:2146`), `SyncGrants` → `store.PutGrants`
(`:2826`), `putConnectorResources` → `store.PutResources` (`:1841`).
`workerCount` defaults to 1 (`syncer.go:4274`); `--parallel-sync` maps to
`min(GOMAXPROCS, 4)` and `--worker-count N` to `max(N, 1)`
(`cli/commands.go:373-380`, `syncer.go:4222-4227`). `Checkpoint`, `EndSync`,
and the store `Close` all run on the main goroutine after `wg.Wait`; the
stop path (`checkpointOnStop`, `:499-504`) is reached only after the batch has
joined.

## 4. Task runner, HTTP, connector wrapper

`pkg/connectorbuilder` has no primitives of its own; it calls
`uhttp.ClearCaches` from `Cleanup` (`connectorbuilder.go:380`).

| Primitive | Type | Guards | Acquirers | Hold |
| --- | --- | --- | --- | --- |
| task semaphore (`connectorrunner/runner.go:212`) | `*semaphore.Weighted`, weight `taskConcurrency` (default 3, CLI range 1–100: `field/defaults.go:24`, `:351-357`) | concurrent task slots | `Acquire` before `tasks.Next` (`:225`); `Release` on `Next` error (`:240`), nil task (`:249`), after one-shot `processTask` (`:264`), or the task goroutine's defer (`:280`) | The whole task, sync included |
| `connectorRunner.debugFileMutex` (`runner.go:45`) | `sync.Mutex` | `debugFile` create/close and logger tee | `setupPersistentLog` (`:64`) from `Run` (`:134`) and debug tasks (`:181`) | Local file open/close |
| `sigChan` (`runner.go:140`) + `WithCancelCause` (`:130`) | `chan os.Signal` (cap 1), context | interrupt → `cancel(ErrSigTerm)` | signal goroutine (`:142-146`) | — |
| `taskQueue.mtx` (`tasks/c1api/task_queue.go:13`) | `sync.Mutex` | `queued`, `inFlight`, `nextPollAt` | `take`, `enqueue`, `markDone`, `fetchParams`, `setNextPoll`, `pollDecision` | Released around the `GetTasks` RPC; `markDone` runs on task goroutines concurrently with `Next` on the loop |
| `c1ApiTaskManager.runnerShouldDebug` (`manager.go:83`) | `atomic.Bool` | persistent-debug switch | `debugHandler.HandleTask` (`debug.go:21`), `ShouldDebug` (`manager.go:391`) | — |
| heartbeat `WithCancelCause` (`task_helpers.go:72`) | context | `rCtx` the sync runs under | `rCancel` on heartbeat failure ×10, server `Cancelled`, or parent cancel (`:82-142`); heartbeat goroutine at `:95` | The sync sees cancellation only through `rCtx`; no mutex is shared between the heartbeat goroutine and the sync. Heartbeat RPCs use the parent `ctx` (`:115`). Only `fullSyncTaskHandler.HandleTask` heartbeats (`full_sync.go:315`) |
| `full_sync.go:297` `WithCancel`; `manager.go:319` `WithTimeout(Background, 30s)` | context | HandleTask lifetime; detached `FinishTask` RPC | — | — |
| `pkg/tasks/local/*` `o sync.Once` (15 managers, e.g. `syncer.go:24`) | `sync.Once` | one-shot task emission from `Next` | `o.Do` | Proto construction |
| `uhttp.cachesMtx` (`uhttp/wrapper.go:118`) | `sync.RWMutex` | global `caches` registry | `NewBaseHttpClientWithContext` Lock (`:177`); `ClearCaches` RLock across every `cache.Clear` (`:125`) | Connector child process, at client construction and `Cleanup` |
| `Transport.gapMu` (`uhttp/transport.go:81`) | `sync.Mutex` | gap-triggered idle-pool drop | `dropIdleConnectionsAfterGap` (`:206`) | Across `closeIdleConnections`, which takes `Transport.mtx.RLock` (`:225`): order `gapMu → mtx` |
| `Transport.mtx` (`transport.go:84`) | `sync.RWMutex` | `roundTripper`, `baseTransport`, `nextCycle` | `cycle` (`:97`, `:104`), `closeIdleConnections` (`:225`) | Local transport swap; never takes `gapMu` |
| `Transport.lastActivityNs` (`transport.go:76`) | `atomic.Int64` | last request time | `RoundTrip` defer (`:268`), `dropIdleConnectionsAfterGap` | — |
| `MemRateLimiter` embedded `sync.Mutex` (`ratelimit/mem_ratelimiter.go:15`) | `sync.Mutex` | `limiter` replacement in `Report` (`:44`) | `Report` only; `Do` calls `limiter.Take()` without it (`:22-39`) | `Take` may block for tokens, unlocked |
| `wrapper.mtx` (`internal/connector/connector.go:93`) | `sync.RWMutex` | `client`, `conn`, `serverStdin` | `C` RLock (`:373`) then Lock on first create (`:381`); `Close` Lock (`:450`) | First create spans subprocess start and a dial loop with a 5s timeout (`:408-438`); later calls are the RLock fast path shared by concurrent tasks |
| lambda `TransportStream.mtx`, `headersSent`, `Server.mu`, `drained` (`lambda/grpc/server.go:27`, `:33`, `:173`, `:187`) | `sync.Mutex`, `atomic.Bool`, `sync.Mutex`, `chan struct{}` | header joins; once-only header send; service registry and active counts; hot-reload drain | per-request handler; `handle` releases `mu` before invoking the handler (`:269-298`) | Lambda transport only |

Goroutines during a service-mode sync: the runner loop; the signal goroutine;
one `processTask` goroutine per in-flight task (up to `taskConcurrency`); the
heartbeat goroutine for a full-sync task; the connector child's `cmd.Wait` and
optional session-server goroutines (`connector.go:283`, `:341`); the optional
`DBCache` cleaner (`uhttp/dbcache.go:116`). A full sync can run concurrently
with grant, revoke, ticket, action, and debug tasks in the same process, each
holding one semaphore slot; compaction runs only in one-shot mode
(`runner.go:830-839`, `:1085-1098`) and never overlaps a service-mode sync.
One-shot mode runs `processTask` inline on the loop (`runner.go:260-274`).

Nesting: `semaphore slot → {taskQueue.mtx (brief), debugFileMutex (brief),
wrapper.mtx}`; `gapMu → Transport.mtx`. Nothing in this layer is held while
calling into the store.

## 5. Cross-layer synthesis

### 5.1 Who is inside the engine barrier at the same time

| Phase | Goroutines that reach `withWrite*` | Concurrent `Wait`ers (`Close`, `CheckpointTo`) |
| --- | --- | --- |
| Collection (`syncParallel` batch) | up to `workerCount` (default 1, max 4 via `--parallel-sync`, unbounded via `--worker-count`) | none: `Checkpoint` and `Close` run on main after `wg.Wait` |
| Serial ops, expansion | main only; the synth-layer worker bypasses the barrier (`checkpointMu.RLock`) | none |
| `EndSync` | main, under `lifecycleMu` | none |
| Store `Close` → `save` → `CheckpointTo` → `Engine.Close` | none (sync finished, workers joined) | main, under `closeMu` |
| `CloneSync` → `CheckpointTo` | none | caller |
| Compactor fold (`BATON_EXPERIMENTAL_FOLD_COMPACT=1`) | `CompactAllRanges` on the merge goroutine, then `save` | same goroutine, sequential |
| Concurrent grant/revoke tasks | different store instances | — |

No in-repo path puts an `Add` site and a `Wait` site on different goroutines
against the same engine. The `writeWG` `Add`/`Wait` ordering violation is
reachable only through the exported `Engine` API called from two goroutines,
which the package does not forbid and the barrier exists to support.

### 5.2 Where the engine barrier sits in the global order

```
closeMu (pebbleStore)  →  writeWG.Wait → writeMu → checkpointMu.Lock          (save → CheckpointTo)
closeMu (pebbleStore)  →  Engine.closeMu → writeWG.Wait → synthLayerMu → writeMu   (Engine.Close)
closeMu (pebbleStore)  →  writeWG.Add → writeMu → leaves                     (source-cache mutations)
lifecycleMu (Engine)   →  sealMu | writeWG.Add → writeMu → leaves            (StartNewSync, CheckpointSync, EndSync)
parallelActionQueue.mu →  runState.mu → runStats.mu                          (pkg/sync, store not called under these)
```

`pkg/sync` never holds one of its mutexes while calling the store, and the
store never calls back into `pkg/sync`, so the two orders do not interact.
