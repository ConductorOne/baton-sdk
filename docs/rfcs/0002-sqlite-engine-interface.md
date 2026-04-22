# RFC 0002: Clean Up the C1Z Storage Interface

- **Status:** Draft (iteration 2 — incorporates critical review)
- **Scope:** `pkg/connectorstore`, `pkg/dotc1z`, `pkg/sync/**`, `pkg/synccompactor/**`, `pkg/provisioner` in baton-sdk; `pkg/temporal/sync_activity/**` and a couple of related packages in c1
- **Out of scope:** Pebble, new file formats, new engines. This RFC is *only* about making the current sqlite-backed interface cleaner. Pebble is a separate future RFC.
- **Why standalone:** the interface improvements stand on their own merits. Landing them first gives us a defensible contract to evaluate future engines against.

## 1. Problem

Over the past year, `pkg/connectorstore` has grown a third interface layer — `InternalWriter` — to carry sync-pipeline-specific query shapes that don't belong on the connector-facing `Writer`:

```
Reader                 — connector gRPC read surface  (connectors depend on this; don't touch)
Writer : Reader        — connector gRPC write surface (connectors depend on this; mostly don't touch)
InternalWriter : Writer — sync-pipeline escape hatches (UpsertGrants w/ Mode, ListGrantsInternal w/ Mode, SetSupportsDiff)
```

Simultaneously, `*dotc1z.C1File` has accumulated ~10 methods *outside* any of these interfaces (`Stats`, `CloneSync`, `GenerateSyncDiff`, `AttachFile`, `LatestSyncID`/`LatestFinishedSyncID`/`GetLatestFinishedSync`, `OutputFilepath`, `ViewSync`, `Dirty`, `ListSyncRuns`) that callers access via concrete type. Some are live, some are dead, some are called only internally — but the interface surface doesn't distinguish them.

Symptoms:

- **Intent hidden in enum values.** `s.store.UpsertGrants(ctx, connectorstore.GrantUpsertOptions{Mode: connectorstore.GrantUpsertModeReplace}, grants...)` — reviewer must know what each mode means to understand the code.
- **Type-assertion patterns that silently fail.** `pkg/sync/syncer.go` defines a local `latestSyncFetcher` interface requiring `LatestFinishedSync(ctx, SyncType) (string, error)`, but `*C1File` exposes `LatestFinishedSyncID` with that signature — the assertion fails silently today, and the guarded code block never runs. This is an active bug caused by interface drift.
- **Dead surface.** Modes, methods, and types exist with zero production callers.
- **Escape hatches leak implementation.** `AttachFile` on `Writer` returns a `*C1FileAttached` that exposes SQL-oriented compaction primitives.

None of these are fatal. Together they make the sync/compact pipeline harder to reason about than it needs to be, and they're the mechanism by which every feature PR has to choose between "widen the interface" and "reach into the concrete type."

## 2. Goals

1. **Preserve the connector-facing contract.** `Reader` and `Writer` are what connectors compile against (directly and via `NewC1FileReader`, `NewExternalC1FileReader`, `connectorstore.Reader`-satisfying types in c1). We leave them alone. Zero source-compat break for connector consumers.
2. **Replace `InternalWriter` with methods named by caller intent, grouped into sub-interfaces.** Flatten the mode enums. Name by what the caller is trying to accomplish. Segregate by concern.
3. **Consolidate `*C1File`'s ad-hoc methods into named sub-interfaces.** Delete unused ones; unexport ones called only within `pkg/dotc1z`.
4. **Move cross-file operations out of the storage interface.** `AttachFile` becomes a compactor primitive in `pkg/synccompactor`, not a storage method. This fixes one of the two "leaky" methods the earlier draft of this RFC punted on.
5. **Ship in a single reviewable PR, atomically with c1's vendor bump.** See §7 for what "atomic" actually means across two repos.

Non-goals:

- Adding Pebble or any other engine.
- Changing the `.c1z` file format.
- Changing gRPC surfaces (`v2.*ServiceServer`, `reader_v2.*ReaderServiceServer`).
- Changing the session store interface.

## 3. Ground truth: every caller, categorized

Every non-test, non-vendor call site of `connectorstore.Reader`, `connectorstore.Writer`, `connectorstore.InternalWriter`, and `*dotc1z.C1File`-specific methods in baton-sdk (`origin/main`) and c1, derived from a ripgrep audit.

**How "dead" is qualified.** We distinguish three states:

- **externally dead** — no non-test caller outside `pkg/dotc1z/` (safe to unexport)
- **package-internal only** — called by `pkg/dotc1z/*.go` files but not from outside (must keep, can unexport)
- **live** — has callers outside `pkg/dotc1z/`

### Reader (40+ sites, all gRPC-shaped)

Used by the syncer to read back what it just wrote, by the provisioner to resolve a grant's parties, by c1's uplift/temporal activities to page through sync data, by the support-dashboard admin compare tool, and by non-C1File Reader implementations (`pkg/connector/mcp/connector_reader.go`, `pkg/spreadsheet/v2/spreadsheet_v2.go`, `pkg/connector/v2/empty.go`). Every call uses the proto request/response shape verbatim.

**Decision: don't touch it.** The Reader interface is the connector-facing read contract; its shape is forced by gRPC, and multiple non-C1File types satisfy it. Changing it provides no value and breaks many things.

### Writer (on top of Reader) — 25 sites

| Method | Call sites | Disposition |
|---|---|---|
| `StartOrResumeSync(ctx, SyncType, syncID)` | 5 | keep |
| `StartNewSync(ctx, SyncType, parentSyncID)` | 4 | keep |
| `SetCurrentSync(ctx, syncID)` | 1 | keep |
| `CurrentSyncStep(ctx)` | 1 | keep |
| `CheckpointSync(ctx, token)` | 1 | keep |
| `EndSync(ctx)` | 4 | keep |
| `Cleanup(ctx)` | 4 | keep |
| `PutAsset(ctx, ref, ct, data)` | 1 | keep |
| `PutResourceTypes(ctx, ...)` | 3 | keep |
| `PutResources(ctx, ...)` | 7 | keep |
| `PutEntitlements(ctx, ...)` | 4 | keep |
| `PutGrants(ctx, ...)` | **0 external production** | keep on `Writer` (part of the connector contract); on `C1File` the method remains as a thin wrapper over the Replace-mode upsert, called only by `Writer` embedders |
| `DeleteGrant(ctx, id)` | 1 | keep |
| `ResumeSync(ctx, SyncType, syncID)` | **0 external; internal callers in sync_runs.go** | unexport to `resumeSync` |

### InternalWriter — 7 sites

| Method | Call sites | What the caller wants |
|---|---|---|
| `UpsertGrants(Replace)` | 4 (syncer) | write grants, replace on conflict, re-extract expansion from payload |
| `UpsertGrants(PreserveExpansion)` | 1 (expander:293) | write grants back after the expander already consumed the expansion annotation |
| `UpsertGrants(IfNewer)` | 0 external; only via `PutGrantsIfNewer` internal method | merge two files, last-write-wins |
| `ListGrantsInternal(ExpansionNeedsOnly)` | 1 (syncer:1404) | page through pending expansion work, no grant payload |
| `ListGrantsInternal(PayloadWithExpansion)` | 2 (syncer:2205, c1 wrapper:182) | stream grants with their annotation inline |
| `ListGrantsInternal(Payload)` / `(Expansion)` | 0 production; test-only | dead as production API; tests must be rewritten or deleted |
| `SetSupportsDiff(ctx, syncID)` | 2 (parallel_syncer) | mark sync as having populated metadata |

Three real operations hidden behind two mode enums. Two dead mode values (with test coverage that must be retired alongside).

### `*C1File` methods accessed via concrete type

| Method | External callers | Internal callers | Disposition |
|---|---|---|---|
| `Stats(ctx, syncType, syncID)` | 5 (all c1) | none | live; signature takes syncType + syncID — this contradicts v0 of this RFC which had `Stats(ctx)` |
| `CloneSync(ctx, outPath, syncID)` | 1 (c1 `storeCompletedSyncC1Z`) | none | live; caller passes output path, not returned from method |
| `GenerateSyncDiff(ctx, base, applied)` returns `(string, error)` | 1 (baton-sdk `pkg/tasks/local/differ.go`) | none | live |
| `AttachFile(other *C1File, name)` returns `(*C1FileAttached, error)` | 2 (compactor, c1 incremental compact) | none | live; moves to `synccompactor` (§4.4) |
| `LatestSyncID(ctx, syncType)` returns `(string, error)` | 2 (baton-sdk dev-util, c1 sync_v0) | `pkg/dotc1z/clone_sync.go`, `c1file.go` | live; semantically a misnomer — returns latest *finished* sync id, not latest sync of any state. We'll clarify in the rename |
| `LatestFinishedSyncID(ctx, syncType)` returns `(string, error)` | 0 (the `latestSyncFetcher` local interface in syncer.go intends to call this but its signature is `LatestFinishedSync` not `LatestFinishedSyncID` — assertion fails silently) | `sync_runs.go` internal | live internally; externally a bug |
| `GetLatestFinishedSync(ctx, req)` returns `(*resp, error)` | 2 (baton-sdk `synccompactor/attached`, syncer:275) | none | live |
| `ViewSync(ctx, syncID)` | 0 | 0 | externally dead, package-internal dead; **truly deletable** |
| `OutputFilepath()` | 0 | 0 | **truly deletable** |
| `Dirty()` | doesn't exist on main | — | N/A |
| `ListSyncRuns(ctx, pt, ps)` | 0 | `sync_runs.go:775` (cleanup), `sync_runs.go:1017` (`ListSyncs` gRPC Reader impl) | keep — `ListSyncs` is the Reader method that calls it |
| `GrantStats(ctx, syncType, syncID)` | 0 | 0 | truly deletable |
| `InitTables(ctx)` | 0 | `c1file.go:467` (constructor) | keep, unexport to `initTables` |
| `DeleteSyncRun(ctx, syncID)` | 0 | `sync_runs.go:{821,838,890}` (cleanup) | keep, unexport to `deleteSyncRun` |
| `Vacuum(ctx)` | 0 | `sync_runs.go:901` (cleanup) | keep, unexport to `vacuum` |
| `SetSyncID(ctx, syncID)` | 0 | 0, commented "testing only" | truly deletable |

Seven methods truly deletable. Four keep-and-unexport. Five live and stay on the public surface.

### Session store — disjoint from engine

40+ call sites of `sessions.SessionStore` and `sessions.SetSessionStore` across `pkg/cli`, `pkg/connectorbuilder`, `pkg/session`, `internal/connector`, `pkg/types/resource`. The sync pipeline references it only to *pass it through* — `pkg/sync/syncer.go:140` holds a `sessions.SetSessionStore` field and `:2735` exposes `WithSessionStore(...)`. The engine does not read or write through it; the syncer hands it to the connector. No changes here.

## 4. Proposed interface

### 4.1 Sub-interface design (the first thing we're changing from v0)

The v0 draft of this RFC proposed a single `dotc1z.Engine` interface that embedded `connectorstore.Writer` and added ~10 methods. With the Reader methods pulled in through Writer's embedding, that interface totalled ~45 methods — a classic god-interface. Critical review pushed back that sub-store composition reads better, tests better, and forces "should this method be here?" to be answered at design time.

v1 adopts a sub-interface decomposition:

```
pkg/dotc1z/store.go
    type C1ZStore interface {       // the top-level aggregate the sync/compact/provisioner packages consume
        connectorstore.Writer       // Reader + Writer (unchanged)
        Grants() GrantStore         // grant-specific operations
        SyncMeta() SyncMeta         // sync-run metadata operations
        FileOps() FileOps           // file-level, same-file operations
        Close(ctx) error            // override with context (matches what *C1File already provides)
    }

pkg/dotc1z/grant_store.go
    type GrantStore interface {
        StoreExpandedGrants(ctx, grants...) error
        MergeGrantsNewestWins(ctx, grants...) error
        PendingExpansion(ctx) iter.Seq2[PendingExpansion, error]
        ListWithAnnotations(ctx) iter.Seq2[GrantAnnotation, error]
    }

pkg/dotc1z/sync_meta.go
    type SyncMeta interface {
        MarkSyncSupportsDiff(ctx, syncID) error
        LatestFullSync(ctx) (*SyncRun, error)
        LatestFinishedSyncOfAnyType(ctx) (*SyncRun, error)
        Stats(ctx, syncType SyncType, syncID string) (map[string]int64, error)
    }

pkg/dotc1z/file_ops.go
    type FileOps interface {
        CloneSync(ctx, outPath string, syncID string) error
        GenerateSyncDiff(ctx, baseSyncID, appliedSyncID string) (diffSyncID string, err error)
    }
```

Important design notes:

- **`C1ZStore` is the name, not `Engine`.** "Engine" is generic and collides with future pebble naming. `C1ZStore` is specific: "a store backed by a .c1z file."
- **No `AttachFile` anywhere.** It moves to `pkg/synccompactor`. See §4.4.
- **`PutGrants` / `PutGrantsIfNewer` / `PutGrantsPreservingExpansion` are gone as public names.** `GrantStore` has three methods with names describing the caller's job. Details in §4.2.
- **`LatestFinishedSync` variadic is gone.** Two named methods. Details in §4.5.
- **Sub-interfaces mean narrow mocks.** `pkg/provisioner` depends on `connectorstore.Reader` (unchanged). `pkg/sync/expand` depends on `GrantStore`. `pkg/synccompactor` depends on the whole `C1ZStore`. Each package pulls in only what it uses.

### 4.2 Grant operations: name by caller intent, not storage semantics

Critical review pointed out that `PutGrantsPreservingExpansion` / `PutGrantsIfNewer` replace one opacity (mode enums) with another (storage-semantics method names). A junior engineer reading `PutGrantsPreservingExpansion` has to know what "preserving expansion" means at the storage layer — which is exactly as opaque as knowing what `Mode: PreserveExpansion` means.

Better: name by what the caller is trying to accomplish.

| Operation | Caller | What they're doing | New name |
|---|---|---|---|
| `UpsertGrants(Replace, grants)` | syncer's 4 sites | "I synced these grants from the connector; store them" | `connectorstore.Writer.PutGrants(ctx, grants...)` (unchanged — it's the connector-facing API) |
| `UpsertGrants(PreserveExpansion, grants)` | `expand/expander.go:293` | "I expanded these source grants into new grants; save them without touching the expansion-metadata columns (those were consumed upstream)" | `GrantStore.StoreExpandedGrants(ctx, grants...)` |
| `UpsertGrants(IfNewer, grants)` | naive compactor (via `PutGrantsIfNewer` wrapper) | "I'm merging grants from two files, newest-discovered wins" | `GrantStore.MergeGrantsNewestWins(ctx, grants...)` |
| `ListGrantsInternal(ExpansionNeedsOnly)` | syncer:1404 | "Give me the next batch of expansion work from the queue" | `GrantStore.PendingExpansion(ctx) iter.Seq2[PendingExpansion, error]` |
| `ListGrantsInternal(PayloadWithExpansion)` | syncer:2205, c1 wrapper:182 | "Stream all grants with their expansion annotations (to drive expansion processing)" | `GrantStore.ListWithAnnotations(ctx) iter.Seq2[GrantAnnotation, error]` |

Reading check:

```go
// expander.go:293
// Before:
err := store.UpsertGrants(ctx, connectorstore.GrantUpsertOptions{
    Mode: connectorstore.GrantUpsertModePreserveExpansion,
}, grants...)

// After:
err := store.Grants().StoreExpandedGrants(ctx, grants...)
```

```go
// syncer.go:1404
// Before:
internalList, err := s.store.ListGrantsInternal(ctx, connectorstore.GrantListOptions{
    Mode:      connectorstore.GrantListModeExpansionNeedsOnly,
    PageToken: action.PageToken,
})
if err != nil { return err }
nextPageToken := internalList.NextPageToken
for _, row := range internalList.Rows {
    def := row.Expansion
    if def == nil { continue }
    // ...process def...
}

// After:
for pe, err := range s.store.Grants().PendingExpansion(ctx) {
    if err != nil { return err }
    // ...process pe (type PendingExpansion)...
}
```

The pagination loop disappears entirely — the iterator owns page-token management internally. The caller writes exactly the loop it wants.

```go
// naive compactor
// Before:
err := dst.PutGrantsIfNewer(ctx, gs...)

// After:
err := dst.Grants().MergeGrantsNewestWins(ctx, gs...)
```

Each name tells you what the caller is *doing*, not what the table is *being set to*. `StoreExpandedGrants` has one caller and if that caller grows a cousin, it'll either reuse the method or make the case for a new named operation — in either case the reviewer knows what to ask about.

### 4.3 Return types: use proto where we can

Critical review pointed out that `ExpansionDef` (my v0 struct) duplicates `v2.GrantExpandable` (the proto annotation). Checking the proto:

```proto
// pb/c1/connector/v2/annotation_grant.proto
message GrantExpandable {
  repeated string entitlement_ids = 1;   // == SourceEntitlementIDs
  bool shallow = 2;                       // == Shallow
  repeated string resource_type_ids = 3;  // == ResourceTypeIDs
}
```

Three fields are an exact duplicate. The other six fields of `ExpandableGrantDef` on main (`RowID`, `GrantExternalID`, `TargetEntitlementID`, `PrincipalResourceTypeID`, `PrincipalResourceID`, `NeedsExpansion`) carry grant identity and a state flag — what the caller would need to look up the grant without having materialized the full payload.

There's a real performance reason this struct exists: for the expansion worker queue, the hot path reads millions of rows and needs only identity + annotation, not the full grant proto. Unmarshalling `data` bytes for each row would be wasted work.

**Proposal:** keep a narrow identity-projection struct, but:

1. Rename for clarity: `PendingExpansion` (not `ExpandableGrantDef`; not `ExpansionDef`).
2. Embed `*v2.GrantExpandable` rather than redeclaring its fields.
3. Document the performance reason.

```go
// pkg/dotc1z/grant_store.go

// PendingExpansion is the lightweight row shape returned by
// GrantStore.PendingExpansion. It contains grant identity and the
// caller's expansion annotation, without materializing the full grant
// payload. Used by the syncer's expansion worker queue, which processes
// millions of rows and benefits from avoiding the grant-payload unmarshal.
type PendingExpansion struct {
    GrantExternalID         string
    TargetEntitlementID     string
    PrincipalResourceTypeID string
    PrincipalResourceID     string

    // Annotation is the grant's expansion annotation. Non-nil by construction
    // (a grant only appears in this list if it has an expansion annotation).
    Annotation *v2.GrantExpandable
}
```

For the other call pattern (`ListGrantsInternal(PayloadWithExpansion)` → `ListWithAnnotations`), we do have the full grant:

```go
// GrantAnnotation is what ListWithAnnotations yields: a grant plus
// (optionally) its expansion annotation. The annotation is nil if the
// grant has none.
type GrantAnnotation struct {
    Grant      *v2.Grant
    Expansion  *v2.GrantExpandable  // nil if the grant isn't expandable
}
```

Both structs are minimal and use the proto type rather than redeclaring fields. `RowID` is gone — nothing outside the `pkg/dotc1z` internals ever needed it, and exposing it forces callers to reason about storage internals.

### 4.4 `AttachFile` moves to `pkg/synccompactor`

v0 of this RFC punted on `AttachFile`, leaving it on the storage interface with a "RFC 0003 will fix it." Critical review called the punt — correctly. The fix is now.

Today, `AttachFile` has exactly two callers, both inside compaction code:

- `pkg/synccompactor/attached/attached.go:84` — the attached compactor itself
- `c1/pkg/temporal/sync_activity/incremental-sync/compact.go:772` — c1's temporal activity that runs an attached compaction

These are the only places that need cross-file SQL attachment. Moving `AttachFile` off the storage interface and onto a compactor primitive is a three-site refactor.

Proposal:

```go
// pkg/synccompactor/attached/attached.go

// Attach couples two C1ZStore instances using SQL ATTACH, returning a
// handle that supports cross-file merge and diff primitives.
//
// Both stores must be sqlite-backed (today, the only implementation).
// A future pebble-backed store would need a different merge primitive,
// handled by a separate compactor flavor in this same package.
func Attach(ctx context.Context, base, applied dotc1z.C1ZStore, attachName string) (*Attached, error)

type Attached struct { /* opaque */ }

func (a *Attached) CompactResourceTypes(ctx, baseSyncID, appliedSyncID string) error
func (a *Attached) CompactResources   (ctx, baseSyncID, appliedSyncID string) error
func (a *Attached) CompactEntitlements(ctx, baseSyncID, appliedSyncID string) error
func (a *Attached) CompactGrants      (ctx, baseSyncID, appliedSyncID string) error
func (a *Attached) UpdateSync(ctx, base, applied *reader_v2.SyncRun) error

// GenerateSyncDiff operates across the attached files, producing upserts
// and deletions sync runs. Returns a named struct to avoid the (string,
// string, error) tuple that's hard to read at the call site.
type SyncDiffResult struct {
    UpsertsSyncID   string
    DeletionsSyncID string
}
func (a *Attached) GenerateSyncDiff(ctx, oldSyncID, newSyncID string) (SyncDiffResult, error)

// Detach releases the attachment.
func (a *Attached) Detach(ctx context.Context) error
```

Inside `Attach`, the implementation does the SQL-specific work: peeks into the concrete `*C1File` via an unexported accessor `sqliteFile()` that the `C1ZStore` interface doesn't expose publicly but `pkg/synccompactor/attached` can get via a sibling-package escape hatch.

Concretely, the escape hatch is an unexported interface:

```go
// pkg/dotc1z/store.go

// SQLiteStore is an unexported interface that lets certain internal
// callers (synccompactor) reach the concrete sqlite handle. NOT part of
// the public API. Only *C1File implements this.
type sqliteStore interface {
    sqliteFile() *C1File
}

// For synccompactor/attached to use, we export an accessor at the dotc1z
// layer, since Go doesn't let packages import each other's unexported names:
func AsSQLiteStore(s C1ZStore) (*C1File, bool) {
    if ss, ok := s.(interface{ sqliteFile() *C1File }); ok {
        return ss.sqliteFile(), true
    }
    return nil, false
}
```

`synccompactor/attached` does `if f, ok := dotc1z.AsSQLiteStore(store); ok { ... ATTACH ... }` and fails cleanly if handed a non-sqlite store (relevant for the future pebble RFC, not this RFC).

**The upshot:** `C1ZStore` — the interface that `pkg/sync`, `pkg/provisioner`, and c1's sync activities see — has no `AttachFile` method. It's clean. The compactor knows it's working with sqlite and does sqlite-specific things through an explicit gate. When pebble lands, its compactor lives in a sibling package that knows how to do pebble-native merges.

### 4.5 `LatestFinishedSync` — split, don't variadic

v0 proposed `LatestFinishedSync(ctx, types ...SyncType)`. Critical review: variadic with "zero-args means 'any'" is a footgun. The naive compactor's attached code explicitly excludes diff sync types; the syncer wants "latest full" specifically. These are different enough to be different methods.

```go
// SyncMeta interface methods:
LatestFullSync(ctx)                  (*SyncRun, error)  // SyncTypeFull only; nil if none
LatestFinishedSyncOfAnyType(ctx)     (*SyncRun, error)  // any finished sync, including diff types; nil if none
```

`SyncRun` is the engine-native type from `pkg/dotc1z/sync_runs.go`'s existing `syncRun` struct (renamed exported). Not `*reader_v2.SyncRun` — exposing the proto means every caller imports the gRPC wire type, which makes evolution painful. The gRPC `Reader` surface still returns proto types for the `GetLatestFinishedSync` / `ListSyncs` RPCs; that's a separate concern.

### 4.6 `Stats` and `CloneSync` — correct signatures

v0 had these wrong. Correcting:

```go
// SyncMeta interface method — takes (syncType, syncID) per main's actual signature
Stats(ctx context.Context, syncType SyncType, syncID string) (map[string]int64, error)

// FileOps interface method — takes output path, returns error
CloneSync(ctx context.Context, outPath, syncID string) error
```

The grab-bag `map[string]int64` stays. Callers log it; no typed-access need.

### 4.7 The `latestSyncFetcher` bug

The local interface at `pkg/sync/syncer.go:1547` declares `LatestFinishedSync(ctx, SyncType) (string, error)`. `*C1File`'s actual method is `LatestFinishedSyncID(ctx, SyncType) (string, error)`. The type assertion at syncer.go:185 always fails; the "fallback" branch always runs; `getPreviousFullSyncID` always returns `""`. This is a pre-existing bug. Fixing it is in scope for this RFC — the new `SyncMeta.LatestFullSync` is directly called from syncer, no type assertion, returns a meaningful value.

### 4.8 `PutGrantsPreservingExpansion` invariant

The main-branch implementation of `UpsertGrants(Mode: PreserveExpansion)` preserves the `expansion` and `needs_expansion` *columns* but always overwrites the `data` column. If the caller passes a grant whose payload still contains a `GrantExpandable` annotation, the stored `data` will disagree with the stored `expansion` column. For today's only caller (`expand/expander.go:293` writing descendant grants that by construction have no annotation), this is harmless.

For `StoreExpandedGrants`, we either:

- **(a)** document the invariant and fail loudly if violated: check each grant's annotations at the top of the method and return an error if any has `GrantExpandable`, or
- **(b)** strip the annotation in the wrapper before calling the underlying upsert.

Proposal: **(b)**. It makes the method total and matches the name: "store these expanded grants" — stripping a vestigial annotation before write is what a caller would expect the method to do. The expander then doesn't have to know.

## 5. Deletions, renames, and unexports

### Deletions (baton-sdk)

From `pkg/connectorstore/connectorstore.go`:

- `InternalWriter` interface
- `GrantUpsertMode` + constants
- `GrantUpsertOptions`
- `GrantListMode` + constants
- `GrantListOptions`
- `InternalGrantRow`
- `InternalGrantListResponse`
- `ExpandableGrantDef` — the identity-projection struct moves to `pkg/dotc1z` as `PendingExpansion` (different shape; see §4.3)

From `pkg/dotc1z/`:

- `(*C1File).ViewSync` — truly dead
- `(*C1File).OutputFilepath` — truly dead
- `(*C1File).GrantStats` — truly dead
- `(*C1File).SetSyncID` — truly dead (commented "testing only", no callers)
- `grants_expandable_query.go`: `GrantListModePayload` and `GrantListModeExpansion` case branches + supporting helpers
- Tests in `pkg/dotc1z/grants_test.go` that exercise the dead modes must be deleted alongside the modes. This is an explicit cost of this RFC; the v0 claim "tests pass unchanged" in §6.1 was wrong.

### Unexports (baton-sdk — methods called only by same-package code)

- `(*C1File).ResumeSync` → `resumeSync` (called by `StartOrResumeSync`)
- `(*C1File).ListSyncRuns` → `listSyncRuns` (called by `ListSyncs` Reader impl and by cleanup)
- `(*C1File).InitTables` → `initTables` (called by constructor)
- `(*C1File).DeleteSyncRun` → `deleteSyncRun` (called by cleanup)
- `(*C1File).Vacuum` → `vacuum` (called by cleanup)

### Renames

- `(*C1File).LatestSyncID` → `LatestFullSync` (moves to `SyncMeta`, returns `*SyncRun` not string; semantics clarified — today's "LatestSyncID" returns the latest finished sync id, which is the same as what `LatestFullSync` will return; name change prevents future confusion)
- `(*C1File).LatestFinishedSyncID` → `LatestFullSync` (same method — `LatestSyncID` and `LatestFinishedSyncID` are byte-identical on main)
- `(*C1File).GetLatestFinishedSync(proto req)` → stays on `*C1File` as the internal impl backing the gRPC Reader method; not exposed on `SyncMeta`. `SyncMeta.LatestFullSync` and `LatestFinishedSyncOfAnyType` are thin wrappers.
- `(*C1File).SetSupportsDiff` → `MarkSyncSupportsDiff` (moves to `SyncMeta`, same impl)
- `(*C1File).AttachFile` → moves to `pkg/synccompactor/attached.Attach` (no longer a `*C1File` method at all; see §4.4)
- `(*C1File).GenerateSyncDiff` — stays on `*C1File`, now accessed via `FileOps.GenerateSyncDiff`
- `(*C1FileAttached)` → `*synccompactor/attached.Attached` (moves out of `pkg/dotc1z`; see §4.4). Its `GenerateSyncDiffFromFile(ctx, old, new) (string, string, error)` becomes `GenerateSyncDiff(ctx, old, new) (SyncDiffResult, error)`.

## 6. Call-site rewrites

Every non-mechanical site. Mechanical find-and-replace for most; interesting cases called out.

**baton-sdk:**

| Site | Rewrite |
|---|---|
| `pkg/sync/syncer.go:185, 1547` | delete `latestSyncFetcher` interface; use `s.store.SyncMeta().LatestFullSync(ctx)` directly (fixes §4.7 bug) |
| `pkg/sync/syncer.go:275` | `s.store.GetLatestFinishedSync(...)` → `s.store.SyncMeta().LatestFullSync(ctx)` |
| `pkg/sync/syncer.go:1404` | `ListGrantsInternal(ExpansionNeedsOnly)` + pagination loop → `for pe, err := range s.store.Grants().PendingExpansion(ctx) { ... }` (iterator owns pagination) |
| `pkg/sync/syncer.go:2205` | `ListGrantsInternal(PayloadWithExpansion)` + pagination loop → `for ga, err := range s.store.Grants().ListWithAnnotations(ctx) { ... }` |
| `pkg/sync/syncer.go:1784, 1965, 2080, 2472` | `UpsertGrants(Replace)` → `s.store.PutGrants(ctx, grants...)` (stays on Writer) |
| `pkg/sync/parallel_syncer.go:213, 462` | `SetSupportsDiff(ctx, id)` → `s.store.SyncMeta().MarkSyncSupportsDiff(ctx, id)` |
| `pkg/sync/expand/expander.go:293` | `UpsertGrants(PreserveExpansion)` → `store.Grants().StoreExpandedGrants(ctx, grants...)` |
| `pkg/tasks/local/differ.go:64` | `GenerateSyncDiff` → `store.FileOps().GenerateSyncDiff(...)` |
| `pkg/synccompactor/naive/naive.go:76-88` | `PutXIfNewer(...)` → `dst.<TypeStore>().MergeNewestWins(...)` for each type (grants + resources + entitlements + resource_types all get a `MergeNewestWins` on their respective sub-stores) |
| `pkg/synccompactor/attached/attached.go` | full rewrite: new package API per §4.4. Callers do `attached.Attach(ctx, base, applied, "attached")` then `a.CompactGrants(...)` etc. |
| `pkg/synccompactor/compactor.go:193, 317` | `dotc1z.NewC1ZFile(...)` returns `*C1File` today; type signature becomes `C1ZStore` for the call-site variable |
| `pkg/synccompactor/compactor.go:208, 212, 226, 243, 248, 328` | all operate through `C1ZStore` sub-interfaces |
| `pkg/provisioner/provisioner.go` | no changes — uses `connectorstore.Reader`, which is untouched |
| `cmd/dev-util/test-uplift/runner.go:105` | `c.LatestSyncID(...)` → `c.SyncMeta().LatestFullSync(ctx).GetId()` (or similar — dev util can adapt) |

**c1:**

| Site | Rewrite |
|---|---|
| `pkg/temporal/sync_activity/sync/wrapper.go:182` | `ListGrantsInternal(PayloadWithExpansion)` + pagination → `for ga, err := range store.Grants().ListWithAnnotations(ctx) { ... }` |
| `pkg/temporal/sync_activity/sync/c1z.go:92` | `c1z.Stats(ctx, syncType, syncID)` → `c1z.SyncMeta().Stats(ctx, syncType, syncID)` |
| `pkg/temporal/sync_activity/sync/c1z.go:354` | `c1z.LatestSyncID(...)` → `c1z.SyncMeta().LatestFullSync(ctx)` |
| `pkg/temporal/sync_activity/sync/c1z.go:399` | `c1z.CloneSync(ctx, outPath, syncID)` → `c1z.FileOps().CloneSync(...)` |
| `pkg/temporal/sync_activity/sync/sync_v0.go:1139` | same `Stats` rewrite |
| `pkg/temporal/sync_activity/incremental-sync/incremental_sync.go:968` | same `Stats` rewrite |
| `pkg/temporal/sync_activity/incremental-sync/compact.go:759, 765, 772, 783` | full compactor rewrite: `c1z.AttachFile(...)` → `attached.Attach(...)`; return type becomes `*Attached`; `GenerateSyncDiffFromFile` → `GenerateSyncDiff` returning `SyncDiffResult` |
| `pkg/temporal/activity/app/uplift_fsm/runner/uplift_runner.go:651` | same `Stats` rewrite through the existing `c1zStatsProvider` interface (the interface definition adjusts) |
| `pkg/utest/fixtures/connector/c1z.go` | fixture helpers retype from `*C1File` to `C1ZStore` |
| `pkg/services/support_dashboard/http_admin_tenant_connector_compare.go:282` | `listC1ZResources(c1f *C1File, ...)` → retype to `connectorstore.Reader` (it only reads resources) |

Total: ~30 call-site rewrites across both repos. Every one mechanical or near-mechanical. No new semantics anywhere.

## 7. How "single atomic PR" actually works across two repos

v0 handwaved this. Concrete plan:

baton-sdk is a dependency; c1 vendors it. You can't land "atomically" across two repos in one git operation. What you can do:

1. **Open a draft PR on baton-sdk** with the interface changes. All tests green. Call sites inside baton-sdk rewritten.
2. **On a c1 branch,** `go mod edit -replace` to point at the baton-sdk branch locally; run `go mod vendor`; rewrite c1's call sites; get c1 tests green.
3. **Confirm both sides build and test** with the local replace wired.
4. **Merge the baton-sdk PR.** This produces a commit SHA.
5. **Immediately** remove the `replace` directive in c1, pin to the baton-sdk SHA, re-vendor, push c1's PR.
6. **Merge c1's PR** within the hour. During that hour, anyone pulling c1 and running `go mod tidy` will get a failure (baton-sdk moved but c1 didn't bump the pin). This is normal vendor-update discipline.

There is no world where both repos change "in a single commit." The plan above is what "atomic" means in practice — end-to-end validation happens pre-merge with the `replace` directive; the merge-then-pin flow produces a ~hour window of incoherence that's managed by team communication.

If that window is unacceptable (it usually isn't for this kind of refactor) we can feature-flag the new interface and land it as an additive change in baton-sdk, then switch c1 over, then delete the old API. Doubles the work, eliminates the window. My recommendation: accept the hour-long window.

## 8. External consumer impact

baton-sdk is public on GitHub. Deleting `connectorstore.InternalWriter` and the supporting types risks breaking out-of-tree consumers. Concrete mitigation plan:

1. **Pre-merge search.** GitHub code search:
   - `org:conductorone "connectorstore.InternalWriter"` / `"GrantUpsertOptions"` / `"GrantListOptions"`
   - Full GitHub search for the same strings (discoverable via the connector fork family)
   - pkg.go.dev reverse dependencies for `pkg/connectorstore`
2. **Audit internal connectors.** The `baton-*` connector family lives in conductorone/ GitHub org. Grep each.
3. **If any external consumer is found,** choose one of:
   - **deprecation window:** keep `InternalWriter` as a thin shim over `C1ZStore` for one release, mark it deprecated, delete in the next release.
   - **coordinate the update:** reach out to the owner, land the change in their repo first.

This RFC's stance: the chances a third-party *connector* imports `InternalWriter` are low (connectors use `Writer`, not the internal sync-pipeline escape hatch). But non-connector consumers (other internal tools, the conductorone CLI, etc.) deserve the 10 minutes of due diligence. If the pre-merge search finds nothing, we ship as a single PR with a note in the release notes. If it finds something, we go deprecation window.

## 9. Test strategy

Six layers in order of cost and confidence.

### 9.1 Existing sqlite tests — with a caveat

The existing `pkg/dotc1z/*_test.go` and `pkg/synccompactor/*_test.go` and `pkg/sync/*_test.go` tests exercise the current behavior and must pass after the rewrite. **Caveat:** tests that exercise the dead `GrantListMode{Payload,Expansion}` modes (`pkg/dotc1z/grants_test.go` around lines 999, 1131, 1139, 1158) cannot pass unchanged — they reference types this RFC deletes. The v0 RFC claim "tests pass unchanged" was wrong. Those specific tests must be deleted alongside the dead modes.

**Acceptance criterion:** `go test ./...` passes in both repos after all changes including the test deletions/rewrites. Any test that fails for a *behavior* reason (not a type-rename reason) is a red flag that we changed semantics, and we stop and investigate.

### 9.2 Interface conformance test

New test file `pkg/dotc1z/store_conformance_test.go` exercises every `C1ZStore`, `GrantStore`, `SyncMeta`, `FileOps` method against a freshly-created `*C1File`. Table-driven, ~20 cases, each ~30 lines.

Purpose: make the contract explicit and executable. When pebble lands (RFC 0003), this table runs against pebble too — so the investment pays forward.

### 9.3 Invariant tests for the semantics that were hidden in enums

Specifically targeted tests for behaviors the v0 RFC glossed over and that critical review flagged:

1. **`StoreExpandedGrants` strips `GrantExpandable` annotations before write.** Pass a grant that still has a `GrantExpandable` annotation (deliberately — as if a caller forgot to strip it); verify the stored `data` has no annotation; verify the `expansion`/`needs_expansion` columns are unchanged from whatever was there.
2. **`StoreExpandedGrants` does not disturb existing expansion columns.** First write a grant via `PutGrants` (which populates expansion columns); then write a modified grant via `StoreExpandedGrants`; verify expansion columns are unchanged.
3. **`MergeGrantsNewestWins` writes newer, skips older.** Seed with grant @ discovered_at=T; call with grant @ T-1 (should skip); call with grant @ T+1 (should update).
4. **`PendingExpansion` returns only needs_expansion=1 rows.** Seed with a mix of expandable-and-needs, expandable-but-done, and non-expandable grants; verify results.
5. **`ListWithAnnotations` returns annotation=nil for non-expandable grants.** Seed mixed; verify struct shape.
6. **`LatestFullSync` ignores in-progress syncs.** Seed with a completed full + an in-progress full; verify returns the completed one.
7. **`LatestFinishedSyncOfAnyType` includes diff syncs.** Seed with a full + a later diff; verify returns the diff.
8. **`MarkSyncSupportsDiff` persists across reopen.** Mark, close, reopen, verify flag.

These tests are ~300 lines total. Worth the investment — they document the contract.

### 9.4 Migration tests

Open a c1z written by main-branch code (committed as a binary test fixture); read it through the new `C1ZStore` interface; verify every sub-store method returns expected values. This catches the "we accidentally changed the on-disk format" class of bugs.

### 9.5 Call-site smoke tests

After each commit in the migration (§10), run:

- baton-sdk: `go test ./pkg/sync/... ./pkg/synccompactor/... ./pkg/provisioner/... ./pkg/dotc1z/...`
- c1: `go test ./pkg/temporal/sync_activity/... ./pkg/connector/v2/manager/... ./pkg/services/support_dashboard/...`

Must be green before each commit lands.

### 9.6 Property / fuzz — explicitly deferred

Out of scope. This is a refactor; existing tests are our semantic witnesses. Property tests can be added later against the now-stable interface.

## 10. Migration plan

Single baton-sdk PR + coordinated c1 PR. Commit sequence within baton-sdk:

1. **Add new types.** `C1ZStore`, `GrantStore`, `SyncMeta`, `FileOps` interfaces. `PendingExpansion`, `GrantAnnotation`, `SyncDiffResult`, `SyncRun` types. No implementations yet. Build is green (nothing uses them).
2. **Make `*C1File` satisfy the new interfaces.** Add the sub-store getters (`Grants() GrantStore`, etc.) returning `*C1File` itself narrowed by new sub-interfaces or narrow wrapper structs. Add the new method names (`StoreExpandedGrants`, `MergeGrantsNewestWins`, `PendingExpansion`, `ListWithAnnotations`, `MarkSyncSupportsDiff`, `LatestFullSync`, `LatestFinishedSyncOfAnyType`). Each is a thin wrapper over the existing internal impl, EXCEPT:
   - `StoreExpandedGrants` additionally strips `GrantExpandable` annotations (§4.8). New small helper `stripExpansionAnnotation`.
   - `LatestFullSync` and `LatestFinishedSyncOfAnyType` return the engine-native `*SyncRun` struct. The existing `GetLatestFinishedSync` (proto envelope) stays; the new methods construct the native struct from the DB row directly.
   - `ListWithAnnotations` and `PendingExpansion` return `iter.Seq2[T, error]`. Both internally wrap the existing page-token loop from `ListGrantsInternal` — each yield pulls the next page when the current page is exhausted. Go 1.25 (per `go.mod`) makes this idiomatic. One small helper `paginateToSeq2` factors the page-loop pattern so both methods share it.
   - Add `var _ C1ZStore = (*C1File)(nil)` assertion.
3. **Rewrite `pkg/sync/syncer.go`, `pkg/sync/parallel_syncer.go`, `pkg/sync/expand/expander.go`.** Change `s.store connectorstore.InternalWriter` to `s.store dotc1z.C1ZStore`. Rewrite all call sites per §6. Delete `latestSyncFetcher` local interface. Fix the silent-type-assertion bug.
4. **Rewrite `pkg/synccompactor/**`.** Move `AttachFile`-using code into `pkg/synccompactor/attached` per §4.4. Naive compactor uses `MergeNewestWins`. Main compactor retypes from `*C1File` to `C1ZStore`.
5. **Rewrite `pkg/provisioner`.** Should be no-op — it uses `connectorstore.Reader` which is unchanged.
6. **Delete dead code.** `connectorstore.InternalWriter` and supporting types. The truly-dead `*C1File` methods. The dead `GrantListMode{Payload,Expansion}` modes and their tests.
7. **Unexport** the package-internal-only `*C1File` methods per §5.
8. **Move `*C1File.AttachFile` and `*C1FileAttached` to `pkg/synccompactor/attached`** per §4.4. This is the largest commit — a file move plus signature changes plus the unexported `AsSQLiteStore` escape hatch.
9. **Add new tests.** Conformance test + invariant tests + migration test per §9.

Each commit builds (`go build ./...`) and tests green (`go test ./pkg/<touched>...`). Commit 2 is the keystone: once `*C1File` satisfies `C1ZStore`, everything downstream is incremental.

c1 PR happens in parallel, tested with `replace` directive pointing at the baton-sdk branch. When baton-sdk merges, c1 pins to the merge SHA and merges within the hour.

## 11. Risks and mitigations

| Risk | Mitigation |
|---|---|
| Rewriting a call site with the wrong new method (e.g. `StoreExpandedGrants` vs `PutGrants`) | §9.3 invariant tests catch semantic errors. The distinct names help reviewers eyeball each rewrite. |
| The coordinated merge window between baton-sdk and c1 breaks someone's CI | Standard vendor-update discipline. Keep the window short; communicate. |
| External baton-sdk consumer imports `connectorstore.InternalWriter` | §8 pre-merge search. If any hits, run a deprecation window. |
| `AttachFile` move breaks a caller we didn't inventory | The inventory in §3 claims 2 callers. Grep again at merge time; if a third appears, fix it in the same PR. |
| `StoreExpandedGrants` stripping annotation silently changes behavior if any current caller was relying on the old (data-with-annotation) shape | Today's sole caller (`expander.go:293`) writes grants that by construction have no annotation. §9.3 test 1 verifies. |
| ~~New `iter.Seq2` return type requires Go 1.23+~~ | **Resolved.** `go.mod` is on Go 1.25; `iter.Seq2` is native. Both list methods use iterators. |
| Missed a call site in §6 | CI catches at `go build ./...`. Smoke tests (§9.5) catch behavior-level misses. |

## 12. Open questions

1. **Should `SyncRun` be a new exported struct or lift an existing unexported type?** The existing `pkg/dotc1z/sync_runs.go` has `syncRun`. Proposal: export it as `SyncRun` with only the fields callers actually need (`Id`, `SyncType`, `EndedAt`, `StartedAt`, `ParentSyncId`, `SupportsDiff`). Other internal fields stay unexported on the same struct.
2. **Does `C1ZStore` belong in `pkg/dotc1z` or a new sub-package?** v0 said `pkg/dotc1z/engine.go`. Rename avoids that confusion. Proposal: `pkg/dotc1z/store.go`. The name `C1ZStore` makes the location obvious.
3. **Does `AsSQLiteStore(s C1ZStore) (*C1File, bool)` need to be public?** It needs to be callable from `pkg/synccompactor/attached`. Making it public is easy but blesses the pattern. Alternative: use a package-private interface and a build-time assertion that `*C1File` satisfies it; expose a public `RegisterStoreAttachHandler` pattern. Overthinking for today. Ship the public accessor, document it as "for compactor use."
4. **Should `Writer.PutGrants` be renamed for symmetry?** No — it's part of the connector-facing contract. Rename would break connectors. Leave it.

### Resolved

- **`iter.Seq2` availability.** `go.mod` requires Go 1.25 on `origin/main` (commit `b064cc2d`, April 2026). `iter.Seq2` is native. Both `PendingExpansion` and `ListWithAnnotations` return iterators.

## 13. Success criteria

- Every sync/compact/provisioner/c1-temporal call site uses `C1ZStore` (or one of its sub-interfaces), `connectorstore.Reader`, or `connectorstore.Writer`. No one type-asserts into `*C1File` except the unexported `AsSQLiteStore` gate in `pkg/synccompactor/attached`.
- `grep -r "InternalWriter\|GrantUpsertMode\|GrantListMode\|ExpandableGrantDef" pkg/` returns nothing in baton-sdk; similar for c1.
- The `latestSyncFetcher` bug is fixed; `syncer.getPreviousFullSyncID` returns real data for partial syncs.
- `go test ./...` passes in both repos.
- `go doc pkg/dotc1z.C1ZStore` produces useful output — every method on every sub-interface has a comment saying what it does and who calls it.
- Adding a new sync-pipeline feature requires either: (a) using existing methods, or (b) adding a named method to one of the sub-interfaces with reviewer pushback. No more enum-mode expansions.

---

*End of RFC 0002 — iteration 2.*
