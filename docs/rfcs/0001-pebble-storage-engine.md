# RFC 0001: Pluggable Storage Engine with Pebble Backend

- **Status:** Draft
- **Author:** morgabra (original), rebased/rewritten on `main` by ???
- **Target SDK version:** 0.8.x+1 (breaking internal APIs; connector-facing API preserved)
- **Prior art:** `morgabra/pebble` branch (commit `ae4b9693`, Aug 2025), `.kiro/specs/pebble-storage-engine/`
- **Related PRs on main:** #591 (parallel sync), #666 (expandable at SQL), #773 (zstd pool), #769/#768 (skip cleanup), #735 (size verification removal)

## 1. Summary

Introduce a storage-engine abstraction in `pkg/dotc1z/` so that the same sync, compaction, and read/write code paths can run against either the current SQLite-in-a-`.c1z` backend or a new Pebble-backed backend. Connectors continue to default to SQLite (unchanged binary footprint, no cgo regressions, full on-disk compatibility). Backend infra (where we own both read and write sides and want more throughput) can opt into the Pebble engine via explicit configuration.

## 2. Motivation

The SQLite-in-a-`.c1z` backend has served well but has become a bottleneck for backend infra:

- **Write throughput** on large tenants is limited by WAL contention, exclusive locks, and vacuum/checkpoint overhead. The recent perf work (#591, #773, #666) has helped but is increasingly stacking workarounds on a single-writer engine.
- **cgo** is a hard requirement for some sqlite drivers; `glebarez/go-sqlite` is cgo-free but pays a perf cost for it.
- **Range scans and prefix iteration** — the dominant access pattern for sync compaction, grant expansion, and resource listings — are a natural fit for an LSM/KV store but require careful query tuning in SQLite.
- **Compaction** between two c1z files requires SQL `ATTACH DATABASE`, tightly coupling us to sqlite as the inter-file merge mechanism.

An LSM engine (Pebble) gives us:

- Pure Go, no cgo, no system dependencies.
- Prefix iteration as the native primitive, which matches how `listConnectorObjects` and compaction scan data.
- Parallel batch commits without a global writer lock.
- Snapshot isolation for the view-sync read path.

We keep SQLite as the default because: it's battle-tested, the on-disk format is public and widely consumed, and connectors benefit from zero-churn.

## 3. Goals and non-goals

### Goals

1. **Pluggable engine boundary.** A single `engine.StorageEngine` interface that both SQLite and Pebble satisfy, placed at a layer that lets the rest of the SDK remain engine-agnostic.
2. **SQLite is the default, unchanged for connectors.** Existing `dotc1z.NewC1ZFile()` with no extra options produces an identical v1 `.c1z` file.
3. **Pebble is opt-in.** A new engine option on the c1z file controls which backend is used; when omitted, we pick SQLite.
4. **Versioned file format.** The on-disk file declares its format version and engine so we can detect and refuse mismatches cleanly, and so future format revisions are non-breaking to load (fail closed on unknown, open correctly on known).
5. **Preserve all main-branch behavior for SQLite.** The zstd pool (#773), skip-cleanup (#768/#769), column-name handling (#757), WAL-off managers (#733), parallel sync (#591), expandable-at-SQL (#666), session store, `InternalWriter` API — all continue to work.
6. **Honest compaction across engines.** Base and applied c1z must use the same engine for a given compaction run.

### Non-goals

1. **Cross-engine compaction.** Merging a sqlite base with a pebble applied is out of scope — callers must match engines.
2. **On-disk compatibility between engines.** A pebble `.c1z` is not readable as a sqlite `.c1z` and vice versa. The file header tells you which it is.
3. **Runtime engine hot-swap.** Once a file is created with an engine, it stays that engine.
4. **Making pebble available to connectors.** Connector binaries do not get a pebble code path in this RFC. Our backend infra binaries do. (Pebble can ship in the SDK as an internal/optional package; connectors simply never enable it.)
5. **New query patterns.** We do not add new methods; we port the existing `connectorstore.Reader`/`Writer`/`InternalWriter` contract onto a pluggable engine.

## 4. Current state (on `main`)

### Package layout

```
pkg/connectorstore/
    connectorstore.go          # Reader, Writer, InternalWriter, SyncType, Grant{List,Upsert}Options
pkg/dotc1z/
    dotc1z.go                  # NewC1FileReader, NewExternalC1FileReader, NewC1ZFileDecoder, C1ZFileCheckHeader
    c1file.go                  # C1File struct + options
    c1file_attached.go         # C1FileAttached (compaction via SQL ATTACH)
    assets.go entitlements.go grants.go resources.go resouce_types.go  # CRUD via goqu
    grants_expandable_query.go # Expandable-at-SQL (#666)
    session_store.go           # sessions.SessionStore on C1File
    sync_runs.go clone_sync.go diff.go decoder.go file.go
    sql_helpers.go             # column resolution + migrations
    pool.go                    # zstd encoder/decoder pool (#773)
    manager/
        local/, s3/            # Return (*C1File, error) from LoadC1Z
pkg/sync/syncer.go             # s.store connectorstore.InternalWriter
pkg/synccompactor/             # attached (ATTACH-based) + naive compactors
```

### The contract `pkg/sync/syncer.go` actually uses

Today `s.store` is `connectorstore.InternalWriter`. It calls:

- From `Reader`: all of the `*ServiceServer` list/get RPCs for resource types, resources, entitlements, grants, syncs, plus `GetAsset`, `Close(ctx)`.
- From `Writer`: `StartOrResumeSync`, `StartNewSync` (with `SyncType` + parent), `SetCurrentSync`, `CurrentSyncStep`, `CheckpointSync`, `EndSync`, `Cleanup`, `PutAsset`, `PutResourceTypes`, `PutResources`, `PutEntitlements`, `PutGrants`, `DeleteGrant`.
- From `InternalWriter`: `UpsertGrants(GrantUpsertOptions)`, `ListGrantsInternal(GrantListOptions)`, `SetSupportsDiff(syncID)`.
- Type-asserted capability: `latestSyncFetcher` (optional).

### What `C1File` exposes that is *not* on the interface

- `sessions.SessionStore` (Get/Set/Delete/...)
- `OutputFilepath()` (used by compaction to open sibling files)
- `NewAttachedC1File` (for compactor's SQL ATTACH dance)
- Direct SQL escape hatches used in compaction

## 5. Design

### 5.1 Package topology

```
pkg/connectorstore/           # unchanged — contracts consumed by ALL SDK callers
pkg/dotc1z/
    dotc1z.go                 # top-level NewC1ZFile; picks format + engine; returns Engine
    format.go                 # file header constants, format detection
    engine/
        engine.go             # StorageEngine interface (super-set of connectorstore.InternalWriter + file-ops)
        errors.go             # ErrEngineMismatch, ErrUnsupportedFormat
        sqlite/               # SQLite engine — everything currently at pkg/dotc1z/ root moves here
            c1file.go assets.go entitlements.go grants.go ... pool.go session_store.go ...
        pebble/               # Pebble engine — new
            engine.go keys.go values.go sync_runs.go grants.go ...
    format/                   # or merge into engine/; see §5.4 below
        v1/                   # sqlite-in-c1z format reader/writer
        v2/                   # new tar-manifest format for pebble (or pebble-in-c1z, TBD §5.4)
    manager/                  # unchanged public surface; returns engine.StorageEngine
```

The key move: `pkg/dotc1z` *itself* becomes a thin dispatch layer. All the SQL code moves under `pkg/dotc1z/engine/sqlite/`. The public `NewC1ZFile(...)` keeps its current signature and becomes a constructor that picks an engine.

### 5.2 The engine interface

```go
package engine

// StorageEngine is the pluggable backend contract. Every method the
// connector SDK sync/compact/query code currently calls on *dotc1z.C1File
// maps to a method here.
//
// Implementations: pkg/dotc1z/engine/sqlite.C1File and pkg/dotc1z/engine/pebble.Engine.
type StorageEngine interface {
    // Embeds main's internal contract verbatim — no reinvention.
    connectorstore.InternalWriter

    // File-level operations the sync/compact pipelines need beyond the reader/writer API.
    OutputFilepath() (string, error)
    Dirty() bool
    Stats(ctx context.Context) (map[string]int64, error)
    CloneSync(ctx context.Context, syncID string) (string, error)
    GenerateSyncDiff(ctx context.Context, baseSyncID, appliedSyncID string) (string, error)
    ViewSync(ctx context.Context, syncID string) error
    ListSyncRuns(ctx context.Context, pageToken string, pageSize uint32) ([]*SyncRun, string, error)
}

// AttachedEngine is the same-engine merge surface used by the attached compactor.
// Implementations must reject merges between different engine types.
type AttachedEngine interface {
    CompactResourceTypes(ctx context.Context, destSyncID, baseSyncID, appliedSyncID string) error
    CompactResources   (ctx context.Context, destSyncID, baseSyncID, appliedSyncID string) error
    CompactEntitlements(ctx context.Context, destSyncID, baseSyncID, appliedSyncID string) error
    CompactGrants      (ctx context.Context, destSyncID, baseSyncID, appliedSyncID string) error
    CompactTable       (ctx context.Context, destSyncID, baseSyncID, appliedSyncID, tableName string) error
    Detach() (StorageEngine, error)
}

// Attacher lets a StorageEngine temporarily couple itself to another for merge.
type Attacher interface {
    Attach(other StorageEngine, dbName string) (AttachedEngine, error)
}
```

Design notes:

- We **embed `connectorstore.InternalWriter`** rather than re-define the method set. That means `pkg/sync/syncer.go`'s existing `s.store connectorstore.InternalWriter` field keeps working. Callers that want engine-specific behavior up-cast to `engine.StorageEngine`.
- **Sessions stay off the engine interface.** The session store is a separate concern. The SQLite engine implements `sessions.SessionStore` as it does today; the Pebble engine either implements it natively (KV is a natural fit) or returns a "not supported" `SessionStore` stub. The syncer receives `sessions.SetSessionStore` as an independent option, so this decoupling already matches main.
- **Attach is an optional capability.** Not every engine has to implement cross-file attach; the compactor checks with a type assertion and falls back to the naive compactor if unavailable. Both sqlite and pebble will implement `Attacher`, but they can only attach to peers of the same kind (pebble-to-pebble, sqlite-to-sqlite). `Attach` returns `ErrEngineMismatch` otherwise.

### 5.3 Engine selection at c1z-open time

The engine is chosen at the `dotc1z.NewC1ZFile` call site. Every call decides: "am I reading? am I writing? what engine do I want for each?" There is no global setting, no env var, no implicit magic. The caller knows what it's doing because the ingestion pattern is asymmetric — we **read** connector-produced c1z (sqlite), then **write** a server-side c1z that we control (pebble).

#### Read side: follow the file

When opening an existing c1z, the engine is dictated by the file's header — we don't get to pick. v1 files are sqlite, full stop. v2 files declare their engine in the manifest. Caller intent only matters for rejecting unexpected engines.

```go
// Default: open with whatever engine the file declares.
f, err := dotc1z.NewC1ZFile(ctx, path, dotc1z.WithReadOnly(true))

// Caller can constrain: "I only know how to deal with sqlite". If the file
// is pebble, fail fast with ErrEngineMismatch rather than surprise the caller.
f, err := dotc1z.NewC1ZFile(ctx, path,
    dotc1z.WithReadOnly(true),
    dotc1z.WithRequireEngine(dotc1z.EngineSQLite),
)
```

#### Write side: caller picks

When creating a new c1z (or opening one for write that doesn't yet exist), the caller picks the engine. Default is sqlite to preserve existing behavior.

```go
// Default behavior — unchanged: write a v1 sqlite c1z.
f, err := dotc1z.NewC1ZFile(ctx, destPath)

// Backend infra explicitly requests pebble:
f, err := dotc1z.NewC1ZFile(ctx, destPath, dotc1z.WithEngine(dotc1z.EnginePebble))
```

#### The asymmetric case: read sqlite, write pebble

The common backend pattern is: "ingest a sqlite c1z produced by a connector; re-save it as a pebble c1z that lives in our storage." This is two separate `NewC1ZFile` calls with two separate decisions:

```go
// 1. Read the connector's c1z — sqlite, per the file header.
src, err := dotc1z.NewC1ZFile(ctx, connectorC1ZPath, dotc1z.WithReadOnly(true))
if err != nil { return err }
defer src.Close(ctx)

// 2. Create the server-side c1z — pebble, by our choice.
dst, err := dotc1z.NewC1ZFile(ctx, serverC1ZPath,
    dotc1z.WithEngine(dotc1z.EnginePebble),
)
if err != nil { return err }
defer dst.Close(ctx)

// 3. Copy data through the engine-agnostic interface.
//    This is a normal List/Put loop, not a file-level operation. Works across engines.
if err := engine.CopyAll(ctx, src, dst); err != nil { return err }
```

Open question: do we provide `engine.CopyAll` as a utility, or let each call site write its own loop? Proposal: ship `engine.CopyAll(ctx, src, dst StorageEngine) error` because it's the primary server-side pattern and we want exactly one implementation. See §5.10.

#### Option summary

| Option | Read | Write | Default |
|---|---|---|---|
| `WithEngine(name)` | ignored (file dictates) | chooses engine | `EngineSQLite` |
| `WithRequireEngine(name)` | hard gate — fail if file engine differs | ignored | unset |
| `WithFormat(format)` | ignored (file dictates) | picks v1 vs v2 envelope | `FormatV1` for sqlite; sqlite engine can opt into v2. Pebble engine forces v2. |
| `WithReadOnly(bool)` | locks to read mode | must be false | false |
| `WithTmpDir`, `WithPragma`, etc. | forwarded to engine | forwarded to engine | engine-specific defaults |

No env var, no global flag. The caller picks the engine at each open.

### 5.4 On-disk file format

We keep the v1 format **exactly as it is today** — no touching it, no re-encoding, no migration pass. Existing `.c1z` files continue to open with zero observable change, forever. Everything new (pebble, future engines, shards, per-file metadata) rides on a new v2 layout.

#### v1: preserved as-is

Today's format:

```
+-------------------+
| C1ZF\x00          |  5-byte magic
+-------------------+
| zstd payload      |  compressed sqlite db
+-------------------+
```

Current header is detected by `pkg/dotc1z/decoder.go:ReadHeader()`. It stays exactly that. Load-path: read 5 bytes, match `C1ZF\x00`, hand the remainder to the zstd decoder, land a sqlite file on disk, open it with the sqlite engine. No changes.

Writing v1 also stays the default for now (see §5.3 — sqlite is the default engine). A caller who does `dotc1z.NewC1ZFile(ctx, "/path.c1z")` with no options gets bit-identical output to main today. That protects every connector binary ever shipped.

#### v2: header + proto manifest + engine payload

New format, used when an engine other than "sqlite-in-v1" is selected, or when the caller explicitly opts into v2:

```
+---------------------------------------+
| C1Z2\x00                              |  5-byte magic — distinct from v1's C1ZF
+---------------------------------------+
| uint32 BE: manifest length (N)        |  4 bytes
+---------------------------------------+
| proto-encoded C1ZManifest             |  N bytes
+---------------------------------------+
| engine payload                        |  opaque bytes, engine decides the layout
+---------------------------------------+
```

**Why this shape:**

- **Magic is still 5 bytes**, distinct from v1. A cheap `bytes.Equal` over the first 5 bytes routes to v1 or v2; no ambiguity. Unknown magic → `ErrInvalidFile`, same as today.
- **Manifest is proto, not JSON.** Because we already have protobuf everywhere in this repo, generators and wire-compat tooling are free. Reserves room for backward-compatible additions (new fields are silently ignored by older readers that shouldn't be reading them anyway).
- **Manifest length is explicit.** Readers can slice out the manifest deterministically without parsing. Engine payload starts at a known offset.
- **Engine payload is opaque.** The dotc1z layer doesn't try to be clever about it. Each engine decides — sqlite can wrap a zstd-compressed db exactly like v1 does today (KISS), pebble can pick whatever archive + compression suits its access patterns. Future engines get to design their own payload without changing the envelope.

#### The C1ZManifest proto

New proto at `proto/c1/c1z/v2/manifest.proto`:

```proto
syntax = "proto3";
package c1.c1z.v2;
option go_package = "github.com/conductorone/baton-sdk/pb/c1/c1z/v2";

import "google/protobuf/timestamp.proto";

// C1ZManifest describes the engine and payload inside a C1Z2 file.
// Written immediately after the 5-byte magic + 4-byte length prefix.
message C1ZManifest {
  // Engine name, e.g. "sqlite", "pebble". Routes NewC1ZFile to the right impl.
  string engine = 1;

  // Engine-specific schema version. Interpreted only by the matching engine.
  // Engines SHOULD bump this when their payload layout changes incompatibly.
  uint32 engine_schema_version = 2;

  // Freeform engine config, opaque to the dotc1z layer.
  // sqlite: unused (empty). pebble: could carry cache-size hints, key-prefix
  //         version, shard count, etc. Engines define their own sub-messages.
  google.protobuf.Any engine_config = 3;

  // Payload encoding of the bytes that follow this manifest.
  // Lets us change compression scheme without a new envelope version.
  PayloadEncoding payload_encoding = 4;

  // Creation metadata. Informational only.
  google.protobuf.Timestamp created_at = 10;
  string created_by_sdk_version = 11;  // e.g. "baton-sdk/0.9.0"
  string created_by_tool = 12;         // e.g. "baton-connector-xyz"
}

enum PayloadEncoding {
  PAYLOAD_ENCODING_UNSPECIFIED = 0;
  PAYLOAD_ENCODING_RAW         = 1;  // engine payload is not further wrapped
  PAYLOAD_ENCODING_ZSTD        = 2;  // engine payload is zstd-compressed
  PAYLOAD_ENCODING_ZSTD_TAR    = 3;  // zstd-compressed tar (for multi-file engines like pebble)
}
```

Design notes on the manifest:

- **Engine is a string, not an enum.** New engines land without regenerating the proto. The dotc1z layer keeps a registry (`map[string]engine.Constructor`) and looks up by name. Unknown engine → `ErrUnsupportedEngine` at load time with a clear message.
- **`engine_schema_version` is per-engine.** The dotc1z layer never interprets it; it hands the manifest to the engine and the engine decides whether it can open the payload. This lets sqlite and pebble evolve independently without a central version-compat matrix.
- **`engine_config` is `Any`.** Engines that need metadata embed a proto sub-message here. Engines that don't (sqlite in its minimal form) leave it empty.
- **`payload_encoding` is in the envelope, not per-engine.** This is deliberate: the dotc1z layer handles decompression uniformly via shared streaming helpers (§5.11) before handing bytes to the engine. Engines never implement `payload_encoding` switching themselves — they receive a clean, already-decoded `io.ReadSeeker` positioned at the logical start of their payload.

#### sqlite under v2: KISS

The sqlite engine, under v2, does the obvious minimal thing:

- Manifest: `engine = "sqlite"`, `engine_schema_version = 1`, `engine_config = {}` (empty).
- Payload encoding: `PAYLOAD_ENCODING_ZSTD`.
- Payload: zstd-compressed sqlite db file. Same compression, same db schema, same migration tables as v1.

In other words: a v2 sqlite `.c1z` is byte-identical to a v1 `.c1z` starting from the payload boundary. The only difference is the 5-byte magic + 4-byte length + serialized `C1ZManifest` prefix. No new sqlite code paths for load or save — they delegate to the same zstd encode/decode that v1 uses.

**sqlite does not need to use v2.** We default sqlite-backed c1z files to v1 to maximize compatibility with anything that reads them today. v2 is only chosen when: (a) the caller explicitly opts in via `WithFormat(FormatV2)`, or (b) the engine requires it (pebble always does).

#### pebble under v2: optimize freely

Pebble is a directory of files (`OPTIONS-*`, `MANIFEST-*`, `*.sst`, `CURRENT`, `WAL`), so the payload is a multi-file blob. Concrete proposal:

- Manifest: `engine = "pebble"`, `engine_schema_version = 1`, `engine_config = c1.c1z.v2.pebble.Config{...}`.
- Payload encoding: `PAYLOAD_ENCODING_ZSTD_TAR`.
- Payload: zstd-compressed tar stream of the pebble directory.

Optional optimizations that become possible because pebble controls its own payload shape:

- **Skip WAL in the payload.** If pebble is cleanly closed before save, the WAL is redundant; we can exclude it and rebuild on next open. Smaller files, faster compaction.
- **Store SST files uncompressed inside the tar** if pebble has already compressed them internally (they're LSM blocks), then zstd the tar only for the manifest/options text files. Trade-off: simpler implementation up front is "compress everything." A future perf PR can split.
- **Skew the archive layout for streaming reads.** Put `MANIFEST` and `OPTIONS` at the front of the tar so a reader can peek metadata without decompressing the whole thing. This makes `ViewSync` / header-only reads fast.
- **Engine config carries pebble format version.** `c1.c1z.v2.pebble.Config { uint32 format_major_version = 1; uint64 cache_size_bytes = 2; ... }` — embedded in `engine_config` as `Any`. Pebble's `format_major_version` is a meaningful compat gate (LSM format changes); embedding it here means a newer pebble binary refuses to open a file written by a much newer format with a clear error, instead of mysteriously corrupting.

None of these optimizations need to land in the first pebble PR. They become possible because v2 gives pebble room to design.

#### Load path

The dotc1z layer does all the envelope-level work — header sniffing, manifest unmarshal, decompression wrapping — and hands the engine a clean `io.ReadSeeker` positioned at the logical start of its payload. Engines never touch zstd, tar, or framing; they just read bytes.

```go
// pkg/dotc1z/format.go (new)

func ReadHeader(r io.Reader) (Format, error) { ... }  // v1 | v2 | unknown

// v1 path (unchanged from today)
//   ReadHeader → v1 → zstdstream.NewReader(...)          // shared streaming helper
//     → sqlite engine gets io.ReadSeeker over the decoded db bytes

// v2 path (new)
//   ReadHeader → v2
//     → read uint32 length (streaming, small read)
//     → read manifest bytes → proto.Unmarshal into C1ZManifest   (buffered; manifests are tiny)
//     → dispatch: registry[manifest.engine] → engine constructor
//     → wrap remaining stream per manifest.payload_encoding via shared middleware (see §5.11)
//         PAYLOAD_ENCODING_RAW       → pass-through
//         PAYLOAD_ENCODING_ZSTD      → zstdstream.NewReader(...)
//         PAYLOAD_ENCODING_ZSTD_TAR  → zstdstream.NewReader(...); engine walks tar itself
//     → engine.Open(ctx, OpenParams{
//           Manifest:   *C1ZManifest,   // config lives here as Any
//           Payload:    io.ReadSeeker,  // decoded bytes, positioned at 0
//           TmpDir:     string,         // for materializing onto disk if needed
//       })
```

**Why `io.ReadSeeker`, not `io.Reader`.** Both sqlite and pebble need to splat the payload onto local disk before opening (sqlite: write out the .db file; pebble: `tar.Extract` the pebble directory). Seekability matters for two reasons:

1. **Range reads.** Pebble's future optimizations (§5.4 "pebble under v2") put `MANIFEST`/`OPTIONS` at the front of the tar so a reader can peek metadata cheaply; that's only useful with a seekable payload.
2. **Retry/re-read safety.** If materialization fails mid-stream (disk full, cancelled ctx), the engine can seek back to 0 and try again without forcing the dotc1z layer to re-open the file and re-decompress from scratch.

If an engine gets a non-seekable source upstream (e.g. HTTP body), the dotc1z layer materializes to a temp file first and hands back an `*os.File` wrapped in the decompression middleware. Engines never have to care about the difference. This cost is already paid today by `decompressC1z` in `pkg/dotc1z/file.go` — we're just moving it up one layer and making it reusable.

**Position in the stream.** `Payload` is positioned at offset 0 of the *decoded, engine-visible* bytes. From the engine's perspective, it's reading its own private file. The envelope, manifest, and compression layer are invisible.

The dotc1z layer never inspects the engine payload as anything other than an opaque byte stream after the decompression wrapper is applied. Engines own their own extraction/open logic from that point on.

#### Writing

Mirror:

```go
// pkg/dotc1z/format.go

// WriteV1(w, payload) writes the C1ZF header + zstd'd payload. Used by the sqlite engine
// when format is v1.
func WriteV1(w io.Writer, rawSqliteDB io.Reader) error { ... }

// WriteV2(w, manifest, payload) writes C1Z2 + length + manifest + payload. Payload encoding
// is controlled by manifest.PayloadEncoding.
func WriteV2(w io.Writer, manifest *c1zv2.C1ZManifest, payload io.Reader) error { ... }
```

Each engine provides its payload to one of these writers on close. Dotc1z doesn't need to know what's inside.

#### Version negotiation

Rules:

1. Sqlite engine: v1 by default. `WithFormat(FormatV2)` opts into v2. Supports both on load.
2. Pebble engine: v2 always. Refuses v1 with `ErrEngineFormatMismatch`.
3. Future engine: declares its own supported format set. Writers pick the lowest supported version that satisfies all constraints.
4. Reader side: pick format from file header, hand to matching engine. Engine-format mismatch is an error, never a silent coerce.

#### What this buys us

- **Zero risk to existing `.c1z` consumers.** Nothing about v1 changes.
- **Pebble gets to design its on-disk layout properly**, including skipping WAL, per-file compression, streamable metadata, format_major_version gating.
- **Future engines (in-memory, sharded, remote) slot in** by registering a constructor and picking an `engine_config` proto; envelope doesn't change.
- **Per-file metadata is available** (creation time, SDK version, tool name) without hacks. Useful for debugging ops incidents where you need to know who wrote a file.
- **Compression policy lives in the envelope**, so a future change to "use something newer than zstd" only touches `PayloadEncoding` and the writer/reader helpers.

Caveat: v2 adds complexity vs. "just bump the magic." We're choosing the complexity deliberately because we're committing to multiple engines long-term, and we don't want to design a v3 envelope the next time someone adds metadata.

### 5.5 Migration: how we get to this cleanly on top of main

Because the branch's refactor predates 7 months of SQLite improvements, we **do not replay the branch's diff**. Instead we redo the refactor on top of `main`:

1. **Commit 1 — interface skeleton.** Add `pkg/dotc1z/engine/engine.go`, `consts.go`, `errors.go`. No behavior change. No new imports in existing code.
2. **Commit 2 — move sqlite engine.** `git mv` each of `pkg/dotc1z/*.go` (except `dotc1z.go`, `format.go`, `manager/`) into `pkg/dotc1z/engine/sqlite/`, rename `package dotc1z` → `package sqlite`, adapt imports. Preserve *all* main-branch behavior: pool, session store, attached, column validation, skip-cleanup, etc. Rename `C1File` to `sqlite.C1File` (or `sqlite.Engine`) and make it implement `engine.StorageEngine`.
3. **Commit 3 — adapt top-level.** `pkg/dotc1z/c1file.go` becomes a thin wrapper that constructs the chosen engine. Preserve *every* existing `C1FOption` by forwarding it to the sqlite engine. External callers see no API break (`dotc1z.NewC1File` still returns something that satisfies `connectorstore.InternalWriter`; type is `engine.StorageEngine` under the hood).
4. **Commit 4 — update internal callers.** `pkg/sync/syncer.go` keeps `s.store connectorstore.InternalWriter` (no change). `pkg/synccompactor/**` switches from `*dotc1z.C1File` to `engine.StorageEngine` and uses `engine.Attacher`/`engine.AttachedEngine` for the attached compactor. `pkg/dotc1z/manager/*` returns `engine.StorageEngine` instead of `*dotc1z.C1File`.
5. **Commit 5 — v2 format envelope.** Add `proto/c1/c1z/v2/manifest.proto`, regenerate, add `pkg/dotc1z/format.go` with v1/v2 `ReadHeader` + `WriteV1`/`WriteV2`, engine registry, and route v2 `engine = "sqlite"` to the existing sqlite engine (sqlite gains a v2 read/write path that's a thin wrapper over its existing zstd codec). Pebble side stubbed — the registry returns `ErrUnsupportedEngine` for `engine = "pebble"`. Add tests: v1 files roundtrip unchanged; v2-with-sqlite files roundtrip and contain bit-identical payloads to v1 starting from the payload offset.
6. **Commit 6 — pebble engine.** Port `pkg/dotc1z/engine/pebble/**` from the branch, *but* bring it up to the main interface: implement `UpsertGrants`, `ListGrantsInternal`, `SetSupportsDiff`, `StartOrResumeSync`, new sync types, `Close(ctx)`. Wire the header route.
7. **Commit 7 — vendor regen.** `go mod tidy && go mod vendor`.
8. **Commit 8 — tests.** Bring over the pebble test suite; add cross-engine tests that exercise the interface parity.

Every commit is independently `go build ./...` + `go test ./pkg/dotc1z/...` green.

### 5.6 What SQLite keeps that Pebble must also implement

Complete list the Pebble engine has to add versus the branch's version:

| Method | Why |
|---|---|
| `UpsertGrants(ctx, GrantUpsertOptions, ...*v2.Grant)` | #666 expandable-at-SQL equivalent; used by sync + expansion |
| `ListGrantsInternal(ctx, GrantListOptions)` | Replaces ad-hoc listing; used 3× in syncer |
| `SetSupportsDiff(ctx, syncID)` | Diff sync (#654-ish) |
| `ResumeSync(ctx, SyncType, syncID)` | Resume path |
| `StartOrResumeSync(ctx, SyncType, syncID)` | Primary syncer entry |
| `StartNewSync(ctx, SyncType, parentSyncID)` | Signature differs from branch's no-arg version — needs ctor change |
| `Close(ctx)` | Signature differs from branch's `Close()` |
| `SyncTypeResourcesOnly`, `SyncTypePartialUpserts`, `SyncTypePartialDeletions` | New sync types — must be understood by sync_runs encoding |
| `sessions.SessionStore` impl (Get/GetMany/Set/SetMany/Delete/Clear/GetAll) | **Required.** Pebble is a KV store — this is its native shape. See below. |

The branch's pebble code has stubs for none of these. They need real implementations. For a KV store most of them are structurally simpler than the SQLite versions.

#### Session store on pebble: required

The `sessions.SessionStore` interface (`pkg/types/sessions/sessions.go`) is:

```go
type SessionStore interface {
    Get(ctx, key, ...opt) ([]byte, bool, error)
    GetMany(ctx, keys, ...opt) (map[string][]byte, []string /*missing*/, error)
    Set(ctx, key, value, ...opt) error
    SetMany(ctx, values, ...opt) error
    Delete(ctx, key, ...opt) error
    Clear(ctx, ...opt) error
    GetAll(ctx, pageToken, ...opt) (map[string][]byte, string /*nextToken*/, error)
}
```

SQLite today implements this in `pkg/dotc1z/session_store.go` over a `connector_sessions` table keyed by `(sync_id, key)`. On pebble this is a direct mapping:

- Key layout: `"session/" + syncID + "/" + prefix + key`. Same prefix-scan pattern already used for resources/grants in the branch's `pkg/dotc1z/engine/pebble/keys.go`.
- `Get`/`Set`/`Delete`: point Get/Set/Delete on the composed key. Pebble batches for `SetMany`.
- `GetMany`: issue point gets in parallel, or a merging iterator over a sorted subset of keys.
- `Clear`: `DeleteRange` on the `"session/" + syncID + "/" + prefix` span — a single LSM primitive, much cheaper than the sqlite equivalent which has to delete row-by-row.
- `GetAll`: range iterator with cursor-based pagination keyed on the last-returned key. The 4 MiB gRPC payload limit (`MaxSessionStoreSizeLimit` in `sessions.go`) applies identically — helpers compose the paginator at the engine layer.

**Size watch.** Session values can be up to 4 MiB each. Pebble is fine with large values but they bypass the block cache and hit I/O for every point lookup. If we see hot large sessions we add a size metric via the observability manager the branch already scaffolded; optionally split very large values using chunked keys (`key#0`, `key#1`, ...). That's a §5.6 follow-up, not a v1 requirement.

**Interface hook.** The session store is not on `engine.StorageEngine` directly; it stays on the concrete engine type, same as today. The syncer receives a `sessions.SetSessionStore` SyncOpt that calls into whichever engine is behind the connector store — no interface churn required.

### 5.7 Compaction across engines

The `attached` compactor relies on SQL `ATTACH DATABASE` — a sqlite-specific mechanism. For pebble we implement a "streaming attach":

- `pebble.Engine.Attach(other StorageEngine, dbName string)` type-asserts that `other` is also `*pebble.Engine`; otherwise returns `ErrEngineMismatch`.
- The returned `AttachedEngine` operates by iterating ranges from `other` and batch-writing into the current engine, keyed by a prefix derived from `dbName`.
- `CompactGrants` etc. become prefix-scan + merge operations, which is a natural LSM pattern.

Callers in `pkg/synccompactor/compactor.go` already check the engine type implicitly (they pass three instances of the same type into `NewAttachedCompactor`). We tighten that by having the compactor type-assert `engine.Attacher` and error cleanly on mismatch.

### 5.8 Caller-visible API changes

- `dotc1z.NewC1ZFile` return type changes from `*dotc1z.C1File` to `engine.StorageEngine`. Since `C1File` implemented the interface and nothing in the public API depended on a concrete pointer type except `c1file_attached.go`, this is source-compatible for 99% of callers. Internal callers in `pkg/synccompactor` and `pkg/dotc1z/manager` get updated in the same PR.
- `dotc1z.WithEngine(name string)` — new option. Default `"sqlite"`.
- `dotc1z.EnginePebble`, `dotc1z.EngineSQLite` — exported constants.
- All `dotc1z.WithC1F*` options continue to work and are forwarded to the sqlite engine. Pebble-specific options will live in `pkg/dotc1z/engine/pebble` (e.g. `pebble.WithCacheSize`) and be surfaced from the top-level via an opaque forward option or a `WithPebbleOption(...)` variadic.
- `pkg/connectorstore` unchanged.

### 5.9 Test strategy

- **Sqlite regression.** The existing `pkg/dotc1z/*_test.go` suite, post-move, must pass against `engine.sqlite.C1File` with zero behavior change. This is the primary safety net.
- **Interface parity test.** A table-driven test in `pkg/dotc1z/engine` that constructs both engines and runs identical read/write workloads, comparing outputs. Skipped if pebble is compile-tagged off.
- **Compaction cross-check.** The existing compactor tests run twice: once against sqlite, once against pebble. Mixed-engine input produces `ErrEngineMismatch`.
- **Fuzzed sync runs.** The branch's benchmark tests (`pkg/dotc1z/engine/benchmarks/`) port over as correctness tests in CI and perf tests run manually.
- **Manager tests.** Local and S3 managers work transparently regardless of engine choice.

### 5.10 Engine-agnostic copy

The server-side pattern "read connector sqlite, write server-side pebble" (§5.3) is common enough to deserve exactly one implementation. We ship `engine.CopyAll`:

```go
// CopyAll copies all data from src to dst via the engine.StorageEngine interface.
// Both engines can be any type — sqlite→pebble, pebble→pebble, sqlite→sqlite, etc.
// Works by starting a new sync on dst and replaying src's latest sync through
// paginated list → batched put calls.
func CopyAll(ctx context.Context, src, dst StorageEngine, opts ...CopyOption) error { ... }
```

Semantics:

- Reads src's latest finished sync (subject to `WithSourceSyncID(id)` override).
- Starts a new sync on dst with the same `SyncType` (or `WithDestSyncType(...)` override) and parent pointer.
- Streams resource types → resources → entitlements → grants → assets in paginated batches. Default batch size tuned to 1000 (matches #736).
- Ends the dst sync and calls `Cleanup` on dst.
- On any error, aborts the dst sync so it's not marked complete. Callers clean up partials normally via `Cleanup`.
- Sessions are **not** copied — they're per-sync scratch, not durable data. If a caller needs them copied they can invoke the session store explicitly.

Open question: should `CopyAll` detect same-engine + same-format pairs and short-circuit to a byte-level copy (e.g. `io.Copy` on the c1z file)? Proposal: no, keep `CopyAll` purely through the engine interface so behavior is predictable. If byte-level is wanted, callers use `os.Rename`/`cpFile` on the file path.

### 5.11 Shared input middleware for payload decoding

Because this is a Go monorepo we can share streaming helpers across all c1z readers/writers instead of duplicating decompression logic into each engine. The dotc1z layer owns a small middleware stack; engines consume the top of the stack as an `io.ReadSeeker`.

Lives in `pkg/dotc1z/format/` (or extracted to `pkg/zstdstream/` if other packages want it):

```go
package format

// PayloadReader wraps a raw c1z file stream and returns a ReadSeeker positioned
// at the first byte of the engine payload, with all envelope+compression peeled off.
// Reuses the existing pooled zstd decoder from pkg/dotc1z/pool.go (#773) so we
// inherit the 20,785× allocation reduction from that PR.
func OpenPayload(ctx context.Context, r io.ReadSeeker) (*PayloadReader, error)

type PayloadReader struct {
    Format    Format                  // v1 or v2
    Manifest  *c1zv2.C1ZManifest      // nil if v1
    Payload   io.ReadSeeker           // decoded engine bytes, positioned at 0
}

// Close releases any pooled resources (zstd decoder, tempfile handles, ...).
func (p *PayloadReader) Close() error
```

**Write side mirror:**

```go
// PayloadWriter accepts the engine's raw payload bytes and wraps them with
// compression + envelope per the caller's format/manifest choices. Uses the
// pooled zstd encoder.
type PayloadWriter struct { ... }

func WriteV1(w io.Writer, payload io.Reader) error
func WriteV2(w io.Writer, manifest *c1zv2.C1ZManifest, payload io.Reader) error
```

**Why shared middleware, not engine-owned decompression:**

1. **One place to get zstd right.** Today's main uses pooled encoders/decoders (#773), a 3 GiB max-decoded-size limit, a 128 MiB max-memory limit, plus env-var overrides (`BATON_DECODER_MAX_DECODED_SIZE_MB`, `BATON_DECODER_MAX_MEMORY_MB`). If each engine re-implemented this, we'd re-learn every bug. Share the helper.
2. **Engines stay tight.** Pebble's open path becomes "here's an `io.ReadSeeker` of a tar stream" → `tar.NewReader` → extract. Sqlite's open path becomes "here's an `io.ReadSeeker` of a sqlite db" → `io.Copy` into a temp file → `sql.Open`. Neither engine imports `klauspost/compress/zstd`.
3. **Future-proofing.** If we add a new `PayloadEncoding` (say zstd → something newer), we change the middleware once, all engines keep working.
4. **Observability.** One helper = one place to record decompression metrics, trace spans, slow-read logs. Engines get that for free.

**What the engine sees (contract):**

```go
// engine.Engine constructor (roughly)
type OpenParams struct {
    Ctx       context.Context
    Manifest  *c1zv2.C1ZManifest  // nil if format == v1 (sqlite reads legacy v1 via its own shim)
    Payload   io.ReadSeeker       // decoded, positioned at 0, seekable. Engine may materialize.
    TmpDir    string              // staging dir the engine owns for the life of the open
    Options   EngineOptions       // engine-specific (cache size, pragmas, ...)
}

func (sqlite.Engine) Open(p OpenParams) error { ... }  // writes p.Payload → tmpDir/db, sql.Open
func (pebble.Engine) Open(p OpenParams) error { ... }  // tar.Extract(p.Payload) → tmpDir, pebble.Open
```

Engines never see compression, framing, or the envelope. They see bytes.

**Handling non-seekable sources.** If the upstream passes an `io.Reader` that isn't seekable (e.g. an HTTP body piped straight in), `OpenPayload` materializes to a temp file first and returns a `*os.File`-backed `PayloadReader`. Callers aren't required to provide seekability up front; the middleware buys it if needed. Cleanup is via `PayloadReader.Close()`.

**Performance note.** For pebble specifically, the tar layout (§5.4 "pebble under v2") puts small metadata files at the front. `Payload.Seek(0, io.SeekStart)` + `tar.NewReader` + `Next()` reads a dozen bytes to grab the first entry — no full decompression required just to validate or peek metadata. This is the downstream benefit of keeping `Payload` as a `ReadSeeker` instead of forcing full materialization up front.

## 6. Open questions

1. **`OutputFilepath()` semantics for pebble.** Pebble is a directory, not a file. For the sync pipeline we compress to a single `.c1z` at `Close()` time, so `OutputFilepath()` returns the target compressed path — same semantics as sqlite. Confirm that's acceptable.
2. **Pebble schema versioning.** `engine_schema_version` in the manifest plus pebble's own `format_major_version` embedded in `engine_config`. Probably also embed a `__schema_version` key inside the pebble keyspace as belt-and-suspenders. Nailed down in the implementation PR.
3. **v2 default for sqlite?** Today: v1. Proposal in §5.4 keeps sqlite on v1 by default indefinitely. Alternatively, we could flip the sqlite default to v2 once the backend has adopted it (6 months out?), at which point all new files have the richer metadata. Question: is there ever a reason to actively deprecate v1 writing? My answer: no — v1 costs us nothing to keep writing, and v1 readers in the wild appreciate it.
4. **Supersede `.kiro/specs/pebble-storage-engine/`?** It's the old branch's stale design. I propose deleting or archiving it when this RFC is approved.

### Resolved

- **v2 unmarshal streaming.** Decision: stream. Engine gets an `io.ReadSeeker` positioned at the payload start. See §5.4 Load path.
- **Payload decompression ownership.** Decision: dotc1z owns it via shared streaming helpers; engines see decoded bytes. See §5.4 Load path and §5.11 Shared input middleware.
- **Session store on pebble.** Decision: required, native. See §5.6.
- **Where backend infra flips engines.** Decision: at each `dotc1z.NewC1ZFile` call — no env var, no global. See §5.3.

## 7. Alternatives considered

1. **Straight rebase of `morgabra/pebble`.** Rejected — see §4–5.5. The branch's interface predates `connectorstore.InternalWriter`, the new sync types, #773 pool, column handling (#757), and the parallel-sync API shape. Replaying the diff silently reverts or misaligns with all of that.
2. **Keep SQLite, add a Pebble backend behind a custom `connectorstore.InternalWriter` impl that doesn't live under `dotc1z`.** Cleaner package-wise but means duplicating the c1z file format, manager dispatch, compaction, and clone logic. Rejected: `pkg/dotc1z` is already the natural home for "how data lives on disk."
3. **Replace SQLite entirely with Pebble.** Rejected. SDK consumers (connectors) expect a stable `.c1z` format. Breaking them is not on the table for this RFC.

## 8. Execution plan / rollout

1. Land this RFC (no code).
2. PR 1: interface skeleton + sqlite move (Commits 1–4 of §5.5). This is mechanically large but behaviorally a no-op and heavily test-covered.
3. PR 2: format detection + sqlite-only pebble stub (Commit 5). Tiny.
4. PR 3: pebble engine real implementation + tests (Commits 6–8). Largest risk; lives behind `WithEngine("pebble")` default-off.
5. Backend infra opts in via deployment config once PR 3 is green in CI and has soak-tested.
6. Deprecation of `.kiro/specs/pebble-storage-engine/` after PR 3 merges.

Each PR is independently revertable. None change connector binary behavior when `WithEngine` is unset.

## 9. Security / ops considerations

- Pebble's on-disk layout is not public; anyone with filesystem access to a pebble c1z must treat it as sensitive the same as sqlite. No new PII surface.
- Pebble adds a meaningful dependency footprint (`vendor/` grows substantially — the branch's `go.mod` added ~40 new transitive deps). Connectors don't link this code unless they import the pebble subpackage; we should confirm with a build-graph check that default connector builds don't pull in pebble.
- Pebble's WAL cleanup, compaction triggers, and disk-space behavior are different from sqlite's. Ops runbooks need a "pebble c1z" section before backend infra flips the switch.

## 10. Timeline estimate

- PR 1 (refactor-only): 2–3 days of careful work + review + soak.
- PR 2 (format detection): 0.5 day.
- PR 3 (pebble real): 1–2 weeks, including bringing up to main's interface and porting/refreshing the branch's test suite.

---

## 11. Interface factoring — revisiting the sync and compaction layers

§5 proposed embedding `connectorstore.InternalWriter` in `engine.StorageEngine` as a compat-preserving move. That lets us land the refactor without chasing call-site churn, but it also **locks in the interface drift** that made this branch hard to rebase in the first place. Before we commit to that, it's worth asking whether the sync and compaction layers themselves deserve a small redesign so the engine contract can be clean.

We're allowed to break compatibility on the internal interfaces here (connectors depend on the `.c1z` file, not on `InternalWriter`), so this is the right time to pay the tax.

### 11.1 What leaked, and why

The `connectorstore.InternalWriter` / `C1File` surface has grown three kinds of leaks over the last year:

**Leak A: SQL-shaped APIs on the engine.** `UpsertGrants(GrantUpsertOptions{Mode: ...})`, `ListGrantsInternal(GrantListOptions{Mode: GrantListModeExpansionNeedsOnly, ...})`, `SetSupportsDiff(syncID)`, `GetLatestFinishedSync(proto request)` — these are shaped like SQL queries with mode enums. They were added because the sync layer needed one more conditional write or one more filtered scan that the plain `Put*`/`List*` API didn't support. Each addition was the cheapest local fix, and each one widened the engine interface by a query pattern. Pebble now has to implement them, even though half of the "modes" are essentially sqlite implementation details (e.g. `GrantUpsertModePreserveExpansion` exists because sqlite grants have expansion columns that mustn't be clobbered).

**Leak B: Cross-engine primitives exposed as engine methods.** `AttachFile(other engine.StorageEngine, dbName string)` returning an `AttachedStorageEngine` with `CompactResourceTypes/Resources/Entitlements/Grants` is literally "sqlite ATTACH DATABASE" with the method names changed. The branch forces the pebble engine to implement this too, even though pebble's merge primitive is completely different (prefix-scan + batch-write, not SQL joins). The compactor calls `AttachFile` because that's the one way to make sqlite compaction fast, and the engine had to expose it. Now every engine has to expose it.

**Leak C: File-format concerns on the engine.** `OutputFilepath()`, `CloneSync() (string, error)`, `GenerateSyncDiff(base, applied) (string, error)` — these return **filesystem paths** from what's nominally a data-access interface. They exist because both the compactor and the tests need to hand c1z files to subsequent steps. The engine interface is carrying the plumbing for "make me a new c1z that contains this subset of data."

In short: the sync/compaction pipelines treat the engine as "a sqlite C1File I can do surgery on," and over time new surgery moved onto the interface rather than onto a layer above it.

### 11.2 The minimal engine contract, proposed

Rather than embed `connectorstore.InternalWriter`, propose a *narrower* engine interface that both SQLite and Pebble can implement cleanly, with *behavior* on top rather than *modes*:

```go
*Preliminary sketch — see §11.6 for the final, call-site-audited shape. This section explains the direction.*

```go
package engine

// Engine is the pluggable storage contract. Both SQLite and Pebble implement it.
// Methods cover the exact use cases from the sync, expand, compact, and parallel_syncer
// packages — no more, no less. See §11.6 for the full final surface.
type Engine interface {
    // Lifecycle
    Close(ctx context.Context) error
    Dirty() bool
    Stats(ctx context.Context) (map[string]int64, error)

    // Sync runs
    StartSync(ctx, syncType SyncType, parentSyncID string) (syncID string, resumed bool, err error)
    SetCurrentSync(ctx, syncID string) error
    CheckpointSync(ctx, syncToken string) error
    CurrentSyncStep(ctx) (string, error)
    EndSync(ctx) error
    Cleanup(ctx) error
    MarkSyncSupportsDiff(ctx, syncID string) error
    Sync(ctx, syncID string) (*SyncRun, error)
    LatestFinishedSync(ctx, types ...SyncType) (*SyncRun, error)
    ListSyncRuns(ctx, pageToken string, pageSize uint32) ([]*SyncRun, string, error)
    ViewSync(ctx, syncID string) error

    // Typed stores
    ResourceTypes() ResourceTypeStore
    Resources()     ResourceStore
    Entitlements()  EntitlementStore
    Grants()        GrantStore
    Assets()        AssetStore
}

// GrantStore is the grant-specific slice of the engine. Concrete — see §11.6
// for why these exact methods and the call-site audit that justifies each one.
type GrantStore interface {
    // Writes
    Put(ctx, ...*v2.Grant) error                     // replace, extract expansion from payloads
    PutPreservingExpansion(ctx, ...*v2.Grant) error  // replace payload, leave expansion rows alone
    PutIfNewer(ctx, ...*v2.Grant) error              // replace only when discovered_at is newer

    // Deletes
    Delete(ctx, grantID string) error

    // Lookups — proto-shaped so they match the reader gRPC surface directly
    Get(ctx, req) (resp, error)
    List(ctx, req) (resp, error)
    ListForEntitlement(ctx, req) (resp, error)
    ListForPrincipal(ctx, req) (resp, error)
    ListForResourceType(ctx, req) (resp, error)

    // Expansion-aware listings — two call sites in syncer, one in c1 wrapper.
    ListWithExpansion(ctx, pageToken string, pageSize uint32) iter.Seq2[*GrantWithExpansion, error]
    ListPendingExpansion(ctx, pageToken string, pageSize uint32) iter.Seq2[*ExpansionDef, error]
}
```

What's different from main:

- **No `InternalWriter` shim.** `UpsertGrants(Mode)` and `ListGrantsInternal(Mode)` are flattened into explicit, purpose-named methods (`Put`, `PutPreservingExpansion`, `PutIfNewer`, `ListWithExpansion`, `ListPendingExpansion`). No enum dispatch inside the engine; call-site intent is legible on sight.
- **`SetSupportsDiff` → `MarkSyncSupportsDiff`** on the engine — it's a sync-lifecycle operation, not a grants operation.
- **No `AttachFile` on the engine.** Cross-file operations move to a separate layer (§11.4).
- **No path-returning methods.** `CloneSync`, `GenerateSyncDiff`, `OutputFilepath` move out (§11.5).
- **Sub-store factoring.** `ResourceTypeStore`, `ResourceStore`, `EntitlementStore`, `GrantStore`, `AssetStore` — each exposing the exact methods the sync/compact code needs for that type.

Every method on this interface is called from a specific audited call site — see §11.6's table. Both engines (sqlite and pebble) implement the full interface; the compiler enforces it.

### 11.3 Where the "internal" APIs go

There are no "internal" APIs to route elsewhere. Main's `connectorstore.InternalWriter` exists as a *compatibility shim* — it bundles `UpsertGrants(Mode)`/`ListGrantsInternal(Mode)`/`SetSupportsDiff` onto `Writer` because extending `Writer` with three new methods would have broken out-of-tree callers. Since we're already breaking the engine surface, we don't need the shim.

The methods those enum-moded calls flatten into live directly on `engine.Engine` and its store sub-interfaces. See §11.6 for the concrete shape: `Grants().Put`, `Grants().PutPreservingExpansion`, `Grants().PutIfNewer`, `Grants().ListWithExpansion`, `Grants().ListPendingExpansion`, and `Engine.MarkSyncSupportsDiff`.

Both engines (sqlite and pebble) implement all of them. The compiler enforces it. No type assertions, no "capability probing," no silent fallbacks.

`pkg/connectorstore` itself gets much smaller: the enums (`GrantUpsertMode`, `GrantListMode`), the option structs (`GrantUpsertOptions`, `GrantListOptions`), the row shapes (`InternalGrantRow`, `InternalGrantListResponse`, `ExpandableGrantDef`), and the `InternalWriter` interface can be deleted outright. `ExpandableGrantDef` survives as `engine.ExpansionDef` since it's a data-layer concept, not a connectorstore concept.

### 11.4 Compaction: move it out from under the engine

The attached compactor today calls `c.dest.AttachFile(c.base, "base")` and then hands out an `AttachedStorageEngine` with `CompactResourceTypes`/`Resources`/`Entitlements`/`Grants` methods. Those methods contain SQL string templates. The engine interface is carrying sqlite merge code around.

Proposal: **compaction is a strategy layer above engines, not a method on engines.**

```go
package compact

type Strategy interface {
    Compact(ctx context.Context, base, applied, dest engine.Engine, destSyncID string) error
}

// NaiveStrategy: iterate base, iterate applied, write dest. Works for any engine pair.
type NaiveStrategy struct{}

// AttachedStrategy: requires both base and dest to be *sqlite.Engine.
// Uses SQL ATTACH. Type-asserts at the start and returns ErrStrategyNotSupported
// if either side isn't sqlite.
type AttachedStrategy struct{}

// PebbleMergeStrategy: requires both base and dest to be *pebble.Engine.
// Uses prefix-scan + batch-write. Same pattern: type-assert up front, fail fast.
type PebbleMergeStrategy struct{}

// PickStrategy picks the best strategy for a given engine pair. Order:
//   - both sqlite     → AttachedStrategy
//   - both pebble     → PebbleMergeStrategy
//   - mixed pair      → NaiveStrategy
// There are exactly three cases because there are exactly two engines.
func PickStrategy(base, applied, dest engine.Engine) Strategy { ... }
```

Note: this is concrete type assertion (`e.(*sqlite.Engine)`), not a "capability interface" probe. We know both engines. Each strategy knows which one it needs. Mixed-engine pairs fall through to naive — that's a real case (read connector sqlite, write server-side pebble, then compact two pebble generations). The `PickStrategy` helper is ~15 lines of switch logic.

Consequences:

- `engine.Engine` loses `AttachFile` and the whole `AttachedStorageEngine` interface.
- `pkg/synccompactor` depends on `engine.Engine`, `*sqlite.Engine`, `*pebble.Engine`, and `compact.Strategy`. It imports the concrete engine packages because it has to type-assert. That's fine — compaction is *specifically* the place where engine-pair knowledge lives.
- Each strategy's implementation lives next to the engine it understands: `pkg/dotc1z/engine/sqlite/attached_compact.go`, `pkg/dotc1z/engine/pebble/merge_compact.go`. SQL stays in sqlite's package; LSM merge stays in pebble's.

Note: today's "attached" compactor also does *grant expansion* via the syncer (see `compactor.go:119-140` — it constructs a syncer with `WithOnlyExpandGrants`). That keeps working under this design; expansion is not a compactor concern, it's a syncer-level post-processing step that happens to run after compaction.

### 11.5 File-level operations move up into `dotc1z`

`OutputFilepath`, `CloneSync` (returns a path), `GenerateSyncDiff` (returns a path), and the idea of "this engine lives inside a c1z file" all belong at the `dotc1z` layer, not on the engine. An engine operates on data; a file knows where it lives on disk.

```go
package dotc1z

// File wraps an Engine with c1z file-format concerns: compression, temp dirs,
// saving to an output path, cloning syncs to sibling files, generating diff files.
type File struct {
    engine engine.Engine
    format Format
    path   string
    ...
}

func NewFile(ctx context.Context, path string, opts ...Option) (*File, error) { ... }

func (f *File) Engine() engine.Engine { return f.engine }
func (f *File) Close() error { ... }               // compresses, writes, cleans up
func (f *File) OutputPath() string { return f.path }
func (f *File) CloneSync(ctx context.Context, syncID string) (*File, error) { ... }
func (f *File) GenerateDiff(ctx context.Context, base, applied string) (*File, error) { ... }
```

The sync layer takes `engine.Engine` for data ops and `*dotc1z.File` only when it needs file operations (e.g., `NewSyncer(WithC1ZFile(f))` internally reaches into `f.Engine()`). The compactor takes three `*dotc1z.File`s and extracts engines from them. Tests that only care about CRUD take `engine.Engine` and don't need a file at all.

**Consequence:** the sync layer loses its dependency on "the store is a c1z file that knows how to write itself to disk." Unit-testing the syncer against an in-memory engine (pebble with `vfs.NewMem()`, or a `fakeengine` for that matter) becomes trivial.

### 11.6 Improving the core interface (not capability-probing)

The earlier draft proposed optional capability interfaces that callers type-assert into. That was speculation — it assumed a parade of future engines that don't exist. We have two engines. Both live in this repo. Both are maintained by the same team. The right answer is to **redesign the core interface to cover the real use cases cleanly**, and require both engines to implement it.

#### What callers actually do (every call site audited)

Across `pkg/sync/syncer.go`, `pkg/sync/parallel_syncer.go`, `pkg/sync/expand/expander.go`, and `pkg/synccompactor/**`:

| Call site | What it needs | Count |
|---|---|---|
| `s.store.UpsertGrants(Mode=Replace, ...)` | "Write grants, replace on conflict, re-extract expansion metadata from the new grant payloads." | 5 (syncer × 4, sync itself) |
| `store.UpsertGrants(Mode=PreserveExpansion, ...)` | "Write grants, replace on conflict, but **don't touch expansion metadata** — the grant payload already went through the expander and the expansion was consumed." | 1 (expander:294) |
| `store.PutGrantsIfNewer(...)` (wraps `UpsertGrants(IfNewer)`) | "Write grants only when their `discovered_at` is newer than what's stored. Re-extract expansion metadata." | 1 (naive compactor) |
| `s.store.ListGrantsInternal(Mode=ExpansionNeedsOnly)` | "Give me expansion definitions for grants that still need expansion, without the grant payload. Used to drive the expansion worker." | 1 (syncer:1405) |
| `s.store.ListGrantsInternal(Mode=PayloadWithExpansion)` | "Stream grants with their expansion metadata inline." | 2 (syncer:2206, c1 wrapper) |
| `s.store.SetSupportsDiff(syncID)` | "Mark this sync run as having SQL-layer grant metadata populated, so downstream diff consumers know it's safe to trust." | 2 (parallel_syncer × 2) |
| `PutResourceTypesIfNewer` / `PutResourcesIfNewer` / `PutEntitlementsIfNewer` / `PutGrantsIfNewer` | Merge two files with last-write-wins semantics. | 4 (naive compactor only) |

The concepts are small and well-bounded:
- **Grants carry an expansion annotation** that describes edges in the entitlement graph. Storage keeps grant payload and expansion metadata *separately addressable* so you can update one without the other.
- **A sync can be marked "supports diff"** meaning its grant metadata is structured enough to diff against.
- **Compaction wants "last-write-wins" writes** for merging two files.

None of these are sqlite implementation details. They're data-model decisions that the sync pipeline depends on. Both engines need to support them.

#### Proposed core interface additions

Rather than capability probing, these become first-class methods on `engine.Engine`:

```go
package engine

type Engine interface {
    // ... other methods from §11.2 ...

    // Grants.Put(ctx, grants) writes grants, extracting their expansion metadata
    // from each grant's GrantExpandable annotation. Existing rows are replaced.
    // Corresponds to main's UpsertGrants(Replace).
    Grants() GrantStore
}

type GrantStore interface {
    Put(ctx, grants ...*v2.Grant) error                      // extract expansion, replace
    PutPreservingExpansion(ctx, grants ...*v2.Grant) error   // replace payload, leave expansion untouched
    PutIfNewer(ctx, grants ...*v2.Grant) error               // extract expansion, replace only when discovered_at newer

    Get(ctx, id string) (*v2.Grant, error)
    Delete(ctx, id string) error
    List(ctx, req) (resp, error)                              // standard proto API
    ListForEntitlement(ctx, req) (resp, error)
    ListForPrincipal(ctx, req) (resp, error)
    ListForResourceType(ctx, req) (resp, error)

    // Expansion-aware list: returns grants with their expansion metadata
    // inline, or just the expansion metadata without payloads, depending on mode.
    // Replaces connectorstore.ListGrantsInternal's four-mode enum with
    // two explicit methods.
    ListWithExpansion(ctx, pageToken string, pageSize uint32) iter.Seq2[*GrantWithExpansion, error]
    ListPendingExpansion(ctx, pageToken string, pageSize uint32) iter.Seq2[*ExpansionDef, error]
}

// GrantWithExpansion pairs a grant payload with its expansion metadata, if any.
// Returned by ListWithExpansion.
type GrantWithExpansion struct {
    Grant     *v2.Grant
    Expansion *ExpansionDef  // nil if the grant has no expansion metadata
}

// ExpansionDef is the storage-layer view of a grant's expandable annotation.
// Same fields as today's connectorstore.ExpandableGrantDef — moved here because
// expansion is a storage/data concept, not a connectorstore concept.
type ExpansionDef struct {
    GrantExternalID         string
    TargetEntitlementID     string
    PrincipalResourceTypeID string
    PrincipalResourceID     string
    SourceEntitlementIDs    []string
    Shallow                 bool
    ResourceTypeIDs         []string
    NeedsExpansion          bool
}
```

Similarly minor additions on the sync-lifecycle surface for `SetSupportsDiff`:

```go
type Engine interface {
    // ... rest from §11.2 ...

    // MarkSyncSupportsDiff sets the supports_diff flag on a sync run.
    // Per parallel_syncer, only called once per sync after graph construction.
    MarkSyncSupportsDiff(ctx, syncID string) error
}
```

And the same `IfNewer` variants on the other stores, since the naive compactor uses all four:

```go
type ResourceTypeStore interface {
    Put(ctx, ...*v2.ResourceType) error          // replace semantics
    PutIfNewer(ctx, ...*v2.ResourceType) error   // last-write-wins
    Get(ctx, id string) (*v2.ResourceType, error)
    List(ctx, req) (resp, error)
    // ... etc
}
```

Likewise for `ResourceStore`, `EntitlementStore`.

#### Why this is better than capability interfaces

1. **Both engines support these operations anyway.** The branch's pebble implementation already has `PutGrantsIfNewer`, `PutResourcesIfNewer`, `PutEntitlementsIfNewer`, `PutResourceTypesIfNewer` — all four. The sqlite engine always has. Making them optional is pretending we have a choice we don't.

2. **Type assertions hide compile-time errors.** If we probe for `BulkGrantWriter` and the engine doesn't implement it, the helper silently falls back to a slow path. That's how you get a 100× perf regression nobody notices for six months. With these on the core interface, a missing implementation is a compile error on day one.

3. **Two engines, one interface.** The parade-of-future-engines justification for capability interfaces was hypothetical. The actual design pressure is: *we have two specific engines we know well and have to keep in lockstep.* Interface methods are the right tool for that.

4. **Fewer concepts.** Capability probing adds a second indirection layer (universal interface + capability interfaces + probing helpers). Direct methods are one layer.

5. **Easier to reason about correctness.** When a syncer call site says `e.Grants().ListPendingExpansion(ctx, ...)`, a reviewer immediately knows what both engines do. When it says `if x, ok := e.Grants().(ExpansionStore); ok { ... }`, the reviewer has to find both branches and check consistency.

6. **Still lets us evolve.** Adding a new method to `engine.Engine` breaks both engines at compile time, which forces us to implement it in both. That's the pain we *want* — it stops one engine silently lagging.

#### What we give up

The narrow §11.2 "just CRUD + Iterate" ideal. The real interface has meaningful domain methods on it: expansion-aware grant listing, preserve-expansion writes, newness-checked writes. That's fine — those are genuine data-layer concepts. The engine interface is still much narrower than main's `InternalWriter` because:

- `GrantUpsertMode`/`GrantListMode` enum-tunneling is gone. Three concrete methods replace one overloaded one. One concrete method replaces a four-mode `ListGrantsInternal`.
- `AttachFile` and the attach-based compaction surface are gone (§11.4).
- `OutputFilepath`/`CloneSync`/`GenerateSyncDiff` with path-returning signatures are gone (§11.5).
- `Close(ctx)` vs `Close()` mismatch resolved.

The interface covers what callers need. Nothing more, nothing less. No speculation.

#### Updated interface shape (supersedes §11.2's sketch)

```go
package engine

type Engine interface {
    // Lifecycle
    Close(ctx context.Context) error
    Dirty() bool
    Stats(ctx context.Context) (map[string]int64, error)

    // Sync runs
    StartSync(ctx, syncType SyncType, parentSyncID string) (syncID string, resumed bool, err error)
    SetCurrentSync(ctx, syncID string) error
    CheckpointSync(ctx, syncToken string) error
    CurrentSyncStep(ctx) (string, error)
    EndSync(ctx) error
    Cleanup(ctx) error
    MarkSyncSupportsDiff(ctx, syncID string) error  // was SetSupportsDiff
    Sync(ctx, syncID string) (*SyncRun, error)
    LatestFinishedSync(ctx, types ...SyncType) (*SyncRun, error)
    ListSyncRuns(ctx, pageToken string, pageSize uint32) ([]*SyncRun, string, error)
    ViewSync(ctx, syncID string) error

    // Stores (see GrantStore above; others follow the same Put/PutIfNewer/Get/List pattern)
    ResourceTypes() ResourceTypeStore
    Resources()     ResourceStore
    Entitlements()  EntitlementStore
    Grants()        GrantStore
    Assets()        AssetStore
}
```

This is ~20 methods on `Engine` + 4 small store sub-interfaces. Each is called from a specific place in the sync/compact pipeline. No mode enums, no option structs with `Mode` fields, no capability probing.

#### Migration impact on the sync/compact layer

Call-site rewrites (per the audit table above):

- `s.store.UpsertGrants(ctx, {Mode: Replace}, grants...)` → `s.store.Grants().Put(ctx, grants...)` (5 sites)
- `store.UpsertGrants(ctx, {Mode: PreserveExpansion}, grants...)` → `store.Grants().PutPreservingExpansion(ctx, grants...)` (1 site)
- `s.store.ListGrantsInternal(ctx, {Mode: ExpansionNeedsOnly, PageToken: ...})` → `s.store.Grants().ListPendingExpansion(ctx, pageToken, pageSize)` (1 site)
- `s.store.ListGrantsInternal(ctx, {Mode: PayloadWithExpansion, PageToken: ...})` → `s.store.Grants().ListWithExpansion(ctx, pageToken, pageSize)` (2 sites, including c1 wrapper)
- `s.store.SetSupportsDiff(ctx, id)` → `s.store.MarkSyncSupportsDiff(ctx, id)` (2 sites)
- Naive compactor's `n.dest.PutXIfNewer(...)` → `n.dest.X().PutIfNewer(...)` (4 sites)

Net code churn: ~15 call sites, all one-liners. The sync layer becomes *more* readable because the intent of each call is in the method name, not in an enum value in an option struct.

### 11.7 Sync-run representation

Today `SyncType` lives in `pkg/connectorstore`, and includes `SyncTypePartialUpserts`/`SyncTypePartialDeletions` (diff syncs) alongside `SyncTypeFull`/`SyncTypePartial`/`SyncTypeResourcesOnly`. The engine has to interpret these strings to decide what it's allowed to do during compaction (the attached compactor on main explicitly rejects diff sync types — `attached.go:63`).

Cleaner factoring: move sync types to `pkg/dotc1z/engine` (it's where they're operationally meaningful), and encode the "is this compactable?" question as a method on the type rather than a hardcoded list in the compactor.

```go
package engine

type SyncType string

const (
    SyncTypeFull             SyncType = "full"
    SyncTypePartial          SyncType = "partial"
    SyncTypeResourcesOnly    SyncType = "resources_only"
    SyncTypePartialUpserts   SyncType = "partial_upserts"
    SyncTypePartialDeletions SyncType = "partial_deletions"
)

func (t SyncType) IsSnapshot() bool {
    switch t {
    case SyncTypeFull, SyncTypePartial, SyncTypeResourcesOnly:
        return true
    default:
        return false
    }
}

func (t SyncType) IsDiff() bool { return !t.IsSnapshot() && t != "" }
```

Compactor then does `if !s.Type.IsSnapshot() { skip }` instead of a hardcoded list. Adding a new sync type becomes self-documenting.

### 11.8 Summary: what `engine.Engine` sheds

Side by side, before and after:

| Current `connectorstore.InternalWriter` / `C1File` surface | Proposed `engine.Engine` |
|---|---|
| `UpsertGrants(GrantUpsertOptions{Mode: Replace})` | `Grants().Put(ctx, grants...)` |
| `UpsertGrants(GrantUpsertOptions{Mode: IfNewer})` (via `PutGrantsIfNewer`) | `Grants().PutIfNewer(ctx, grants...)` |
| `UpsertGrants(GrantUpsertOptions{Mode: PreserveExpansion})` | `Grants().PutPreservingExpansion(ctx, grants...)` |
| `ListGrantsInternal(GrantListOptions{Mode: ExpansionNeedsOnly})` | `Grants().ListPendingExpansion(ctx, pageToken, pageSize)` |
| `ListGrantsInternal(GrantListOptions{Mode: PayloadWithExpansion})` | `Grants().ListWithExpansion(ctx, pageToken, pageSize)` |
| `ListGrantsInternal(Mode: Payload / Expansion)` (defined but unused) | *deleted — dead code* |
| `SetSupportsDiff(ctx, syncID)` | `Engine.MarkSyncSupportsDiff(ctx, syncID)` |
| `AttachFile` / `AttachedStorageEngine` / `CompactResourceTypes/Resources/…` | Moves to `compact.Strategy` above the engine (§11.4). |
| `OutputFilepath()`, `CloneSync() (string, error)`, `GenerateSyncDiff() (string, error)` | Moves to `dotc1z.File` (§11.5). |
| `StartNewSync(ctx)` / `StartNewSyncV2(ctx, type, parent)` / `StartOrResumeSync(ctx, type, sid)` / `ResumeSync(ctx, type, sid)` (4 overlapping entries) | `StartSync(ctx, type, parent) (id, resumed, err)` — one entry point, `resumed` bit signals resume. |
| `Close(ctx)` vs `Close()` mismatch | `Close(ctx)` everywhere. |
| `GetLatestFinishedSync(req *proto)` | `LatestFinishedSync(ctx, types ...SyncType)` — typed. The proto wrapper stays on the reader gRPC surface, which the engine provides via a thin helper. |
| `connectorstore.InternalWriter`, `GrantUpsertOptions`, `GrantListOptions`, `InternalGrantRow`, `InternalGrantListResponse`, `GrantUpsertMode`, `GrantListMode` | *deleted from `connectorstore`*. `ExpandableGrantDef` moves to `engine.ExpansionDef`. |

Result: `engine.Engine` is ~20 methods + four small store sub-interfaces. Every method is called from a specific place in the sync/compact pipeline. Both engines implement everything. Compiler enforces parity. No mode enums, no option structs with `Mode` fields, no capability probing.

### 11.9 Migration impact

The changes land in four places, all in this repo (and c1's vendored copy):

1. **`pkg/sync/syncer.go` + `pkg/sync/parallel_syncer.go` + `pkg/sync/expand/expander.go`** — 15 call sites rewritten per §11.6's migration table. Mechanical; each rewrite is a one-liner.
2. **`pkg/synccompactor/**`** — naive compactor uses `dst.X().PutIfNewer(...)` pattern. Attached compactor's `CompactX` methods move into `pkg/dotc1z/engine/sqlite/attached_strategy.go`; `pkg/synccompactor/attached` becomes a thin strategy dispatcher.
3. **`pkg/connectorstore`** — delete `InternalWriter`, the enums, the option structs, the row shapes. `ExpandableGrantDef` moves to `engine.ExpansionDef`. Net ~90 lines removed from `connectorstore.go`.
4. **c1 (`/data/squire/src/c1`)** — one call site in `pkg/temporal/sync_activity/sync/wrapper.go:183` uses `GrantListModePayloadWithExpansion`; rewrite to `ListWithExpansion(...)`. All other c1 uses go through the `Reader`/`Writer` gRPC interfaces, which are unchanged.

No external SDK consumer (connectors) is affected: connectors use `Reader`/`Writer`, not `InternalWriter`. The public c1z file format and connector gRPC contracts are untouched.

### 11.10 Recommendation

The right layering is:

- **Engine interface as §11.6**: cover the real use cases with direct methods. Store sub-interfaces for readability. Both engines implement everything. No capability probing.
- **`compact.Strategy` as §11.4**: attached/naive/pebble-merge as explicit strategies, picked by engine-pair probing. Engine no longer carries merge code.
- **`dotc1z.File` wraps the engine as §11.5**: file/path/format concerns at the `dotc1z` layer. Engines are data-only.

§11.3 ("where do the internal APIs go") is a non-question — they're folded into the engine interface directly.

Trade-off vs. §5's original "embed `InternalWriter`":

| | Embed `InternalWriter` (§5) | Redesigned interface (§11) |
|---|---|---|
| Churn in syncer/expand/parallel | None | ~15 call sites, one-liner rewrites |
| Churn in compactor | Light | Medium — strategy refactor + naive compactor rewrite |
| Pebble surface area | All of `InternalWriter` + new sync types as-is | `engine.Engine` (smaller, cleaner) |
| Interface enforcement | Runtime (assertions, silent gaps possible) | Compile-time (both engines must match) |
| Readability of sync code | `s.store.UpsertGrants(ctx, {Mode: Replace}, ...)` | `s.store.Grants().Put(ctx, ...)` |
| Time to first merged PR | Fastest | +3–5 days |

Recommendation: **§11**. The extra days pay for themselves the first time someone has to reason about what a call site does. And they eliminate the drift mechanism that made this branch un-rebaseable in the first place.

Staged rollout unchanged from before:

1. §5.5 commits 1–4: engine interface + sqlite move, preserving today's main-branch behavior verbatim. (Biggest, lowest-risk.)
2. Follow-up PR: §11.4 + §11.5 + §11.6. Rewrite the sync/compact call sites, delete `InternalWriter`, move attached compaction to a sqlite-side strategy. (Churny but mechanical.)
3. Pebble engine is written against §11.6's interface from day one. (The hard, fun work.)

### 11.11 Open questions specific to §11

1. **Sub-store split** (`e.Grants().Put(...)`) vs. flat methods (`e.PutGrants(...)`)? Sub-stores make the interface self-documenting — `Grants()` returns something whose methods are obviously grant-related. Flat methods keep diffs small vs. today. Both are defensible; I lean sub-store for readability. Preference?
2. **Iter.Seq2 for `ListWithExpansion` / `ListPendingExpansion`?** Currently shown as iterators. Alternative: paginated list returning a slice + next-token. Iterators compose better (the expander's `listAllGrantsWithExpansion` already wraps pagination into an iterator). Need to confirm go.mod is on Go 1.23+.
3. **Is `MarkSyncSupportsDiff` a standalone call, or an option at `StartSync`?** Parallel syncer calls it conditionally after graph construction — can't fold into `StartSync`. Keep it standalone.
4. **Does `CopyAll` (§5.10) use the full engine interface or a narrower "source" subset?** Proposal: accept `engine.Engine` for both sides, but internally only call the Reader-ish methods on `src` and the Writer-ish methods on `dst`. No separate interface needed.

---

*End of RFC.*
