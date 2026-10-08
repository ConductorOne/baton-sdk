# Oracle case schema (version 1)

`lake exe c1z-oracle` prints one JSON document to stdout. The Go
conformance test `pkg/dotc1z/engine/pebble/formal_conformance_test.go`
reads the checked-in copy at `formal/c1z/generated/cases.json`, asserts
`version` and every entry of `counts`, and replays each family against
the real Pebble engine. The consumer rejects unknown `kind`, `op`, and
`expected` strings instead of defaulting.

Byte strings are lowercase hex with no prefix (`"6170703a31"` is
`"app:1"`). Every byte the generator emits is below 256. Record
"values" are display names (plain JSON strings) chosen so the test can
recognize which write produced a stored row.

```json
{
  "version": 1,
  "counts": {
    "keys": 0, "entitlement_strip": 0, "writes": 0,
    "pagination": 0, "bare_id": 0, "sync": 0
  },
  "keys": [ ... ],
  "entitlement_strip": [ ... ],
  "writes": [ ... ],
  "pagination": [ ... ],
  "bare_id": [ ... ],
  "sync": [ ... ]
}
```

## keys

Model: `C1z.Identity` (`ResourceTypeId.key`, `ResourceId.key`,
`EntitlementId.key`, `GrantId.key`). Go: `encodeResourceTypeKey`,
`encodeResourceKey`, `encodeEntitlementIdentityKey ∘
entitlementIdentityFromParts`, `encodeGrantIdentityKey ∘
grantIdentityFromParts` (or the equivalent in `keys.go` / `identity.go`).

```json
{ "name": "resource same rid other rt",
  "kind": "resource_type" | "resource" | "entitlement" | "grant",
  "rt": "hex", "rid": "hex",            // resource_type: only "ext"
  "ext": "hex",                         // resource_type id or entitlement external id
  "prt": "hex", "prid": "hex",          // grant only
  "key": "hex" }                        // full primary key bytes
```

The test compares `key` byte-for-byte. Cases include empty components,
`0x00` and `0x01` bytes, colons inside owner components, multibyte
UTF-8, and equal raw ids under different owners.

## entitlement_strip

Model: `compressEnt`. Go: `entitlementIdentityFromParts`.

```json
{ "rt": "hex", "rid": "hex", "ext": "hex", "stripped": true, "tail": "hex" }
```

## writes

Model: `C1z.Store.putBatch` / `erase` over `ResourceId.key`. Go: a
fresh engine with one started sync; apply `ops` in order as separate
calls (one `PutResources` per `put` op unless `batch` groups them),
then list every resource of type `rt` and compare to `final` in order.

```json
{ "name": "last write wins across calls",
  "rt": "hex",
  "ops": [
    { "op": "put", "batch": [ { "rid": "hex", "value": "v1" }, ... ] },
    { "op": "delete", "rid": "hex" }
  ],
  "final": [ { "rid": "hex", "value": "v2" }, ... ] }   // key order
```

`value` is carried in the resource `display_name`. A `batch` with a
repeated `rid` pins the per-call "last occurrence wins" rule.

## pagination

Model: `C1z.Paginate.traverse` with `visible = true` over the keys of
the given rids under one resource type. Go: write the resources, then
call `ListResources` filtered by `rt` with `page_size`, following
tokens until the token is empty; compare each page's rids in order and
whether a next token was returned. `page_size` 0 pins `clampPageSize`.

```json
{ "name": "three keys page size two",
  "rt": "hex",
  "rids": [ "hex", ... ],                      // insertion order, may be unsorted
  "page_size": 2,
  "pages": [ { "rids": [ "hex", ... ], "has_next": true }, ... ] }
```

## bare_id

Model: `C1z.Result.resolveBare` over the entitlements whose external id
equals `lookup`. Go: write the entitlements, then `GetEntitlement` by
bare id.

```json
{ "name": "same external id on two resources",
  "entitlements": [ { "rt": "hex", "rid": "hex", "ext": "hex" }, ... ],
  "lookup": "hex",
  "expected": "not_found" | "found" | "ambiguous",
  "found": { "rt": "hex", "rid": "hex", "ext": "hex" } }   // only when found
```

## sync

Model: `C1z.Sync`. Go: a fresh engine, apply `ops` in order, compare
each op's `result`.

```json
{ "name": "end then resume reopens writes",
  "ops": [
    { "op": "start_new", "id": "s1", "type": "full",   "result": "ok" | "sync_in_progress" },
    { "op": "write",                                   "result": "allowed" | "no_current_sync" | "engine_sealed" },
    { "op": "end",                                     "result": "ok" | "no_current_sync" },
    { "op": "resume", "id": "s1",                      "result": "ok" | "not_found" },
    { "op": "latest_finished", "type": "any" | "full" | "partial" | "resources_only",
                                                       "result": "none" | "<sync id>" }
  ] }
```

`start_new` is refused with `sync_in_progress` while a sync started by
an earlier `start_new` is still open (`ResetForNewSync` refuses while
`IsFreshSync()`); after `end`, or after `resume` (which binds without
the fresh flag), it is accepted and wipes the file. The Go test reads
`IsFreshSync()` before the call to classify the refusal, because the
engine returns a plain error.

`write` means one `PutResources` call of a single well-formed resource.
Time is not part of the sync cases; the 7-day unfinished fallback is
covered by Lean witnesses only.

Consumer notes, from writing the Go test:

- Sync ids are symbolic. The engine requires KSUIDs
  (`StartNewSyncWithID` rejects anything else), so the test allocates a
  fresh KSUID per symbolic id and maps `latest_finished` results back;
  an id no op named is a failure.
- `EndSync` on an unbound engine returns a plain error, not the
  `ErrNoCurrentSync` sentinel (adapter.go `"EndSync: no open sync"`).
  The test reads `CurrentSyncID()` before the call and classifies an
  error from an unbound engine as `no_current_sync`.
- `engine_sealed` is a valid result string that the generator never
  emits: `EndSync` clears the binding before a caller can observe the
  seal, and the bound check runs first (`C1z.Sync.writeGate`). It stays
  in the schema so a model change that makes it reachable fails loudly
  on the Go side instead of being decoded as a zero value.
- `entitlement_strip` cases carry no `name`; subtests are named by index.

## Oracle modes

`lake exe c1z-oracle` has three modes. Every mode writes a document of
the shape above to stdout, so one Go decoder and one replay loop serve
all of them.

| Invocation | Output | Used by |
|---|---|---|
| `c1z-oracle` | the fixed, hand-chosen corpus; byte-identical across runs | `generated/cases.json` (checked in), `make formal-c1z-check` freshness gate |
| `c1z-oracle --random N --seed S` | the fixed corpus plus `N` pseudo-random cases per family from a seeded generator; deterministic for a given `S` | `make formal-c1z-conformance-random`, written to `generated/cases-random.json` (not checked in) |
| `c1z-oracle --respond` | reads a **request document** on stdin and writes the corresponding case document | `make formal-c1z-property`, the live property test |

Random case names are `random/<family>/<i>`. Random inputs for the
families that pass through protobuf string fields (`writes`,
`pagination`, `bare_id`, `sync`) are valid UTF-8; `keys` and
`entitlement_strip` use arbitrary bytes.

### Request document (`--respond`)

Same top-level shape, without `counts`, and with every expected field
omitted. The oracle fills in the expected fields by running the model
and emits a complete case document with `version` and `counts`.

| Family | Request carries | Oracle adds |
|---|---|---|
| `keys` | `name`, `kind`, identity components | `key` |
| `entitlement_strip` | `rt`, `rid`, `ext` | `stripped`, `tail` |
| `writes` | `name`, `rt`, `ops` | `final` |
| `pagination` | `name`, `rt`, `rids`, `page_size` | `pages` |
| `bare_id` | `name`, `entitlements`, `lookup` | `expected`, `found` |
| `sync` | `name`, `ops` with `op`, `id`, `type` | `result` on every op |

A family may be absent or empty in the request; it is then empty in
the response with count 0. The oracle exits non-zero with a message on
stderr, and writes nothing to stdout, if the request is malformed, has
unknown fields, or contains an input the model rejects (an entitlement
or grant that fails `WellFormed`, a byte of 256 or more, a `sync` op the
model has no transition for). The consumer treats a non-zero exit as a
test failure, never as "no cases".

### Go-side environment

| Variable | Effect |
|---|---|
| `C1Z_FORMAL_CASES=<path>` | `TestFormalConformance` reads this file instead of `generated/cases.json` |
| `C1Z_FORMAL_ORACLE=<path>` | enables `TestFormalProperty`, which generates random inputs in Go, runs `<path> --respond`, and replays the response; skipped when unset |
| `C1Z_FORMAL_PROPERTY_N` | cases per family for `TestFormalProperty` (default 200) |
| `C1Z_FORMAL_PROPERTY_SEED` | seed for the Go generator (default: derived from time, printed in the test log so a failure can be replayed) |
| `C1Z_FORMAL_DISK=1` | open each case's engine on disk (`t.TempDir`) instead of Pebble's in-memory filesystem; about 50x slower because `EndSync` fsyncs serialize at the device |
