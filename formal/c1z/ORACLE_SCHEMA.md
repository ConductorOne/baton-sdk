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
    "pagination": 0, "bare_id": 0, "sync": 0,
    "grant_writes": 0, "entitlement_writes": 0, "grant_list": 0,
    "grants_by_principal": 0, "grant_bare_id": 0,
    "stream": 0, "digest": 0, "reopen": 0
  },
  "keys": [ ... ],
  "entitlement_strip": [ ... ],
  "writes": [ ... ],
  "pagination": [ ... ],
  "bare_id": [ ... ],
  "sync": [ ... ],
  "grant_writes": [ ... ],
  "entitlement_writes": [ ... ],
  "grant_list": [ ... ],
  "grants_by_principal": [ ... ],
  "grant_bare_id": [ ... ],
  "stream": [ ... ],
  "digest": [ ... ],
  "reopen": [ ... ]
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

## Increment 1 and 2 families

Grants are addressed in requests and responses by their structural
identity; `ext_id` is the producer's `external_id` (may be empty).

```json
"grant": { "ent": { "rt": "hex", "rid": "hex", "ext": "hex" }, "prt": "hex", "prid": "hex", "ext_id": "hex" }
```

All five identity components are non-empty (`GrantId.WellFormed`) and
every component is valid UTF-8, since grants travel through v2 protos.
Stored grants in `final` are listed in key order and carry the stored
`ext_id`; the Go test compares identity and `ext_id`, and additionally
checks that the returned v2 `Grant.Id` equals `ext_id` when it is
non-empty and the rebuilt public id `ent.ext ":" prt ":" prid` when it
is empty.

### grant_writes

Model: `GrantStore.putGrants` / `deleteGrant`. Go: one `PutGrants` per
`put` op, the structural delete API per `delete` op, then `ListGrants`
to exhaustion.

```json
{ "name": "same identity different external id collapses",
  "ops": [
    { "op": "put", "batch": [ <grant>, ... ] },
    { "op": "delete", "ent": {...}, "prt": "hex", "prid": "hex" }
  ],
  "final": [ <grant>, ... ] }
```

Cases include: two grants differing only in `ext_id` (one row, the
later `ext_id`); the same `ext_id` on two structures (two rows); a grant
whose entitlement and principal have no record (stored); delete by
identity then re-put; delete of an absent identity (no-op).

### entitlement_writes

Model: `EntitlementStore.putEntitlements` / `deleteEntitlement`. Go:
`PutEntitlements`, the identity delete, then list entitlements to
exhaustion.

```json
{ "name": "same external id on two resources",
  "ops": [
    { "op": "put", "batch": [ { "rt": "hex", "rid": "hex", "ext": "hex", "value": "v1" }, ... ] },
    { "op": "delete", "rt": "hex", "rid": "hex", "ext": "hex" }
  ],
  "final": [ { "rt": "hex", "rid": "hex", "ext": "hex", "value": "v1" }, ... ] }   // key order
```

### grant_list

Model: `GrantStore.grantsForEntitlement`,
`grantsForEntitlementByPrincipalType`, `grantForEntitlementPrincipal`.
Go: one `PutGrants`, then `ListGrantsForEntitlement` with the structured
entitlement ref, optionally with the principal-type filter, or the
entitlement-plus-principal point lookup, paginated with `page_size`.

```json
{ "name": "two entitlements disjoint",
  "grants": [ <grant>, ... ],
  "query": { "ent": { "rt": "hex", "rid": "hex", "ext": "hex" },
             "prt": "hex",            // optional principal-type filter
             "prid": "hex" },         // optional; with prt makes it a point lookup
  "page_size": 2,
  "pages": [ { "grants": [ <grant>, ... ], "has_next": true }, ... ] }
```

Pages are computed with `Paginate.page` over the keys of the
entitlement's grants, with `visible` being the principal-type filter
when `prt` is given. That reproduces the engine's lookahead: a full
page whose remaining raw rows are all filtered out carries a token and
is followed by an empty terminal page. With `prt` and `prid` both
given the query is a point lookup: one page, no token, zero or one
grant. Without `prt`, `prid` is not allowed.

### grants_by_principal

Model: `IndexedGrants`. Go: a fresh engine with a sync. `put` is
`PutGrants` (indexed inline); `put_deferred` is `PutExpandedGrantRecords`
on the v3 records translated from the same grants (the deferred path:
arms the marker, adds no index entry for the written identities);
`delete` is the structural delete; `end_sync` is `EndSync`; `read` is
`ListGrantsForPrincipal` for the principal, paginated to exhaustion,
with the grants returned in the order the engine gives. Expected
values for `read` come from `IndexedGrants.grantsForPrincipal`, which
lists in primary key order; the Go test compares as ordered lists, so
if the engine's `by_principal` order differs for one principal it will
show up (both orders are entitlement-first for a fixed principal, so
they should agree).

```json
{ "name": "deferred write invisible until end sync",
  "ops": [
    { "op": "put", "batch": [ <grant>, ... ] },
    { "op": "put_deferred", "batch": [ <grant>, ... ] },
    { "op": "delete", "ent": {...}, "prt": "hex", "prid": "hex" },
    { "op": "read", "prt": "hex", "prid": "hex", "grants": [ <grant>, ... ] },
    { "op": "end_sync" }
  ] }
```

A `read` after `end_sync` equals the primary view. A `read` after a
`put_deferred` of a new identity omits that identity; a `put_deferred`
of an identity that already has an entry stays visible; a plain `put`
after arming is visible. `end_sync` must be the last op (the engine is
sealed afterwards); after `end_sync` a final `read` is allowed.

### grant_bare_id

Model: `GrantLookup.resolve`. Go: `PutEntitlements` for `entitlements`,
`PutGrants` for `grants` (a grant's `ext_id` becomes v2 `Grant.Id`, and
may be empty), then `GetGrant` with `lookup` as the grant id. The
returned grant's identity is compared through its refs; its stored
`external_id` is read with the engine's v3 record getter (the v2 `Id`
cannot distinguish an empty stored id from the rebuilt one).

```json
{ "name": "opaque entitlement unreachable without row",
  "entitlements": [ { "rt": "hex", "rid": "hex", "ext": "hex" }, ... ],
  "grants": [ <grant>, ... ],
  "lookup": "hex",
  "expected": "not_found" | "found" | "ambiguous",
  "found": <grant> }        // only when found
```

`not_found` is `pebble.ErrNotFound` from `GetGrant`; `ambiguous` is
`ErrAmbiguousExternalID`. Any other error fails the test.

## Increment 3, 5, and 7 families

### stream

Model: `C1z.Stream.run` over `Stream.grantRows` (grants) or the sorted
primary rows (resources, entitlements). Go: `StreamGrants`,
`StreamResources`, `StreamEntitlements` with an empty sync id on a
fresh engine with a started sync.

```json
{ "name": "cancelled over empty keyspace yields nothing",
  "kind": "grants" | "resources" | "entitlements",
  "entitlements": [ { "rt": "hex", "rid": "hex", "ext": "hex" }, ... ],   // rows written first
  "resources": [ { "rt": "hex", "rid": "hex" }, ... ],                   // resources kind
  "grants": [ <grant>, ... ],                                            // grants kind, PutGrants
  "deferred": [ <grant>, ... ],                                          // grants kind, PutExpandedGrantRecords
  "filter": { "ent_ext": "hex", "prt": "hex", "prid": "hex" }            // grants: each optional
          | { "rt": "hex" }                                              // resources: optional
          | {},                                                          // entitlements
  "consumer": { "cancel_after": 0, "break_after": null },                // each null or a count
  "yields": [ { "grant": <grant> } | { "resource": {...} } | { "entitlement": {...} }
            | { "error": "cancelled" }, ... ] }
```

`cancel_after: 0` means the context is cancelled before the stream
starts; `cancel_after: k` means the consumer cancels it right after
receiving the k-th record and keeps ranging. `break_after: k` means the
consumer stops after the k-th record. The error on the index path is
wrapped by the engine; the Go test classifies with `errors.Is(err,
context.Canceled)`. Which rows a grant stream scans: `ent_ext` set →
that entitlement's prefix (the oracle only emits an `ent_ext` that
matches exactly one entitlement row, or matches none and contains no
colon, so the engine's bare-id resolution is unambiguous and the
no-match case is an empty stream); `prt` alone → the `by_principal`
index under that type, in index key order, with the deferred-index gap;
anything else → a full primary scan. `prt` and `prid` are post-filters.
Resources: full primary scan with `rt` as a post-filter.

### digest

Model: `C1z.Digest`. Go: a fresh engine with the digest option on
(the default), `PutEntitlements`, `PutGrants` carrying the
`GrantImmutable` annotation and the sources map, then the ops.

```json
{ "name": "write after seal invalidates partition and global",
  "entitlements": [ { "rt": "hex", "rid": "hex", "ext": "hex" }, ... ],
  "grants": [ { "ent": {...}, "prt": "hex", "prid": "hex", "ext_id": "hex",
                "immutable": false, "sources": [ { "key": "hex", "is_direct": true }, ... ] }, ... ],
  "ops": [
    { "op": "seal" },                                                    // EndSync
    { "op": "read", "ent": {...}, "found": true, "count": 2, "width": 0 },
    { "op": "read_global", "found": true, "count": 5 },
    { "op": "resume" },                                                  // ResumeSync(current id)
    { "op": "put", "batch": [ <digest grant>, ... ] },
    { "op": "delete", "ent": {...}, "prt": "hex", "prid": "hex" }
  ],
  "equal_content": [ [ {...}, {...} ], ... ],                           // entitlement pairs with equal canonical content
  "distinct_content": [ [ {...}, {...} ], ... ] }                       // entitlement pairs with different canonical content
```

`read` expects `found`, `count`, and `width` from
`GetEntitlementDigestRoot`; `read_global` from `GetGrantDigestGlobalRoot`.
After `seal`, a `put` or `delete` under an entitlement makes that
entitlement and the global root read `found: false` while other
partitions still read `found: true`; a later `seal` makes everything
found again with the fresh values. `seal` may appear more than once;
`put`/`delete` need a `resume` after a `seal`.

The hash is not modeled, so hash bytes never appear in the document.
The Go test checks instead: (1) every pair in `equal_content` has equal
root hashes after the last `seal`; (2) every pair in `distinct_content`
has different root hashes, which is the collision assumption made
explicit (a failure there is an xxHash64 collision, not a model error,
and the test says so); (3) for every partition, the root equals the
engine's own `GrantDigestAccumulator` fed v2 grants built from the
case's grants with `ext_id` replaced by a fixed different string and no
`discovered_at`, which pins the canonicalization (excluded fields do not
affect the hash). `width` is `chooseWidth(count)`.

### reopen

Model: `C1z.Sync` with `reopen` and `setStartedAt`, plus
`C1z.IndexedGrants` for the data. Go: ops on an engine that may be
closed and reopened on the same in-memory filesystem (or the same
directory with `C1Z_FORMAL_DISK=1`). The model's clock is fixed at
`now = 1000000000` seconds; `start_new` stamps `started_at = now`.

```json
{ "name": "unfinished sync readable after reopen within cutoff",
  "ops": [
    { "op": "start_new", "id": "s1", "type": "full", "result": "ok" | "sync_in_progress" },
    { "op": "put", "batch": [ <grant>, ... ] },
    { "op": "put_deferred", "batch": [ <grant>, ... ] },
    { "op": "end", "result": "ok" | "no_current_sync" },
    { "op": "reopen" },
    { "op": "age_sync", "days": 8 },                                     // started_at := now - days
    { "op": "write", "result": "allowed" | "no_current_sync" | "engine_sealed" },
    { "op": "list_grants", "result": "ok" | "no_current_sync", "grants": [ <grant>, ... ] },
    { "op": "read_by_principal", "prt": "hex", "prid": "hex",
      "result": "ok" | "no_current_sync", "grants": [ <grant>, ... ] },
    { "op": "resume", "id": "s1", "result": "ok" | "not_found" },
    { "op": "latest_finished", "type": "any", "result": "none" | "<sync id>" }
  ] }
```

`age_sync` is `PutSyncRunRecord` with the same id and `started_at =
time.Now() - days*24h`; it needs an open engine and no bound sync, so
it appears only right after `reopen`. `list_grants` and
`read_by_principal` resolve the default sync like the adapter does:
`no_current_sync` when nothing resolves (no record, or an unfinished
record older than the cutoff), otherwise the model's collection.
`put`/`put_deferred` are allowed only while a sync is bound (after
`start_new` or `resume`). `reopen` is legal in any state; after it the
engine is unbound and unsealed. Sync ids are symbolic and mapped to
KSUIDs as in the `sync` family.

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
| `grant_writes` | `name`, `ops` | `final` |
| `entitlement_writes` | `name`, `ops` | `final` |
| `grant_list` | `name`, `grants`, `query`, `page_size` | `pages` |
| `grants_by_principal` | `name`, `ops` (a `read` op carries `prt`, `prid`) | `grants` on every `read` op |
| `grant_bare_id` | `name`, `entitlements`, `grants`, `lookup` | `expected`, `found` |
| `stream` | `name`, `kind`, rows, `filter`, `consumer` | `yields` |
| `digest` | `name`, `entitlements`, `grants`, `ops` with `op` and inputs | `found`, `count`, `width` on reads; `equal_content`, `distinct_content` |
| `reopen` | `name`, `ops` with `op` and inputs | `result` on every op; `grants` on reads |

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
