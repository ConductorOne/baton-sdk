# Injective Grant & Entitlement IDs via Edge Encoding (Pebble)

Status: planned / implementable

Scope: `pkg/dotc1z/engine/pebble/` (the Pebble v3 engine), plus the id
encode/decode helpers in `pkg/types/grant`, `pkg/types/entitlement`, and the
baton edges that emit/accept ids. SQLite is out of scope. Covers grants and
entitlements.

## 0. Goal & approach

The public grant/entitlement id is a lossy `:`-join and is documented
non-injective:

```27:30:proto/c1/connector/v2/grant.proto
  string id = 3 [deprecated = true];  // "these ids may not map one to one with the grant itself!"
```

Two distinct logical records can fold to the same id string and collide where
that string is used as a key. This blocks the synthesized-grant skip-Get fast
path and is a latent correctness hazard.

**Approach (decided): encode at the edges.** The id is a baton-owned, canonical,
escaped string produced when baton *emits* an id and parsed when baton *accepts*
one. Internally, identity is the structured tuple, stored with efficient indexes.

- **Identity is structural**, never the lossy string:
  - grant = `(entitlement_rt, entitlement_rid, entitlement_kind, entitlement_name, principal_rt, principal_id)`
  - entitlement = `(resource_type_id, resource_id, entitlement_kind, name)`
  - `name` is the permission component of the entitlement id — NOT the `slug`
    field (see §1.2).
- **The id string is an edge encoding**: a flat escaped join of the identity
  components. Colon-free components → byte-identical to today's `fmt.Sprintf`
  ids, so the common case is unchanged for operators and C1.
- **No lossy id is ever used as a storage key**, so there is no fold and no
  resolution ambiguity. No `external_id → record` index.
- **The canonical encoding is the round-trip id** baton emits and accepts. The
  connector's **original id is retained** as a stored, non-key field, so when the
  canonical id diverges from the original (colon-bearing or connector-custom)
  baton can **optionally output both** (e.g. a second CLI column / extra field).
  For the colon-free common case the two are equal, so there is nothing extra to
  show. Decided in §8.
- **Full migration** to the efficient internal layout — not a halfway,
  partly-old-partly-new state.

This also pays down expansion cost. Other benches put **~50% of grant-expansion
time in index writes**, dominated by the misordered principal-keyed families. The
raw-tuple layout drops three index families (one of them a scattered
principal-keyed index, `by_principal_resource_type`) and slims the rest, so the
redesign is both the correctness fix and a write-cost cut (quantified on the whale,
including c1z size — §2.3, §7).

## 1. Ground truth (cited)

### 1.1 The lossy joins

```79:81:pkg/types/entitlement/entitlement.go
func NewEntitlementID(resource *v2.Resource, permission string) string {
	return fmt.Sprintf("%s:%s:%s", resource.GetId().GetResourceType(), resource.GetId().GetResource(), permission)
}
```

```85:85:pkg/types/grant/grant.go
	grant.SetId(fmt.Sprintf("%s:%s:%s", entitlement.GetId(), resourceID.GetResourceType(), resourceID.GetResource()))
```

So `entitlement.Id = rt:rid:name` and `grant.Id = entitlement.Id:prinRT:prinID`
`= rt:rid:name:prinRT:prinID`. Unescaped joins; any `:` inside a component folds
distinct tuples to the same string. Grant ids are also produced by `NewGrantID`
(`grant.go:112`), `expander.go:670`, and `topological_merge.go:573` — all the
same shape and all must use the new encoder.

### 1.2 `name` is the identity component, not `slug`

The constructors set `Id = NewEntitlementID(resource, name)` and `Slug = name`,
but `WithSlug(...)` overrides the slug **without changing the Id**:

```83:96:pkg/types/entitlement/entitlement.go
func NewPermissionEntitlement(resource *v2.Resource, name string, ...) *v2.Entitlement {
	entitlement := v2.Entitlement_builder{
		Id:          NewEntitlementID(resource, name),
		DisplayName: name,
		Slug:        name,           // WithSlug can later override this, NOT Id
		...
```

So the entitlement's identity component is the `name` baked into the `Id`
(3rd `:`-field) for SDK-generated ids, recoverable by stripping the known
`rt:rid:` prefix using the record's resource fields. `slug` is a separate,
overridable display field and must not be used for identity. Grants embed
`entitlement.Id` (hence `name`, not `slug`) — `grant.go:85`.

### 1.3 What records carry (refs.proto / records.proto)

```16:20:proto/c1/storage/v3/refs.proto
message EntitlementRef { string resource_type_id = 1; string resource_id = 2; string entitlement_id = 3; }
```
```34:39:proto/c1/storage/v3/refs.proto
message PrincipalRef { string resource_type_id = 1; string resource_id = 2; string parent_resource_type_id = 3; string parent_resource_id = 4; }
```
- `EntitlementRecord` (records.proto:108-145) carries `external_id`,
  `resource.{rt,rid}`, and `slug` (field 9) separately.
- A grant carries `entitlement.{rt,rid,entitlement_id}` + `principal.{rt,id}`
  (`EntitlementRef`/`PrincipalRef`). It does NOT carry the entitlement slug, but
  it does carry the entitlement id, so `name` is recoverable from it. Principal
  identity is `(rt, id)`; `parent_*` is contextual, not part of identity.

### 1.4 Current storage layout (`keys.go`)

- Grant primary: `v3│typeGrant│0x00│esc(external_id)`.
- Entitlement primary: `v3│typeEntitlement│0x00│esc(external_id)`.
- Grant indexes (all escaped-tuple, with `external_id` as the tail back-pointer):
  `by_entitlement` `(entitlement_id, prin_rt, prin_id, external_id)`,
  `by_principal` `(prin_rt, prin_id, external_id)`,
  `by_principal_resource_type` `(prin_rt, external_id)`,
  `by_entitlement_resource` `(ent_rt, ent_rid, external_id)`,
  `by_needs_expansion` `(external_id)`.
- Entitlement index: `by_resource` `(res_rt, res_id, external_id)`.
- Index prefixes already use `codec.AppendTupleStrings` (order-preserving,
  0x00-separator, escapes 0x00) so they are injective; only the `external_id`
  tail/primary-key is the lossy string.
- Read materialization: index scan → decode `external_id` tail →
  `Get(encodeGrantKey/encodeEntitlementKey(external_id))`
  (`paginate.go`, `grants.go`, `entitlements.go`).

### 1.5 Where ids enter/leave baton (the edges)

- **Output:** `GetGrant`/`ListGrants` set `v2.Grant.Id = external_id`
  (`translate_v2.go:126`); CLI `baton grants`/CSV/diff print the proto id; the
  reader RPC streams it.
- **Input:** local-CLI revoke `provisioner.go:198` `GetGrant(GrantId:
  revokeGrantID)` from `--revoke-grant`; explorer cache `storecache.go:161`.
  Production C1 revoke carries the **full grant** in the task
  (`tasks/c1api/revoke.go:43-45`) — no id→record lookup.
- The expander resolves entitlements by id internally during expansion
  (`expander.go:373,410`, `topological_merge.go:311`).
- Id **parsers**: `c1zsanitize/transform.go:103` (`strings.Split(id, ":")`) and
  `bid.ParseEntitlementBid` (`syncer.go:2268`).

### 1.6 Migration machinery (and limits)

- `keyspaceVersion=2` (`keyspace_version.go:28`) is a hard global gate: on
  mismatch it **rejects** the file, never converts (`:60-62`).
- `index_migrations.go` is a per-index, versioned, idempotent **backfill**
  registry (currently empty), run at writable Open after the version check
  (`engine.go:111-121`), skipped read-only, not resumable. It only **adds**
  index entries — it cannot rewrite primary keys.
- A full re-key + index rebuild is a new migration shape. Reuse the registry's
  run-once gating idea and the bulk/SST writer (`bulk_import.go`).
- `scanGrantIndexFieldsRaw` (`raw_records.go:311`) is a shallow `protowire`
  scan pulling entitlement-ref + principal-ref + needs_expansion — the cheap way
  to read identity from a record value without a full unmarshal.

## 2. Design

### 2.1 Canonical edge id encoding

Add a flat, escaped, order-preserving string encoding (in `pkg/types/grant` and
`pkg/types/entitlement`), used at **every** edge: id generation, baton output,
and the inverse at baton input.

```
esc(s)   = s with '\' -> '\\' and ':' -> '\:'        (reversible, colon-free output)

SDK-generated entitlement:
  encodeEntitlementID(rt, rid, kind="sdk", name) =
    esc(rt):esc(rid):esc(name)                    // 3 leaves, byte-identical today

Connector-custom entitlement:
  encodeEntitlementID(rt, rid, kind="custom", name=external_id) =
    esc(rt):esc(rid):custom:esc(external_id)      // 4 leaves, explicit tag

SDK-generated grant:
  encodeGrantID(rt, rid, kind="sdk", name, prinRT, prinID) =
    esc(rt):esc(rid):esc(name):esc(prinRT):esc(prinID)

Connector-custom entitlement grant:
  encodeGrantID(rt, rid, kind="custom", name=external_id, prinRT, prinID) =
    esc(rt):esc(rid):custom:esc(external_id):esc(prinRT):esc(prinID)

decode(id) = split on UNESCAPED ':' then unescape each part:
  entitlement: 3 leaves => sdk, 4 leaves with leaf[2]=="custom" => custom
  grant:       5 leaves => sdk, 6 leaves with leaf[2]=="custom" => custom
```

Properties:
- **Injective.** Distinct component tuples → distinct strings (escaping makes the
  delimiter unambiguous); `decode` is total and exact.
- **Byte-identical to today for SDK-generated, colon-free components.** `esc` is
  the identity on colon/backslash-free strings, so `encodeGrantID` of a normal
  SDK-generated grant equals today's `rt:rid:name:prinRT:prinID`. Operators and C1
  see no change in the common case. (This is about the **emitted id string** only
  — the internal storage keys and indexes are fully migrated regardless, §3.)
- **Flat, not nested.** The grant id is a single leaf join, NOT
  `esc(entitlement_id):...` — double-escaping would diverge from today's bytes.
  A grant's entitlement id is the entitlement subtuple at the front of its grant
  id: first 3 leaves for SDK, first 4 leaves for custom.
- `name` comes from the entitlement (recovered via the known `rt:rid:` strip,
  §1.2), never from `slug`.

Two derivations to make explicit:
- **A grant carries the entitlement's legacy id, not `name`.** `EntitlementRef`
  has `(rt, rid, entitlement_id)` where `entitlement_id` is the connector's
  string. To key the grant we need the entitlement's canonical id, so we recover
  `name = TrimPrefix(entitlement_id, rt+":"+rid+":")` (exact strip on the known
  resource fields, §1.2) and derive `(rt, rid, kind="sdk", name)`. This makes
  the grant's entitlement key-prefix **byte-equal to the entitlement's own primary
  key tail**, so `by_entitlement` lookups and the expander graph agree.
- **Custom ids where the strip fails.** If `entitlement_id` does not start with
  `rt+":"+rid+":"` (a connector-custom entitlement id), there is no SDK `name` to
  strip. Do **not** collapse this to the same tuple shape as an SDK permission:
  `(rt,rid,"admin")` from custom id `"admin"` would collide with SDK-generated
  `rt:rid:admin` on the same resource. Instead derive a tagged entitlement
  identity:
  - SDK-generated: `(rt, rid, kind="sdk", name=stripped_name)`
  - connector-custom: `(rt, rid, kind="custom", name=external_id)`
  The strip is exact (it removes the literal known `rt`/`rid` values, so colons
  inside `rt`/`rid` are not a problem). The connector's original id is retained in
  the value regardless.
- **One shared derivation.** The strip+fallback that turns `(rt, rid,
  entitlement_id)` into the canonical entitlement id must be a **single helper**
  used by both entitlement keying and grant keying (and the migration), so the
  grant's entitlement component and the entitlement's own key are always
  byte-equal — including the custom/tag case.

Generation: `NewGrant`/`NewGrantID`/`NewEntitlementID`, `expander.go:670`,
`topological_merge.go:573` switch from `fmt.Sprintf("%s:%s:%s", ...)` to these
encoders. Output: `translate_v2.go` read path sets `v2.*.Id = encode(identity)`.
Input: revoke/explorer `decode` the id → identity → internal key.

### 2.2 Internal identity & keys

Identity is stored structurally; the lossy string is never a key.

**Colon-escaping is edge-only; keys never double-encode.** The `encodeXID`
escapes from §2.1 exist solely to make the external id *string* an unambiguous
flat join. Internal keys already get injective, order-preserving separation from
the `0x00` tuple codec (`AppendTupleStrings`, `keys.go:35-46`), so keys store the
**raw** components — no colon layer wrapped inside a tuple component.

- Entitlement internal key tail = `tuple(rt, rid, ent_kind, name)` (raw
  components; `ent_kind` is `sdk` or `custom`).
- Grant internal key tail = `tuple(ent_rt, ent_rid, ent_kind, name, prin_rt,
  prin_id)` (raw components). The first four components are **byte-identical to
  the entitlement primary key tail**, so grant↔entitlement joins line up with no
  decode/re-encode.
- These are opaque internal keys, decoded only by the engine's tuple decoder. The
  colon-escaped form appears only on the wire/CLI id (§2.1), never in a key.
- `GetGrant(id)`/`GetEntitlement(id)`: `decode(id)` (edge) → raw components →
  build the `0x00` key → direct primary Get. Total and unambiguous for every id;
  **no `by_external_id` index, no resolution fallback.**
- The connector's **original id is retained in the record value** (the
  `external_id` field stays as the connector set it) — non-key, for display /
  "output both" (§4). The key is derived from the structured identity, not from
  this field.

### 2.3 Efficient indexes (raw-tuple; three families fold, `by_entitlement` slimmed)

Primary tails (raw components, entitlement-first for grants):
- grant primary = `(ent_rt, ent_rid, ent_kind, name, prin_rt, prin_id)` (was
  `(external_id)`).
- entitlement primary = `(rt, rid, ent_kind, name)` (was `(external_id)`).

The rule: a read that wants the grant **value** (`Sources`, refs) goes to the
primary (prefix-scan or point-Get); a read that wants **only principal identity**
uses a skinny index so the key-only scan stays IO-cheap. Every fold below is a
strict-prefix equivalence verified against the current encoders/readers.

Reader inventory (verified):
- `grantIndexKeys` (`grants.go:441-465`) writes exactly: `by_entitlement`
  (`ent.GetEntitlementId()`,prin), `by_entitlement_resource` (`ent_rt`,`ent_rid`),
  `by_principal` (prin_rt,prin_id), `by_principal_resource_type` (prin_rt),
  `by_needs_expansion`.
- `by_entitlement` is read three ways: (a) full records by entitlement
  (`PaginateGrantsByEntitlement`, scan+Get), (b) **principal keys only**
  (`PaginateGrantPrincipalKeysByEntitlement`, decode from index key, no Get — the
  expansion hot path), (c) full record by `(ent,prin)`
  (`PaginateGrantsByEntitlementPrincipal`, scan+Get).
- `by_principal` (`PaginateGrantsByPrincipal`, prefix `(prin_rt,prin_id)`) and
  `by_principal_resource_type` (`PaginateGrantsByPrincipalResourceType`, prefix
  `(prin_rt)`) — the latter is a strict prefix of the former's key.

| query (current reader) | new home | key / prefix | materialize |
|---|---|---|---|
| grants for entitlement, full (a) | grant **primary** | prefix `(ent_rt, ent_rid, ent_kind, name)` | returns records (incl. `Sources`); drops per-row Get |
| grant for `(ent, principal)` (c) | grant **primary** | point key `(ent_rt, ent_rid, ent_kind, name, prin_rt, prin_id)` | identity is unique → point Get; no index |
| principals for entitlement (b) | **skinny `by_entitlement`** | `ent_rt│ent_rid│ent_kind│name│prin_rt│prin_id`, empty value | key-only; `(prin_rt,prin_id)` decoded from tail |
| grants for entitlement-resource | grant **primary** | prefix `(ent_rt, ent_rid)` | **fold — drop `by_entitlement_resource`** |
| entitlements for resource | entitlement **primary** | prefix `(rt, rid)` | **fold — drop `by_resource`** |
| grants for principal | `by_principal` | `prin_rt│prin_id│ent_rt│ent_rid│ent_kind│name` | tail `(ent_rt,ent_rid,ent_kind,name)` → reorder → Get |
| grants for principal-RT | `by_principal` | prefix `(prin_rt)` | **fold — drop `by_principal_resource_type`** |
| grants needing expansion | `by_needs_expansion` (partial) | `ent_rt│ent_rid│ent_kind│name│prin_rt│prin_id` | reorder → Get |

So: **3 grant/entitlement index families dropped** (`by_entitlement_resource`,
`by_resource`, `by_principal_resource_type`), `by_entitlement` slimmed to skinny,
`by_principal` + `by_needs_expansion` kept (re-keyed). `idxResourceByParent` is
untouched (resources, out of scope).

Fold-equivalence proofs (why each drop is safe, not just convenient):
- `by_entitlement_resource(ent_rt,ent_rid)` ≡ grant-primary prefix
  `(ent_rt,ent_rid)`: the primary's first two components **are** the entitlement's
  resource (same source — the grant's `EntitlementRef`), so the prefix selects the
  identical set. Order changes (`name,prin` vs `external_id`); cursors reset.
- `by_resource(rt,rid)` ≡ entitlement-primary prefix `(rt,rid)`: same, low volume.
- `by_principal_resource_type(prin_rt)` ≡ `by_principal` prefix `(prin_rt)`: the
  principal-RT query is literally the leading component of the by_principal key;
  add a `prin_rt`-only by-value prefix (load-bearing trailing sep) and the set +
  materialization (Get per row) are identical.
- **Why raw components are required (not a single opaque entitlement-id):** the
  `(ent_rt, ent_rid)` resource fold needs `ent_rt`/`ent_rid` to be *separate*
  leading tuple components. If the primary keyed the entitlement as one
  colon-joined string component, a resource-prefix byte scan on `"rt:rid:"` would
  string-prefix-match across the resource boundary (e.g. resource `(a,"b")` vs
  `(a,"b:x")`) — re-introducing the exact colon fold this project removes. Raw
  components make the boundary a `0x00` separator, which the codec escapes, so the
  prefix is unambiguous.

Materialization details (the part that breaks silently if wrong):
- **Reorder, not just decode.** `by_principal`/`by_needs_expansion` store the
  entitlement coords in a different order than the primary, so the read path
  decodes prefix‖tail and **reassembles** the primary key
  `(ent_rt,ent_rid,ent_kind,name,prin_rt,prin_id)` before Get. `lastTupleComponent`/
  `decodeTwoTupleComponents` (`grants.go:766,788`) generalize to "decode N
  trailing components"; the reorder is new logic.
- **Entitlement-id decode at the by-entitlement read entrypoints.** Reads (a)/(b)/(c)
  receive an entitlement **id string** today (`ents[i].GetId()`); they must decode
  it to `(rt,rid,ent_kind,name)` (the §2.1 inverse) to build the primary/skinny
  prefix. The reader's `v2.Entitlement.Resource` may be unpopulated, so decode the
  canonical id rather than trusting the resource fields.

Write-cost framing (the real driver). Other benches show **~50% of grant
expansion cost is writing the indexes**, and the mechanism is **LSM compaction
write amplification**: the **misordered** (principal-keyed) families —
`by_principal` and `by_principal_resource_type` — are written in principal order,
uncorrelated with expansion's entitlement-order production, so each flush scatters
keys across the whole keyspace and compaction repeatedly rewrites them, moving a
large multiple of the actual data size. This redesign attacks that directly:
- **Dropping `by_principal_resource_type` removes one of the two scattered
  indexes outright** — a direct cut to the misordered-write cost, not just a read
  simplification.
- `by_principal` must stay (it is the principal-ordered view); its scatter is the
  residual target for the deferred-sort / spill-sort treatment.
- Entitlement-keyed writes (primary, skinny `by_entitlement`, `by_needs_expansion`)
  are **clustered by entitlement**, i.e. roughly in expansion production order, so
  they are the cheap writes.

Skinny `by_entitlement` (path b) — keep, but benchable on the whale:
- Its key equals the grant identity tuple (byte-identical to the primary tail and
  to `by_needs_expansion`, modulo the discriminator) with an **empty value** — a
  deliberate value-less duplicate that keeps path (b)'s blocks dense.
- It is a *sequential* (entitlement-clustered) write, so it does **not** add to the
  misordered ~50%; its costs are (i) extra write volume and (ii) **added c1z
  size** (a full second copy of every grant key).
- Keeping it pays only if a key-only scan of the *primary* is meaningfully more IO.
  Baton writes each grant once → value is inline in the data block (no value
  separation configured, `options.go:112-114`); at ~32 KB blocks the skinny index
  packs ~10–20× more keys/block than the value-laden primary, so path (b) should
  be that much cheaper.
- **Resolve on the whale**, measuring both axes: (1) expansion wall-time with the
  skinny index kept vs folded (path (b) reading the primary key-only); (2) the c1z
  size delta the skinny index adds. If wall-time is ~flat, fold it and reclaim the
  size; if it meaningfully speeds path (b), keep it and accept the size.

Reversibility (because index changes are hard to undo):
- **Hard to undo (one-way):** the primary-key *structure*
  (`ent_rt,ent_rid,ent_kind,name,…` / `rt,rid,ent_kind,name`) and the structured
  identity model — baked into every key and the migration. Get these right up
  front.
- **Reversible:** dropping the three secondary families. `index_migrations.go` is a
  backfill framework that *adds* index entries, so any dropped secondary can be
  re-added later via a versioned backfill without touching primaries. The folds are
  the low-risk part of this change.

### 2.4 Expansion graph

Build the graph keyed by the canonical entitlement id, identity baked in at
construction so the expander does no expansion-time lookup:
- One pass over the (few-thousand) entitlement keyspace builds a
  `legacy-label → canonical-id` map (only needed for ids that changed).
- `loadEntitlementGraph` (`syncer.go:1558`) builds nodes with canonical ids:
  destination from the grant's `EntitlementRef` inline, sources via the map. It
  already does a per-source `GetEntitlement` for validation (`:1563`), so this
  folds into the existing pass.
- The expander (`expander.go:373,410`, `topological_merge.go:311`) resolves by
  canonical id directly. Enumerating a source's principals stays on the **skinny
  `by_entitlement` index** (key-only, §2.3 path (b)); the descendant `Sources`-merge
  check is a direct primary Get on `(ent_rt, ent_rid, ent_kind, name, prin_rt,
  prin_id)` (path (c)). Graph nodes carry the structured `(rt, rid, ent_kind,
  name)`, not just the id string, so both keys build without a lookup.
- Cost: checkpoint/graph format bump; a version mismatch rebuilds the graph from
  synced grants (expansion is re-runnable).

### 2.5 Audit-driven site changes

- **Storage/index (engine):** key/index encoders (`keys.go`), write/dedup
  (`grants.go`, `entitlements.go`), index delete (`raw_records.go:134-158`),
  `bulk_import.go:296`, `if_newer.go`, `merge_accessor.go` exported helpers used
  by `synccompactor/pebble`, pagination cursors (`adapter_reader.go:217`,
  `grants_for_entitlements.go:93`) — all move to the raw structured internal key
  (entitlement-first for grants) and the reduced index set (§2.3):
  `by_entitlement_resource`, `by_resource`, and `by_principal_resource_type` are
  dropped (their reads become primary / `by_principal` prefix scans);
  `by_entitlement` is slimmed to a skinny key-only covering index; the
  full-record/`(ent,prin)` reads move to primary prefix-scan/point-Get; and
  `by_principal` + `by_needs_expansion` stay as written secondary indexes (re-keyed
  to raw tuples, materialized via reorder→Get).
- **Internal logic:** projection temp-DB key tail
  (`topological_merge_projection.go:286,341`) and streaming sort tie-break
  (`topological_merge_streaming.go:185`) key by the raw structured components.
- **Edges:** id generation, `translate_v2.go` output, revoke/explorer input use
  encode/decode. `c1zsanitize/transform.go:103` may need to become escape-aware
  (split on unescaped `:`) depending on which id form it processes — see §5;
  `bid` is a separate format and is unaffected.
- **Unchanged externally:** `v2.Grant.Id`/`v2.Entitlement.Id` for SDK-generated
  colon-free components are byte-identical; production C1 revoke carries the full
  grant.

## 3. Migration

Full, one-shot rebuild to the new layout, gated by a new engine-meta stamp
`grant_entitlement_id_format` (uint32; absent/0 = legacy, 1 = canonical), stored
like the index applied-version markers (`index_migrations.go:105`). Runs at
writable Open after `verifyOrStampKeyspaceVersion`, gated on stamp `< 1`; skipped
read-only (legacy read path retained for read-only legacy files).

**Mirror the migration state into the c1z header/manifest.** The Pebble
engine-meta stamp remains the authoritative in-payload state, but tools should be
able to tell "legacy indexes vs new-style indexes" without extracting/decoding the
Pebble payload. Add a cheap header projection to `C1ZManifestV3` (the manifest
immediately after `C1Z3\0` + length, read by `format/v3.ReadManifestHeader`):

```
message C1ZManifestV3 {
  ...
  PebbleIdIndexFormat pebble_id_index_format = 42; // header mirror, advisory
}

enum PebbleIdIndexFormat {
  PEBBLE_ID_INDEX_FORMAT_UNSPECIFIED = 0; // unknown/legacy/no header mirror
  PEBBLE_ID_INDEX_FORMAT_LEGACY_EXTERNAL_ID = 1;
  PEBBLE_ID_INDEX_FORMAT_STRUCTURED_V1 = 2; // this plan
}
```

Save path: `BuildManifestWithSyncRuns` (`manifest.go:41`) reads the engine-meta
stamp and sets the manifest field. Header read path: `unmarshalManifestHeader`
(`format/v3/envelope.go:526`) shallow-decodes the new enum alongside
`engine_schema_version`, `sync_runs`, and `fold_dead_bytes`. Invariant:
manifest/header state is **advisory** and may be absent on older files; writable
Open still trusts and migrates based on engine-meta. After a successful migration,
the next save writes `STRUCTURED_V1`, so compaction/source-selection/tooling can
inspect format state via `ReadManifestHeader` without unpacking Pebble.

**Decoupled from expansion (so it is cheaply testable).** The migration only
re-keys whatever grant/entitlement records exist — it never runs the expander, so
it applies equally to a **synced-but-not-expanded** whale (small base grant set)
and a **post-expansion** whale (~54M grants). Expose the rebuild as a standalone,
directly-invokable operation (open writable → run rebuild → stamp → close) — not
something only reachable through a full sync+expand cycle — so the migration can be
benched and iterated on the cheap pre-expansion sync without paying for expansion
each run. The two inputs differ only in grant volume, so pre-expansion is the fast
inner-loop fixture and post-expansion is the scale check.

Per record (grants are the volume):
1. Read identity via raw/shallow scans — **avoid full proto decode on the hot
   path**:
   - Grants: use `scanGrantIndexFieldsRaw` (`raw_records.go:311`) to extract
     `EntitlementRef`, `PrincipalRef`, and `needs_expansion` directly from the
     marshaled value.
   - Entitlements: add the equivalent raw scanner for `resource.{rt,rid}` and
     `external_id`/`id` (enough to derive `(ent_kind,name)` via §2.1). Full proto
     unmarshal is fallback-only for malformed/unexpected wire shapes, and should
     be counted/logged.
2. Compute the raw structured primary key (entitlement-first for grants) + the
   written secondary-index keys: skinny `by_entitlement`, `by_principal`,
   `by_needs_expansion` (§2.3). `by_entitlement_resource`, `by_resource`, and
   `by_principal_resource_type` are NOT written — they are primary / `by_principal`
   prefix-scan reads.
3. Collapse legacy duplicate grants that map to the same structured identity
   before writing the new primary key. Merge rule:
   - `discovered_at`: keep the earliest non-nil timestamp. This is the first-seen
     time for the logical grant.
   - `external_id`: keep the external id from the record that supplied the winning
     `discovered_at`; tie-break lexicographically for deterministic output. This
     remains informational after the migration.
   - `annotations`: concatenate, then dedupe by stable `(type_url, value_bytes)`.
   - `sources`: merge by source entitlement id. Duplicate source keys OR
     `is_direct`; keep resource fields from a direct entry when present, otherwise
     the first non-empty fields.
   - `needs_expansion`: OR.
   - `expansion`: merge when either side has one. `entitlement_ids` and
     `resource_type_ids` are set-unioned and sorted; `shallow` is AND (deep
     expansion wins if either record requested it).
4. Write the collapsed **record value** under the new primary key. For records
   without duplicates this is byte-for-byte unchanged; duplicate groups are the
   only values rewritten. The connector's original `external_id` field is still
   retained on the winner (§2.2); emit secondary-index entries from the collapsed
   winner.
5. Do all writes through the **SST ingest path**, not normal Pebble point writes:
   build sorted replacement SSTs for each destination family (grant primary,
   entitlement primary, skinny `by_entitlement`, `by_principal`,
   `by_needs_expansion`) using the bulk/SST writer (`bulk_import.go`). This avoids
   the LSM churn/compaction amplification that motivated the project.
6. Delete old primary + old index ranges and ingest the replacement SSTs
   (range-replace, mirroring the compactor).
7. Stamp `= 1` with `pebble.Sync` only after success.

Crash-safety / idempotency (important — legacy and canonical primary keys live in
the same `v3│typeGrant│…` range and are not cheaply distinguishable in place, so
do **not** rely on per-key "already migrated?" detection). Instead use a
build-then-replace shape, mirroring the compactor:
1. Read the **old** keyspace (never mutated during the build) and write the
   complete new grant/entitlement primary + index families into fresh SSTs via
   the bulk/SST writer (`bulk_import.go`), sorted, avoiding LSM churn.
2. Replace each family's whole key range with its new SST(s) (range-delete +
   ingest, or the compactor's range-replace), then stamp `= 1` with `pebble.Sync`.

Crash before the replace → old data intact, migration re-runs from scratch
(idempotent: it rebuilds the same SSTs from the same old data). Crash after the
stamp → done. The migration **blocks Open**, is not internally resumable, and on
the whale (54M grants) is a multi-minute one-shot — log progress.
`ResetForNewSync` preserves engine-meta (the stamp survives) but wipes data;
post-reset syncs write canonical keys directly (stamp ≥ 1), so no re-migration.

## 4. External-interface preservation

- Colon-free components (the norm): `encode` == today's `fmt.Sprintf` id →
  emitted `v2.*.Id` byte-identical → operators, CSV/diff, C1 see no change.
- Colon-bearing components: the emitted id is now escaped (correct and
  unambiguous; the old id was already broken/folded).
- Connector-set custom (or otherwise divergent) ids: the canonical encoding is
  the emitted round-trip id. For custom entitlements this is the explicit tagged
  form `esc(rt):esc(rid):custom:esc(original_external_id)` (and grant ids use the
  same tagged entitlement subtuple). The original id is **retained in the record
  value** (non-key) so baton can optionally output both when they differ. The id
  is baton-owned and deprecated, so the canonical form is authoritative for
  lookup/revoke; the original is informational. (§8)
- Production C1 revoke unaffected (carries the full grant). Local-CLI revoke and
  explorer round-trip the canonical (escaped) id, which baton both emits and
  parses.
- BID is a separate, already-escaped id scheme — untouched.

## 5. Risks & edge cases

- **`c1zsanitize` `strings.Split(id, ":")`** (`transform.go:103`): verify which
  id form it sees. If it transforms the canonical emitted id (escaped), it must
  split on **unescaped** `:` and re-escape per component, or it will corrupt
  escaped components; if it only ever transforms the stored original id (raw,
  un-escaped), it is unaffected. Resolve during phase 7 and cover with its tests
  (`idalign_test.go`).
- **Checkpoint/graph format bump** (§2.4): rebuild-on-mismatch; accepted.
- **Projection temp DB / compactor** build keys via the engine encoders; they
  must switch in lockstep or the whale benches and compaction parity tests fail.
- **Index-emitter lockstep (silent-corruption risk).** Grant index keys are
  produced in three places that must change together: `grantIndexKeys` +
  `writeGrantIndexesScratch` (`grants.go`), and `ForEachGrantIndexKey` +
  `ForEachGrantIndexKeyRaw` (`merge_accessor.go:260,290`); the delete side is
  `deleteGrantIndexesRaw`/`raw_records.go:134-158`. Entitlement index keys:
  `AppendEntitlementIndexKeyRaw(Bytes)` (`merge_accessor.go:240-254`). Dropping the
  three families also means removing their exported `*LowerBound`/`*UpperBound`
  helpers (`keys.go:451-488`) and the `synccompactor/pebble` code that copies those
  ranges. A single missed emitter writes a stale/orphan index. Add a test that the
  written index-key set equals the documented set (§2.3) for a sample grant.
- **Header mirror is not authoritative.** `pebble_id_index_format` in the c1z
  manifest is for shallow inspection only; it can be absent or stale if a file was
  produced by an older SDK or copied before save. Writable Open must rely on
  engine-meta and fix/save the header after migration. Tests must cover header
  absent + engine-meta legacy, header legacy + engine-meta structured, and header
  structured + engine-meta missing/corrupt (payload wins; corrupt payload still
  errors).
- **Migration fast path must stay raw + SST.** Full proto unmarshals in the whale
  migration loop or point writes through `Batch.Set` will make the migration look
  like a second expansion. Instrument migration counters: records scanned, raw
  scan successes/fallback unmarshals, SST bytes emitted, ingested SST count, range
  delete bytes, compaction bytes-written. Treat fallback unmarshals as exceptional
  and alert in the bench output.
- **Custom-id supersession** changes the emitted id for connectors that set a
  custom grant/entitlement id (rare; deprecated field). Accepted (§8).
- **Principal parent** stays out of identity (grant identity is
  `(ent_rt, ent_rid, ent_kind, name, prin_rt, prin_id)`, matching the
  entitlement-first primary key and the expander's `descendantGrantKey(rt, res)`
  dedup).

## 6. Phasing

All phases land as a single release gated by the migration (§3) — there is no
supported half-migrated on-disk state. The ordering below is build/review order;
e.g. phase 2 changes only the emitted id *string* until phase 4 re-keys storage,
and a fresh sync between those phases still works (the engine keys by whatever
`external_id` it is handed, as today).

1. **Edge codec** (`pkg/types/grant`, `pkg/types/entitlement`): `esc`,
   `encodeGrantID`/`encodeEntitlementID`/`decode*`; unit-tested injectivity,
   SDK-vs-custom tagging, and byte-identity for SDK-generated colon-free ids.
   Pure additions.
2. **Generation** switches to the encoders (`NewGrant`, `NewGrantID`,
   `NewEntitlementID`, `expander.go:670`, `topological_merge.go:573`).
3. **Engine keys/indexes** (`keys.go`): raw structured internal-key builders
   (entitlement-first grants) + the reduced secondary-index encoders + tail
   decoders; primary-prefix readers for the folded families (§2.3).
4. **Engine write/read** (`grants.go`, `entitlements.go`, `raw_records.go`,
   `bulk_import.go`, `paginate.go`, `adapter*.go`): key by structured identity;
   `GetGrant`/`GetEntitlement` decode the id → key.
5. **Migration** (§3): rewrite pass + stamp + Open wiring, plus a standalone,
   directly-invokable entry point (no sync/expand) for the cost bench.
6. **Manifest/header mirror** (§3): add `pebble_id_index_format` to
   `C1ZManifestV3`, set it from engine-meta in `BuildManifestWithSyncRuns`, and
   shallow-decode it in `ReadManifestHeader`.
7. **Expansion graph** (`graph.go`, `syncer.go`): canonical-id nodes + one-time
   map; checkpoint version bump.
8. **Edges & sanitizer**: `translate_v2.go` output/`decode` input;
   `c1zsanitize/transform.go` escape-aware if it sees canonical ids (§5).
9. **Compactor** (`synccompactor/pebble`): consume the new encoders and prefer the
   manifest/header mirror for cheap source inspection while treating payload
   engine-meta as authoritative after open.

## 7. Test plan

- Codec: round-trip `encode`/`decode`; injectivity (two folded tuples → distinct
  ids); byte-identity vs `fmt.Sprintf` for SDK-generated colon-free ids;
  `WithSlug` case proves identity uses `name`, not `slug`; custom-vs-SDK case
  proves `(rt,rid,kind="custom",name="admin")` does not collide with
  `(rt,rid,kind="sdk",name="admin")` and emits the 4-/6-leaf tagged form.
- Engine: collision test — two records that fold to the same legacy id get
  distinct internal keys and coexist; `GetGrant`/`GetEntitlement` by canonical id
  resolve; reader output byte-identical to pre-change for SDK-generated records
  whose original id already equals the canonical encoding (custom/colon-bearing
  ids are expected to change, §4).
- Folded-read parity: `by_entitlement_resource`/`by_resource` (primary prefix) and
  `by_principal_resource_type` (`by_principal` prefix on `prin_rt`) return the same
  set as the old indexes; the surviving `by_principal`/`by_needs_expansion`
  reconstruct + **reorder** prefix‖tail into the primary key correctly before Get.
- Identity uniqueness: at most one grant per `(entitlement, principal)`, so the
  path-(c) `(ent, prin)` check is a point Get (regression-guards the scan→Get
  swap).
- Skinny-index decision bench (whale): expansion wall-time with skinny
  `by_entitlement` kept vs folded (path (b) reading the primary key-only), **plus
  the c1z size delta** the skinny index adds. Decides keep vs fold (§2.3).
- Migration: legacy fixture → writable Open → stamp=1, all keys canonical,
  `GetGrant`/`GetEntitlement` work, idempotent re-run is a no-op, read-only open
  of a legacy fixture still reads.
- Migration fast path: fixture with grants/entitlements migrates using raw scanners
  (zero fallback unmarshals in the normal case) and replacement SST ingest, not
  point-write batches; assert emitted family SSTs are sorted and complete.
- Header/manifest mirror: save legacy and structured files and verify
  `ReadManifestHeader` reports `LEGACY_EXTERNAL_ID` vs `STRUCTURED_V1` without
  extracting Pebble; verify absent/stale header fields do not override engine-meta
  during writable Open.
- Migration cost bench (standalone, no expansion — §3): run the rebuild on two
  whale fixtures and report wall-time, compaction bytes-written / write-amp, and
  c1z size delta for each:
  (1) **pre-expansion** whale sync (base grant set) — the fast inner-loop fixture;
  (2) **post-expansion** whale (~54M grants) — the scale check.
  The pre-expansion run is what makes iterating on the migration cheap; the
  post-expansion run bounds the one-shot cost operators will actually pay.
- Differential: `topological_merge_differential_test.go` stays green (projection
  re-key risk).
- Sanitizer: `c1zsanitize` escape-aware split round-trips escaped ids.
- Whale: migration wall-time + post-migration expansion correctness on
  `whale_rolled_back_960.pebble.c1z`; expansion wall-time vs the pre-change
  baseline (expecting a cut from dropping the scattered `by_principal_resource_type`
  + slimmer keys); **compaction bytes-written / write-amplification** (the real
  cost mechanism — Pebble compaction metrics, expecting the biggest drop here from
  removing a scattered family); and **c1z size before vs after** (net of three
  dropped families, slimmer tuple keys, and the skinny `by_entitlement` duplicate).
  Current reference run (2026-06-30, `BATON_EXPAND_BY_PRINCIPAL_PASS=1`,
  `BATON_PEBBLE_DEFER_EXPANSION_INDEXES=1`, `BATON_EXPAND_SKIP_GET=1`):
  base grants `3,678,491`; dirty grants written `54,050,798`; final grants
  `57,727,817`; projection rows `904,370`; expansion elapsed `9m44.580s`;
  test elapsed `718.990s`; expanded output size `3,846,272,858` bytes. Treat this
  as the comparison baseline for wall-time and c1z size until a newer controlled
  run supersedes it.

## 8. Decisions (resolved)

- **Encode at the edges, not a stored internal-id encoding.** The id is a
  canonical escaped string emitted/parsed at baton's edges; identity is
  structured internally.
- **Flat escaped join**; SDK-generated colon-free components → byte-identical to
  today (no needless compat break). `esc` escapes `:` and `\`. Connector-custom
  entitlement ids use an explicit `custom` marker in the canonical id so they
  cannot collide with SDK-generated names on the same resource.
- **`name`, not `slug`, is the entitlement identity component**, plus
  `ent_kind` (`sdk` vs `custom`) to distinguish SDK-derived names from
  connector-custom ids (`WithSlug` diverges from the `Id`, `entitlement.go:35-39`).
- **Colon-escaping is edge-only; keys store raw components.** The `0x00` tuple
  codec already separates injectively, so keys never wrap the colon-escaped id —
  no double encoding. Internal layout keys by the raw structured identity; no
  `external_id`-as-key, no `by_external_id` index.
- **Index folding is per-read, verified as strict-prefix equivalences** (§2.3).
  With an entitlement-first grant primary, three families drop —
  `by_entitlement_resource` and `by_resource` (→ primary prefix scans) and
  `by_principal_resource_type` (→ `by_principal` prefix on `prin_rt`); full-record
  "grants for entitlement" → primary prefix and the `(ent,prin)` check → point Get.
  **`by_entitlement` stays** as a skinny covering index (expansion's key-only
  principal scan wants dense blocks; benchable — §2.3). `by_principal` and
  `by_needs_expansion` stay, re-keyed. Net: drop three families, slim one, keep two.
- **Reversibility:** the primary-key structure + identity model are one-way (baked
  into every key and the migration) — get them right up front. Dropping the three
  secondaries is reversible: `index_migrations.go` backfills can re-add any of them
  later without touching primaries.
- **Canonical encoding is the authoritative round-trip id; the original id is
  retained** (non-key, in the record value). On divergence baton may optionally
  output both; the canonical form is used for lookup/revoke (grant id is
  baton-owned and deprecated, `grant.proto:27`). Custom entitlements round-trip as
  `rt:rid:custom:<escaped-original-id>` rather than the raw original id, because
  direct lookup still needs the resource tuple and the custom-vs-SDK tag.
- **Full migration** (not partial/byte-identical-only), gated by a new
  engine-meta format stamp; reuse the bulk/SST writer; `keyspaceVersion` stays 2
  (bumping it rejects old files).
- **C1Z header/manifest mirrors the migration state.** Add
  `pebble_id_index_format` to `C1ZManifestV3` so `ReadManifestHeader` can report
  legacy vs structured indexes without extracting Pebble. Engine-meta remains
  authoritative; the header is advisory and refreshed on save.
- **Expansion graph re-keyed to canonical ids**, built lookup-free via a
  one-time map; checkpoint/graph format bumped (rebuild-on-mismatch).
