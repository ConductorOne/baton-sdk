# Raw-Component Identity: external ids are a contract, not a key

Status: proposed — supersedes the "encode at the edges" §0 approach of
`grant-entitlement-injective-id.md`. The structural-identity keyspace, the
dropped index families, and the expansion write-path work all carry over
unchanged; what changes is how identity is derived and what baton emits.

## 0. Constraints (why the edge-encoding approach is out)

1. **External ids are an external-consumer contract.** C1 carries
   control-plane configuration attached to the exact id strings connectors
   emit today. The emitted/stored id definition must remain byte-identical
   to `main` — no escaping, no `custom` kind marker, no canonical
   re-encoding, no rewriting of stored strings, ever.
2. **Ids must not be used internally for identity.** The lossy `:`-join is
   non-injective; any scheme that *parses* an id string to decide identity
   inherits its ambiguity (this is where every divergent-reading bug on
   this branch came from).
3. **SQLite code paths stay untouched.** SQLite is soon read-only (to serve
   self-hosted connectors that have not updated); it keeps the legacy id
   semantics it has always had.
4. **This branch is unmerged and has run nowhere.** No file in existence
   contains the branch's current escaped ids or structured keys. We are
   free to change the branch's on-disk layout without compatibility shims.
   The only inputs that exist are `main`-format pebble files and sqlite
   files, both containing raw legacy ids exclusively.

## 1. Identity derivation: byte-prefix stripping, no grammar

Records already carry every identity component as structured fields.
Identity is derived from those fields only:

- **Entitlement identity** from `(resource.rt, resource.rid, external_id)`:

  ```
  if external_id starts with the exact bytes rt + ":" + rid + ":":
      flag = stripped;  tail = external_id[len(prefix):]
  else:
      flag = opaque;    tail = external_id
  identity = (rt, rid, flag, tail)
  ```

- **Grant identity** = `(entitlement identity from EntitlementRef, principal_rt, principal_id)`
  using `EntitlementRef{rt, rid, entitlement_id}` and `PrincipalRef{rt, id}`.

Properties:

- **Bijective given (rt, rid).** Stripped identities reconstruct as
  `rt:rid:tail`; opaque ones are exactly the ids that do *not* start with
  that prefix, so the two ranges cannot collide, and reconstruction is
  byte-exact. This is the injectivity fix with zero interpretation: no
  unescaping, no `custom:` decoding, no canonical re-emission. (It is the
  old `sdk`/`custom` split reduced to what it really was — a compression
  flag — with the escape grammar deleted.)
- **Key shapes and arities are unchanged** from the branch's current
  layout: entitlement primary tail stays 4 tuple components
  (`rt│rid│flag│tail` replacing `rt│rid│kind│name`), grant primary tail
  stays 6, the by_principal permutation and the
  `appendGrantByPrincipalKeyFromPrimary` splice are untouched. The flag is
  a 1-byte component (marginally smaller than the literal `"sdk"`/
  `"custom"` strings). All size/write-cost wins of the redesign (dropped
  index families, entitlement-first primaries, prefix stripping) are
  preserved.
- **One derivation function.** `entitlementIdentityFromParts` becomes the
  byte-prefix rule above; `DeriveLegacyEntitlementIDParts`,
  `DeriveEntitlementIDParts`'s decode fallback, and
  `entitlementIdentityFromID` (grammar decode) are deleted. Migration,
  bulk import, write paths, and readers all share the single rule, so
  primary/index divergence is impossible by construction.

## 2. Emitted ids: revert to main's definitions

- `NewEntitlementID` → `fmt.Sprintf("%s:%s:%s", rt, rid, name)` (as main).
- `NewGrant` / `NewGrantID` / the expansion evaluators' synthesized-grant
  ids → `entitlement.Id + ":" + principal_rt + ":" + principal_id`
  (as main; `expander.go`'s existing concat becomes *the* definition
  again rather than a must-change site).
- Delete from `pkg/types`: `JoinEscapedID`, `SplitEscapedID`,
  `EncodeEntitlementID`, `DecodeEntitlementID`, `EntitlementIDParts`,
  `Derive*EntitlementIDParts`, `EncodeGrantID`, `DecodeGrantID`,
  `GrantIDParts`, the kind constants, and their tests.
- Translation (`V3*ToV2`) emits **stored strings only**: entitlement id =
  record `external_id`; grant entitlement stub id = the ref's raw
  `entitlement_id`; grant id = record `external_id` when present,
  otherwise the legacy concat reconstruction (byte-identical to what main
  emits for synthesized grants). `canonicalEntitlementRecordID` /
  `canonicalEntitlementRequestID` and the adapter's canonicalization
  sites (`PendingExpansionPage` etc.) are removed — graph ids, sources-map
  keys, expandable annotation ids, and stored refs are all the same raw
  connector strings end to end. The canonical-vs-raw sources hazard (and
  the parity-test carve-outs that hid it) disappears; those assertions
  come back.
- `pkg/c1zsanitize/transform.go` reverts to main's raw-split behavior.

## 3. Point lookups by bare id string (query planning, not identity)

Readers that receive only an id string (`GetEntitlement`, `GetGrant`,
`DeleteGrant`, `ListEntitlementsByIds`, …) must recover components at the
lookup edge. Parsing here is a *query plan* — a miss is a miss, never a
mis-keyed write. No new on-disk index: lookups are probe-first with a
lazy in-memory fallback, keeping the change read-side only.

Resolution rule: **probe combinatorially, exactly one hit wins.** A probe
hit is an exact match, not a guess — a candidate split `(rt, rid, tail)`
that finds a row identifies a record whose reconstructed external id is
byte-for-byte the query string (bijectivity of the flag/tail encoding).
Collecting all candidate hits therefore computes "every row whose external
id equals this string": one hit → that row; zero → NotFound; more than one
→ ambiguous (error). The >1 case is real in the new layout — two
entitlements on different resources may share one external-id string
(legacy keyed by the string, so one silently overwrote the other) — and
the rule surfaces it instead of picking silently.

- **Tier 1 — candidate probing** (covers ~all real ids, zero state): a
  prefix-shaped id carries its own components; every colon is a candidate
  `rt`/`rid`/`tail` boundary. Probe each candidate identity key with a
  point Get — **no early exit** (uniqueness must be certified), O(colon
  count) probes for entitlements, mostly cheap misses. Colon-bearing
  resource ids (ARNs) resolve because every boundary is tried.
- **Tier 2 — lazy id map** (opaque ids only, on demand): probing cannot
  see ids with no splittable shape. If all probes miss, build
  `map[external_id] → []primary key` (a multimap, so the exactly-one rule
  applies here too) from one scan of the entitlement primaries, cached on
  the engine behind an entitlement-write generation counter (any
  entitlement put/delete bumps it; next lookup rebuilds). Cost is paid
  only when an opaque bare-id lookup actually happens; the scan amortizes
  across subsequent lookups and across a `ListEntitlementsByIds` batch.
  Entitlement cardinality is small relative to grants, so the map is
  memory-bounded; entitlement writes cluster early in a sync while
  bare-id lookups (expansion prep, reader APIs) come later, so rebuilds
  are rare.
- **Grant lookup** `GetGrant(id)` with `id = entID + ":" + prt + ":" + pid`:
  enumerate candidate `(entID, prt, pid)` splits; resolve each candidate
  `entID` through the entitlement lookup above to obtain `(rt, rid)`;
  build the grant identity and point-Get. All candidates probed
  (O(colons²) worst case, bounded and edge-path only); exactly one hit
  wins, zero → NotFound, multiple → ambiguous. No per-grant id index
  (which would double index writes on a large tenant's file).
- **Scope: combinatorial bare-id resolution is an interactive-edge
  concern only — enforced, not just conventional.** Verified consumers of
  Get/Delete-by-bare-id: the `--revoke-grant` local task path
  (`provisioner.go`), and the baton explorer's store cache. The C1-driven
  RPC task paths (`tasks/c1api` grant/revoke) carry the full grant object
  — identity there comes from refs, never the id string. Sync and grant
  expansion never reach the probing machinery:
  - every expansion grant-listing call passes the fetched entitlement
    record (refs derive identity from parts); `expanderStoreAdapter`
    rejects a refs-less request at the boundary rather than letting it
    fall through to string resolution;
  - the syncer's external-principal rewrite deletes via
    `DeleteGrantByRefs`, and incomplete refs there are an error, not a
    bare-id fallback;
  - the only string expansion resolves is a `GrantExpandable` source
    entitlement id (connectors provide bare strings by contract), and
    that uses ONLY the exact-match record map — byte-equality against
    stored external ids, SQLite-parity semantics — erroring loudly if
    the id matches records on two different resources.
- **Custom grant external ids (no concat shape, e.g. `"grant-42"`)**
  cannot be recovered by splitting and have no scan-sized fallback.
  Resolution: the baton utility computes and displays the identity token
  (tuple-encoded refs, opaque/base64) alongside the unchanged external
  id, and id-accepting flags take either form — token → exact lookup;
  raw id → combinatorial probe; ambiguous → error listing the candidate
  matches (the CLI is interactive; surfacing candidates is useful). An
  exceptions-only `external_id → identity` index (rows only where the
  stored id differs from the concat reconstruction) remains available as
  a fallback if a non-CLI consumer ever needs raw-string grant
  addressing; not built now.
- Per policy: resolvable → resolve; genuinely unresolvable → NotFound /
  error. Never guess into a write.
- Side effect: `PutEntitlementRecords`' dead read-before-write still goes
  away (independent of lookups — nothing needs cleanup on overwrite).

## 4. Migration and conversion

- **Pebble id-index migration** (main-format → structured): re-key both
  record types from value fields via the single derivation. No id parsing,
  no ambiguity errors, no stored-string rewrites (the normalization added
  earlier is removed). Keeps: skip-with-warning for rows missing
  refs/resource; grant duplicate merge (identical refs, distinct external
  ids) with warning. Entitlement duplicates become impossible (identity
  contains the unique `external_id`), so the strict duplicate check stays
  as an invariant. Bump the engine-meta id-index stamp so any dev
  artifacts written with the interim escaped-identity layout are rejected
  loudly rather than misread.
- **sqlite→pebble conversion**: same derivation swap inside the pebble
  bulk import; the sqlite reader side is untouched. Keeps the entitlement
  spill sorter (identity-tuple order still diverges from the external-id
  scan order) and the grant duplicate merge+warn; drops the entitlement
  duplicate-resolve policy (impossible again → strict).

## 5. What carries over vs. reverts from recent work

Carries over: structural keyspace + dropped index families; entitlement
spill sorter in bulk import; duplicate-resolving k-way merge and the grant
merge+warn policy (migration and conversion); missing-ref skip+warn;
compaction scheduler; deferred index build; synth encode fast path
(kind→flag component swap only).

Reverts/removes: escaped-id emitters and the id grammar in `pkg/types`;
`legacyEntitlementIdentityForMigration` and the stored-string
normalization; entitlement duplicate merge policy; adapter/expansion
canonicalization sites; sanitizer escape-aware split; tests pinning
canonical/escaped ids (rewritten to raw expectations).

## 6. Test additions

1. Property: identity ↔ external id bijectivity given (rt, rid) —
   `reconstruct(derive(id)) == id` byte-exact for adversarial ids
   (colon-bearing rt/rid, `rt:rid:`-prefixed opaque ids, escapes-looking
   bytes, empty tails).
2. End-to-end with ARN-style colon-bearing resource ids and opaque
   entitlement ids: put → list → point-Get → delete on pebble; grants for
   entitlement/principal.
3. Migration: main-format fixture with opaque + prefixed + duplicate-ref
   rows → re-keyed, byte-identical emitted ids, by_principal resolves.
4. Lookup tiers: prefix-shaped and ARN-style ids resolve via probing;
   opaque ids resolve via the lazy map (including after an entitlement
   write invalidates it); unresolvable ids → NotFound. Grant lookups
   resolve colon-bearing entitlement ids via the candidate-split walk.
5. Restore parity-suite assertions removed on this branch (`sourceDirect`
   comparison; pebble shallow/resource-type cases; pebble resume parity).
