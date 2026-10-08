# Formal contract for v3 (Pebble) c1z snapshots and readers

A small Lean 4 package that states, and proves for an executable model,
what a successful read of a v3 c1z means: how records are addressed,
what a write does, what a complete paginated traversal returns, when a
sync is readable, and which results may be taken as exhaustion. The
model generates test vectors that a Go test replays against the real
Pebble engine.

This package follows the proof agenda in the C1 proposal "Baton SDK
proof contract for c1z snapshots and readers". It covers that
proposal's suggested first deliverable: structural identities, write
semantics, point lookup plus complete paginated enumeration of one
finished snapshot, explicit exhaustion versus failure, and a validity
predicate that states exactly what was established. The SQLite/v1
engine is out of scope throughout; where the engines differ, this
package follows Pebble.

Nothing here verifies Go code. The Lean theorems are about the model in
`C1z/`; the Go test shows the engine agrees with the model on the
generated cases. Keep "proved for the model" and "the engine passed
these cases" distinct when citing this package.

## Layout

```
lean-toolchain          pinned toolchain (elan reads it)
lakefile.toml           Lake config; no external dependencies
C1z.lean, C1z/          the proved core (never `import Lean`)
  Basic.lean            Byte, Bytes
  Codec.lean            tuple codec, key headers, scan prefixes
  Order.lean            bytewise lexicographic order
  Identity.lean         structural identities and their keys
  Store.lean            one keyspace: replace semantics, enumeration
  Paginate.lean         page tokens, clamping, complete traversal
  Sync.lean             one-sync-per-file lifecycle and selection
  Result.lean           outcome algebra, bare-id resolution
  Records.lean          grant and entitlement stores under structural keys
  Index.lean            the by_principal index, deferred writes, hidden rows
  GrantLookup.lean      grant lookup by bare external id (two-phase rule)
  Stream.lean           streaming readers, cancellation, early stop
  Digest.lean           grant digests: canonicalization, fold, invalidation
  Views.lean            one theorem that every read view agrees; bulk reads
  Container.lean        the .c1z envelope: seal, public open, damage classes
Oracle/                 case generator (tooling; may `import Lean`)
generated/cases.json    the oracle's output, checked in (see "Trust")
ORACLE_SCHEMA.md        the JSON contract between oracle and Go test
scripts/check.sh        warnings-as-errors build, axiom audit, freshness
scripts/Axioms.lean     the `#print axioms` list the audit runs
AXIOMS.golden           reviewed axiom sets, one line per theorem
ROADMAP.md              next increments, ordered by expected yield
```

The Go consumers are `pkg/dotc1z/engine/pebble/formal_conformance_test.go`
(every family that drives the engine directly) and
`pkg/dotc1z/formal_container_test.go` (the `container` family, which
goes through the public `NewStore`/`Close` path and so lives in the
package that owns it).

## Running

```bash
elan toolchain install "$(cat formal/c1z/lean-toolchain)"   # once
make formal-c1z-check          # build, axiom audit, oracle freshness
make formal-c1z-oracle         # regenerate generated/cases.json
make formal-c1z-conformance    # replay cases against the Pebble engine
```

`formal-c1z-conformance` needs only Go and runs in the ordinary test
suite; the other two need `lake`.

Two opt-in paths widen the oracle's coverage beyond the hand-chosen
corpus. Neither runs in CI and neither output is checked in.

```bash
make formal-c1z-conformance-random   # fixed corpus + C1Z_RANDOM_N random cases per family
make formal-c1z-property             # Go-generated random inputs answered by the live oracle
```

The random corpus is a pure function of `C1Z_RANDOM_N` and
`C1Z_RANDOM_SEED`, generated in Lean and replayed by the same Go test.
The property test generates inputs in Go, runs `c1z-oracle --respond`
as a subprocess to obtain the model's expected outputs, and replays
them; it logs its seed so a failure can be rerun with
`C1Z_FORMAL_PROPERTY_SEED=<seed>`. See `ORACLE_SCHEMA.md`, "Oracle
modes", for the request protocol and environment variables.

For a long run, `scripts/soak.sh` (or `make formal-c1z-soak
SOAK_ARGS="-n 1000 -s 1 -e 50 -j 2"`) loops the property test over a
seed range, one `go test` per seed and `-j` seeds at a time, reporting
every failing seed with its replay command. Per-seed logs land in
`.lake/soak-<seed>.log`. The container family is disk-bound because each
case closes the store (a real checkpoint and envelope write) and reopens
it by extracting the envelope, so it runs `C1Z_FORMAL_CONTAINER_N` cases
per seed (default a tenth of `C1Z_FORMAL_PROPERTY_N`); lower that to
shrink it, or set `C1Z_FORMAL_TMPDIR` to a RAM disk to relocate it.

Cases run as parallel subtests, and each case's engine opens over
Pebble's in-memory filesystem, so a 200-per-family run takes about a
second and a 1000-per-family seed a few seconds. Set `C1Z_FORMAL_DISK=1`
to open engines on disk instead; that path is about 50x slower because
`EndSync` fsyncs serialize at the device, and it exercises nothing the
model describes.

## Model-to-implementation map

| Model | Implementation |
|---|---|
| `Codec.escape`, `encodeTuple` | `codec/tuple.go` `appendEscaped`, `AppendTupleStrings` |
| `Codec.encodeKey`, `header` | `keys.go` `encodeResourceKey` family; `internal/rawdb/keyspace.go` bytes |
| `compressEnt`, `expandEnt` | `identity.go` `entitlementIdentityFromParts`, `externalID` |
| `GrantId`, `GrantRecord` | `identity.go` `grantIdentity` (no `external_id` in the key) |
| `lexLt` | Pebble default comparer (`bytes.Compare`) |
| `Store.put`, `putBatch`, `erase` | `internal/rawdb/records.go` whole-value `Set`; per-call last-occurrence dedup in `grants.go`, `resources.go`, `entitlements.go`, `resource_types.go` |
| `Paginate.page`, `clampPageSize`, `checkCursor` | `paginate.go` `iteratePrimaryPageWithKey`, `clampPageSize`, `rangeAfter` |
| `Sync.writeGate`, `startNewSync`, `endSync`, `resumeSync` | `engine.go` `withWrite`, `requireCurrentSync`; `adapter.go`; `cleanup.go` `ResetForNewSync` |
| `Sync.latestFinished`, `resolveActiveSync` | `sync_runs.go` `LatestFinishedSyncRecord`; `adapter_reader.go` `resolveActiveSyncForReader` |
| `Result.resolveBare` | `lookup.go` exactly-one rule, `ErrAmbiguousExternalID` |
| `Result.complete`, `ErrorTerminal` | `pkg/connectorstore/streaming.go` contract; `adapter_streaming.go` |
| `GrantStore.putGrants`, `grantsForEntitlement` | `grants.go` `PutGrants`; `paginate.go` `paginateGrantsByEntitlement` with `keep` |
| `IndexedGrants.putGrants`, `putGrantsDeferred`, `endSyncRebuild` | `rawdb` `StageGrantPutInline` / `StageGrantPutDeferred`; `deferred_index.go` `BuildDeferredGrantIndexes` |
| `GrantLookup.resolve` | `lookup.go` `resolveGrantIdentityByExternalID` (candidates, then scan) |
| `Sync.reopen`, `setStartedAt` | `engine.go` `Open` (empty binding); `sync_runs.go` `PutSyncRunRecord` |
| `Stream.run`, `grantRows` | `adapter_streaming.go` `StreamGrants` switch and per-row `ctx.Err()` check |
| `Digest.canonical`, `fold`, `chooseWidth`, `State.invalidate`, `repair` | `grant_digest.go` `grantContentHash64`; `digest.go` combiner and `chooseDigestWidth`; `rawdb` `stageGrantDigestInvalidation`; `grant_digest_repair.go` |
| `Views.bulkResources`, `bulkEntitlements` | `adapter_reader.go` `ListResourcesByIds`, `ListEntitlementsByIds` |
| `Container.sealArtifact`, `openArtifact`, `damage` | `pkg/dotc1z/pebble_store.go` `save`/`OpenStore` (`InitCurrentSync`); `format/v3/envelope.go`, `indexed.go`; `engine_registry.go` `selectStoreDriver` |

## Guarantees

Each row names the Lean statement, how the engine relates to it, and
the evidence. "Enforced" means the engine's code makes it so;
"required of producers" means the engine does not check it; "model
only" means the Lean statement has no Go counterpart yet.

### Identity and addressing (proposal §1)

| Statement | Status | Evidence |
|---|---|---|
| Resource identity is `(resource_type_id, resource_id)`; resource type is `external_id`; entitlement is owner plus raw external id; grant is entitlement identity plus principal `(type, id)` | Enforced | `Identity.lean` definitions; `keys` oracle family |
| Distinct identities of one kind never share a primary key | Proved, enforced | `ResourceId.key_injective`, `EntitlementId.key_injective`, `GrantId.key_injective`; `Codec.encodeKey_injective` |
| Keys of different kinds never collide | Proved | `key_kind_disjoint` |
| The entitlement strip rule loses nothing | Proved | `expandEnt_compressEnt`, `compressEnt_stripped_iff`; `entitlement_strip` oracle family |
| Equal raw ids under different owners coexist | Proved | `ResourceId.key_ne_of_rt_ne`, `EntitlementId.key_ne_of_rid_ne` |
| A by-value scan prefix matches exactly the keys whose leading components equal the scanned values (`"us"` does not match `"user"`) | Proved | `Codec.encodeScanPrefix_isPrefix_iff`; grants-of-entitlement corollaries `GrantId.key_under_entitlement_prefix`, `ent_eq_of_key_under_prefix` |
| Primary keys use no hashing | Enforced | research: only `idxGrantByEntitlementPrincipalHash` hashes, and its key still carries the full identity |
| Grant `external_id` is not part of the key: two grants that differ only there share one row | Proved (negative) | `GrantRecord.key_eq_iff`; `TestBulkImportMergesDuplicateIdentityGrants` |
| Distinct identities can print the same public id, so bare-id grant lookup can be ambiguous | Proved (negative) | `publicId_not_injective`; `GrantLookup.ambiguous_of_two_candidates` shows it through `GetGrant` |
| Bare-id lookup returns exactly one match or an explicit outcome (`ErrNotFound`, `ErrAmbiguousExternalID`) | Proved for the rule, enforced for entitlements and grants | `Result.resolveBare_found_imp_unique`; `lookup.go`; `bare_id` oracle family. Resources have no bare-id path. |
| Non-empty components | Enforced for entitlements and grants only | `identity.go`; `EntitlementId.WellFormed`, `GrantId.WellFormed`. Resources and resource types accept empty ids. |
| Grants of one entitlement are exactly the rows under its scan prefix, disjoint from every other entitlement's | Proved | `GrantStore.grantsForEntitlement_eq_filter`, `grantsForEntitlement_disjoint`; `grant_list` oracle family |
| The principal-type filter of `ListGrantsForEntitlement` selects exactly that type | Proved | `mem_grantsForEntitlementByPrincipalType` |
| A grant whose entitlement or principal has no record is stored and returned | Enforced; definitional in the model | `get_putGrants_independent_of_entitlements`; `grant_writes` oracle family. Not referential integrity. |
| Grant bare-id lookup (`GetGrant`) returns only a stored grant whose stored id is the query or whose stored id is empty and public id is the query | Proved | `GrantLookup.found_mem`, `found_matches`; `grant_bare_id` oracle family |
| Grant bare-id lookup is the exactly-one rule over stored ids | False in general | `GrantLookup.masking`: a public-id hit on one grant hides another grant whose stored id equals the query, with no ambiguity. Exactly-one holds only when no candidate hits (`resolve_eq_resolveBare_scan`). |
| An empty-id grant is addressable by the public id `ListGrants` shows for it | Conditional | `GrantLookup.found_of_unique_publicId`: yes, when its entitlement id is stripped-shaped and no other stored grant prints the same public id (the weaker `found_of_unique_candidate` is the exact condition). `opaque_unreachable`: not when the entitlement id is opaque and no entitlement row with that exact identity exists. `custom_ext_hides_public`: a custom stored id is never reachable by the public id. |

### Writes (proposal §2)

| Statement | Status | Evidence |
|---|---|---|
| A write replaces the whole value; last write wins; no field merge | Proved for the model, enforced on the normal put paths | `Store.get_put_self`, `put_put_last`; `records.go` `Set`; `writes` oracle family |
| Writing one key leaves other keys unchanged | Proved | `Store.get_put_of_ne` |
| Within one call, the last occurrence of a key wins, and pre-deduplicating does not change the result | Proved | `Store.get_putBatch`, `putBatch_dedupLast` |
| Enumeration never emits a key twice; enumeration and lookup agree on existence | Proved | `Store.keys_nodup`, `mem_keys_iff` |
| One put call or page commit is atomic | Enforced | `page_unit.go`; not modeled beyond `putBatch` being a pure function |
| `discovered_at` survives an overwrite | Not on the normal put path | Only `PutExpandedGrantRecords` keeps it. Out of model scope. |
| Batch deletes are all-or-nothing | Not enforced | `DeleteGrantsByIdentityRefs` commits in chunks of 1000. Out of model scope. |
| Bulk import and the id-index migration merge duplicate grant values field-wise | Enforced, out of model scope | `mergeDuplicateGrantValues` |
| Two grants differing only in `external_id`: one row, the later id; the same id on two structures: two rows | Proved, enforced | `GrantStore.getGrant_putGrants_collapse`, `getGrant_putGrants_distinct`; `grant_writes` oracle family |
| The same entitlement id on two resources: two rows | Proved, enforced | `EntitlementStore.getEntitlement_putEntitlements_distinct_rid`; `entitlement_writes` oracle family |
| Deleting an entitlement cascades to its grants | False | `deleteEntitlement_no_cascade` (definitional); the engine has no cascade for any kind |

### Enumeration and pagination (proposal §3)

| Statement | Status | Evidence |
|---|---|---|
| A complete traversal returns every visible key once, in key order, for any positive page size, even when the size changes between pages | Proved | `Paginate.traverse_complete`, `traverse_flatten_eq_of_pos`, `traverseWith_complete`; `pagination` oracle family |
| A continuation token preserves position: resuming at any minted token yields exactly the visible keys after it | Proved | `traverse_resume` |
| Every emitted key lies in the scan range, after the cursor | Proved | `page_items_mem` |
| A token is minted only on a full, non-empty page; the token is the last key | Proved | `page_next_isSome_imp_full`, `page_next_isSome_imp_nonempty`, `page_next_eq_getLast` |
| A page without a token has emitted every remaining visible key | Proved | `page_next_none_imp_exhausted` |
| End of results is the empty token alone; the final page may be non-empty | Proved, enforced | same; `TestPaginationClampedPageSize` |
| A full page may be followed by an empty terminal page when trailing raw rows are not emitted | Proved (negative) | `Paginate.lean` witness with `visible` |
| Page size 0 and oversize requests become 10000; the effective size is in `1..10000` | Proved, enforced at the adapter | `clampPageSize_pos`, `clampPageSize_le`; engine-level `Paginate*` do not clamp above 10000 |
| A token from another keyspace is rejected | Proved, enforced | `checkCursor_invalid_of_not_prefix`; `TestCrossKeyspaceCursorRejected` |
| Tokens are bound to the sync, filter, or page size | Not enforced | A token is base64 of the raw key. A narrower filter's token is accepted by a broader scan with the same prefix; a forged in-prefix key is accepted. |
| Pages are read from one snapshot | Not enforced | Each page opens a fresh iterator; the theorems assume a static keyspace. |
| `ListGrantsForEntitlements` batched token | Out of scope | Its checksum mismatch silently restarts from the first entitlement. |

### Sync selection (proposal §4)

| Statement | Status | Evidence |
|---|---|---|
| A v3 file holds exactly one sync; no data key carries a sync id | Enforced | `keys.go`; `TestEncodersOmitSyncID`; `Sync.lean` models one record |
| Records from another sync cannot leak into a read | Enforced structurally | one sync per file; `StartNewSync` wipes the keyspace (`hasRecords_startNewSync`) |
| Record writes need a bound, unsealed engine | Proved, enforced | `writeGate_opened`, `writeGate_endSync`; `withWrite`, `requireCurrentSync`; `sync` oracle family |
| `StartNewSync` is refused while a sync started by `StartNewSync` is still open; after `EndSync` it is accepted and wipes the file | Proved, enforced | `startNewSync_refused_of_fresh`, `startNewSync_startNewSync`, `startNewSync_endSync`; `ResetForNewSync` guard on `IsFreshSync`. The live property test found the model missing this refusal on its first run. |
| A sync reopened by `ResumeSync` is protected from `StartNewSync` | False | `startNewSync_resumeSync`: the resumed binding is not fresh, so a following `StartNewSync` wipes it without refusal. |
| `EndSync` marks the record finished, seals, and unbinds; a write afterwards is refused as "no current sync" because the bound check runs before the sealed check | Proved, enforced | `finished_endSync`, `writeGate_endSync`, `writeGate_engineSealed_iff`; the first oracle run caught the model stating `engineSealed` here, and the engine's order won |
| A finished sync is immutable | False | `writeGate_resumeSync_finished`, `endSync_resumeSync_endSync`: `ResumeSync` reopens it and a later `EndSync` overwrites `ended_at`. |
| After `StartNewSync`, nothing is finished: replacement hides the previous record rather than retaining it | Proved | `latestFinished_startNewSync` |
| Latest-finished selection returns only a finished record of the requested type | Proved | `latestFinished_spec`, `latestFinished_type`, `latestFinished_none_of_unfinished` |
| Default sync resolution never invents an id; stale unfinished runs do not resolve | Proved | `resolveActiveSync_source`, `resolveActiveSync_none_of_stale` |
| The requested sync id is checked against the file | Not enforced | `resolveActiveSync_annotation`: the resolved id is a non-empty gate only. Reads with a mismatched id return the file's records. |
| Coverage metadata (which kinds or scopes were fully enumerated) | Unsupported | The stats sidecar holds counts only. Absence claims need caller-supplied authority. |

### Close and reopen (proposal §9)

| Statement | Status | Evidence |
|---|---|---|
| A clean reopen changes no rows and rebuilds no index; the binding resets to unbound, not fresh, unsealed | Enforced; modeled | `Sync.reopen`, `run_reopen`, `hasRecords_reopen`; `Open` writes stamps only on an empty DB, the id-index layout is current, the migration registry is empty; `reopen` oracle family |
| After reopen, writes are refused until a rebind; `ResumeSync` rebinds and allows writes | Proved, enforced | `writeGate_reopen`, `writeGate_resumeSync_reopen` |
| A reopened finished sync is the default sync for reads | Proved, enforced | `resolveActiveSync_reopen_finished` |
| A reopened unfinished sync is readable only while started within 7 days; past that, reads report no current sync | Proved, enforced | `resolveActiveSync_reopen_unfinished`; replayed by rewriting `started_at` through `PutSyncRunRecord` |
| An unfinished sync is protected from `StartNewSync` after reopen | False | `startNewSync_reopen`: the reopened binding is not fresh, so it is accepted and wipes the file. This is what `StartOrResumeSync` does once a record ages past the cutoff. |
| The in-process `sealed` bit survives reopen | False (engine-level only) | after `EndSync` in process the engine is sealed; after reopen it is not. Adapter writes report no-current-sync either way; engine-level record writes differ. |
| The read view of a finished sync is stable across reopen for a file written by this version | Enforced | the id-index migration and digest drops fire only on legacy or crashed files; out of model scope |

### Streams (proposal §5, §7)

| Statement | Status | Evidence |
|---|---|---|
| A patient consumer receives exactly the filtered collection, in the order the paginated reader returns | Proved, enforced | `Stream.run_eq_filter`; `stream` oracle family |
| At most one error is yielded and it is last; records received are a prefix of the collection | Proved, enforced | `run_error_terminal`, `run_at_most_one_error`, `records_run_prefix` |
| A cancelled context over an empty keyspace yields nothing, not even an error | Proved (negative), enforced | `run_cancelled_empty`; the check runs per scanned row |
| A cancelled context over a non-empty keyspace yields the error even when no row matches the filter | Proved, enforced | `run_cancelled_nonempty`; post-filters run after the check |
| Early stop yields a prefix and no signal; stopping at the last match is indistinguishable from exhaustion | Proved (negative) | `run_break`, `run_break_eq_patient_at_end` |
| Cancelling after `k` records yields exactly the first `k` matches | Proved | `records_run_cancel` |
| The type-only grant stream walks `by_principal` and shares its deferred-index gap | Enforced; modeled | `grantRows (.principalType _)` uses `grantsForPrincipalType` |
| A requested non-empty sync id scopes the stream | Not enforced | the argument only skips resolution |

### Digests (proposal §8)

| Statement | Status | Evidence |
|---|---|---|
| The hash sees the identity tuple, the `GrantImmutable` flag, and source keys with `is_direct`; not `external_id`, `discovered_at`, `expansion`, `needs_expansion`, `source_scope_key`, other annotations, or source reference fields | Enforced; modeled | `Digest.canonical`, `canonical_ignores_excluded`; `digest` oracle family's accumulator check |
| Roots are a count and an XOR fold; splitting and reordering do not change them | Proved | `fold_append`, `fold_perm` |
| A partition root's count is the number of grants under the entitlement; the global root is the combination of partition roots | Proved | `partitionRoot_count`, `globalRoot_eq_combine` |
| Equal canonical content gives equal roots; grants differing only in `external_id` give equal roots | Proved | `fold_eq_of_content_eq`, `partitionRoot_putGrants_externalId`. Content includes the entitlement identity, so two non-empty partitions in one file never have equal content; the equality that matters is the same entitlement across files or across rewrites, which the accumulator check in the `digest` family pins. |
| Equal roots imply equal content | False, even with no hash collision among the inputs | `fold_not_injective`: XOR is not injective on sets. Treating equal roots as equal content is a collision assumption; the Go test labels its distinct-content check as exactly that. |
| Leaf width depends only on the count and is at most 16 | Proved | `chooseWidth_le`, `chooseWidth_spec` |
| A grant write, or a delete of a stored grant, after sealing drops its entitlement's partition and the global root; other partitions are untouched; the dropped ones read absent, not stale | Proved, enforced | `State.afterPut`, `State.afterDelete`, `lookup_invalidate_self`, `lookup_invalidate_of_ne`, `global_invalidate` |
| A delete of an absent grant invalidates its partition | False | `afterDelete_absent`: the delete stages nothing, so the roots stay. The live property test caught the model invalidating unconditionally; the engine's behavior won. |
| A later `EndSync` rebuilds the missing partitions to the fresh values; a second one changes nothing | Proved | `repair_eq_build`, `accurate_invalidate_putGrants`, `repair_idempotent`, `invalidate_idempotent` |
| Absent, never built, option off, and invalidated are distinguishable | Not enforced | all read as `found = false` with no error. A built empty partition is `found = true, count 0`. `ComputeEntitlementBucketDigest` on an invalidated partition returns zeros that look like "no grants". |
| Roots across ABI versions are comparable | Not enforced | the stamp exists so a mismatch drops or marks the state; the model is ABI v2 only |

### Reader agreement (proposal §5)

| Statement | Status | Evidence |
|---|---|---|
| Point lookup, full listing, entitlement scan, the patient streams over each, the principal index walk, and the type index walk agree on existence for a store built by the engine's own writes with a complete index | Proved | `Views.views_agree`; `views` oracle family |
| Without a complete index the index walks disagree with every other view | Proved (negative) | `views_disagree_without_complete_index` |
| Bulk reads return each record paired with its own id, in request order, repeats kept, nothing invented | Proved, enforced | `bulkResources_assoc`, `bulkResources_sublist`, `mem_bulkResources_iff`; `views` family |
| Bulk reads mark missing ids | False | `bulkResources_absence_unmarked`: a missing id is skipped silently; the caller must diff request against response |
| A bulk entitlement read with an ambiguous bare id fails as a whole; a successful one returns only unique matches | Proved, enforced | `bulkEntitlements_none_of_ambiguous`, `bulkEntitlements_some_unique` |
| Lookup by bare id is a view of the store | False | it is a resolution rule; see `GrantLookup` and the entitlement exactly-one rule |
| Field agreement beyond identity and `external_id` | Tested, not proved | the Go test compares display fields where both views return them; the v2 projection drops `discovered_at`, `needs_expansion`, `source_scope_key`, and source reference fields |

### Container (proposal §9)

| Statement | Status | Evidence |
|---|---|---|
| A sealed `.c1z` reopens to the state that was sealed; save is flush, checkpoint, envelope, with every normalization done by `EndSync` | Proved for the model; enforced | `Container.open_seal`; `container` oracle family through `NewStore`/`Close` |
| The public open binds the default sync: a finished file reopened writable accepts writes without `ResumeSync`; a read-only open never allows one and differs from a writable one only in the gate | Proved, enforced | `writeGate_publicOpen_finished`, `writeGate_publicOpen_readOnly`, `writeGate_publicOpen_readOnly_ne_allowed`, `openArtifact_readOnly_state`; `InitCurrentSync` |
| A read-only store reports "read only" on every refused write | False | `writeGate_readOnly_unbound`: the adapter checks for a bound sync first, so an unbound read-only store reports "no current sync". The live property test caught the model reporting read-only. |
| An unfinished file reopened past the 7-day cutoff binds nothing | Proved, enforced | `publicOpen_unfinished_stale` |
| A truncated header, bad magic, unknown engine, flipped payload byte, or truncated tail fails the open; none opens with fewer records | Proved for the model; enforced for the default indexed encoding | `open_damage_error`, `open_damage_class`; the indexed envelope hashes the manifest and every frame |
| The `TAR` and `TAR_ZSTD` encodings detect every truncation | Unverified | plain tar has no manifest hash, and a cut between entries is not an archive error; whether the engine then opens is not established. Out of model scope. |
| The manifest's sync runs, stats, and digest root agree with the keyspace | Not enforced | they are advisory projections never read back; the keyspace is authoritative |
| `engine_schema_version` is validated at open | Not enforced | written, never checked; version gating is the keyspace stamp |
| The in-process binding survives the round trip | No | binding and sealed state are in memory; the public open rebinds through resolution |

### Errors and exhaustion (proposal §7)

| Statement | Status | Evidence |
|---|---|---|
| A failed or abandoned traversal is not complete | Proved (definitional) | `Result.complete_iff`, `not_complete_failed`, `not_complete_abandoned` |
| A list error returns no partial page | Enforced | `paginate.go` returns `nil, ""` |
| Streams yield at most one error and nothing after it | Enforced; model states the consumer's verdict | `ErrorTerminal`, `streamEnd_failed_of_error`; `adapter_streaming.go` |
| Primary-row decode failures surface as errors | Enforced | `paginate.go` "page unmarshal" |
| Index-backed reads distinguish a missing or deferred index from an empty result | Not enforced; proved as a negative | `IndexedGrants.grantsForPrincipal_deferred_incomplete`, `not_complete_putGrantsDeferred_of_new`: a `PutExpandedGrantRecords` write of a new identity is invisible to `ListGrantsForPrincipal` until `EndSync`, with success status. `grants_by_principal` oracle family. Dangling index entries are skipped silently. |
| The index view is a subset of the primary view, and equals it once the index is complete; `EndSync` makes it complete; plain writes and deletes keep it complete; a second `EndSync` without writes changes nothing | Proved | `grantsForPrincipal_subset`, `grantsForPrincipal_eq_of_complete`, `complete_endSyncRebuild`, `complete_putGrants`, `complete_deleteGrant`, `endSyncRebuild_idempotent`, `keyed_putGrantsDeferred` |
| An unknown bare entitlement id in a filtered grant list is an error | Not enforced | It is an empty success. |
| Context cancellation surfaces on an empty range | Not enforced | The primary-scan streams check `ctx` only per record. |

## Non-guarantees worth repeating

These came out of the implementation survey and are the facts a
downstream consumer most needs. None is a theorem; each is a boundary.

- The read view of a finished sync is not stable across `Open`: the
  id-index migration can re-key entitlement and grant rows and drops
  rows that lack reference fields.
- `StartOrResumeSync` with an unknown explicit id starts a new sync and
  wipes the file (`TestStartOrResumeSyncUnknownIDWipesRecords`).
- Grants whose entitlement or principal is absent from the snapshot
  are accepted and returned; nothing enforces referential integrity.
- Equal digests mean equal content only under a collision assumption;
  digests are outside this package.
- `EndSync` with no bound sync returns a plain error, not the
  `ErrNoCurrentSync` sentinel; callers cannot `errors.Is` it.
- Sync ids must be KSUIDs at the adapter boundary.

## Oracle and conformance

The oracle runs the model to produce `generated/cases.json` (schema in
`ORACLE_SCHEMA.md`, versioned, with per-family counts). The Go test
asserts the version and counts, rejects unknown fields and enum
strings, and replays each family against a fresh engine. Families:
`keys` (byte-exact key encoding), `entitlement_strip`, `writes`,
`pagination`, `bare_id`, `sync`, and from the second increment
`grant_writes`, `entitlement_writes`, `grant_list`,
`grants_by_principal`, `grant_bare_id`, and from the third increment
`stream`, `digest`, `reopen`, and from the fourth increment `views`
and `container`. The `container` family is replayed by
`TestFormalContainer` in `pkg/dotc1z`, since it needs the public store
API that imports the engine package.

The fixed generator deliberately does not produce: rows hidden by
`visible` (dangling index entries), injected faults, `discovered_at`
behavior, grant-record writes, or the 7-day unfinished fallback (Lean
witnesses only). Those remain proved-but-not-differentially-tested, or
out of scope, as marked above. The random and property paths widen the
input distribution within the same six families; they do not add
families.

## Trust

- The toolchain is pinned in `lean-toolchain`; the core imports only
  the Lean prelude and `Init`.
- Four model errors were caught by replay rather than by review: the
  write-gate order after `EndSync` (fixed corpus), the missing
  `StartNewSync` refusal (live property test), digest invalidation on a
  delete of an absent grant (live property test), and the read-only
  gate order on the public open path (live property test). All are now
  theorems. Two of the four were the same mistake, a check order the
  model guessed, which is worth remembering when modeling any new gate.
  Treat that as the expected failure mode of Joint 1: the model is a
  transcription, and the oracle is what checks it.
- `scripts/check.sh` fails on any compiler warning (so on any `sorry`),
  diffs `#print axioms` for every exported theorem against
  `AXIOMS.golden`, and rejects any axiom other than `propext`,
  `Classical.choice`, `Quot.sound`. No `native_decide`, `partial`, or
  custom axiom is used.
- `generated/cases.json` is checked in, against the usual advice,
  because the Go CI has no Lean toolchain. `check.sh` fails if the
  checked-in file differs from a fresh run, so the two cannot drift
  silently once that gate runs.
- Assurance level: kernel acceptance plus axiom audit plus human review
  of statements. `lean4checker` and external checkers are not run; for
  a first-party model that is the intended stopping point.

## Review checklist for statement changes

Statements carry the meaning; proofs only certify them. When changing
a theorem: does the name say what the statement says; is any conjunct
a restated hypothesis; is there a witness that the hypotheses are
satisfiable; is it true of the type alone (then it is not a domain
property); and does the oracle generate the part of the domain it
quantifies over.
