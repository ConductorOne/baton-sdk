# Roadmap: what the model should handle next

Ordered by expected yield: how likely each increment is to surface a
real model-engine disagreement, weighted by how much a downstream
consumer (C1's uplift and reconciliation) depends on the answer. Each
increment is the same shape of work as the first: a model extension
with theorems, an oracle family (fixed, random, and `--respond`), and a
Go replay function. The proposal sections referenced are in the C1
document "Baton SDK proof contract for c1z snapshots and readers".

Status: increments 1, 2, 3, 5, and 7 are done (see README's guarantee
tables); 4 is deferred; 8 and 9 are done; 6 remains, plus the
standing items. The first six
families cover resource writes, resource pagination, entitlement
bare-id lookup, key encoding, and the sync lifecycle. 1.5M replayed
cases found no engine disagreement there; the two mismatches were model
errors. The engine paths below are where the implementation survey
found behavior that a reader could misinterpret.

## 1. Grant and entitlement records (proposal §1, §2, §6) — done

The highest-value gap. Every reconciliation input C1 cares about is a
grant, and the grant family has the sharpest identity rule: the key is
`(entitlement identity, principal type, principal id)` and the producer's
`external_id` is stored only in the value.

Model:
- `Store` instances keyed by `EntitlementId.key` and `GrantId.key`, with
  `GrantRecord` as the value so `external_id` collapse is observable.
- Theorems already present (`GrantRecord.key_eq_iff`,
  `GrantId.key_under_entitlement_prefix`) become replayable.
- Reference semantics as predicates, not constraints: a grant whose
  entitlement or principal is absent is stored and returned
  (`ForEachDanglingGrantEntitlement` finds them; nothing rejects them).
  State this as the documented non-guarantee and generate it.

Oracle families: `grant_writes` (puts and `DeleteGrantByIdentityRefs`,
two grants differing only in `external_id`, same `external_id` on two
structures, dangling references), `entitlement_writes` (same
`external_id` on two resources, `DeleteEntitlementRecordByIdentity`),
and `grant_list` (`ListGrantsForEntitlement` as a prefix scan, with the
principal-type filter inside the scan).

Go replay: `PutGrants`, `PutEntitlements`, `DeleteGrantByIdentityRefs`,
`ListGrants`, `ListGrantsForEntitlement`.

Expected findings: the grant bare-id lookup (`GetGrant` by external id)
has a candidate-split search capped at 64 colons and 4096 candidates and
a full-scan fallback. That path is where ambiguity classification could
diverge from `Result.resolveBare`.

## 2. Hidden rows and index-backed readers (proposal §5, §7) — done

`Paginate.page` already models `visible`, and `page_next_none_imp_
exhausted` and the trailing-empty-page witness are proved, but the
generator never emits a hidden row. The engine produces them in three
ways: index entries whose primary row is missing, undecodable index
keys, and `keep`-rejected rows.

Model: no change for the page law. Add a `DeferredIndex` flag to the
sync state: `by_principal` can be deferred until `EndSync`, and no reader
checks `DeferredIdxPending`, so `ListGrantsForPrincipal` before `EndSync`
returns a result that looks complete and empty. State as a theorem about
the model that the index-backed view equals the primary view only when
the index is built; state the non-guarantee that the engine does not
signal the other case.

Oracle family: `grants_by_principal` with the read placed before and
after `EndSync`.

Go replay: `ListGrantsForPrincipal`, `ListGrantsForResourceType`,
`StreamGrants` with `PrincipalResourceType`, and an engine-level write
that leaves a dangling index entry (the test seams in `test_seams.go`
may already allow this; otherwise the hidden-row cases stay model only).

Expected findings: the first case where the engine returns incomplete
results with a successful status. That is the most important
non-guarantee for C1 to see demonstrated rather than described.

## 3. Close and reopen (proposal §9) — done

Every case today writes and reads in one open engine. The read view of
a finished sync is not stable across `Open`: the id-index migration can
re-key entitlement and grant rows and drops rows that lack reference
fields, interrupted digest builds are dropped, and the engine binding
resets to unbound.

Model: a `reopen` transition on `FileState` (binding cleared, records
kept, sealed cleared) and a `Store` law that the logical view survives
reopen for well-formed rows. The migration's row drop is a documented
non-guarantee, not a theorem.

Oracle family: `reopen` cases that write, `EndSync`, reopen, then
replay the point and list reads of increment 1.

Go replay: `Close` then `Open` on the same directory. This increment
requires the on-disk path (`C1Z_FORMAL_DISK=1`) or a shared `vfs.MemFS`
across the two opens.

Expected findings: low for well-formed rows; the value is pinning the
binding reset and the sealed-engine behavior after reopen, which the
reader's default-sync resolution depends on.

## 4. Page tokens as adversarial input (proposal §3, §7) — deferred

Deferred by decision: the token writers are this repository's own code, so forged tokens are not a consumer risk worth the increment. The reuse cases (a narrower filter's token on a broader scan; the batched `ListGrantsForEntitlements` checksum restart) remain documented non-guarantees.


`checkCursor` is proved but never replayed. The engine accepts any
in-prefix key, including forged ones, and rejects out-of-prefix keys
with `ErrInvalidPageToken`. A token from a narrower filter is accepted by
a broader scan with the same prefix; the batched `ListGrantsForEntitlements`
token silently restarts from the first entitlement on checksum mismatch.

Model: `Paginate.page` with a forged cursor (a key not in `ks`) already
has defined semantics (resume strictly after it). Add a theorem that a
forged in-prefix cursor never causes a key to be emitted twice or
skipped relative to the sorted keyspace, which is the property a
consumer needs when a token leaks between calls.

Oracle family: `tokens` with forged in-prefix keys, out-of-prefix keys,
malformed base64, a narrower filter's token reused on a broader scan,
and a token from a different file with the same keyspace.

Go replay: `ListResources` with constructed `PageToken` values.

Expected findings: the schema has to classify "accepted and resumed
after a key that never existed" as a result, which forces the
documentation to say it plainly.

## 5. Streams and cancellation (proposal §7) — done

`Result.ErrorTerminal` and `streamEnd` are proved for the consumer's
side; the engine side is pinned only by existing unit tests. Context
cancellation is checked per record, so a cancelled context over an
empty range yields no error.

Oracle family: `stream` cases comparing `StreamGrants` to the paginated
collection, plus early `break`, plus a pre-cancelled context over empty
and non-empty ranges.

Go replay: `StreamGrants`, `StreamResources`, `StreamEntitlements`.

Expected findings: the empty-range cancellation gap is a known
non-guarantee; replaying it makes the README row evidence instead of a
claim.

## 6. Wider distributions in the existing families

Cheap, and worth doing alongside any of the above:

- Page sizes at and above `maxPageSize` (10000) and corpora longer than
  one page, so the clamp's second branch and multi-page-at-max are
  replayed. The engine-level `Paginate*` functions do not clamp above
  10000; only the adapter does.
- Identifiers of hundreds of bytes and ids that are prefixes of each
  other at many lengths, to stress `scanPrefix` boundaries.
- Longer sync op sequences and larger id pools in `sync`, so
  `latest_finished` returns an id more than 3% of the time.
- `SyncType.unspecified` through the adapter's unknown-string mapping.

## 7. Digests (proposal §8) — done

Treat as optimization evidence with an explicit collision assumption.
Model canonicalization (which fields affect the digest), bucket
partitioning without overlap, and the invalidation side effect that a
grant write removes its whole entitlement partition plus the global
root. Out of scope until increments 1 and 2 are in, because digests
are over grants.

## 8. Container round trip (proposal §9) — done

Seal a store into a `.c1z` through the public writer, reopen it through
the public reader, and compare every view to the model. Add the
failure classes at open (truncation, bad magic or version, corrupt
manifest, corrupt archive member) with the expectation that each is an
error and never a shorter successful view.

## 9. Reader agreement as one theorem — done

`C1z.Views.views_agree`: point lookup, full listing, entitlement scan,
patient streams, and the index walk (once complete) agree on existence
for a store built by the engine's own writes. Bulk-by-id reads are
added with their characterized semantics. The known exceptions are
listed beside the theorem.

## Standing items

- CI: a job that installs elan and runs `make formal-c1z-check`, so the
  freshness gate runs on every PR and `generated/cases.json` can stop
  being checked in.
- Run `scripts/soak.sh` after any change under
  `pkg/dotc1z/engine/pebble/`; four minutes buys 1.5M cases.
- Every new theorem goes into `scripts/Axioms.lean`; every new family
  gets a count in `counts` and a strict decoder in the Go test.
- Keep the README's guarantee table current: each row names its
  theorem, its enforcing code, and its replay family, or says which of
  the three it lacks.
