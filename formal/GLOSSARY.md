# Glossary — sync scheduling formal model (deliverable 0)

Pinned vocabulary for the model, its documents, and its P identifiers.
Where a term names something that exists in code today, the anchor is
cited; graph-runtime terms describe CANDIDATE semantics under evaluation
(see `docs/tasks/demand-graph-sync-brief.md`) and are definitions of what
the model checks, not descriptions of shipped behavior.

## Shared vocabulary

- **Sync**: the logical unit of work that produces one artifact. Identified
  by a sync ID; spans one or more attempts.
- **Attempt**: one process execution of a sync. A crash or interrupt ends
  an attempt; a resume starts a new attempt of the same sync from its last
  durable checkpoint. The **attempt boundary** is the crash/resume seam.
- **Artifact**: the durable output of a sync (a c1z): row partitions, a
  manifest, and — once sealed — a completion verdict. The **previous
  artifact** is an immutable, read-only replay base consulted only via the
  lookup.
- **Row**: one stored record (resource, entitlement, or grant — the model
  abstracts these to one row kind axis). Row identity is its storage
  identity within a row kind.
- **Scope / output key**: the stable storage identity a partition is keyed
  by. In the walker this is the connector-chosen `(row_kind, scope_key)`
  (`annotation_source_cache.proto`); in the graph runtime it is the node's
  output key. Deliberately distinct from the derivation hash (scheduling
  identity): re-derived work must land in the same partition.
- **Partition**: the set of current-sync rows stamped with one scope.
  **Partition invariant**: scopes partition rows — each row identity
  belongs to exactly one scope per sync; a cross-scope restamp or
  out-of-scope delete is a partition violation and poisons the losing
  scope (proto contract).
- **Validator**: the opaque upstream change-detection token persisted per
  scope (HTTP ETag, delta token). **Truthful-validator assumption** (trust
  boundary): a validator matches iff the scope's upstream content is
  unchanged.
- **Manifest**: the artifact's map of scope → manifest entry (validator,
  plus a sealed row count). An entry is published only after its page's
  rows and tombstones commit.
- **Attestation**: the claim a manifest entry makes: this artifact's
  partition for scope S equals replay-of-the-attested-base plus the
  declared compositions (overlay upserts, tombstones, or replacement),
  produced under validator V. P1 (binding integrity) is the integrity of
  this claim.
- **Consult**: resolving a scope's freshness this sync: a lookup against
  the previous artifact's manifest, then — on a hit — connector
  revalidation of the validator against upstream. A scope was "consulted
  against upstream during sync N" if its verdict came from revalidation or
  fresh fetch within sync N.
- **Hit**: a warm-lookup consult that found a manifest entry. Recorded
  with its validator in the checkpoint-durable hit map
  (`state.sourceCacheHits`, CO-6b-004).
- **Record**: fresh-fetched rows stamped into a scope's partition, with a
  validator publish when the page carries one (`SourceCacheRecord`).
- **Replay**: copying the previous artifact's partition for a scope into
  the current sync (`SourceCacheReplay`).
- **Elision**: what replay skips — the enumeration the connector would
  have performed for the scope. Replay copies rows, not side effects:
  session writes (and reads) the elided enumeration would have made do
  not occur this sync. Elision is sound only if the enumeration is
  side-effect-free beyond its rows (model spec scenario 7).
- **Replacement**: the replay copy's semantics — the destination partition
  is cleared, then the base is copied
  (`clearReplayDestinationScopeLocked`). Contrast **overlay**: upserts and
  tombstones applied on top of a replayed base (delta semantics).
- **Composition enum (proposed)**: a `SourceCacheRecord` marker declaring
  OVERLAY vs REPLACES semantics for a page's contribution; REPLACES blocks
  the artifact as a future replay seed until supersede machinery exists
  (deliverable 3 models this staged mitigation).
- **Poison**: a durable per-scope marker recording a partition violation.
  A poisoned scope reads as a lookup miss and is refused as a replay
  source (preflight).
- **Preflight**: source-side validation before a replay copy: manifest
  entry present and not invalidated, scope not poisoned, sealed row count
  equals the index cardinality with stamp verification
  (`validateReplaySourceScope`).
- **Checkpoint**: the durable snapshot of scheduler state — action stack,
  provenance sets, flags — written between dispatch batches
  (`state.Marshal` → sync token), and FORCE-written at Init, before seal,
  and on graceful stop (`checkpointOnStop`, run-expiry). A stop-forced
  checkpoint captures live mid-batch state: mid-chain cursors of
  unfinished actions, admitted-but-undrained spawns, and hits recorded
  during the aborted batch. Resume restores exactly the checkpoint and
  nothing else; all other scheduler state is volatile.
- **Seal**: the transition that completes a sync's artifact (EndSync).
  Sealed row counts are stamped before the end stamp; after seal the
  artifact is immutable. **Seal obligations** are the checks that must
  hold at this point.
- **Warm / cold**: warm = a previous-artifact lookup is installed (every
  consume gate G1–G7 passed, this attempt); cold = `NoopLookup`, every
  consult misses. Degradation to cold is always safe; it costs caching
  value only.
- **Compat key**: the four-field byte-matched record gating warm installs
  (connector cache generation, config fingerprint, SDK materialization
  generation, selection fingerprint — plan B4). Two artifacts from the
  same connector and config share a compat key; this is why validator
  binding (CO-6b-004) exists.
- **Smear**: the baseline time-inconsistency of any paginated walk:
  different scopes observe upstream at different instants, so every
  artifact is a mixture of upstream states that existed during the sync.
  Smear is not a defect; P3 bounds which mixtures are acceptable.

## Walker-specific

- **Action**: the checkpointed unit of scheduled work: op, page token,
  resource keying, spawn/type-scope markers (`state.Action`). The stack is
  LIFO by admission order; per-resource phases dispatch batches of up to
  100 consecutive same-op actions to bounded workers.
- **Spawn vs continuation**: `EnqueuePageTokens` ADMITS new sibling cursor
  actions — each an independently checkpointed, schedulable identity,
  deduplicated per process by identity digest. `NextPageToken` advances
  the SAME action's cursor in place. Spawn creates schedulable identity;
  continuation does not.
- **Restart-from-root (CO-6b-002)**: resume restores actions as of the
  MOST RECENT durable checkpoint. In crash-only histories that is a
  loop-top/forced checkpoint holding only root tokens, so an
  interrupted paginated action re-executes from its root token; under
  a graceful stop the forced stop-checkpoint may capture a mid-chain
  cursor, and resume continues from it — including when that
  checkpoint survives a LATER hard crash. Restart-from-root is a
  property of which checkpoint survives, not of the crash itself. Page
  processing is at-least-once across an attempt boundary in all modes.
- **Hit-set / replayed-set**: the checkpoint-durable, within-sync-monotone
  provenance maps. The hit-set (row kind → scope → validator) authorizes
  same-sync replays; the replayed-set dedups the once-per-sync replacement
  copy. Hits recorded in the dispatch batch that crashes are lost with it
  (CO-6b-006).
- **Warm gate (attempt-scoped)**: the `sourceCacheWarm` flag — set only
  after THIS attempt installed a deliverable warm lookup; consulted before
  every replay copy. A restored hit-set does not re-authorize replay in a
  cold or drifted resume (CO-6b-003).
- **Scope lock**: the per-(row kind, scope) mutex every scoped page holds
  from before its row puts through its validator publish (CO-6b-005/006);
  closes the duplicate-copy TOCTOU and the record-page/replacement-copy
  interleaving.

## Graph-runtime (candidate semantics)

- **Node**: a re-issuable request description (call site + parameters).
  Persistent scheduling identity; may be executed any number of times.
- **Execution**: one attempt of one node. Identified by (node,
  generation).
- **Generation**: a node's restart counter. An output produced by
  execution (n, g) is **dead** once n restarts into generation g' > g.
- **Derivation hash**: the canonical hash of a node's request derivation;
  the scheduling identity used for revisit suppression. Distinct from the
  output key by design.
- **Spawn lineage**: the single-parent tree of which execution admitted
  which node; used for eager purge of a restarted node's spawn-subtree
  (variant E).
- **Support / data lineage**: refcounted demand edges recording which
  outputs support which rows and nodes; DERIVED, not stored; retraction is
  transitive when support drops to zero (variant E).
- **Demand closure**: the set of nodes and outputs transitively demanded
  from the sync's roots as of seal time.
- **Sweep**: the seal-time pass that drops every partition outside the
  final demand closure and nothing inside it (P5).
- **Fresh-artifact supersession**: a newer execution's outputs replace an
  older generation's outputs for the same output key, rather than
  composing with them.
- **Causal stamp (variant S)**: a compact causal timestamp over node
  generations carried by every output (rows, session values, spawned
  tokens). Reads merge the value's stamp into the reader's; writes carry
  the writer's merged stamp; validation happens at observation points
  (demand derivation, session reads, seal). Lossy compression is
  admissible because the error direction is false staleness → redone
  work, never wrong data.
- **Consistent cut (variant S)**: the variant-S form of P1/P3: no
  published partition mixes outputs from causally incomparable
  generations, checkable mechanically from stamps at seal.
- **Sealed cut (P7)**: the checkpointable fact "no pending or reachable
  node can emit row-kind K" — the closure precondition grant expansion's
  aggregate node requires before condensing.

## Session store

- **Variant A (shipped)**: free-form KV, sync-scoped, durable across
  attempt boundaries (`pkg/types/sessions`,
  `pkg/dotc1z/engine/pebble/session_store.go`). Any execution may read any
  key; no lineage is recorded — the dependency channel in calibration
  case 2.
- **Variant B (proposed primitives)**: private scratch (readable only by
  its own execution, dies with the execution's generation); single-writer
  publish (reads are tracked edges; the writer's re-derivation retracts
  readers); keyed-contribution merge (per-writer retraction).
- **Session taint (proposed)**: produce-side marking that a kind's
  enumeration touched the session store during a replay-capable phase
  (replay-capable = the kind is in the declared source-cache flow,
  regardless of the recording attempt's warm/cold state), recorded in
  the artifact's produce state (checkpoint-cadence durability,
  self-healing under at-least-once re-execution); a tainted kind's
  scopes read as lookup misses in later syncs — degradation, never a
  loud verdict. Write-only taint closes the elision hole (scenario 7a)
  but not the stale-read dual (7b); full-traffic taint (session
  isolation) closes both. A capability-level opt-out — the connector
  attests emission-irrelevance of its session traffic — disables the
  detector for those kinds; trust-boundary machinery, and a dishonest
  opt-out reproduces 7a/7b exactly.

## The three-invariant framing

- **Demand-gating**: nothing outside the final demand closure is
  published (consume/sweep side; P5).
- **Validator-gating**: every published row entered via a fetch or a
  replay whose validator was consulted against upstream during this sync
  (P2).
- **Binding integrity**: every manifest entry attests exactly its
  partition's declared composition over the attested base (P1,
  produce-time).
