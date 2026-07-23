# Bug-catching guidance for baton-sdk

Guidance for finding bugs in this repo — written for AI agents and humans alike.
Derived from a multi-agent review of the incremental grant-expansion branch (4
verified correctness/soundness bugs), cross-validated against the repo's historical
fix commits, and revised against the type-scoped/fan-out branch's record (5
adversarial review rounds, ~18 confirmed bugs — including the worst one, which no
review round found). Status: draft under active refinement.

This file stays terse and checkable: every sentence should be a check an agent can
run, a criterion it can apply, or one line of evidence with a citation. It is
self-contained; §7 resolves the effort shorthand the evidence bullets use.

How to use it: §1 is the repo's failure model — read it first. §2 routes the
change's risk to instruments *before* review rounds are spent. §3 is the review
itself (seven passes). §4 is what to do with each confirmed finding. §5 is the
pattern checklist to prompt passes with. §6 is process mechanics. §7 resolves the
effort shorthand ("fan-out branch", "timeout effort") used in evidence bullets.

## 1. Why this repo breaks the way it does

baton-sdk is a **stateful pipeline over durable artifacts**. Sync state lives in three
places at once — the sync token, the c1z contents, and in-memory syncer state — and
they constantly disagree mid-flight; that's normal. The invariant is:

> **Disagreement is fine; durable divergence is not.**

At every point where execution can stop (checkpoint written, sync sealed, artifact
published, process killed), the surviving state must be either self-consistent or
safely regenerable from what survived. Most real bugs here are not logic errors inside
a function; they are transient disagreements that got **made durable** — a sidecar
fsync'd before the grants it describes, a copied artifact keeping metadata from its
source, a token checkpointed mid-cleanup that a resumed process trusts.

Corollaries:

- **Execution is distributed.** Connectors run as child processes in lambdas/containers
  with no sticky routing — a resume, a retry, or the next page of the same listing may
  execute on a different machine. State that isn't in the sync token or the c1z does
  not exist. In-memory caches, connector-side sessions, temp files, and "we already
  fetched this" assumptions are all invalid across any task boundary.
- **The session store is a network service, not a cache.** It looks like a local map
  but is parent-process gRPC locally and a *separate gRPC service* in prod, with
  different implementations, failure modes, and consistency behavior per environment
  (`pkg/session`: client/server split, memory and noop variants).
- **A contract is only as strong as its consumers.** SDK error codes, checkpoint
  semantics, and preservability classifications are load-bearing for the c1 host,
  which can silently neutralize them (timeout effort: a host-side rewrap stripped
  the status that made timeouts retryable). Review a changed contract *at its
  consumers*, and put the contract test on the consumer side of the seam — the two
  highest-impact bugs of that effort were invisible from inside either repo alone.
- **SQLite is deprecated** — major bug fixes only. Pebble is the target engine. New
  features may be Pebble-only *by design*, with graceful degradation (feature cleanly
  disabled, not broken) on SQLite.
- **Degradation must be visible in proportion to exposure.** A *feature* cleanly
  disabled is an Info line; a *safety check* silently skipped makes a never-validated
  artifact indistinguishable from one that passed. The referential invariants skipped
  on SQLite logged at Info while type-scoped connectors depended on exactly those
  checks — the escalation to Warn came only from a dedicated invariant audit, not
  from any review round.

Review accordingly: for each write, ask "if we stop right after this — possibly
restarting elsewhere — what does the survivor believe, and is it wrong?" Trace state
ownership and stop-points first, function bodies second.

## 2. Review has no closure criterion — route bug classes to instruments first

### Risk triage: score the failure mode, not the subsystem

Risk is **escape × consequence**: how likely a defect ships and survives, times
what it costs to undo. Path lists ("sync, checkpointing, serialization…") are
projections of these axes and rot as the code moves; the axes don't.

**Escape axes** — how likely a defect ships and persists:

1. **Silence** — if this code is wrong, what makes noise? A panic is low; a
   well-formed wrong row is high. (No grep for this one; it's assessed.)
2. **Durability** — does any effect outlive the process? Persisted output has
   unbounded temporal blast radius: it will be read by code that doesn't exist
   yet. Durable *and* version-crossing (tokens, c1z, session values) outranks
   durable-and-local.
3. **Uncontrolled dimensions** — does correctness or liveness depend on
   schedule, crash timing, environment faults, the version pair, or **data
   volume**? Scale is the dimension local testing systematically undersamples:
   an O(n²) pass is correct at every N and dead at whale N, and fixtures never
   reach whales.
4. **Consumer distance** — who interprets this output: the next line, another
   process, another repo, a future SDK version? Error codes, config schemas,
   and exported contracts are remote-consumer surfaces even when they look like
   plumbing.

**Consequence — the remediation ladder.** Score what unfucking costs, not how
bad it sounds:

1. **Redeploy** — defect lives in code only; ship a fix.
2. **Re-sync** — bad state in our own artifacts; regenerable from source.
3. **Migrate** — every artifact in the fleet must be visited, under budgets, at
   whale scale. (The remediation is itself a migration — see the intersection
   class below.)
4. **Coordinate** — the contract itself is wrong: version bumps and lockstep
   changes across repos and deployments we don't control. (Evidence: the c1z
   spec break that forced a new SDK version — the cost was not the bug but the
   compatibility matrix stood up after the fact.)
5. **Irrecoverable** — external side effects: provisioning writes, credential
   operations, deletions in customer systems. No remediation exists, only
   prevention.

**Verdict rules:**

- **Silent + durable is HIGH**, and it compounds: every day undetected, more
  artifacts carry the defect, so detection latency converts escape into
  consequence — a format defect's migration bill grows with time-to-detection.
- **Any version-pair dependence is HIGH** (the class review cannot see at all).
- **Remediation rung ≥ 4 is HIGH regardless of escape score**, and flips the
  defense from detection to prevention: golden artifact corpus for formats
  (old artifacts in testdata opened by the new SDK; new artifacts opened by a
  pinned old SDK), dry-run/plan-apply and idempotency-by-identity for external
  effects, staged rollout for contracts.
- Otherwise, two or more escape yeses ⇒ HIGH.
- **Exposure modifies the verdict**: default-path changes outrank option-gated
  ones (§5.8's caveat applies — "gated" claims rot).

**Cost contracts.** On a short declared list of hot paths — grant expansion,
compaction, the per-checkpoint loop, anything that runs at artifact-open time —
performance is a correctness property, because budgets convert latency into
failure (deadline cascades into the retry/preservability machinery) and fixed
memory converts peak-RSS into an OOM crash loop (same artifact, same ceiling,
same kill: livelock). Doubling time or peak memory there is a breakage, not a
cost. The reviewable object is the **cost curve** (state the big-O delta in
grants/entitlements/graph size — "fine at fixture scale" is not a claim), and
the enforcement is a whale-scale benchmark gate, ratcheted against main
(allocations are deterministic where CI wall-clock is noisy; the killer is
accretion — nobody ships a 2×, five people ship 15%s). A cost contract without
a benchmark is dead prose; the benchmark is also what keeps this path list from
rotting — it is the registration.

**Migrations are the intersection class**: they run once per artifact at open
time, inside someone else's deadline, at whale scale, on data written by past
versions, on by default fleet-wide, behind a one-way format door. Chunked and
resumable is mandatory — each attempt makes durable forward progress (§5.7) —
because an over-budget migration retries the same artifact into a permanent
every-sync failure. (Evidence: the grant migration whose cost curve failed
every sync in prod; same class as #911.)

High-risk changes get §2's routing (instrument before review rounds) and the
full pass set; the "cannot-be-reviewed" criterion below is this triage's
extreme end. Evidence the axes beat the intuitive list: the GUI-default
coupling (#1024, a "mere" config field — axis 4) and the Lambda clock-freeze
(#1005, "mere" HTTP plumbing — axes 2+3) were high-risk changes no path list
would have flagged, while the age branch cleared every axis except consumer
distance — exactly where its one residual risk sat.

### The frame: coverage-driven verification

The frame is borrowed from hardware design verification (**coverage-driven
verification** — the cleanest existence proof that an industry facing
invisible-failure bug classes abandoned inspection-as-confidence and survived).
Three named parts: a **verification plan**, written before the bug hunt, states the
spaces to cover and the contracts that must hold over them; a **coverage model**
enumerates that space mechanically (never "we looked carefully"); **closure** is
the sign-off — the model exhausted, every assertion held. Done is a measurement,
not a feeling.

Review is sampling of the behavior space with no coverage model attached. After any
number of rounds no one can state what fraction of the space was inspected, so
"review converged" is not slow to reach — it is *undefined*. What review is for:
**discovering the plan** — the contracts worth stating ("spawned cursors must
drain") and the axes worth sweeping ("the resumer may be a different version") —
and finding bugs in cells already written down. Reviewers write the verification
plan; instruments close it.

Policy, stated up front:

- **Stopping rule: budget two, at most three review rounds**, then switch
  instruments. Review coverage is unmeasurable, so the budget is an economic
  cutoff, not an epistemic one; "the last round was clean" measures the reviewers
  as much as the code. (Corroboration: 15+ rounds without convergence, sync-replay
  effort; new high-severity bugs every round for 5 rounds, fan-out branch.)
- **Decorrelate reviewers**: on identical prompts, four models found nearly disjoint
  bug sets. Multiple models is coverage, not redundancy.
- **Prompts are not capabilities.** The worst bug on record — cross-version
  checkpoint resume silently sealing an incomplete sync — survived 5 adversarial
  rounds whose Pass 2 prompt *explicitly named the scenario*, and fell to a
  two-artifact harness in an afternoon, by counting rows. Reading cannot execute the
  other artifact.

Four classes review is structurally blind to, and where to send them:

- **Multi-artifact properties** — any property that is a function of more than one
  artifact: two SDK versions (checkpoint skew), two storage engines, current code +
  data written by past code, spec + implementation. Instrument: an execution harness
  that instantiates *both* artifacts and compares counted results against ground
  truth, in both directions.
- **Emergent schedule properties** — data races, lost wakeups, livelocks.
  Instruments: `-race` on the full suite as a hard gate (it found in minutes what
  every review round missed); randomized soak (topology + failure injection, many
  seeds) under race; bounded-exhaustive interleaving harnesses for small lock-based
  objects. Review still owns the *logic-level* concurrency bugs (abort-path commits,
  TOCTOU admission) — that is Pass 7's job.
- **Absence bugs** — code that should exist but doesn't; review verifies only what
  is written. (Fan-out: spawned cursors were never wired into the worker batch —
  every path a reviewer read looked complete.) Instruments: positive-evidence
  runtime invariants (enroll at admission, reconcile at quiesce — §4 rung 3);
  contract-coverage mapping (every behavioral sentence in a proto/doc contract maps
  to a test that fails if the sentence is false).
- **Error-path properties** — code that runs only when the environment fails.
  Review reads it; nothing executes it, because ordinary tests run over an
  implicitly cooperative environment (disks persist, RPCs return, deadlines never
  fire). Instrument: fault injection at a seam, with a stated recovery contract as
  the oracle (§5.10; house example: the engine errorfs sweep — every write/fsync
  failure in the EndSync window must reopen as resumable-unfinished or
  finished-and-complete, exhaustively).

### Subsystems that cannot be reviewed or ad-hoc-tested into confidence

Recognition criterion — all three must hold:

1. **Failures are silent**: absence, a hang, or well-formed wrong data — no error,
   no log line. (A hung scheduler looks like a slow sync; a dropped cursor looks
   like a smaller customer.)
2. **The behavior space is a product of uncontrolled dimensions** — worker
   schedules × crash/checkpoint cut points × version pairs × retry timings. Ad-hoc
   tests sample it at measure zero; a reviewer simulates exactly one schedule: the
   intended one.
3. **The oracle is not observable in any single execution** — correctness is
   defined against something no run exhibits (what the store *would* contain
   uninterrupted). The oracle must be constructed.

House example: the parallel scheduler + checkpoint/resume seam (5 adversarial rounds
did not converge on it; from first principles, should not have). For such
subsystems, the only conversion of effort into confidence is **verification as a
design deliverable**:

1. **State the contract as explicit properties** — exactly-once execution, dedup
   soundness, termination, transition atomicity, bounded memory, crash-cut
   consistency. If the property list can't be written, the design is not done.
2. **Construct the oracle** — the load-bearing step for silent failures:
   *self-equivalence* (an interrupted-and-resumed run must produce the same sealed
   store as an uninterrupted one); *derived ground truth* (a deterministic topology
   makes the expected store computable); *contract audit* (record the operation
   log, check it against the properties post-hoc).
3. **Mechanically cover a stated space** — every checkpoint cut point of a
   deterministic sync, all interleavings of a bounded configuration, N seeds of an
   adversarial grammar. Coverage of a *stated* space converts effort into
   confidence; review rounds and ad-hoc tests accumulate effort with unknown
   coverage. Claim **closure** only where the space is bounded and exhausted (the
   errorfs EndSync-window sweep, with its completeness proof); a seeded soak is
   *measured sampling*, not closure — the verification plan states which one each
   instrument claims.
4. **The harness is part of the subsystem** — designed and maintained with it, of
   comparable size and status. Here the §4 ladder inverts: rungs 2–3 are the
   *starting point* of the design, not the escalation path after a bug.

Production complement, not substitute: runtime positive-evidence invariants and
reconciliation ledgers (§4 rung 3) make a slice of the silent class loud in
operation, but liveness failures and anything they don't ledger stay invisible —
the verification harness is the only instrument for the rest.

## 3. The seven mandatory passes

Every non-trivial change gets each of these as a distinct pass. Separate,
separately-prompted reviewer agents per pass work well (see §6). Check §2's routing
first: if the change's risk concentrates in a multi-artifact, schedule, absence, or
error-path class, build the harness before spending review rounds.

### Pass 1: Edge cases

Empty/nil inputs, first-run vs. incremental, the referenced thing was deleted,
duplicates, partial vs. full sync types. Repo-specific: multi-valued annotations
(`entitlement_ids`), cycle-collapsed graph nodes, dangling refs (skip-with-warn
conventions). Engines: Pebble is the target; verify SQLite degradation paths return
clean nils/no-ops, not errors — and flag new SQLite feature-parity work as probably
wasted effort.

### Pass 2: Checkpointing & resuming

For every point execution can stop: does what survives stay consistent *or
regenerable*? Can the same checkpoint be resumed twice without double-apply? Does
write-durability ordering (`pebble.Sync` vs. `NoSync` vs. EndSync flush) match what
the state claims? Are sealed-window writes (`AllowSealed`) justified? Assume the
resumer is a different process on a different machine with a cold cache — possibly a
different SDK version. If correctness depends on anything the previous process knew
but didn't persist, it's broken.

Reading can only *ask* these questions; §2's harnesses answer them. Mechanize the
stop-point sweep: hook the checkpoint call, capture every token a deterministic sync
writes, resume a fresh process from each one, and assert the final store matches the
uninterrupted run — every checkpoint the sync ever writes becomes a tested cut
point. Resume each token twice (double-apply), and resume under a different worker
count (schedule-independence). The cross-version cell needs the two-artifact harness.

### Pass 3: Systematic permutation coverage

When behavior is parameterized by multiple dimensions — (in base? × in current?),
(edge spec: shallow/deep × filter wider/narrower/same), (sync type × engine × option
flags), (error category × recovery obligation) — write out the dimensions explicitly,
form the cross-product, and account for every cell: handled, collapsed by symmetry, or
excluded with a stated reason. **Bugs live in the cells nobody wrote down.** Reviewers
should ask for the table, not read the if-branches and try to reconstruct it.

- Special case worth memorizing: when the update is computed as a delta between two
  snapshots, the delta must be complete over the domain's symmetry — whatever you do
  for "present now, absent before," consciously decide for "absent now, present
  before." Removal is the classically-forgotten mutation, because fixtures grow but
  rarely shrink.
- The same discipline applies to **dynamic state, not just input parameters**: for a
  stateful object (work queue, action stack, state machine), enumerate
  (state × event) and account for every transition — with "aborted",
  "mid-checkpoint", and "resumed" included as states. The fan-out branch's
  atomicity family (parent cursor advanced before child admission could fail;
  completion marker set before the work it records committed; abort path still
  committing state) all lived in transition cells nobody had written down.
- House style to aspire to: the ingest-invariants verdict table — "a policy hole is a
  missing row, not a missing if-branch." Small space → explicit dispatch table in
  code. Combinatorial space → generate it (that's what fuzzing is: mechanized
  permutation coverage). Neither → the PR states the dimensions and disposes of each
  cell.
- **Where the table lives**: cells this repo can execute become rows of one
  table-driven test — the expected value *is* the disposition, and excluded cells
  are skipped rows with a reason string, so the table cannot drift from behavior.
  Cells that require an artifact this repo cannot execute (an older SDK's
  behavior) become a comment on the seam itself — where the next editor of the
  format will be looking — and migrate to the two-artifact harness when it
  exists. The PR description only *points* at both: nobody rereads a merged PR,
  and the table is needed by whoever touches the format next.
- **Design corollary: do not create state spaces too large to test.** Every
  independent boolean option, mode flag, and capability interface multiplies the
  cross-product. If the space can't be enumerated or fuzzed, the design is wrong, not
  the test plan.

### Pass 4: Error handling, classification & budgets

Every error lands in a declared category (retryable / decline-to-slow-path / fatal /
warn-and-skip), and each category's recovery obligation is discharged (e.g., restore
store state before falling back). Additionally:

- **Trace every deadline-bearing context to the work it bounds.** "A deadline exists
  but the inner work uses a different context" is a proven recurring class here
  (#899, #891, #893, the incremental branch's sidecar persist — and two more on the
  fan-out branch: a discarded timeout context in the skip-sync path, and a run
  duration that never reached root planner I/O).
- **Error identity is part of the contract:** status codes and sentinel matches must
  survive every wrap/remap boundary between the producer and the classifier that
  acts on them (§5.3, classification is a contract).
- No deferred `Close`/`Sync` may swallow errors on a success path (#811: torn
  artifacts reported as success).
- Retried operations must make durable forward progress in units smaller than the
  retry budget, or interruption livelocks (#911: unbounded DELETE vs. deadline, prod
  stuck 1h+).
- Watch for silent degradation: a fallback that always fires is a dead feature with
  green tests.
- A declared error category no test can *cause* is dead code with a taxonomy:
  fault-inject each category at its producing seam and assert the recovery
  obligation is actually discharged (§5.10).

### Pass 5: Invariant & verification gating

Any code that writes rows is an ingestion path and must pass through the invariant
seam (`RunIngestInvariants` + verification marker). New write paths that bypass the
syncer are guilty until proven registered. Artifacts must satisfy their
self-description: sidecar sync IDs match the sync-run record, verification markers
current, graphs marked expanded, format versions valid. (This pass polices the
seam; §5.9 is the design principle for why the seam exists and where it must sit.)

### Pass 6: Performance

Per-checkpoint / per-iteration fixed cost at whale scale (100k entitlements, MB-scale
graphs). What does the *failure* path cost? (A declined/fallback attempt is pure
overhead — it must be cheap.) Serialization in loops is the repo's recurring sin: the
sync token re-marshals everything embedded in state every ~10 seconds for the sync's
lifetime. Session-store access inside per-resource/per-grant loops is a network storm
in prod (§5.6).

On cost-contract paths (§2: expansion, compaction, per-checkpoint loop,
artifact-open-time work), this pass has a deliverable, not just a look: state the
cost-curve delta in grants/entitlements/graph size, and point at the benchmark that
enforces it. "No benchmark exists" is itself the finding.

### Pass 7: Concurrency

Check-then-act (TOCTOU) on anything shared (#812). Concurrent writers to the same
artifact — lambda retries mean the same task may run twice, possibly simultaneously;
conflict winners must not depend on which replica was slower. Goroutine lifecycle:
panics recovered in background goroutines (#845), error returns structured so defers
actually run (#854). Remember §2: this pass finds *logic* concurrency bugs; the data
races are the race detector's job, not yours.

## 4. The systematic-solutions ladder

When a bug is found, don't just fix it — climb as high on this ladder as is
proportionate:

1. **Type system / API shape** (best): make the invalid state unrepresentable.
   - Capability interfaces over type assertions — but note assertions on *optional*
     capabilities fail silently forever after a rename (#774). Every implementation
     needs a compile-time check (`var _ Iface = (*Impl)(nil)`) or a test that the
     production store satisfies it.
   - Version/generation fields in every serialized envelope, with both obligations
     that make the field real: **writers bump** on any reader-visible semantic
     change (enforce with a schema-fingerprint test that fails on field-set drift,
     forcing an explicit bump-or-exempt decision), and **readers fail closed** on
     unknown versions — degrading to redone work, never a silent partial parse.
     Conditional stamping (bump only when the new markers are actually present)
     keeps downgrades seamless on plain data. Evidence: the cross-version data-loss
     bug shipped *with* a version field present; nobody bumped it, and JSON
     zero-fill parsed the new fields away without error.
   - A single stored-state descriptor (key range + cleanup policy declared once) that
     all cleanup paths iterate, making "forgot to register" impossible rather than
     greppable.
   - Sealed-state types that can't be written; constructors that force the invariant
     pass.
   - Secrets get a wrapper type that redacts on `String()`/`Format()` at the boundary
     where they enter. Direct logging of secrets is ordinary review's job; the type is
     for the *indirect* flow — a third-party parse error echoing secret-bearing input
     into a wrapped error that lands in a durable artifact (log, token, checkpoint).
     At seams that wrap third-party errors around secret input, add one
     `NotContains(err.Error(), secret)` test — upstream error content is not ours to
     control (age branch tests exactly this seam).
2. **Fuzzing / property tests with an oracle.** This repo is rich in free oracles:
   incremental vs. full recomputation, marshal/unmarshal round-trips (including
   converters — #997 dropped a field in toPebble), retry-twice-converges idempotency.
   Differential testing beats hand-written cases when the space is combinatorial.
   (Pebble-vs-SQLite equivalence is a *sunsetting* oracle: keep existing tests while
   both engines live; build nothing new on it.)
3. **One validator over many point tests.** An artifact fsck — sealed-c1z
   self-description checked at the end of every producing test — retires a bug
   *category*. Include a leak check: every key/record in a store is owned by the live
   sync or explicitly allowlisted. Cheap variant: run the suite once with the noop
   session store; any failure is a source-of-truth violation hiding in the cache
   layer.
   - **Positive-evidence variant for completeness** (the absence class from §2): an
     evidence set maintained at the state-mutation funnels — enroll on admission,
     delete only on legitimate completion, rebuild from the checkpoint on resume —
     reconciled at quiesce. Absence bugs produce *missing* rows with perfect
     referential integrity; only positive evidence of completion can see them
     (the fan-out branch's spawned-cursor drain invariant — not yet on main;
     main ships I1–I9 in `pkg/sync/ingest_invariants.go`). The stronger form is a full
     ledger: `sealed = received − deliberately_dropped + generated`, every term of
     which the SDK can count — a mismatch is attributable to the SDK by
     construction.
4. **Integration tests** that exercise the real store/engine lifecycle — resume, seal,
   fold, *reuse* — not mocks. Single-sync fresh-store tests are structurally blind to
   the orphan class.
5. **Unit tests** (last resort, for logic-dense pure functions).

## 5. Bug patterns, organized by principle (the "we've been burned" list)

Each family is a **general principle** with the local evidence that earned it a slot.
The principle is the reusable part — apply it to code the evidence never touched.
When adding an entry, first try to file it under an existing principle; a new
principle needs at least a plausible second instance.

### 5.1 Durable claims are ordered after the facts they claim

A durable record asserting work *happened* must be written after the work is durably
done. (Records of *intent* may be written before — that's a write-ahead log. Never
confuse the two directions.)

- Completion flag written before the work it records was durably scheduled: any
  intervening error becomes a permanent skip (fan-out branch, `TypeScopedPlanned`).
- Graph sidecar fsync'd before the sync seals: it describes data a resume is free to
  rewrite (incremental branch).
- Success reported before the OS flushed the artifact: deferred `Close` swallows
  write-back errors (#811). Explicit Sync+Close+err-check on success paths.

### 5.2 Every exit path discharges the same obligations

Enumerate the exits — success, error, abort/cancel, panic — and verify each leaves
the same postconditions. Cancellation is an event that gets its own cell in Pass 3's
(state × event) table; it is the one nobody writes down.

- Abort path that still commits state, bypassing the dedup set and caps the normal
  path enforces (fan-out branch).
- Validate everything, then mutate: a parent cursor advanced before sibling
  admission could fail silently drops the rejected cursors on resume (fan-out
  branch). All-or-nothing commit; a validation failure leaves prior state untouched.
- Error returns structured so deferred cleanup actually runs (#854); panics
  recovered in background goroutines (#845).

### 5.3 Classification is a contract: explicit, total, preserved, coherent

Wherever behavior branches on the class or identity of a value — an error's
retryability, a cursor's identity, a record's kind, a store's capability — that
classification must be **explicit** (a dedicated field, never an in-band magic
value), **total** (closed over every producer of the domain, including implicit
ones), **preserved across seams** (wraps, RPC hops, and serialization must not strip
the status/sentinel identity downstream classifiers act on), and **coherent** (two
classifiers over one domain state their intended relationship, asserted in a test).

- Explicit: inferring "type-scoped" from `ResourceID == ""` misrouted
  malformed-but-real stored rows into the new behavior (fan-out branch).
- Total: a dedup identity that enumerated "the kinds of cursor" missed the implicit
  one — the parent's own continuation token collided with a spawned sibling's
  (fan-out branch). Same failure at the API level: `Pick` on annotations silently
  collapses N>1 to one; count before picking where multiplicity matters.
- Preserved: a host-side wrapper string-matched `err.Error()` and returned a fresh
  error, stripping the gRPC status — retryable lambda timeouts became
  non-retryable, discarding hours of checkpoint progress ~1,100×/fortnight (timeout
  effort; visible only by tracing the value across the repo seam).
  `strings.Contains(err.Error(), ...)` gates are findings by default; a rewrap
  preserves status/sentinels or explicitly declares the downgrade, enforced by a
  consumer-side test.
- Coherent: `retry.go` held `DeadlineExceeded` retryable while `IsSyncPreservable`
  refused to preserve it — incoherent without reading either body. State the
  relation (retryable ⊆ preservable) and assert it (timeout effort).
- Type assertions are classifications too: an optional-capability assertion fails
  silently forever after a rename (#774: months). Compile-time
  `var _ Iface = (*Impl)(nil)` per implementation.
- Drift-prone shape: `(bool, error)` returns with sentinel taxonomies — callers
  reconstruct outcomes wrong. Prefer typed results.

### 5.4 Stored state has a lifecycle contract; registration must be structural

Every keyspace, sidecar, and record type must be handled by *every* lifecycle
surface — cleanup, reset-for-new-sync, fold rewrite-or-delete, format conversion.
Convention-maintained registries are independently forgettable; the structural fix
is one stored-state descriptor that all surfaces iterate (§4 rung 1).

- Orphaned records after syncs: broken prod repeatedly, never reproduces locally —
  fresh-store single-sync tests are structurally blind. New stored state needs a
  reuse-lifecycle test (sync → sync again / reset / fold / resume; assert nothing
  keyed to the first sync survives).
- Copied artifacts inherit metadata keyed to the source sync: fold/copy must rewrite
  or delete every such sidecar/marker (stats, invariant marker, graph — the graph
  was the miss on the incremental branch).
- The inverse failure — convert/copy paths *lose* fields: converters must be closed
  over the full record schema, enforced by round-trip equivalence (#997: toPebble
  dropped `discovered_at`; recurred in #1023: resources dropped
  `profile`/`status`/`created_at` — the class jumps between record types). The
  instrument, still unbuilt: one test over every (v2 message, v3 record) converter
  pair that populates *every* field via `protoreflect` (a hand-written fixture is a
  point test that rots), round-trips, and requires each field to survive or appear
  on an exemption list with a stated reason. The proto descriptor is a free
  coverage model; this is closure over it.
- The syncer is not the only writer: resume-and-write compactor paths bypass every
  side effect attached to the syncer (invariants, markers, cleanup).

### 5.5 Bytes crossing a boundary are hostile

Anything serialized across a process, version, or service boundary gets a version, a
fail-closed reader, and a round-trip test.

- Go JSON zero-fills silently: unversioned structs parse wrong data after a rename —
  and versioned ones parse *new fields* away just as silently unless writers bump
  and readers fail closed (§4 rung 1: the rollback data-loss bug happened *with* a
  version field present).
- Session values cross process/version boundaries through a service: version
  envelopes, tolerate absence, never trust shape.
- Cursors must be self-contained: a page token encodes everything a cold-started
  process needs; the connector doesn't "remember."
- If producer and consumer share a module, a convention specified in prose is a
  finding: export one function both sides call, collapsing the contract to "call
  this" — greppable, and it cannot drift. (Age branch: `key_ids =
  hex(sha256(recipient))` lives in a proto comment while the consumer imports this
  repo.) Prose is the fallback only when no code can be shared across the seam.

### 5.6 Distribution invalidates local intuitions

Applications of §1's corollaries — the local topology (parent process, one machine,
sticky everything) systematically hides these.

- No sticky routing → no cross-boundary caching: memoizing within one invocation is
  fine; any map that outlives one operation is suspect.
- Session-store calls are RPCs, not map reads: per-key lookups in hot loops are a
  prod network storm; batch, and give session errors a declared category (the local
  transport almost never fails, so error paths are untested by default — §5.10 is
  the systematic answer).
- No read-your-writes across task boundaries: the noop and memory session
  implementations legally return nothing; session data is a hint that may be
  absent. Correctness-critical state lives in the token or the c1z.
- The same task may run twice, possibly concurrently: idempotency-by-identity on
  every write path; conflict winners must not depend on replica speed.
- Clock skew feeds newer-wins merges: `discovered_at` comparisons cross machines;
  distrust wall-clock ordering across process boundaries.

### 5.7 Budgets bound work only if the plumbing connects them

A declared limit (deadline, retry budget, checkpoint interval) constrains nothing by
existing; trace the connection from the declaration to the work it is supposed to
bound.

- Dangling deadlines: a deadline-bearing context exists but the inner work runs
  under a different one (#899, #891, #893, incremental sidecar persist, fan-out
  skip-sync and root-planner paths). Trace the plumbing, not the declaration.
- Retry-granularity livelock: work units bigger than the retry deadline make zero
  forward progress under repeated interruption (#911: prod cleanup stuck 1h+).
  Batch with durable commits so each attempt advances. Migrations are the worst
  case (§2's intersection class): an open-time migration whose cost curve exceeds
  the budget at whale scale retries the same artifact into a permanent every-sync
  failure — chunked and resumable is mandatory, not an optimization. (Evidence:
  the grant migration that failed every sync in prod on big-O grounds.)
- Marshal-in-checkpoint-loop: anything embedded in sync state is re-serialized
  every ~10 seconds for the sync's lifetime; per-checkpoint cost is a budget nobody
  declared.

### 5.8 Unfinished transitions rot into hazards

Any "temporary" dual state — a rollout flag, a deprecated engine, an aging branch —
is a standing bug source carrying a finish-or-delete obligation. Silence is the
common thread: each of these fails by *nothing happening*.

- Feature-flag rot is the dual of "a fallback that always fires": a gate that never
  graduates is dead code with a confusing name (`SYNC_PRESERVABLE_C1Z`, a *host-side*
  gate — not greppable in this repo: off for
  99.2% of the events it governed; the stale gate repeatedly derailed the timeout
  investigation). Every flag carries graduation-or-delete, and each long-lived flag
  doubles Pass 3's state space.
- Deprecated-engine rot cuts both ways: SQLite fallback paths decay untested but
  stay load-bearing until removal ships. Touching a dual-engine seam: verify the
  SQLite path or explicitly gate it off. The capability interfaces and
  degrade-to-token dualities are the removal-day cleanup checklist.
- In exported library API the transition may be *unfinishable*: deprecated symbols
  can be aliased, never deleted. The standard is old name = thin delegate to the new
  path — never a parallel implementation — with the equivalence asserted in a test.
  (Age branch: two registries and two resolvers over one config type, nothing pinning
  them coherent; §5.3's coherence clause applies doubly when the dual state is
  permanent.)
- Branch aging: a conflict-free rebase can be semantically stale — review the
  *rebased* semantics against current main (the incremental branch predated the
  invariant seam entirely).

### 5.9 An obligation owned by every path is owned by none — localize it

When a cross-cutting obligation (validation, derived rows, cleanup, markers) is
implemented per-path, paths drift, readers make path-dependent assumptions, and new
paths silently omit it. The coupling is unmarked — nothing tells the author of path
N+1 what it must replicate — so review is the only enforcement, and review has no
closure criterion (§2). This is the design-side principle behind Pass 5, §5.4's "the syncer
is not the only writer," and the stored-state descriptor (§4 rung 1). Two
escalating fixes, both used in this repo:

- **Funnel the paths**: force all writers through one choke point and implement the
  obligation once on the shared path (Phase 2, #1016 — the rawdb choke point: a new
  path has no way in except through it).
- **Re-derive the obligation from state, not events**: redefine it as a function of
  the *resulting* state, evaluated once at a terminal seam, making it independent of
  how the state arrived (Phase 3, #1017 — ingest invariants: "each side effect is
  defined as a function of the store and evaluated after every writer has
  finished"). Stronger than the funnel: it severs the event coupling instead of
  centralizing it.

The caveat that bit anyway: **the seam must sit at the true convergence point.**
Phase 3 attached evaluation to the syncer, but the syncer is a client of the store,
not its funnel — the compactor's resume-and-write path bypassed the syncer while
still reaching the sealed artifact (incremental branch). The point nothing can avoid
is the store's seal; anything shallower is a funnel with a gap.

Costs to accept knowingly: a post-hoc scan instead of piggybacking on the write (I4
became fail-fast-only for exactly this reason); *detection* is late — fine for
validation, wrong for anything that must be *prevented*; and per-row provenance the
check needs must be recorded in the data (exemption annotations), because the call
stack that knew it is gone.

### 5.10 The environment is an input — sweep it like one

Ordinary tests hold the environment implicitly cooperative, so every error path is
dead code until a fault injector executes it. Treat the fault schedule as a test
dimension and apply Pass 3's coverage doctrine to it: exhaustive over a stated
window, seeded-random over the combinatorial rest. House exemplar: the engine
errorfs sweep (#1015; `pkg/dotc1z/engine/pebble/errorfs_sweep_test.go`).

- **Contract first.** Injection without a stated recovery contract is chaos, not
  verification. The sweep's oracle — any injected failure followed by a crash must
  reopen as resumable-unfinished or finished-and-content-complete; anything else is
  a durability-ordering bug by definition — is what converts injected faults into
  findings. The injector supplies adversarial schedules; the oracle judges them;
  neither is useful alone.
- **Model the deployment's physics, not what's easy to inject.** A dying disk does
  not recover: fail the k-th write *and all subsequent*, or post-failure background
  writes heal the crash image. A crash keeps only fsynced data (strictest image). A
  storage-engine fatal is a wedged process, observed from outside — not recovered
  in-process. Lambdas die mid-task and run the same task twice. A fault model
  gentler than production tests faults that do not occur — and some production
  faults are invisible to local physics entirely: a Lambda freeze pauses the
  monotonic clock, so `IdleConnTimeout` never sees the gap while proxies time
  pooled connections out on the wall clock (#1005 — the fix measures the gap in
  wall-clock time, and its tests count connections to prove the pool was dropped).
- **Seams are a design requirement.** Injection reaches only what flows through an
  interface (pebble vfs, session store, connector RPCs, contexts/clocks). Raw
  `os.*` IO is outside every fault domain — the sweep's own stated exclusion (the
  v3 envelope save) is an untested error path by construction. State exclusions
  explicitly; each one is a finding-in-waiting, and new IO should route through an
  injectable seam rather than enlarge the exclusion list.
- **Completeness must be provable.** The sweep arms at EndSync and raises k until
  an armed run completes without injecting anything — positive evidence the window
  was covered end-to-end. Prefer that shape over "we injected some faults."
- **Prove the instrument can see its faults** (DV's mutation adequacy): plant a
  known contract violation and watch the harness fail. A sweep that has never
  caught a planted bug is itself unverified code.
- Latency and deadline expiry are faults too: injecting them is the instrument for
  §5.7's dangling-deadline class — a budget no test can cause to fire bounds
  nothing.

## 6. Process guidance for AI-driven review

- Route first (§2): review is one instrument among several, and its budget is two to
  three rounds. Multi-artifact, schedule, absence, and error-path risks get
  harnesses, not more reviewers.
- If the change touches a subsystem meeting §2's silent/combinatorial/no-oracle
  criterion, ask for the property list and the verification harness — their absence
  is the finding. Do not accept review rounds as a substitute; no number of them
  constitutes coverage there.
- Run the seven passes as **parallel, separately-prompted agents** — and assign
  passes across *different models*, not just different prompts: on identical
  prompts, different models found nearly disjoint bug sets. Each agent reads the
  surrounding unchanged code (not just the diff), cites file:line, and discards
  anything unverifiable. Then **verify every finding against the code before
  accepting it** — severity re-grading in both directions is normal.
- **Slice this document when spawning pass agents; do not paste it whole.** Pass
  agents receive no context except their prompt, so the orchestrator inlines the
  relevant sections verbatim. Feeding all ~50 checkable items to every agent
  dilutes the ones it owns. The map (§1 goes to everyone; add §7 if the slice
  cites effort shorthand):
  - Pass 1 (edge cases): §1 + Pass 1 + §5.3
  - Pass 2 (checkpoint/resume): §1 + Pass 2 + §5.1, §5.4, §5.5
  - Pass 3 (permutations): §1 + Pass 3 + §5.2, §5.3
  - Pass 4 (errors/budgets): §1 + Pass 4 + §5.3, §5.7, §5.10
  - Pass 5 (invariant gating): §1 + Pass 5 + §5.9
  - Pass 6 (performance): §1 + Pass 6 + §5.6, §5.7
  - Pass 7 (concurrency): §1 + Pass 7 + §5.6
  - Fix-time agent: §4 + this section's failing-test-first and recurrence rules
  - Orchestrator (you): the whole document, read once.
  A consequence: mild redundancy across sections is load-bearing — sliced readers
  never see the other copies. Do not deduplicate it away.
- Check merge-base age first; review what the change means on *current* main. Also
  confirm `git log base..head` contains exactly the intended commits before spending
  review budget: a worktree branched from a dirty local HEAD shipped foreign commits
  into a PR, and reviewers produced confident findings about code nobody under
  review wrote (timeout effort).
- When vendored or dependency changes dominate the diff, verify provenance
  mechanically before reviewing the rest: the version is a tagged upstream release,
  `go mod verify` passes, and every *new* transitive dependency — especially
  cryptographic — is named in the PR description with a reason. A 3,000-line vendor
  diff nobody mentions is a finding (age branch: ~75% of the diff was a vendored age
  upgrade pulling in new post-quantum primitives).
- **Failing test first** for every confirmed bug: write the test, watch it fail for
  the right reason, then fix. (Empirically load-bearing on the fan-out branch: fix
  rounds stopped regressing the moment this became mandatory.)
- Contracts check on any exported API touched: doc comments match actual behavior;
  functions whose names promise purity (Marshal, Get, Read) don't mutate arguments.
  For proto contracts, map every behavioral sentence to the test that would fail if
  it were false — unmapped sentences are the absence class (§2).
- End reviews with: a coverage-vs-gap table, a confidence statement, and — for each
  finding — which rung of the §4 ladder would have caught it mechanically. If a
  finding has no mechanical catcher, that's a gap in this document: add it.
- **The recurrence rule** (dual of the above): when a bug belongs to a class this
  document already names, the fix must ship the ladder climb, not just the patch —
  point-fixing a documented class is itself a finding. Evidence: converter field
  loss was point-fixed in #997 (grants, `discovered_at`); the class recurred 19
  days later in #1023 (resources, `profile`/`status`/`created_at`) and was
  point-fixed again; the descriptor-closure test that retires the class still does
  not exist (§5.4).

## 7. Case-name index

Evidence bullets cite recurring efforts by shorthand. PR numbers resolve with
`git log --grep`; branch refs age out, so the descriptions are the durable part.

- **Incremental branch** — `manoj/incremental-grant-expansion` (unmerged at time of
  writing): diff-aware grant expansion with a persisted graph sidecar. Source of
  the sidecar-ordering, graph-copy, and compactor-bypass evidence.
- **Fan-out / type-scoped branch** — the type-scoped sync fan-out effort (parallel
  per-resource-type cursors spawned from a parent; unmerged at time of writing).
  5 adversarial review rounds, ~18 confirmed bugs; source of the atomicity-family,
  absence, and dedup-identity evidence — and of the worst bug on record, the
  cross-version checkpoint resume that silently sealed an incomplete sync.
- **Timeout effort** — lambda-timeout preservability work spanning this repo and
  the c1 host: retryable-timeout classification, preservable sync state, and the
  host-side rewrap that stripped gRPC status. Source of the cross-repo seam
  evidence; its contract tests live on the consumer side.
- **Sync-replay effort** — the 15+-review-round record cited for non-convergence.
- **Age branch** — `paul.querna/age-recipient-encryption`, merged as #1021: age
  recipient encryption provider. The small-stateless counter-case: most passes
  route to N/A and the residual risk is all at repo boundaries.
- **Phases** — the verification-infrastructure PR series: #1015 (Phase 1, pebble
  fault-injection harness), #1016 (Phase 2, rawdb choke point), #1017 (Phase 3,
  ingestion invariants I1–I9), #1022 (Phase 4, source-cache replay protos).
- **The c1z spec break** — a format change that old SDKs could not read, forcing a
  new SDK version and a compatibility matrix stood up after the fact. §2's
  remediation-rung-4 exemplar: the cost was coordination, not the code fix.
- **The grant migration** — an open-time migration whose big-O was fine at fixture
  scale and over budget at whale scale; it retried the same artifact and failed
  every sync in prod. §2's intersection-class exemplar (same livelock shape as
  #911).
