# RFC 0010: SQLite demotion — conversion-only, one live engine

Status: proposed. Supersedes RFC 0008 §8, whose sizing is stale in three
material ways (§2).

Risk routing (REVIEW_CHECKLIST §2): this is a behavior change, not a cleanup,
and it does not route as one thing.

- **Sanitize on Pebble (§4.2) is HIGH.** Relaxing a guard whose whole purpose
  is preventing silent destination corruption is silent + durable, and the
  remediation rung is ≥4: a corrupted sanitize output is not re-derivable
  without a multi-hour re-run against a source that may have rotated.
  `BUG_CATCHING.md` §5.4 (stored state has a lifecycle contract) and Pass 2
  (checkpointing & resuming — "resumable twice, the resumer is a different
  process with a cold cache") govern; the full step-up applies.
- **Unconditional conversion on open (§4.3) is HIGH.** Conversion keeps one
  sync and renames over the source. Default-path exposure, durable, and
  irrecoverable once the rename lands — the top of the consequence scale.
  `BUG_CATCHING.md` §5.12 (a commit point is a contract) applies to the rename.
- **Forcing Pebble for compaction output (§4.4) is MEDIUM.** The conversion it
  relies on already runs in production for mixed-engine inputs.
- **Retiring explicit SQLite configuration (§4.5) is MEDIUM and loud** — a
  rejected config fails at startup. Its risk is migration coverage, not
  silence.
- **Deleting the SQLite write paths (§4.6) is LOW.** Compile-verified; the risk
  is in the steps that precede it.

## 1. What this is

Demote SQLite from a live storage engine to a conversion-and-read-only path:
a newer SDK still opens a v1 `.c1z`, converts it to Pebble, and reads one
read-only, but nothing syncs into SQLite, compacts through it, expands in it,
or sanitizes out to it.

The prize is not the deleted lines. It is that `c1zstore.Store` and its family
stop having two implementors, so the runtime capability probes that currently
choose between a fast path and a lossier fallback become ordinary method calls
that fail to compile when the method goes away. That class of silent
degradation is the most expensive thing about the current arrangement, and it
is the reason to do this at all.

This RFC covers both repositories, because the sequencing constraint that
matters most lives in the hosted backend, not the SDK.

## 2. Where we actually are — three corrections to RFC 0008 §8

RFC 0008 §8 sized this work before three things changed. Anyone planning from
that section will plan the wrong thing.

**The default already flipped in the SDK, and has not reached the backend.**
`selectStoreDriver` defaults an unset engine to Pebble
(`pkg/dotc1z/engine_registry.go:246`). But the hosted backend vendors an
older baton-sdk release where the same line reads `EngineSQLite`. So "new
files are SQLite today" is false of this repository and true of production.
§8.3 calls the default flip "step zero"; the real step zero is the SDK bump
(§4.1).

**Diff is gone, not deferred.** §8.3 calls `GenerateSyncDiff` "the hard one"
and proposes moving diff up a layer. There is nothing to move: `8bd11917`
("Remove diff sync support", #1098) deleted the feature, so the method does not
exist on either engine and `pkg/dotc1z/diff.go`,
`pkg/tasks/local/differ.go` and `engine/pebble/adapter_diff.go` are absent. The
removal is recorded as final in
`docs/rfcs/0004-storage-engine-v4/tracker.md:56-63` — the feature was never
enabled in production and had no consumer. The backend has no reference to it
outside `vendor/`. The largest prerequisite on the §8.3 list is therefore
closed, and closed by deletion rather than by porting.

What survives is a read obligation, already discharged: an old SQLite c1z can
carry the removed diff-sync shape (a `partial_upserts`/`partial_deletions`
pair with bidirectional `linked_sync_id` and a cross-file `parent_sync_id`),
and `pkg/dotc1z/legacy_diff_sync_rows_test.go` pins that those rows list
cleanly and that `ToPebble` ignores them — failing loudly with "no convertible
sync found" when they are all the file holds, rather than emitting a silent
empty artifact. That is the precedent the permanent conversion reader should
follow for every other legacy shape.

**The capability-probe payoff is real but smaller than claimed.** §8.1 says 28
non-test sites discover capabilities by type assertion, ~22 of them because one
engine can do what the other cannot. On the current tree the engine-driven
probes in `pkg/sync/syncer.go` number about ten (`:289`, `:293`, `:355`,
`:384`, `:393`, `:421`, `:428`, `:436`, `:444`, `:475`), and the invariant gate
is `pkg/sync/ingest_invariants.go:507`, where four `requiresStore` invariants
(`:379`, `:386`, `:393`, `:400`) are skipped when the optional Pebble
inspection surface is absent — the comment at `:282` states plainly that SQLite
artifacts never satisfy them. Much of §8.1's inventory has already been cleaned
up. The argument stands on the invariant gate and the remaining ten; it should
not be sold on the old numbers.

## 3. The conversion boundary is already right: inline, in place, paid once

An earlier framing of this work proposed converting at ingestion, either in the
upload RPC or in a dedicated activity. Both are wrong, and the reason is worth
recording because it is not obvious from the SDK side alone.

Conversion is not a cost added to a budget. It is an investment that shrinks
it: Pebble is roughly 10× faster on the grant path and produces ~40% smaller
output (`docs/rfcs/0004-storage-engine-v4/tracker.md:31`), and expansion and
compaction are exactly the grant-heavy work. Converting first and then
expanding is a large net win inside a single activity.

It is also paid once, not once per window. The backend's sync activity opens
the *live* artifact writable with the resolved engine, so a v1 file hits the
in-place conversion at `pkg/dotc1z/engine_registry.go:280`, which renames the
converted temp over the artifact path. The same artifact is carried through
the activity's callers and committed back. The live artifact in object
storage becomes v3, and every later window reads a v3 header and skips
conversion entirely. (Backend call sites are tracked in the internal
ticket.)

The upload RPC is therefore the one place conversion must *not* go — a
connector-held gRPC stream cannot block on a multi-hour rewrite — and a
dedicated activity would only add an orchestration hop plus a second multi-GB
round trip, while separating the cost from the benefit that repays it.

Two consequences follow, and both shape the plan:

1. **Read-only opens must not convert.** The conversion gate at `:280` requires
   `!options.readOnly`, and that is load-bearing. The backend's file-connector
   ingest opens uploads read-only, so a v1 upload is
   served as a SQLite store for a pure read-through with no expansion or
   compaction downstream to recoup a conversion. Converting there would be
   paying the cost precisely where nothing recovers it. A read-only SQLite
   reader has to survive this RFC (§4.7).
2. **The cost lands on one window.** The first sync after a connector's engine
   resolves to Pebble pays conversion inside its ordinary budget — 120 minutes
   for incremental sync (`incremental-sync/incremental_sync.go:185-186`), 210
   for file-connector sync (`file-connector-sync/activity.go:63-64`). That is a
   rollout concern (stagger, watch first-window durations), not an
   architectural one.

## 4. Prerequisites, in dependency order

### 4.1 Step 0 — bump the vendored SDK in the backend

Nothing else on this list changes production behavior until this lands. The
bump also carries the two things later steps depend on: the diff removal (§2)
and `2c05c0b9` (#1103), the `ResumeSync` fix whose engine-level tests are the
scaffolding §4.2 builds on.

Acceptance: the backend's resolved-engine behavior is unchanged for every
connector — an unset engine with the tenant flag off must keep producing the
same on-disk format it produces today. This is the step where a silent default
change would be easiest to ship by accident, because the same source line means
`EngineSQLite` before and `EnginePebble` after.

### 4.2 Step 1 — sanitize with a Pebble destination

This is the gating item and the hardest one. The backend's sanitize activity is
the only remaining live-SQLite **writer** in the hosted path: it reads either
format through `NewStore` but opens its destination with `NewC1ZFile`, because
the snapshot loop takes the concrete `*C1File`. Explicit SQLite cannot be
rejected (§4.5) while a production activity requires a SQLite writer.

The SDK has two Pebble guards in `pkg/c1zsanitize/sanitize.go`, and they carry
different weight.

The **multi-sync source** rejection (`:148`) is likely already unreachable in
the hosted flow: sanitize's source is a completed artifact, and the backend
isolates completed artifacts to a single sync when it stores them. Confirm
against real artifacts before relying on it. Under conversion-only it becomes
structurally unreachable.

The **resumable** rejection (`:379`, in `isResumableDestination` at `:378`) is
the real work, and there is a hypothesis worth testing: the guard may be
broader than its own justification. It says Pebble's "replace-in-place
`StartNewSync` wipes any prior sync's data", but the resume path does not call
`StartNewSync` — it rebinds via `SetCurrentSync(rs.dstSyncID)` (`:552`), and
`StartNewSync` appears only on the fresh-sync branch (`:570`). With a single
source sync there is exactly one destination sync, so a resume never takes the
wiping branch. Where the guard genuinely earns its keep is the multi-sync case:
sanitize iterates source syncs, and calling `StartNewSync` for one that had not
started in a prior run excises the destination syncs the prior run completed.
That is real and structural under one-sync-per-file — but it is a function of
source cardinality, not of resume, and the guard keys on engine alone.

Two changes follow, if the hypothesis survives verification:

- Narrow the resumable guard from "engine is Pebble" to "engine is Pebble **and**
  the source has more than one sync."
- Switch the resume rebind from `SetCurrentSync` to `ResumeSync`
  (`engine/pebble/adapter.go:143`). This is correct independent of engine
  choice: Pebble's `SetCurrentSync` (`:199`) treats a missing record as legal
  and binds anyway, so a read failure would bind a sync with no record and then
  accept writes into it. `ResumeSync` fails closed. This is precisely the bug
  class #1103 closed in the compactor, where `StartOrResumeSync` (`:176`) fell
  through to `StartNewSync` on any non-clean lookup and excised the merge
  output.

Backend-side: `startSnapshotLoop` and `restoreOrStartDst` must work through the
store interface rather than `*C1File`, and the four SQLite pragmas below
`sanitize_activity.go:852` become meaningless.

Because this is HIGH: frozen behavioral plan before implementation;
instruments on the destination-sync lifecycle (fresh, resumed-partial,
resumed-ended, multi-sync-source rejection); each instrument validated against
a planted violation, since the failure is silent; acceptance is the crash /
interrupt harness with resume-twice-from-a-cold-process, not the PR suite. The
existing 12-hour per-attempt envelope and 24-hour job ceiling
(`sanitize_activity.go:166`, `:195`) mean a wrong answer here costs a day of
worker time before it surfaces, which is why detection is not an acceptable
defense.

### 4.3 Step 2 — decide the conversion policy, then make it unconditional

Today only an explicit Pebble request on a writable v1 file converts
(`engine_registry.go:280`). Making conversion unconditional for writable opens
is what "conversion-only" means operationally. It is also HIGH, and not for
mechanical reasons: conversion keeps exactly one sync and renames over the
source, so every other sync the file held is gone with only a WARN line as the
record (`pkg/dotc1z/convert_open.go:64`).

The policy decision belongs here, before the code:

- Does the pre-conversion artifact get retained? The backend still has a
  multi-sync `CloneSync` branch (`sync/c1z.go:799`), which implies multi-sync
  files do occur in practice. Retention costs storage and keeps the lossy step
  reversible; discarding makes the WARN the only record. RFC 0009's
  preserve-by-default posture for sync artifacts is the closest precedent and
  argues for retention at least through the migration window.
- Does the hosted path pass an explicit sync ID instead of relying on
  resolution? `ToPebble` takes one (`pkg/dotc1z/to_pebble.go:223`); the
  activity knows which sync the run concerns. Selecting explicitly removes the
  lossiness question from the hosted path entirely and leaves the
  "keep newest, WARN the rest" default governing only local and CLI conversion
  of legacy files, where it is the right tradeoff.

### 4.4 Step 3 — force Pebble for new stores and compaction output

Two spots in the compactor default to SQLite: `resolvedEngine` treats the zero
value as SQLite (`pkg/synccompactor/compactor.go:106`, returning at `:108`) and
`inferEngineFromInputs` returns SQLite when no input is Pebble (`:133`,
returning at `:175` and `:178`). Make the output always Pebble.

The conversion this depends on already ships: `convertSQLiteInputToPebble`
(`compactor_pebble.go:1046`) converts v1 inputs whenever the output engine is
Pebble, which after this step is always. Generalize `ErrEnginePolicyConflict`
from "explicit SQLite with a Pebble input" to "explicit SQLite, ever."

### 4.5 Step 4 — retire explicit SQLite configuration

A breaking config change in three places, all of which need a migration and not
just tightened validation:

- `StorageEngineField`'s allowed values (`pkg/field/defaults.go:346`).
- The `storage_engine` in-lookup on the gRPC sync task
  (`proto/c1/connectorapi/baton/v1/baton.proto:63`, generated into
  `pb/c1/connectorapi/baton/v1/baton.pb.validate.go:4069`). The task carries the
  engine to the connector process, so this is a version-pair surface: an old
  connector receiving an unfamiliar value, and a new connector receiving
  `sqlite`, both need defined behavior.
- The backend's per-connector storage-engine setting, which is
  admin-settable. Any connector currently pinned to SQLite there must be
  migrated before the value is rejected, and the corresponding tenant
  feature flag does not override a per-connector setting. (Exact call
  sites tracked in the internal ticket; they are not part of this
  repository.)

Also drop the `sqlite` leg of the CI matrix (`.github/workflows/ci.yaml:113`).

### 4.6 Step 5 — delete the SQLite write paths

Compile-verified once the steps above land. The targets:

- The attached compactor (`pkg/synccompactor/attached/attached.go`, 143 lines,
  asserting `AsSQLiteStore` at `:30` and `:34`) and the SQLite compaction
  branch.
- `sqliteDriver` (`engine_registry.go:106`, `:377`) and the driver registry's
  dual dispatch, along with `EngineDriver` itself once there is one live
  implementor.
- The SQLite-only write operations and their CLI entry points:
  `RollbackExpansion` (`pkg/dotc1z/rollback_expansion.go`, 420 lines, reached
  from `cmd/baton/rollback_expansion.go:174`), `Vacuum`
  (`pkg/dotc1z/sync_runs.go:1187`, via `cmd/baton/optimize.go:39-40`), and the
  `CopyIsolateSync` fast path (`pkg/dotc1z/copy_isolate_sync.go`, 244 lines,
  reached from `cmd/baton/rollback_expansion.go:145`). These are features
  dropped on purpose, not dead code.
- The write halves of the paired implementations: grants, streaming, clone,
  sessions, sync metadata.
- The cross-engine parity harness — `pkg/dotc1z/cross_engine_parity_test.go`
  (422 lines) and `pkg/dotc1z/engine/equivalence/` (548 lines) — plus the
  SQLite half of the engine-parameterized tests. This is the largest single
  block and the clearest signal the abstraction has collapsed to one side.

Sizing for scheduling: the goqu-dependent non-test files in `pkg/dotc1z` total
~8,150 lines, of which `to_pebble.go` (1,202) is retained as the converter;
`clone_sync.go` (368), `copy_isolate_sync.go` (244),
`rollback_expansion.go` (420) and `pool.go` (128) sit alongside. Expect the
write-side deletion to land near the RFC 0008 estimate of ~8,000 production
lines once the read floor in §4.7 is carved out, but treat that as a range and
re-derive it at implementation time rather than quoting it: `c1file_attached.go`
alone went from 462 lines to 188 in `8bd11917`, well after §8 was sized.

### 4.7 Step 6 — define the read-only floor and hold the line

"Retain internal SQLite read code solely for conversion" is not reachable, for
two reasons, and pretending otherwise will produce a scope surprise late.

It is not internal: `AsSQLiteStore` (`pkg/dotc1z/store.go:16`) and
`(*C1File).ToPebble` are reached from `pkg/synccompactor`
(`compactor_pebble.go:1068`), so the conversion reader stays exported unless
conversion itself moves.

It is not solely for conversion: the backend's file-connector ingest and its
uplift datasource reader both perform general read-only SQLite reads that no
conversion would repay (§3).

So the retained surface is *conversion plus read-only ingest*: the open/close
path in `c1file.go`, sync-run listing in `sync_runs.go`, the raw record list
paths in `grants.go` / `resources.go` / `entitlements.go` / `resouce_types.go`,
`sql_helpers.go`, and `to_pebble.go`. Name it explicitly, put a guard test on
it, and treat any new SQLite write reachable from it as a regression — the
alternative is that "conversion-only" quietly regrows into a second engine.

## 5. What this does not buy

Unchanged from RFC 0008 §8.2, and worth repeating because the guesses are
persistent. No dependency or vendor reduction: conversion reads SQLite, so
`modernc.org/sqlite` stays, and `goqu` stays for both the conversion queries
and `pkg/uhttp/dbcache.go`'s unrelated HTTP cache. No build simplification:
there are no SQLite build tags and no cgo requirement. No end to format
dispatch: `NewStore` must still sniff magic bytes to route v1 files to the
conversion reader, and the version-skew constraint is unaffected.

## 6. What we are not doing

**Converting in the upload RPC or a dedicated activity.** See §3. Inline at the
first writable open is both faster and amortized; the alternatives are strictly
worse.

**Converting on read-only opens.** Pays the cost where nothing recovers it, and
narrows a multi-sync file to one sync for a reader that could have seen all of
them.

**Removing the diff-sync read path.** The generator is gone; the ability to
read a legacy file that contains diff-sync rows is a compatibility obligation
with a pinned test, and it stays.

**Rejecting `sqlite` configuration before §4.2 lands.** The ordering is not
cosmetic: the hosted sanitize activity needs a SQLite writer until it doesn't.

## 7. Sequencing summary

Step 0 (SDK bump) gates everything. Step 1 (sanitize on Pebble) gates Steps 3
and 4, because rejecting SQLite configuration while a production activity
requires a SQLite writer is not possible. Step 2 (conversion policy) is
independent of Step 1 and should be decided early, since it is the only
irreversible decision in the plan. Steps 3 and 4 are ordered before Step 5, and
Step 5 is mechanical once they land. Step 6 is a standing constraint rather
than a step, and should be written as a test the day Step 5 starts.

Orthogonal to RFC 0008 steps 1–3, which concern lifecycle and gating inside the
Pebble stack. Strongly amplified by RFC 0008 step 4: once the protocol face is
a view over one engine, `connectorstore.Reader` and `Writer` stop being
dual-engine abstractions and become what they claim to be.

Constraints that apply throughout: a newer SDK must keep opening older `.c1z`
files, and the connector-facing API cannot change — there are hundreds of
connector repositories. Every step above is internal or configuration-facing by
construction; none changes a connector-visible signature.
