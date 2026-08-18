# RFC 0009: Sync artifact retention — preserve by default, discard on typed verdict

Status: proposed
Risk routing (REVIEW_CHECKLIST §2): error classification across the SDK ↔
hosted-runner seam — silent + durable (a wrong discard silently costs hours of
re-collection; a wrong preserve is bounded by existing resume budgets) with a
correctness-affecting version pair (SDK release × runner deploy). HIGH for the
seam contract; the retention inversion itself is a small, testable branch.
`BUG_CATCHING.md` §5.3 (host wrappers stripping status across the seam) applies
directly and its step-up governs verification.

## 1. Problem

When `syncer.Sync()` returns an error, the hosted runner decides whether to
commit the partial c1z (the "live" checkpoint artifact the next attempt resumes
from) or throw it away. Today that decision is an allowlist over the *stop
reason*:

1. `ErrSyncNotComplete` (run-duration expiry) → preserve, loop immediately.
2. Timeout-shaped errors (`context.DeadlineExceeded`, `codes.DeadlineExceeded`)
   → preserve.
3. Worker drain (stop channel) → preserve.
4. `IsSyncPreservable(err)` (a gRPC-code allowlist, feature-flag gated;
   recently enabled broadly) → preserve.
5. **Everything else → discard**, logged as "not persisting c1z to avoid
   potential corruption."

`IsSyncPreservable` (`pkg/sync/syncer.go:84`) keeps `DeadlineExceeded`,
`Unavailable`, `NotFound`, `PermissionDenied`, `ResourceExhausted`,
`FailedPrecondition`, `Aborted`, `Unauthenticated`, `OK`. It discards
`Canceled`, `Unknown`, `Internal`, and every plain (non-status) Go error.

Production cost, observed on a multi-day large-tenant sync: one worker recycle
during a deploy surfaced `context canceled` from in-flight connector calls,
fell through to branch 5, and rolled completed actions from ~222k back to
~122k — an hour-plus of collection re-done for an artifact that was never
inconsistent. During an earlier incident (RFC 0007), every failed attempt of a
looping sync skipped persist and resumed from zero for ~26 hours; discard
multiplied a scheduler bug into days of wasted work.

## 2. Diagnosis

### 2.1 Retention asks a question the error cannot answer

"Which error did the connector return?" cannot answer "is the artifact
consistent?" Consistency is a property of the store's commit protocol and is
invariant to the stop reason. The codebase already commits to this: `Close()`
finalizes the c1z on a context detached from the caller's cancellation
(`pkg/sync/syncer.go:3731`), checkpoints are crash-consistent by contract (the
crash harness kills the process mid-sync and requires clean resume), and the
pebble store refuses to destroy data even when its own save fails
(`pkg/dotc1z/pebble_store.go:1037-1052` leaves the unpacked DB on disk rather
than deleting the only copy). A context cancel is strictly gentler than the
SIGKILL these mechanisms already survive. There is no connector-side error —
transport failure, timeout, cancel, application bug, garbage response — that
can make a committed checkpoint inconsistent.

### 2.2 The default branch is the destructive one

An allowlist with default-discard means every *unclassified* error pays the
maximum cost. The allowlist has grown by incident (deadline-preservation was
added in mid-2026 after exactly this shape of loss) and will keep growing by
incident, because the space of error shapes is open — especially across the
lambda boundary, where old connectors serialize errors through several
generations of encoding.

### 2.3 Cancellation is systematically misclassified

The SDK's own shutdown mechanics guarantee that a stopping sync surfaces
`Canceled`-shaped errors:

- `parallelSync` propagates run-duration expiry to workers by *cancelling*
  `workerCtx` (`pkg/sync/parallel_syncer.go:140-143`); in-flight connector
  calls observe `ctx.Err() == context.Canceled` regardless of the cause.
- The worker loop treats any action error — including those cancels — as a
  fatal batch error and logs "cancelling context due to error in action"
  (`pkg/sync/parallel_syncer.go:745-750`), once per worker.
- `handleOperationError` (`pkg/sync/parallel_syncer.go:425-448`) rescues the
  batch error only when `context.Cause(runCtx)` is `DeadlineExceeded`,
  converting it to a checkpoint + `ErrSyncNotComplete`. An *external* cancel —
  activity teardown, deploy, heartbeat loss — returns bare `Canceled`, which
  is not preservable.

So the same physical event (stop a running sync) is preserved or discarded
depending on who owned the timer. The signal cannot distinguish "infra
recycled the worker" from anything else; a classification that cannot
distinguish its cases and defaults to destroying work is wrong even when every
line behaves as written.

The hosted path makes shape-based preservation not just fragile but
unimplementable: the runner's own invoke-error mapping (`mapInvokeError` in
the hosted connector client) replaces every non-timeout lambda invoke failure
with a *fresh* infra error — no `%w`, chain discarded — so even the `Canceled`
identity of an action error is stripped before the syncer sees it. Any policy
of the form "preserve when the error looks like X" is defeated by middleware
that both sides legitimately own.

### 2.4 String-matched seam contracts break under version skew

A recent incident is the cautionary case: a change that added sanitization
markers to lambda transport errors (#1048) silently broke a
`strings.Contains` capability gate that tolerated old connector lambdas
lacking `ListStaticEntitlements` — those syncs failed outright instead of
skipping the step. The fix moves the gate to a typed classification
(`codes.Unimplemented` / failure class); as of this writing that routing is
still on a branch, and the runner's main continues to string-match the
transport markers (which is why §4.4 freezes their text). The lesson
generalizes: **any
contract that crosses the connector↔SDK or SDK↔runner seam must be typed and
conformance-tested, never `err.Error()` text**, because either side may
re-wrap, sanitize, or re-classify independently of the other's deploy
schedule.

### 2.5 Discard is not even a working circuit breaker

The one theoretical benefit of discard — escaping a poisoned checkpoint that
deterministically re-fails on resume — does not materialize. The RFC 0007 loop
restarted from zero every ~45 minutes and failed identically, because the bug
was in code, not in the checkpoint. Meanwhile the runner already has explicit,
targeted breakers: a bounded activity retry policy, a hard reset of sync
status after ~15 attempts, and live-artifact invalidation after repeated
corrupted-artifact failures. Those mechanisms answer "stop resuming" directly;
retention-by-error-code answers it by accident, at the cost of §1.

## 3. Invariants

- **I1 — Retention is a storage question.** Whether a partial c1z is kept
  depends only on whether the store committed it cleanly. Connector errors
  never decide retention.
- **I2 — Discard is a positive, typed verdict.** An artifact is discarded only
  when a typed signal says it is unusable. No default branch discards.
- **I3 — The seam is typed.** Every signal crossing SDK↔runner is a sentinel
  error or typed value tested with `errors.Is`; nothing on the retention path
  matches `err.Error()` text. The connector↔SDK interface is untouched: old
  lambdas cannot emit new signals, so the design must not (and does not)
  require one.
- **I4 — Stop reason chooses control flow, not retention.** Cause
  classification (deadline vs. cancel vs. failure) selects loop / retry /
  terminate behavior only.

## 4. Design

### 4.1 SDK: name the discard verdict

Add one sentinel, following the `ErrIngestInvariantViolated` pattern
(`pkg/sync/ingest_invariants.go:126-133` — wraps without altering the
operator-facing message, consumed via `errors.Is`):

```go
// ErrArtifactUnusable classifies STORAGE VERDICTS — the local c1z may not
// reflect a clean commit — as distinct from connector or scheduling
// failures. It is the only signal that permits a runner to discard a
// partial sync artifact. Producers are storage-layer close/finalize
// failures, which reach a runner through Close() alone; connector errors
// never carry it. Test with errors.Is.
var ErrArtifactUnusable = errors.New("sync artifact unusable")

// ShouldDiscardSyncArtifact reports whether err carries the storage
// verdict. Everything else — including cancellation, timeouts, and all
// connector-side failures — preserves the artifact.
func ShouldDiscardSyncArtifact(err error) bool {
    return errors.Is(err, ErrArtifactUnusable)
}
```

Producers: the close/finalize paths that had mutations to commit but failed
before the output c1z was rewritten (e.g. a failed `save` in the pebble
store, a failed WAL checkpoint or `saveC1z` in the sqlite finalize). The
storage layer already distinguishes these — §2.1's save-failure path
deliberately preserves the on-disk data and returns an error; that error is
where the sentinel attaches.

**Change order (implementation):** the producer set narrowed to paths a
runner reaches through `Close()` alone. The mid-sync storage commits
(`EndSync`, `CheckpointSync`, `Cleanup`) — named as candidate producers in
an earlier draft of this section — return bare errors on purpose: they
mutate the *working store*, not the output c1z, so a failure there leaves
the previous artifact a faithful commit, and a verdict would instruct a
runner to discard a still-valid artifact. Consequence for the seam: a
`Sync()` error never carries the verdict, and both engines' post-save
teardown failures (cleanup, engine close) stay bare for the same reason.

**What "discard" means at the seam.** A verdict withholds the *commit*; it
never deletes a stored artifact. Both runner paths open the store on a local
copy of the currently stored c1z and commit that same path back, and both
engines save atomically — so a verdict means the local file was never
rewritten and is still byte-identical to what was downloaded. Skipping the
commit therefore leaves the stored artifact intact as the next attempt's
resume point, and committing it would have been a content no-op. A verdict
cannot cost an earlier attempt's progress; it only declines to overwrite a
good stored copy with a file that does not represent this run.

`IsSyncPreservable` stays exported and **frozen** (deprecated in doc comment).
Changing its behavior in place would ship a silent policy change to every
runner that still calls it — the exact version-skew hazard of §2.4. Runners
migrate by switching call sites in the same change that vendors the new SDK.

### 4.2 SDK: stop laundering shutdown into failure

Two mechanical fixes in `pkg/sync/parallel_syncer.go`. Both are
**side-effect-only**: the error `Sync()` returns on every path is
byte-for-byte what it returns today.

1. **Worker loop** (~line 745): when an action error arrives and the run is
   stopping, the worker is observing shutdown, not discovering a failure.
   Suppress the "cancelling context due to error in action" error log for
   that case, keeping genuine action failures as the only producers of the
   line. The test is the **pre-batch** context, not the batch's own
   cancel-cause context: the latter is canceled by the first failing sibling,
   so keying on it would swallow the line for a second worker's independent
   genuine failure.    This is *only* log suppression: the re-cancel is already a
   no-op (`WithCancelCause` keeps the first cause), and `queue.abort()` **must
   stay** on every exit — it promptly releases workers blocked in
   `queue.next()` and gates post-failure `transition()` commits (without it,
   exit waits for stragglers to drain and late transitions keep admitting
   children onto a canceled batch).
2. **Exit paths**: the stop exits force a checkpoint only for
   `DeadlineExceeded` causes today. Extend the same best-effort checkpoint to
   cancel causes, on a detached bounded context (`context.WithoutCancel` +
   timeout, the established finalize pattern from `Close()` and `dotc1z`), so
   the local file is as fresh as possible at every stop. The checkpoint is a
   new side effect only; the returned error is exactly today's — the batch
   error on the batch path (the runner's activity-log suppression
   string-matches text inside it), the cancel cause at the loop top, and a
   checkpoint failure on this new path is logged, not joined into the return.
   `ErrSyncNotComplete` stays reserved for run-duration expiry so runners keep
   their loop-immediately behavior (I4).

   **Change order (implementation):** there are **three** such exits, not two.
   Beyond `handleOperationError` and the `runCtx.Done()` branch, the loop-top
   *periodic* checkpoint runs on the caller's context, so a cancel can surface
   there as a checkpoint failure that returns before either of the other two.
   It takes the same rescue, guarded on `ctx.Err() != nil` so a genuine store
   failure with a live caller is not retried on a detached context.
   **Change order (review): this exit now has a dedicated deterministic
   instrument.** The first version of this order dispositioned it as
   defense-in-depth without one, reasoning that determinism required a
   production-visible after-action hook to force the "cancel lands as an op
   completes" interleaving through the full `Sync()` stack. Review pointed
   out a cheaper shape with no production surface: invoke `parallelSync`
   directly — an established pattern in this suite — with a pre-canceled
   caller context and a store stub that fails `CheckpointSync` exactly when
   the caller's context is done. The loop's first periodic checkpoint then
   fails deterministically, and the stub's recorded outcomes pin both the
   rescue (a second, detached, successful write) and the guard direction (a
   genuine store failure with a live caller gets exactly one write — no
   detached retry).    The one-shot `InitOp` checkpoints deliberately get no rescue:
   nothing is in the store yet worth a stop write, and the next attempt
   rebuilds the same plan. The supports-diff marker write at the fresh
   entry to grant expansion is likewise excluded: a metadata-only write
   for the unused diff-sync feature, with no progress since the loop-top
   checkpoint. The rescue pattern stays scoped to checkpoint failures. The detached window is bounded by a
   **stop-specific** timeout (~1 minute for one sync-token write), not
   `FinalizeTimeout` — it is caller-uninterruptible and sits in front of
   `Sync()` returning, so it must not consume the drain grace that `Close()`
   needs to pack and commit the c1z.

   **Change order (review): a fourth candidate exit was examined and
   deliberately left without a rescue.** After `parallelSync` returns
   cleanly, `Sync()` forces a checkpoint that clears completed actions and
   the entitlement graph from the token; a cancel landing there fails that
   write and returns. A rescue at that point would be actively harmful: the
   plan is already cleared from state, so the only token a detached write
   could produce is the *empty* one, and a resume from an empty token pushes
   `InitOp` and re-runs the whole collection. Failing without a write leaves
   the last mid-plan periodic token in the store, which resumes by re-running
   only the final batch. The rescue set is the three stop exits inside
   `parallelSync`, where the token still describes in-progress work.

### 4.3 Runner: invert the branch

The hosted runner's post-`Sync()` decision becomes:

```
err := syncer.Sync(ctx)
closeErr := closer.Close(ctx)          // detached finalize inside
joined := errors.Join(err, closeErr)

if sdkSync.ShouldDiscardSyncArtifact(joined) {
    // typed storage verdict: skip commit; existing invalidation and
    // budgets take over
} else {
    commit the live artifact           // always, on the detached finalize ctx
}

// control flow only (I4):
//   ErrSyncNotComplete            -> loop immediately (as today)
//   anything else                 -> return err; activity retry policy,
//                                    attempt budgets, and non-retryable
//                                    classification behave as today
```

The existing budgets — bounded activity retries, the ~15-attempt hard reset,
live-artifact invalidation after repeated corrupt-open failures — are
unchanged and are the explicit answer to poisoned checkpoints (§2.5).
Retryability classification (`ErrIngestInvariantViolated` →
non-retryable) is likewise unchanged and orthogonal: an invariant verdict
stops the retry loop but does not discard the artifact, which is consistent —
the artifact is never sealed, and preserving it is free.

**The runner has two retention call sites, and the change owns both.** The
full-sync activity's decision tree (feature-flag-gated `IsSyncPreservable`
branch) and the incremental-sync path — which calls `IsSyncPreservable`
*ungated* and maps "preservable" to a nil activity error. Migrating one and
not the other leaves two classifiers over one domain with no stated relation
(§5.3 coherence); both flip to `ShouldDiscardSyncArtifact` in the same PR.
The grant-expansion path's `ErrSyncNotComplete` handling is control flow, not
retention, and is untouched (I4).

**Change order (implementation): the retention obligation is per exit, not
per call site.** Counting *sync-error* branches undercounts the work — a
failed `Close()` after an otherwise fine sync is also a retention decision,
and each such exit returned without committing. Migrating only the sync-error
branches would leave the same obligation discharged by some exits and not
others (§5.9). The exits that now preserve unless the verdict is present:
the full-sync error branch and its post-success close failure, and the
incremental error branch, its post-success close failure, and the
grants-only increment's close failure. A close failure that does *not* carry
the verdict means the c1z is a faithful commit — for the post-success exits,
of a *completed* sync, the most valuable checkpoint there is.

**Change order (implementation): preserve-on-cancel requires a detached
commit.** The full-sync path's upload already runs on a detached context, so
it survives a canceled activity ctx; the incremental path's commit ran on the
raw ctx. Left alone, every incremental preserve-on-cancel — worker drain,
deploy, operator stop, i.e. exactly the cases this RFC exists for — would
have failed with "context canceled" and lost the checkpoint, while the
returned error became the upload failure instead of the stop reason. All
incremental commits (including the success path, where a drain mid-upload
would otherwise discard a completed increment) now go through one helper that
detaches. Durability of the commit is part of the retention contract, not an
incidental property of whichever context the call site happened to hold.
Coverage: the detachment property is carried by a doc comment on the helper,
not by a test — pinning it needs the runstore's package-private fake
(blockstore + fake remote + crypto + activity env), which is not reachable
from the activity packages. This matches how the full-sync path's identical
property is documented on `main`. If it regresses, it regresses silently, so
the helper is the single place either path can commit from.

**No kill switch (decided at implementation).** An earlier draft gated the
inversion behind a hosted-side feature flag whose off-state reproduced the
old tree. Dropped, deliberately: the marginal risk the flag would guard is
already in production — the hosted gate that selects preservation today is
enabled fleet-wide, so the fleet already preserves the whole
`IsSyncPreservable` allowlist, and preserve-by-default only widens that set.
Poisoned-checkpoint escape was never the flag's job; it is the budgets'
(bounded retries, hard reset, corrupt-open invalidation), which apply
identically to both sets, and the
worst case without a flag — treadmill until budgets fire, then restart from
zero — is exactly today's behavior on discard. The one genuinely new failure
(a broken verdict committing a stale artifact) is pinned by the SDK
conformance suite and still only regresses to an older checkpoint. Against
that, the flag's cost was structural: two decision trees living side by side
until a cleanup that historically never ships. So the legacy tree is
*deleted*, not gated, and the old gate is retired in the same PR: after this
change there is no feature flag on either side of the retention decision. Its
only consumer was the deleted branch — `IsSyncPreservable` survives ungated as
the incremental path's control-flow classifier, with the superset relation
stated in code. The rollback lever is a one-commit revert of the runner PR.

### 4.4 Compatibility across the three interfaces

**Connector lambda ↔ SDK (frozen, and skewed in both directions forever).**
No new signal. Retention stops reading connector errors entirely, which
*shrinks* the behavior surface that depends on old-connector error encodings
instead of growing it. Old lambdas keep returning whatever serialized status
or legacy payloads they return today; the worst any of them can cause is a
preserved artifact and a retry, both bounded.

The reverse direction — a connector rebuilt against the new tag, invoked by
an old host — is equally inert, and unlike the runner window it can never be
sequenced: the tag is public and connector adoption is uncontrolled, so
old-host × new-connector is a permanent skew pair, not a rollout phase.
Verified surfaces:

- The strings the hosted runner matches (`lambda_transport:`, `logSummary:`)
  are stamped by the *invoking client* in the host's vendored SDK
  (`pkg/lambda/grpc/client.go`, `failure.go` — whose doc comment names the
  downstream sanitizers, and whose failure tests pin the markers). A
  connector build cannot alter them regardless of its SDK version.
- The changed code does not execute in a served connector: neither
  `pkg/lambda` nor `pkg/connectorbuilder` imports `pkg/sync`; a lambda
  connector serves RPCs and its errors cross the seam as serialized gRPC
  status in the response frame, which this design does not touch.
- A service-mode connector on the new tag *does* run the §4.2 syncer changes
  locally, but everything it reports upstream is text-insensitive at the
  platform (`FinishTask` does no message matching) and code-preserving
  (`%w`); it uploads only on success and deletes the local partial on error,
  both unchanged. The only new behavior is a best-effort local checkpoint
  that its own handler deletes moments later — waste, not a behavior change.

Connector-side obligations are therefore standing seam contracts, not merge
criteria: no SDK release may change connector-side response/error
serialization across the lambda seam without treating it as a breaking seam
change (the both-sides conformance suite in §5 is the enforcement).

**SDK ↔ runner (the one new contract).** The typed set crossing the seam is
small and closed: `ErrSyncNotComplete` (loop), `ErrArtifactUnusable` +
`ShouldDiscardSyncArtifact` (retention), `ErrIngestInvariantViolated`
(retryability, pre-existing). Conformance tests in the SDK pin the wrap shapes
each runner relies on (the `full_sync_classify_test.go` /
`TestIsSyncPreservable` pattern: bare, `%w`-wrapped, `errors.Join`ed with a
close error), so a refactor that breaks `errors.Is` traversal fails in the SDK
repo before it ships.

**Service-mode connectors (verified out of scope).** Connectors running as a
service execute their sync locally via `pkg/tasks/c1api` and upload the c1z
**only after a successful sync**; on any error the local partial is deleted
and only the error (as a gRPC status) reaches the platform. The platform
therefore never holds a partial artifact from a service-mode connector —
there is no server-side retention decision for them, and nothing in this
design changes what they run or report. `IsSyncPreservable` has no callers in
this repo; every retention consumer lives in the hosted runner. Service-mode
connectors gain local preservation only via the §4.5 follow-up, on their own
update cadence, orthogonally to this rollout.

**Version skew.**

| Runner | SDK | Behavior |
|---|---|---|
| old | new | Runner calls frozen `IsSyncPreservable`; behavior identical to today. |
| new | old | `ShouldDiscardSyncArtifact` unavailable → the runner's vendored SDK defines it; a runner is always built against exactly one SDK, so this pair cannot occur at runtime. The runtime skew pair is connector-vs-host, covered above. |
| new | new | Preserve by default. |

**Sequencing: SDK releases land first, and stay inert.** Several SDK versions
may be vendored into the hosted runner before the runner's branch inversion
lands. That window is safe if and only if the SDK holds three properties,
which are release obligations, not incidental facts:

1. **Additive only.** `ErrArtifactUnusable` and `ShouldDiscardSyncArtifact`
   are dead code until a runner calls them. Attaching the sentinel to storage
   failures must use `%w`/`errors.Join` so existing `errors.Is` /
   `status.FromError` traversal is unchanged — the obligation is
   **invariance**: every error classifies under the old branch exactly as it
   did before the sentinel attached. For most close/finalize failures that is
   plain → not preservable → discard, the correct outcome for a storage
   verdict. The one deadline-shaped producer (a finalize that outlives
   `FinalizeTimeout` wraps `DeadlineExceeded`) was preservable before and
   stays preservable: the old runner keeps the previous on-disk artifact,
   which the atomic save left untouched — stale but faithful, harmless.
2. **Frozen error surface.** The old runner's branches key on `Sync()`'s
   returned shapes *and*, on the hosted path, on specific error text. The
   full frozen set, verified against the runner's current main:
   - `ErrSyncNotComplete` via `errors.Is` — run-duration expiry (preserve +
     loop, no retry consumed).
   - `ErrTooManyWarnings` via `errors.Is` — in the frozen
     `IsSyncPreservable` allowlist; the warning-budget exit must keep
     wrapping it with `%w`.
   - `DeadlineExceeded` (`errors.Is` and gRPC code) — timeout preservation.
   - `Canceled` visible at the loop-top/cause paths; connector status codes
     passing through unaltered on the batch path.
   - **Lambda transport marker text** `lambda_transport:` and `logSummary:` —
     the runner's invoke-error mapping on main *string-matches these markers*
     to classify infra failures (the typed failure-class routing that
     replaces this is not yet on main). Rewording them re-runs the §2.4
     incident verbatim. This text freeze lifts only when the typed routing
     lands in the runner.

   Any SDK release in the window that changes one of these silently changes
   hosted retention, retry-budget accounting, or infra-error classification
   under an unchanged consumer. The conformance suite (§5) pins all of them
   so a violation fails in this repo before release.
3. **Behavior-neutral mechanical fixes.** The §4.2 changes must not alter
   `Sync()`'s returned errors on any path (they are specified as
   side-effect-only, including *which* error the batch path returns — the
   batch error, not the cancel cause). Under the old runner they only make
   the local file fresher (invisible, since the old branch discards on
   cancel anyway) and remove misleading logs.

One tempting shortcut is rejected: mapping external cancels to
`ErrSyncNotComplete` would make the *old* runner preserve on cancel today,
but `ErrSyncNotComplete` returns success-incomplete — a cancel storm would
loop the workflow without consuming any retry budget. Cancels stay errors;
preservation for them arrives only with the runner's branch inversion.

**Merge criteria (the landing gate).** The runner vendors the SDK in-tree, so
the runner-side change is mechanically one commit: dependency bump + vendor +
both retention call sites + old-flag retirement. The gate exploits that:

1. The runner PR is written and approved **before** the SDK release is
   tagged, built against the release candidate commit, with both call sites
   flipped and the runner-side retention seam test (the §5 property test at
   the shapes the runner actually sees) green. The rollback lever is a
   one-commit revert of this PR (§4.3, no kill switch).
2. The SDK tag is cut only once (1) holds; the runner PR switches to the tag
   and merges immediately after. The window between tag and runner deploy is
   thereby minutes-to-hours by policy.
3. The window is *shortened* by (1)–(2) but **not structurally closed**: the
   tag is public and anyone can vendor it into the runner for unrelated
   reasons before the retention PR lands. Obligations 1–3 above therefore
   bind the tagged release itself — they are what make the window safe, and
   the tandem landing is what makes it short. Neither substitutes for the
   other. The tag also reaches **connector builds** immediately (lambda and
   service-mode), a consumer that can never be sequenced; that cell is
   dispositioned in the connector paragraph above — safe because the design
   adds nothing connector-side, enforced as a standing seam contract rather
   than a rollout gate.
4. Deploy-transient skew (old and new activity pods serving attempts of the
   same sync during rollout, and rollback from new to old) is safe by
   per-attempt independence: retention is decided per attempt over an
   unchanged artifact format, either version reads artifacts the other
   committed, and poisoned-artifact escape remains the budgets' job.
5. Out-of-repo operational dependencies are a named checklist item, not a
   code gate: dashboards or monitors keyed on the "not persisting c1z" log
   line change meaning when the branch inverts, and the misleading
   "cancelling context due to error in action" burst disappears at every
   stop. Both are reviewed at rollout.

### 4.5 Alignment: the SDK's own runner

`pkg/tasks/c1api/full_sync.go:321-330` deletes the partial c1z unconditionally
on any sync error (it predates all of this; only the "spare" replay artifact
survives). It should adopt the same helper: keep the partial unless
`ShouldDiscardSyncArtifact`, and let its existing spare-promotion logic prefer
the fresher artifact. This is a follow-up in the same series, not a blocker
for the hosted change.

### 4.6 Out of scope, named: mid-window durability

The live artifact is committed to the object store once per attempt, at exit.
A hard pod kill (no drain signal, no exit path) still loses up to a full run
window of *local* checkpoints regardless of retention policy. The levers are
periodic mid-window live commits or shorter run windows; either is its own
change with its own cost model (upload bandwidth × 4.8 GB artifacts) and is
deliberately not bundled here.

## 5. Verification

Per the step-up (silent + durable + version pair):

- **Conformance suite (both sides of the seam).** Every sentinel × wrap-shape
  combination the production paths produce; a planted `errors.Join` /
  sanitization change must fail these before it ships (negative control
  reproducing the §2.4 incident shape).
- **Frozen-surface pin for `Sync()` and the lambda transport.** Direct tests
  that run-duration expiry returns `ErrSyncNotComplete`, the warning-budget
  exit wraps `ErrTooManyWarnings` with `%w`, an external cancel surfaces as
  `context.Canceled` / `codes.Canceled` at the cause paths, connector status
  codes pass through unaltered on the batch path, and lambda invoke failures
  carry the `lambda_transport:` / `logSummary:` markers. These are the shapes
  and strings the *old* runner branches on during the SDK-first window (§4.4
  sequencing); this pin makes an accidental reshaping fail in this repo
  instead of shipping as a silent hosted retention or infra-classification
  change. The marker pin carries a comment naming its consumer and lifts when
  the runner's typed failure-class routing lands.
- **Retention property test.** A fault-injecting connector produces the full
  error taxonomy (status codes, plain errors, cancels, timeouts, lambda
  transport strings); the oracle asserts `retention decision ==
  presence of ErrArtifactUnusable`, never a function of the connector error.
  The coherence pin against the frozen classifier states its exception
  explicitly: a verdict can wrap an old-policy-*preservable* cause (a
  finalize that outlives `FinalizeTimeout` wraps `DeadlineExceeded`), and
  the verdict wins — discard classifies the artifact, not the cause, and a
  carried verdict means the artifact holds no progress worth preserving.
- **Cancellation chaos.** Extend `chaos_cancellation_test.go`: cancel with a
  connector call blocked in flight; assert the cancel surfaces in a frozen
  shape, no "cancelling context due to error in action" error is logged for
  shutdown-shaped exits, a detached stop checkpoint runs *after* the cancel
  signal, `Sync()` returns boundedly, and the artifact reopens and
  cold-resumes to the baseline content. (An earlier draft promised a
  checkpoint-*age* assertion against the §4.2 bound; the bound is a timeout
  on the rescue write, not an age guarantee, so the implemented oracle is
  the pair that matters: a post-cancel checkpoint happened, and the resumed
  sync converges.) Run the case at a multi-worker count: siblings that
  exhausted the queue park in `queue.next()` while the blocked call is
  outstanding, so the bounded-return assertion also pins pool drain.
  (Change order, review: an earlier draft also promised a worker-count-1
  cell; dropped — the multi-worker count is the dimension that buys
  something here, and a single-worker run exercises nothing the other
  cancellation chaos tests don't already cover.) On `queue.abort()` itself,
  the accurate claim is promptness and post-failure hygiene, not deadlock:
  `queue.done()` precedes the error return, so waiters wake when stragglers
  drain even without it — but
  removing it would delay exit and let post-failure transitions keep
  admitting children onto a canceled batch. It must stay on every exit.
- **Storage verdict injection.** Force a save/commit failure; assert the
  sentinel is carried through `Sync()`/`Close()` wrapping and the runner-side
  branch discards — the only path that may.

## 6. What this deliberately does not do

- **No new connector-side signal.** Old lambdas are frozen; the design removes
  a dependency on their error shapes rather than adding one.
- **No in-place widening of `IsSyncPreservable`.** That ships a silent policy
  change to unmigrated callers — the §2.4 hazard in miniature.
- **No discard for "permanent" connector errors.** Loop prevention is
  retryability's job (`ErrTaskNonRetryable`), poisoned-checkpoint escape is
  the budgets' job; retention does neither.
- **No attempt to detect "a human canceled this sync."** No such signal exists
  on the full-sync path today, and retention must not guess at one; if a
  product-level "cancel and forget" appears later, it arrives as an explicit
  typed instruction to the runner, not an inference from `context.Canceled`.
