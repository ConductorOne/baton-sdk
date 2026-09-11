# Review Checklist

The operational index for `docs/BUG_CATCHING.md` (the handbook). Load this page
in every review conversation; consult the handbook by section — §6's slice map
names which sections each pass agent receives. This page is derived and
deliberately terse: when it and the handbook disagree, the handbook wins. Any
handbook change to the pipeline, passes, ladder, or principle list updates this
page in the same commit.

## Route the change (§2)

Risk = escape × consequence; score the failure mode, not the subsystem.

- Escape axes: silence (what makes noise?), durability (outlives the
  process?), uncontrolled dimensions (schedule, crash timing, version pair,
  scale), consumer distance (next line → another repo → a future SDK).
- Consequence, as remediation cost: redeploy < re-sync < migrate <
  coordinate < irrecoverable.
- HIGH verdicts: silent + durable; correctness-affecting version-pair
  dependence; remediation rung ≥ 4 (defense flips from detection to
  prevention); otherwise two escape yeses. Default-path exposure outranks
  option-gated.
- On declared hot paths the cost curve is a correctness property: state the
  big-O delta and point at the enforcing benchmark, or state no-change.

## Step-up pipeline (§2 policy, §6)

For HIGH changes and silent/combinatorial/no-single-run-oracle subsystems:

frozen behavioral plan → implementation-obligation addendum → instruments →
mutation adequacy → execution → structural-coverage triage → independent
evidence audit → focused implementation review → repository gates → signoff.

Hard rules along the way:

- Plan committed and frozen before implementation inspection; post-freeze
  changes are versioned change orders, each re-routed through the risk model.
- Criterion states: not assessed / evidence incomplete / verified to stated
  coverage / failed / explicitly excluded / deferred to a named stage.
  "Accounted for" is not closure; never claim closure from sampling.
- Validate every instrument: premise proven, oracle fails on a planted
  violation.
- Structural-coverage triage: disposition every uncovered changed branch as
  dead code, a missing obligation, or a missing instrument.
- Closure is a rate: the final-code decorrelated read and independent evidence
  audit produce zero new findings, plus a clean soak where a model-based or
  randomized instrument exists. Any post-fix change restarts the clock.
- Review budget: two, at most three rounds; then switch instruments.
- Failing evidence first for every confirmed bug. A recurrence of a documented
  class ships the §4 ladder climb, not just the patch.

## The seven passes (§3) — select by risk, record omissions

1. Edge cases — empty/nil, first-run vs. incremental, deleted referents,
   duplicates, partial vs. full sync.
2. Checkpointing & resuming — every stop point consistent or regenerable,
   resumable twice; the resumer is a different process/version with a cold
   cache.
3. Systematic permutation coverage — name the dimensions, form the
   cross-product, account for every cell; bugs live in unwritten cells.
4. Error handling, classification & budgets — every error in a declared
   category, every category's recovery obligation discharged and injectable.
5. Invariant & verification gating — every write path passes the invariant
   seam; bypasses are guilty until proven registered.
6. Performance — per-iteration cost at whale scale, failure-path cost,
   cost-contract deliverable on hot paths.
7. Concurrency — TOCTOU, duplicate writers, goroutine lifecycle; data races
   belong to the race detector.

## The instruments ladder (§4) — climb as high as proportionate

1. Type system / API shape — make the invalid state unrepresentable.
2. Fuzzing / property tests with an oracle — including the stateful executable
   reference model for lifecycle interleavings.
3. One validator over many point tests — production seam, ride-along fixture
   checker, or positive-evidence ledger.
4. Integration tests over the real store lifecycle (resume, seal, fold,
   reuse), not mocks.
5. Unit tests for logic-dense pure functions.

## Commit points (§5.12) — six answers per site

Executable failure · failure re-converges derived state · success reaches the
checkpoint · partial progress accounted and retryable · released exactly once
· durability class justified by a named fence.

## Syncer structure (`pkg/sync`)

Structural rules for `syncer`. They exist because two feature branches grew it
from 57 fields to 81, from 3 test hooks to 11, and from 13 capability type
assertions to 54 — each feature got a file, but every function became a method
on `*syncer` and every value a new field. Reject on these, not on taste.

- A feature adds a runtime struct with its own file and its own receiver,
  owned by `syncer` as exactly one field. It does not add fields to `syncer`.
  "One field that is a bag of eleven" does not satisfy this.
- Store capabilities — anything `pkg/sync` discovers on the store or its
  sub-stores by optional interface, whether that interface is exported from
  `c1zstore`/`dotc1z`/`connectorstore`, declared locally, or written inline as
  an anonymous `interface{...}` in the assertion — are resolved once,
  at attach, into `storeCaps` (`store_caps.go`). Attach means `setStore`, the
  only writer of `syncer.store` — a store attached without its caps does not
  fail, it silently runs the SQLite paths on a Pebble store. The check is
  `rg -nU '\.store\s*=[^=]|syncer\{[^}]*\bstore:' pkg/sync`: one hit, in
  `setStore`. It sees any receiver name and multi-line keyed literals; it
  does not see positional literals, a `store:` key after a nested `{}` in the
  same literal, or writes through `&s.store` — read those by eye. The syncer
  builds its expansion adapter through `s.expanderStore()`, never a literal.
  Package-level entry points that
  receive a bare store (`GraphFromStore`, `runIngestInvariants`,
  `NewExpanderStore`) resolve at entry through the same functions. No
  `x.(Interface)` on a store at a use site in `pkg/sync`; capability
  resolution belongs in `store_caps.go`. A use-site assertion hides how many
  capabilities the syncer depends on and makes each new dependency a two-line
  change nobody reviews as a dependency. Out of scope for this rule:
  - `pkg/sync/expand` widens its own `ExpanderStore` parameter by assertion.
    That is the expander's interface design, not store capability discovery.
  - Engine identity, as distinct from a capability: `enginepkg.AsEngine(...)`
    in `syncer.go` asks which engine wrote an artifact, and answers it through
    a helper in `pkg/dotc1z/engine/pebble` that owns the
    `w.(engineAccessor)` assertion. The question is "is this a Pebble file",
    not "does this store implement operation X", and its answer does not
    select a fallback path the way a nil `storeCaps` field does.
- Test seams live in `syncTestHooks` (`hooks.go`) and nowhere else, reached as
  `s.testHooks.x`. A seam stays nil in production and no production path may
  set one. The struct name carries the "test" marking, so its fields do not:
  apart from the `syncer.testHooks` anchor itself, any `test`-prefixed field
  in `pkg/sync` is by itself the finding.
- `runState` (`run_state.go`) is the resumable action stack plus the sync
  facts a resume needs (`sync_facts.go`); nothing else goes on it. Run
  accounting is `runStats` (`run_stats.go`) and the entitlement graph is
  `expansionGraph` (`expansion_graph.go`), each its own field on `syncer`.
  `runState` and `runStats` each carry a mutex; `syncFacts` has none because
  it lives inside `runState` and `runState.mu` guards it, and
  `expansionGraph` has none because the graph it hands out is mutated
  outside any lock anyway — a holder that skips the mutex says why in its
  doc comment. `marshalToken` is the only caller that holds two, `run`
  before `stats`; a second such caller takes the same order.
  `TestRunStatsLockOrder` checks both halves against the package AST,
  because the race detector has no lock-order analysis and an inverted
  caller would only hang, and only when it interleaved. It sees a function
  that takes both mutexes directly and one that takes one and calls the
  other holder's own locking method; a hop through a method on some other
  type, `*syncer` included, is not followed, and its header comment says so.
  CXE-1356 split the old `state` grab-bag into these three; put
  new state on the one whose lifetime it shares, or in a new type, rather than
  back onto `runState`.
- Configuration is immutable after `NewSyncer`. Anything mutated during
  `Sync` is not configuration and does not belong in `syncConfig` — for
  something that resumes across process boundaries, the immutable/mutable
  line is a correctness property, not bookkeeping. The check is
  `rg -nU '\.cfg\.\w+\s*=[^=]|syncer\{[^}]*\bcfg:' pkg/sync --glob '!*_test.go'`:
  every hit is inside a `With*` option or `NewSyncer`. Same blind spots as
  the store check above.

## Principle index (§5)

5.1 durable claims are ordered after the facts they claim · 5.2 every exit
path discharges the same obligations · 5.3 classification is a contract ·
5.4 stored state has a lifecycle contract · 5.5 bytes crossing a boundary are
hostile · 5.6 distribution invalidates local intuitions · 5.7 budgets bound
work only if the plumbing connects them · 5.8 unfinished transitions rot ·
5.9 an obligation owned by every path is owned by none · 5.10 the environment
is an input · 5.11 derived state is a proof · 5.12 a commit point is a
contract.
