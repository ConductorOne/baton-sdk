# Round 6 — targeted spot review of the v8 addendum (scenario 7)

Scope: §9 scenario 7 (cells 7a/7b/7c + fix runs), §7 P6-R, §8
counterfactual-ghost note, §6 `sessionTaintWrites`/`sessionTaintAll` +
kill obligations, §10.5 additions, glossary entries "Elision" and
"Session taint" — per the §11 v8 change-order entry. Not a full round;
v1–v7 material out of scope except where the addendum destabilizes it.

Method: mechanical reachability walk of all cells and fix runs under
§3/§4/§5; hand-application of the fold (round-5 F1 pin), P2, P3′,
P6-A, and P6-R over every schedule the cells produce; coherence sweep
against rounds 3–5; verification of every shipped-system claim against
`source_cache_orchestration.go`, `session.proto`, `sessions.go`,
`pebble/session_store.go`, `session_server.go`, and the 6b plan.

Verdict: **REJECT — 4 majors + 5 minors + 3 notes, ALL
fix-without-re-review**; no round 7 warranted. Verified clean: all six
shipped-system claims true (no sessions × source-cache coupling;
sync_id-scoped namespaces; ungated session RPC during listings); every
cell and fix-run verdict re-derives as scripted under the recoverable
readings; 7c's green is entailed by policy purity, not assumed;
round-5 F1 and the torn-round boundary undisturbed; kill tables
internally consistent; "R violates no pinned obligation" consistent
with the verdict-as-data and trust-boundary framing.

Findings and dispositions (all applied as v9):

- **F1 (MAJOR)**: scenario 7 silently widened §1's one-row-kind
  abstraction — two kinds require two OPS for the sequential-phase
  premise (same-op W/R would batch together and interleave), plus
  per-kind produce/consume state, none declared. → KIND axis declared
  in §1 as an (op, scope) pair riding on an unchanged storage row-kind
  axis; small-scope table row added (2 kinds in scenario 7 only); §8
  note updated; taint state scoped by the axis.
- **F2 (MAJOR)**: P6-R's stamp-travel extension put traveled stamps
  inside P6-A's quantification domain, where two readings diverge on
  7b and the ⊥ stamp is ill-typed. → P6-A domain pinned: only stamps
  embedded by reads within the sealing sync; traveled and ⊥ stamps
  belong to P6-R; P6-A vacuously green on 7a/7b/7c.
- **F3 (MAJOR)**: taint durability unstated (5b/CO-6b-003 precedent
  requires pinning); the volatile-until-seal reading silently breaks
  the 7a kill obligation in interrupted histories. → pinned
  checkpoint-cadence (ingest-quality-style) in §5 and §6, with the
  self-healing argument (re-execution re-records) claimed explicitly —
  stronger than `compositionEnum`'s detection-evidence story.
- **F4 (MAJOR)**: "replay-capable kind's phase" ambiguous; the
  warm-install reading makes the 7a fix run unsatisfiable (sync N is
  cold, so no taint would record). → pinned: flow membership,
  independent of the recording attempt's warm/cold state; fix-run text
  states the taint records in a cold attempt by design.
- **F5 (minor)**: consume-side enforcement unspecified and "runs W
  cold" collides with §4's loud COLD verdict. → §3 `eLookup` extended
  (tainted kind → miss; degradation, never loud); §9.7 wording
  clarified.
- **F6 (minor)**: §7's P6-R scoping ("no mid-attempt mutation")
  admitted between-attempt mutation §8 declares unsound. → aligned to
  between-sync-only (single epoch per (sync, scope) per sync).
- **F7 (minor)**: counterfactual unpinned for multi-write producers;
  reader-timing independence implied, not stated. → pinned as the
  producer's phase-final value under empty-namespace evaluation;
  no-cross-op-spawn config constraint and timing independence stated.
- **F8 (minor)**: "divergent rows" cannot diverge in content under
  §3 (policies never choose content); ghost-only divergence forced but
  unstated. → stated in 7a plus a §8 under-approximation note (the
  real system has no content oracle for fresh rounds either — the
  finding survives the abstraction).
- **F9 (minor)**: 7a's "kind outside the replay flow" premise route
  is unmodeled. → struck; route pinned to "policy fetches fresh."
- **F10 (NOTE)**: shipped session surface includes
  Delete/DeleteMany/Clear; model reaches Get/Set only, newly
  load-bearing for the taint definition. → registered in §1; §6 WRITE
  defined as any mutating op.
- **F11 (NOTE)**: fix runs re-execute the full two-sync script with
  the toggle ON (first produce-side toggle family — acts a sync before
  the red verdict); was implied only. → stated in §9.7.
- **F12 (NOTE)**: §3's `eCopyScope` ghost tuple lacked the embedded
  session stamp P6-R requires to travel. → extended: lineage fields
  added, embedded stamps copied unchanged.

Also applied in the same v9 revision (signoff-discussion items, not
review findings): the capability-level OPT-OUT (attested
emission-irrelevance; a dishonest opt-out reproduces 7a/7b exactly)
and the out-of-model enforcement layers (static analyzer, pre-release
`SessionStoreUsage` conformance assert, runtime taint — one detector,
three timings).

Process observation (from the review, for the freeze record): v6 and
v8 share a pattern — new machinery arriving through scenario scripts
rather than through §1/§3/§5 first. Scenario cells were sound both
times; the undeclared machinery beneath them was where all four majors
lived.
