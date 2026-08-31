# Model spec adversarial review — round 1

Reviewer: independent agent (decorrelated from the spec author's session
context; formed its view from the brief, the frozen 6b contract, and the
code). Verdict: **REJECT** — 2 blockers, 8 majors, 3 minors, 2 notes.
Spec revision v2 addresses every finding; dispositions below. The full
review text is preserved in the review agent's transcript; this file
records the findings and their dispositions for the freeze record.

| # | severity | finding (condensed) | disposition in spec v2 |
|---|---|---|---|
| 1 | BLOCKER | Restart-from-root does not fall out: the spec had workers return per-page continuations applied to the stack, so loop-top checkpoints could capture mid-chain cursors — a resume granularity hard crashes don't have (CO-6b-002). | §3 MWorker now owns an action's entire page chain (mirrors `syncOneAction`); stack entries keep root tokens until finish; loop-top checkpoints therefore contain only root tokens. Graceful-stop checkpoints (new, finding 3) are the one place mid-chain state becomes durable, and CO-6b-002's "unreachable in practice" claim becomes an explicit conformance question (§9 C1). |
| 2 | BLOCKER | P1 monitor vacuous: the "declared composition log" was the announced store ops, whose fold trivially equals the partition; the attestation clause was undefined and cannot be defined from the wire vocabulary (legit delta overlay and phantom union are wire-identical). | §7 P1 rewritten: declared composition = connector-intent ghost provenance per page (verdict class + consult epoch, an honest label of the policy decision that already drives emissions); contractual fold (one replacement per scope per sync, overlays compose only with this-sync replay); attestation = published validator's epoch equals the fold result's epoch. Honesty argument added; §10.5 mutation-adequacy check extended. |
| 3 | MAJOR | Graceful-stop forced checkpoints missing (run-expiry / external cancel force-checkpoint an aborted batch's mid-flight state — hits, spawns, mid-chain cursors); spawn admission timing wrong (same-op spawns drain in the live batch, so pending carriers arise from batch-cap splitting, not batch-end admission). | §3: eStop interruption mode added with forced checkpoint of live state; same-op spawns admitted to the live batch queue (drained in-batch), cross-op spawns to the stack; §9 case 1's pending-carrier premise re-scripted on batch-cap splitting (CO-6b-006's construction). |
| 4 | MAJOR | Replay-blocked flag given op-commit durability the real mechanism lacks (rides ingest-quality state, checkpoint-cadence durable); composition-enum detection modeled store-side/omniscient masks two real windows. | §5 table corrected (checkpoint-cadence durability); §4 deliverable-3 semantics remodeled as syncer-side detection over volatile/checkpointed state (weak variant is the default fix-run configuration); §10.6 obligation extended to detection-state visibility and mark durability. |
| 5 | MAJOR | Produce-side blocking condition broader than contract ("any gate-outcome difference" vs the two real triggers) — over-blocking masks multi-sync propagation. | §3 restated to exactly: (i) compat key recomputed differently across attempts (B4); (ii) attempt without source-cache handling over prior-attempt produce state (CO-6b-003), fail-closed on read error. Consume-side degradation alone never blocks. |
| 6 | MAJOR | P3′ strengthening false for multi-page fetches under mid-attempt mutation (crash-free walks observe multiple instants per scope). | §7 P3′ scoped: checked only in scenarios without mid-attempt mutation; mid-attempt-mutation scenarios rely on P1/P2; per-page refinement noted as future work. |
| 7 | MAJOR | MEnv had no attempt-failure behavior; neither P4 hang shape (deterministic cold re-failure; leaked-lock deadlock) was representable; livelock undetectable in a bounded scenario via raw liveness. | §3 MEnv resume-on-failure rule added; §4 warm-verdict page failure + in-attempt retry added (lock scenario); §7 P4 livelock detection restated as a safety rule (two consecutive resumes failing from identical restored state with identical verdict). |
| 8 | MAJOR | Crash-vs-in-flight race underspecified for multiple outstanding ops (non-prefix-closed committed sets possible); "queue drained" ambiguous. | §5 pinned: eCrash's position in MStore's queue partitions dead-attempt ops — before = committed, after = dropped; per-sender FIFO preserves per-worker prefix closure; quiesce = drop, never process. |
| 9 | MAJOR | Hit-recording semantics unpinned (record-at-lookup-hit regardless of revalidation outcome; last-write-wins; P2's "validated" = upstream match, not lookup hit) — case 3 depends on all three. | §3/§7 pinned with code citations; §9 case 3's "also exhibits with no re-consult" scoped to `hitValidatorBinding` off. |
| 10 | MAJOR | Carrier-less phantom variant missing: copy commits durably mid-batch, crash before checkpoint → durable debris with no durable replayed mark; attempt-2 fresh fetch upserts over it. More reachable than the scripted variant; defeats syncer-side composition detection. | §9 case 1 gains the carrier-less variant as a first-class cell; flagged as the priority trace to compile into a chaos scenario (deliverable 6). |
| 11 | MINOR | §2.1 vs §3.3 disagreement on batch composition as a choice point; cap-shrinking argument implicit. | §3: batch cap nondeterministic ∈ {1..capMax} per iteration; small-scope scaling argument stated (CO-6b-006's 102-action construction is the existence proof). |
| 12 | MINOR | P6-A fired on any later-attempt divergence (d1→d2→d1 false positive). | §7 P6-A compares embedded stamps against the writer's FINAL derived value at seal. |
| 13 | MINOR | Case 1 interleaving B's expected verdict depends on unpinned carrier publish behavior. | §9 splits interleaving B into two cells: carrier publishes V1 → P3′ violation (P1 green); deferred publish → P1 attestation violation. |
| 14 | NOTE | annotationBinding fix run silent on empty-validator replay pages (legal per proto). | §9 case 3 fix run gains an empty-annotation cell; the fix's coverage boundary is a required output of the run. |
| 15 | NOTE | §5 checkpoint-token row understated (ingest quality incl. replay-blocked reason flags is load-bearing for deliverable 3). | §5 table row added. |

Reviewer's stated residual risk (recorded verbatim in substance): the
composition enum exists only in PR discussion; the model must choose its
durability/detection semantics, and an eventual implementation is not
bound by that choice. The carrier-less phantom trace must be compiled to
a chaos scenario and kept green against the real implementation, or a
model-green "propagation bounded" claim can be true of the model and
false of the shipped mitigation.
