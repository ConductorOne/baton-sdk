# Model spec adversarial review — round 3 (of spec v3)

Reviewer: independent agent (fresh, decorrelated; built its own model
from the 6b contract and code before reading the spec). Verdict:
**REJECT** — 1 blocker, 0 majors, 4 minors, 2 notes. Substantial
convergence from round 2: the reviewer mechanically walked ALL 18 §9
scenario cells (reachability + verdict derivation, applying the P1 fold
by hand) and confirmed 17; it verified 12 of 13 round-2 dispositions as
genuine (the 13th — finding 6 — was half-resolved: the warmGate leg
held, the trigger-2 leg is this round's blocker). Spec revision v4
addresses every finding; dispositions below.

| # | severity | finding (condensed) | disposition in spec v4 |
|---|---|---|---|
| F1 | BLOCKER | §4/§9-5b scripted "fail closed: loud attempt failure" for a replay annotation arriving in a capability-withdrawn attempt. Ground truth: B1 ignores every source-cache annotation wholesale when capability is absent (`sourceCachePageOps` returns nil ops when `sourceCacheEnabled()` is false, `source_cache_orchestration.go` 463–468 — verified directly during disposition); the CO-6b-005 capability-withdrawn chaos cell completes GREEN with the artifact replay-blocked; the block fires at install time (CO-6b-003), not page arrival. §3 (install-time) and §4 (page-arrival) also contradicted each other. Frozen as-is: a machine-checked falsehood, and the cell masks the real shipped hazard (silent annotation drop under a green sync). | §4 bullet rewritten to B1 silent-ignore with trigger 2 pinned to install time; §9 5b re-scripted: green seal, cold consults, empty partition for S, artifact blocked — with the SILENT SCOPE DROPOUT promoted to a required design finding whose oracle is the scripted seal-state expectation (see F7). |
| F2 | MINOR | P1 fold order unanchored for page-interleaved rounds (first- vs last-page commit); all scripted logs agree under either anchor, but out-of-script counterexample logs — exactly what §9's pre-committed classification covers — need not. | §7: fold order pinned to ROUND COMPLETION (commit of a round's last page), with the rationale stated. |
| F3 | MINOR | §3 said completed actions pop "at batch end"; in code the pop commits at completion, mid-batch (state commit ~656 precedes queue admission ~667) — and the spec's own 2-stop cell depends on the mid-batch pop. §2.5 violation (correct semantics lived only in the eStop paragraph and the script). | §3 bullet 4 and MWorker corrected: pops commit at completion, mid-batch; the 2-stop dependency is named in place. |
| F4 | MINOR | §5 and the glossary overclaimed "under hard crash the surviving checkpoint contains only root tokens" — false for stop-then-crash, where a stop-forced checkpoint with mid-chain cursors/spawns survives a later hard crash. | §5 resume paragraph and the glossary entry restated: restart-from-root is a property of WHICH checkpoint survives (crash-only histories), not of the crash itself. |
| F5 | MINOR | Spawn dedup modeled only on cross-op stack pushes; code applies the spawnedAdmitted guard to ALL spawned admissions plus commit-local duplicate-cursor rejection. Without it, legal connector re-mentions produce spurious P4 livelock counterexamples. | §3 bullet 4: dedup extended to all admissions (re-mentions skipped, not errors) plus the commit-local loud rejection; dedup index volatility unchanged. |
| F6 | NOTE | Stop-stranding window stated too narrowly ("before a worker dequeues"); any stop before the carrier's first atomic step completes qualifies. Documentation-only (the scripted route was reachable). | §9 preamble widened. |
| F7 | NOTE | Even corrected, 5b has a shape invisible to P1 (vacuously green — no legal round) and P2 (quantifies over rows present): the carrier's scope rows silently missing from a green seal. | Folded into the 5b re-script: the dropout is the cell's required design finding, checked by the scripted seal-state oracle; a general completeness/coverage oracle is recorded as a deliverable-6 chaos-bridge question. |

§9 cells walked and confirmed by the reviewer (reachability + verdict):
1a, 1b-i, 1b-ii, 1c, the 1-fix cells including the crash-window
variant, 2-stop, 2-crash (both schedules — the G-first alarm and the
required-green H-first), 3A (both interleavings), 3B, the 3-fix cells
including empty-validator, 4 (locks off and on), 5a (including the
warmGate-off mutant kill via the P1 config clause), the C1 probe
(checkpoint token carries `sourceCacheHits` per `state.go` Marshal, so
the restored hit map passes the SDK hit check — C1 is genuinely
reachable via the stop path). 5b was walked and refuted (F1).

Residual risks recorded by the reviewer (carried into the calibration
report's obligations):

1. The 1c compositionEnum trace must be compiled to a chaos scenario
   and kept green against the real implementation (carried from rounds
   1–2).
2. The fold-anchor pin (F2) matters precisely on out-of-script
   counterexample logs; keep it in view when classifying them.
3. Connector purity under-approximates legal within-sync connector
   memory (carried; §8 records it).
4. The silent-missing-rows shape behind 5b is invisible to P1/P2 by
   construction; the scripted seal-state oracle covers the calibration
   cell, and the general question is a deliverable-6 obligation.
5. The empty-validator vacuous-binding residual from the scenario-3 fix
   runs remains open by design.
