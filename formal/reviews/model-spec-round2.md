# Model spec adversarial review — round 2 (of spec v2)

Reviewer: independent agent (fresh, decorrelated; built its own model
from the 6b contract and code before reading the spec). Verdict:
**REJECT** — 2 blockers, 4 majors, 4 minors, 3 notes. It verified 12 of
the 15 round-1 dispositions as genuinely resolved (1, 4, 5, 7–15) and
confirmed all of v2's code citations and the durability table. Spec
revision v3 addresses every finding; dispositions below. Both blockers
were scenario-REACHABILITY defects — the machine semantics were right,
and the scripts contradicted them — which is why v3 adds the standing
reachability-walk obligation (§2.6, §10.0).

| # | severity | finding (condensed) | disposition in spec v3 |
|---|---|---|---|
| 1 | BLOCKER | Scenarios 1a/1b and 3 unreachable as scripted: a same-op spawn can never be stranded at a loop-top checkpoint (spawns drain in-batch, `parallel_syncer.go` 656–697), a mid-batch spawn admission is volatile under hard crash, and the pre-pushed-carrier alternative needs connector cross-call state the spec forbids. The reachable premise generator is the graceful-stop forced checkpoint. | §9 re-scripted on the STOP-STRANDING pattern: the stop-forced checkpoint durably captures {parent mid-chain cursor, admitted-but-undrained carrier, hit map} together (checkpointability pinned by `TestSpawnedActionsSurviveCheckpoint`); the attempt-2 re-consult comes from the parent's next page. Verdicts route through page tokens (no cross-call memory); the purity-vs-proto boundary is recorded in §8. Verified against code before adoption. |
| 2 | BLOCKER | Scenario 2's hard-crash cell claimed expected-green; false — session KV is durable, pops are volatile, so both actions re-run, and the G-before-H schedule (a genuine choice point) re-embeds stale d1 → P6-A alarms. Real shipped behavior; freezing the green expectation would force the P code to mask it. | §9 case 2 split into 2-stop (deterministic script) and 2-crash (interleaving-dependent expected FINDING, with the complementary H-first schedule required green). The false parenthetical claim removed. |
| 3 | MAJOR | P1 fold under-specified where it decides alarm-vs-miss: per-page vs per-round granularity, undefined epoch for partially applied delta rounds at publish (plan B5 legally publishes the new token before overlay pages land), deterministic-fold vs existential formulations mixed. | §7 P1 fully pinned: round definition (maximal same-verdict run of one chain's pages), complete-rounds-only deterministic fold in commit order (replacement/overlay/fresh rules; fresh REPLACES in the fold), log-legality rules, content+attestation+config checks at seal, ATTESTATION-ONLY at publish. |
| 4 | MAJOR | P3′ scoping incomplete: a fresh round torn mid-chain by a stop and resumed after between-attempt mutation observes two epochs inside the allowed scenario class → spurious counterexamples contaminating calibration attribution. | §7 P3′ doubly scoped: no mid-attempt mutation AND no torn round for the scope (ghost torn-round flag). 1b-i's P3′ verdict re-checked under the new scoping (its rounds are not torn). |
| 5 | MAJOR | Scenario 3's claimed P2 violation contradicts P2's own pinning: attempt 1's validation match qualifies the scope as consulted this sync, so P2 passes; the actual catch is P1-attestation. Interleaving with attempt 2's fetch-fresh round was underived. | §9 case 3 re-derived per cell: P1 attestation (carrier last) or P1 content (carrier first/interleaved); P2 explicitly GREEN in every cell; 3B (binding-off) premise simplified to the no-re-consult route. |
| 6 | MAJOR | `warmGate` had no killing scenario (all scripted attempts were warm), so §10.5's mutation-adequacy obligation was unsatisfiable; produce trigger 1 also unexercised. | §9 scenario 5 added: 5a compat-drift cell (trigger 1) kills warmGate via the new P1 config clause; 5b capability-withdrawal cell exercises trigger 2 fail-closed. §6 gains an explicit kill-obligation table. |
| 7 | MINOR | G6 (capability) folded into a static "previous artifact usable" boolean, making produce trigger 2 unreachable. | §1/§3: G6 is a per-attempt bit on MEnv's schedule; warm install and trigger 2 consume it; scenario 5b exercises it. |
| 8 | MINOR | §4's replayed-set check missing its `[toggle: oncePerScope]` marker. | Marker added. |
| 9 | MINOR | Glossary "Restart-from-root" and "Checkpoint" entries state the hard-crash rule as universal, contradicting the spec's (correct) stop-path semantics; the glossary is read first. | Both `GLOSSARY.md` entries corrected: forced sites enumerated, stop-checkpoint contents named, restart-from-root scoped to hard crash. |
| 10 | MINOR | C1 had no carrying scenario (no policy places a replay annotation on page ≥ 2), so it would resolve vacuously. | §9 scenario 5 gains the C1 probe: replay annotation on page 2, stop between pages, resume without fresh consult — model answers C1 "reachable via the stop path", recorded as a conformance finding to test through the chaos bridge. |
| 11 | NOTE | Session KV shares MStore's durability domain/crash cut in the model; prod session store is a separate, non-prefix-ordered service. | §8 records the trust boundary of P6-A's verdicts (brief pins variant A durable). |
| 12 | NOTE | "Intent" named both the P1 ghost label and the deliverable-3 wire enum; the ghost exists with `compositionEnum` off. | Ghost renamed "verdict class" throughout; "intent" reserved for the wire enum (§4 states the reservation). |
| 13 | NOTE | Batch-cap argument overclaimed "every split shape"; parts ≥ 3 unexplored; the true justification (in-batch spawn admission is uncapped; multi-way splits decompose into two-way splits) was unstated. | §1 argument rewritten with the honest justification. |

Residual risks recorded by the reviewer (carried into the calibration
report's obligations):

1. The composition enum exists only in PR discussion; the 1c trace must
   be compiled to a chaos scenario and kept green against the real
   implementation, or a model-green "propagation bounded" claim can be
   true of the model and false of the shipped mitigation. (Carried
   from round 1; §4 and §10.6 keep it explicit.)
2. Cross-attempt fresh-debris unions (no replay involved) are real
   shipped behavior adjacent to scenario 1's config; §9 now
   pre-commits to classifying out-of-script counterexamples of that
   shape as design findings, not model noise.
3. Connector purity under-approximates legal connector behavior
   (within-sync memory); extensions are change orders (§8).
4. The pre-seal forced checkpoint site, which the reviewer accepted
   from the spec's assertion, was verified directly during disposition:
   `syncer.go` 1162 (post-expansion, pre-seal, force=true), alongside
   Init (232/268/1115) and stop/expiry (195/469/490).
