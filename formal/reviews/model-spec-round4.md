# Model spec adversarial review — round 4 (of spec v4)

Reviewer: independent agent (fresh, decorrelated; built its own model
from the 6b contract and code before reading the spec). Verdict:
**REJECT — 0 blockers, 0 majors, 3 minors, 3 notes**, with the explicit
disposition that all three minors are fix-without-re-review: "land the
three edits and freeze; no further full round is warranted." All seven
round-3 dispositions were verified as genuinely resolved against ground
truth (not just present); the corrected 5b cell was walked end to end
against code and the CO-6b-005 chaos cell; the fold was re-applied by
hand across 1a/1b-i/1b-ii/1c/3A/3B/4; every cited line number and test
pin was confirmed to exist. Spec revision v5 applies all six findings;
dispositions below.

| # | severity | finding (condensed) | disposition in spec v5 |
|---|---|---|---|
| 1 | MINOR | v4's own dedup extension (F5) made cell 4's "duplicate replay carriers" premise unreachable as written: byte-identical duplicates are rejected commit-locally within one transition or skipped by the spawnedAdmitted guard across transitions. The realized ground-truth shape (CO-6b-003/005 chaos instruments) uses pages from DIFFERENT resources targeting one scope — distinct identity digests. | §9 cell 4 premise pinned: byte-distinct page tokens encoding the same (scope, verdict); literal duplicates explicitly noted as unable to produce the premise. |
| 2 | MINOR | P1's strict overlay precondition (fold value must equal rows(e_from)) alarms on cell 4's scripted GREEN locks-on run: B5 legally copy-skips the duplicate replay and applies its upserts normally, making the second overlay round's precondition unsatisfiable — the fold ambiguity F2 was meant to kill, surviving in the passage F2 edited. | §7 fold gains the duplicate-tolerance clause: a copy-skipped overlay round folds as a NO-OP when the fold value already equals rows(e_to); replacement-count legality counts COMMITTED copies, not verdict labels (locks-off's two committed copies alarm; locks-on's copy-skip folds green). |
| 3 | MINOR | "Produce-side blocking — exactly two triggers, nothing broader" is false of the system: page-arrival shape guards (child-resource declarations, `InsertResourceGrants`), ingest-filter drops (B6), and unknown-prior-checkpoint conservatism also block. None are reachable in the model, so no cell verdict was wrong — but frozen as a fidelity statement it is a falsehood readers and model extensions inherit. | §3 scoped to "within the modeled fragment" with the excluded triggers named; §1's abstraction list registers them with the reason each is unreachable in the model. |
| 4 | NOTE | 5b's "matching the CO-6b-005 chaos cell" over-attributed: the chaos cell pins green completion, cold consults, compat retention, and the blocked mark — NOT the empty-partition dropout (its connector adapts cold on miss; no stranded carrier exists there). | §9 5b narrowed: the four pinned outcomes attributed to the chaos cell; the dropout attributed solely to the model's scripted seal-state oracle. |
| 5 | NOTE | "Loop-top checkpoints contain only root tokens" is true of the modeled (batched) population only; sequential non-fanned ops (e.g. `SyncResourceTypesOp`) can checkpoint mid-chain at loop tops in crash-only histories. | §8 boundary note added; future scenarios touching sequential ops must not inherit the claim. |
| 6 | NOTE | The commit-local duplicate-cursor rejection is same-op-scoped in code (`queue.transition` checks only children with the batch op); §3 stated it unqualified. | §3 qualifier added. |

Verification coverage reported by the reviewer: round-3 dispositions
F1–F6 + §11 all verified genuine (F1 against `sourceCachePageOps`
463–468, the proto's capability wording, `installSourceCacheLookup`'s
install-time block, and `Checkpoint`'s `SetIngestQuality` durability;
F4 against the derived stop-then-crash ground truth). Cells re-walked
by hand: 1a, 1b-i, 1b-ii, 1c (+ fix-run claim boundaries), 2-stop,
2-crash (both schedules), 3A (both interleavings), 3B, 4 (all
togglings — findings 1 and 2 live here), 5a (both togglings), 5b (end
to end), C1 probe. Freeze sweep: §5 table vs `state.go`
Marshal/Unmarshal (warm flag genuinely absent from the token), §6 kill
table vs cells, §7 vocabulary vs events, §11 vs all round logs,
glossary vs spec and code.

Residual risks recorded by the reviewer (carried into the freeze
record and the calibration report's obligations):

1. No v3/v4 baseline exists to diff — `formal/` is untracked, so the
   "surgical delta" claims were verified by re-walking, not
   mechanically. COMMIT THE SPEC AT FREEZE so change orders have a
   diffable baseline.
2. The composition enum's semantics exist only in PR discussion and
   could not be verified against any repo artifact; the spec labels
   this an input assumption (§4). Confirm before deliverable 3's runs;
   keep the 1c trace compiled to a chaos scenario (carried from rounds
   1–3).
3. The silent-dropout shape behind 5b has no chaos-suite analogue; the
   deliverable-6 completeness-oracle question is the only path to an
   executable check.
4. Carried: fold-anchor discipline on out-of-script logs (now with
   finding 2's clause), connector-purity under-approximation, the
   empty-validator vacuous-binding residual in the scenario-3 fix runs.
5. Finding 5's boundary: future scenarios touching sequential ops must
   not inherit the batched-population checkpoint claim.
