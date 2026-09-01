/* Scenario 1 — phantom union (MODEL_SPEC 9 case 1).
   Shipped toggles ON in every cell: the residual exists in the shipped
   design. Expected verdicts:
   - tc1a1b_P1 (stop-stranding, carrier publishes, 2 syncs): P1-CONTENT
     red on 1a schedules (carrier drains between/before fresh pages).
   - tc1a1b_P3: P3'-COHERENCE red on 1b-i schedules (carrier drains
     after the complete fresh round; content green, epoch incoherent).
   - tc1a1b_P2 (3 syncs, corollary-run scoping): P2-STALENESS red on
     the verification sync — the union replays warm, hops reach 2
     (the unbounded branch).
   - tc1bii_P1 (carrier validator-less): P1 red; the attestation-only
     edge (entry V2 over folded rows(e1)) is among the violations.
   - tc1c_P1/tc1c_P2 (carrier-less hard crash): P1-CONTENT red with one
     crash and no replay in attempt 2; P2 red on the verification sync.
   - tcGreen_All (no interruption, no mutation, 2 syncs incl. one
     honest replay): all monitors green — the abstraction sanity
     control.
   MS-CO-003 grounding ladder (the shipped groundRecordScope fix,
   modeled; see CALIBRATION.md's MS-CO-003 subsection):
   - tc1cNoPub_P1 RED / tc1cNoPubGround_P1 GREEN — the toggle's kill
     pair on the validator-less flavor.
   - tc1cGround_P1/tc1cGround_P2 RED — the faithful shipped rule's
     REGISTERED RESIDUAL (published-replay verdict flip).
   - tc1cGroundV_P1/tc1cGroundV_P2 GREEN — the validator-bound
     candidate closure (design arbitration; not shipped). */

fun shippedToggles(): tToggles {
    return (warmGate = true, hitValidatorBinding = true, scopeLocks = true, oncePerScope = true, annotationBinding = false, abandonLadder = false, sessionTaintWrites = false, sessionTaintAll = false, recordGrounding = false, groundValidatorBound = false);
}

// MS-CO-003: shipped toggles + record-round grounding (the
// groundRecordScope fix, faithful conditional form — manifest-entry
// skip included). Paired against tc1c_P1/tc1c_P2: same config with the
// toggle off is the calibrated red, so the pair is the mutation-
// adequacy witness for the grounding mechanism itself.
fun groundingToggles(): tToggles {
    return (warmGate = true, hitValidatorBinding = true, scopeLocks = true, oncePerScope = true, annotationBinding = false, abandonLadder = false, sessionTaintWrites = false, sessionTaintAll = false, recordGrounding = true, groundValidatorBound = false);
}

// MS-CO-003 candidate closure: shipped grounding + the validator-bound
// rule (clear also when the published entry's validator differs from
// the record round's incoming validator). NOT shipped — the tc1cGroundV
// greens are design arbitration for closing tc1cGround's residual.
fun groundingBoundToggles(): tToggles {
    return (warmGate = true, hitValidatorBinding = true, scopeLocks = true, oncePerScope = true, annotationBinding = false, abandonLadder = false, sessionTaintWrites = false, sessionTaintAll = false, recordGrounding = true, groundValidatorBound = true);
}

machine Test1a {
    start state I {
        entry {
            var c: tScenarioCfg;
            c = defaultCfg();
            c.scenario = 1;
            c.interrupt = 1;
            c.mutateBetweenAttempts = true;
            new MEnv(c);
        }
    }
}

machine Test1aVerify {
    start state I {
        entry {
            var c: tScenarioCfg;
            c = defaultCfg();
            c.scenario = 1;
            c.interrupt = 1;
            c.mutateBetweenAttempts = true;
            c.nSyncs = 3;
            c.verificationOnlyIfInterrupted = true;
            new MEnv(c);
        }
    }
}

machine Test1bii {
    start state I {
        entry {
            var c: tScenarioCfg;
            c = defaultCfg();
            c.scenario = 1;
            c.interrupt = 1;
            c.mutateBetweenAttempts = true;
            c.carrierPublishes = false;
            new MEnv(c);
        }
    }
}

machine Test1c {
    start state I {
        entry {
            var c: tScenarioCfg;
            c = defaultCfg();
            c.scenario = 1;
            c.cell = 3;
            c.interrupt = 2;
            c.mutateBetweenAttempts = true;
            new MEnv(c);
        }
    }
}

machine Test1cVerify {
    start state I {
        entry {
            var c: tScenarioCfg;
            c = defaultCfg();
            c.scenario = 1;
            c.cell = 3;
            c.interrupt = 2;
            c.mutateBetweenAttempts = true;
            c.nSyncs = 3;
            c.verificationOnlyIfInterrupted = true;
            new MEnv(c);
        }
    }
}

machine Test1cGround {
    start state I {
        entry {
            var c: tScenarioCfg;
            c = defaultCfg();
            c.scenario = 1;
            c.cell = 3;
            c.interrupt = 2;
            c.mutateBetweenAttempts = true;
            c.toggles = groundingToggles();
            new MEnv(c);
        }
    }
}

machine Test1cGroundVerify {
    start state I {
        entry {
            var c: tScenarioCfg;
            c = defaultCfg();
            c.scenario = 1;
            c.cell = 3;
            c.interrupt = 2;
            c.mutateBetweenAttempts = true;
            c.nSyncs = 3;
            c.verificationOnlyIfInterrupted = true;
            c.toggles = groundingToggles();
            new MEnv(c);
        }
    }
}

machine Test1cGroundV {
    start state I {
        entry {
            var c: tScenarioCfg;
            c = defaultCfg();
            c.scenario = 1;
            c.cell = 3;
            c.interrupt = 2;
            c.mutateBetweenAttempts = true;
            c.toggles = groundingBoundToggles();
            new MEnv(c);
        }
    }
}

machine Test1cGroundVVerify {
    start state I {
        entry {
            var c: tScenarioCfg;
            c = defaultCfg();
            c.scenario = 1;
            c.cell = 3;
            c.interrupt = 2;
            c.mutateBetweenAttempts = true;
            c.nSyncs = 3;
            c.verificationOnlyIfInterrupted = true;
            c.toggles = groundingBoundToggles();
            new MEnv(c);
        }
    }
}

// The validator-less (1b-ii-style) inline replay: the replay round
// completes at its copy and publishes NO entry — every crash flavor
// leaves UNPUBLISHED rows, exactly the debris shipped grounding
// clears. This pair is the shipped toggle's kill: same config red
// with grounding off, green with it on.
machine Test1cNoPub {
    start state I {
        entry {
            var c: tScenarioCfg;
            c = defaultCfg();
            c.scenario = 1;
            c.cell = 3;
            c.interrupt = 2;
            c.mutateBetweenAttempts = true;
            c.carrierPublishes = false;
            new MEnv(c);
        }
    }
}

machine Test1cNoPubGround {
    start state I {
        entry {
            var c: tScenarioCfg;
            c = defaultCfg();
            c.scenario = 1;
            c.cell = 3;
            c.interrupt = 2;
            c.mutateBetweenAttempts = true;
            c.carrierPublishes = false;
            c.toggles = groundingToggles();
            new MEnv(c);
        }
    }
}

machine TestGreen {
    start state I {
        entry {
            var c: tScenarioCfg;
            c = defaultCfg();
            c.scenario = 1;
            new MEnv(c);
        }
    }
}

module Walker = { MEnv, MStore, MUpstream, MSyncAttempt, MWorker };

// Expected RED (counterexample = the calibration find):
test tc1a1b_P1 [main=Test1a]: assert P1 in (union Walker, { Test1a });
test tc1a1b_P3 [main=Test1a]: assert P3prime in (union Walker, { Test1a });
test tc1a1b_P2 [main=Test1aVerify]: assert P2 in (union Walker, { Test1aVerify });
test tc1bii_P1 [main=Test1bii]: assert P1 in (union Walker, { Test1bii });
test tc1c_P1 [main=Test1c]: assert P1 in (union Walker, { Test1c });
test tc1c_P2 [main=Test1cVerify]: assert P2 in (union Walker, { Test1cVerify });

// Expected GREEN (sanity controls):
test tcGreen_All [main=TestGreen]: assert P1, P2, P3prime in (union Walker, { TestGreen });
// The P1/P3 cells must also hold P1 green where their premise implies
// no alarm; P2's corollary config must stay green in honest histories.
test tc1c_P2_honest [main=Test1c]: assert P2 in (union Walker, { Test1c });
// Probe: the verify config must still contain the sync-2 union premise.
test tc1c_P1_probe [main=Test1cVerify]: assert P1 in (union Walker, { Test1cVerify });
// MS-CO-003 ladder (see CALIBRATION.md for adjudication):
// 1. Shipped grounding's kill pair — the validator-less flavor, where
//    every crash leaves unpublished debris: red off, green on.
test tc1cNoPub_P1 [main=Test1cNoPub]: assert P1 in (union Walker, { Test1cNoPub });
test tc1cNoPubGround_P1 [main=Test1cNoPubGround]: assert P1 in (union Walker, { Test1cNoPubGround });
// 2. The faithful shipped design (manifest-entry skip included): RED —
//    the registered residual. A replay round that completed AND
//    published before the crash is skipped by grounding; the re-run's
//    flipped record round accumulates over its rows (audited flavor:
//    every counterexample publishes before crashing).
test tc1cGround_P1 [main=Test1cGround]: assert P1 in (union Walker, { Test1cGround });
test tc1cGround_P2 [main=Test1cGroundVerify]: assert P2 in (union Walker, { Test1cGroundVerify });
// 3. The validator-bound candidate closure: green.
test tc1cGroundV_P1 [main=Test1cGroundV]: assert P1 in (union Walker, { Test1cGroundV });
test tc1cGroundV_P2 [main=Test1cGroundVVerify]: assert P2 in (union Walker, { Test1cGroundVVerify });
