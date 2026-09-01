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
     control. */

fun shippedToggles(): tToggles {
    return (warmGate = true, hitValidatorBinding = true, scopeLocks = true, oncePerScope = true, annotationBinding = false, abandonLadder = false, sessionTaintWrites = false, sessionTaintAll = false);
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
