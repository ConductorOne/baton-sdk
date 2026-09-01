/* Scenario 5 — warm-drift (MODEL_SPEC 9 case 5): the warmGate kill and
   both produce-side block triggers. World: upstream never moves (drift
   is config-side). Sync 1 seeds artifact A = rows(e1) under V1 with
   compat record K1 (baseConfig = 1). Sync 2 is the premise sync:
   attempt 1 (warm, K1) consults, records hit {S: V1}, spawns carrier C
   (cell-31 planning shape: consult+spawn+pop) and the stop strands C;
   MEnv applies the scripted drift input to attempt 2 ONLY in premise
   histories (checkpoint holds a stranded carrier), so non-premise
   schedules run undrifted and green. Expected verdicts:
   - tc5a_P1 (cell 51, warmGate OFF — the kill): RED. Attempt 2
     computes K2; install: G7 mismatch -> COLD, trigger 1 (B4) marks
     produce-blocked. With the gate off, C passes the hit check
     (restored {S: V1}) and binding (base unchanged, V1) and copies
     K1-tagged rows into the K2 attempt -> P1-CONFIG (clause c).
   - tc5a_Gate_All (cell 51, shipped toggles — warmGate ON): GREEN.
     C's warm-gate check fails LOUD (eAnnLoudCold, chain cold, no
     ops); the artifact seals blocked with partition[S] empty. The
     attempt-failure/abandonLadder ladder is P4's tranche; here loud
     cold ends the chain and the sync seals (scenario-3 precedent).
   - tc5b_Dropout_All (cell 52, shipped toggles, G6 withdrawn in
     attempt 2): GREEN, and the green IS the required design finding:
     trigger 2 blocks the artifact at install; C's replay-annotated
     page arrives in the handling-less attempt and is SILENTLY
     IGNORED (B1) — no failure, no rows; the sync seals green with
     partition[S] EMPTY and the artifact blocked. P1/P2 are blind
     here (no rows, no illegal round); SealExpect's scripted
     wantBlocked + wantScopeEmpty expectation is the dropout's only
     executable oracle (MODEL_SPEC 9.5b).
   - tc5b_CrashWindow (cell 52, interrupt 3 = stop attempt 1, crash
     attempt 2): RED on SealExpect — the crash-window finding.
     Trigger 2's block lives ONLY in attempt 2's volatile flag until
     a checkpoint carries it; a crash landing before that first
     checkpoint kills the flag with the attempt. Attempt 3 has
     handling back (withdrawal is attempt-2-exact), re-detects
     nothing (no compat mismatch), runs warm, and seals UNBLOCKED —
     the expectation's wantBlocked assert fires. Schedules where the
     crash lands after a checkpoint carrying the flag seal blocked
     and stay green: the red is exactly the window.
   - tc5c_C1Probe (cell 53, shipped toggles, plain stop): RED on the
     C1Probe reachability monitor — the CO-6b-002 conformance answer.
     One action, replay annotation MID-CHAIN: page 0 consults (hit
     recorded at lookup), page 1 replays. The stop lands between the
     pages; the checkpoint holds the mid-chain cursor + the hit map;
     the resumed page 1 performs NO fresh consult and its hit check
     passes on the RESTORED map. The counterexample trace is the
     witness (finding, not a bug); confirm against the real
     implementation via the chaos bridge (deliverable 6). */

machine Test5aGateOff {
    start state I {
        entry {
            var c: tScenarioCfg;
            c = defaultCfg();
            c.scenario = 5;
            c.cell = 51;
            c.interrupt = 1;
            c.baseConfig = 1;
            c.driftCompat = true;
            c.toggles = (warmGate = false, hitValidatorBinding = true, scopeLocks = true, oncePerScope = true, annotationBinding = false, abandonLadder = false, sessionTaintWrites = false, sessionTaintAll = false, recordGrounding = false, groundValidatorBound = false);
            new MEnv(c);
        }
    }
}

machine Test5aGateOn {
    start state I {
        entry {
            var c: tScenarioCfg;
            c = defaultCfg();
            c.scenario = 5;
            c.cell = 51;
            c.interrupt = 1;
            c.baseConfig = 1;
            c.driftCompat = true;
            new MEnv(c);
        }
    }
}

machine Test5bDropout {
    start state I {
        entry {
            var c: tScenarioCfg;
            c = defaultCfg();
            c.scenario = 5;
            c.cell = 52;
            c.interrupt = 1;
            c.baseConfig = 1;
            c.withdrawG6 = true;
            new MEnv(c);
        }
    }
}

machine Test5bCrash {
    start state I {
        entry {
            var c: tScenarioCfg;
            c = defaultCfg();
            c.scenario = 5;
            c.cell = 52;
            c.interrupt = 3;
            c.baseConfig = 1;
            c.withdrawG6 = true;
            new MEnv(c);
        }
    }
}

machine Test5cC1 {
    start state I {
        entry {
            var c: tScenarioCfg;
            c = defaultCfg();
            c.scenario = 5;
            c.cell = 53;
            c.interrupt = 1;
            new MEnv(c);
        }
    }
}

// Expected RED (the warmGate kill, the crash-window finding, then the
// C1 reachability witness):
test tc5a_P1 [main=Test5aGateOff]: assert P1 in (union Walker, { Test5aGateOff });
test tc5b_CrashWindow [main=Test5bCrash]: assert SealExpect in (union Walker, { Test5bCrash });
test tc5c_C1Probe [main=Test5cC1]: assert C1Probe, P1, P2 in (union Walker, { Test5cC1 });

// Expected GREEN:
test tc5a_Gate_All [main=Test5aGateOn]: assert P1, P2, SealExpect in (union Walker, { Test5aGateOn });
test tc5b_Dropout_All [main=Test5bDropout]: assert P1, P2, SealExpect in (union Walker, { Test5bDropout });
