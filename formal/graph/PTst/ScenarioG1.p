/* G1 family — phantom-union premises under the graph runtime
   (SPEC 9). Topology cell 11: parent P (node 0, fetch-fresh, every
   row names C) -> consult node C (node 1, marker machinery).
   Expected verdicts (declared before first run):
   - tcG1i_All: leg (i) crash-before/around-commit, fetch-fresh
     policy (diffPolicy off), e1->e2 between syncs — GREEN all.
   - tcG1ii_All: leg (ii) diff unit + crash + e2->e3 between
     attempts, re-derive fetch-fresh (MATCH-only forces re-derive;
     record REPLACES deletes the marker) — GREEN all. (Also the G1c
     honest chassis.)
   - tcG1iii_E / tcG1iii_S: leg (iii) crash-after-commit, no
     mutation — MATCH, digest equal, writer bit clear -> ADOPT —
     GREEN all, both lineage variants.
   - tcG1sup_P1: suppressionOff on the leg-(iii) chassis —
     P1-LEGALITY first-find on the racing double-admission schedule
     (sequential schedules adopt under the declared live-fromGen
     deviation, R2-N1). RED.
   - tcG1b_All: two-crash generation-reuse probe, honest — GREEN
     (forced resume checkpoint fences resume minting, F4).
   - tcG1bMut_PGEN: resumeCkptOff — P-GEN RED (attempts 2 and 3
     re-mint one generation from the stale table).
   - tcG1c_All: FAIL-adopt probe honest (same chassis as leg (ii)):
     MATCH-only forces re-derive, fetch reflects e3 — GREEN.
   - tcG1cMut_Adopt: adoptOnFail — adopts rows(e2) after a FAIL
     consult — P-ADOPT RED. (Calibration find G1-CAL-1 moved this
     kill off SealExpect: completed-across-crash schedules seal
     attempt-1 content honestly under sync-scoped freshness, so the
     scripted expectation accepts any attempt-start world and the
     FAIL-adopt laundering is only mechanism-visible.)
   - tcG1cMut_P3: adoptOnFail — GP3prime stays GREEN (the FAIL
     re-consult does not qualify; the last qualifying verdict is
     attempt 1's diff at e2, which expects the mutant's own rows —
     the adopt-legality monitor is the only oracle that flips). */

machine TestG1i {
    start state I {
        entry {
            var c: tGCfg;
            c = defaultGCfg();
            c.cell = 11;
            c.interrupt = 2;
            c.mutateBetweenSyncs = true;
            new MGEnv(c);
        }
    }
}

machine TestG1ii {
    start state I {
        entry {
            var c: tGCfg;
            c = defaultGCfg();
            c.cell = 11;
            c.interrupt = 2;
            c.mutateBetweenSyncs = true;
            c.mutateBetweenAttempts = true;
            c.diffPolicy = true;
            c.rederiveFresh = true;
            new MGEnv(c);
        }
    }
}

machine TestG1iiiE {
    start state I {
        entry {
            var c: tGCfg;
            c = defaultGCfg();
            c.cell = 11;
            c.interrupt = 2;
            new MGEnv(c);
        }
    }
}

machine TestG1iiiS {
    start state I {
        entry {
            var c: tGCfg;
            c = defaultGCfg();
            c.cell = 11;
            c.interrupt = 2;
            c.lineage = LIN_S;
            new MGEnv(c);
        }
    }
}

machine TestG1sup {
    start state I {
        entry {
            var c: tGCfg;
            var t: tGToggles;
            c = defaultGCfg();
            c.cell = 11;
            c.interrupt = 2;
            t = defaultGToggles();
            t.suppression = false;
            c.toggles = t;
            new MGEnv(c);
        }
    }
}

machine TestG1b {
    start state I {
        entry {
            var c: tGCfg;
            c = defaultGCfg();
            c.cell = 11;
            c.interrupt = 3;
            new MGEnv(c);
        }
    }
}

machine TestG1bMut {
    start state I {
        entry {
            var c: tGCfg;
            var t: tGToggles;
            c = defaultGCfg();
            c.cell = 11;
            c.interrupt = 3;
            t = defaultGToggles();
            t.resumeCkpt = false;
            c.toggles = t;
            new MGEnv(c);
        }
    }
}

machine TestG1cMut {
    start state I {
        entry {
            var c: tGCfg;
            var t: tGToggles;
            c = defaultGCfg();
            c.cell = 11;
            c.interrupt = 2;
            c.mutateBetweenSyncs = true;
            c.mutateBetweenAttempts = true;
            c.diffPolicy = true;
            c.rederiveFresh = true;
            t = defaultGToggles();
            t.adoptOnFail = true;
            c.toggles = t;
            new MGEnv(c);
        }
    }
}

module Graph = { MGEnv, MGStore, MGUpstream, MGraphSched, MGNodeExec };

// Expected GREEN:
test tcG1i_All [main=TestG1i]: assert GP1, GP2, GP3prime, PGEN, PMARK, PADOPT, SealExpectG in (union Graph, { TestG1i });
test tcG1ii_All [main=TestG1ii]: assert GP1, GP2, GP3prime, PGEN, PMARK, PADOPT, SealExpectG in (union Graph, { TestG1ii });
test tcG1iii_E [main=TestG1iiiE]: assert GP1, GP2, GP3prime, PGEN, PMARK, PADOPT, SealExpectG in (union Graph, { TestG1iiiE });
test tcG1iii_S [main=TestG1iiiS]: assert GP1, GP2, GP3prime, PGEN, PMARK, PADOPT, SealExpectG, GP6S, GPASS in (union Graph, { TestG1iiiS });
test tcG1b_All [main=TestG1b]: assert GP1, GP2, GP3prime, PGEN, PMARK, PADOPT, SealExpectG in (union Graph, { TestG1b });
test tcG1cMut_P3 [main=TestG1cMut]: assert GP3prime in (union Graph, { TestG1cMut });
test tcG1cMut_Seal [main=TestG1cMut]: assert SealExpectG in (union Graph, { TestG1cMut });

// Expected RED (counterexample = the calibration find):
test tcG1sup_P1 [main=TestG1sup]: assert GP1 in (union Graph, { TestG1sup });
test tcG1bMut_PGEN [main=TestG1bMut]: assert PGEN in (union Graph, { TestG1bMut });
test tcG1cMut_Adopt [main=TestG1cMut]: assert PADOPT in (union Graph, { TestG1cMut });
