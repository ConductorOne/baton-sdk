/* G2 family — session laundering (SPEC 9), plus the G1d/G1e probes
   that ride the session chassis. Topology cell 21: parent P (node 0,
   row 0 names writer H = node 2, row 1 names reader G = node 3). H
   publishes session key 0 (value = its consult epoch: same-premise
   re-derivations re-publish the same value); G reads it and embeds
   the value in its rows.

   Common chassis: 2 syncs, crash in sync 2 attempt 1. The LAUNDERING
   legs mutate H's scope between attempts so H's re-publish value
   differs; the ANNOUNCE-WINDOW / flap-back legs are premise-stable
   (no mutation) so adoption eligibility and same-value re-publish
   are reachable.

   Expected verdicts (declared before first run):
   - tcG2ea_P6G: E + session variant A — THE FINDING: no retraction,
     no stamps; a schedule where G completed (+ckpt) before the
     crash and H re-published a different value seals G's rows
     embedding the dead value — P6-G RED.
   - tcG2ea_Core: same cell, artifact-level monitors only — GREEN
     (the laundering is invisible to P1/P2/P3'/SealExpect: the
     blindness contrast is the point).
   - tcG2eb_All: E + variant B (retraction + quiesce) — GREEN all
     (incl. P6-G, P6-E).
   - tcG2ebRetrOff_P6G: retractionOff — P6-G RED.
   - tcG2s_All: S (stamps + pre-seal pass) — GREEN all (incl. P6-S).
   - tcG2sStampOff_P6G: stampMergeOff — P6-G RED (the pass never
     sees the dead writer stamp on G's output).
   - tcG2awE_All / tcG2awS_All: announce-window + writer flap-back
     honest legs (premise-stable): H's publish-bearing marker is
     adoption-INELIGIBLE -> REPLAY re-derive -> re-publish same
     value under the new generation -> readers cleared (retraction /
     stamp delta) — GREEN all.
   - tcG2awE_Redo / tcG2awS_Redo: REDO-PROBE existence exhibits on
     the same chassis — RED (the at-least-once cost is real: a
     forced reader redo exists).
   - tcG2awWA_E: writerAdopt — H adopts, never re-publishes; the
     dead-generation publish strands; G is never retracted — P6-E
     RED. (P6-G stays green: the value is unchanged — the kill is
     mechanism-visible only.)
   - tcG2awWA_S: writerAdopt — G's re-reads keep merging the dead
     {H: g1} stamp; the pass exhausts its budget — P6-S RED.
   - tcG1d_P6G: E+B chassis + quiesceOff (round-2 R2-F1): the
     retraction bump lands while the dying reader executes; the dead
     execution's late record round overwrites the live re-derivation
     — P6-G RED. Honest quiesce covered by tcG2eb_All.
   - tcG2eb2c_All: honest two-crash session chassis — GREEN.
   - tcG1e_PGEN: two crashes + midBumpFenceOff (round-3 R3-F2) —
     P-GEN RED (generation identity reuse; the first-find may fire
     on the first-admission mint path, which rides the same toggle
     per GS-CO-001 — same discipline, same monitor).
   - tcG2fbE_All / tcG2fbS_All: WRITER FLAP-BACK PROBE (round-3
     R3-F1's second registration): sync-2 attempt 1 commits H's
     DIFF-verdict publish-bearing unit (d2, marker digest FAIL);
     crash in the announce window; upstream flaps back
     (content(e3) = content(e1)). Resume: H ineligible
     (pubBearing + digest delta) -> re-derives -> re-consult vs the
     PREV artifact (V1) MATCHes truthfully -> REPLAY verdict -> the
     body-op pin re-publishes d1@g2 anyway -> readers cleared
     (retraction / stamp delta) -> GREEN all. Under an elision
     reading this history strands d2@g1 and reds P6-G/P6-S honestly
     — the probe is the body-op pin's load-bearing witness.
   - tcG2fbE_Redo: REDO-PROBE RED on the flap-back chassis (the
     forced reader redo exists — the declared expected count >= 1).
   - tcG2ebPend_Redo: REDO-PROBE RED on the honest E+B laundering
     chassis — the G-pending re-retraction clause (R2-M6(i),
     GS-CO-004's catch-up + re-publish paths) actually fires. */

machine TestG2ea {
    start state I {
        entry {
            var c: tGCfg;
            c = defaultGCfg();
            c.cell = 21;
            c.lineage = LIN_E;
            c.sessVar = SESS_A;
            c.interrupt = 2;
            c.mutateBetweenAttempts = true;
            new MGEnv(c);
        }
    }
}

machine TestG2eb {
    start state I {
        entry {
            var c: tGCfg;
            c = defaultGCfg();
            c.cell = 21;
            c.lineage = LIN_E;
            c.sessVar = SESS_B;
            c.interrupt = 2;
            c.mutateBetweenAttempts = true;
            new MGEnv(c);
        }
    }
}

machine TestG2ebRetrOff {
    start state I {
        entry {
            var c: tGCfg;
            var t: tGToggles;
            c = defaultGCfg();
            c.cell = 21;
            c.lineage = LIN_E;
            c.sessVar = SESS_B;
            c.interrupt = 2;
            c.mutateBetweenAttempts = true;
            t = defaultGToggles();
            t.retraction = false;
            c.toggles = t;
            new MGEnv(c);
        }
    }
}

machine TestG2s {
    start state I {
        entry {
            var c: tGCfg;
            c = defaultGCfg();
            c.cell = 21;
            c.lineage = LIN_S;
            c.interrupt = 2;
            c.mutateBetweenAttempts = true;
            new MGEnv(c);
        }
    }
}

machine TestG2sStampOff {
    start state I {
        entry {
            var c: tGCfg;
            var t: tGToggles;
            c = defaultGCfg();
            c.cell = 21;
            c.lineage = LIN_S;
            c.interrupt = 2;
            c.mutateBetweenAttempts = true;
            t = defaultGToggles();
            t.stampMerge = false;
            c.toggles = t;
            new MGEnv(c);
        }
    }
}

machine TestG2awE {
    start state I {
        entry {
            var c: tGCfg;
            c = defaultGCfg();
            c.cell = 21;
            c.lineage = LIN_E;
            c.sessVar = SESS_B;
            c.interrupt = 2;
            new MGEnv(c);
        }
    }
}

machine TestG2awS {
    start state I {
        entry {
            var c: tGCfg;
            c = defaultGCfg();
            c.cell = 21;
            c.lineage = LIN_S;
            c.interrupt = 2;
            new MGEnv(c);
        }
    }
}

machine TestG2awWAE {
    start state I {
        entry {
            var c: tGCfg;
            var t: tGToggles;
            c = defaultGCfg();
            c.cell = 21;
            c.lineage = LIN_E;
            c.sessVar = SESS_B;
            c.interrupt = 2;
            t = defaultGToggles();
            t.writerAdopt = true;
            c.toggles = t;
            new MGEnv(c);
        }
    }
}

machine TestG2awWAS {
    start state I {
        entry {
            var c: tGCfg;
            var t: tGToggles;
            c = defaultGCfg();
            c.cell = 21;
            c.lineage = LIN_S;
            c.interrupt = 2;
            t = defaultGToggles();
            t.writerAdopt = true;
            c.toggles = t;
            new MGEnv(c);
        }
    }
}

machine TestG1d {
    start state I {
        entry {
            var c: tGCfg;
            var t: tGToggles;
            c = defaultGCfg();
            c.cell = 21;
            c.lineage = LIN_E;
            c.sessVar = SESS_B;
            c.interrupt = 2;
            c.mutateBetweenAttempts = true;
            c.nWorkers = 3;
            t = defaultGToggles();
            t.quiesce = false;
            c.toggles = t;
            new MGEnv(c);
        }
    }
}

machine TestG2eb2c {
    start state I {
        entry {
            var c: tGCfg;
            c = defaultGCfg();
            c.cell = 21;
            c.lineage = LIN_E;
            c.sessVar = SESS_B;
            c.interrupt = 3;
            c.mutateBetweenAttempts = true;
            new MGEnv(c);
        }
    }
}

machine TestG2fbE {
    start state I {
        entry {
            var c: tGCfg;
            c = defaultGCfg();
            c.cell = 21;
            c.lineage = LIN_E;
            c.sessVar = SESS_B;
            c.interrupt = 2;
            c.mutateBetweenSyncs = true;
            c.mutateBetweenAttempts = true;
            c.flapBack = true;
            c.diffPolicy = true;
            new MGEnv(c);
        }
    }
}

machine TestG2fbS {
    start state I {
        entry {
            var c: tGCfg;
            c = defaultGCfg();
            c.cell = 21;
            c.lineage = LIN_S;
            c.interrupt = 2;
            c.mutateBetweenSyncs = true;
            c.mutateBetweenAttempts = true;
            c.flapBack = true;
            c.diffPolicy = true;
            new MGEnv(c);
        }
    }
}

machine TestG1e {
    start state I {
        entry {
            var c: tGCfg;
            var t: tGToggles;
            c = defaultGCfg();
            c.cell = 21;
            c.lineage = LIN_E;
            c.sessVar = SESS_B;
            c.interrupt = 3;
            c.mutateBetweenAttempts = true;
            t = defaultGToggles();
            t.midBumpFence = false;
            c.toggles = t;
            new MGEnv(c);
        }
    }
}

// Expected GREEN:
test tcG2ea_Core [main=TestG2ea]: assert GP1, GP2, GP3prime, PGEN, PMARK, PADOPT, SealExpectG in (union Graph, { TestG2ea });
test tcG2eb_All [main=TestG2eb]: assert GP1, GP2, GP3prime, PGEN, PMARK, PADOPT, SealExpectG, GP6G, GP6E in (union Graph, { TestG2eb });
test tcG2s_All [main=TestG2s]: assert GP1, GP2, GP3prime, PGEN, PMARK, PADOPT, SealExpectG, GP6G, GP6S, GPASS in (union Graph, { TestG2s });
test tcG2awE_All [main=TestG2awE]: assert GP1, GP2, GP3prime, PGEN, PMARK, PADOPT, SealExpectG, GP6G, GP6E in (union Graph, { TestG2awE });
test tcG2awS_All [main=TestG2awS]: assert GP1, GP2, GP3prime, PGEN, PMARK, PADOPT, SealExpectG, GP6G, GP6S, GPASS in (union Graph, { TestG2awS });
test tcG2eb2c_All [main=TestG2eb2c]: assert GP1, GP2, GP3prime, PGEN, PMARK, PADOPT, SealExpectG, GP6G, GP6E in (union Graph, { TestG2eb2c });
test tcG2fbE_All [main=TestG2fbE]: assert GP1, GP2, GP3prime, PGEN, PMARK, PADOPT, SealExpectG, GP6G, GP6E in (union Graph, { TestG2fbE });
test tcG2fbS_All [main=TestG2fbS]: assert GP1, GP2, GP3prime, PGEN, PMARK, PADOPT, SealExpectG, GP6G, GP6S, GPASS in (union Graph, { TestG2fbS });

// Expected RED:
test tcG2ea_P6G [main=TestG2ea]: assert GP6G in (union Graph, { TestG2ea });
test tcG2ebRetrOff_P6G [main=TestG2ebRetrOff]: assert GP6G in (union Graph, { TestG2ebRetrOff });
test tcG2sStampOff_P6G [main=TestG2sStampOff]: assert GP6G in (union Graph, { TestG2sStampOff });
test tcG2awE_Redo [main=TestG2awE]: assert REDOPROBE in (union Graph, { TestG2awE });
test tcG2awS_Redo [main=TestG2awS]: assert REDOPROBE in (union Graph, { TestG2awS });
test tcG2fbE_Redo [main=TestG2fbE]: assert REDOPROBE in (union Graph, { TestG2fbE });
test tcG2ebPend_Redo [main=TestG2eb]: assert REDOPROBE in (union Graph, { TestG2eb });
test tcG2awWA_E [main=TestG2awWAE]: assert GP6E in (union Graph, { TestG2awWAE });
test tcG2awWA_S [main=TestG2awWAS]: assert GP6S in (union Graph, { TestG2awWAS });
test tcG1d_P6G [main=TestG1d]: assert GP6G in (union Graph, { TestG1d });
// Existence probe: the forced reader redo arms on the G1d chassis
// (the reachability ladder's first rung; see CALIBRATION G1D-REACH).
test tcG1dProbe_Redo [main=TestG1d]: assert REDOPROBE in (union Graph, { TestG1d });
test tcG1e_PGEN [main=TestG1e]: assert PGEN in (union Graph, { TestG1e });
