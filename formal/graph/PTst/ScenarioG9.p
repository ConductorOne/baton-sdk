/* G9 — compression admissibility (round-1 F10). Re-runs the S legs
   under `stampCompression`: the pre-seal pass compares FLOOR-BUCKETED
   stamps (buckets of 2) — lossy but STALE-ERRING (never false-live:
   safety preserved by construction; the recorded cost is extra
   redos, because an odd-generation completion always looks stale
   until the forced re-run lands on the bumped even generation).

   ADMISSIBILITY CLAIM (refutable): every safety verdict is identical
   to the uncompressed legs; only the redo counts grow. Any safety
   change here REFUTES the claim and halts the S recommendation.

   Growth is measured on the NO-CRASH fan-in chassis (cell 28),
   where the counts are deterministic (G9-CAL-1: a crash script
   masks the growth — the resume redo and the heal-wave redo both
   peak at 2 per node, so the per-node bound cannot move):
   tcG9cBase_All (uncompressed) is GREEN at bound 1 — every node
   executes once; tcG9c_All (compressed) needs bound 2 — the first
   pass scan heals every odd first-admission generation with one
   forced redo; tcG9c_Redo exhibits the growth at bound 1. */

machine TestG9s {
    start state I {
        entry {
            var c: tGCfg;
            c = defaultGCfg();
            c.cell = 21;
            c.lineage = LIN_S;
            c.interrupt = 2;
            c.mutateBetweenAttempts = true;
            c.stampCompression = true;
            new MGEnv(c);
        }
    }
}

machine TestG9awS {
    start state I {
        entry {
            var c: tGCfg;
            c = defaultGCfg();
            c.cell = 21;
            c.lineage = LIN_S;
            c.interrupt = 2;
            c.stampCompression = true;
            new MGEnv(c);
        }
    }
}

machine TestG9G5aS {
    start state I {
        entry {
            var c: tGCfg;
            c = defaultGCfg();
            c.cell = 24;
            c.lineage = LIN_S;
            c.interrupt = 2;
            c.mutateBetweenAttempts = true;
            c.stampCompression = true;
            new MGEnv(c);
        }
    }
}

machine TestG9cBase {
    start state I {
        entry {
            var c: tGCfg;
            c = defaultGCfg();
            c.cell = 28;
            c.lineage = LIN_S;
            c.execBound = 1;
            new MGEnv(c);
        }
    }
}

machine TestG9c {
    start state I {
        entry {
            var c: tGCfg;
            c = defaultGCfg();
            c.cell = 28;
            c.lineage = LIN_S;
            c.stampCompression = true;
            c.execBound = 2;
            new MGEnv(c);
        }
    }
}

machine TestG9cProbe {
    start state I {
        entry {
            var c: tGCfg;
            c = defaultGCfg();
            c.cell = 28;
            c.lineage = LIN_S;
            c.stampCompression = true;
            c.execBound = 1;
            new MGEnv(c);
        }
    }
}

// Expected GREEN (safety verdicts identical to the uncompressed legs):
test tcG9s_All [main=TestG9s]: assert GP1, GP2, GP3prime, PGEN, PMARK, PADOPT, SealExpectG, GP6G, GP6S, GPASS in (union Graph, { TestG9s });
test tcG9awS_All [main=TestG9awS]: assert GP1, GP2, GP3prime, PGEN, PMARK, PADOPT, SealExpectG, GP6G, GP6S, GPASS in (union Graph, { TestG9awS });
test tcG9G5aS_All [main=TestG9G5aS]: assert GP1, GP2, GP3prime, PGEN, PMARK, PADOPT, SealExpectG, GP5, GDEADDISPATCH, GPASS in (union Graph, { TestG9G5aS });
test tcG9cBase_All [main=TestG9cBase]: assert GEXECBOUND in (union Graph, { TestG9cBase });
test tcG9c_All [main=TestG9c]: assert GP1, GP2, GP3prime, PGEN, PMARK, PADOPT, SealExpectG, GP5, GDEADDISPATCH, GEXECBOUND, GPASS in (union Graph, { TestG9c });

// Expected RED (the bucketing redo-growth exhibit):
test tcG9c_Redo [main=TestG9cProbe]: assert GEXECBOUND in (union Graph, { TestG9cProbe });
