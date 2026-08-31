/* G6 — redo-work bake-off (METRIC cells; the verdicts are counts,
   not pass/fail; adequacy §10.1 count-oracle kills). The count
   oracle is GEXECBOUND: executions per node per sync <= cfg.execBound
   (announced at init). The minimal GREEN bound is the
   checker-verified worst-case redo for the leg; the bound-minus-one
   RED probe is the existence exhibit that the redo is real. The
   bake-off table (CALIBRATION.md) is emitted from these verdicts.

   G6a (cell 26 chain, crash sync 2, re-derivation IDENTICAL): P ->
   S1 -> C -> GC; per-announce demand notes admit the chain mid-round,
   so a checkpoint can capture completed descendants under a pending
   S1 — the divergence script from round-1 F9(a). Declared REFUTABLE
   expectation: both variants redo <= 1 per node (bound 2): E purges
   pending only (completed C/GC stand); S1's re-run re-derives the
   same naming and G-RULE-2 suppression keeps completions.

   G6b (same chassis + between-attempt mutation of S1's scope):
   re-derivation CHANGED — the C/GC tail's demand shrinks; recorded
   counts are arbitration data, no declared winner (round-1 F9(b)).

   G6c (cell 28 fan-in, crash sync 2): C demanded by S1 AND S2; a
   crash killing one parent mid-round must not purge (E, ∀-predicate
   on the survivor's live edge) or false-refuse (S, live edge) C.
   Expected bound 2; the compression divergence lives in G9. */

machine TestG6aE {
    start state I {
        entry {
            var c: tGCfg;
            c = defaultGCfg();
            c.cell = 26;
            c.lineage = LIN_E;
            c.interrupt = 2;
            c.execBound = 2;
            new MGEnv(c);
        }
    }
}

machine TestG6aS {
    start state I {
        entry {
            var c: tGCfg;
            c = defaultGCfg();
            c.cell = 26;
            c.lineage = LIN_S;
            c.interrupt = 2;
            c.execBound = 2;
            new MGEnv(c);
        }
    }
}

machine TestG6aEProbe {
    start state I {
        entry {
            var c: tGCfg;
            c = defaultGCfg();
            c.cell = 26;
            c.lineage = LIN_E;
            c.interrupt = 2;
            c.execBound = 1;
            new MGEnv(c);
        }
    }
}

machine TestG6aSProbe {
    start state I {
        entry {
            var c: tGCfg;
            c = defaultGCfg();
            c.cell = 26;
            c.lineage = LIN_S;
            c.interrupt = 2;
            c.execBound = 1;
            new MGEnv(c);
        }
    }
}

/* GS-CO-005(c) v1 CONTROLS: the metric's zero-crash floor. No
   interrupt, bound 1 — every node executes exactly once per sync
   under BOTH variants; a red is a variant-overhead find (Axis-3
   data), not a kill. */
machine TestG6aECtl {
    start state I {
        entry {
            var c: tGCfg;
            c = defaultGCfg();
            c.cell = 26;
            c.lineage = LIN_E;
            c.execBound = 1;
            new MGEnv(c);
        }
    }
}

machine TestG6aSCtl {
    start state I {
        entry {
            var c: tGCfg;
            c = defaultGCfg();
            c.cell = 26;
            c.lineage = LIN_S;
            c.execBound = 1;
            new MGEnv(c);
        }
    }
}

machine TestG6cECtl {
    start state I {
        entry {
            var c: tGCfg;
            c = defaultGCfg();
            c.cell = 28;
            c.lineage = LIN_E;
            c.execBound = 1;
            new MGEnv(c);
        }
    }
}

machine TestG6cSCtl {
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

machine TestG6bE {
    start state I {
        entry {
            var c: tGCfg;
            c = defaultGCfg();
            c.cell = 26;
            c.lineage = LIN_E;
            c.interrupt = 2;
            c.mutateBetweenAttempts = true;
            c.execBound = 2;
            new MGEnv(c);
        }
    }
}

machine TestG6bEProbe {
    start state I {
        entry {
            var c: tGCfg;
            c = defaultGCfg();
            c.cell = 26;
            c.lineage = LIN_E;
            c.interrupt = 2;
            c.mutateBetweenAttempts = true;
            c.execBound = 1;
            new MGEnv(c);
        }
    }
}

machine TestG6bSProbe {
    start state I {
        entry {
            var c: tGCfg;
            c = defaultGCfg();
            c.cell = 26;
            c.lineage = LIN_S;
            c.interrupt = 2;
            c.mutateBetweenAttempts = true;
            c.execBound = 1;
            new MGEnv(c);
        }
    }
}

machine TestG6bS {
    start state I {
        entry {
            var c: tGCfg;
            c = defaultGCfg();
            c.cell = 26;
            c.lineage = LIN_S;
            c.interrupt = 2;
            c.mutateBetweenAttempts = true;
            c.execBound = 2;
            new MGEnv(c);
        }
    }
}

machine TestG6cE {
    start state I {
        entry {
            var c: tGCfg;
            c = defaultGCfg();
            c.cell = 28;
            c.lineage = LIN_E;
            c.interrupt = 2;
            c.execBound = 2;
            new MGEnv(c);
        }
    }
}

machine TestG6cS {
    start state I {
        entry {
            var c: tGCfg;
            c = defaultGCfg();
            c.cell = 28;
            c.lineage = LIN_S;
            c.interrupt = 2;
            c.execBound = 2;
            new MGEnv(c);
        }
    }
}

machine TestG6cEProbe {
    start state I {
        entry {
            var c: tGCfg;
            c = defaultGCfg();
            c.cell = 28;
            c.lineage = LIN_E;
            c.interrupt = 2;
            c.execBound = 1;
            new MGEnv(c);
        }
    }
}

machine TestG6cSProbe {
    start state I {
        entry {
            var c: tGCfg;
            c = defaultGCfg();
            c.cell = 28;
            c.lineage = LIN_S;
            c.interrupt = 2;
            c.execBound = 1;
            new MGEnv(c);
        }
    }
}

// Expected GREEN (bound 2 holds on every schedule):
test tcG6aE_All [main=TestG6aE]: assert GP1, GP2, GP3prime, PGEN, PMARK, PADOPT, SealExpectG, GP5, GDEADDISPATCH, GEXECBOUND in (union Graph, { TestG6aE });
test tcG6aS_All [main=TestG6aS]: assert GP1, GP2, GP3prime, PGEN, PMARK, PADOPT, SealExpectG, GP5, GDEADDISPATCH, GEXECBOUND, GPASS in (union Graph, { TestG6aS });
test tcG6bE_All [main=TestG6bE]: assert GP1, GP2, GP3prime, PGEN, PMARK, PADOPT, SealExpectG, GP5, GDEADDISPATCH, GEXECBOUND in (union Graph, { TestG6bE });
test tcG6bS_All [main=TestG6bS]: assert GP1, GP2, GP3prime, PGEN, PMARK, PADOPT, SealExpectG, GP5, GDEADDISPATCH, GEXECBOUND, GPASS in (union Graph, { TestG6bS });
test tcG6cE_All [main=TestG6cE]: assert GP1, GP2, GP3prime, PGEN, PMARK, PADOPT, SealExpectG, GP5, GDEADDISPATCH, GEXECBOUND in (union Graph, { TestG6cE });
test tcG6cS_All [main=TestG6cS]: assert GP1, GP2, GP3prime, PGEN, PMARK, PADOPT, SealExpectG, GP5, GDEADDISPATCH, GEXECBOUND, GPASS in (union Graph, { TestG6cS });

// Expected GREEN (GS-CO-005(c) v1 controls: zero-crash floor at bound 1):
test tcG6aE_Ctl [main=TestG6aECtl]: assert GP1, GP2, GP3prime, PGEN, PMARK, PADOPT, SealExpectG, GP5, GDEADDISPATCH, GEXECBOUND in (union Graph, { TestG6aECtl });
test tcG6aS_Ctl [main=TestG6aSCtl]: assert GP1, GP2, GP3prime, PGEN, PMARK, PADOPT, SealExpectG, GP5, GDEADDISPATCH, GEXECBOUND, GPASS in (union Graph, { TestG6aSCtl });
test tcG6cE_Ctl [main=TestG6cECtl]: assert GP1, GP2, GP3prime, PGEN, PMARK, PADOPT, SealExpectG, GP5, GDEADDISPATCH, GEXECBOUND in (union Graph, { TestG6cECtl });
test tcG6cS_Ctl [main=TestG6cSCtl]: assert GP1, GP2, GP3prime, PGEN, PMARK, PADOPT, SealExpectG, GP5, GDEADDISPATCH, GEXECBOUND, GPASS in (union Graph, { TestG6cSCtl });

// Expected RED (bound-1 probes: the redo exists):
test tcG6aE_Redo [main=TestG6aEProbe]: assert GEXECBOUND in (union Graph, { TestG6aEProbe });
test tcG6aS_Redo [main=TestG6aSProbe]: assert GEXECBOUND in (union Graph, { TestG6aSProbe });
test tcG6bE_Redo [main=TestG6bEProbe]: assert GEXECBOUND in (union Graph, { TestG6bEProbe });
test tcG6bS_Redo [main=TestG6bSProbe]: assert GEXECBOUND in (union Graph, { TestG6bSProbe });
test tcG6cE_Redo [main=TestG6cEProbe]: assert GEXECBOUND in (union Graph, { TestG6cEProbe });
test tcG6cS_Redo [main=TestG6cSProbe]: assert GEXECBOUND in (union Graph, { TestG6cSProbe });
