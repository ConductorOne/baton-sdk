/* G5 family — sweep, purge, and the closure oracle (SPEC 9, round-1
   F5/F6/F8 re-scripts). Topology cell 24: PAGINATED parent P (node
   0, 2-page record round) names consult node C (node 1) on its
   page-1 row ONLY — the row the e1->e2 mutation deletes. The
   mutation target is P's OWN scope (key 0), so an attempt-2
   re-execution emits NO child naming: upstream demand shrink.

   The per-announce demand note (G-RULE-1 TIMING PIN) is the family's
   load-bearing mechanism: P's page-1 commit admits C mid-round, so a
   checkpoint can capture C-pending AND P-pending — the window the
   resume ∀-purge (R2-F5) exists for. Carrier-only derivation makes
   every shape below unreachable (calibration find; the spec pinned
   per-announce timing from the start).

   Shrink chassis (mutateBetweenAttempts on key 0, crash in sync 2):
   - tcG5aE_All / tcG5aS_All: honest sweep, both variants — GREEN.
     Which parent world survives is schedule-dependent (sync-scoped
     freshness): P re-ran at e2 -> C purged (E) or refused (S) or
     never admitted, C's debris swept, seal {0}; P completed at e1 ->
     C is live demand, seal {0, 1}. C's key is EXCLUDED from the
     attempt-2 expectation (the structural question is GP5's).
   - tcG5bE_P5 / tcG5bS_P5: sweepOff — the P-re-ran schedule seals
     C's partition with no living namer — P5-UNDER RED both variants.
   - tcG5e_Probe: PURGE-PROBE existence exhibit on the honest E
     chassis — RED (the resume purge fires; the counterexample
     exhibits the mid-round checkpoint window).
   - tcG5e_PurgeOff: purgeOff (E) — the restored C dispatches with
     every admitted-by edge dead — DEAD-DISPATCH RED (the G5e count
     oracle: dead demand executed).

   No-shrink chassis (same cell, no mutation, crash in sync 2):
   - tcG5f_All: honest — GREEN. The R2-F5 no-starvation witness: the
     mid-round window purges C at resume, P's live re-derivation at
     the SAME epoch re-names it, the purged hash is re-admissible
     (G-RULE-2 removal), C re-runs, seal {0, 1}. Starvation would
     red SealExpect (expected key missing).
   - tcG5f_Drop: demandDropOff — one derived admission silently
     dropped; P's sealed rows name a child that never ran —
     P5-OVER / SEAL-EXPECT RED (the closure oracle's kill).
   - tcG5c_P5: sweepOverreach — the seal drops an in-closure key;
     the sealed parent still names it — P5-OVER RED. */

machine TestG5aE {
    start state I {
        entry {
            var c: tGCfg;
            c = defaultGCfg();
            c.cell = 24;
            c.lineage = LIN_E;
            c.interrupt = 2;
            c.mutateBetweenAttempts = true;
            new MGEnv(c);
        }
    }
}

machine TestG5aS {
    start state I {
        entry {
            var c: tGCfg;
            c = defaultGCfg();
            c.cell = 24;
            c.lineage = LIN_S;
            c.interrupt = 2;
            c.mutateBetweenAttempts = true;
            new MGEnv(c);
        }
    }
}

machine TestG5bE {
    start state I {
        entry {
            var c: tGCfg;
            var t: tGToggles;
            c = defaultGCfg();
            c.cell = 24;
            c.lineage = LIN_E;
            c.interrupt = 2;
            c.mutateBetweenAttempts = true;
            t = defaultGToggles();
            t.sweep = false;
            c.toggles = t;
            new MGEnv(c);
        }
    }
}

machine TestG5bS {
    start state I {
        entry {
            var c: tGCfg;
            var t: tGToggles;
            c = defaultGCfg();
            c.cell = 24;
            c.lineage = LIN_S;
            c.interrupt = 2;
            c.mutateBetweenAttempts = true;
            t = defaultGToggles();
            t.sweep = false;
            c.toggles = t;
            new MGEnv(c);
        }
    }
}

machine TestG5eOff {
    start state I {
        entry {
            var c: tGCfg;
            var t: tGToggles;
            c = defaultGCfg();
            c.cell = 24;
            c.lineage = LIN_E;
            c.interrupt = 2;
            c.mutateBetweenAttempts = true;
            t = defaultGToggles();
            t.purge = false;
            c.toggles = t;
            new MGEnv(c);
        }
    }
}

machine TestG5f {
    start state I {
        entry {
            var c: tGCfg;
            c = defaultGCfg();
            c.cell = 24;
            c.lineage = LIN_E;
            c.interrupt = 2;
            new MGEnv(c);
        }
    }
}

machine TestG5fDrop {
    start state I {
        entry {
            var c: tGCfg;
            var t: tGToggles;
            c = defaultGCfg();
            c.cell = 24;
            c.lineage = LIN_E;
            c.interrupt = 2;
            t = defaultGToggles();
            t.demandDrop = true;
            c.toggles = t;
            new MGEnv(c);
        }
    }
}

machine TestG5c {
    start state I {
        entry {
            var c: tGCfg;
            var t: tGToggles;
            c = defaultGCfg();
            c.cell = 24;
            c.lineage = LIN_E;
            c.interrupt = 2;
            t = defaultGToggles();
            t.sweepOverreach = true;
            c.toggles = t;
            new MGEnv(c);
        }
    }
}

// Expected GREEN:
test tcG5aE_All [main=TestG5aE]: assert GP1, GP2, GP3prime, PGEN, PMARK, PADOPT, SealExpectG, GP5, GDEADDISPATCH in (union Graph, { TestG5aE });
test tcG5aS_All [main=TestG5aS]: assert GP1, GP2, GP3prime, PGEN, PMARK, PADOPT, SealExpectG, GP5, GDEADDISPATCH, GPASS in (union Graph, { TestG5aS });
test tcG5f_All [main=TestG5f]: assert GP1, GP2, GP3prime, PGEN, PMARK, PADOPT, SealExpectG, GP5, GDEADDISPATCH in (union Graph, { TestG5f });

// Expected RED:
test tcG5bE_P5 [main=TestG5bE]: assert GP5 in (union Graph, { TestG5bE });
test tcG5bS_P5 [main=TestG5bS]: assert GP5 in (union Graph, { TestG5bS });
test tcG5c_P5 [main=TestG5c]: assert GP5 in (union Graph, { TestG5c });
test tcG5e_Probe [main=TestG5aE]: assert PURGEPROBE in (union Graph, { TestG5aE });
test tcG5e_PurgeOff [main=TestG5eOff]: assert GDEADDISPATCH in (union Graph, { TestG5eOff });
test tcG5f_Drop [main=TestG5fDrop]: assert SealExpectG, GP5 in (union Graph, { TestG5fDrop });
