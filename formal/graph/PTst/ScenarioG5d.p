/* G5d — cross-variant seal-world meta-analysis (GS-CO-005(d)).
   The honest shrink chassis (cell 24, crash in sync 2, key-0
   mutation between attempts) admits MULTIPLE legitimate sealed
   worlds under sync-scoped freshness; the meta-analysis compares
   the REACHABLE world sets across lineage variants via GSEALWORLD
   existence probes (RED = the target world is reachable).

   Declared (before run, both variants):
   - W1 = {0->2}: P re-ran at e2, C's demand shrank away, C swept
     (E: purge / S: refusal / or never admitted) — REACHABLE, RED.
   - W2 = {0->1, 1->1}: P completed-across-crash at e1 (G-RULE-2),
     C live demand — REACHABLE, RED.
   - W3 = {0->2, 1->1}: the sweep-failure world (P's e2 rows name no
     C, yet C's partition seals) — UNREACHABLE, GREEN. The W1/W2
     REDs on the same chassis are the positive controls against a
     vacuous GREEN here; the sweepOff kill (tcG5bE_P5/tcG5bS_P5,
     P5-UNDER) is the registered mechanism-off contrast.

   A world reachable under exactly ONE variant is a divergence
   finding and blocks Axis-3 citation (GS-CO-005(d)). */

machine TestG5dEW1 {
    start state I {
        entry {
            var c: tGCfg;
            var w: map[int, int];
            c = defaultGCfg();
            c.cell = 24;
            c.lineage = LIN_E;
            c.interrupt = 2;
            c.mutateBetweenAttempts = true;
            w[0] = 2;
            c.sealWorld = w;
            new MGEnv(c);
        }
    }
}

machine TestG5dSW1 {
    start state I {
        entry {
            var c: tGCfg;
            var w: map[int, int];
            c = defaultGCfg();
            c.cell = 24;
            c.lineage = LIN_S;
            c.interrupt = 2;
            c.mutateBetweenAttempts = true;
            w[0] = 2;
            c.sealWorld = w;
            new MGEnv(c);
        }
    }
}

machine TestG5dEW2 {
    start state I {
        entry {
            var c: tGCfg;
            var w: map[int, int];
            c = defaultGCfg();
            c.cell = 24;
            c.lineage = LIN_E;
            c.interrupt = 2;
            c.mutateBetweenAttempts = true;
            w[0] = 1;
            w[1] = 1;
            c.sealWorld = w;
            new MGEnv(c);
        }
    }
}

machine TestG5dSW2 {
    start state I {
        entry {
            var c: tGCfg;
            var w: map[int, int];
            c = defaultGCfg();
            c.cell = 24;
            c.lineage = LIN_S;
            c.interrupt = 2;
            c.mutateBetweenAttempts = true;
            w[0] = 1;
            w[1] = 1;
            c.sealWorld = w;
            new MGEnv(c);
        }
    }
}

machine TestG5dEW3 {
    start state I {
        entry {
            var c: tGCfg;
            var w: map[int, int];
            c = defaultGCfg();
            c.cell = 24;
            c.lineage = LIN_E;
            c.interrupt = 2;
            c.mutateBetweenAttempts = true;
            w[0] = 2;
            w[1] = 1;
            c.sealWorld = w;
            new MGEnv(c);
        }
    }
}

machine TestG5dSW3 {
    start state I {
        entry {
            var c: tGCfg;
            var w: map[int, int];
            c = defaultGCfg();
            c.cell = 24;
            c.lineage = LIN_S;
            c.interrupt = 2;
            c.mutateBetweenAttempts = true;
            w[0] = 2;
            w[1] = 1;
            c.sealWorld = w;
            new MGEnv(c);
        }
    }
}

// Declared RED (reachable worlds; the probe mechanism's positive
// controls for the W3 GREEN):
test tcG5dE_W1 [main=TestG5dEW1]: assert GSEALWORLD in (union Graph, { TestG5dEW1 });
test tcG5dS_W1 [main=TestG5dSW1]: assert GSEALWORLD in (union Graph, { TestG5dSW1 });
test tcG5dE_W2 [main=TestG5dEW2]: assert GSEALWORLD in (union Graph, { TestG5dEW2 });
test tcG5dS_W2 [main=TestG5dSW2]: assert GSEALWORLD in (union Graph, { TestG5dSW2 });

// Declared GREEN (the sweep-failure world is unreachable honestly):
test tcG5dE_W3 [main=TestG5dEW3]: assert GSEALWORLD in (union Graph, { TestG5dEW3 });
test tcG5dS_W3 [main=TestG5dSW3]: assert GSEALWORLD in (union Graph, { TestG5dSW3 });
