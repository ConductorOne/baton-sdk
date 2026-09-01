/* G7 — progress under churn (walker P4 analog; round-1 F11 pins).
   Cell 27 = cell-11 topology; node 1 (C) fails LOUD at execution
   start, deterministically, in sync 2 — with a GENERATION-BLIND
   fingerprint (a fingerprint hashing the generation never matches
   across bumped resumes and the stuck detector goes blind; that was
   F11's finding).

   - tcG7_Ladder: the abandon ladder gives up after 2 identical
     fingerprints — GREEN (the sync abandons, announce-visible; the
     walker's P4 tranche machinery carries over rather than
     dissolving).
   - tcG7_Stuck: ladderOff — the retry loop re-fails; the third
     identical fingerprint is P4-STUCK RED (attempt budget 3,
     SPEC 8). */

machine TestG7Ladder {
    start state I {
        entry {
            var c: tGCfg;
            c = defaultGCfg();
            c.cell = 27;
            c.lineage = LIN_E;
            c.failNode = 1;
            c.failSync = 2;
            c.ladder = true;
            new MGEnv(c);
        }
    }
}

machine TestG7Stuck {
    start state I {
        entry {
            var c: tGCfg;
            c = defaultGCfg();
            c.cell = 27;
            c.lineage = LIN_E;
            c.failNode = 1;
            c.failSync = 2;
            c.ladder = false;
            new MGEnv(c);
        }
    }
}

// Expected GREEN:
test tcG7_Ladder [main=TestG7Ladder]: assert GP1, GP2, GP3prime, PGEN, PMARK, PADOPT, SealExpectG, GP4STUCK in (union Graph, { TestG7Ladder });

// Expected RED:
test tcG7_Stuck [main=TestG7Stuck]: assert GP4STUCK in (union Graph, { TestG7Stuck });
