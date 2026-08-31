/* G3 — artifact swap + rebind (walker case 3 premise re-run;
   confirmation cells, round-1 walked clean). Topology cell 25 =
   cell-11 shape: parent P (node 0) names consult node C (node 1) on
   every row.

   Script: 2 syncs, graceful stop (interrupt 1) in sync 2 — C's
   attempt-1 execution stops AFTER its consult announce (MATCH
   verdict pending, nothing committed; the stop-forced checkpoint
   captures C pending at its consult-granularity cursor). Between
   attempts the env swaps C's PREV artifact to sibling content
   (epoch 9). Resume bumps the stopped generation (g1 dies with no
   commits); the re-consult hits the ACTUALLY current (swapped) base,
   truthfully FAILs validation, and falls to fetch-fresh; the
   3-atomic closure carries over from the walker.

   nWorkers = 1: serial execution, so the stop leaves no straggler
   worker racing late commits into the stopped attempt's agen.

   Expected GREEN on both variants — the graph adds generation death
   of the stopped execution to the walker's case-3 premise, and
   nothing reds: no marker was written (stop precedes the round), no
   adoption is offered, SealExpect binds the live world. */

machine TestG3E {
    start state I {
        entry {
            var c: tGCfg;
            c = defaultGCfg();
            c.cell = 25;
            c.lineage = LIN_E;
            c.interrupt = 1;
            c.nWorkers = 1;
            new MGEnv(c);
        }
    }
}

machine TestG3S {
    start state I {
        entry {
            var c: tGCfg;
            c = defaultGCfg();
            c.cell = 25;
            c.lineage = LIN_S;
            c.interrupt = 1;
            c.nWorkers = 1;
            new MGEnv(c);
        }
    }
}

// Expected GREEN:
test tcG3_E [main=TestG3E]: assert GP1, GP2, GP3prime, PGEN, PMARK, PADOPT, SealExpectG, GP5, GDEADDISPATCH in (union Graph, { TestG3E });
test tcG3_S [main=TestG3S]: assert GP1, GP2, GP3prime, PGEN, PMARK, PADOPT, SealExpectG, GP5, GDEADDISPATCH, GP6S, GPASS in (union Graph, { TestG3S });
