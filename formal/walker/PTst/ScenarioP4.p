/* P4 tranche — progress (MODEL_SPEC 7 P4, 4 failure semantics).
   Attempt-level loud failure is ON in these cells only
   (cfg.loudColdFailsAttempt — decision 10's deviation closes here):
   a cold verdict fails the ATTEMPT; the offending cursor is
   checkpointed, so the failure recurs deterministically on resume.

   Stuck/ladder premise = the 5a drift script (cell 51, warmGate ON):
   sync 2 attempt 1 strands carrier C; drift lands K2 and PERSISTS
   (env latch); every resume dispatches C cold and fails loud at the
   warm gate from byte-identical restored state.
   - tcP4stuck_P4 (abandonLadder OFF): RED on P4-STUCK — attempts 2
     and 3 fail consecutively from identical restored checkpoint
     state {[C@0], hits {S:V1}} at the same step (warm gate, cursor
     0) — CO-6b-004's deterministic re-failure / stuck-resume
     finding. Budget exhaustion is the recorded outcome shape.
   - tcP4ladder_All (abandonLadder ON, k = 2, 3 syncs): GREEN incl.
     the P4Live liveness monitor — after 2 identical failures the env
     abandons sync 2 UNSEALED; sync 3 starts against the last sealed
     artifact (sync 1's), runs COLD (persisted K2 vs the K1 compat
     record — the 6c ladder's "next sync runs cold"), fetches fresh,
     and SEALS: the chain eventually seals, no wrong rows (P1's
     config clause holds: K2 rows under a K2 seal).

   Leaked-lock premise = CO-6b-007 (cell 31, no interruption): the
   carrier's replay page suffers ONE injected destination-write
   failure after acquiring the scope lock; the worker retries
   IN-ATTEMPT, re-entering the page sequence (syncOneAction's retry
   loop).
   - tcP4leak_P1 (release-on-error edge REMOVED — the mutation
     check): RED as a checker DEADLOCK — the retry re-requests the
     scope lock it still holds and waits forever on its own leak; the
     attempt never seals and the env never unblocks.
   - tcP4release_All (edge present, shipped): GREEN — the retry
     re-acquires cleanly, the page completes, the sync seals. */

machine TestP4Stuck {
    start state I {
        entry {
            var c: tScenarioCfg;
            c = defaultCfg();
            c.scenario = 8;
            c.cell = 51;
            c.interrupt = 1;
            c.baseConfig = 1;
            c.driftCompat = true;
            c.loudColdFailsAttempt = true;
            new MEnv(c);
        }
    }
}

machine TestP4Ladder {
    start state I {
        entry {
            var c: tScenarioCfg;
            c = defaultCfg();
            c.scenario = 8;
            c.cell = 51;
            c.interrupt = 1;
            c.baseConfig = 1;
            c.driftCompat = true;
            c.loudColdFailsAttempt = true;
            c.nSyncs = 3;
            c.toggles = (warmGate = true, hitValidatorBinding = true, scopeLocks = true, oncePerScope = true, annotationBinding = false, abandonLadder = true, sessionTaintWrites = false, sessionTaintAll = false);
            new MEnv(c);
        }
    }
}

machine TestP4Leak {
    start state I {
        entry {
            var c: tScenarioCfg;
            c = defaultCfg();
            c.scenario = 8;
            c.cell = 31;
            c.warmPageFails = true;
            c.lockReleaseOnError = false;
            new MEnv(c);
        }
    }
}

machine TestP4Release {
    start state I {
        entry {
            var c: tScenarioCfg;
            c = defaultCfg();
            c.scenario = 8;
            c.cell = 31;
            c.warmPageFails = true;
            new MEnv(c);
        }
    }
}

// Expected RED (stuck-resume detection; then the leaked-lock DEADLOCK —
// the checker's deadlock report, no monitor involved):
test tcP4stuck_P4 [main=TestP4Stuck]: assert P4Stuck in (union Walker, { TestP4Stuck });
test tcP4leak_P1 [main=TestP4Leak]: assert P1 in (union Walker, { TestP4Leak });

// Expected GREEN (P4Stuck is deliberately NOT asserted in the ladder
// cell: the ladder abandons exactly when detection fires — k = 2 IS
// the deterministic re-failure; the cell's claim is the recovery):
test tcP4ladder_All [main=TestP4Ladder]: assert P4Live, P1, P2 in (union Walker, { TestP4Ladder });
test tcP4release_All [main=TestP4Release]: assert P1, P2 in (union Walker, { TestP4Release });
