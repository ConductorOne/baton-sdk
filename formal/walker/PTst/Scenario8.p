/* Scenario 8 — external principals (SyncExternalResources x
   crash-resume; the deleteStaleExternalPrincipals contract). One
   sync, one external-phase action (cell 8): page 0 LISTs the source's
   current answer and commits the reconciliation op; page 1 COPYs the
   answer's principals. The external answer is the truthful epoch
   table (e1 = {0,1}; e2 = {0} — the between-attempt shrink drops
   principal 1). Committed copies are durable across crashes — the
   debris premise — and a resumed phase restarts from its root token.
   Expected verdicts:
   - tc8green_P8 (no interruption, no mutation): GREEN — cold baseline.
   - tc8crash_P8 (hard crash in attempt 1, shrink between attempts,
     capable engine): GREEN — every crash placement heals: the resumed
     attempt re-lists the current answer and reconciliation deletes
     the dead attempt's stale copies before the fresh writes. Includes
     the completed-then-crash schedules where attempt 2 seals attempt
     1's answer without re-running the phase — deliberately green
     (sync-scoped freshness; the P8 seal clause compares against the
     last-RUN list, not truth-at-seal).
   - tc8stop_P8 (graceful stop + shrink, capable engine): GREEN — the
     restart-from-root reset re-lists; no mid-phase cursor can copy a
     fresh answer over a stale reconciliation.
   - tc8reconOff_P8 (crash + shrink, NON-DELETING ENGINE): RED on
     P8-EXT-STALE — the warn-and-continue degrade ships the dead
     attempt's principal 1 in the sealed artifact (the SQLite
     degradation pinned by
     SQLiteExternalPrincipalResumeDegradesWithoutFailure).
   - tc8staleList_P8 (crash + shrink, resume consumes the dead
     attempt's answer): RED on P8-EXT-CURRENT — the recency mutant the
     ResumeUsesCurrentExternalAnswer chaos pin forbids. */

machine Test8Green {
    start state I {
        entry {
            var c: tScenarioCfg;
            c = defaultCfg();
            c.scenario = 8;
            c.cell = 8;
            c.nSyncs = 1;
            new MEnv(c);
        }
    }
}

machine Test8Crash {
    start state I {
        entry {
            var c: tScenarioCfg;
            c = defaultCfg();
            c.scenario = 8;
            c.cell = 8;
            c.interrupt = 2;
            c.interruptSync = 1;
            c.nSyncs = 1;
            c.mutateBetweenAttempts = true;
            new MEnv(c);
        }
    }
}

machine Test8Stop {
    start state I {
        entry {
            var c: tScenarioCfg;
            c = defaultCfg();
            c.scenario = 8;
            c.cell = 8;
            c.interrupt = 1;
            c.interruptSync = 1;
            c.nSyncs = 1;
            c.mutateBetweenAttempts = true;
            new MEnv(c);
        }
    }
}

machine Test8ReconOff {
    start state I {
        entry {
            var c: tScenarioCfg;
            c = defaultCfg();
            c.scenario = 8;
            c.cell = 8;
            c.interrupt = 2;
            c.interruptSync = 1;
            c.nSyncs = 1;
            c.mutateBetweenAttempts = true;
            c.extRecon = false;
            new MEnv(c);
        }
    }
}

machine Test8StaleList {
    start state I {
        entry {
            var c: tScenarioCfg;
            c = defaultCfg();
            c.scenario = 8;
            c.cell = 8;
            c.interrupt = 2;
            c.interruptSync = 1;
            c.nSyncs = 1;
            c.mutateBetweenAttempts = true;
            c.extStaleList = true;
            new MEnv(c);
        }
    }
}

// Expected GREEN (the shipped capable-engine path heals every
// interruption placement):
test tc8green_P8 [main=Test8Green]: assert P8 in (union Walker, { Test8Green });
test tc8crash_P8 [main=Test8Crash]: assert P8 in (union Walker, { Test8Crash });
test tc8stop_P8 [main=Test8Stop]: assert P8 in (union Walker, { Test8Stop });

// Expected RED (the degrade path's debris; the recency mutant):
test tc8reconOff_P8 [main=Test8ReconOff]: assert P8 in (union Walker, { Test8ReconOff });
test tc8staleList_P8 [main=Test8StaleList]: assert P8 in (union Walker, { Test8StaleList });
