/* Scenario 7 — session elision under replay (MODEL_SPEC 9 case 7,
   signoff addendum). Pure two-sync scripts, no interruption machinery.
   Kinds in sequential phases: W (scope 0) writes session key K as a
   side effect of its FRESH enumeration; R (scope 1) derives its rows'
   ghost stamps from reading K. The shipped design has no coupling
   between sessions and the source cache, and R's connector violates
   no pinned obligation in any cell — the missing contract clause IS
   the design finding. Expected verdicts:
   - tc7a_P6R (write elision): RED — W warm-replays, its session write
     is structurally elided, R (policy: always fresh) reads MISS and
     stamps 0; counterfactual v1.
   - tc7a_P1P2 GREEN — required finding: every row individually
     well-formed, every scope consulted; the corruption is invisible
     to content/attestation/staleness checks.
   - tc7b_P6R (stale-read replay, the dual): RED — W's upstream moves
     between syncs (W fresh, writes v2) while R's scope is unchanged
     (R warm; copied rows carry stamp v1); counterfactual v2. No
     elided write anywhere — this cell kills write-only bans.
   - tc7c_All (both-warm control): GREEN — carried stamps v1 equal the
     counterfactual v1; required so P6-R does not overfit to "replay
     near sessions alarms".
   Fix runs (produce-side taint; the full two-sync script re-executes
   with the toggle ON — sync N's artifact differs from the red run's):
   - tc7aTaintW_P6R: GREEN — sync N taints W (write during a capable
     phase); sync N+1 consults W MISS, W re-runs fresh, K present.
   - tc7bTaintW_P6R: RED — REQUIRED residual: R's hazard is a READ;
     the write-only rule is half a fix.
   - tc7aTaintAll_P6R, tc7bTaintAll_P6R: GREEN — R's read taints R's
     kind too; replay is forfeited exactly where sessions are used
     (the toggle's honest price, recorded not hidden). */

fun taintWritesToggles(): tToggles {
    return (warmGate = true, hitValidatorBinding = true, scopeLocks = true, oncePerScope = true, annotationBinding = false, abandonLadder = false, sessionTaintWrites = true, sessionTaintAll = false);
}

fun taintAllToggles(): tToggles {
    return (warmGate = true, hitValidatorBinding = true, scopeLocks = true, oncePerScope = true, annotationBinding = false, abandonLadder = false, sessionTaintWrites = false, sessionTaintAll = true);
}

machine Test7a {
    start state I {
        entry {
            var c: tScenarioCfg;
            c = defaultCfg();
            c.scenario = 7;
            c.cell = 7;
            c.readerAlwaysFresh = true;
            new MEnv(c);
        }
    }
}

machine Test7b {
    start state I {
        entry {
            var c: tScenarioCfg;
            c = defaultCfg();
            c.scenario = 7;
            c.cell = 7;
            c.mutateBetweenSyncs = true;
            new MEnv(c);
        }
    }
}

machine Test7c {
    start state I {
        entry {
            var c: tScenarioCfg;
            c = defaultCfg();
            c.scenario = 7;
            c.cell = 7;
            new MEnv(c);
        }
    }
}

machine Test7aTaintW {
    start state I {
        entry {
            var c: tScenarioCfg;
            c = defaultCfg();
            c.scenario = 7;
            c.cell = 7;
            c.readerAlwaysFresh = true;
            c.toggles = taintWritesToggles();
            new MEnv(c);
        }
    }
}

machine Test7bTaintW {
    start state I {
        entry {
            var c: tScenarioCfg;
            c = defaultCfg();
            c.scenario = 7;
            c.cell = 7;
            c.mutateBetweenSyncs = true;
            c.toggles = taintWritesToggles();
            new MEnv(c);
        }
    }
}

machine Test7aTaintAll {
    start state I {
        entry {
            var c: tScenarioCfg;
            c = defaultCfg();
            c.scenario = 7;
            c.cell = 7;
            c.readerAlwaysFresh = true;
            c.toggles = taintAllToggles();
            new MEnv(c);
        }
    }
}

machine Test7bTaintAll {
    start state I {
        entry {
            var c: tScenarioCfg;
            c = defaultCfg();
            c.scenario = 7;
            c.cell = 7;
            c.mutateBetweenSyncs = true;
            c.toggles = taintAllToggles();
            new MEnv(c);
        }
    }
}

// Expected RED (design findings, then the required fix residual):
test tc7a_P6R [main=Test7a]: assert P6R in (union Walker, { Test7a });
test tc7b_P6R [main=Test7b]: assert P6R in (union Walker, { Test7b });
test tc7bTaintW_P6R [main=Test7bTaintW]: assert P6R in (union Walker, { Test7bTaintW });

// Expected GREEN:
test tc7a_P1P2 [main=Test7a]: assert P1, P2 in (union Walker, { Test7a });
test tc7c_All [main=Test7c]: assert P1, P2, P3prime, P6A, P6R in (union Walker, { Test7c });
test tc7aTaintW_P6R [main=Test7aTaintW]: assert P6R in (union Walker, { Test7aTaintW });
test tc7aTaintAll_P6R [main=Test7aTaintAll]: assert P6R in (union Walker, { Test7aTaintAll });
test tc7bTaintAll_P6R [main=Test7bTaintAll]: assert P6R in (union Walker, { Test7bTaintAll });
