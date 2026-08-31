/* Scenario 4 — duplicate replay carriers (MODEL_SPEC 9 case 4).
   Two carriers with byte-distinct page tokens (distinct aids) encoding
   the same (scope, verdict); no interruption, no mutation; 2 syncs.
   Expected verdicts:
   - tc4shipped_All (oncePerScope + scopeLocks ON): GREEN — the second
     carrier's copy is deduped under the lock (grant carries the
     replayed status; the mark commits at release); its B5-legal
     copy-skipped round folds as a no-op re-publish.
   - tc4noOnce_P1 (oncePerScope OFF, locks ON): RED — P1-LEGALITY,
     both copies commit (the lock serializes but does not dedup).
   - tc4noLocks_P1 (oncePerScope ON, locks OFF): RED — P1-LEGALITY via
     the check-then-mark TOCTOU: both carriers read the replayed set
     before either transition commits the mark.
   - tc4atomic_All (V-ATOMIC re-run, v11 scope): GREEN — carriers do
     not exist under the variant (replay is inline at the consult);
     the duplicate-carrier premise is structurally unreachable. */

fun noOnceToggles(): tToggles {
    return (warmGate = true, hitValidatorBinding = true, scopeLocks = true, oncePerScope = false, annotationBinding = false, abandonLadder = false, sessionTaintWrites = false, sessionTaintAll = false);
}

fun noLocksToggles(): tToggles {
    return (warmGate = true, hitValidatorBinding = true, scopeLocks = false, oncePerScope = true, annotationBinding = false, abandonLadder = false, sessionTaintWrites = false, sessionTaintAll = false);
}

machine Test4Shipped {
    start state I {
        entry {
            var c: tScenarioCfg;
            c = defaultCfg();
            c.scenario = 4;
            c.cell = 4;
            new MEnv(c);
        }
    }
}

machine Test4NoOnce {
    start state I {
        entry {
            var c: tScenarioCfg;
            c = defaultCfg();
            c.scenario = 4;
            c.cell = 4;
            c.toggles = noOnceToggles();
            new MEnv(c);
        }
    }
}

machine Test4NoLocks {
    start state I {
        entry {
            var c: tScenarioCfg;
            c = defaultCfg();
            c.scenario = 4;
            c.cell = 4;
            c.toggles = noLocksToggles();
            new MEnv(c);
        }
    }
}

machine Test4Atomic {
    start state I {
        entry {
            var c: tScenarioCfg;
            c = defaultCfg();
            c.scenario = 4;
            c.variant = VAR_ATOMIC;
            c.cell = 4;
            new MEnv(c);
        }
    }
}

// Expected RED (mitigation kill runs):
test tc4noOnce_P1 [main=Test4NoOnce]: assert P1 in (union Walker, { Test4NoOnce });
test tc4noLocks_P1 [main=Test4NoLocks]: assert P1 in (union Walker, { Test4NoLocks });

// Expected GREEN:
test tc4shipped_All [main=Test4Shipped]: assert P1, P2, P3prime in (union Walker, { Test4Shipped });
test tc4atomic_All [main=Test4Atomic]: assert P1, P2, P3prime in (union Walker, { Test4Atomic });
