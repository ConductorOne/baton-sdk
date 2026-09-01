/* Scenario 6 — atomic-unit design variant, fetch-fresh flavor
   (MODEL_SPEC 9 case 6). The bake-off pair plus the V-ATOMIC re-runs
   of the scenario-1 premise family. Expected verdicts:
   - tc6naive_P1 (V-NAIVE: marker as a separate op outside any unit,
     crash script): P1-CONTENT red — a crash between eCopyScope and the
     marker leaves unmarked debris; the resumed attempt re-consults,
     revalidation fails, and the fresh round unions over the debris.
   - tc6atomic_All (V-ATOMIC, same crash script): GREEN — the unit
     {clear, copy, marker, publish} holds one queue position; every
     crash placement leaves either nothing or the complete unit, and
     the marker suppresses re-derivation on resume.
   - tc6atomicStop_All (V-ATOMIC, stop-stranding script of 1a/1b):
     GREEN — replay executes inline on the consulting page, so the
     stranded-carrier premise is structurally unreachable.
   Both variants run with the shipped mitigation toggles ON; the
   variant is a commit-structure change, not a toggle (MODEL_SPEC 9.6). */

machine Test6Naive {
    start state I {
        entry {
            var c: tScenarioCfg;
            c = defaultCfg();
            c.scenario = 6;
            c.variant = VAR_NAIVE;
            c.cell = 3;
            c.interrupt = 2;
            c.mutateBetweenAttempts = true;
            new MEnv(c);
        }
    }
}

machine Test6Atomic {
    start state I {
        entry {
            var c: tScenarioCfg;
            c = defaultCfg();
            c.scenario = 6;
            c.variant = VAR_ATOMIC;
            c.cell = 3;
            c.interrupt = 2;
            c.mutateBetweenAttempts = true;
            new MEnv(c);
        }
    }
}

machine Test6AtomicStop {
    start state I {
        entry {
            var c: tScenarioCfg;
            c = defaultCfg();
            c.scenario = 6;
            c.variant = VAR_ATOMIC;
            c.interrupt = 1;
            c.mutateBetweenAttempts = true;
            new MEnv(c);
        }
    }
}

/* v10 addendum — overlay flavor. 6-overlay runs with oncePerScope AND
   scopeLocks OFF (the structural claim 1d could not make); the marker
   inside the unit is the dedup. 6-overlay-naive runs shipped toggles;
   its defect is the unit boundary, which no toggle repairs.
   mutateBetweenAttempts stays ON in the crash cells (the pre-refactor
   env mutated unconditionally on crash; the green claims were made
   under that broader drift and are preserved as-is). */

fun overlayToggles(): tToggles {
    return (warmGate = true, hitValidatorBinding = true, scopeLocks = false, oncePerScope = false, annotationBinding = false, abandonLadder = false, sessionTaintWrites = false, sessionTaintAll = false);
}

machine Test6OverlayCrash {
    start state I {
        entry {
            var c: tScenarioCfg;
            c = defaultCfg();
            c.scenario = 6;
            c.variant = VAR_OVERLAY_UNIT;
            c.cell = 3;
            c.interrupt = 2;
            c.toggles = overlayToggles();
            c.mutateBetweenAttempts = true;
            c.mutateBetweenSyncs = true;
            new MEnv(c);
        }
    }
}

machine Test6OverlayStop {
    start state I {
        entry {
            var c: tScenarioCfg;
            c = defaultCfg();
            c.scenario = 6;
            c.variant = VAR_OVERLAY_UNIT;
            c.interrupt = 1;
            c.toggles = overlayToggles();
            c.mutateBetweenSyncs = true;
            new MEnv(c);
        }
    }
}

/* MS-CO-001 (parallel-review F6) — the o-iv-removal mutant: the
   6-overlay STOP config with the consult-reset removed (o4Mutant).
   In sub-case (b)'s schedule the resume honors the restored mid-chain
   cursor with an EMPTY collect buffer, collects only the final overlay
   page, and commits a unit missing the first page's ops → partition
   diverges from the self-grounding fold's rows(e2). Expected RED
   (P1-CONTENT); kills the one load-bearing line the overlay flavor
   adds beyond V-ATOMIC. */
machine Test6OverlayMutO4 {
    start state I {
        entry {
            var c: tScenarioCfg;
            c = defaultCfg();
            c.scenario = 6;
            c.variant = VAR_OVERLAY_UNIT;
            c.interrupt = 1;
            c.toggles = overlayToggles();
            c.mutateBetweenSyncs = true;
            c.o4Mutant = true;
            new MEnv(c);
        }
    }
}

machine Test6OverlayNaive {
    start state I {
        entry {
            var c: tScenarioCfg;
            c = defaultCfg();
            c.scenario = 6;
            c.variant = VAR_OVERLAY_NAIVE;
            c.cell = 3;
            c.interrupt = 2;
            c.mutateBetweenAttempts = true;
            c.mutateBetweenSyncs = true;
            new MEnv(c);
        }
    }
}

/* v11 (round-7 F2) — the third placement gets its own cell. Same crash
   premise as 6-overlay-naive; the placement differs: NO unit — clear+
   copy per-page at the replay boundary, marker+publish LAST as two
   separate trailing ops. Expected RED via w2 (crash between marker and
   publish: marked, entry-less, content-complete scope suppressed on
   resume; non-empty partition vs EMPTY fold). w1 (crash before the
   marker) must NOT alarm: the re-verdict's clear wipes the debris and
   the cross-attempt double copy is legal under the complete-rounds
   replacement-counting pin — the pre-pin monitor would have raised
   P1-LEGALITY on this converging history. */
machine Test6OverlayLast {
    start state I {
        entry {
            var c: tScenarioCfg;
            c = defaultCfg();
            c.scenario = 6;
            c.variant = VAR_OVERLAY_LAST;
            c.cell = 3;
            c.interrupt = 2;
            c.mutateBetweenAttempts = true;
            c.mutateBetweenSyncs = true;
            new MEnv(c);
        }
    }
}

machine Test6OverlayNaiveVerify {
    start state I {
        entry {
            var c: tScenarioCfg;
            c = defaultCfg();
            c.scenario = 6;
            c.variant = VAR_OVERLAY_NAIVE;
            c.cell = 3;
            c.interrupt = 2;
            c.mutateBetweenAttempts = true;
            c.mutateBetweenSyncs = true;
            c.nSyncs = 3;
            c.verificationOnlyIfInterrupted = true;
            new MEnv(c);
        }
    }
}

// Expected RED:
test tc6naive_P1 [main=Test6Naive]: assert P1 in (union Walker, { Test6Naive });
test tc6overlayNaive_P1 [main=Test6OverlayNaive]: assert P1 in (union Walker, { Test6OverlayNaive });
test tc6overlayNaive_P2 [main=Test6OverlayNaiveVerify]: assert P2 in (union Walker, { Test6OverlayNaiveVerify });
test tc6overlayLast_P1 [main=Test6OverlayLast]: assert P1 in (union Walker, { Test6OverlayLast });
test tc6overlayMutO4_P1 [main=Test6OverlayMutO4]: assert P1 in (union Walker, { Test6OverlayMutO4 });

// Expected GREEN (the structural claim):
test tc6atomic_All [main=Test6Atomic]: assert P1, P2, P3prime in (union Walker, { Test6Atomic });
test tc6atomicStop_All [main=Test6AtomicStop]: assert P1, P2, P3prime in (union Walker, { Test6AtomicStop });
test tc6overlay_All [main=Test6OverlayCrash]: assert P1, P2, P3prime in (union Walker, { Test6OverlayCrash });
test tc6overlayStop_All [main=Test6OverlayStop]: assert P1, P2, P3prime in (union Walker, { Test6OverlayStop });
