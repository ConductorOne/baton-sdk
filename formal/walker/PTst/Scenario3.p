/* Scenario 3 — artifact swap + hit rebind (MODEL_SPEC 9 case 3).
   World: upstream sits at e2 throughout (preMutate) and never moves.
   Sync 1 seals artifact A = rows(e2) under V_A = 2. Sync 2 is the
   premise sync: the stop strands the carrier and MEnv swaps the
   previous artifact to sibling B = rows(e1) under V_B = 1 (equal
   compat; truthful validators — V_B does not validate against e2, and
   rows(B) != rows(up) = rows(A)). Expected verdicts:
   - tc3a_P1 (shipped: hitValidatorBinding ON — the residual hole):
     RED. Attempt 2's re-consult overwrites the hit map with V_B
     (lookup-time recording, last-write-wins); revalidation fails;
     verdict fetch-fresh. The carrier then passes the binding check
     (hit V_B == base B's manifest V_B) and installs rows(B) while
     publishing its annotation's V_A. Carrier-last schedules seal
     rows(B) under entry V_A -> P1-ATTEST-SEAL; carrier-first or
     interleaved schedules leave B's id-1 debris under the fresh round
     -> P1-CONTENT. Schedules where the carrier drains BEFORE the
     re-consult hit the binding gate honestly (hit still V_A != base
     V_B) -> loud cold, green: both faces of the same toggle.
   - tc3a_P2: GREEN in every cell — attempt 1's validation match
     qualifies the scope as consulted this sync, and copied rows carry
     hops 1 (corrected expectation; spec v2 wrongly claimed a P2 red).
   - tc3b_P1 (pre-CO-6b-004: hitValidatorBinding OFF, 1-page planning
     that pops at the stop checkpoint): RED on a weaker premise — no
     re-consult exists, the hit map keeps V_A, and NO binding check
     runs: the carrier copies swapped base B and publishes V_A ->
     P1-ATTEST-SEAL.
   - tc3bBindingOn_All (same premise, binding ON — the CO-6b-004
     kill): GREEN — the binding gate compares hit V_A to base V_B,
     fails loud-cold, and no wrong data lands (the scope seals empty
     in the premise schedules).
   The annotationBinding fix runs (3A fix + empty-validator coverage
   cell) are de-scoped from the build with the toggle itself (v11). */

fun noBindingToggles(): tToggles {
    return (warmGate = true, hitValidatorBinding = false, scopeLocks = true, oncePerScope = true, annotationBinding = false, abandonLadder = false, sessionTaintWrites = false, sessionTaintAll = false);
}

machine Test3A {
    start state I {
        entry {
            var c: tScenarioCfg;
            c = defaultCfg();
            c.scenario = 3;
            c.interrupt = 1;
            c.preMutate = true;
            c.swapBase = true;
            new MEnv(c);
        }
    }
}

machine Test3B {
    start state I {
        entry {
            var c: tScenarioCfg;
            c = defaultCfg();
            c.scenario = 3;
            c.cell = 31;
            c.interrupt = 1;
            c.preMutate = true;
            c.swapBase = true;
            c.toggles = noBindingToggles();
            new MEnv(c);
        }
    }
}

machine Test3BBindOn {
    start state I {
        entry {
            var c: tScenarioCfg;
            c = defaultCfg();
            c.scenario = 3;
            c.cell = 31;
            c.interrupt = 1;
            c.preMutate = true;
            c.swapBase = true;
            new MEnv(c);
        }
    }
}

/* v11 — the case-3 re-run under V-ATOMIC (MODEL_SPEC 9.6): the
   subsumption witness for the annotationBinding de-scope. Same stop +
   base-swap premise as 3A, but replay is consult-inline with the
   atomic unit: no carrier and no annotation exist to trust; the marker
   lives in the CURRENT sync's artifact (untouched by the swap);
   restored hits are inert. Either the unit committed before the stop
   (attempt 2 marker-suppresses — the seal is the unit's coherent
   contents) or nothing did (attempt 2 re-consults the ACTUALLY current
   base: swapped B's V1 fails validation against upstream e2 →
   fetch-fresh). Expected GREEN across P1/P2/P3'. */
machine Test3Atomic {
    start state I {
        entry {
            var c: tScenarioCfg;
            c = defaultCfg();
            c.scenario = 3;
            c.variant = VAR_ATOMIC;
            c.interrupt = 1;
            c.preMutate = true;
            c.swapBase = true;
            new MEnv(c);
        }
    }
}

// Expected RED (the residual hole, then the weaker pre-fix premise):
test tc3a_P1 [main=Test3A]: assert P1 in (union Walker, { Test3A });
test tc3b_P1 [main=Test3B]: assert P1 in (union Walker, { Test3B });

// Expected GREEN:
test tc3a_P2 [main=Test3A]: assert P2 in (union Walker, { Test3A });
test tc3bBindingOn_All [main=Test3BBindOn]: assert P1, P2, P3prime in (union Walker, { Test3BBindOn });
test tc3atomic_All [main=Test3Atomic]: assert P1, P2, P3prime in (union Walker, { Test3Atomic });
