/* Scenario 2 — session laundering (MODEL_SPEC 9 case 2, P6-A).
   One sync, two same-op actions on the root stack: H (aid 1) writes
   the session key with a value derived from upstream on EACH of its
   two pages; G (aid 2) reads the key and emits a row embedding the
   read value. Sessions variant A (shipped): the session KV is durable
   at op commit and survives attempts; committed rows are never
   invalidated by later session writes. Both red cells are EXPECTED
   FINDINGS (no fix run — variant B is the graph addendum's
   obligation). Expected verdicts:
   - tc2stop_P6A (graceful stop, upstream mutated between attempts):
     RED — the schedule where H's d1 write commits, G reads d1, emits,
     and finishes, then the stop strands H mid-chain; H alone re-runs
     and derives d2; G's committed row embeds d1 != final d2.
   - tc2crash_P6A (hard crash, at-least-once): RED — both re-run from
     root; the interleaving where G re-reads the DURABLE stale d1
     before H's re-derivation lands re-commits the d1 embed under a
     final d2. The complementary interleaving (H re-derives first, G
     embeds d2) is green — both outcomes live in this one config.
   - tc2green_P6A (no interruption, no mutation): GREEN — the reader
     embeds what the writer wrote; a read-miss emits nothing. */

machine Test2Stop {
    start state I {
        entry {
            var c: tScenarioCfg;
            c = defaultCfg();
            c.scenario = 2;
            c.cell = 2;
            c.interrupt = 1;
            c.interruptSync = 1;
            c.nSyncs = 1;
            c.mutateBetweenAttempts = true;
            new MEnv(c);
        }
    }
}

machine Test2Crash {
    start state I {
        entry {
            var c: tScenarioCfg;
            c = defaultCfg();
            c.scenario = 2;
            c.cell = 2;
            c.interrupt = 2;
            c.interruptSync = 1;
            c.nSyncs = 1;
            c.mutateBetweenAttempts = true;
            new MEnv(c);
        }
    }
}

machine Test2Green {
    start state I {
        entry {
            var c: tScenarioCfg;
            c = defaultCfg();
            c.scenario = 2;
            c.cell = 2;
            c.nSyncs = 1;
            new MEnv(c);
        }
    }
}

// P6-C cells (session-checkpoint consistency — the CO-6b-009 root
// cause). Same crash chassis as Test2Crash; the axis is
// cfg.sessVariant (the store's session semantics at the crash
// boundary). All three variants of the conversation are cells:
//   - variant 0 (shipped, durable-at-op-commit): expected RED on
//     P6-C-ZOMBIE — H's beyond-checkpoint d1 survives the crash and
//     the re-run G reads it before H's re-derivation lands.
//   - variant 1 (the REJECTED wholesale resume-clear): expected RED
//     on P6-C-AMNESIA — a checkpoint-committed d1 is destroyed and
//     G's re-read misses data whose producing work will not re-run.
//   - variant 2 (checkpoint-consistent sessions — the correct fix,
//     future work per CO-6b-009): expected GREEN, both directions.

machine Test2CrashP6C {
    start state I {
        entry {
            var c: tScenarioCfg;
            c = defaultCfg();
            c.scenario = 2;
            c.cell = 2;
            c.interrupt = 2;
            c.interruptSync = 1;
            c.nSyncs = 1;
            c.mutateBetweenAttempts = true;
            new MEnv(c);
        }
    }
}

// The amnesia and fix cells run cell 21 (root order REVERSED: the
// writer pops first). In cell 2 the reader G pops first under LIFO,
// so every checkpoint that still contains G predates H's writes — a
// checkpoint-COMMITTED session value with a reader re-run ahead of it
// (the amnesia premise) is structurally unreachable there. Verified:
// tc2clear_P6C on cell 2 is green at 10k schedules for exactly this
// reason, not because the rejected fix is sound.
machine Test2ClearP6C {
    start state I {
        entry {
            var c: tScenarioCfg;
            c = defaultCfg();
            c.scenario = 2;
            c.cell = 21;
            c.interrupt = 2;
            c.interruptSync = 1;
            c.nSyncs = 1;
            c.mutateBetweenAttempts = true;
            c.sessVariant = 1;
            new MEnv(c);
        }
    }
}

machine Test2ConsistentP6C {
    start state I {
        entry {
            var c: tScenarioCfg;
            c = defaultCfg();
            c.scenario = 2;
            c.cell = 21;
            c.interrupt = 2;
            c.interruptSync = 1;
            c.nSyncs = 1;
            c.mutateBetweenAttempts = true;
            c.sessVariant = 2;
            new MEnv(c);
        }
    }
}

// Expected RED (both are design findings of sessions variant A):
test tc2stop_P6A [main=Test2Stop]: assert P6A in (union Walker, { Test2Stop });
test tc2crash_P6A [main=Test2Crash]: assert P6A in (union Walker, { Test2Crash });

// Expected GREEN:
test tc2green_P6A [main=Test2Green]: assert P6A in (union Walker, { Test2Green });

// P6-C cells. Expected RED (shipped zombie; rejected-fix amnesia):
test tc2crash_P6C [main=Test2CrashP6C]: assert P6C in (union Walker, { Test2CrashP6C });
test tc2clear_P6C [main=Test2ClearP6C]: assert P6C in (union Walker, { Test2ClearP6C });

// Expected GREEN (checkpoint-consistent sessions close both directions):
test tc2consistent_P6C [main=Test2ConsistentP6C]: assert P6C in (union Walker, { Test2ConsistentP6C });
