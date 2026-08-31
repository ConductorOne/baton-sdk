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

// Expected RED (both are design findings of sessions variant A):
test tc2stop_P6A [main=Test2Stop]: assert P6A in (union Walker, { Test2Stop });
test tc2crash_P6A [main=Test2Crash]: assert P6A in (union Walker, { Test2Crash });

// Expected GREEN:
test tc2green_P6A [main=Test2Green]: assert P6A in (union Walker, { Test2Green });
