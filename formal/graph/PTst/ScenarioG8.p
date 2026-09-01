/* G8 — supersession family (round-1 F6's dedicated cells).

   G8a (record REPLACES over dead debris): NOT a dedicated cell — the
   honest G1 crash cells explore every mid-record-round crash
   position and the REPLACES clear wipes the debris on re-run;
   registered as G1 sweep coverage (CALIBRATION.md).

   G8b (OVERLAY-intent dead base): honest leg — the DIFF unit's
   atomic clear + prev-copy re-derives the base; GREEN. The
   `overlayComposeDead` mutant is CONTENT-INVISIBLE in this envelope
   (2 row ids + truthful TOTAL diffs: every diff from the debris's
   base overwrites or removes every debris id) but NOT
   mechanism-invisible (calibration find G8B-CAL-1): on the crash
   re-run the skipped clear leaves the dead attempt's copy round
   LIVE under the composed diff, and P1's one-live-replacement-copy
   legality reds it. tcG8bMut_P1 is the kill [P1-LEGALITY];
   tcG8bMut_Ctl keeps the content-level oracles GREEN as the
   registered content-invisibility evidence (adequacy §10.1).

   G8c (same-key distinct-derivation race, cell 29): P names two
   DISTINCT derivations sharing output key 1 (keyOf); suppression is
   correctly silent (different hashes); the store poisons the key on
   the second derivation's first commit, VOIDS its marker (R2-M8),
   refuses adoption store-side (R3-M2), and the poison announce
   excludes the key from SealExpect; P1's legality exemption covers
   the double commit. GP5 is NOT asserted here: its key = hash - 1
   identity convention is exactly what this cell breaks.

   G8d (marker flap-back probe, R2-F4): cell 11 + flapBack + two
   crashes + between-attempt mutation (raw e1 -> e2 -> e3 with
   content(e3) = content(e1)). Attempt 1 commits C's REPLAY unit
   (marker v=1); attempt 2 re-derives FETCH-FRESH via record REPLACES
   (the clear DELETES the marker); attempt 3 finds NO marker,
   consults, v=1 MATCHes content(e3) truthfully, REPLAY verdict
   (R3-M1) — GREEN. Mutant `markerCleanupOff`: the stale marker
   survives the REPLACES; attempt 3 premise-matches it and adopts
   content the marker no longer describes — P-MARK RED. */

machine TestG8b {
    start state I {
        entry {
            var c: tGCfg;
            c = defaultGCfg();
            c.cell = 11;
            c.lineage = LIN_E;
            c.interrupt = 2;
            c.mutateBetweenSyncs = true;
            c.diffPolicy = true;
            new MGEnv(c);
        }
    }
}

machine TestG8bMut {
    start state I {
        entry {
            var c: tGCfg;
            var t: tGToggles;
            c = defaultGCfg();
            c.cell = 11;
            c.lineage = LIN_E;
            c.interrupt = 2;
            c.mutateBetweenSyncs = true;
            c.diffPolicy = true;
            t = defaultGToggles();
            t.overlayComposeDead = true;
            c.toggles = t;
            new MGEnv(c);
        }
    }
}

machine TestG8c {
    start state I {
        entry {
            var c: tGCfg;
            c = defaultGCfg();
            c.cell = 29;
            c.lineage = LIN_E;
            new MGEnv(c);
        }
    }
}

machine TestG8d {
    start state I {
        entry {
            var c: tGCfg;
            c = defaultGCfg();
            c.cell = 11;
            c.lineage = LIN_E;
            c.interrupt = 3;
            c.mutateBetweenAttempts = true;
            c.flapBack = true;
            new MGEnv(c);
        }
    }
}

machine TestG8dMut {
    start state I {
        entry {
            var c: tGCfg;
            var t: tGToggles;
            c = defaultGCfg();
            c.cell = 11;
            c.lineage = LIN_E;
            c.interrupt = 3;
            c.mutateBetweenAttempts = true;
            c.flapBack = true;
            t = defaultGToggles();
            t.markerCleanup = false;
            c.toggles = t;
            new MGEnv(c);
        }
    }
}

// Expected GREEN:
test tcG8b_All [main=TestG8b]: assert GP1, GP2, GP3prime, PGEN, PMARK, PADOPT, SealExpectG, GP5 in (union Graph, { TestG8b });
test tcG8bMut_Ctl [main=TestG8bMut]: assert GP2, GP3prime, SealExpectG, GP5 in (union Graph, { TestG8bMut });
test tcG8c_All [main=TestG8c]: assert GP1, GP2, GP3prime, PGEN, PMARK, PADOPT, SealExpectG in (union Graph, { TestG8c });
test tcG8d_All [main=TestG8d]: assert GP1, GP2, GP3prime, PGEN, PMARK, PADOPT, SealExpectG, GP5 in (union Graph, { TestG8d });

// Expected RED:
test tcG8bMut_P1 [main=TestG8bMut]: assert GP1 in (union Graph, { TestG8bMut });
test tcG8c_Poison [main=TestG8c]: assert POISONPROBE in (union Graph, { TestG8c });
test tcG8dMut_PMARK [main=TestG8dMut]: assert PMARK in (union Graph, { TestG8dMut });
