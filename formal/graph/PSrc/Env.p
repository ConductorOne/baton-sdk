/* MGEnv: the per-scenario test driver (SPEC 3, walker parity). Owns
   the sync chain, attempt lifecycle, crash scripting (armed
   injection; timing genuinely explored), between-attempt/between-sync
   upstream mutation, and the SealExpect counterfactual (the scripted
   closure + content expectation, announced from live upstream state
   at each attempt start — the env-side independent oracle base). */

machine MGEnv {
    var store: machine;
    var upstream: machine;
    var cfg: tGCfg;
    var crash1Used: bool;
    var crash2Used: bool;

    start state Run {
        entry (c: tGCfg) {
            var syncN: int;
            var attempt: int;
            var agen: int;
            var ck: tGCkpt;
            var hasCk: bool;
            var sched: machine;
            var syncDone: bool;
            var crashedThisAttempt: bool;
            var sealedSeen: bool;
            var failCount: int;
            var attemptFailed: bool;
            cfg = c;
            announce eAnnScenarioInit, (maxStaleness = 1,);
            if (cfg.execBound > 0) {
                announce eAnnExecBound, (bound = cfg.execBound,);
            }
            if (sizeof(cfg.sealWorld) > 0) {
                announce eAnnSealWorld, (syncN = cfg.interruptSync, exp = cfg.sealWorld);
            }
            store = new MGStore();
            upstream = new MGUpstream((flapBack = cfg.flapBack,));
            syncN = 1;
            while (syncN <= cfg.nSyncs) {
                send store, eGReset, (client = this, syncN = syncN);
                receive { case eStoreAck: {} }
                announce eAnnSyncStart, (syncN = syncN,);
                attempt = 1;
                syncDone = false;
                failCount = 0;
                while (!syncDone) {
                    assert attempt <= 3, "attempt budget exceeded (SPEC 8)";
                    agen = syncN * 10 + attempt;
                    hasCk = false;
                    if (attempt > 1) {
                        send store, eGReadCkptReq, (client = this,);
                        receive {
                            case eGReadCkptResp: (r: (ck: tGCkpt, has: bool)) {
                                ck = r.ck;
                                hasCk = r.has;
                            }
                        }
                    }
                    announceExpectation(syncN);
                    // Arm-and-confirm BEFORE creating the attempt: an
                    // arm racing the attempt can land after the seal,
                    // never resolve, and deadlock this machine (the
                    // store's at-seal resolution point only sees arms
                    // queued ahead of the seal).
                    crashedThisAttempt = false;
                    if (syncN == cfg.interruptSync) {
                        if ((cfg.interrupt == 2 || cfg.interrupt == 3) && attempt == 1 && !crash1Used) {
                            crash1Used = true;
                            crashedThisAttempt = true;
                        }
                        if (cfg.interrupt == 3 && attempt == 2 && !crash2Used) {
                            crash2Used = true;
                            crashedThisAttempt = true;
                        }
                        if (crashedThisAttempt) {
                            send store, eCrashArm, (client = this, agen = agen);
                            receive { case eCrashArmed: {} }
                        }
                    }
                    sched = new MGraphSched((env = this, store = store, upstream = upstream, agen = agen, syncN = syncN, attempt = attempt, cfg = cfg, ck = ck, has = hasCk));
                    if (crashedThisAttempt) {
                        receive { case eCrashAck: {} }
                        send store, eReadSealedReq, (client = this,);
                        sealedSeen = false;
                        receive {
                            case eReadSealedResp: (r: (sealed: bool)) {
                                sealedSeen = r.sealed;
                            }
                        }
                        if (sealedSeen) {
                            // The attempt sealed before the crash
                            // landed; consume its end report.
                            receive { case eGAttemptEnded: (r: (sealed: bool, failed: bool)) {} }
                            syncDone = true;
                        } else {
                            betweenAttempts();
                            attempt = attempt + 1;
                        }
                    } else {
                        attemptFailed = false;
                        receive {
                            case eGAttemptEnded: (r: (sealed: bool, failed: bool)) {
                                sealedSeen = r.sealed;
                                attemptFailed = r.failed;
                            }
                        }
                        if (attemptFailed) {
                            // Loud failure (G7): the abandon ladder
                            // gives up after 2 identical fingerprints
                            // (the fail script is deterministic, so
                            // consecutive fingerprints are identical
                            // by construction); without it the retry
                            // loop re-fails until P4-STUCK fires.
                            failCount = failCount + 1;
                            if (cfg.ladder && failCount >= 2) {
                                announce eAnnAbandon, (syncN = syncN,);
                                syncDone = true;
                            } else {
                                betweenAttempts();
                                attempt = attempt + 1;
                            }
                        } else if (cfg.interrupt == 1 && syncN == cfg.interruptSync && attempt == 1) {
                            // Scripted graceful stop (G3): the flagged
                            // consult always fires before any seal.
                            assert !sealedSeen, "stop-scripted attempt sealed";
                            betweenAttempts();
                            attempt = attempt + 1;
                        } else {
                            assert sealedSeen, "attempt ended unsealed without a crash script";
                            syncDone = true;
                        }
                    }
                }
                if (cfg.mutateBetweenSyncs && syncN == 1) {
                    mutateUpstream(mutKey());
                }
                syncN = syncN + 1;
            }
            goto FinishedEnv;
        }
    }

    state FinishedEnv {
        ignore eGAttemptEnded, eCrashAck, eCrashArmed;
    }

    // The mutation target: the consult node's scope (cell 11), the
    // writer's scope (cell 21), or the PARENT's own scope (cell 24 —
    // the G5 demand-shrink family mutates what the parent derives
    // demand FROM).
    fun mutKey(): int {
        if (cfg.cell == 21) { return 2; }
        if (cfg.cell == 24) { return 0; }
        return 1;
    }

    fun betweenAttempts() {
        if (cfg.mutateBetweenAttempts) {
            mutateUpstream(mutKey());
        }
        if (cfg.cell == 25) {
            // G3: swap the PREV artifact for C's key to sibling
            // content between attempts (epoch 9 never validates).
            send store, eGSwapPrev, (client = this, key = 1, epoch = 9);
            receive { case eStoreAck: {} }
        }
    }

    fun mutateUpstream(k: int) {
        send upstream, eMutate, (client = this, scope = k);
        receive { case eMutateAck: {} }
    }

    // SealExpect counterfactual (SPEC 7/9): the expected sealed key
    // set is the cell's demand closure; the expected content per key
    // is rows at the key's LIVE upstream epoch, read (never executed)
    // at attempt start. Later attempts overwrite the expectation, so
    // the check always binds the sealing attempt's world.
    fun announceExpectation(syncN: int) {
        var exp: map[int, int];
        var excluded: map[int, bool];
        exp[0] = epochOf(0);
        if (cfg.cell == 11 || cfg.cell == 25 || cfg.cell == 27) {
            exp[1] = epochOf(1);
        }
        if (cfg.cell == 21) {
            exp[2] = epochOf(2);
            exp[3] = epochOf(3);
        }
        if (cfg.cell == 24) {
            // C is demanded only while P's row 1 exists (epoch 1).
            // After the shrink C's key is EXCLUDED, not merely
            // unexpected: whether it seals depends on which attempt's
            // parent world survives (sync-scoped freshness) — the
            // structural question belongs to GP5's artifact closure,
            // not the counterfactual.
            if (epochOf(0) <= 1) {
                exp[1] = epochOf(1);
            } else {
                excluded[1] = true;
            }
        }
        if (cfg.cell == 26) {
            // Chain P -> S1 -> C -> GC; the C/GC tail is demanded
            // only while S1's row 1 exists (G6b's shrink excludes it,
            // same sync-scoped reasoning as cell 24).
            exp[1] = epochOf(1);
            if (epochOf(1) <= 1) {
                exp[4] = epochOf(4);
                exp[5] = epochOf(5);
            } else {
                excluded[4] = true;
                excluded[5] = true;
            }
        }
        if (cfg.cell == 28) {
            exp[1] = epochOf(1);
            exp[4] = epochOf(4);
            exp[5] = epochOf(5);
        }
        if (cfg.cell == 29) {
            // Key 1 is poisoned on every schedule (two distinct
            // derivations both commit it); eAnnPoison excludes it in
            // the monitor — the expectation still names it so a
            // MISSING poison (defense failure) surfaces.
            exp[1] = epochOf(1);
        }
        announce eAnnExpectSeal, (syncN = syncN, exp = exp, excluded = excluded);
    }

    fun epochOf(k: int): int {
        var e: int;
        send upstream, eValidateReq, (client = this, scope = k, v = -1);
        receive {
            case eValidateResp: (r: (ok: bool, epoch: int)) { e = r.epoch; }
        }
        return e;
    }
}
