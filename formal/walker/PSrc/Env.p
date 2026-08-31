/* MEnv: the per-scenario test driver (MODEL_SPEC 3). Owns the sync
   chain, attempt lifecycle, interruption scripting (one stop OR one
   crash per scenario-1 config), and between-attempt upstream mutation.

   Interruption timing is genuinely explored, not hand-placed: the stop
   (or crash) event is sent when the attempt starts and the P scheduler
   explores every delivery point in the attempt's lifetime. Schedules
   where it lands too late (after seal) are vacuous and legal.

   Crash quiesce (MODEL_SPEC 5): eCrash's queue position at MStore
   partitions the dead attempt's ops; dropped ops are never acked, so
   dead machines block forever and are abandoned. After eCrashAck no
   dead-attempt op can commit (its position was behind the crash). The
   sealed-read disambiguates "attempt beat the crash" schedules. */

machine MEnv {
    var store: machine;
    var upstream: machine;
    var cfg: tScenarioCfg;
    var stopUsed: bool;
    var crashUsed: bool;
    // Scenario-5 premise flag, re-evaluated per attempt from the
    // restored checkpoint: the stop-stranding premise landed iff the
    // checkpoint stack holds a replay-annotated carrier. The scripted
    // drift inputs (compat recompute, G6 withdrawal) apply only in
    // premise histories — non-premise schedules run undrifted and
    // green, so exploration never manufactures out-of-premise alarms.
    var premise: bool;
    // Once compat drift lands it PERSISTS (the recomputed config is
    // the new reality): later attempts AND later syncs run K2 — the
    // 6c-ladder cell's abandoned-sync successor is cold against the
    // K1 base exactly because of this latch.
    var drifted: bool;
    // Consecutive loud attempt failures this sync (resume-on-failure
    // bookkeeping; abandonLadder abandons after k = 2).
    var failCount: int;

    start state Run {
        entry (c: tScenarioCfg) {
            var syncN: int;
            var attempt: int;
            var gen: int;
            var ckpt: tCheckpoint;
            var sched: machine;
            var syncDone: bool;
            var crashedThisAttempt: bool;
            var sealedSeen: bool;
            var stoppedSeen: bool;
            var failedSeen: bool;
            var interrupted: bool;
            cfg = c;
            interrupted = false;
            announce eAnnScenarioInit, (maxStaleness = 1,);
            store = new MStore();
            upstream = new MUpstream();
            if (cfg.preMutate) {
                // Case-3 world: upstream sits at e2 from the start (and
                // never moves), so the fabricated sibling artifact B
                // (rows at e1) is content-distinct from rows(up).
                mutateUpstream();
            }
            syncN = 1;
            while (syncN <= cfg.nSyncs) {
                // P2 corollary-run scoping: the verification sync is
                // meaningful only in histories where the scripted
                // interruption landed (MODEL_SPEC 7, "corollary runs").
                if (cfg.verificationOnlyIfInterrupted && syncN == cfg.nSyncs && !interrupted) {
                    break;
                }
                send store, eStoreReset, (client = this, syncN = syncN, sessVariant = cfg.sessVariant);
                receive { case eStoreAck: {} }
                announce eAnnSyncStart, (syncN = syncN,);
                if (cfg.cell == 7) {
                    announceCounterfactual(syncN);
                }
                if (cfg.cell == 8) {
                    announceExtTruth(syncN);
                }
                attempt = 1;
                syncDone = false;
                failCount = 0;
                while (!syncDone) {
                    assert attempt <= 3, "attempt budget exceeded (MODEL_SPEC small scope)";
                    gen = syncN * 10 + attempt;
                    ckpt = attemptCheckpoint(attempt);
                    premise = hasStrandedCarrier(ckpt);
                    if (cfg.driftCompat && premise && syncN == cfg.interruptSync && attempt >= 2) {
                        drifted = true;
                    }
                    if ((cfg.cell == 51 || cfg.cell == 52) && syncN == cfg.interruptSync && attempt == 2 && premise) {
                        announceSealExpectation(syncN);
                    }
                    sched = new MSyncAttempt((env = this, store = store, upstream = upstream, gen = gen, syncN = syncN, cfg = cfg, aconfig = attemptConfig(syncN, attempt), g6 = attemptG6(syncN, attempt), ckpt = ckpt));
                    crashedThisAttempt = false;
                    if ((cfg.interrupt == 1 || cfg.interrupt == 3) && syncN == cfg.interruptSync && attempt == 1 && !stopUsed) {
                        stopUsed = true;
                        send sched, eStopAttempt;
                    }
                    if ((cfg.interrupt == 2 && attempt == 1 || cfg.interrupt == 3 && attempt == 2) && syncN == cfg.interruptSync && !crashUsed) {
                        crashUsed = true;
                        crashedThisAttempt = true;
                        send store, eCrashArm, (client = this, gen = gen);
                    }
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
                            // The attempt sealed before the crash landed;
                            // consume its end report and finish the sync.
                            receive { case eAttemptEnded: (r: (stopped: bool, sealed: bool, failed: bool)) {} }
                            syncDone = true;
                        } else {
                            // Dead attempt (quiesced by ack); apply the
                            // between-attempt script and resume.
                            interrupted = true;
                            betweenAttempts(syncN);
                            attempt = attempt + 1;
                        }
                    } else {
                        sealedSeen = false;
                        stoppedSeen = false;
                        failedSeen = false;
                        receive {
                            case eAttemptEnded: (r: (stopped: bool, sealed: bool, failed: bool)) {
                                sealedSeen = r.sealed;
                                stoppedSeen = r.stopped;
                                failedSeen = r.failed;
                            }
                        }
                        if (sealedSeen) {
                            syncDone = true;
                        } else {
                            assert stoppedSeen || failedSeen, "attempt ended neither sealed, stopped, nor failed";
                            if (failedSeen) {
                                // Resume-on-failure (MODEL_SPEC 3): the
                                // ladder abandons the sync UNSEALED
                                // after k = 2 identical failures; the
                                // next sync starts against the last
                                // sealed artifact (6c ladder).
                                failCount = failCount + 1;
                                if (cfg.toggles.abandonLadder && failCount >= 2) {
                                    syncDone = true;
                                } else {
                                    betweenAttempts(syncN);
                                    attempt = attempt + 1;
                                }
                            } else {
                                interrupted = true;
                                betweenAttempts(syncN);
                                attempt = attempt + 1;
                            }
                        }
                    }
                }
                // Overlay-family premise: one upstream mutation between
                // sync 1 and sync 2 (e1 -> e2), none afterwards.
                if (cfg.mutateBetweenSyncs && syncN == 1) {
                    mutateUpstream();
                }
                syncN = syncN + 1;
            }
            goto Finished;
        }
    }

    state Finished {
        ignore eAttemptEnded, eCrashAck;
    }

    fun attemptCheckpoint(attempt: int): tCheckpoint {
        var root: seq[tAction];
        var ck: tCheckpoint;
        var got: bool;
        if (attempt == 1) {
            return rootCheckpoint();
        }
        send store, eReadCheckpointReq, (client = this,);
        receive {
            case eReadCheckpointResp: (r: (ckpt: tCheckpoint, hasCkpt: bool)) {
                ck = r.ckpt;
                got = r.hasCkpt;
            }
        }
        if (!got) {
            // Crash before any post-Init checkpoint: restart-from-root.
            return rootCheckpoint();
        }
        return ck;
    }

    // Fresh sync root: empty hit map and replayed set (per-sync volatile
    // scheduler state). Cell 2 roots TWO same-op actions — H the session
    // writer (aid 1) and G the session reader (aid 2) — one batch when
    // the nondet cap allows; every other cell roots one planning action.
    fun rootCheckpoint(): tCheckpoint {
        var root: seq[tAction];
        if (cfg.cell == 2) {
            root += (0, (aid = 1, op = OP_PLANNING, scope = 0, cursor = 0, hasAnnotation = false, annotationV = -1, publishes = false));
            root += (1, (aid = 2, op = OP_PLANNING, scope = 0, cursor = 0, hasAnnotation = false, annotationV = -1, publishes = false));
            return (stack = root, hits = default(map[int, int]), replayed = default(map[int, bool]), blocked = false);
        }
        if (cfg.cell == 21) {
            // P6-C chassis: cell 2's actors with the ROOT ORDER
            // REVERSED — LIFO pops the writer H first, so a loop-top
            // checkpoint can commit H's session write while the reader
            // G still has its run ahead of it. That is the amnesia
            // premise (checkpoint-committed value + a re-run read),
            // structurally unreachable in cell 2 where G pops first.
            root += (0, (aid = 2, op = OP_PLANNING, scope = 0, cursor = 0, hasAnnotation = false, annotationV = -1, publishes = false));
            root += (1, (aid = 1, op = OP_PLANNING, scope = 0, cursor = 0, hasAnnotation = false, annotationV = -1, publishes = false));
            return (stack = root, hits = default(map[int, int]), replayed = default(map[int, bool]), blocked = false);
        }
        if (cfg.cell == 7) {
            // Kinds in sequential phases: W (top, popped first) and R
            // carry DIFFERENT ops, so the same-op batch prefix never
            // spans both — R's phase starts only after W's completes.
            root += (0, (aid = 2, op = OP_CARRIER, scope = 1, cursor = 0, hasAnnotation = false, annotationV = -1, publishes = false));
            root += (1, (aid = 1, op = OP_PLANNING, scope = 0, cursor = 0, hasAnnotation = false, annotationV = -1, publishes = false));
            return (stack = root, hits = default(map[int, int]), replayed = default(map[int, bool]), blocked = false);
        }
        if (cfg.cell == 53) {
            // C1-probe root: ONE action carrying the mid-chain replay
            // annotation in its token from the start (the policy
            // places replay at page 1; the annotation's validator is
            // truthful V1). Sync 1 never reaches it (consult misses ->
            // fresh chain at cursor 2).
            root += (0, (aid = 1, op = OP_PLANNING, scope = 0, cursor = 0, hasAnnotation = true, annotationV = 1, publishes = true));
            return (stack = root, hits = default(map[int, int]), replayed = default(map[int, bool]), blocked = false);
        }
        root += (0, (aid = 1, op = OP_PLANNING, scope = 0, cursor = 0, hasAnnotation = false, annotationV = -1, publishes = false));
        return (stack = root, hits = default(map[int, int]), replayed = default(map[int, bool]), blocked = false);
    }

    // The stop-stranding premise witness: a replay-annotated carrier
    // sits in the restored checkpoint stack (spawned by the planning
    // consult, undrained when the interruption checkpointed).
    fun hasStrandedCarrier(ck: tCheckpoint): bool {
        var i: int;
        i = 0;
        while (i < sizeof(ck.stack)) {
            if (ck.stack[i].hasAnnotation) { return true; }
            i = i + 1;
        }
        return false;
    }

    // Per-attempt compat config (0 = configs unmodeled). 5a drift
    // (trigger 1): attempts >= 2 of the drift sync recompute K2 =
    // K1 + 1 — only in premise histories (see the premise var note),
    // PERSISTING once landed (the drifted latch).
    fun attemptConfig(n: int, attempt: int): int {
        if (cfg.baseConfig == 0) { return 0; }
        if (drifted) { return cfg.baseConfig + 1; }
        return cfg.baseConfig;
    }

    // Per-attempt G6 capability bit. 5b withdrawal (trigger 2): attempt
    // 2 of the drift sync EXACTLY runs handling-less — attempt 3 (the
    // crash cell's resume) has handling back, which is what makes the
    // lost block observable rather than re-detected.
    fun attemptG6(n: int, attempt: int): bool {
        if (cfg.withdrawG6 && premise && n == cfg.interruptSync && attempt == 2) {
            return false;
        }
        return true;
    }

    // Scenario-5 seal-state oracle (MODEL_SPEC 9.5b): announced when
    // the premise lands, checked by the SealExpect monitor at this
    // sync's seal. Both 5a and 5b expect a BLOCKED seal (trigger 1 /
    // trigger 2 fired at attempt-2 install); the 5b stop cell also
    // pins the silent dropout (partition[S] empty). The 5b crash cell
    // wants blocked but may legally seal rows (attempt 3 is warm), and
    // its red verdict is the crash-window: a schedule sealing
    // UNBLOCKED because the trigger's flag died with attempt 2.
    fun announceSealExpectation(n: int) {
        announce eAnnExpectSeal, (syncN = n, scope = 0, wantBlocked = true, wantScopeEmpty = cfg.cell == 52 && cfg.interrupt == 1);
    }

    // Between-attempt premise script: base swap (case 3), upstream
    // mutation (drift premises). Order: swap first — both are env-level
    // initial-condition surgery for the NEXT attempt.
    fun betweenAttempts(n: int) {
        if (cfg.swapBase) {
            swapBaseArtifact();
        }
        if (cfg.mutateBetweenAttempts) {
            mutateUpstream();
        }
        // Scenario 8: the truth ghost follows every mutation, so the
        // P8 CURRENT clause always compares an attempt's list against
        // the answer that was live when the attempt listed.
        if (cfg.cell == 8 && cfg.mutateBetweenAttempts) {
            announceExtTruth(n);
        }
    }

    // Case-3 sibling artifact B: truthful rows at e1 under validator 1;
    // equal compat record (compat is not modeled as a distinguishing
    // axis here — both artifacts are warm-installable); content-distinct
    // from rows(up) = rows(e2) because id 1 exists only at e1.
    fun swapBaseArtifact() {
        var rowsB: seq[tRow];
        rowsB += (0, (id = 0, epoch = 1, hops = 0, config = 0, stamp = -1));
        rowsB += (1, (id = 1, epoch = 1, hops = 0, config = 0, stamp = -1));
        send store, eSwapBase, (client = this, scope = 0, vB = 1, rowsB = rowsB);
        receive { case eStoreAck: {} }
    }

    fun mutateUpstream() {
        send upstream, eMutate, (client = this, scope = 0);
        receive { case eMutateAck: {} }
    }

    // P6-R counterfactual ghost (MODEL_SPEC 7): W's policy writes
    // K = f(epoch of W's scope) as its phase-final value, so the
    // all-fresh counterfactual at this sync is exactly that epoch —
    // computed by a read, never by a second execution. Sound only
    // because case-7 configs mutate upstream BETWEEN syncs (single
    // epoch per (sync, scope)).
    fun announceCounterfactual(n: int) {
        var e: int;
        send upstream, eValidateReq, (client = this, scope = 0, v = -1);
        receive {
            case eValidateResp: (r: (ok: bool, epoch: int)) { e = r.epoch; }
        }
        announce eAnnCounterfactual, (syncN = n, key = 0, val = e);
    }

    // Scenario-8 truth ghost: the external source's CURRENT answer
    // (the truthful epoch table at the current epoch) — computed by a
    // read, never by an execution, like the P6-R counterfactual.
    fun announceExtTruth(n: int) {
        var e: int;
        send upstream, eValidateReq, (client = this, scope = 0, v = -1);
        receive {
            case eValidateResp: (r: (ok: bool, epoch: int)) { e = r.epoch; }
        }
        announce eAnnExtTruth, (syncN = n, ids = upstreamRowIds(e));
    }
}
