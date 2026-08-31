/* MWorker: executes whole action chains page by page (syncOneAction's
   whole-chain ownership, MODEL_SPEC 3). Each page's store ops commit
   (acked) before the transition is reported; the worker then awaits
   continue/abort, so a graceful stop aborts at page boundaries with
   the current page's work committed (MODEL_SPEC 5).

   Scenario-1 connector policy (scripted, pure — MODEL_SPEC 9 case 1):
   planning chain cursors: 0 = consult page (may spawn carrier [cell 1]
   or replay inline [cell 1c]); 1 = re-consult page; 2,3 = fresh round
   pages (fetch + upsert; publish on the last). Carrier actions
   (hasAnnotation) run the MODEL_SPEC 4 replay page sequence.

   scopeLocks note: upsert pages are single atomic store ops (the
   MODEL_SPEC 1 store abstraction), and no scenario-1 cell's verdict
   depends on lock-excluded interleavings, so the lock itself is not
   yet contended in this build; the toggle becomes load-bearing in the
   case-4/7 cells (check-then-mark TOCTOU, leaked-lock retry). */

machine MWorker {
    var scheduler: machine;
    var store: machine;
    var upstream: machine;
    var gen: int;
    var syncN: int;
    var cfg: tScenarioCfg;
    var warm: bool;
    var g6: bool;       // this attempt's source-cache capability bit (B1)
    var aconfig: int;   // this attempt's compat config (0 = unmodeled)
    var action: tAction;
    var hits: map[int, int];
    var replayedSnap: map[int, bool];
    var dead: bool;   // this gen crashed; park (ops dropped, MODEL_SPEC 5)
    var abortedMid: bool;  // aborted at an in-page wait (lock grant)
    var failedChain: bool; // attempt-level loud failure reported (P4 cells)
    var warmFailUsed: bool; // the one-shot CO-6b-007 write-failure injection
    // V-OVERLAY-UNIT volatile collect buffer (MODEL_SPEC 5 row): worker
    // memory only, never checkpointed; buffer loss forces re-consult
    // via the marker-absent resume rule (pin o-iv).
    var ovFrom: int;
    var ovTo: int;
    var bufUp: seq[tRow];
    var bufRm: seq[int];
    // Case-7 reader: the session-derived stamp applied to this chain's
    // fresh rows (-1 = the chain has no session derivation; 0 = miss).
    var pageStamp: int;

    state Idle {
        on eDispatch do (p: (action: tAction, hits: map[int, int], replayed: map[int, bool])) {
            action = p.action;
            hits = p.hits;
            replayedSnap = p.replayed;
            dead = false;
            abortedMid = false;
            failedChain = false;
            warmFailUsed = false;
            // Pin o-iv (V-OVERLAY-UNIT): the collect buffer is volatile,
            // so cursor continuation without it is undefined; the model
            // realizes the spec's transition-deferral pin (round-7 F1,
            // CALIBRATION decision 21) as a reset to the consult page.
            // Honest price: at-least-once re-fetch, lost work but never
            // debris. The o4Mutant (MS-CO-001 F6 kill) REMOVES the
            // reset: the resume honors the mid-chain cursor with an
            // empty buffer and commits a unit missing the earlier
            // pages' ops.
            if (cfg.variant == VAR_OVERLAY_UNIT && action.cursor != 0 && !cfg.o4Mutant) {
                action.cursor = 0;
            }
            bufUp = default(seq[tRow]);
            bufRm = default(seq[int]);
            pageStamp = -1;
            runChain();
        }
        // A stale abort can reach an idle worker when its last
        // transition raced the stop; nothing to roll back.
        ignore eAbortWorker;
    }

    fun runChain() {
        var res: (nextCursor: int, done: bool, spawn: tAction, hasSpawn: bool, hitScope: int, hitV: int, hasHit: bool, markReplayed: bool, replayedScope: int);
        var aborted: bool;
        aborted = false;
        while (!aborted) {
            res = execPage();
            if (dead) { return; }
            // Attempt-level loud failure: eChainFailed already sent;
            // no transition — the scheduler restores the action at the
            // offending cursor and quiesces the attempt.
            if (failedChain) { return; }
            if (abortedMid) {
                send scheduler, eWorkerAborted, (aid = action.aid, cursor = action.cursor);
                return;
            }
            send scheduler, eActionTransition, (aid = action.aid, nextCursor = res.nextCursor, done = res.done, spawn = res.spawn, hasSpawn = res.hasSpawn, hitScope = res.hitScope, hitV = res.hitV, hasHit = res.hasHit, markReplayed = res.markReplayed, replayedScope = res.replayedScope);
            if (res.done) { return; }
            receive {
                case eContinuePage: {
                    action.cursor = res.nextCursor;
                }
                case eAbortWorker: {
                    send scheduler, eWorkerAborted, (aid = action.aid, cursor = res.nextCursor);
                    aborted = true;
                }
            }
        }
    }

    // Executes the page at action.cursor; returns the transition.
    fun execPage(): (nextCursor: int, done: bool, spawn: tAction, hasSpawn: bool, hitScope: int, hitV: int, hasHit: bool, markReplayed: bool, replayedScope: int) {
        // C1-probe shape (MODEL_SPEC 9.5 C1, cell 53): ONE action whose
        // policy places the replay annotation MID-CHAIN — page 0
        // consults (hit recorded at lookup), page 1 is the annotated
        // replay page, pages 2+ are the fresh chain (consult miss /
        // revalidation failure). A stop between pages 0 and 1
        // checkpoints the mid-chain cursor + the hit map; the resumed
        // page 1 performs NO fresh consult and the hit check passes on
        // the RESTORED map — the C1Probe monitor's red is exactly that
        // witness.
        if (cfg.cell == 53) {
            if (action.cursor == 0) {
                return c1ConsultPage();
            }
            if (action.cursor == 1) {
                return replayPage(action.annotationV, action.publishes, action.aid * 100, false, -1);
            }
            return freshPage();
        }
        if (action.hasAnnotation) {
            return replayPage(action.annotationV, action.publishes, action.aid * 100, false, -1);
        }
        if (cfg.cell == 2) {
            return sessionPage();
        }
        if (cfg.cell == 7 && (action.cursor == 0 || action.cursor == 1)) {
            return kindPage7();
        }
        if (action.cursor >= 4) {
            return overlayPage();
        }
        if (action.cursor == 0 || action.cursor == 1) {
            return consultPage();
        }
        return freshPage();
    }

    // Case-7 kind chains (sessions x replay, MODEL_SPEC 9.7). Kind =
    // (op, scope): W = scope 0 (producer), R = scope 1 (reader); the
    // env roots them with DIFFERENT ops so batching runs the phases
    // sequentially. Page 0 consults the kind's own scope:
    // - valid hit (and, for R, an opted-in policy): warm replay INLINE
    //   (shipped carrier-less path). ELISION IS STRUCTURAL: the fresh
    //   enumeration — and W's session write inside it — never runs.
    // - otherwise fetch-fresh: W writes K = f(this sync's epoch) as the
    //   enumeration side effect; R reads K and stamps its fresh rows
    //   with the read value (0 = read-miss); chains continue on the
    //   shared fresh pages (cursors 2, 3).
    fun kindPage7(): (nextCursor: int, done: bool, spawn: tAction, hasSpawn: bool, hitScope: int, hitV: int, hasHit: bool, markReplayed: bool, replayedScope: int) {
        var hit: bool;
        var v: int;
        var ok: bool;
        var e: int;
        var found: bool;
        var val: int;
        send store, eLookupReq, (client = this, gen = gen, scope = action.scope);
        receive {
            case eLookupResp: (r: (hit: bool, v: int)) {
                hit = r.hit;
                v = r.v;
            }
            case eStoreDead: { dead = true; }
        }
        if (dead) { return mkTransition(0, true, false, -1, false); }
        ok = false;
        e = -1;
        if (hit) {
            hits[action.scope] = v;
            send upstream, eValidateReq, (client = this, scope = action.scope, v = v);
            receive {
                case eValidateResp: (r: (ok: bool, epoch: int)) {
                    ok = r.ok;
                    e = r.epoch;
                }
            }
        }
        announce eAnnConsult, (syncN = syncN, scope = action.scope, hit = hit, v = v, validated = ok, epoch = e, freshFetch = false, diffVerdict = false, attempt = gen);
        if (hit && ok && !(action.scope == 1 && cfg.readerAlwaysFresh)) {
            return replayPage(v, true, action.aid * 100, true, v);
        }
        if (action.scope == 0) {
            // W fresh: the session write is part of the producer's
            // enumeration phase (side effect, op-commit durable).
            send upstream, eFetchReq, (client = this, scope = action.scope, page = 0);
            receive {
                case eFetchResp: (r: (rows: seq[tRow], epoch: int, morePages: bool)) { e = r.epoch; }
            }
            send store, eSessionSet, (client = this, gen = gen, scope = action.scope, key = 0, val = e, taint = cfg.toggles.sessionTaintWrites || cfg.toggles.sessionTaintAll);
            receive { case eStoreAck: {} case eStoreDead: { dead = true; } }
            if (dead) { return mkTransition(0, true, false, -1, false); }
            return mkTransition(2, false, hit, v, false);
        }
        // R fresh: derive the row stamp from the session read (0 = miss).
        send store, eSessionGetReq, (client = this, gen = gen, scope = action.scope, key = 0, taint = cfg.toggles.sessionTaintAll);
        receive {
            case eSessionGetResp: (r: (found: bool, val: int)) {
                found = r.found;
                val = r.val;
            }
            case eStoreDead: { dead = true; }
        }
        if (dead) { return mkTransition(0, true, false, -1, false); }
        if (found) {
            pageStamp = val;
        } else {
            pageStamp = 0;
        }
        return mkTransition(2, false, hit, v, false);
    }

    // Case-2 connector policies (session laundering, MODEL_SPEC 9.2).
    // H (aid 1): 2-page writer; EACH page derives from upstream and
    // writes the session key (op-commit durable) — a mid-chain resume
    // re-derives on its remaining pages. G (aid 2): 1-page reader;
    // reads the session key and emits a row EMBEDDING the read value.
    // A read-miss emits nothing (the reader consumes a value the writer
    // produced; absent key = nothing to embed), so read-before-write
    // races don't manufacture alarms outside the laundering premise.
    fun sessionPage(): (nextCursor: int, done: bool, spawn: tAction, hasSpawn: bool, hitScope: int, hitV: int, hasHit: bool, markReplayed: bool, replayedScope: int) {
        var e: int;
        var found: bool;
        var val: int;
        var rows: seq[tRow];
        var ghost: tRoundGhost;
        if (action.aid == 1) {
            send upstream, eFetchReq, (client = this, scope = action.scope, page = 0);
            receive {
                case eFetchResp: (r: (rows: seq[tRow], epoch: int, morePages: bool)) { e = r.epoch; }
            }
            send store, eSessionSet, (client = this, gen = gen, scope = action.scope, key = 0, val = e, taint = cfg.toggles.sessionTaintWrites || cfg.toggles.sessionTaintAll);
            receive { case eStoreAck: {} case eStoreDead: { dead = true; } }
            if (dead) { return mkTransition(0, true, false, -1, false); }
            if (action.cursor == 0) {
                return mkTransition(1, false, false, -1, false);
            }
            return mkTransition(0, true, false, -1, false);
        }
        send store, eSessionGetReq, (client = this, gen = gen, scope = action.scope, key = 0, taint = cfg.toggles.sessionTaintAll);
        receive {
            case eSessionGetResp: (r: (found: bool, val: int)) {
                found = r.found;
                val = r.val;
            }
            case eStoreDead: { dead = true; }
        }
        if (dead) { return mkTransition(0, true, false, -1, false); }
        if (!found) {
            return mkTransition(0, true, false, -1, false);
        }
        ghost = (roundId = action.aid * 100, verdict = V_FRESH, consultEpoch = 0, config = 0, lastOp = true, attempt = gen);
        rows += (0, (id = 1, epoch = 0, hops = 0, config = 0, stamp = val));
        send store, eUpsertPage, (client = this, gen = gen, scope = action.scope, rows = rows, ghost = ghost);
        receive { case eStoreAck: {} case eStoreDead: { dead = true; } }
        return mkTransition(0, true, false, -1, false);
    }

    // C1-probe page 0: consult and CONTINUE mid-chain (no spawn, no
    // inline replay). Hit + valid -> the annotated replay page (cursor
    // 1); miss or revalidation failure -> the fresh chain (cursor 2).
    // Hit recording is pinned at lookup time as everywhere.
    fun c1ConsultPage(): (nextCursor: int, done: bool, spawn: tAction, hasSpawn: bool, hitScope: int, hitV: int, hasHit: bool, markReplayed: bool, replayedScope: int) {
        var hit: bool;
        var v: int;
        var ok: bool;
        var e: int;
        send store, eLookupReq, (client = this, gen = gen, scope = action.scope);
        receive {
            case eLookupResp: (r: (hit: bool, v: int)) {
                hit = r.hit;
                v = r.v;
            }
            case eStoreDead: { dead = true; }
        }
        if (dead) { return mkTransition(0, true, false, -1, false); }
        ok = false;
        e = -1;
        if (hit) {
            send upstream, eValidateReq, (client = this, scope = action.scope, v = v);
            receive {
                case eValidateResp: (r: (ok: bool, epoch: int)) {
                    ok = r.ok;
                    e = r.epoch;
                }
            }
        }
        announce eAnnConsult, (syncN = syncN, scope = action.scope, hit = hit, v = v, validated = ok, epoch = e, freshFetch = false, diffVerdict = false, attempt = gen);
        if (hit && ok) {
            return mkTransition(1, false, true, v, false);
        }
        return mkTransition(2, false, hit, v, false);
    }

    fun consultPage(): (nextCursor: int, done: bool, spawn: tAction, hasSpawn: bool, hitScope: int, hitV: int, hasHit: bool, markReplayed: bool, replayedScope: int) {
        var hit: bool;
        var v: int;
        var ok: bool;
        var e: int;
        var carrier: tAction;
        var marked: bool;
        var willDiff: bool;
        var ghost: tRoundGhost;
        var rp: (nextCursor: int, done: bool, spawn: tAction, hasSpawn: bool, hitScope: int, hitV: int, hasHit: bool, markReplayed: bool, replayedScope: int);
        // Cold attempt (warm install failed at attempt start): no
        // source-cache lookup exists — the connector adapts cold and
        // fetches fresh (CO-6b-005 shape). No hit is recorded.
        if (!warm && (cfg.cell == 51 || cfg.cell == 52)) {
            return mkTransition(2, false, false, -1, false);
        }
        // Scenario-6 variants: the marker check precedes the consult
        // (clause iii). A marked scope's work completed this sync;
        // re-consult and re-derivation are suppressed.
        if (cfg.variant != VAR_SHIPPED && action.cursor == 0) {
            send store, eMarkerReadReq, (client = this, gen = gen, scope = action.scope);
            receive {
                case eMarkerReadResp: (r: (marked: bool)) { marked = r.marked; }
                case eStoreDead: { dead = true; }
            }
            if (dead) { return mkTransition(0, true, false, -1, false); }
            if (marked) {
                return mkTransition(0, true, false, -1, false);
            }
        }
        send store, eLookupReq, (client = this, gen = gen, scope = action.scope);
        receive {
            case eLookupResp: (r: (hit: bool, v: int)) {
                hit = r.hit;
                v = r.v;
            }
            case eStoreDead: { dead = true; }
        }
        if (dead) { return mkTransition(0, true, false, -1, false); }
        ok = false;
        e = -1;
        if (hit) {
            // Hit recording is pinned at lookup-hit time, before and
            // regardless of the revalidation outcome (MODEL_SPEC 3).
            hits[action.scope] = v;
            send upstream, eValidateReq, (client = this, scope = action.scope, v = v);
            receive {
                case eValidateResp: (r: (ok: bool, epoch: int)) {
                    ok = r.ok;
                    e = r.epoch;
                }
            }
        }
        // Changed-with-diff verdict (overlay flavors): the diff-based
        // consult counts as consulted-against-upstream (round-5 P2 pin).
        willDiff = hit && !ok && (cfg.variant == VAR_OVERLAY_NAIVE || cfg.variant == VAR_OVERLAY_UNIT || cfg.variant == VAR_OVERLAY_LAST);
        announce eAnnConsult, (syncN = syncN, scope = action.scope, hit = hit, v = v, validated = ok, epoch = e, freshFetch = false, diffVerdict = willDiff, attempt = gen);
        if (!hit) {
            // No previous artifact surface: fetch-fresh.
            return mkTransition(2, false, false, -1, false);
        }
        if (ok) {
            if (cfg.variant != VAR_SHIPPED) {
                // Variants replay INLINE on the consulting page — no
                // carrier spawn exists (V-ATOMIC clause i; V-NAIVE
                // shares the inline shape, differing only in commit
                // structure).
                return variantReplay(v);
            }
            if (action.cursor == 0 && cfg.cell == 1) {
                // Verdict replay: spawn a same-op carrier with the
                // replay annotation in its token; chain continues.
                carrier = (aid = action.aid + 100, op = action.op, scope = action.scope, cursor = 0, hasAnnotation = true, annotationV = v, publishes = cfg.carrierPublishes);
                return (nextCursor = 1, done = false, spawn = carrier, hasSpawn = true, hitScope = action.scope, hitV = v, hasHit = true, markReplayed = false, replayedScope = -1);
            }
            if (action.cursor == 0 && (cfg.cell == 31 || cfg.cell == 51 || cfg.cell == 52)) {
                // Case-3B/5 shape: 1-page planning — consult, spawn the
                // carrier, and POP (done at the stop checkpoint). No
                // re-consult page exists; the hit map keeps V_A.
                carrier = (aid = action.aid + 100, op = action.op, scope = action.scope, cursor = 0, hasAnnotation = true, annotationV = v, publishes = cfg.carrierPublishes);
                return (nextCursor = 0, done = true, spawn = carrier, hasSpawn = true, hitScope = action.scope, hitV = v, hasHit = true, markReplayed = false, replayedScope = -1);
            }
            if (action.cursor == 0 && cfg.cell == 4) {
                // Scenario 4: first of two duplicate carriers —
                // byte-distinct tokens (distinct aids) encoding the
                // same (scope, verdict), dodging spawn dedup.
                carrier = (aid = action.aid + 100, op = action.op, scope = action.scope, cursor = 0, hasAnnotation = true, annotationV = v, publishes = cfg.carrierPublishes);
                return (nextCursor = 1, done = false, spawn = carrier, hasSpawn = true, hitScope = action.scope, hitV = v, hasHit = true, markReplayed = false, replayedScope = -1);
            }
            if (action.cursor == 1 && cfg.cell == 4) {
                // Second duplicate carrier; chain ends.
                carrier = (aid = action.aid + 200, op = action.op, scope = action.scope, cursor = 0, hasAnnotation = true, annotationV = v, publishes = cfg.carrierPublishes);
                return (nextCursor = 0, done = true, spawn = carrier, hasSpawn = true, hitScope = action.scope, hitV = v, hasHit = true, markReplayed = false, replayedScope = -1);
            }
            if (action.cursor == 0 && cfg.cell == 3) {
                // Carrier-less variant (1c): replay inline on the
                // consulting page itself; no spawn, no stop needed.
                rp = replayPage(v, true, action.aid * 100, true, v);
                return rp;
            }
            // Re-consult page (cursor 1): still valid, nothing new.
            return mkTransition(0, true, true, v, false);
        }
        if (willDiff) {
            ovFrom = v;
            ovTo = e;
            if (cfg.variant == VAR_OVERLAY_NAIVE) {
                // THE MISDRAW (6-overlay-naive): the unit {clear, copy,
                // marker, publish(V_to)} commits at the CONSULT
                // boundary; overlay pages follow per-page. The round is
                // NOT complete here (lastOp waits for the final overlay
                // page), but the marker and post-diff validator are
                // already durable.
                ghost = (roundId = action.aid * 100, verdict = V_OVERLAY, consultEpoch = e, config = 0, lastOp = false, attempt = gen);
                send store, eReplayUnit, (client = this, gen = gen, scope = action.scope, v = e, ghost = ghost);
                receive { case eStoreAck: {} case eStoreDead: { dead = true; } }
                if (dead) { return mkTransition(0, true, false, -1, false); }
            }
            if (cfg.variant == VAR_OVERLAY_LAST) {
                // THE THIRD PLACEMENT (6-overlay-last, round-7 F2): no
                // unit anywhere — clear and copy commit as two separate
                // per-page ops at the replay boundary; marker+publish
                // trail LAST (see overlayPage). A crash before the
                // marker leaves unmarked debris that the re-verdict's
                // own clear WIPES (no reduction to 6-naive's union).
                ghost = (roundId = action.aid * 100, verdict = V_OVERLAY, consultEpoch = e, config = 0, lastOp = false, attempt = gen);
                send store, eClearScope, (client = this, gen = gen, scope = action.scope, ghost = ghost);
                receive { case eStoreAck: {} case eStoreDead: { dead = true; } }
                if (dead) { return mkTransition(0, true, false, -1, false); }
                send store, eCopyScope, (client = this, gen = gen, scope = action.scope, ghost = ghost);
                receive { case eStoreAck: {} case eStoreDead: { dead = true; } }
                if (dead) { return mkTransition(0, true, false, -1, false); }
            }
            // V-OVERLAY-UNIT: no store op commits at consult; the
            // collect starts (pin o-ii).
            return mkTransition(4, false, true, v, false);
        }
        // Revalidation failed: verdict fetch-fresh; chain continues
        // with the fresh round (hit stays recorded — last-write-wins).
        return mkTransition(2, false, true, v, false);
    }

    // Overlay pages (cursors 4, 5): diff collection. V-OVERLAY-UNIT
    // buffers pages and commits ONE eOverlayUnit at the final page
    // (pins o-i/o-ii/o-iii); the naive misdraw commits per-page via the
    // shipped path (upserts, then tombstones with the round's lastOp).
    fun overlayPage(): (nextCursor: int, done: bool, spawn: tAction, hasSpawn: bool, hitScope: int, hitV: int, hasHit: bool, markReplayed: bool, replayedScope: int) {
        var page: int;
        var ups: seq[tRow];
        var rms: seq[int];
        var more: bool;
        var i: int;
        var ghost: tRoundGhost;
        page = action.cursor - 4;
        send upstream, eDiffReq, (client = this, scope = action.scope, fromEpoch = ovFrom, page = page);
        receive {
            case eDiffResp: (r: (upserts: seq[tRow], removes: seq[int], epoch: int, morePages: bool)) {
                ups = r.upserts;
                rms = r.removes;
                more = r.morePages;
            }
        }
        if (cfg.variant == VAR_OVERLAY_UNIT) {
            i = 0;
            while (i < sizeof(ups)) { bufUp += (sizeof(bufUp), ups[i]); i = i + 1; }
            i = 0;
            while (i < sizeof(rms)) { bufRm += (sizeof(bufRm), rms[i]); i = i + 1; }
            if (more) {
                return mkTransition(action.cursor + 1, false, false, -1, false);
            }
            ghost = (roundId = action.aid * 100, verdict = V_OVERLAY, consultEpoch = ovTo, config = 0, lastOp = true, attempt = gen);
            send store, eOverlayUnit, (client = this, gen = gen, scope = action.scope, v = ovTo, upserts = bufUp, removes = bufRm, ghost = ghost);
            receive { case eStoreAck: {} case eStoreDead: { dead = true; } }
            return mkTransition(0, true, false, -1, false);
        }
        // Naive and third-placement: per-page commits on the shipped path.
        if (more) {
            ghost = (roundId = action.aid * 100, verdict = V_OVERLAY, consultEpoch = ovTo, config = 0, lastOp = false, attempt = gen);
            send store, eUpsertPage, (client = this, gen = gen, scope = action.scope, rows = ups, ghost = ghost);
            receive { case eStoreAck: {} case eStoreDead: { dead = true; } }
            return mkTransition(action.cursor + 1, false, false, -1, false);
        }
        if (cfg.variant == VAR_OVERLAY_LAST) {
            // Third placement's tail: tombstones are NOT the round's
            // last prescribed op — marker and publish(V_to) trail as
            // two separate queue positions. w2 = a crash between them:
            // marked, entry-less, content-complete; clause (iii)
            // suppresses the re-execution and the scope seals with an
            // EMPTY fold (publish never committed) — the P1 witness.
            ghost = (roundId = action.aid * 100, verdict = V_OVERLAY, consultEpoch = ovTo, config = 0, lastOp = false, attempt = gen);
            send store, eTombstonePage, (client = this, gen = gen, scope = action.scope, removes = rms, ghost = ghost);
            receive { case eStoreAck: {} case eStoreDead: { dead = true; } }
            if (dead) { return mkTransition(0, true, false, -1, false); }
            send store, eMarkerPut, (client = this, gen = gen, scope = action.scope);
            receive { case eStoreAck: {} case eStoreDead: { dead = true; } }
            if (dead) { return mkTransition(0, true, false, -1, false); }
            ghost = (roundId = action.aid * 100, verdict = V_OVERLAY, consultEpoch = ovTo, config = 0, lastOp = true, attempt = gen);
            send store, ePublishEntry, (client = this, gen = gen, scope = action.scope, v = ovTo, ghost = ghost);
            receive { case eStoreAck: {} case eStoreDead: { dead = true; } }
            return mkTransition(0, true, false, -1, false);
        }
        ghost = (roundId = action.aid * 100, verdict = V_OVERLAY, consultEpoch = ovTo, config = 0, lastOp = true, attempt = gen);
        send store, eTombstonePage, (client = this, gen = gen, scope = action.scope, removes = rms, ghost = ghost);
        receive { case eStoreAck: {} case eStoreDead: { dead = true; } }
        return mkTransition(0, true, false, -1, false);
    }

    fun freshPage(): (nextCursor: int, done: bool, spawn: tAction, hasSpawn: bool, hitScope: int, hitV: int, hasHit: bool, markReplayed: bool, replayedScope: int) {
        var rows: seq[tRow];
        var e: int;
        var more: bool;
        var page: int;
        var i: int;
        var r2: tRow;
        var ghost: tRoundGhost;
        page = action.cursor - 2;
        send upstream, eFetchReq, (client = this, scope = action.scope, page = page);
        receive {
            case eFetchResp: (r: (rows: seq[tRow], epoch: int, morePages: bool)) {
                rows = r.rows;
                e = r.epoch;
                more = r.morePages;
            }
        }
        // Session-derived chains (case-7 reader) stamp their fresh rows
        // with the value read in the derivation phase; config-modeled
        // cells (case 5) tag fresh rows with this attempt's compat
        // config (the P1 clause-c ghost).
        if (pageStamp != -1 || aconfig != 0) {
            i = 0;
            while (i < sizeof(rows)) {
                r2 = rows[i];
                if (pageStamp != -1) { r2.stamp = pageStamp; }
                r2.config = aconfig;
                rows[i] = r2;
                i = i + 1;
            }
        }
        announce eAnnConsult, (syncN = syncN, scope = action.scope, hit = false, v = -1, validated = false, epoch = e, freshFetch = true, diffVerdict = false, attempt = gen);
        ghost = (roundId = action.aid * 100 + 2, verdict = V_FRESH, consultEpoch = e, config = aconfig, lastOp = false, attempt = gen);
        send store, eUpsertPage, (client = this, gen = gen, scope = action.scope, rows = rows, ghost = ghost);
        receive { case eStoreAck: {} case eStoreDead: { dead = true; } }
        if (more) {
            return mkTransition(action.cursor + 1, false, false, -1, false);
        }
        // Last fresh page: publish the new validator (epoch-valued,
        // truthful); publish is the round's last prescribed op.
        ghost = (roundId = action.aid * 100 + 2, verdict = V_FRESH, consultEpoch = e, config = aconfig, lastOp = true, attempt = gen);
        send store, ePublishEntry, (client = this, gen = gen, scope = action.scope, v = e, ghost = ghost);
        receive { case eStoreAck: {} case eStoreDead: { dead = true; } }
        return mkTransition(0, true, false, -1, false);
    }

    // MODEL_SPEC 4 replay page sequence. inline = the 1c shape (consult
    // and replay on one page; the hit was recorded this page).
    fun replayPage(vAnn: int, publishes: bool, roundId: int, inline: bool, inlineHitV: int): (nextCursor: int, done: bool, spawn: tAction, hasSpawn: bool, hitScope: int, hitV: int, hasHit: bool, markReplayed: bool, replayedScope: int) {
        var ghost: tRoundGhost;
        var baseV: int;
        var basePresent: bool;
        var doCopy: bool;
        var hitHas: bool;
        var hitNow: int;
        // B1 (MODEL_SPEC 4): a replay-annotated page arriving in an
        // attempt WITHOUT source-cache handling is SILENTLY IGNORED —
        // no-op page ops, no failure, no announce, no marks. Precedes
        // every gate (the page ops are nil, so no gate ever evaluates).
        // The 5b scripted seal-state expectation is this path's oracle.
        if (!g6) {
            return mkTransition(0, true, false, -1, false);
        }
        // warm-gate check (toggle: warmGate): a cold attempt's replay
        // page fails LOUD — announced, chain ends cold, no ops (the 5a
        // mitigation's success path; behavior, not an assert, because
        // scripted drift configs reach this gate legitimately). In P4
        // cells the loud failure fails the ATTEMPT (MODEL_SPEC 4).
        if (cfg.toggles.warmGate && !warm) {
            announce eAnnLoudCold, (syncN = syncN, scope = action.scope, reason = 2);
            if (cfg.loudColdFailsAttempt) {
                failedChain = true;
                send scheduler, eChainFailed, (aid = action.aid, cursor = action.cursor, scope = action.scope, reason = 2);
            }
            return mkTransition(0, true, false, -1, false);
        }
        // hit check (B5 provenance; loud cold on absence). The hit map
        // is ONE sync-level structure, recorded at lookup time,
        // last-write-wins — so the carrier reads it LIVE at drain time
        // (a re-consult in the same attempt may have overwritten it;
        // the case-3A rebind hole is this read seeing the overwrite).
        if (inline) {
            hitHas = true;
            hitNow = inlineHitV;
        } else {
            send scheduler, eHitReadReq, (worker = this, scope = action.scope);
            receive {
                case eHitReadResp: (r: (has: bool, v: int)) {
                    hitHas = r.has;
                    hitNow = r.v;
                }
                case eAbortWorker: { abortedMid = true; }
            }
            if (abortedMid) { return mkTransition(0, true, false, -1, false); }
        }
        assert hitHas, "unmodeled loud-cold: replay without recorded hit";
        // oncePerScope check-and-mark. Locks ON: the grant carries the
        // replayed status and the release commits the mark — atomic
        // check-then-mark. Locks OFF: lock-free read here, mark lands
        // at the action transition — the case-4 TOCTOU window.
        doCopy = true;
        if (cfg.toggles.scopeLocks) {
            send scheduler, eScopeLockAcquire, (worker = this, scope = action.scope);
            receive {
                case eScopeLockGrant: (g: (replayed: bool)) {
                    if (cfg.toggles.oncePerScope && g.replayed) { doCopy = false; }
                }
                case eAbortWorker: { abortedMid = true; }
            }
            if (abortedMid) { return mkTransition(0, true, false, -1, false); }
        } else {
            send scheduler, eReplayedCheckReq, (worker = this, scope = action.scope);
            receive {
                case eReplayedCheckResp: (r: (replayed: bool)) {
                    if (cfg.toggles.oncePerScope && r.replayed) { doCopy = false; }
                }
            }
        }
        // CO-6b-007 premise: ONE injected destination-write failure
        // after the scope lock is acquired; the worker retries the
        // action IN-ATTEMPT (mirrors syncOneAction's retry loop),
        // re-entering the page sequence from the top. With the
        // release-on-error edge present (shipped) the retry re-acquires
        // cleanly and the page completes; with the edge REMOVED (the
        // model's mutation check) the retry blocks forever on its own
        // leaked lock — the hang surfaces as a checker deadlock.
        if (cfg.warmPageFails && !warmFailUsed) {
            warmFailUsed = true;
            if (cfg.lockReleaseOnError && cfg.toggles.scopeLocks) {
                send scheduler, eScopeLockRelease, (scope = action.scope, mark = false);
            }
            return replayPage(vAnn, publishes, roundId, inline, inlineHitV);
        }
        if (doCopy) {
            if (cfg.toggles.hitValidatorBinding) {
                send store, eBaseReadReq, (client = this, gen = gen, scope = action.scope);
                receive {
                    case eBaseReadResp: (r: (v: int, present: bool)) {
                        baseV = r.v;
                        basePresent = r.present;
                    }
                    case eStoreDead: { dead = true; }
                }
                if (dead) { return mkTransition(0, true, false, -1, false); }
                if (!basePresent || baseV != hitNow) {
                    // Loud cold (CO-6b-004 kill): the binding gate
                    // detects a base the recorded hit did not attest;
                    // the chain fails cold — no copy, no publish, no
                    // wrong data. Behavior, not an assert: scenario-3
                    // schedules reach this gate legitimately and green.
                    // In P4 cells the loud failure fails the ATTEMPT.
                    announce eAnnLoudCold, (syncN = syncN, scope = action.scope, reason = 1);
                    if (cfg.toggles.scopeLocks) {
                        send scheduler, eScopeLockRelease, (scope = action.scope, mark = false);
                    }
                    if (cfg.loudColdFailsAttempt) {
                        failedChain = true;
                        send scheduler, eChainFailed, (aid = action.aid, cursor = action.cursor, scope = action.scope, reason = 1);
                    }
                    return mkTransition(0, true, false, -1, false);
                }
            }
            ghost = (roundId = roundId, verdict = V_REPLAY, consultEpoch = vAnn, config = aconfig, lastOp = false, attempt = gen);
            send store, eClearScope, (client = this, gen = gen, scope = action.scope, ghost = ghost);
            receive { case eStoreAck: {} case eStoreDead: { dead = true; } }
            ghost = (roundId = roundId, verdict = V_REPLAY, consultEpoch = vAnn, config = aconfig, lastOp = !publishes, attempt = gen);
            send store, eCopyScope, (client = this, gen = gen, scope = action.scope, ghost = ghost);
            receive { case eStoreAck: {} case eStoreDead: { dead = true; } }
        }
        if (cfg.toggles.scopeLocks) {
            send scheduler, eScopeLockRelease, (scope = action.scope, mark = doCopy);
        }
        if (publishes) {
            // A copy-skipped replacement round's publish still runs
            // (MODEL_SPEC 4: ePublishEntry even on a copy-skipped page).
            ghost = (roundId = roundId, verdict = V_REPLAY, consultEpoch = vAnn, config = aconfig, lastOp = true, attempt = gen);
            send store, ePublishEntry, (client = this, gen = gen, scope = action.scope, v = vAnn, ghost = ghost);
            receive { case eStoreAck: {} case eStoreDead: { dead = true; } }
        }
        return (nextCursor = 0, done = true, spawn = default(tAction), hasSpawn = false, hitScope = action.scope, hitV = hitNow, hasHit = inline, markReplayed = doCopy, replayedScope = action.scope);
    }

    // Scenario-6 replay-with-marker, inline on the consulting page.
    // V-NAIVE: shipped steps with the marker as a SEPARATE op after
    // eCopyScope (clear, copy, marker, publish — four queue positions;
    // a crash can land between any two). V-ATOMIC: one eReplayUnit op.
    fun variantReplay(v: int): (nextCursor: int, done: bool, spawn: tAction, hasSpawn: bool, hitScope: int, hitV: int, hasHit: bool, markReplayed: bool, replayedScope: int) {
        var ghost: tRoundGhost;
        if (cfg.variant == VAR_ATOMIC || cfg.variant == VAR_OVERLAY_NAIVE || cfg.variant == VAR_OVERLAY_UNIT) {
            ghost = (roundId = action.aid * 100, verdict = V_REPLAY, consultEpoch = v, config = 0, lastOp = true, attempt = gen);
            send store, eReplayUnit, (client = this, gen = gen, scope = action.scope, v = v, ghost = ghost);
            receive { case eStoreAck: {} case eStoreDead: { dead = true; } }
            return (nextCursor = 0, done = true, spawn = default(tAction), hasSpawn = false, hitScope = action.scope, hitV = v, hasHit = true, markReplayed = true, replayedScope = action.scope);
        }
        ghost = (roundId = action.aid * 100, verdict = V_REPLAY, consultEpoch = v, config = 0, lastOp = false, attempt = gen);
        send store, eClearScope, (client = this, gen = gen, scope = action.scope, ghost = ghost);
        receive { case eStoreAck: {} case eStoreDead: { dead = true; } }
        if (dead) { return mkTransition(0, true, false, -1, false); }
        send store, eCopyScope, (client = this, gen = gen, scope = action.scope, ghost = ghost);
        receive { case eStoreAck: {} case eStoreDead: { dead = true; } }
        if (dead) { return mkTransition(0, true, false, -1, false); }
        send store, eMarkerPut, (client = this, gen = gen, scope = action.scope);
        receive { case eStoreAck: {} case eStoreDead: { dead = true; } }
        if (dead) { return mkTransition(0, true, false, -1, false); }
        ghost = (roundId = action.aid * 100, verdict = V_REPLAY, consultEpoch = v, config = 0, lastOp = true, attempt = gen);
        send store, ePublishEntry, (client = this, gen = gen, scope = action.scope, v = v, ghost = ghost);
        receive { case eStoreAck: {} case eStoreDead: { dead = true; } }
        return (nextCursor = 0, done = true, spawn = default(tAction), hasSpawn = false, hitScope = action.scope, hitV = v, hasHit = true, markReplayed = true, replayedScope = action.scope);
    }

    fun mkTransition(next: int, done: bool, hasHit: bool, hitV: int, markReplayed: bool): (nextCursor: int, done: bool, spawn: tAction, hasSpawn: bool, hitScope: int, hitV: int, hasHit: bool, markReplayed: bool, replayedScope: int) {
        return (nextCursor = next, done = done, spawn = default(tAction), hasSpawn = false, hitScope = action.scope, hitV = hitV, hasHit = hasHit, markReplayed = markReplayed, replayedScope = action.scope);
    }

    start state Boot {
        entry (p: (scheduler: machine, store: machine, upstream: machine, gen: int, syncN: int, cfg: tScenarioCfg, warm: bool, g6: bool, aconfig: int)) {
            scheduler = p.scheduler;
            store = p.store;
            upstream = p.upstream;
            gen = p.gen;
            syncN = p.syncN;
            cfg = p.cfg;
            warm = p.warm;
            g6 = p.g6;
            aconfig = p.aconfig;
            goto Idle;
        }
    }
}
