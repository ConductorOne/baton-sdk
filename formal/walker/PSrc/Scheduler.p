/* MSyncAttempt: the walker scheduler, one machine per attempt
   (MODEL_SPEC 3). LIFO action stack; batch = consecutive same-op
   prefix, cap nondet in {1,2}; spawned same-op actions are admitted to
   the live batch uncapped. Checkpoints at loop tops (between dispatch
   batches) and force-written on graceful stop, capturing live mid-batch
   state: mid-chain cursors, admitted-but-undrained spawns, hits
   recorded during the aborted batch (GLOSSARY "Checkpoint").

   The transition-commit / worker-reply / spawn-dispatch split into
   separate self-events mirrors the shipped ordering (state commit
   precedes queue admission) and is what makes the stop-stranding
   premise reachable as a genuine interleaving rather than by
   hand-placement (MODEL_SPEC 9, premise generator). */

machine MSyncAttempt {
    var env: machine;
    var store: machine;
    var upstream: machine;
    var gen: int;
    var syncN: int;
    var cfg: tScenarioCfg;
    var warm: bool;
    var aconfig: int;     // this attempt's compat config (0 = unmodeled)
    var g6: bool;         // this attempt's source-cache capability bit
    // Produce-blocked flag: ingest-quality state, volatile at
    // checkpoint-cadence durability (MODEL_SPEC 5) — restored from the
    // checkpoint token, OR'd with this attempt's install triggers,
    // durable again only at the next checkpoint and at seal. The 5b
    // crash window is a crash landing between the install-time trigger
    // and the first checkpoint carrying it.
    var blocked: bool;
    var stack: seq[tAction];
    var hits: map[int, int];
    var replayed: map[int, bool];
    var workers: seq[machine];
    var busyWith: map[machine, int];        // worker -> aid (absent = free)
    var outstanding: map[int, tAction];     // aid -> action at last committed cursor
    var owner: map[int, machine];
    var pendingSpawns: seq[tAction];
    var restoreList: seq[tAction];
    var stopping: bool;
    var storeDead: bool;   // this gen crashed; ops dropped (MODEL_SPEC 5)
    var lockHeld: map[int, machine];        // scope -> holding worker
    var lockWait: map[int, seq[machine]];   // scope -> waiting workers
    // Attempt-level loud failure (P4 cells): the failing step, plus
    // the RESTORED checkpoint's progress state for the P4Stuck record.
    var failing: bool;
    var failScope: int;
    var failReason: int;
    var failCursor: int;
    var restoredStack: seq[tAction];
    var restoredHits: map[int, int];
    var restoredReplayed: map[int, bool];

    start state Boot {
        entry (p: (env: machine, store: machine, upstream: machine, gen: int, syncN: int, cfg: tScenarioCfg, aconfig: int, g6: bool, ckpt: tCheckpoint)) {
            env = p.env; store = p.store; upstream = p.upstream;
            gen = p.gen; syncN = p.syncN; cfg = p.cfg;
            aconfig = p.aconfig; g6 = p.g6;
            stack = p.ckpt.stack;
            hits = p.ckpt.hits;
            replayed = p.ckpt.replayed;
            blocked = p.ckpt.blocked;
            restoredStack = p.ckpt.stack;
            restoredHits = p.ckpt.hits;
            restoredReplayed = p.ckpt.replayed;
            warm = true;
            if (cfg.cell == 51 || cfg.cell == 52) {
                installProduceState();
                if (storeDead) { goto DeadState; }
            }
            workers += (0, new MWorker((scheduler = this, store = store, upstream = upstream, gen = gen, syncN = syncN, cfg = cfg, warm = warm, g6 = g6, aconfig = aconfig)));
            workers += (1, new MWorker((scheduler = this, store = store, upstream = upstream, gen = gen, syncN = syncN, cfg = cfg, warm = warm, g6 = g6, aconfig = aconfig)));
            goto Running;
        }
    }

    // Warm install at attempt start (MODEL_SPEC 3,
    // installSourceCacheLookup): warm = usable-prev ∧ this attempt's G6
    // bit ∧ G4(prev not replay-blocked) ∧ G7(compat byte-match vs the
    // prev artifact's compat record) ∧ no drift this attempt (vs this
    // sync's earlier-attempt record). Produce-side block triggers fire
    // HERE (install time, MODEL_SPEC 4): trigger 1 = compat recomputed
    // differently across attempts of this sync (B4); trigger 2 = this
    // attempt runs handling-less (G6 off) over prior-attempt produce
    // state. The trigger lands in the VOLATILE blocked flag only.
    // A handling attempt then records its own compat config. Scoped to
    // the cells that model configs so calibrated op streams elsewhere
    // are undisturbed.
    fun installProduceState() {
        var prevCompat: int;
        var prevBlocked: bool;
        var curCompat: int;
        var hasCur: bool;
        var hasPrev: bool;
        send store, eProduceReadReq, (client = this, gen = gen);
        receive {
            case eProduceReadResp: (r: (prevCompat: int, prevBlocked: bool, curCompat: int, hasCur: bool, hasPrev: bool)) {
                prevCompat = r.prevCompat;
                prevBlocked = r.prevBlocked;
                curCompat = r.curCompat;
                hasCur = r.hasCur;
                hasPrev = r.hasPrev;
            }
            case eStoreDead: { storeDead = true; }
        }
        if (storeDead) { return; }
        warm = hasPrev && g6 && !prevBlocked && prevCompat == aconfig && (!hasCur || curCompat == aconfig);
        if (hasCur && curCompat != aconfig) { blocked = true; }   // trigger 1 (B4)
        if (!g6 && hasCur) { blocked = true; }                    // trigger 2 (CO-6b-003)
        if (g6) {
            send store, eCompatPut, (client = this, gen = gen, k = aconfig);
            receive {
                case eStoreAck: {}
                case eStoreDead: { storeDead = true; }
            }
        }
    }

    state Running {
        entry {
            loopTop();
        }

        on eLoopTop do {
            loopTop();
        }

        on eActionTransition do (p: (aid: int, nextCursor: int, done: bool, spawn: tAction, hasSpawn: bool, hitScope: int, hitV: int, hasHit: bool, markReplayed: bool, replayedScope: int)) {
            var act: tAction;
            var w: machine;
            // COMMIT the transition into live scheduler state first
            // (hit recording pinned at lookup time is carried here; the
            // reply to the worker is a separate self-event so a stop can
            // land between commit and continuation).
            if (p.hasHit) { hits[p.hitScope] = p.hitV; }
            if (p.markReplayed) { replayed[p.replayedScope] = true; }
            if (p.hasSpawn) { pendingSpawns += (sizeof(pendingSpawns), p.spawn); }
            if (p.done) {
                w = owner[p.aid];
                outstanding -= p.aid;
                owner -= p.aid;
                busyWith -= w;
                if (p.hasSpawn && !stopping && !failing) { send this, eDispatchPending; }
                checkBatchEnd();
            } else {
                act = outstanding[p.aid];
                act.cursor = p.nextCursor;
                outstanding[p.aid] = act;
                send this, eReplyWorker, (aid = p.aid, worker = owner[p.aid]);
                if (p.hasSpawn && !stopping && !failing) { send this, eDispatchPending; }
            }
        }

        on eReplyWorker do (p: (aid: int, worker: machine)) {
            if (stopping || failing) {
                send p.worker, eAbortWorker;
            } else {
                send p.worker, eContinuePage;
            }
        }

        on eDispatchPending do {
            dispatchPending();
        }

        on eScopeLockAcquire do (p: (worker: machine, scope: int)) {
            var q: seq[machine];
            if (stopping) {
                send p.worker, eAbortWorker;
                return;
            }
            if (p.scope in lockHeld) {
                if (p.scope in lockWait) { q = lockWait[p.scope]; }
                q += (sizeof(q), p.worker);
                lockWait[p.scope] = q;
                return;
            }
            lockHeld[p.scope] = p.worker;
            send p.worker, eScopeLockGrant, (replayed = p.scope in replayed,);
        }

        on eScopeLockRelease do (p: (scope: int, mark: bool)) {
            var q: seq[machine];
            var w: machine;
            if (p.mark) { replayed[p.scope] = true; }
            lockHeld -= p.scope;
            if (p.scope in lockWait && sizeof(lockWait[p.scope]) > 0) {
                q = lockWait[p.scope];
                w = q[0];
                q -= (0);
                lockWait[p.scope] = q;
                if (stopping) {
                    send w, eAbortWorker;
                } else {
                    lockHeld[p.scope] = w;
                    send w, eScopeLockGrant, (replayed = p.scope in replayed,);
                }
            }
        }

        on eReplayedCheckReq do (p: (worker: machine, scope: int)) {
            // Lock-free read (scopeLocks OFF): the mark lands later at
            // the transition — the TOCTOU window is real here.
            send p.worker, eReplayedCheckResp, (replayed = p.scope in replayed,);
        }

        on eHitReadReq do (p: (worker: machine, scope: int)) {
            if (stopping) {
                send p.worker, eAbortWorker;
                return;
            }
            if (p.scope in hits) {
                send p.worker, eHitReadResp, (has = true, v = hits[p.scope]);
            } else {
                send p.worker, eHitReadResp, (has = false, v = -1);
            }
        }

        on eStopAttempt do {
            var scopes: seq[int];
            var q: seq[machine];
            var i: int;
            var j: int;
            stopping = true;
            announce eAnnStop, (syncN = syncN,);
            // Grant-waiters abort at the wait point (their pages restart
            // from the current cursor on resume).
            scopes = keys(lockWait);
            i = 0;
            while (i < sizeof(scopes)) {
                q = lockWait[scopes[i]];
                j = 0;
                while (j < sizeof(q)) {
                    send q[j], eAbortWorker;
                    j = j + 1;
                }
                lockWait -= scopes[i];
                i = i + 1;
            }
            checkBatchEnd();
        }

        on eWorkerAborted do (p: (aid: int, cursor: int)) {
            var act: tAction;
            var w: machine;
            act = outstanding[p.aid];
            act.cursor = p.cursor;
            restoreList += (sizeof(restoreList), act);
            w = owner[p.aid];
            outstanding -= p.aid;
            owner -= p.aid;
            busyWith -= w;
            checkBatchEnd();
        }

        on eChainFailed do (p: (aid: int, cursor: int, scope: int, reason: int)) {
            // Attempt-level loud failure: the offending action is
            // restored AT ITS FAILING CURSOR (so the forced failure
            // checkpoint reproduces the premise and the failure recurs
            // deterministically on resume — MODEL_SPEC 4).
            var act: tAction;
            var w: machine;
            act = outstanding[p.aid];
            act.cursor = p.cursor;
            restoreList += (sizeof(restoreList), act);
            w = owner[p.aid];
            outstanding -= p.aid;
            owner -= p.aid;
            busyWith -= w;
            failing = true;
            failScope = p.scope;
            failReason = p.reason;
            failCursor = p.cursor;
            checkBatchEnd();
        }
    }

    state Done {
        ignore eStopAttempt, eActionTransition, eReplyWorker, eDispatchPending, eLoopTop, eWorkerAborted, eScopeLockRelease, eReplayedCheckReq, eChainFailed;
        on eScopeLockAcquire do (p: (worker: machine, scope: int)) {
            send p.worker, eAbortWorker;
        }
        on eHitReadReq do (p: (worker: machine, scope: int)) {
            send p.worker, eAbortWorker;
        }
    }

    // Crashed attempt: every op from this gen is dropped. Workers are
    // released (aborts) so nothing blocks; the machine parks forever.
    // MEnv resumes from the last durable checkpoint independently.
    state DeadState {
        entry {
            var i: int;
            i = 0;
            while (i < sizeof(workers)) {
                send workers[i], eAbortWorker;
                i = i + 1;
            }
        }
        on eActionTransition do (p: (aid: int, nextCursor: int, done: bool, spawn: tAction, hasSpawn: bool, hitScope: int, hitV: int, hasHit: bool, markReplayed: bool, replayedScope: int)) {
            if (p.aid in owner) {
                send owner[p.aid], eAbortWorker;
            }
        }
        ignore eStopAttempt, eReplyWorker, eDispatchPending, eLoopTop, eWorkerAborted, eScopeLockRelease, eReplayedCheckReq, eChainFailed;
        on eScopeLockAcquire do (p: (worker: machine, scope: int)) {
            send p.worker, eAbortWorker;
        }
        on eHitReadReq do (p: (worker: machine, scope: int)) {
            send p.worker, eAbortWorker;
        }
    }

    fun loopTop() {
        if (stopping) {
            finalizeStopIfQuiesced();
            return;
        }
        if (failing) {
            finalizeFailIfQuiesced();
            return;
        }
        doCheckpoint();
        if (storeDead) {
            goto DeadState;
        }
        if (sizeof(stack) == 0 && sizeof(outstanding) == 0 && sizeof(pendingSpawns) == 0) {
            doSeal();
            return;
        }
        popBatchAndDispatch();
    }

    fun doCheckpoint() {
        var ck: tCheckpoint;
        ck = buildCheckpoint();
        send store, eCheckpointReq, (client = this, gen = gen, ckpt = ck);
        receive {
            case eStoreAck: {}
            case eStoreDead: { storeDead = true; }
        }
    }

    // Checkpoint token contents pinned by MODEL_SPEC 5: action stack
    // (with cursors as currently committed, incl. admitted-but-undrained
    // spawns and outstanding mid-chain actions), hit map, replayed set.
    fun buildCheckpoint(): tCheckpoint {
        var st: seq[tAction];
        var i: int;
        var aids: seq[int];
        st = stack;
        aids = keys(outstanding);
        i = 0;
        while (i < sizeof(aids)) {
            st += (sizeof(st), outstanding[aids[i]]);
            i = i + 1;
        }
        i = 0;
        while (i < sizeof(restoreList)) {
            st += (sizeof(st), restoreList[i]);
            i = i + 1;
        }
        i = 0;
        while (i < sizeof(pendingSpawns)) {
            st += (sizeof(st), pendingSpawns[i]);
            i = i + 1;
        }
        return (stack = st, hits = hits, replayed = replayed, blocked = blocked);
    }

    fun doSeal() {
        send store, eSealReq, (client = this, gen = gen, blocked = blocked, config = aconfig);
        receive {
            case eStoreAck: {}
            case eStoreDead: { storeDead = true; }
        }
        if (storeDead) {
            goto DeadState;
        }
        send env, eAttemptEnded, (stopped = false, sealed = true, failed = false);
        goto Done;
    }

    fun popBatchAndDispatch() {
        var cap: int;
        var batch: seq[tAction];
        var act: tAction;
        var op0: tOp;
        cap = 1 + choose(2);   // nondet in {1, 2} per dispatch
        act = stack[sizeof(stack) - 1];
        stack -= (sizeof(stack) - 1);
        op0 = act.op;
        batch += (sizeof(batch), act);
        while (sizeof(stack) > 0 && sizeof(batch) < cap && stack[sizeof(stack) - 1].op == op0) {
            act = stack[sizeof(stack) - 1];
            stack -= (sizeof(stack) - 1);
            batch += (sizeof(batch), act);
        }
        while (sizeof(batch) > 0) {
            act = batch[0];
            batch -= (0);
            dispatchAction(act);
        }
    }

    fun dispatchAction(act: tAction) {
        var w: machine;
        var found: bool;
        var i: int;
        found = false;
        i = 0;
        while (i < sizeof(workers) && !found) {
            if (!(workers[i] in busyWith)) {
                w = workers[i];
                found = true;
            }
            i = i + 1;
        }
        // At loop tops cap <= worker count; spawns beyond free workers
        // wait in pendingSpawns until a worker frees up.
        if (!found) {
            pendingSpawns += (sizeof(pendingSpawns), act);
            return;
        }
        outstanding[act.aid] = act;
        owner[act.aid] = w;
        busyWith[w] = act.aid;
        send w, eDispatch, (action = act, hits = hits, replayed = replayed);
    }

    fun dispatchPending() {
        var act: tAction;
        if (stopping || failing) { return; }
        while (sizeof(pendingSpawns) > 0 && freeWorkerExists()) {
            act = pendingSpawns[0];
            pendingSpawns -= (0);
            dispatchAction(act);
        }
    }

    fun freeWorkerExists(): bool {
        var i: int;
        i = 0;
        while (i < sizeof(workers)) {
            if (!(workers[i] in busyWith)) { return true; }
            i = i + 1;
        }
        return false;
    }

    fun checkBatchEnd() {
        if (sizeof(outstanding) > 0) { return; }
        if (stopping) {
            finalizeStopIfQuiesced();
            return;
        }
        if (failing) {
            finalizeFailIfQuiesced();
            return;
        }
        if (sizeof(pendingSpawns) > 0) {
            dispatchPending();
            return;
        }
        send this, eLoopTop;
    }

    fun finalizeStopIfQuiesced() {
        if (sizeof(outstanding) > 0) { return; }
        // Force stop-checkpoint of live state: remaining stack, aborted
        // actions at their committed cursors, undrained spawns, and the
        // hits/replayed recorded during the aborted batch.
        doCheckpoint();
        if (storeDead) {
            goto DeadState;
        }
        send env, eAttemptEnded, (stopped = true, sealed = false, failed = false);
        goto Done;
    }

    // Attempt-level loud failure (P4): quiesce in-flight pages, force
    // the failure checkpoint (the offending cursor is IN it — the
    // deterministic-recurrence pin), announce the failure record for
    // P4Stuck, and report the failed attempt to the env.
    fun finalizeFailIfQuiesced() {
        if (sizeof(outstanding) > 0) { return; }
        doCheckpoint();
        if (storeDead) {
            goto DeadState;
        }
        announce eAnnAttemptFailed, (syncN = syncN, gen = gen, stack = restoredStack, hits = restoredHits, replayed = restoredReplayed, scope = failScope, reason = failReason, cursor = failCursor);
        send env, eAttemptEnded, (stopped = false, sealed = false, failed = true);
        goto Done;
    }
}
