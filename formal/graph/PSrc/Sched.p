/* MGraphSched: the frontier scheduler, one machine per attempt
   (SPEC 3). Owns the frontier, admitted-derivation set (G-RULE-2
   death semantics), generation-qualified admitted-by edges, the
   generation table (G-RULE-3), demand derivation (G-RULE-1), lineage
   state per variant, and dispatch to 2 workers.

   Announce processing is per-carrier atomic: a worker's completion
   report (eGNodeDone) hosts all derived effects — demand admissions,
   retraction enqueues (E+B), dead-read observations (S), deferred
   quiesce bumps — emitted as derived announces inside that handler
   (G-RULE-1 derived-announce carrier pin, R2-M2).

   Elective checkpoints commit at loop tops under a genuine choice
   point (SPEC 3: placement a choice point — the G1e/R3-F2 crash
   window needs skip schedules). The forced resume checkpoint (F4)
   and the mid-bump fence (R3-F2) are UNCONDITIONAL commits, removed
   only by their kill toggles. */

machine MGraphSched {
    var env: machine;
    var store: machine;
    var upstream: machine;
    var agen: int;
    var syncN: int;
    var attempt: int;
    var cfg: tGCfg;
    var frontier: seq[tPendingNode];
    var admitted: map[int, bool];
    var completedH: map[int, bool];
    var edges: map[int, seq[tEdge]];
    var genTable: map[int, int];
    var readers: seq[tReadRec];
    var retractQ: seq[int];          // reader nodes owing a re-run (E+B)
    var deferredBumps: seq[int];     // quiesce-deferred bump targets
    var workers: seq[machine];
    var busyW: map[machine, bool];
    var owner: map[int, machine];    // execId -> worker
    var outstanding: map[int, tPendingNode];
    var inFlight: map[int, int];     // node -> execId
    var execSeq: int;
    var storeDead: bool;
    var passIters: int;
    var droppedHash: int;            // demandDrop inject target (0 = unarmed)
    var carrierDirty: bool;
    var catchupDone: map[int, bool];  // reader*100+wgen -> catch-up retraction spent

    start state Boot {
        entry (p: (env: machine, store: machine, upstream: machine, agen: int, syncN: int, attempt: int, cfg: tGCfg, ck: tGCkpt, has: bool)) {
            var i: int;
            var j: int;
            var pend: tPendingNode;
            var bumped: seq[int];
            var hs: seq[int];
            var stillPending: map[int, bool];
            env = p.env; store = p.store; upstream = p.upstream;
            agen = p.agen; syncN = p.syncN; attempt = p.attempt; cfg = p.cfg;
            execSeq = agen * 100;
            if (!p.has) {
                // Fresh roots (attempt 1, or crash before any durable
                // checkpoint: restart-from-root).
                rootFrontier();
                // F4 mint fence (R3-F2 extension): the initial minted
                // table is durable BEFORE first dispatch, so a later
                // attempt always restores past these identities.
                if (cfg.toggles.resumeCkpt) {
                    doCheckpoint(true);
                    if (storeDead) { goto DeadSched; }
                }
            } else {
                frontier = p.ck.pending;
                admitted = p.ck.admitted;
                edges = p.ck.edges;
                genTable = p.ck.genTable;
                readers = p.ck.readers;
                // Resume (SPEC 3): bump every pending node (the prior
                // generation is dead)...
                i = 0;
                while (i < sizeof(frontier)) {
                    pend = frontier[i];
                    if (!inIntSeq(bumped, pend.node)) {
                        genTable[pend.node] = genTable[pend.node] + 1;
                        // Resume bumps are re-mints: bucket-aligned
                        // under compression (G9-CAL-1).
                        if (cfg.stampCompression && genTable[pend.node] % 2 == 1) {
                            genTable[pend.node] = genTable[pend.node] + 1;
                        }
                        bumped += (sizeof(bumped), pend.node);
                    }
                    pend.gen = genTable[pend.node];
                    frontier[i] = pend;
                    i = i + 1;
                }
                // ...variant E purges pending nodes under the
                // ∀-PREDICATE (R2-F5: purge only when EVERY admitted-by
                // edge names a dead generation), removing purged hashes
                // from the admitted-derivation set. Roots (no edges)
                // never purge.
                if (cfg.lineage == LIN_E && cfg.toggles.purge) {
                    i = 0;
                    while (i < sizeof(frontier)) {
                        pend = frontier[i];
                        if (pend.hash in edges && sizeof(edges[pend.hash]) > 0 && allEdgesDead(pend.hash)) {
                            announce eAnnPurge, (syncN = syncN, node = pend.node, hash = pend.hash);
                            admitted -= pend.hash;
                            frontier -= i;
                        } else {
                            i = i + 1;
                        }
                    }
                }
                // Completed iff admitted ∧ ¬pending, evaluated AFTER
                // removals (G-RULE-2 resume rule).
                i = 0;
                while (i < sizeof(frontier)) {
                    stillPending[frontier[i].hash] = true;
                    i = i + 1;
                }
                hs = keys(admitted);
                i = 0;
                while (i < sizeof(hs)) {
                    if (!(hs[i] in stillPending)) {
                        completedH[hs[i]] = true;
                    }
                    i = i + 1;
                }
                // FORCED RESUME CHECKPOINT (F4): the bumped table is
                // durable before any dispatch; resume bumps ride its
                // commit (R2-M2). resumeCkptOff is the G1b kill.
                if (cfg.toggles.resumeCkpt) {
                    doCheckpoint(true);
                    if (storeDead) { goto DeadSched; }
                }
                i = 0;
                while (i < sizeof(bumped)) {
                    announce eAnnGenBump, (syncN = syncN, node = bumped[i], newGen = genTable[bumped[i]], reason = 1);
                    i = i + 1;
                }
            }
            i = 0;
            while (i < cfg.nWorkers) {
                workers += (sizeof(workers), new MGNodeExec((sched = this, store = store, upstream = upstream, agen = agen, syncN = syncN, attempt = attempt, cfg = cfg)));
                i = i + 1;
            }
            goto Running;
        }
    }

    state Running {
        entry {
            loopTop();
        }

        on eGLoopTop do {
            loopTop();
        }

        on eGReadNote do (p: (reader: int, rgen: int, skey: int, val: int, writer: int, wgen: int)) {
            var isDead: bool;
            readers += (sizeof(readers), (reader = p.reader, rgen = p.rgen, skey = p.skey, val = p.val, writer = p.writer, wgen = p.wgen));
            isDead = p.wgen >= 0 && p.writer in genTable && p.wgen < genTable[p.writer];
            if (isDead) {
                // S observation point (ii): dead-read count.
                announce eAnnDeadRead, (syncN = syncN, reader = p.reader, skey = p.skey);
            }
            // E+B registration-side CATCH-UP retraction, ONCE per
            // (reader, dead wgen): a read that registers after its
            // value's death was already knowable may have missed the
            // re-publish carrier's retraction (note-vs-carrier race);
            // repeated retraction is exclusively RE-PUBLISH-driven
            // (R2-M6(i)), so an unretractable strand (writerAdopt:
            // the writer adopts and never re-publishes) costs one
            // bounded re-run and then SEALS, where P6-E reds the
            // still-dead final read. An unbounded registration-side
            // rule livelocks that cell instead: the frontier never
            // drains and the at-seal oracle never evaluates.
            if (isDead && cfg.lineage == LIN_E && cfg.sessVar == SESS_B && cfg.toggles.retraction) {
                if (p.reader in genTable && p.rgen == genTable[p.reader] && !((p.reader * 100 + p.wgen) in catchupDone)) {
                    catchupDone[p.reader * 100 + p.wgen] = true;
                    if (cfg.toggles.quiesce && p.reader in inFlight) {
                        if (!inIntSeq(deferredBumps, p.reader)) {
                            deferredBumps += (sizeof(deferredBumps), p.reader);
                        }
                    } else {
                        bumpAndReadmit(p.reader, 2);
                        if (storeDead) { goto DeadSched; }
                        if (sizeof(frontier) > 0) { dispatchFree(); }
                    }
                }
            }
        }

        on eGDemandNote do (p: (node: int, gen: int, key: int, rows: seq[tGRow])) {
            var i: int;
            // A dead generation's late page note derives nothing:
            // demand rides LIVE emissions (the live re-run re-derives
            // its own naming).
            if (!(p.node in genTable) || p.gen != genTable[p.node]) { return; }
            carrierDirty = false;
            i = 0;
            while (i < sizeof(p.rows)) {
                if (p.rows[i].childHash > 0) {
                    admitChild(p.rows[i].childHash, p.node, p.gen);
                }
                i = i + 1;
            }
            // The note is its own carrier: its derived effects commit
            // as ONE durable delta (GS-CO-003 discipline; GS-CO-001(b):
            // no minted generation dispatches before its table delta).
            if (carrierDirty && cfg.toggles.midBumpFence) {
                doCheckpoint(true);
            }
            if (storeDead) { goto DeadSched; }
            if (sizeof(frontier) > 0) { dispatchFree(); }
        }

        on eGNodeFail do (p: (node: int, gen: int, fingerprint: int)) {
            var i: int;
            // Loud attempt failure (G7): announce the generation-blind
            // fingerprint, park the workers, end failed. No forced
            // checkpoint — the resume restores the last durable one.
            announce eAnnAttemptFail, (syncN = syncN, attempt = attempt, node = p.node, fingerprint = p.fingerprint);
            i = 0;
            while (i < sizeof(workers)) {
                send workers[i], eGAbortWorker;
                i = i + 1;
            }
            send env, eGAttemptEnded, (sealed = false, failed = true);
            goto DoneSched;
        }

        on eGStopReq do (p: (node: int, gen: int)) {
            var i: int;
            // Graceful stop (G3): checkpoint with the stopped node
            // still pending at its cursor (buildCheckpoint includes
            // in-flight nodes), park the workers, end unsealed. The
            // store lives; resume bumps the stopped generation.
            doCheckpoint(false);
            if (storeDead) { goto DeadSched; }
            i = 0;
            while (i < sizeof(workers)) {
                send workers[i], eGAbortWorker;
                i = i + 1;
            }
            send env, eGAttemptEnded, (sealed = false, failed = false);
            goto DoneSched;
        }

        on eGNodeDone do (p: (report: tGReport)) {
            var r: tGReport;
            var i: int;
            var w: machine;
            r = p.report;
            w = owner[r.execId];
            owner -= r.execId;
            busyW -= w;
            outstanding -= r.execId;
            if (r.node in inFlight && inFlight[r.node] == r.execId) {
                inFlight -= r.node;
            }
            if (r.aborted) {
                storeDead = true;
                goto DeadSched;
            }
            completedH[r.hash] = true;
            // Retraction-queue drain (E): the re-admitted reader's
            // completion removes its entry.
            i = 0;
            while (i < sizeof(retractQ)) {
                if (retractQ[i] == r.node) {
                    retractQ -= i;
                } else {
                    i = i + 1;
                }
            }
            // Demand derivation from announced row content (G-RULE-1),
            // atomic with this carrier.
            carrierDirty = false;
            i = 0;
            while (i < sizeof(r.rows)) {
                if (r.rows[i].childHash > 0) {
                    admitChild(r.rows[i].childHash, r.node, r.gen);
                }
                i = i + 1;
            }
            // CARRIER-DURABILITY FENCE (GS-CO-003): the carrier's
            // derived effects — every admission, mint, and
            // admitted-by edge — commit durably as ONE delta, and no
            // checkpoint may separate the carrier's completion from
            // its admissions. Per-mint fencing leaves a lost-demand
            // window: crash between two children's fences restores
            // the parent COMPLETED with the second child's admission
            // gone, and a completed parent never re-derives — the
            // demand starves and the closure oracle reds an honest
            // history.
            if (carrierDirty && cfg.toggles.midBumpFence) {
                doCheckpoint(true);
            }
            if (storeDead) { goto DeadSched; }
            // Session reads register at READ time (eGReadNote,
            // R2-M1 read-through) — NOT here: carrier-time
            // registration makes the in-flight retraction race
            // (R2-F1) unreachable.
            // Re-publish reader retraction (E+B): enqueue a retraction
            // entry per reader execution of the now-dead value
            // (SPEC 3 retraction queue; keying per MSessionStore).
            if (cfg.lineage == LIN_E && cfg.sessVar == SESS_B && cfg.toggles.retraction) {
                i = 0;
                while (i < sizeof(r.pubs)) {
                    retractReaders(r.pubs[i].skey, r.node, r.pubs[i].wgen);
                    i = i + 1;
                }
            }
            // Quiesce-deferred bump lands atomically with the dying
            // execution's completion (R2-F1).
            i = 0;
            while (i < sizeof(deferredBumps)) {
                if (deferredBumps[i] == r.node) {
                    deferredBumps -= i;
                    bumpAndReadmit(r.node, 2);
                } else {
                    i = i + 1;
                }
            }
            if (storeDead) { goto DeadSched; }
            if (sizeof(outstanding) == 0) {
                send this, eGLoopTop;
            } else if (sizeof(frontier) > 0) {
                dispatchFree();
            }
        }
    }

    state DoneSched {
        ignore eGLoopTop, eGNodeDone, eGReadNote, eGDemandNote, eGStopReq, eGNodeFail;
    }

    // Crashed attempt: ops from this agen are dropped; workers park;
    // MEnv resumes independently from the last durable checkpoint.
    state DeadSched {
        entry {
            var i: int;
            i = 0;
            while (i < sizeof(workers)) {
                send workers[i], eGAbortWorker;
                i = i + 1;
            }
        }
        ignore eGLoopTop, eGNodeDone, eGReadNote, eGDemandNote, eGStopReq, eGNodeFail;
    }

    fun loopTop() {
        if (storeDead) { goto DeadSched; }
        // Elective checkpoint: placement is a genuine choice point.
        if (choose(2) == 0) {
            doCheckpoint(false);
            if (storeDead) { goto DeadSched; }
        }
        if (sizeof(frontier) == 0 && sizeof(outstanding) == 0) {
            sealPhase();
            return;
        }
        dispatchFree();
    }

    fun dispatchFree() {
        var pend: tPendingNode;
        var w: machine;
        var found: bool;
        var i: int;
        while (sizeof(frontier) > 0 && freeWorkerExists()) {
            pend = frontier[0];
            frontier -= 0;
            // S observation point (i): dispatch-time refusal — a
            // pending node all of whose admission edges name dead
            // generations is dropped (hash removed, G-RULE-2) unless
            // a live re-derivation re-admitted it.
            if (cfg.lineage == LIN_S && pend.hash in edges && sizeof(edges[pend.hash]) > 0 && allEdgesDead(pend.hash)) {
                admitted -= pend.hash;
                continue;
            }
            // Dead-demand dispatch (unreachable honestly: E purges at
            // resume, S refuses above) — the purgeOff kill's alarm.
            if (pend.hash in edges && sizeof(edges[pend.hash]) > 0 && allEdgesDead(pend.hash)) {
                announce eAnnDeadDispatch, (syncN = syncN, node = pend.node, hash = pend.hash);
            }
            found = false;
            i = 0;
            while (i < sizeof(workers) && !found) {
                if (!(workers[i] in busyW)) {
                    w = workers[i];
                    found = true;
                }
                i = i + 1;
            }
            execSeq = execSeq + 1;
            outstanding[execSeq] = pend;
            owner[execSeq] = w;
            busyW[w] = true;
            inFlight[pend.node] = execSeq;
            announce eAnnExec, (syncN = syncN, attempt = attempt, node = pend.node, gen = pend.gen);
            send w, eGDispatch, (pend = pend, execId = execSeq, attempt = attempt, stop = cfg.interrupt == 1 && syncN == cfg.interruptSync && attempt == 1 && pend.node == 1);
        }
    }

    fun freeWorkerExists(): bool {
        var i: int;
        i = 0;
        while (i < sizeof(workers)) {
            if (!(workers[i] in busyW)) { return true; }
            i = i + 1;
        }
        return false;
    }

    // Demand admission (G-RULE-2): MUST suppress iff the hash is
    // pending or completed this sync; purge/refusal removals make a
    // live re-derivation re-admissible. suppressionOff duplicates run
    // at the SAME generation (identity-duplicate mutant; the racing
    // schedule is the P1-LEGALITY first-find, sequential schedules
    // adopt with the declared live-fromGen deviation, R2-N1).
    fun admitChild(h: int, parent: int, pgen: int) {
        var node: int;
        var g: int;
        var es: seq[tEdge];
        var dup: bool;
        var dupGen: int;
        var i: int;
        dup = false;
        i = 0;
        while (i < sizeof(frontier)) {
            if (frontier[i].hash == h) { dup = true; dupGen = frontier[i].gen; }
            i = i + 1;
        }
        i = 0;
        while (i < sizeof(keys(outstanding))) {
            if (outstanding[keys(outstanding)[i]].hash == h) { dup = true; dupGen = outstanding[keys(outstanding)[i]].gen; }
            i = i + 1;
        }
        if (cfg.toggles.suppression && (dup || h in completedH)) {
            // Edge-only registration still dirties the carrier: a
            // LOST live admitted-by edge mis-arms the ∀-purge
            // predicate on resume (GS-CO-003 covers edges too).
            // Idempotent per (parent, pgen): the page note and the
            // completion carrier derive the same emission once.
            if (!hasEdge(h, parent, pgen)) {
                if (h in edges) { es = edges[h]; }
                es += (sizeof(es), (parent = parent, pgen = pgen));
                edges[h] = es;
                carrierDirty = true;
            }
            return;
        }
        if (cfg.toggles.demandDrop && (droppedHash == 0 || droppedHash == h)) {
            // INJECT (G5f kill): silently drop every admission of the
            // first hash a derivation names — a lost derivation
            // PATHWAY, not a lost message (per-announce notes and the
            // completion carrier derive the same emission twice, so a
            // single-message drop is always healed; calibration find).
            droppedHash = h;
            return;
        }
        node = h - 1;
        if (dup) {
            g = dupGen;
        } else {
            if (node in genTable) {
                genTable[node] = genTable[node] + 1;
                // Refusal re-admissions are re-mints: bucket-aligned
                // under compression (G9-CAL-1, see bumpAndReadmit).
                if (cfg.stampCompression && genTable[node] % 2 == 1) {
                    genTable[node] = genTable[node] + 1;
                }
            } else {
                genTable[node] = 1;
            }
            g = genTable[node];
        }
        admitted[h] = true;
        if (h in completedH) { completedH -= h; }
        if (!hasEdge(h, parent, pgen)) {
            if (h in edges) { es = edges[h]; }
            es += (sizeof(es), (parent = parent, pgen = pgen));
            edges[h] = es;
        }
        frontier += (sizeof(frontier), (node = node, kind = kindOf(cfg.cell, node), hash = h, key = keyOf(cfg.cell, node), gen = g));
        carrierDirty = true;
        // Durability rides the CARRIER fence (GS-CO-003), committed
        // once after the whole demand loop — never per-mint.
    }

    fun hasEdge(h: int, parent: int, pgen: int): bool {
        var i: int;
        var es: seq[tEdge];
        if (!(h in edges)) { return false; }
        es = edges[h];
        i = 0;
        while (i < sizeof(es)) {
            if (es[i].parent == parent && es[i].pgen == pgen) { return true; }
            i = i + 1;
        }
        return false;
    }

    // E+B retraction (SPEC 3): a re-publish under wgen retracts every
    // reader execution of the same key's earlier (now-dead) value —
    // including re-runs that read a stale value before the re-publish
    // landed (the G-pending re-retraction clause, R2-M6(i)).
    fun retractReaders(skey: int, writer: int, wgen: int) {
        var i: int;
        var rd: int;
        var seen: seq[int];
        i = 0;
        while (i < sizeof(readers)) {
            if (readers[i].skey == skey && readers[i].writer == writer && readers[i].wgen < wgen) {
                rd = readers[i].reader;
                if (!inIntSeq(seen, rd) && rd in genTable && readers[i].rgen == genTable[rd]) {
                    seen += (sizeof(seen), rd);
                    if (!inIntSeq(retractQ, rd)) {
                        retractQ += (sizeof(retractQ), rd);
                    }
                    // QUIESCE-BEFORE-BUMP (R2-F1): defer while the
                    // dying generation's execution is in flight;
                    // quiesceOff is the G1d kill.
                    if (cfg.toggles.quiesce && rd in inFlight) {
                        if (!inIntSeq(deferredBumps, rd)) {
                            deferredBumps += (sizeof(deferredBumps), rd);
                        }
                    } else {
                        bumpAndReadmit(rd, 2);
                    }
                }
            }
            i = i + 1;
        }
    }

    // Mid-attempt bump + re-admission (retraction- or observation-
    // forced). MID-BUMP FENCE (R3-F2): the bump's generation-table
    // delta commits durably (forced checkpoint) between the bump's
    // carrier announce and the new generation's first dispatch;
    // midBumpFenceOff is the G1e kill.
    fun bumpAndReadmit(node: int, reason: int) {
        var h: int;
        var i: int;
        var updated: bool;
        var pend: tPendingNode;
        genTable[node] = genTable[node] + 1;
        // Under compression every scheduler RE-mint is BUCKET-ALIGNED
        // (even): floor-bucketed stamps prove liveness only on bucket
        // boundaries, and an unaligned heal re-creates the odd
        // generation it just chased one demand-level down — the pass
        // then needs O(demand-depth) iterations and reds PASS-BUDGET
        // on honest histories (G9-CAL-1). First-admission mints stay
        // at 1, so the mixed-parity stamp population compression must
        // digest — and its redo cost — remain in the model.
        if (cfg.stampCompression && genTable[node] % 2 == 1) {
            genTable[node] = genTable[node] + 1;
        }
        announce eAnnGenBump, (syncN = syncN, node = node, newGen = genTable[node], reason = reason);
        h = node + 1;
        admitted[h] = true;
        if (h in completedH) { completedH -= h; }
        // If the node is already pending, re-ground that entry at the
        // new generation instead of duplicating it (a duplicate would
        // dispatch the dead generation a second time).
        updated = false;
        i = 0;
        while (i < sizeof(frontier)) {
            if (frontier[i].node == node) {
                pend = frontier[i];
                pend.gen = genTable[node];
                frontier[i] = pend;
                updated = true;
            }
            i = i + 1;
        }
        if (!updated) {
            frontier += (sizeof(frontier), (node = node, kind = kindOf(cfg.cell, node), hash = h, key = keyOf(cfg.cell, node), gen = genTable[node]));
        }
        if (cfg.toggles.midBumpFence) {
            doCheckpoint(true);
            if (storeDead) { return; }
        }
        announce eAnnReadmit, (syncN = syncN, node = node, hash = h, gen = genTable[node], reason = reason);
    }

    fun allEdgesDead(h: int): bool {
        var i: int;
        var es: seq[tEdge];
        es = edges[h];
        i = 0;
        while (i < sizeof(es)) {
            if (!(es[i].parent in genTable) || es[i].pgen >= genTable[es[i].parent]) {
                return false;
            }
            i = i + 1;
        }
        return true;
    }

    // SEAL SEQUENCE (SPEC 3): frontier drained -> pre-seal pass (S) /
    // retraction queue empty (E) -> SWEEP -> eSeal.
    fun sealPhase() {
        var stamps: map[int, map[int, int]];
        var owners: map[int, int];
        var ks: seq[int];
        var ns: seq[int];
        var i: int;
        var j: int;
        var forced: bool;
        var got: bool;
        var keep: seq[int];
        // The pass's domain is the DEMAND CLOSURE: a dead stamp on an
        // out-of-closure key owes nothing — the sweep drops the key.
        // Chasing it burns the pass budget on an honest history (the
        // G5 shrink chassis: the refused node's key keeps its stale
        // stamp forever). Recomputed on every sealPhase entry, so a
        // forced re-run's naming changes are honored next scan.
        keep = computeClosure();
        if (storeDead) { goto DeadSched; }
        if (cfg.lineage == LIN_S) {
            // PRE-SEAL PASS (observation point (iii)): one iteration =
            // one scan over a drained frontier (R3-M3); budget <= 3
            // (R2-M7); a budget-exhausted seal is announce-visible and
            // P6-S catches surviving dead stamps at seal.
            if (passIters < 3) {
                got = false;
                send store, eGReadStampsReq, (client = this, agen = agen);
                receive {
                    case eGReadStampsResp: (r: (stamps: map[int, map[int, int]], owners: map[int, int])) {
                        stamps = r.stamps;
                        owners = r.owners;
                        got = true;
                    }
                    case eStoreDead: { storeDead = true; }
                }
                if (!got) { goto DeadSched; }
                passIters = passIters + 1;
                announce eAnnPassIter, (syncN = syncN, iter = passIters);
                forced = false;
                ks = keys(stamps);
                i = 0;
                while (i < sizeof(ks)) {
                    if (!inIntSeq(keep, ks[i])) { i = i + 1; continue; }
                    ns = keys(stamps[ks[i]]);
                    j = 0;
                    while (j < sizeof(ns)) {
                        if (ns[j] in genTable && staleStamp(stamps[ks[i]][ns[j]], genTable[ns[j]]) ) {
                            if (!forcedAlready(ks[i], owners)) {
                                bumpAndReadmit(owners[ks[i]], 3);
                                if (storeDead) { goto DeadSched; }
                                forced = true;
                            }
                            // Compressed AMBIGUOUS entry (G9-CAL-1):
                            // floor(s) == cur - 1 with cur ODD means s
                            // may EQUAL cur — a live entry the owner's
                            // re-run cannot make provable (the re-read
                            // merges the named node's unchanged odd
                            // generation forever). The heal also bumps
                            // the NAMED node onto an even generation:
                            // parity ambiguity dies with the odd gen,
                            // and any raced re-read that merged the
                            // old gen becomes unambiguously DEAD for
                            // the next scan's owner-bump rule.
                            if (cfg.stampCompression && ns[j] != owners[ks[i]]
                                    && genTable[ns[j]] % 2 == 1
                                    && stamps[ks[i]][ns[j]] - stamps[ks[i]][ns[j]] % 2 == genTable[ns[j]] - 1) {
                                if (!nodeInFrontier(ns[j])) {
                                    bumpAndReadmit(ns[j], 3);
                                    if (storeDead) { goto DeadSched; }
                                    forced = true;
                                }
                            }
                        }
                        j = j + 1;
                    }
                    i = i + 1;
                }
                if (forced) {
                    // Un-drained frontier: resume dispatch; the next
                    // scan begins only after it re-drains.
                    dispatchFree();
                    return;
                }
            } else {
                announce eAnnBudgetExhausted, (syncN = syncN,);
            }
        }
        if (cfg.toggles.sweepOverreach && sizeof(keep) > 1) {
            // INJECT (G5c kill): drop an in-closure key from the keep
            // set (the last discovered, never the root).
            keep -= (sizeof(keep) - 1);
        }
        send store, eGSealReq, (client = this, agen = agen, keep = keep, doSweep = cfg.toggles.sweep, genTable = genTable);
        receive {
            case eStoreAck: {}
            case eStoreDead: { storeDead = true; }
        }
        if (storeDead) { goto DeadSched; }
        send env, eGAttemptEnded, (sealed = true, failed = false);
        goto DoneSched;
    }

    // Pass staleness test. G9 compression admissibility: under
    // stampCompression the pass sees FLOOR-BUCKETED stamps (buckets
    // of 2) — lossy but STALE-erring, never false-live (safety
    // preserved; extra redos are the recorded cost). The forced
    // re-run lands on the bumped (even) generation, so the next scan
    // converges. Stored stamps and the P6-S judge stay exact.
    fun staleStamp(s: int, cur: int): bool {
        if (cfg.stampCompression) {
            return (s - s % 2) < cur;
        }
        return s < cur;
    }

    fun nodeInFrontier(n: int): bool {
        var i: int;
        i = 0;
        while (i < sizeof(frontier)) {
            if (frontier[i].node == n) { return true; }
            i = i + 1;
        }
        return false;
    }

    fun forcedAlready(key: int, owners: map[int, int]): bool {
        var i: int;
        if (!(key in owners)) { return true; }   // ownerless key: nothing to force
        i = 0;
        while (i < sizeof(frontier)) {
            if (frontier[i].node == owners[key]) { return true; }
            i = i + 1;
        }
        return false;
    }

    // Final demand closure over CURRENT store content (the durable
    // truth survives crashes): BFS from the cell's root keys along
    // announced childHash references.
    fun computeClosure(): seq[int] {
        var out: seq[int];
        var queue: seq[int];
        var k: int;
        var i: int;
        var rows: seq[tGRow];
        var got: bool;
        queue += (0, 0);   // every scripted cell roots node 0 / key 0
        while (sizeof(queue) > 0) {
            k = queue[0];
            queue -= 0;
            if (inIntSeq(out, k)) { continue; }
            out += (sizeof(out), k);
            got = false;
            send store, eGReadRowsReq, (client = this, agen = agen, key = k);
            receive {
                case eGReadRowsResp: (r: (rows: seq[tGRow], present: bool)) {
                    rows = r.rows;
                    got = true;
                }
                case eStoreDead: { storeDead = true; }
            }
            if (!got) { return out; }
            i = 0;
            while (i < sizeof(rows)) {
                if (rows[i].childHash > 0 && !inIntSeq(out, rows[i].childHash - 1)) {
                    queue += (sizeof(queue), rows[i].childHash - 1);
                }
                i = i + 1;
            }
        }
        return out;
    }

    fun doCheckpoint(forced: bool) {
        var ck: tGCkpt;
        ck = buildCheckpoint();
        send store, eGCheckpointReq, (client = this, agen = agen, ck = ck, forced = forced);
        receive {
            case eStoreAck: {}
            case eStoreDead: { storeDead = true; }
        }
    }

    // Checkpoint contents pinned by G-RULE-4: pending nodes (frontier
    // + in-flight at their current generations), the admitted set,
    // admitted-by edges, the generation table, the session index.
    fun buildCheckpoint(): tGCkpt {
        var pend: seq[tPendingNode];
        var ids: seq[int];
        var i: int;
        pend = frontier;
        ids = keys(outstanding);
        i = 0;
        while (i < sizeof(ids)) {
            pend += (sizeof(pend), outstanding[ids[i]]);
            i = i + 1;
        }
        return (pending = pend, admitted = admitted, edges = edges, genTable = genTable, readers = readers);
    }

    fun rootFrontier() {
        // Cell scripts: every cell roots the parent P (node 0).
        genTable[0] = 1;
        admitted[1] = true;
        frontier += (0, (node = 0, kind = NK_PARENT, hash = 1, key = 0, gen = 1));
    }
}

fun inIntSeq(s: seq[int], v: int): bool {
    var i: int;
    i = 0;
    while (i < sizeof(s)) {
        if (s[i] == v) { return true; }
        i = i + 1;
    }
    return false;
}
