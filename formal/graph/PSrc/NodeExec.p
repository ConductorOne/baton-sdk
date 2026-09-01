/* MGNodeExec: worker-side execution body (SPEC 3). One execution =
   marker check (SPEC 4a) -> consult -> verdict (ADOPT | REPLAY |
   CHANGED-WITH-DIFF | FETCH-FRESH) -> emissions. Unit-mode
   materialization for replay and diff rounds (settled hand-off);
   record rounds for fetch-fresh (no marker, G8a pin).

   SESSION-PUBLISH BODY-OP PIN (R3-F1): a writer's session publish is
   a body store op executed by every NON-ADOPTED execution regardless
   of verdict class; only adoption skips it.

   Scripted policies (build decisions, CALIBRATION.md): the reader G
   always fetches fresh (walker case-7a precedent), so markers arise
   on consult-kind and writer-kind nodes only; re-derivations after an
   ineligible or refused marker check use fetch-fresh when
   cfg.rederiveFresh (the G1(ii)/G1c scripted shape); a writer's
   published value is a pure function of its consult epoch (so
   same-premise re-derivations re-publish the same value). */

machine MGNodeExec {
    var sched: machine;
    var store: machine;
    var upstream: machine;
    var agen: int;
    var syncN: int;
    var attempt: int;
    var cfg: tGCfg;
    var dead: bool;
    var stopAfterConsult: bool;      // G3 scripted stop (dispatch flag)

    start state InitW {
        entry (p: (sched: machine, store: machine, upstream: machine, agen: int, syncN: int, attempt: int, cfg: tGCfg)) {
            sched = p.sched; store = p.store; upstream = p.upstream;
            agen = p.agen; syncN = p.syncN; attempt = p.attempt; cfg = p.cfg;
            goto IdleW;
        }
    }

    state IdleW {
        on eGDispatch do (p: (pend: tPendingNode, execId: int, attempt: int, stop: bool)) {
            stopAfterConsult = p.stop;
            runNode(p.pend, p.execId);
        }
        on eGAbortWorker do {
            goto DeadW;
        }
    }

    state DeadW {
        ignore eGDispatch, eGAbortWorker;
    }

    fun runNode(pend: tPendingNode, execId: int) {
        // Loud deterministic failure (G7): fails at execution start,
        // every generation, with a GENERATION-BLIND fingerprint.
        if (pend.node == cfg.failNode && syncN == cfg.failSync) {
            send sched, eGNodeFail, (node = pend.node, gen = pend.gen, fingerprint = pend.node * 1000 + 7);
            return;
        }
        if (pend.kind == NK_PARENT) {
            runParent(pend, execId);
        } else if (pend.kind == NK_READER) {
            runReader(pend, execId);
        } else {
            runConsultKind(pend, execId);
        }
    }

    // ---- parent: fetch-fresh record round, rows name children ----
    fun runParent(pend: tPendingNode, execId: int) {
        var rows: seq[tGRow];
        var epoch: int;
        var g: tGGhost;
        var noPubs: seq[tPub];
        var noReads: seq[tReadRec];
        var stamp: map[int, int];
        g = (roundId = execId, verdict = GV_FRESH, consultEpoch = 0, vBase = -1, lastOp = false, attempt = attempt, node = pend.node, gen = pend.gen);
        rows = fetchAll(pend, execId);
        if (dead) { abortReport(pend, execId); return; }
        epoch = rows[0].epoch;
        g.consultEpoch = epoch;
        announce eAnnConsult, (syncN = syncN, key = pend.key, hit = false, v = -1, validated = false, epoch = epoch, freshFetch = true, diffVerdict = false, attempt = attempt, node = pend.node, gen = pend.gen);
        commitRecordRound(pend, execId, rows, epoch, g);
        if (dead) { abortReport(pend, execId); return; }
        sendDone(execId, pend, GV_FRESH, rows, noPubs, noReads);
    }

    // ---- reader G: always-fresh record round embedding the session
    // read; stamp = {G: g} merged with the read value's writer stamp
    // (S read-side merge; stampMergeOff is the kill) ----
    fun runReader(pend: tPendingNode, execId: int) {
        var rows: seq[tGRow];
        var i: int;
        var epoch: int;
        var g: tGGhost;
        var noPubs: seq[tPub];
        var reads: seq[tReadRec];
        var found: bool;
        var sv: int;
        var sw: int;
        var swg: int;
        g = (roundId = execId, verdict = GV_FRESH, consultEpoch = 0, vBase = -1, lastOp = false, attempt = attempt, node = pend.node, gen = pend.gen);
        // Session read (observation point (ii): read-through).
        found = false; sv = -1; sw = -1; swg = -1;
        send store, eGSessionGetReq, (client = this, agen = agen, reader = pend.node, rgen = pend.gen, skey = 0);
        receive {
            case eGSessionGetResp: (r: (found: bool, val: int, writer: int, wgen: int)) {
                found = r.found; sv = r.val; sw = r.writer; swg = r.wgen;
            }
            case eStoreDead: { dead = true; }
        }
        if (dead) { abortReport(pend, execId); return; }
        if (found) {
            reads += (0, (reader = pend.node, rgen = pend.gen, skey = 0, val = sv, writer = sw, wgen = swg));
            // Read-time registration (R2-M1 read-through): the
            // scheduler sees the read while this execution flies.
            send sched, eGReadNote, (reader = pend.node, rgen = pend.gen, skey = 0, val = sv, writer = sw, wgen = swg);
        }
        rows = fetchAll(pend, execId);
        if (dead) { abortReport(pend, execId); return; }
        epoch = rows[0].epoch;
        g.consultEpoch = epoch;
        i = 0;
        while (i < sizeof(rows)) {
            if (found) {
                rows[i] = (id = rows[i].id, epoch = rows[i].epoch, hops = rows[i].hops, childHash = rows[i].childHash, sVal = sv, sWriter = sw, sWGen = swg);
            }
            i = i + 1;
        }
        announce eAnnConsult, (syncN = syncN, key = pend.key, hit = false, v = -1, validated = false, epoch = epoch, freshFetch = true, diffVerdict = false, attempt = attempt, node = pend.node, gen = pend.gen);
        commitRecordRoundStamped(pend, execId, rows, epoch, g, readerStamp(pend, sw, swg, found));
        if (dead) { abortReport(pend, execId); return; }
        sendDone(execId, pend, GV_FRESH, rows, noPubs, reads);
    }

    fun readerStamp(pend: tPendingNode, sw: int, swg: int, found: bool): map[int, int] {
        var st: map[int, int];
        st[pend.node] = pend.gen;
        if (found && cfg.toggles.stampMerge && sw >= 0) {
            st[sw] = swg;
        }
        return st;
    }

    // ---- consult-kind (C) and writer-kind (H) executions ----
    fun runConsultKind(pend: tPendingNode, execId: int) {
        var present: bool;
        var m: tMarker;
        var hit: bool;
        var v: int;
        var outcome: int;
        var epoch: int;
        var rederived: bool;
        var eligible: bool;
        var d2: tDigest;
        var g: tGGhost;
        var rows: seq[tGRow];
        var ups: seq[tGRow];
        var rms: seq[int];
        var pubs: seq[tPub];
        var noReads: seq[tReadRec];
        var stamp: map[int, int];
        var mk: tMarker;
        var isWriter: bool;
        var adoptedRows: seq[tGRow];
        var adoptOk: bool;
        var verdict: tGVerdict;
        isWriter = pend.kind == NK_WRITER;
        // Marker check (SPEC 4a).
        present = false;
        send store, eGMarkerReadReq, (client = this, agen = agen, key = pend.key);
        receive {
            case eGMarkerReadResp: (r: (present: bool, marker: tMarker)) {
                present = r.present;
                m = r.marker;
            }
            case eStoreDead: { dead = true; }
        }
        if (dead) { abortReport(pend, execId); return; }
        // One consult per execution (the marker path's re-consult is
        // THE consult when it falls through to re-derivation).
        hit = false; v = -1;
        send store, eGLookupReq, (client = this, agen = agen, key = pend.key);
        receive {
            case eGLookupResp: (r: (hit: bool, v: int)) { hit = r.hit; v = r.v; }
            case eStoreDead: { dead = true; }
        }
        if (dead) { abortReport(pend, execId); return; }
        send upstream, eValidateReq, (client = this, scope = pend.key, v = v);
        receive {
            case eValidateResp: (r: (ok: bool, epoch: int)) {
                epoch = r.epoch;
                if (!hit) { outcome = 0; }
                else if (r.ok) { outcome = 1; }
                else { outcome = 2; }
            }
        }
        rederived = false;
        if (present && !m.voided) {
            // ADOPTION ELIGIBILITY (round-2 pins): MATCH-only (R2-F3;
            // adoptOnFail waives) and writer-ineligibility (R2-F2;
            // writerAdopt waives). Digest EQUAL and eligible -> ADOPT.
            d2 = (v = v, outcome = outcome, sVal = -1, sWriter = -1, sWGen = -1, hasSess = false);
            eligible = (outcome == 1 || cfg.toggles.adoptOnFail) && d2 == m.digest && (!m.pubBearing || cfg.toggles.writerAdopt);
            if (eligible) {
                // The consult that justifies the adoption is announced
                // BEFORE the adopt commits (P-ADOPT ordering: the
                // justification precedes the act).
                announce eAnnConsult, (syncN = syncN, key = pend.key, hit = hit, v = v, validated = outcome == 1, epoch = epoch, freshFetch = false, diffVerdict = false, attempt = attempt, node = pend.node, gen = pend.gen);
                g = (roundId = execId, verdict = GV_ADOPT, consultEpoch = epoch, vBase = m.contentEpoch, lastOp = true, attempt = attempt, node = pend.node, gen = pend.gen);
                adoptOk = false;
                send store, eGAdoptReq, (client = this, agen = agen, key = pend.key, node = pend.node, fromGen = m.gen, toGen = pend.gen, roundId = m.roundId, allowLiveFrom = !cfg.toggles.suppression, ghost = g);
                receive {
                    case eGAdoptResp: (r: (ok: bool, rows: seq[tGRow])) {
                        adoptOk = r.ok;
                        adoptedRows = r.rows;
                    }
                    case eStoreDead: { dead = true; }
                }
                if (dead) { abortReport(pend, execId); return; }
                if (adoptOk) {
                    sendDone(execId, pend, GV_ADOPT, adoptedRows, pubs, noReads);
                    return;
                }
            }
            rederived = true;
        }
        // Verdict as if unmarked (SPEC 4a re-derive clause).
        if (outcome == 1) {
            verdict = GV_REPLAY;
        } else if (outcome == 2 && cfg.diffPolicy && !(cfg.rederiveFresh && rederived)) {
            verdict = GV_DIFF;
        } else {
            verdict = GV_FRESH;
        }
        announce eAnnConsult, (syncN = syncN, key = pend.key, hit = hit, v = v, validated = outcome == 1, epoch = epoch, freshFetch = verdict == GV_FRESH, diffVerdict = verdict == GV_DIFF, attempt = attempt, node = pend.node, gen = pend.gen);
        // Scripted graceful stop (G3, walker case 3): AFTER the
        // consult, before any round commit — the node stays pending
        // at its consult-granularity cursor.
        if (stopAfterConsult) {
            stopAfterConsult = false;
            send sched, eGStopReq, (node = pend.node, gen = pend.gen);
            return;
        }
        // Writer body op (R3-F1 pin): session publish on every
        // non-adopted execution, before the round's store commit.
        if (isWriter) {
            g = (roundId = execId, verdict = verdict, consultEpoch = epoch, vBase = -1, lastOp = false, attempt = attempt, node = pend.node, gen = pend.gen);
            send store, eGSessionPub, (client = this, agen = agen, skey = 0, val = epoch, writer = pend.node, wgen = pend.gen, ghost = g);
            receive {
                case eStoreAck: {}
                case eStoreDead: { dead = true; }
            }
            if (dead) { abortReport(pend, execId); return; }
            pubs += (0, (skey = 0, val = epoch, wgen = pend.gen));
        }
        stamp = default(map[int, int]);
        stamp[pend.node] = pend.gen;
        if (verdict == GV_REPLAY) {
            mk = (node = pend.node, gen = pend.gen, roundId = execId, digest = (v = v, outcome = 1, sVal = -1, sWriter = -1, sWGen = -1, hasSess = false), pubBearing = isWriter, voided = false, contentEpoch = v);
            g = (roundId = execId, verdict = GV_REPLAY, consultEpoch = epoch, vBase = v, lastOp = true, attempt = attempt, node = pend.node, gen = pend.gen);
            send store, eGReplayUnit, (client = this, agen = agen, key = pend.key, v = epoch, marker = mk, stamp = stamp, hash = pend.hash, ghost = g);
            receive {
                case eGUnitResp: (r: (rows: seq[tGRow])) { rows = r.rows; }
                case eStoreDead: { dead = true; }
            }
            if (dead) { abortReport(pend, execId); return; }
            sendDone(execId, pend, GV_REPLAY, rows, pubs, noReads);
            return;
        }
        if (verdict == GV_DIFF) {
            send upstream, eDiffReq, (client = this, scope = pend.key, fromEpoch = v, page = 0);
            receive {
                case eDiffResp: (r: (upserts: seq[tGRow], removes: seq[int], epoch: int, morePages: bool)) { ups = r.upserts; }
            }
            send upstream, eDiffReq, (client = this, scope = pend.key, fromEpoch = v, page = 1);
            receive {
                case eDiffResp: (r: (upserts: seq[tGRow], removes: seq[int], epoch: int, morePages: bool)) { rms = r.removes; }
            }
            mk = (node = pend.node, gen = pend.gen, roundId = execId, digest = (v = v, outcome = 2, sVal = -1, sWriter = -1, sWGen = -1, hasSess = false), pubBearing = isWriter, voided = false, contentEpoch = epoch);
            g = (roundId = execId, verdict = GV_DIFF, consultEpoch = epoch, vBase = v, lastOp = true, attempt = attempt, node = pend.node, gen = pend.gen);
            send store, eGOverlayUnit, (client = this, agen = agen, key = pend.key, v = epoch, upserts = ups, removes = rms, marker = mk, stamp = stamp, hash = pend.hash, composeDead = cfg.toggles.overlayComposeDead, ghost = g);
            receive {
                case eGUnitResp: (r: (rows: seq[tGRow])) { rows = r.rows; }
                case eStoreDead: { dead = true; }
            }
            if (dead) { abortReport(pend, execId); return; }
            sendDone(execId, pend, GV_DIFF, rows, pubs, noReads);
            return;
        }
        // FETCH-FRESH record round (no marker, G8a pin).
        g = (roundId = execId, verdict = GV_FRESH, consultEpoch = epoch, vBase = -1, lastOp = false, attempt = attempt, node = pend.node, gen = pend.gen);
        rows = fetchAll(pend, execId);
        if (dead) { abortReport(pend, execId); return; }
        commitRecordRound(pend, execId, rows, epoch, g);
        if (dead) { abortReport(pend, execId); return; }
        sendDone(execId, pend, GV_FRESH, rows, pubs, noReads);
    }

    // Fetch every page of the node's key at the current epoch,
    // decorated with the cell's child-naming script.
    fun fetchAll(pend: tPendingNode, execId: int): seq[tGRow] {
        var rows: seq[tGRow];
        var page: int;
        var more: bool;
        var i: int;
        var r: tGRow;
        var ch: int;
        page = 0;
        more = true;
        while (more) {
            send upstream, eFetchReq, (client = this, scope = pend.key, page = page);
            receive {
                case eFetchResp: (rp: (rows: seq[tGRow], epoch: int, morePages: bool)) {
                    i = 0;
                    while (i < sizeof(rp.rows)) {
                        r = rp.rows[i];
                        ch = childHashFor(cfg.cell, pend.node, r.id);
                        if (ch > 0) {
                            r = (id = r.id, epoch = r.epoch, hops = r.hops, childHash = ch, sVal = r.sVal, sWriter = r.sWriter, sWGen = r.sWGen);
                        }
                        rows += (sizeof(rows), r);
                        i = i + 1;
                    }
                    more = rp.morePages;
                }
            }
            page = page + 1;
        }
        return rows;
    }

    fun commitRecordRound(pend: tPendingNode, execId: int, rows: seq[tGRow], epoch: int, g: tGGhost) {
        var stamp: map[int, int];
        stamp[pend.node] = pend.gen;
        commitRecordRoundStamped(pend, execId, rows, epoch, g, stamp);
    }

    // Record round (REPLACES): clear (deleting the marker — the
    // markerCleanup toggle removes exactly that, R2-F4), per-page
    // upserts, publish. Per-op commits give the crash protocol its
    // windows.
    fun commitRecordRoundStamped(pend: tPendingNode, execId: int, rows: seq[tGRow], epoch: int, g: tGGhost, stamp: map[int, int]) {
        var i: int;
        var pageRows: seq[tGRow];
        send store, eGClearScope, (client = this, agen = agen, key = pend.key, delMarker = cfg.toggles.markerCleanup, ghost = g);
        receive {
            case eStoreAck: {}
            case eStoreDead: { dead = true; }
        }
        if (dead) { return; }
        i = 0;
        while (i < sizeof(rows)) {
            pageRows = default(seq[tGRow]);
            pageRows += (0, rows[i]);
            send store, eGUpsertPage, (client = this, agen = agen, key = pend.key, rows = pageRows, ghost = g);
            receive {
                case eStoreAck: {}
                case eStoreDead: { dead = true; }
            }
            if (dead) { return; }
            // Per-announce demand derivation (G-RULE-1 TIMING PIN):
            // a committed child-naming page reaches the scheduler AT
            // the page announce, not at completion — the mid-round
            // checkpoint window (G5e) exists only under this timing.
            if (pageRows[0].childHash > 0) {
                send sched, eGDemandNote, (node = pend.node, gen = pend.gen, key = pend.key, rows = pageRows);
            }
            i = i + 1;
        }
        g.lastOp = true;
        send store, eGPublishEntry, (client = this, agen = agen, key = pend.key, v = epoch, stamp = stamp, hash = pend.hash, ghost = g);
        receive {
            case eStoreAck: {}
            case eStoreDead: { dead = true; }
        }
    }

    fun sendDone(execId: int, pend: tPendingNode, verdict: tGVerdict, rows: seq[tGRow], pubs: seq[tPub], reads: seq[tReadRec]) {
        send sched, eGNodeDone, (report = (execId = execId, node = pend.node, gen = pend.gen, hash = pend.hash, key = pend.key, kind = pend.kind, verdict = verdict, rows = rows, pubs = pubs, reads = reads, aborted = false),);
    }

    fun abortReport(pend: tPendingNode, execId: int) {
        var noRows: seq[tGRow];
        var noPubs: seq[tPub];
        var noReads: seq[tReadRec];
        send sched, eGNodeDone, (report = (execId = execId, node = pend.node, gen = pend.gen, hash = pend.hash, key = pend.key, kind = pend.kind, verdict = GV_FRESH, rows = noRows, pubs = noPubs, reads = noReads, aborted = true),);
        goto DeadW;
    }
}

// Child-naming script (G-RULE-1: demand rides row content).
fun childHashFor(cell: int, node: int, rowId: int): int {
    if (cell == 11 && node == 0) { return 2; }            // P names C on every row
    if (cell == 21 && node == 0 && rowId == 0) { return 3; }  // P names H
    if (cell == 21 && node == 0 && rowId == 1) { return 4; }  // P names G
    // Cell 24 (G5 family): the PAGINATED parent names C on page 1
    // ONLY — the row the e1->e2 mutation deletes, so the re-execution
    // at e2 emits no child naming (upstream demand shrink).
    if (cell == 24 && node == 0 && rowId == 1) { return 2; }
    if (cell == 25 && node == 0) { return 2; }            // G3: cell-11 topology
    // Cell 26 (G6a/b chain): P -> S1 -> C -> GC; S1 names C on its
    // page-1 row (the row the e1->e2 mutation deletes: G6b's shrink).
    if (cell == 26 && node == 0) { return 2; }
    if (cell == 26 && node == 1 && rowId == 1) { return 5; }
    if (cell == 26 && node == 4 && rowId == 0) { return 6; }
    // Cell 27 (G7): cell-11 topology.
    if (cell == 27 && node == 0) { return 2; }
    // Cell 28 (G6c fan-in): P names S1 + S2; both name C.
    if (cell == 28 && node == 0 && rowId == 0) { return 2; }
    if (cell == 28 && node == 0 && rowId == 1) { return 5; }
    if (cell == 28 && node == 1 && rowId == 0) { return 6; }
    if (cell == 28 && node == 4 && rowId == 0) { return 6; }
    // Cell 29 (G8c same-key): P names two DISTINCT derivations that
    // share output key 1 (keyOf).
    if (cell == 29 && node == 0 && rowId == 0) { return 2; }
    if (cell == 29 && node == 0 && rowId == 1) { return 5; }
    return 0;
}
