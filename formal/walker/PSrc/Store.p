/* MStore: the durable store, one per scenario, holding the artifact
   chain (MODEL_SPEC 3). Abstraction per MODEL_SPEC 1: partitions with
   atomic page commits and a manifest of scope -> validator entries;
   batched clear/copy abstracted to TWO atomic steps (separate ops).

   Crash protocol (MODEL_SPEC 5, pinned): eCrash's position in this
   machine's queue partitions the dead attempt's outstanding ops — ops
   processed before it committed; ops behind it are dropped when they
   arrive (gen check), never processed, never acked. Per-sender FIFO is
   P's own delivery guarantee (the WAL property). */

machine MStore {
    var prevPart: tPartition;          // previous artifact (replay source)
    var prevMan: map[int, int];        // previous manifest: scope -> validator
    var curPart: tPartition;           // this sync's artifact
    var curMan: map[int, int];
    var ckpt: tCheckpoint;
    var hasCkpt: bool;
    var deadGens: map[int, bool];
    var syncN: int;
    var sealed: bool;
    var armed: bool;
    var armedGen: int;
    var armedClient: machine;
    var curMarkers: map[int, bool];   // scenario-6 markers, per current sync
    // Session store (cases 2, 7): sync-scoped KV, durable at op commit.
    // Survives attempts and crashes within the sync (committed writes
    // are durable; the crash protocol only drops UNPROCESSED ops);
    // reset at sync rotation like the checkpoint token.
    var sessionKV: map[int, int];
    // Session durability variant (P6-C, cfg.sessVariant): sessCkpt is
    // the session state latched with the last committed checkpoint —
    // consumed only by variant 2 (checkpoint-consistent rollback).
    var sessVariant: int;
    var sessCkpt: map[int, int];
    // Produce-side session taint (case-7 fix runs): kinds (scopes here)
    // marked non-replayable in the artifact being produced. Rotates
    // with the artifact; the NEXT sync's consult on a prev-tainted
    // kind returns MISS (degradation, not a loud verdict).
    var curTaint: map[int, bool];
    var prevTaint: map[int, bool];
    // Case-5 produce state: compat records (per artifact; -1 = none)
    // and the sealed artifact's produce-blocked flag.
    var curCompat: int;
    var prevCompat: int;
    var sealedBlocked: bool;
    var prevBlocked: bool;
    // Scenario-8 external-principal keyspace (BatonID-annotated rows,
    // separate from scope partitions). Durable at op commit like every
    // row write: a dead attempt's committed copies SURVIVE the crash —
    // which is exactly why eExtReconReq exists.
    var extRows: map[int, bool];

    start state Serving {
        on eStoreReset do (p: (client: machine, syncN: int, sessVariant: int)) {
            // Begin-of-sync rotation: the sealed artifact becomes the
            // replay source; the new artifact starts empty; the
            // checkpoint token belongs to a sync and does not survive it.
            if (sealed) {
                prevPart = curPart;
                prevMan = curMan;
                prevTaint = curTaint;
                prevCompat = curCompat;
                prevBlocked = sealedBlocked;
            }
            curPart = default(tPartition);
            curMan = default(map[int, int]);
            curMarkers = default(map[int, bool]);
            curTaint = default(map[int, bool]);
            curCompat = -1;
            sealedBlocked = false;
            sessionKV = default(map[int, int]);
            sessVariant = p.sessVariant;
            sessCkpt = default(map[int, int]);
            extRows = default(map[int, bool]);
            hasCkpt = false;
            ckpt = default(tCheckpoint);
            sealed = false;
            syncN = p.syncN;
            send p.client, eStoreAck;
        }

        on eSessionSet do (p: (client: machine, gen: int, scope: int, key: int, val: int, taint: bool)) {
            maybeCrash();
            if (p.gen in deadGens) { send p.client, eStoreDead; return; }
            sessionKV[p.key] = p.val;
            if (p.taint) { curTaint[p.scope] = true; }
            announce eAnnSessionSet, (syncN = syncN, key = p.key, val = p.val);
            send p.client, eStoreAck;
        }

        on eSessionGetReq do (p: (client: machine, gen: int, scope: int, key: int, taint: bool)) {
            maybeCrash();
            if (p.gen in deadGens) { send p.client, eStoreDead; return; }
            if (p.taint) { curTaint[p.scope] = true; }
            if (p.key in sessionKV) {
                announce eAnnSessionGet, (syncN = syncN, key = p.key, found = true, val = sessionKV[p.key]);
                send p.client, eSessionGetResp, (found = true, val = sessionKV[p.key]);
            } else {
                announce eAnnSessionGet, (syncN = syncN, key = p.key, found = false, val = 0);
                send p.client, eSessionGetResp, (found = false, val = 0);
            }
        }

        on eExtReconReq do (p: (client: machine, gen: int, live: seq[int], supported: bool)) {
            var ids: seq[int];
            var liveSet: map[int, bool];
            var deleted: seq[int];
            var i: int;
            maybeCrash();
            if (p.gen in deadGens) { send p.client, eStoreDead; return; }
            i = 0;
            while (i < sizeof(p.live)) {
                liveSet[p.live[i]] = true;
                i = i + 1;
            }
            if (p.supported) {
                // The capable path: delete every ext row the current
                // answer no longer contains (one atomic pass —
                // deleteStaleExternalPrincipals runs before the
                // current answer's writes).
                ids = keys(extRows);
                i = 0;
                while (i < sizeof(ids)) {
                    if (!(ids[i] in liveSet)) {
                        deleted += (sizeof(deleted), ids[i]);
                        extRows -= ids[i];
                    }
                    i = i + 1;
                }
            }
            // supported FALSE: warn-and-continue — the round still
            // announces (the list happened), nothing is deleted.
            announce eAnnExtRound, (syncN = syncN, live = p.live, supported = p.supported, deleted = deleted);
            send p.client, eStoreAck;
        }

        on eExtCopy do (p: (client: machine, gen: int, ids: seq[int])) {
            var i: int;
            maybeCrash();
            if (p.gen in deadGens) { send p.client, eStoreDead; return; }
            i = 0;
            while (i < sizeof(p.ids)) {
                extRows[p.ids[i]] = true;
                i = i + 1;
            }
            send p.client, eStoreAck;
        }

        on eCompatPut do (p: (client: machine, gen: int, k: int)) {
            maybeCrash();
            if (p.gen in deadGens) { send p.client, eStoreDead; return; }
            curCompat = p.k;
            send p.client, eStoreAck;
        }

        on eProduceReadReq do (p: (client: machine, gen: int)) {
            maybeCrash();
            if (p.gen in deadGens) { send p.client, eStoreDead; return; }
            send p.client, eProduceReadResp, (prevCompat = prevCompat, prevBlocked = prevBlocked, curCompat = curCompat, hasCur = curCompat > 0, hasPrev = sizeof(prevMan) > 0);
        }

        on eSwapBase do (p: (client: machine, scope: int, vB: int, rowsB: seq[tRow])) {
            // Case-3 premise: replace the previous sealed artifact for
            // this scope with sibling B. Env-level initial-condition
            // surgery (B is a legal artifact from another history), not
            // an attempt op — no gen gate, no crash point.
            var i: int;
            var m: map[int, tRow];
            i = 0;
            while (i < sizeof(p.rowsB)) {
                m[p.rowsB[i].id] = p.rowsB[i];
                i = i + 1;
            }
            prevPart[p.scope] = m;
            prevMan[p.scope] = p.vB;
            send p.client, eStoreAck;
        }

        on eLookupReq do (p: (client: machine, gen: int, scope: int)) {
            maybeCrash();
            if (p.gen in deadGens) { send p.client, eStoreDead; return; }
            // A kind tainted in the previous artifact's produce state
            // consults MISS (case-7 fix runs): replay is forfeited.
            if (p.scope in prevMan && !(p.scope in prevTaint)) {
                send p.client, eLookupResp, (hit = true, v = prevMan[p.scope]);
            } else {
                send p.client, eLookupResp, (hit = false, v = -1);
            }
        }

        on eBaseReadReq do (p: (client: machine, gen: int, scope: int)) {
            maybeCrash();
            if (p.gen in deadGens) { send p.client, eStoreDead; return; }
            if (p.scope in prevMan) {
                send p.client, eBaseReadResp, (v = prevMan[p.scope], present = true);
            } else {
                send p.client, eBaseReadResp, (v = -1, present = false);
            }
        }

        on eClearScope do (p: (client: machine, gen: int, scope: int, ghost: tRoundGhost)) {
            maybeCrash();
            if (p.gen in deadGens) { send p.client, eStoreDead; return; }
            curPart[p.scope] = default(map[int, tRow]);
            announce eAnnClear, (syncN = syncN, scope = p.scope, ghost = p.ghost);
            send p.client, eStoreAck;
        }

        on eCopyScope do (p: (client: machine, gen: int, scope: int, ghost: tRoundGhost)) {
            var ids: seq[int];
            var i: int;
            var r: tRow;
            var dst: map[int, tRow];
            maybeCrash();
            if (p.gen in deadGens) { send p.client, eStoreDead; return; }
            if (p.scope in curPart) { dst = curPart[p.scope]; }
            if (p.scope in prevPart) {
                ids = keys(prevPart[p.scope]);
                i = 0;
                while (i < sizeof(ids)) {
                    r = prevPart[p.scope][ids[i]];
                    // P2 ghost: replay travel increments the hop counter.
                    r.hops = r.hops + 1;
                    dst[r.id] = r;
                    i = i + 1;
                }
            }
            curPart[p.scope] = dst;
            announce eAnnReplay, (syncN = syncN, scope = p.scope, vBase = prevManValue(prevMan, p.scope), cBase = baseConfigOf(prevPart, p.scope), ghost = p.ghost);
            send p.client, eStoreAck;
        }

        on eUpsertPage do (p: (client: machine, gen: int, scope: int, rows: seq[tRow], ghost: tRoundGhost)) {
            var i: int;
            var dst: map[int, tRow];
            maybeCrash();
            if (p.gen in deadGens) { send p.client, eStoreDead; return; }
            if (p.scope in curPart) { dst = curPart[p.scope]; }
            i = 0;
            while (i < sizeof(p.rows)) {
                dst[p.rows[i].id] = p.rows[i];
                i = i + 1;
            }
            curPart[p.scope] = dst;
            announce eAnnUpsert, (syncN = syncN, scope = p.scope, rows = p.rows, ghost = p.ghost);
            send p.client, eStoreAck;
        }

        on ePublishEntry do (p: (client: machine, gen: int, scope: int, v: int, ghost: tRoundGhost)) {
            maybeCrash();
            if (p.gen in deadGens) { send p.client, eStoreDead; return; }
            curMan[p.scope] = p.v;
            announce eAnnPublish, (syncN = syncN, scope = p.scope, v = p.v, ghost = p.ghost);
            send p.client, eStoreAck;
        }

        on eMarkerPut do (p: (client: machine, gen: int, scope: int)) {
            maybeCrash();
            if (p.gen in deadGens) { send p.client, eStoreDead; return; }
            curMarkers[p.scope] = true;
            send p.client, eStoreAck;
        }

        on eMarkerReadReq do (p: (client: machine, gen: int, scope: int)) {
            maybeCrash();
            if (p.gen in deadGens) { send p.client, eStoreDead; return; }
            send p.client, eMarkerReadResp, (marked = p.scope in curMarkers,);
        }

        on eReplayUnit do (p: (client: machine, gen: int, scope: int, v: int, ghost: tRoundGhost)) {
            var ids: seq[int];
            var i: int;
            var r: tRow;
            var dst: map[int, tRow];
            var g: tRoundGhost;
            maybeCrash();
            if (p.gen in deadGens) { send p.client, eStoreDead; return; }
            // One atomic commit: clear, copy, marker, publish. The
            // constituent announces fire together at commit; the last
            // (publish) carries lastOp — round completion IS unit commit.
            curPart[p.scope] = default(map[int, tRow]);
            g = p.ghost;
            g.lastOp = false;
            announce eAnnClear, (syncN = syncN, scope = p.scope, ghost = g);
            dst = curPart[p.scope];
            if (p.scope in prevPart) {
                ids = keys(prevPart[p.scope]);
                i = 0;
                while (i < sizeof(ids)) {
                    r = prevPart[p.scope][ids[i]];
                    r.hops = r.hops + 1;
                    dst[r.id] = r;
                    i = i + 1;
                }
            }
            curPart[p.scope] = dst;
            announce eAnnReplay, (syncN = syncN, scope = p.scope, vBase = prevManValue(prevMan, p.scope), cBase = baseConfigOf(prevPart, p.scope), ghost = g);
            curMarkers[p.scope] = true;
            curMan[p.scope] = p.v;
            announce eAnnPublish, (syncN = syncN, scope = p.scope, v = p.v, ghost = p.ghost);
            send p.client, eStoreAck;
        }

        on eTombstonePage do (p: (client: machine, gen: int, scope: int, removes: seq[int], ghost: tRoundGhost)) {
            var i: int;
            var dst: map[int, tRow];
            maybeCrash();
            if (p.gen in deadGens) { send p.client, eStoreDead; return; }
            if (p.scope in curPart) { dst = curPart[p.scope]; }
            i = 0;
            while (i < sizeof(p.removes)) {
                if (p.removes[i] in dst) { dst -= p.removes[i]; }
                i = i + 1;
            }
            curPart[p.scope] = dst;
            announce eAnnTombstones, (syncN = syncN, scope = p.scope, removes = p.removes, ghost = p.ghost);
            send p.client, eStoreAck;
        }

        on eOverlayUnit do (p: (client: machine, gen: int, scope: int, v: int, upserts: seq[tRow], removes: seq[int], ghost: tRoundGhost)) {
            var ids: seq[int];
            var i: int;
            var r: tRow;
            var dst: map[int, tRow];
            var g: tRoundGhost;
            maybeCrash();
            if (p.gen in deadGens) { send p.client, eStoreDead; return; }
            // ONE atomic commit: clear, copy(base), overlay upserts and
            // tombstones in prescribed page order, marker, publish(V_to).
            // Constituent announces fire together; publish carries
            // lastOp — round completion IS unit commit, and the round
            // folds as the self-grounding overlay (own copy committed).
            g = p.ghost;
            g.lastOp = false;
            curPart[p.scope] = default(map[int, tRow]);
            announce eAnnClear, (syncN = syncN, scope = p.scope, ghost = g);
            dst = curPart[p.scope];
            if (p.scope in prevPart) {
                ids = keys(prevPart[p.scope]);
                i = 0;
                while (i < sizeof(ids)) {
                    r = prevPart[p.scope][ids[i]];
                    r.hops = r.hops + 1;
                    dst[r.id] = r;
                    i = i + 1;
                }
            }
            i = 0;
            while (i < sizeof(p.upserts)) {
                dst[p.upserts[i].id] = p.upserts[i];
                i = i + 1;
            }
            announce eAnnReplay, (syncN = syncN, scope = p.scope, vBase = prevManValue(prevMan, p.scope), cBase = baseConfigOf(prevPart, p.scope), ghost = g);
            announce eAnnUpsert, (syncN = syncN, scope = p.scope, rows = p.upserts, ghost = g);
            i = 0;
            while (i < sizeof(p.removes)) {
                if (p.removes[i] in dst) { dst -= p.removes[i]; }
                i = i + 1;
            }
            announce eAnnTombstones, (syncN = syncN, scope = p.scope, removes = p.removes, ghost = g);
            curPart[p.scope] = dst;
            curMarkers[p.scope] = true;
            curMan[p.scope] = p.v;
            announce eAnnPublish, (syncN = syncN, scope = p.scope, v = p.v, ghost = p.ghost);
            send p.client, eStoreAck;
        }

        on eCheckpointReq do (p: (client: machine, gen: int, ckpt: tCheckpoint)) {
            maybeCrash();
            if (p.gen in deadGens) { send p.client, eStoreDead; return; }
            ckpt = p.ckpt;
            hasCkpt = true;
            // Checkpoint-consistent sessions (variant 2): the session
            // overlay flushes atomically with the checkpoint token.
            if (sessVariant == 2) { sessCkpt = sessionKV; }
            announce eAnnCheckpoint, (syncN = syncN,);
            send p.client, eStoreAck;
        }

        on eSealReq do (p: (client: machine, gen: int, blocked: bool, config: int)) {
            // Guaranteed resolution point for an armed crash: it fires
            // either just before the seal (dropping it) or right after.
            if (armed && armedGen == p.gen) {
                if (choose(2) == 0) {
                    fireCrash();
                } else {
                    sealed = true;
                    sealedBlocked = p.blocked;
                    announce eAnnExtSeal, (syncN = syncN, ids = keys(extRows));
                    announce eAnnSeal, (syncN = syncN, partition = curPart, manifest = curMan, blocked = p.blocked, config = p.config);
                    send p.client, eStoreAck;
                    fireCrash();
                    return;
                }
            }
            if (p.gen in deadGens) { send p.client, eStoreDead; return; }
            sealed = true;
            sealedBlocked = p.blocked;
            announce eAnnExtSeal, (syncN = syncN, ids = keys(extRows));
            announce eAnnSeal, (syncN = syncN, partition = curPart, manifest = curMan, blocked = p.blocked, config = p.config);
            send p.client, eStoreAck;
        }

        on eReadCheckpointReq do (p: (client: machine)) {
            send p.client, eReadCheckpointResp, (ckpt = ckpt, hasCkpt = hasCkpt);
        }

        on eReadSealedReq do (p: (client: machine)) {
            send p.client, eReadSealedResp, (sealed = sealed,);
        }

        on eCrashArm do (p: (client: machine, gen: int)) {
            armed = true;
            armedGen = p.gen;
            armedClient = p.client;
        }
    }

    fun maybeCrash() {
        if (armed && choose(2) == 0) {
            fireCrash();
        }
    }

    fun fireCrash() {
        deadGens[armedGen] = true;
        armed = false;
        // Session state at the crash boundary (P6-C). Variant 0
        // (shipped) keeps sessionKV untouched: writes beyond the last
        // checkpoint survive the cursor rollback (the zombie
        // direction). Variant 1 models the rejected wholesale
        // resume-clear: checkpoint-committed data is destroyed too
        // (the amnesia direction). Variant 2 rolls sessions back to
        // the state latched with the last checkpoint — both
        // directions closed. Equivalent to acting at resume start:
        // dead-gen ops arriving after the crash are dropped by the
        // gen gate and can never observe the adjusted map.
        if (sessVariant == 1) {
            sessionKV = default(map[int, int]);
        }
        if (sessVariant == 2) {
            sessionKV = sessCkpt;
        }
        announce eAnnCrash, (syncN = syncN,);
        send armedClient, eCrashAck;
    }
}

fun prevManValue(m: map[int, int], scope: int): int {
    if (scope in m) { return m[scope]; }
    return -1;
}

// Config ghost of the copied base content: rows in a scope share one
// attempt config, so the first row's tag stands for the base. -1 when
// the base is empty or configs are unmodeled (all rows tag 0 -> the
// caller treats 0 as unmodeled too).
fun baseConfigOf(part: tPartition, scope: int): int {
    var ids: seq[int];
    if (scope in part) {
        ids = keys(part[scope]);
        if (sizeof(ids) > 0) { return part[scope][ids[0]].config; }
    }
    return -1;
}
