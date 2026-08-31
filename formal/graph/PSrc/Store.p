/* MGStore: the durable store, one per scenario (SPEC 3/5). Holds the
   artifact chain (partitions + manifest), unit markers (per-sync),
   causal stamps (ride outputs), the session KV, poison flags, and the
   frontier checkpoint. Crash protocol is walker parity: eCrash's
   queue position partitions the dead attempt's outstanding ops; ops
   behind it are dropped on arrival (agen check), never acked;
   receivers get eStoreDead and park.

   Poison (SPEC 4b): a second DISTINCT derivation hash committing rows
   for one key this sync poisons the key on its first hash-carrying
   commit, voids the key's marker (R2-M8), and the eGAdopt refusal is
   store-side (R3-M2). Post-poison rounds commit legally; the scope is
   seal-excluded by the SealExpect exclusion and P1's exemption. */

type tSessCell = (val: int, writer: int, wgen: int);

machine MGStore {
    var prevPart: tGPart;
    var prevMan: map[int, int];
    var curPart: tGPart;
    var curMan: map[int, int];
    var curMarkers: map[int, tMarker];
    var curStamps: map[int, map[int, int]];
    var curOwners: map[int, int];        // key -> producing node (pass target)
    var sessKV: map[int, tSessCell];
    var poisoned: map[int, bool];
    var firstHash: map[int, int];        // key -> first committed derivation hash this sync
    var ckpt: tGCkpt;
    var hasCkpt: bool;
    var deadGens: map[int, bool];
    var syncN: int;
    var sealed: bool;
    var armed: bool;
    var armedGen: int;
    var armedClient: machine;

    start state Serving {
        on eGReset do (p: (client: machine, syncN: int)) {
            if (sealed) {
                prevPart = curPart;
                prevMan = curMan;
            }
            curPart = default(tGPart);
            curMan = default(map[int, int]);
            curMarkers = default(map[int, tMarker]);   // markers are per-sync (SPEC 3)
            curStamps = default(map[int, map[int, int]]);
            curOwners = default(map[int, int]);
            sessKV = default(map[int, tSessCell]);
            poisoned = default(map[int, bool]);
            firstHash = default(map[int, int]);
            ckpt = default(tGCkpt);
            hasCkpt = false;
            sealed = false;
            syncN = p.syncN;
            send p.client, eStoreAck;
        }

        on eGSwapPrev do (p: (client: machine, key: int, epoch: int)) {
            // G3 between-attempt rebind (env scripting, no agen — no
            // attempt is live): the PREV artifact's manifest and
            // partition for the key become sibling content, so the
            // resumed consult validates against the actually-current
            // base and truthfully FAILs.
            var rows: map[int, tGRow];
            rows[0] = (id = 0, epoch = p.epoch, hops = 0, childHash = 0, sVal = -1, sWriter = -1, sWGen = -1);
            prevMan[p.key] = p.epoch;
            prevPart[p.key] = rows;
            send p.client, eStoreAck;
        }

        on eGLookupReq do (p: (client: machine, agen: int, key: int)) {
            maybeCrash();
            if (p.agen in deadGens) { send p.client, eStoreDead; return; }
            if (p.key in prevMan) {
                send p.client, eGLookupResp, (hit = true, v = prevMan[p.key]);
            } else {
                send p.client, eGLookupResp, (hit = false, v = -1);
            }
        }

        on eGMarkerReadReq do (p: (client: machine, agen: int, key: int)) {
            maybeCrash();
            if (p.agen in deadGens) { send p.client, eStoreDead; return; }
            if (p.key in curMarkers) {
                send p.client, eGMarkerReadResp, (present = true, marker = curMarkers[p.key]);
            } else {
                send p.client, eGMarkerReadResp, (present = false, marker = default(tMarker));
            }
        }

        on eGClearScope do (p: (client: machine, agen: int, key: int, delMarker: bool, ghost: tGGhost)) {
            maybeCrash();
            if (p.agen in deadGens) { send p.client, eStoreDead; return; }
            // Marker lifecycle rides the clear (R2-F4): the delete is
            // announced BEFORE the clear so P-MARK sees the unbind
            // first (one atomic op; markerCleanupOff removes this).
            if (p.delMarker && p.key in curMarkers) {
                curMarkers -= p.key;
                announce eAnnMarkerDel, (syncN = syncN, key = p.key);
            }
            curPart[p.key] = default(map[int, tGRow]);
            announce eAnnClear, (syncN = syncN, key = p.key, ghost = p.ghost);
            send p.client, eStoreAck;
        }

        on eGUpsertPage do (p: (client: machine, agen: int, key: int, rows: seq[tGRow], ghost: tGGhost)) {
            var i: int;
            var dst: map[int, tGRow];
            maybeCrash();
            if (p.agen in deadGens) { send p.client, eStoreDead; return; }
            if (p.key in curPart) { dst = curPart[p.key]; }
            i = 0;
            while (i < sizeof(p.rows)) {
                dst[p.rows[i].id] = p.rows[i];
                i = i + 1;
            }
            curPart[p.key] = dst;
            announce eAnnUpsert, (syncN = syncN, key = p.key, rows = p.rows, ghost = p.ghost);
            send p.client, eStoreAck;
        }

        on eGPublishEntry do (p: (client: machine, agen: int, key: int, v: int, stamp: map[int, int], hash: int, ghost: tGGhost)) {
            maybeCrash();
            if (p.agen in deadGens) { send p.client, eStoreDead; return; }
            poisonCheck(p.key, p.hash);
            curMan[p.key] = p.v;
            curStamps[p.key] = p.stamp;
            curOwners[p.key] = p.ghost.node;
            announce eAnnPublish, (syncN = syncN, key = p.key, v = p.v, ghost = p.ghost);
            send p.client, eStoreAck;
        }

        on eGReplayUnit do (p: (client: machine, agen: int, key: int, v: int, marker: tMarker, stamp: map[int, int], hash: int, ghost: tGGhost)) {
            var ids: seq[int];
            var i: int;
            var r: tGRow;
            var dst: map[int, tGRow];
            var g: tGGhost;
            var copied: seq[tGRow];
            maybeCrash();
            if (p.agen in deadGens) { send p.client, eStoreDead; return; }
            // ONE atomic commit: marker (announced first, P-MARK
            // convention), clear, copy, publish. Round completion IS
            // unit commit; the publish constituent carries lastOp.
            poisonCheck(p.key, p.hash);
            g = p.ghost;
            g.lastOp = false;
            curMarkers[p.key] = p.marker;
            announce eAnnMarkerPut, (syncN = syncN, key = p.key, node = p.marker.node, gen = p.marker.gen, roundId = p.marker.roundId, pubBearing = p.marker.pubBearing, contentEpoch = p.marker.contentEpoch, ghost = g);
            curPart[p.key] = default(map[int, tGRow]);
            announce eAnnClear, (syncN = syncN, key = p.key, ghost = g);
            dst = curPart[p.key];
            if (p.key in prevPart) {
                ids = keys(prevPart[p.key]);
                i = 0;
                while (i < sizeof(ids)) {
                    r = prevPart[p.key][ids[i]];
                    r.hops = r.hops + 1;
                    dst[r.id] = r;
                    copied += (sizeof(copied), r);
                    i = i + 1;
                }
            }
            curPart[p.key] = dst;
            announce eAnnReplayCopy, (syncN = syncN, key = p.key, vBase = p.ghost.vBase, rows = copied, ghost = g);
            curStamps[p.key] = p.stamp;
            curOwners[p.key] = p.ghost.node;
            curMan[p.key] = p.v;
            announce eAnnPublish, (syncN = syncN, key = p.key, v = p.v, ghost = p.ghost);
            send p.client, eGUnitResp, (rows = rowsOf(curPart, p.key),);
        }

        on eGOverlayUnit do (p: (client: machine, agen: int, key: int, v: int, upserts: seq[tGRow], removes: seq[int], marker: tMarker, stamp: map[int, int], hash: int, composeDead: bool, ghost: tGGhost)) {
            var ids: seq[int];
            var i: int;
            var r: tGRow;
            var dst: map[int, tGRow];
            var g: tGGhost;
            var copied: seq[tGRow];
            maybeCrash();
            if (p.agen in deadGens) { send p.client, eStoreDead; return; }
            poisonCheck(p.key, p.hash);
            g = p.ghost;
            g.lastOp = false;
            curMarkers[p.key] = p.marker;
            announce eAnnMarkerPut, (syncN = syncN, key = p.key, node = p.marker.node, gen = p.marker.gen, roundId = p.marker.roundId, pubBearing = p.marker.pubBearing, contentEpoch = p.marker.contentEpoch, ghost = g);
            if (p.composeDead && p.key in curPart && sizeof(curPart[p.key]) > 0) {
                // INJECT (G8b kill): compose the diff onto whatever the
                // partition already holds — possibly a dead
                // generation's partial debris — instead of the unit's
                // clear + prev-copy base (the §4b precondition waived).
                dst = curPart[p.key];
                announce eAnnReplayCopy, (syncN = syncN, key = p.key, vBase = p.ghost.vBase, rows = copied, ghost = g);
            } else {
                curPart[p.key] = default(map[int, tGRow]);
                announce eAnnClear, (syncN = syncN, key = p.key, ghost = g);
                dst = curPart[p.key];
                if (p.key in prevPart) {
                    ids = keys(prevPart[p.key]);
                    i = 0;
                    while (i < sizeof(ids)) {
                        r = prevPart[p.key][ids[i]];
                        r.hops = r.hops + 1;
                        dst[r.id] = r;
                        copied += (sizeof(copied), r);
                        i = i + 1;
                    }
                }
                announce eAnnReplayCopy, (syncN = syncN, key = p.key, vBase = p.ghost.vBase, rows = copied, ghost = g);
            }
            i = 0;
            while (i < sizeof(p.upserts)) {
                dst[p.upserts[i].id] = p.upserts[i];
                i = i + 1;
            }
            announce eAnnUpsert, (syncN = syncN, key = p.key, rows = p.upserts, ghost = g);
            i = 0;
            while (i < sizeof(p.removes)) {
                if (p.removes[i] in dst) { dst -= p.removes[i]; }
                i = i + 1;
            }
            announce eAnnTombstones, (syncN = syncN, key = p.key, removes = p.removes, ghost = g);
            curPart[p.key] = dst;
            curStamps[p.key] = p.stamp;
            curOwners[p.key] = p.ghost.node;
            curMan[p.key] = p.v;
            announce eAnnPublish, (syncN = syncN, key = p.key, v = p.v, ghost = p.ghost);
            send p.client, eGUnitResp, (rows = rowsOf(curPart, p.key),);
        }

        on eGAdoptReq do (p: (client: machine, agen: int, key: int, node: int, fromGen: int, toGen: int, roundId: int, allowLiveFrom: bool, ghost: tGGhost)) {
            var m: tMarker;
            var st: map[int, int];
            var outRows: seq[tGRow];
            var ids: seq[int];
            var i: int;
            var deadFrom: bool;
            maybeCrash();
            if (p.agen in deadGens) { send p.client, eStoreDead; return; }
            // Store-side preconditions: marker present and not voided;
            // key not poisoned (R3-M2); fromGen dead per the last
            // durable generation table (R2-N1; allowLiveFrom is the
            // suppressionOff declared deviation).
            deadFrom = hasCkpt && p.node in ckpt.genTable && p.fromGen < ckpt.genTable[p.node];
            if (!(p.key in curMarkers) || curMarkers[p.key].voided || (p.key in poisoned) || (!deadFrom && !p.allowLiveFrom)) {
                send p.client, eGAdoptResp, (ok = false, rows = outRows);
                return;
            }
            m = curMarkers[p.key];
            m.gen = p.toGen;
            m.roundId = p.ghost.roundId;
            curMarkers[p.key] = m;
            if (p.key in curStamps) { st = curStamps[p.key]; }
            if (p.node in st) { st -= p.node; }
            st[p.node] = p.toGen;
            curStamps[p.key] = st;
            if (p.key in curPart) {
                ids = keys(curPart[p.key]);
                i = 0;
                while (i < sizeof(ids)) {
                    outRows += (sizeof(outRows), curPart[p.key][ids[i]]);
                    i = i + 1;
                }
            }
            announce eAnnAdopt, (syncN = syncN, key = p.key, node = p.node, fromGen = p.fromGen, toGen = p.toGen, adoptedRoundId = p.roundId, rows = outRows, ghost = p.ghost);
            send p.client, eGAdoptResp, (ok = true, rows = outRows);
        }

        on eGSessionPub do (p: (client: machine, agen: int, skey: int, val: int, writer: int, wgen: int, ghost: tGGhost)) {
            maybeCrash();
            if (p.agen in deadGens) { send p.client, eStoreDead; return; }
            sessKV[p.skey] = (val = p.val, writer = p.writer, wgen = p.wgen);
            announce eAnnSessionSet, (syncN = syncN, skey = p.skey, val = p.val, writer = p.writer, wgen = p.wgen, ghost = p.ghost);
            send p.client, eStoreAck;
        }

        on eGSessionGetReq do (p: (client: machine, agen: int, reader: int, rgen: int, skey: int)) {
            var c: tSessCell;
            maybeCrash();
            if (p.agen in deadGens) { send p.client, eStoreDead; return; }
            if (p.skey in sessKV) {
                c = sessKV[p.skey];
                announce eAnnSessionRead, (syncN = syncN, reader = p.reader, rgen = p.rgen, skey = p.skey, found = true, val = c.val, writer = c.writer, wgen = c.wgen);
                send p.client, eGSessionGetResp, (found = true, val = c.val, writer = c.writer, wgen = c.wgen);
            } else {
                announce eAnnSessionRead, (syncN = syncN, reader = p.reader, rgen = p.rgen, skey = p.skey, found = false, val = -1, writer = -1, wgen = -1);
                send p.client, eGSessionGetResp, (found = false, val = -1, writer = -1, wgen = -1);
            }
        }

        on eGCheckpointReq do (p: (client: machine, agen: int, ck: tGCkpt, forced: bool)) {
            maybeCrash();
            if (p.agen in deadGens) { send p.client, eStoreDead; return; }
            ckpt = p.ck;
            hasCkpt = true;
            announce eAnnCheckpoint, (syncN = syncN, forced = p.forced);
            send p.client, eStoreAck;
        }

        on eGReadCkptReq do (p: (client: machine)) {
            send p.client, eGReadCkptResp, (ck = ckpt, has = hasCkpt);
        }

        on eGReadRowsReq do (p: (client: machine, agen: int, key: int)) {
            var out: seq[tGRow];
            var ids: seq[int];
            var i: int;
            maybeCrash();
            if (p.agen in deadGens) { send p.client, eStoreDead; return; }
            if (p.key in curPart) {
                ids = keys(curPart[p.key]);
                i = 0;
                while (i < sizeof(ids)) {
                    out += (sizeof(out), curPart[p.key][ids[i]]);
                    i = i + 1;
                }
                send p.client, eGReadRowsResp, (rows = out, present = true);
            } else {
                send p.client, eGReadRowsResp, (rows = out, present = false);
            }
        }

        on eGReadStampsReq do (p: (client: machine, agen: int)) {
            maybeCrash();
            if (p.agen in deadGens) { send p.client, eStoreDead; return; }
            send p.client, eGReadStampsResp, (stamps = curStamps, owners = curOwners);
        }

        on eGSealReq do (p: (client: machine, agen: int, keep: seq[int], doSweep: bool, genTable: map[int, int])) {
            // Guaranteed resolution point for an armed crash (walker
            // parity): fires either just before the seal or right after.
            if (armed && armedGen == p.agen) {
                if (choose(2) == 0) {
                    fireCrash();
                } else {
                    commitSeal(p.keep, p.doSweep, p.genTable);
                    send p.client, eStoreAck;
                    fireCrash();
                    return;
                }
            }
            if (p.agen in deadGens) { send p.client, eStoreDead; return; }
            commitSeal(p.keep, p.doSweep, p.genTable);
            send p.client, eStoreAck;
        }

        on eReadSealedReq do (p: (client: machine)) {
            send p.client, eReadSealedResp, (sealed = sealed,);
        }

        on eCrashArm do (p: (client: machine, agen: int)) {
            armed = true;
            armedGen = p.agen;
            armedClient = p.client;
            send p.client, eCrashArmed;
        }
    }

    // Seal-time sweep (SPEC 3 seal sequence): drop partitions,
    // manifest entries, and stamps for keys outside the final demand
    // closure. Markers never travel into the sealed artifact (per-sync
    // scoping pin) — modeled by curMarkers being sync-local state that
    // eGReset discards.
    fun commitSeal(keep: seq[int], doSweep: bool, genTable: map[int, int]) {
        var ks: seq[int];
        var i: int;
        if (doSweep) {
            ks = keys(curPart);
            i = 0;
            while (i < sizeof(ks)) {
                if (!inKeep(keep, ks[i])) {
                    curPart -= ks[i];
                    if (ks[i] in curMan) { curMan -= ks[i]; }
                    if (ks[i] in curStamps) { curStamps -= ks[i]; }
                }
                i = i + 1;
            }
            ks = keys(curMan);
            i = 0;
            while (i < sizeof(ks)) {
                if (!inKeep(keep, ks[i])) {
                    curMan -= ks[i];
                }
                i = i + 1;
            }
        }
        sealed = true;
        announce eAnnGSeal, (syncN = syncN, partition = curPart, manifest = curMan, stamps = curStamps, genTable = genTable);
    }

    fun inKeep(keep: seq[int], k: int): bool {
        var i: int;
        i = 0;
        while (i < sizeof(keep)) {
            if (keep[i] == k) { return true; }
            i = i + 1;
        }
        return false;
    }

    // Same-key distinct-derivation detection (SPEC 4b poison row):
    // the second distinct hash's first hash-carrying commit poisons
    // the key and VOIDS its marker (R2-M8).
    fun poisonCheck(key: int, hash: int) {
        var m: tMarker;
        if (key in firstHash && firstHash[key] != hash) {
            if (!(key in poisoned)) {
                poisoned[key] = true;
                if (key in curMarkers) {
                    m = curMarkers[key];
                    m.voided = true;
                    curMarkers[key] = m;
                }
                announce eAnnPoison, (syncN = syncN, key = key);
            }
            return;
        }
        firstHash[key] = hash;
    }

    fun maybeCrash() {
        if (armed && choose(2) == 0) {
            fireCrash();
        }
    }

    fun rowsOf(part: tGPart, key: int): seq[tGRow] {
        var out: seq[tGRow];
        var ids: seq[int];
        var i: int;
        if (!(key in part)) { return out; }
        ids = keys(part[key]);
        i = 0;
        while (i < sizeof(ids)) {
            out += (sizeof(out), part[key][ids[i]]);
            i = i + 1;
        }
        return out;
    }

    fun fireCrash() {
        deadGens[armedGen] = true;
        armed = false;
        announce eAnnCrash, (syncN = syncN,);
        send armedClient, eCrashAck;
    }
}
