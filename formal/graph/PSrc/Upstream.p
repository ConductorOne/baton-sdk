/* MGUpstream: the external system of record (walker parity).
   Truthful: a validator (content-epoch-valued) validates iff its
   content equals the current content. Row table: epoch 1 has rows
   {0,1}; epoch >= 2 has {0} (row 1 deleted at the e1->e2 mutation),
   so unions and resurrections are content-visible with 2 row ids.

   FLAP-BACK (G2 flap-back probe, G8d): with cfg.flapBack, raw epoch
   >= 3 serves content identical to epoch 1. The upstream reports
   CONTENT epochs everywhere — validate responses, fetched rows, diff
   pages — so raw epochs never escape it and every downstream consumer
   (markers, manifests, folds, SealExpect worlds) stays coherent. */

fun upstreamRowIds(epoch: int): seq[int] {
    var ids: seq[int];
    ids += (0, 0);
    if (epoch <= 1) {
        ids += (1, 1);
    }
    return ids;
}

// rows(k, e) as id -> content-epoch map (monitor-side fold table).
fun rowsAt(epoch: int): map[int, int] {
    var ids: seq[int];
    var m: map[int, int];
    var i: int;
    ids = upstreamRowIds(epoch);
    i = 0;
    while (i < sizeof(ids)) {
        m[ids[i]] = epoch;
        i = i + 1;
    }
    return m;
}

machine MGUpstream {
    var epochs: map[int, int];  // key -> current RAW epoch
    var flapBack: bool;

    start state Serving {
        entry (p: (flapBack: bool)) {
            flapBack = p.flapBack;
                epochs[0] = 1;
                epochs[1] = 1;
                epochs[2] = 1;
                epochs[3] = 1;
                epochs[4] = 1;
                epochs[5] = 1;
        }

        on eValidateReq do (p: (client: machine, scope: int, v: int)) {
            send p.client, eValidateResp, (ok = p.v == contentEpoch(p.scope), epoch = contentEpoch(p.scope));
        }

        on eFetchReq do (p: (client: machine, scope: int, page: int)) {
            var ids: seq[int];
            var rows: seq[tGRow];
            var e: int;
            e = contentEpoch(p.scope);
            ids = upstreamRowIds(e);
            if (p.page < sizeof(ids)) {
                rows += (0, (id = ids[p.page], epoch = e, hops = 0, childHash = 0, sVal = -1, sWriter = -1, sWGen = -1));
            }
            // Fixed 2-page rounds (pages per key per round <= 2).
            send p.client, eFetchResp, (rows = rows, epoch = e, morePages = p.page == 0);
        }

        on eDiffReq do (p: (client: machine, scope: int, fromEpoch: int, page: int)) {
            var e: int;
            var ups: seq[tGRow];
            var rms: seq[int];
            var fromIds: seq[int];
            var toIds: seq[int];
            var i: int;
            e = contentEpoch(p.scope);
            // Truthful total diff from the base content to the current
            // content: page 0 upserts (every current row when content
            // differs), page 1 removes (base rows absent now).
            if (p.page == 0) {
                if (p.fromEpoch != e) {
                    toIds = upstreamRowIds(e);
                    i = 0;
                    while (i < sizeof(toIds)) {
                        ups += (sizeof(ups), (id = toIds[i], epoch = e, hops = 0, childHash = 0, sVal = -1, sWriter = -1, sWGen = -1));
                        i = i + 1;
                    }
                }
            } else {
                fromIds = upstreamRowIds(p.fromEpoch);
                i = 0;
                while (i < sizeof(fromIds)) {
                    if (!(fromIds[i] in rowsAt(e))) {
                        rms += (sizeof(rms), fromIds[i]);
                    }
                    i = i + 1;
                }
            }
            send p.client, eDiffResp, (upserts = ups, removes = rms, epoch = e, morePages = p.page == 0);
        }

        on eMutate do (p: (client: machine, scope: int)) {
            epochs[p.scope] = epochs[p.scope] + 1;
            send p.client, eMutateAck;
        }
    }

    fun contentEpoch(scope: int): int {
        if (flapBack && epochs[scope] >= 3) { return 1; }
        return epochs[scope];
    }
}
