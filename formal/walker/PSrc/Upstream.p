/* MUpstream: the external system of record (MODEL_SPEC 3).
   Truthful: a validator (epoch-valued) validates iff it equals the
   current epoch. Row table: epoch 1 has rows {0,1}; epoch >= 2 has {0}
   (row 1 deleted upstream at the e1->e2 mutation), so unions and
   resurrections are content-visible with 2 row ids (MODEL_SPEC 8). */

// Row ids present at an epoch. Shared by MUpstream (fetch) and the
// monitors (the rows(s,e) fold table) so the oracle and the system
// draw from one table.
fun upstreamRowIds(epoch: int): seq[int] {
    var ids: seq[int];
    ids += (0, 0);
    if (epoch <= 1) {
        ids += (1, 1);
    }
    return ids;
}

// rows(s, e) as id -> content-epoch map (monitor-side fold table).
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

machine MUpstream {
    var epochs: map[int, int];  // scope -> current epoch

    start state Serving {
        entry {
            epochs[0] = 1;
            epochs[1] = 1;
        }

        on eValidateReq do (p: (client: machine, scope: int, v: int)) {
            send p.client, eValidateResp, (ok = p.v == epochs[p.scope], epoch = epochs[p.scope]);
        }

        on eFetchReq do (p: (client: machine, scope: int, page: int)) {
            var ids: seq[int];
            var rows: seq[tRow];
            var e: int;
            e = epochs[p.scope];
            ids = upstreamRowIds(e);
            if (p.page < sizeof(ids)) {
                rows += (0, (id = ids[p.page], epoch = e, hops = 0, config = 0, stamp = -1));
            }
            // Fixed 2-page rounds (pages per scope per round <= 2).
            send p.client, eFetchResp, (rows = rows, epoch = e, morePages = p.page == 0);
        }

        on eDiffReq do (p: (client: machine, scope: int, fromEpoch: int, page: int)) {
            var e: int;
            var ups: seq[tRow];
            var rms: seq[int];
            var fromIds: seq[int];
            var i: int;
            e = epochs[p.scope];
            // Diff from base epoch to current: page 0 carries upserts
            // (rows whose content changed), page 1 carries removes
            // (ids present at base, absent now). Truthful and total.
            if (p.page == 0) {
                if (p.fromEpoch != e) {
                    ups += (0, (id = 0, epoch = e, hops = 0, config = 0, stamp = -1));
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
}
