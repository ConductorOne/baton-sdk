/* Property monitors (SPEC 7). Announce-subscribed; ghost fields are
   labels of decisions the model already made. Fold and legality
   implement the walker round-5 F1 pin with SPEC 4d generation
   grounding: a round completes at the commit of its last prescribed
   op; a generation's COMPLETE round stays in fold and count until
   ADOPTED (contribution transfers) or SUPERSEDED, and the
   supersession removal is DEATH-GATED (a live-rows removal reading
   would dissolve the walker cell-4 alarm — the suppressionOff racing
   red lives on exactly this gate). Poisoned keys are legality- and
   content-EXEMPT (the poison is the alarm; the scope is
   seal-excluded by SealExpect). */

type tGRound = (key: int, verdict: tGVerdict, consultEpoch: int, vBase: int, hasCopy: bool, completed: bool, node: int, gen: int);

// Partition key content as id -> epoch (ghost content tag).
fun contentOfG(part: tGPart, key: int): map[int, int] {
    var out: map[int, int];
    var ids: seq[int];
    var i: int;
    if (!(key in part)) { return out; }
    ids = keys(part[key]);
    i = 0;
    while (i < sizeof(ids)) {
        out[ids[i]] = part[key][ids[i]].epoch;
        i = i + 1;
    }
    return out;
}

spec GP1 observes eAnnSyncStart, eAnnClear, eAnnReplayCopy, eAnnUpsert, eAnnTombstones, eAnnPublish, eAnnMarkerPut, eAnnAdopt, eAnnGenBump, eAnnPoison, eAnnGSeal {
    var rounds: map[int, tGRound];
    var foldEpoch: map[int, int];        // key -> folded content epoch
    var copyRounds: map[int, seq[int]];  // key -> LIVE copy-contributing roundIds
    var latestGen: map[int, int];        // node -> latest generation seen
    var poisonedK: map[int, bool];

    start state Monitoring {
        on eAnnSyncStart do (p: (syncN: int)) {
            rounds = default(map[int, tGRound]);
            foldEpoch = default(map[int, int]);
            copyRounds = default(map[int, seq[int]]);
            latestGen = default(map[int, int]);
            poisonedK = default(map[int, bool]);
        }
        on eAnnGenBump do (p: (syncN: int, node: int, newGen: int, reason: int)) {
            bumpLatest(p.node, p.newGen);
        }
        on eAnnPoison do (p: (syncN: int, key: int)) {
            poisonedK[p.key] = true;
        }
        on eAnnMarkerPut do (p: (syncN: int, key: int, node: int, gen: int, roundId: int, pubBearing: bool, contentEpoch: int, ghost: tGGhost)) {
            trackOp(p.key, p.ghost, false, -1);
        }
        on eAnnClear do (p: (syncN: int, key: int, ghost: tGGhost)) {
            var live: seq[int];
            var i: int;
            var r: tGRound;
            trackOp(p.key, p.ghost, false, -1);
            // SPEC 4d supersession removal, DEATH-GATED: the clear
            // removes the copy contribution of DEAD complete rounds
            // only; a live round's contribution stays (and a second
            // completed copy then trips legality).
            if (p.key in copyRounds) {
                i = 0;
                while (i < sizeof(copyRounds[p.key])) {
                    r = rounds[copyRounds[p.key][i]];
                    if (r.node in latestGen && r.gen < latestGen[r.node]) {
                        live = copyRounds[p.key];
                        live -= i;
                        copyRounds[p.key] = live;
                    } else {
                        i = i + 1;
                    }
                }
            }
        }
        on eAnnReplayCopy do (p: (syncN: int, key: int, vBase: int, rows: seq[tGRow], ghost: tGGhost)) {
            trackOp(p.key, p.ghost, true, p.vBase);
        }
        on eAnnUpsert do (p: (syncN: int, key: int, rows: seq[tGRow], ghost: tGGhost)) {
            trackOp(p.key, p.ghost, false, -1);
        }
        on eAnnTombstones do (p: (syncN: int, key: int, removes: seq[int], ghost: tGGhost)) {
            trackOp(p.key, p.ghost, false, -1);
        }
        on eAnnPublish do (p: (syncN: int, key: int, v: int, ghost: tGGhost)) {
            // Attestation-only at publish (walker parity): truthful
            // validators are epoch-valued.
            assert p.v == p.ghost.consultEpoch, "P1-ATTEST-PUBLISH: published validator epoch differs from the round's verdict epoch";
            trackOp(p.key, p.ghost, false, -1);
        }
        on eAnnAdopt do (p: (syncN: int, key: int, node: int, fromGen: int, toGen: int, adoptedRoundId: int, rows: seq[tGRow], ghost: tGGhost)) {
            var r: tGRound;
            bumpLatest(p.ghost.node, p.ghost.gen);
            // Contribution TRANSFER (SPEC 4a/4d): the adopted round is
            // re-grounded at the adopting generation; fold and count
            // are unchanged.
            if (p.adoptedRoundId in rounds) {
                r = rounds[p.adoptedRoundId];
                r.gen = p.toGen;
                rounds[p.adoptedRoundId] = r;
            }
        }
        on eAnnGSeal do (p: (syncN: int, partition: tGPart, manifest: map[int, int], stamps: map[int, map[int, int]], genTable: map[int, int])) {
            var ks: seq[int];
            var i: int;
            var k: int;
            var have: map[int, int];
            ks = keys(p.partition);
            i = 0;
            while (i < sizeof(ks)) {
                k = ks[i];
                if (!(k in poisonedK) && sizeof(p.partition[k]) > 0) {
                    have = contentOfG(p.partition, k);
                    if (k in foldEpoch) {
                        assert have == rowsAt(foldEpoch[k]), "P1-CONTENT: sealed partition diverges from the round-log fold";
                        if (k in p.manifest) {
                            assert p.manifest[k] == foldEpoch[k], "P1-ATTEST-SEAL: manifest entry epoch differs from the fold epoch";
                        }
                    } else {
                        assert false, "P1-CONTENT: incomplete-round debris sealed";
                    }
                }
                i = i + 1;
            }
            // A manifest entry over an empty fold attests a
            // composition the round log does not contain (walker
            // round-7 F3 pin).
            ks = keys(p.manifest);
            i = 0;
            while (i < sizeof(ks)) {
                if (!(ks[i] in poisonedK)) {
                    assert ks[i] in foldEpoch, "P1-ATTEST-EMPTY: manifest entry published over an empty fold";
                }
                i = i + 1;
            }
        }
    }

    fun bumpLatest(node: int, gen: int) {
        if (!(node in latestGen) || gen > latestGen[node]) {
            latestGen[node] = gen;
        }
    }

    fun trackOp(key: int, ghost: tGGhost, isCopy: bool, vBase: int) {
        var info: tGRound;
        var cr: seq[int];
        bumpLatest(ghost.node, ghost.gen);
        if (ghost.roundId in rounds) {
            info = rounds[ghost.roundId];
        } else {
            info = (key = key, verdict = ghost.verdict, consultEpoch = ghost.consultEpoch, vBase = -1, hasCopy = false, completed = false, node = ghost.node, gen = ghost.gen);
        }
        info.consultEpoch = ghost.consultEpoch;
        if (isCopy) {
            info.hasCopy = true;
            info.vBase = vBase;
        }
        if (ghost.lastOp) {
            info.completed = true;
        }
        rounds[ghost.roundId] = info;
        if (ghost.lastOp) {
            // Round completion IS the fold order. Replacement counting
            // happens HERE: committed copies within COMPLETE rounds
            // only (walker round-7 F2 pin), over LIVE contributions
            // only (SPEC 4d grounding).
            if (info.hasCopy) {
                if (key in copyRounds) { cr = copyRounds[key]; }
                cr += (sizeof(cr), ghost.roundId);
                copyRounds[key] = cr;
                if (!(key in poisonedK)) {
                    assert sizeof(copyRounds[key]) <= 1, "P1-LEGALITY: second live complete-round replacement copy for one key in one sync";
                }
            }
            if (ghost.verdict == GV_REPLAY) {
                if (info.hasCopy) {
                    foldEpoch[key] = info.vBase;
                }
            } else if (ghost.verdict != GV_ADOPT) {
                foldEpoch[key] = ghost.consultEpoch;
            }
        }
    }
}

spec GP2 observes eAnnScenarioInit, eAnnSyncStart, eAnnConsult, eAnnPoison, eAnnGSeal {
    var bound: int;
    var consulted: map[int, bool];
    var poisonedK: map[int, bool];

    start state Monitoring {
        on eAnnScenarioInit do (p: (maxStaleness: int)) {
            bound = p.maxStaleness;
        }
        on eAnnSyncStart do (p: (syncN: int)) {
            consulted = default(map[int, bool]);
            poisonedK = default(map[int, bool]);
        }
        on eAnnPoison do (p: (syncN: int, key: int)) {
            poisonedK[p.key] = true;
        }
        on eAnnConsult do (p: (syncN: int, key: int, hit: bool, v: int, validated: bool, epoch: int, freshFetch: bool, diffVerdict: bool, attempt: int, node: int, gen: int)) {
            // Consulted-against-upstream (walker pin): validation
            // match, fresh fetch, or CHANGED-WITH-DIFF verdict. The
            // adopting MATCH qualifies (MATCH-only eligibility makes
            // the freshness claim true, SPEC 4a).
            if (p.validated || p.freshFetch || p.diffVerdict) {
                consulted[p.key] = true;
            }
        }
        on eAnnGSeal do (p: (syncN: int, partition: tGPart, manifest: map[int, int], stamps: map[int, map[int, int]], genTable: map[int, int])) {
            var ks: seq[int];
            var ids: seq[int];
            var i: int;
            var j: int;
            var k: int;
            ks = keys(p.partition);
            i = 0;
            while (i < sizeof(ks)) {
                k = ks[i];
                if (!(k in poisonedK) && sizeof(p.partition[k]) > 0) {
                    assert k in consulted, "P2-CONSULT: sealed key not consulted against upstream this sync";
                    ids = keys(p.partition[k]);
                    j = 0;
                    while (j < sizeof(ids)) {
                        assert p.partition[k][ids[j]].hops <= bound, "P2-STALENESS: row replay-travel exceeds the scenario bound";
                        j = j + 1;
                    }
                }
                i = i + 1;
            }
        }
    }
}

spec GP3prime observes eAnnSyncStart, eAnnConsult, eAnnPoison, eAnnGSeal {
    var lastEpoch: map[int, int];   // key -> epoch of last qualifying consult
    var poisonedK: map[int, bool];

    start state Monitoring {
        on eAnnSyncStart do (p: (syncN: int)) {
            lastEpoch = default(map[int, int]);
            poisonedK = default(map[int, bool]);
        }
        on eAnnPoison do (p: (syncN: int, key: int)) {
            poisonedK[p.key] = true;
        }
        on eAnnConsult do (p: (syncN: int, key: int, hit: bool, v: int, validated: bool, epoch: int, freshFetch: bool, diffVerdict: bool, attempt: int, node: int, gen: int)) {
            if (p.validated || p.freshFetch || p.diffVerdict) {
                lastEpoch[p.key] = p.epoch;
            }
        }
        on eAnnGSeal do (p: (syncN: int, partition: tGPart, manifest: map[int, int], stamps: map[int, map[int, int]], genTable: map[int, int])) {
            var ks: seq[int];
            var i: int;
            var k: int;
            ks = keys(p.partition);
            i = 0;
            while (i < sizeof(ks)) {
                k = ks[i];
                if (k in lastEpoch && !(k in poisonedK) && sizeof(p.partition[k]) > 0) {
                    assert contentOfG(p.partition, k) == rowsAt(lastEpoch[k]), "P3'-COHERENCE: sealed content epoch differs from last consulted verdict epoch";
                }
                i = i + 1;
            }
        }
    }
}

// P-GEN (G-RULE-3 / R2-N3): no two attempts contain store-commit
// announces attributed to the same (node, generation); adoption
// re-announces attribute to the ADOPTER. Scoped per sync (the
// generation table is per-sync scheduling state).
spec PGEN observes eAnnSyncStart, eAnnClear, eAnnReplayCopy, eAnnUpsert, eAnnTombstones, eAnnPublish, eAnnMarkerPut, eAnnAdopt, eAnnSessionSet {
    var attemptOf: map[int, int];   // node*1000 + gen -> attempt

    start state Monitoring {
        on eAnnSyncStart do (p: (syncN: int)) {
            attemptOf = default(map[int, int]);
        }
        on eAnnClear do (p: (syncN: int, key: int, ghost: tGGhost)) { check(p.ghost); }
        on eAnnReplayCopy do (p: (syncN: int, key: int, vBase: int, rows: seq[tGRow], ghost: tGGhost)) { check(p.ghost); }
        on eAnnUpsert do (p: (syncN: int, key: int, rows: seq[tGRow], ghost: tGGhost)) { check(p.ghost); }
        on eAnnTombstones do (p: (syncN: int, key: int, removes: seq[int], ghost: tGGhost)) { check(p.ghost); }
        on eAnnPublish do (p: (syncN: int, key: int, v: int, ghost: tGGhost)) { check(p.ghost); }
        on eAnnMarkerPut do (p: (syncN: int, key: int, node: int, gen: int, roundId: int, pubBearing: bool, contentEpoch: int, ghost: tGGhost)) { check(p.ghost); }
        on eAnnAdopt do (p: (syncN: int, key: int, node: int, fromGen: int, toGen: int, adoptedRoundId: int, rows: seq[tGRow], ghost: tGGhost)) { check(p.ghost); }
        on eAnnSessionSet do (p: (syncN: int, skey: int, val: int, writer: int, wgen: int, ghost: tGGhost)) { check(p.ghost); }
    }

    fun check(g: tGGhost) {
        var gk: int;
        gk = g.node * 1000 + g.gen;
        if (gk in attemptOf) {
            assert attemptOf[gk] == g.attempt, "P-GEN: store-commit announces attributed to one (node, generation) in two attempts (identity reuse)";
        } else {
            attemptOf[gk] = g.attempt;
        }
    }
}

// P-MARK (R2-F4): a marker present for a key ⟹ the key's partition
// equals the marked round's committed outputs. Checkable form: no
// FOREIGN store op mutates a marked key's partition — every legal
// transition rides an op that first rebinds or removes the marker
// (unit put overwrites first; REPLACES clear deletes first; poison
// voids; adoption rebinds).
spec PMARK observes eAnnSyncStart, eAnnMarkerPut, eAnnMarkerDel, eAnnAdopt, eAnnPoison, eAnnClear, eAnnReplayCopy, eAnnUpsert, eAnnTombstones {
    var markedRound: map[int, int];   // key -> marking roundId
    var voidedK: map[int, bool];

    start state Monitoring {
        on eAnnSyncStart do (p: (syncN: int)) {
            markedRound = default(map[int, int]);
            voidedK = default(map[int, bool]);
        }
        on eAnnMarkerPut do (p: (syncN: int, key: int, node: int, gen: int, roundId: int, pubBearing: bool, contentEpoch: int, ghost: tGGhost)) {
            markedRound[p.key] = p.roundId;
            if (p.key in voidedK) { voidedK -= p.key; }
        }
        on eAnnMarkerDel do (p: (syncN: int, key: int)) {
            if (p.key in markedRound) { markedRound -= p.key; }
        }
        on eAnnAdopt do (p: (syncN: int, key: int, node: int, fromGen: int, toGen: int, adoptedRoundId: int, rows: seq[tGRow], ghost: tGGhost)) {
            markedRound[p.key] = p.ghost.roundId;
        }
        on eAnnPoison do (p: (syncN: int, key: int)) {
            voidedK[p.key] = true;
        }
        on eAnnClear do (p: (syncN: int, key: int, ghost: tGGhost)) { check(p.key, p.ghost); }
        on eAnnReplayCopy do (p: (syncN: int, key: int, vBase: int, rows: seq[tGRow], ghost: tGGhost)) { check(p.key, p.ghost); }
        on eAnnUpsert do (p: (syncN: int, key: int, rows: seq[tGRow], ghost: tGGhost)) { check(p.key, p.ghost); }
        on eAnnTombstones do (p: (syncN: int, key: int, removes: seq[int], ghost: tGGhost)) { check(p.key, p.ghost); }
    }

    fun check(key: int, g: tGGhost) {
        if (key in markedRound && !(key in voidedK)) {
            assert markedRound[key] == g.roundId, "P-MARK: a foreign round mutated a marked key's partition (marker no longer describes the partition)";
        }
    }
}

// SealExpect: the scripted closure + content oracle (SPEC 7 closure
// oracle / SPEC 9 SealExpect discipline). The env announces the
// expected sealed key set and per-key LIVE content epoch at EVERY
// attempt start; the monitor accumulates the sync's acceptable
// epoch SET per key. Sealed content must match SOME attempt-start
// epoch of this sync — the artifact freshness contract is
// SYNC-scoped (staleness bound 1), and a key whose derivation
// completed and checkpointed before a crash legitimately seals the
// earlier attempt's world (completed-across-crash, G-RULE-2;
// calibration find G1-CAL-1). Closure is exact both directions.
// Poisoned/excluded keys are exempt.
spec SealExpectG observes eAnnSyncStart, eAnnExpectSeal, eAnnPoison, eAnnGSeal {
    var active: bool;
    var expSync: int;
    var acc: map[int, seq[int]];
    var excluded: map[int, bool];

    start state Monitoring {
        on eAnnSyncStart do (p: (syncN: int)) {
            if (active && p.syncN != expSync) { active = false; }
        }
        on eAnnExpectSeal do (p: (syncN: int, exp: map[int, int], excluded: map[int, bool])) {
            var ks: seq[int];
            var es: seq[int];
            var i: int;
            if (!active || expSync != p.syncN) {
                acc = default(map[int, seq[int]]);
                excluded = default(map[int, bool]);
            }
            active = true;
            expSync = p.syncN;
            ks = keys(p.exp);
            i = 0;
            while (i < sizeof(ks)) {
                es = default(seq[int]);
                if (ks[i] in acc) { es = acc[ks[i]]; }
                if (!inIntSeq(es, p.exp[ks[i]])) {
                    es += (sizeof(es), p.exp[ks[i]]);
                }
                acc[ks[i]] = es;
                i = i + 1;
            }
            ks = keys(p.excluded);
            i = 0;
            while (i < sizeof(ks)) {
                excluded[ks[i]] = true;
                i = i + 1;
            }
        }
        on eAnnPoison do (p: (syncN: int, key: int)) {
            excluded[p.key] = true;
        }
        on eAnnGSeal do (p: (syncN: int, partition: tGPart, manifest: map[int, int], stamps: map[int, map[int, int]], genTable: map[int, int])) {
            var ks: seq[int];
            var i: int;
            var j: int;
            var k: int;
            var anyMatch: bool;
            var have: map[int, int];
            if (!active || p.syncN != expSync) { return; }
            ks = keys(acc);
            i = 0;
            while (i < sizeof(ks)) {
                k = ks[i];
                if (!(k in excluded)) {
                    assert k in p.partition && sizeof(p.partition[k]) > 0, "SEAL-EXPECT: expected demand-closure key missing from the sealed artifact";
                    have = contentOfG(p.partition, k);
                    anyMatch = false;
                    j = 0;
                    while (j < sizeof(acc[k])) {
                        if (have == rowsAt(acc[k][j])) { anyMatch = true; }
                        j = j + 1;
                    }
                    assert anyMatch, "SEAL-EXPECT: sealed content matches no attempt-start world of this sync";
                }
                i = i + 1;
            }
            ks = keys(p.partition);
            i = 0;
            while (i < sizeof(ks)) {
                k = ks[i];
                if (!(k in excluded) && sizeof(p.partition[k]) > 0) {
                    assert k in acc, "SEAL-EXPECT: sealed key outside the scripted demand closure";
                }
                i = i + 1;
            }
            active = false;
        }
    }
}

// P-ADOPT (adopt legality, R2-F2 MATCH-only eligibility): every
// adoption must be justified by a prior validated MATCH consult by
// the same (node, generation) — adoption and its consult always
// share one execution. A FAIL-consult followed by adoption is the
// G1c laundering mutant: it smuggles a stale world past the seal
// inside the sync-scoped freshness envelope, so no artifact-level
// oracle can see it; the mechanism monitor is the kill.
spec PADOPT observes eAnnSyncStart, eAnnConsult, eAnnAdopt {
    var matched: map[int, bool];   // node*1000+gen -> validated MATCH seen

    start state Monitoring {
        on eAnnSyncStart do (p: (syncN: int)) {
            matched = default(map[int, bool]);
        }
        on eAnnConsult do (p: (syncN: int, key: int, hit: bool, v: int, validated: bool, epoch: int, freshFetch: bool, diffVerdict: bool, attempt: int, node: int, gen: int)) {
            if (p.hit && p.validated) {
                matched[p.node * 1000 + p.gen] = true;
            }
        }
        on eAnnAdopt do (p: (syncN: int, key: int, node: int, fromGen: int, toGen: int, adoptedRoundId: int, rows: seq[tGRow], ghost: tGGhost)) {
            assert (p.ghost.node * 1000 + p.ghost.gen) in matched, "P-ADOPT: adoption without a validated MATCH consult by the adopting generation (MATCH-only eligibility violated)";
        }
    }
}

// P6-G (laundering oracle, all legs; SPEC 7): at seal, every row
// derived THIS SYNC (hops 0) embedding a real session value must
// embed the FINAL session value — comparison by VALUE, so same-value
// re-derivation is green (R2-F6). Mechanism-independent.
spec GP6G observes eAnnSyncStart, eAnnSessionSet, eAnnGSeal {
    var sess: map[int, int];

    start state Monitoring {
        on eAnnSyncStart do (p: (syncN: int)) {
            sess = default(map[int, int]);
        }
        on eAnnSessionSet do (p: (syncN: int, skey: int, val: int, writer: int, wgen: int, ghost: tGGhost)) {
            sess[p.skey] = p.val;
        }
        on eAnnGSeal do (p: (syncN: int, partition: tGPart, manifest: map[int, int], stamps: map[int, map[int, int]], genTable: map[int, int])) {
            var ks: seq[int];
            var ids: seq[int];
            var i: int;
            var j: int;
            var k: int;
            var r: tGRow;
            ks = keys(p.partition);
            i = 0;
            while (i < sizeof(ks)) {
                k = ks[i];
                ids = keys(p.partition[k]);
                j = 0;
                while (j < sizeof(ids)) {
                    r = p.partition[k][ids[j]];
                    if (r.sVal >= 0 && r.hops == 0) {
                        assert 0 in sess && sess[0] == r.sVal, "P6-G: sealed row embeds a session value differing from the final derived value (laundered dead read)";
                    }
                    j = j + 1;
                }
                i = i + 1;
            }
        }
    }
}

// P6-E (mechanism conformance, E+B; SPEC 7): retraction liveness —
// every reader execution of a value whose writer generation is dead
// at seal re-ran (a later read by the same reader exists). Asserted
// only in E+B cells.
spec GP6E observes eAnnSyncStart, eAnnSessionRead, eAnnGSeal {
    var reads: seq[tReadRec];

    start state Monitoring {
        on eAnnSyncStart do (p: (syncN: int)) {
            reads = default(seq[tReadRec]);
        }
        on eAnnSessionRead do (p: (syncN: int, reader: int, rgen: int, skey: int, found: bool, val: int, writer: int, wgen: int)) {
            if (p.found) {
                reads += (sizeof(reads), (reader = p.reader, rgen = p.rgen, skey = p.skey, val = p.val, writer = p.writer, wgen = p.wgen));
            }
        }
        on eAnnGSeal do (p: (syncN: int, partition: tGPart, manifest: map[int, int], stamps: map[int, map[int, int]], genTable: map[int, int])) {
            var i: int;
            var j: int;
            var later: bool;
            i = 0;
            while (i < sizeof(reads)) {
                if (reads[i].writer in p.genTable && reads[i].wgen < p.genTable[reads[i].writer]) {
                    later = false;
                    j = 0;
                    while (j < sizeof(reads)) {
                        if (reads[j].reader == reads[i].reader && reads[j].rgen > reads[i].rgen) {
                            later = true;
                        }
                        j = j + 1;
                    }
                    assert later, "P6-E: a reader execution of a dead session value never re-ran before seal (retraction liveness)";
                }
                i = i + 1;
            }
        }
    }
}

// REDO-PROBE (existence probe, walker C1-probe pattern): asserts NO
// forced re-admission (retraction- or observation-forced) ever
// happens. Asserted in cells DECLARING an expected forced-redo count
// >= 1; RED is the pass verdict — the counterexample is the exhibit
// that the mechanism pays its at-least-once cost (R2-F6: the count
// is a metric, never a property; this probe is how a metric claim
// becomes checkable without asserting it on every schedule).
spec REDOPROBE observes eAnnReadmit {
    start state Monitoring {
        on eAnnReadmit do (p: (syncN: int, node: int, hash: int, gen: int, reason: int)) {
            assert p.reason == 1, "REDO-PROBE: a forced redo occurred (existence exhibit, not a failure)";
        }
    }
}

// P5 (artifact demand-closure, both directions, at seal — the G5
// family's subject). Self-contained over the sealed artifact itself,
// no environment counterfactual (which attempt's parent world
// survives a crash is schedule-dependent under sync-scoped
// freshness):
//   CLOSED (P5-UNDER kill): every non-root sealed key is named by
//   some sealed row's childHash — under-sweep debris has no living
//   namer.
//   COMPLETE (P5-OVER / demand-drop kill): every childHash named by
//   a sealed row has a non-empty sealed partition — an overreaching
//   sweep or a dropped admission strands a named child.
// Identity conventions: hash = node id + 1, key = node id.
spec GP5 observes eAnnGSeal {
    start state Monitoring {
        on eAnnGSeal do (p: (syncN: int, partition: tGPart, manifest: map[int, int], stamps: map[int, map[int, int]], genTable: map[int, int])) {
            var named: map[int, bool];
            var ks: seq[int];
            var ids: seq[int];
            var r: tGRow;
            var i: int;
            var j: int;
            ks = keys(p.partition);
            i = 0;
            while (i < sizeof(ks)) {
                ids = keys(p.partition[ks[i]]);
                j = 0;
                while (j < sizeof(ids)) {
                    r = p.partition[ks[i]][ids[j]];
                    if (r.childHash > 0) { named[r.childHash] = true; }
                    j = j + 1;
                }
                i = i + 1;
            }
            i = 0;
            while (i < sizeof(ks)) {
                if (ks[i] != 0 && sizeof(p.partition[ks[i]]) > 0) {
                    assert (ks[i] + 1) in named, "P5-UNDER: sealed artifact contains a partition no sealed row names (un-swept debris)";
                }
                i = i + 1;
            }
            ks = keys(named);
            i = 0;
            while (i < sizeof(ks)) {
                assert (ks[i] - 1) in p.partition && sizeof(p.partition[ks[i] - 1]) > 0, "P5-OVER: a sealed row names a child whose partition is missing (overreach or dropped demand)";
                i = i + 1;
            }
        }
    }
}

// PURGE-PROBE (existence probe, REDO-PROBE pattern): asserts the
// resume-time ∀-purge NEVER fires; RED is the pass verdict — the
// counterexample exhibits the mid-round checkpoint window
// (C-pending & parent-pending) that only per-announce demand timing
// (G-RULE-1 TIMING PIN) makes reachable.
spec PURGEPROBE observes eAnnPurge {
    start state Monitoring {
        on eAnnPurge do (p: (syncN: int, node: int, hash: int)) {
            assert false, "PURGE-PROBE: a resume purge occurred (existence exhibit, not a failure)";
        }
    }
}

// DEAD-DISPATCH (the G5e count oracle in checkable form): no node is
// EVER dispatched while every admitted-by edge names a dead
// generation. Honest mechanisms make this unreachable (E purges at
// resume, S refuses at dispatch); purgeOff executes dead demand.
spec GDEADDISPATCH observes eAnnDeadDispatch {
    start state Monitoring {
        on eAnnDeadDispatch do (p: (syncN: int, node: int, hash: int)) {
            assert false, "DEAD-DISPATCH: a dead-demand node was dispatched (purge/refusal failed)";
        }
    }
}

// PASS-BUDGET (§10.8): the pre-seal pass converges within its budget
// in every honest S cell — a budget-exhausted honest seal is a
// finding, not noise. Kill cells that RELY on exhaustion (writerAdopt
// S) do not assert it.
spec GPASS observes eAnnBudgetExhausted {
    start state Monitoring {
        on eAnnBudgetExhausted do (p: (syncN: int)) {
            assert false, "PASS-BUDGET: the pre-seal pass exhausted its budget on an honest history";
        }
    }
}

// EXEC-BOUND (G6 bake-off count oracle): executions per node per
// sync never exceed the cell's declared bound (announced by the env
// from cfg at scenario init; 0 = unmonitored). The minimal GREEN
// bound is the checker-verified worst-case count for the leg; the
// bound-minus-one RED probe is the existence exhibit that the redo
// is real (adequacy §10.1: count-oracle kills).
spec GEXECBOUND observes eAnnScenarioInit, eAnnExecBound, eAnnSyncStart, eAnnExec {
    var bound: int;
    var counts: map[int, int];

    start state Monitoring {
        on eAnnScenarioInit do (p: (maxStaleness: int)) { bound = 0; }
        on eAnnExecBound do (p: (bound: int)) { bound = p.bound; }
        on eAnnSyncStart do (p: (syncN: int)) { counts = default(map[int, int]); }
        on eAnnExec do (p: (syncN: int, attempt: int, node: int, gen: int)) {
            if (bound <= 0) { return; }
            if (p.node in counts) {
                counts[p.node] = counts[p.node] + 1;
            } else {
                counts[p.node] = 1;
            }
            assert counts[p.node] <= bound, "EXEC-BOUND: a node exceeded the cell's per-sync execution bound";
        }
    }
}

// POISON-PROBE (existence probe, REDO-PROBE pattern): asserts the
// same-key distinct-derivation poison NEVER fires; RED is the pass
// verdict on the G8c chassis (both derivations always commit).
spec POISONPROBE observes eAnnPoison {
    start state Monitoring {
        on eAnnPoison do (p: (syncN: int, key: int)) {
            assert false, "POISON-PROBE: the same-key poison fired (existence exhibit, not a failure)";
        }
    }
}

// P4-STUCK (G7, walker P4 analog): three consecutive attempt
// failures with an identical generation-blind fingerprint and no
// abandon is a stuck sync (attempt budget 3, SPEC 8). The abandon
// ladder gives up after 2, so the honest leg never reaches 3.
spec GP4STUCK observes eAnnSyncStart, eAnnAttemptFail {
    var lastFp: int;
    var streak: int;

    start state Monitoring {
        on eAnnSyncStart do (p: (syncN: int)) { lastFp = -1; streak = 0; }
        on eAnnAttemptFail do (p: (syncN: int, attempt: int, node: int, fingerprint: int)) {
            if (p.fingerprint == lastFp) {
                streak = streak + 1;
            } else {
                lastFp = p.fingerprint;
                streak = 1;
            }
            assert streak < 3, "P4-STUCK: three identical-fingerprint attempt failures without an abandon";
        }
    }
}

// P6-S (mechanism conformance, S; SPEC 7, at-seal form only, R2-F6):
// no sealed output carries a dead-generation stamp. Red MEANS the
// mechanism failed (a dead stamp survived the pre-seal pass).
// Asserted only in S cells.
spec GP6S observes eAnnGSeal {
    start state Monitoring {
        on eAnnGSeal do (p: (syncN: int, partition: tGPart, manifest: map[int, int], stamps: map[int, map[int, int]], genTable: map[int, int])) {
            var ks: seq[int];
            var ns: seq[int];
            var i: int;
            var j: int;
            ks = keys(p.stamps);
            i = 0;
            while (i < sizeof(ks)) {
                if (ks[i] in p.partition && sizeof(p.partition[ks[i]]) > 0) {
                    ns = keys(p.stamps[ks[i]]);
                    j = 0;
                    while (j < sizeof(ns)) {
                        assert !(ns[j] in p.genTable) || p.stamps[ks[i]][ns[j]] >= p.genTable[ns[j]], "P6-S: sealed output carries a dead-generation stamp (the pass failed or exhausted its budget)";
                        j = j + 1;
                    }
                }
                i = i + 1;
            }
        }
    }
}

// SEAL-WORLD (GS-CO-005(d) existence probe, REDO-PROBE pattern): the
// G5d cross-variant seal-world meta-analysis. The env announces a
// target world (manifest restricted to keys sealing non-empty
// partitions) for the interrupted sync; the probe asserts that world
// is NEVER sealed — RED is the pass verdict on reachable-world
// probes, GREEN on the declared-unreachable sweep-failure world.
// Asserted alone in tcG5d* cells; the honest G5a greens carry the
// SealExpect envelope this probe's reachable worlds must sit inside.
spec GSEALWORLD observes eAnnSealWorld, eAnnGSeal {
    var target: map[int, int];
    var targetSync: int;
    var armed: bool;

    start state Monitoring {
        on eAnnSealWorld do (p: (syncN: int, exp: map[int, int])) {
            target = p.exp;
            targetSync = p.syncN;
            armed = true;
        }
        on eAnnGSeal do (p: (syncN: int, partition: tGPart, manifest: map[int, int], stamps: map[int, map[int, int]], genTable: map[int, int])) {
            var world: map[int, int];
            var ks: seq[int];
            var i: int;
            if (!armed || p.syncN != targetSync) { return; }
            ks = keys(p.manifest);
            i = 0;
            while (i < sizeof(ks)) {
                if (ks[i] in p.partition && sizeof(p.partition[ks[i]]) > 0) {
                    world[ks[i]] = p.manifest[ks[i]];
                }
                i = i + 1;
            }
            assert !(world == target), "SEAL-WORLD: the target sealed world was reached";
        }
    }
}
