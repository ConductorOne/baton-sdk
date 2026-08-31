/* Property monitors (MODEL_SPEC 7). Announce-subscribed; ghost fields
   are labels of decisions the model already made (2.4). The fold and
   its legality rules implement the round-5 F1 pin: a round completes
   at the commit of its last prescribed store op; scheduler transitions
   are not fold events; incomplete rounds contribute no fold entry and
   their debris surfaces as content divergence.

   Torn rounds (pages committed under more than one attempt) are outside
   P1-content's and P3's designed domain (MODEL_SPEC 7 BOUNDARY); the
   monitors track attempt ghosts and exclude torn scopes rather than
   trusting the configs to never tear — placement of the single stop is
   explored, so a torn fresh round is reachable and must not false-alarm. */

// Round bookkeeping shared shape: roundId -> info. vBase/hasCopy track
// the replacement copy actually committed (announce-side truth): a
// replacement round folds by the base it COPIED, not by the carrier's
// belief — the distinction scenario 3 (artifact swap) exists to test.
type tRoundInfo = (scope: int, verdict: tVerdict, consultEpoch: int, vBase: int, hasCopy: bool, attempts: map[int, bool], completed: bool);

spec P1 observes eAnnSyncStart, eAnnClear, eAnnReplay, eAnnUpsert, eAnnTombstones, eAnnPublish, eAnnSeal {
    var rounds: map[int, tRoundInfo];
    var foldEpoch: map[int, int];       // scope -> folded content epoch
    var copies: map[int, int];          // scope -> committed replacement copies
    var tornScopes: map[int, bool];

    start state Monitoring {
        on eAnnSyncStart do (p: (syncN: int)) {
            rounds = default(map[int, tRoundInfo]);
            foldEpoch = default(map[int, int]);
            copies = default(map[int, int]);
            tornScopes = default(map[int, bool]);
        }
        on eAnnClear do (p: (syncN: int, scope: int, ghost: tRoundGhost)) {
            trackOp(p.scope, p.ghost, false, -1);
        }
        on eAnnReplay do (p: (syncN: int, scope: int, vBase: int, cBase: int, ghost: tRoundGhost)) {
            // Counting/legality moved to round completion (round-7 F2
            // pin): a copy inside a round that never completes is
            // pre-committed-classification debris — it surfaces through
            // content divergence, never through the count. See trackOp.
            trackOp(p.scope, p.ghost, true, p.vBase);
        }
        on eAnnUpsert do (p: (syncN: int, scope: int, rows: seq[tRow], ghost: tRoundGhost)) {
            trackOp(p.scope, p.ghost, false, -1);
        }
        on eAnnTombstones do (p: (syncN: int, scope: int, removes: seq[int], ghost: tRoundGhost)) {
            trackOp(p.scope, p.ghost, false, -1);
        }
        on eAnnPublish do (p: (syncN: int, scope: int, v: int, ghost: tRoundGhost)) {
            // Publish-time check is ATTESTATION ONLY (B5 permits early
            // token publish; no content check here). Truthful
            // validators are epoch-valued.
            assert p.v == p.ghost.consultEpoch, "P1-ATTEST-PUBLISH: published validator epoch differs from the publishing round's verdict epoch";
            trackOp(p.scope, p.ghost, false, -1);
        }
        on eAnnSeal do (p: (syncN: int, partition: tPartition, manifest: map[int, int], blocked: bool, config: int)) {
            var scopes: seq[int];
            var ids: seq[int];
            var i: int;
            var j: int;
            var s: int;
            var have: map[int, int];
            scopes = keys(p.partition);
            i = 0;
            while (i < sizeof(scopes)) {
                s = scopes[i];
                if (!(s in tornScopes)) {
                    have = contentOf(p.partition, s);
                    if (s in foldEpoch) {
                        assert have == rowsAt(foldEpoch[s]), "P1-CONTENT: sealed partition diverges from the round-log fold";
                        if (s in p.manifest) {
                            assert p.manifest[s] == foldEpoch[s], "P1-ATTEST-SEAL: manifest entry epoch differs from the fold epoch";
                        }
                    } else {
                        // No completed round for this scope: any
                        // content is incomplete-round debris.
                        assert sizeof(have) == 0, "P1-CONTENT: incomplete-round debris sealed";
                    }
                }
                // Clause (c) — CONFIG (MODEL_SPEC 7): every sealed
                // row's ghost config tag equals the sealing attempt's
                // compat config (5a's warmGate kill: K1-tagged rows
                // copied into a K2 attempt). Vacuous when configs are
                // unmodeled (both sides 0). Torn scopes are NOT
                // excluded: config drift is between-attempt by
                // construction, so a mixed-config scope is exactly the
                // alarm, never a legal smear.
                ids = keys(p.partition[s]);
                j = 0;
                while (j < sizeof(ids)) {
                    assert p.partition[s][ids[j]].config == p.config, "P1-CONFIG: sealed row's compat config tag differs from the sealing attempt's config";
                    j = j + 1;
                }
                i = i + 1;
            }
            // Round-7 F3 pin (b): a published manifest entry for a
            // scope whose fold result is EMPTY attests a composition
            // the round log does not contain — attestation violation
            // outright, even when the partition is empty for the scope
            // (the sealed-empty case the partition loop never visits).
            scopes = keys(p.manifest);
            i = 0;
            while (i < sizeof(scopes)) {
                s = scopes[i];
                if (!(s in tornScopes)) {
                    assert s in foldEpoch, "P1-ATTEST-EMPTY: manifest entry published over an empty fold";
                }
                i = i + 1;
            }
        }
    }

    fun trackOp(scope: int, ghost: tRoundGhost, isCopy: bool, vBase: int) {
        var info: tRoundInfo;
        if (ghost.roundId in rounds) {
            info = rounds[ghost.roundId];
        } else {
            info = (scope = scope, verdict = ghost.verdict, consultEpoch = ghost.consultEpoch, vBase = -1, hasCopy = false, attempts = default(map[int, bool]), completed = false);
        }
        info.attempts[ghost.attempt] = true;
        info.consultEpoch = ghost.consultEpoch;
        if (isCopy) {
            info.hasCopy = true;
            info.vBase = vBase;
        }
        if (ghost.lastOp) {
            info.completed = true;
        }
        rounds[ghost.roundId] = info;
        if (sizeof(info.attempts) > 1) {
            tornScopes[scope] = true;
        }
        if (ghost.lastOp && sizeof(info.attempts) == 1) {
            // Round completion IS the fold order (completion = commit
            // of the last prescribed op; this announce). Fold rule
            // (round-5 F1 + round-4 F2): a completed REPLACEMENT round
            // folds by the base it ACTUALLY copied (announce-side
            // vBase), not the carrier's belief — a swapped base folds
            // as its own content, and the mismatch with the published
            // entry surfaces as P1-ATTEST-SEAL. A copy-skipped
            // replacement (B5-legal duplicate) folds as a NO-OP. Fresh
            // and overlay rounds fold their verdict epoch.
            // Legality (round-7 F2 pin): replacement counting happens
            // HERE — committed copies within COMPLETE rounds only.
            // Cross-attempt at-least-once re-copies (the incomplete
            // first try) are legal B5 idempotence and stay uncounted.
            if (info.hasCopy) {
                if (scope in copies) {
                    copies[scope] = copies[scope] + 1;
                } else {
                    copies[scope] = 1;
                }
                assert copies[scope] <= 1, "P1-LEGALITY: second complete-round replacement copy for one scope in one sync";
            }
            if (ghost.verdict == V_REPLAY) {
                if (info.hasCopy) {
                    foldEpoch[scope] = info.vBase;
                }
            } else {
                foldEpoch[scope] = ghost.consultEpoch;
            }
        }
    }
}

// Partition scope content as id -> epoch (ghost content tag).
fun contentOf(part: tPartition, scope: int): map[int, int] {
    var out: map[int, int];
    var ids: seq[int];
    var i: int;
    if (!(scope in part)) { return out; }
    ids = keys(part[scope]);
    i = 0;
    while (i < sizeof(ids)) {
        out[ids[i]] = part[scope][ids[i]].epoch;
        i = i + 1;
    }
    return out;
}

spec P2 observes eAnnScenarioInit, eAnnSyncStart, eAnnConsult, eAnnSeal {
    var bound: int;
    var consulted: map[int, bool];

    start state Monitoring {
        on eAnnScenarioInit do (p: (maxStaleness: int)) {
            bound = p.maxStaleness;
        }
        on eAnnSyncStart do (p: (syncN: int)) {
            consulted = default(map[int, bool]);
        }
        on eAnnConsult do (p: (syncN: int, scope: int, hit: bool, v: int, validated: bool, epoch: int, freshFetch: bool, diffVerdict: bool, attempt: int)) {
            // Consulted-against-upstream (MODEL_SPEC 7 pin): validation
            // match, fresh fetch, or CHANGED-WITH-DIFF verdict; a lookup
            // hit alone does not qualify.
            if (p.validated || p.freshFetch || p.diffVerdict) {
                consulted[p.scope] = true;
            }
        }
        on eAnnSeal do (p: (syncN: int, partition: tPartition, manifest: map[int, int], blocked: bool, config: int)) {
            var scopes: seq[int];
            var ids: seq[int];
            var i: int;
            var j: int;
            var s: int;
            scopes = keys(p.partition);
            i = 0;
            while (i < sizeof(scopes)) {
                s = scopes[i];
                if (sizeof(p.partition[s]) > 0) {
                    assert s in consulted, "P2-CONSULT: sealed scope not consulted against upstream this sync";
                    ids = keys(p.partition[s]);
                    j = 0;
                    while (j < sizeof(ids)) {
                        assert p.partition[s][ids[j]].hops <= bound, "P2-STALENESS: row replay-travel exceeds the scenario bound";
                        j = j + 1;
                    }
                }
                i = i + 1;
            }
        }
    }
}

spec P3prime observes eAnnSyncStart, eAnnConsult, eAnnUpsert, eAnnTombstones, eAnnSeal {
    var lastEpoch: map[int, int];   // scope -> epoch of last consulted-against-upstream verdict
    var attemptsSeen: map[int, map[int, bool]];  // roundId -> attempts (torn tracking)
    var tornScopes: map[int, bool];

    start state Monitoring {
        on eAnnSyncStart do (p: (syncN: int)) {
            lastEpoch = default(map[int, int]);
            attemptsSeen = default(map[int, map[int, bool]]);
            tornScopes = default(map[int, bool]);
        }
        on eAnnConsult do (p: (syncN: int, scope: int, hit: bool, v: int, validated: bool, epoch: int, freshFetch: bool, diffVerdict: bool, attempt: int)) {
            if (p.validated || p.freshFetch || p.diffVerdict) {
                lastEpoch[p.scope] = p.epoch;
            }
        }
        on eAnnUpsert do (p: (syncN: int, scope: int, rows: seq[tRow], ghost: tRoundGhost)) {
            trackTorn(p.scope, p.ghost);
        }
        on eAnnTombstones do (p: (syncN: int, scope: int, removes: seq[int], ghost: tRoundGhost)) {
            trackTorn(p.scope, p.ghost);
        }
        on eAnnSeal do (p: (syncN: int, partition: tPartition, manifest: map[int, int], blocked: bool, config: int)) {
            var scopes: seq[int];
            var i: int;
            var s: int;
            scopes = keys(p.partition);
            i = 0;
            while (i < sizeof(scopes)) {
                s = scopes[i];
                // Doubly scoped (MODEL_SPEC 7): configs schedule no
                // mid-attempt mutation (env only mutates between
                // attempts/syncs), and torn scopes are excluded here.
                if (s in lastEpoch && !(s in tornScopes) && sizeof(p.partition[s]) > 0) {
                    assert contentOf(p.partition, s) == rowsAt(lastEpoch[s]), "P3'-COHERENCE: sealed content epoch differs from last consulted verdict epoch";
                }
                i = i + 1;
            }
        }
    }

    fun trackTorn(scope: int, ghost: tRoundGhost) {
        var att: map[int, bool];
        if (ghost.roundId in attemptsSeen) {
            att = attemptsSeen[ghost.roundId];
        }
        att[ghost.attempt] = true;
        attemptsSeen[ghost.roundId] = att;
        if (sizeof(att) > 1) {
            tornScopes[scope] = true;
        }
    }
}

// P6-A (MODEL_SPEC 7, case 2): session-embed agreement, WITHIN-SYNC
// form. At seal, every row whose stamp was derived THIS SYNC (hops 0 —
// copied rows carry last sync's stamps and belong to P6-R) and embeds
// a real value (stamp >= 1; the miss marker 0 is P6-R's domain) must
// embed the FINAL session value — comparison is by VALUE, so
// same-value re-derivation is green. Rows with stamp -1 carry no
// session data. The session KV is sync-scoped: tracking resets at
// sync start. P6-A is vacuously green on 7a/7b/7c by this scoping.
spec P6A observes eAnnSyncStart, eAnnSessionSet, eAnnSeal {
    var sess: map[int, int];

    start state Monitoring {
        on eAnnSyncStart do (p: (syncN: int)) {
            sess = default(map[int, int]);
        }
        on eAnnSessionSet do (p: (syncN: int, key: int, val: int)) {
            sess[p.key] = p.val;
        }
        on eAnnSeal do (p: (syncN: int, partition: tPartition, manifest: map[int, int], blocked: bool, config: int)) {
            var scopes: seq[int];
            var ids: seq[int];
            var i: int;
            var j: int;
            var s: int;
            var st: int;
            scopes = keys(p.partition);
            i = 0;
            while (i < sizeof(scopes)) {
                s = scopes[i];
                ids = keys(p.partition[s]);
                j = 0;
                while (j < sizeof(ids)) {
                    st = p.partition[s][ids[j]].stamp;
                    if (st >= 1 && p.partition[s][ids[j]].hops == 0) {
                        assert 0 in sess && sess[0] == st, "P6-A: sealed row embeds a session stamp differing from the final session value";
                    }
                    j = j + 1;
                }
                i = i + 1;
            }
        }
    }
}

// P6-C (session-checkpoint consistency; the CO-6b-009 root cause made
// executable). THE CONSTRAINT: observable session state after a crash
// must equal session state at the restored checkpoint — in BOTH
// directions. Direction 1 (ZOMBIE): a value a dead attempt wrote
// AFTER its last checkpoint must not be observable by the re-run —
// the cursor rolled back, the work that produced the value will run
// again, and the re-run window would otherwise consume its own
// future. Direction 2 (AMNESIA): a value observable at the restored
// checkpoint must REMAIN observable — the work that produced it will
// NOT re-run, so deleting it is unrecoverable data loss. Provenance
// is tracked monitor-side from the announce stream: writes are
// uncommitted until an eAnnCheckpoint folds them; a crash turns the
// still-uncommitted residue into zombies (unless value-identical to
// the committed state, where survival is unobservable); a live
// rewrite reclaims the key. Variant 0 (shipped, durable-at-op-commit)
// violates direction 1; the rejected wholesale resume-clear
// (variant 1) violates direction 2; checkpoint-consistent sessions
// (variant 2) satisfy both.
spec P6C observes eAnnSyncStart, eAnnSessionSet, eAnnSessionGet, eAnnCheckpoint, eAnnCrash {
    var committed: map[int, int];    // key -> value at the last checkpoint
    var uncommitted: map[int, int];  // writes since the last checkpoint
    var zombies: map[int, int];      // dead attempts' beyond-checkpoint values

    start state Monitoring {
        on eAnnSyncStart do (p: (syncN: int)) {
            committed = default(map[int, int]);
            uncommitted = default(map[int, int]);
            zombies = default(map[int, int]);
        }
        on eAnnSessionSet do (p: (syncN: int, key: int, val: int)) {
            // A live write takes over the key: whatever is durable now
            // is attributable to the current attempt.
            uncommitted[p.key] = p.val;
            if (p.key in zombies) { zombies -= p.key; }
        }
        on eAnnCheckpoint do (p: (syncN: int)) {
            var ks: seq[int];
            var i: int;
            ks = keys(uncommitted);
            i = 0;
            while (i < sizeof(ks)) {
                committed[ks[i]] = uncommitted[ks[i]];
                i = i + 1;
            }
            uncommitted = default(map[int, int]);
        }
        on eAnnCrash do (p: (syncN: int)) {
            var ks: seq[int];
            var i: int;
            var k: int;
            ks = keys(uncommitted);
            i = 0;
            while (i < sizeof(ks)) {
                k = ks[i];
                if (!(k in committed && committed[k] == uncommitted[k])) {
                    zombies[k] = uncommitted[k];
                }
                i = i + 1;
            }
            uncommitted = default(map[int, int]);
        }
        on eAnnSessionGet do (p: (syncN: int, key: int, found: bool, val: int)) {
            if (p.key in zombies) {
                assert !(p.found && p.val == zombies[p.key]), "P6-C-ZOMBIE: session read observed a dead attempt's beyond-checkpoint write (the cursor rolled back; the session state did not)";
            }
            if (p.key in committed) {
                assert p.found, "P6-C-AMNESIA: session read missed a checkpoint-committed value (session data silently deleted; the work that produced it will not re-run)";
            }
        }
    }
}

// P6-R (MODEL_SPEC 7, case 7): replay-session coherence. Per (sync,
// key) the model carries a COUNTERFACTUAL session value — the producer
// policy's phase-final value under an all-fresh execution at this
// sync's epoch (announced by MEnv as a computed ghost). At seal, every
// committed row whose scripted derivation includes a session input
// (stamp >= 0: real values AND the miss marker 0) must match the
// counterfactual. Covers both duals: the fresh reader deriving from a
// read-miss whose producer was elided (7a: counterfactual v1, embedded
// 0) and the replayed row carrying a stamp the producer re-derived
// differently this sync (7b: counterfactual v2, embedded v1 travels
// with the copy). Defined only for configs that mutate upstream
// BETWEEN syncs (single epoch per (sync, scope)).
spec P6R observes eAnnSyncStart, eAnnCounterfactual, eAnnSeal {
    var cf: map[int, int];

    start state Monitoring {
        on eAnnSyncStart do (p: (syncN: int)) {
            cf = default(map[int, int]);
        }
        on eAnnCounterfactual do (p: (syncN: int, key: int, val: int)) {
            cf[p.key] = p.val;
        }
        on eAnnSeal do (p: (syncN: int, partition: tPartition, manifest: map[int, int], blocked: bool, config: int)) {
            var scopes: seq[int];
            var ids: seq[int];
            var i: int;
            var j: int;
            var s: int;
            var st: int;
            scopes = keys(p.partition);
            i = 0;
            while (i < sizeof(scopes)) {
                s = scopes[i];
                ids = keys(p.partition[s]);
                j = 0;
                while (j < sizeof(ids)) {
                    st = p.partition[s][ids[j]].stamp;
                    if (st >= 0 && 0 in cf) {
                        assert st == cf[0], "P6-R: sealed session-derived row diverges from the all-fresh counterfactual value";
                    }
                    j = j + 1;
                }
                i = i + 1;
            }
        }
    }
}

// C1Probe (MODEL_SPEC 9.5 C1, CO-6b-002 conformance question): a
// REACHABILITY probe, not a safety property — its red is the witness
// that a replay copy can commit in an attempt that never freshly
// consulted the scope (the hit check passed on the checkpoint-RESTORED
// hit map after a mid-chain stop-resume). Asserted only in the cell-53
// probe test; the counterexample trace is the C1 answer ("reachable
// via the stop path"), to be confirmed against the real implementation
// through the chaos bridge (deliverable 6), not treated as a model bug.
spec C1Probe observes eAnnSyncStart, eAnnConsult, eAnnReplay {
    var consultedBy: map[int, map[int, bool]];   // scope -> attempt gens

    start state Monitoring {
        on eAnnSyncStart do (p: (syncN: int)) {
            consultedBy = default(map[int, map[int, bool]]);
        }
        on eAnnConsult do (p: (syncN: int, scope: int, hit: bool, v: int, validated: bool, epoch: int, freshFetch: bool, diffVerdict: bool, attempt: int)) {
            var att: map[int, bool];
            if (p.scope in consultedBy) { att = consultedBy[p.scope]; }
            att[p.attempt] = true;
            consultedBy[p.scope] = att;
        }
        on eAnnReplay do (p: (syncN: int, scope: int, vBase: int, cBase: int, ghost: tRoundGhost)) {
            assert p.scope in consultedBy && p.ghost.attempt in consultedBy[p.scope], "C1-PROBE: replay copy committed in an attempt with no fresh consult of the scope (restored-hit mid-chain resume) — CO-6b-002 reachable via the stop path";
        }
    }
}

// P4Stuck (MODEL_SPEC 7 P4, safety form): livelock DETECTION checkable
// in bounded runs — two CONSECUTIVE resume attempts that fail from
// identical restored checkpoint state (scheduler-progress fields:
// stack, hits, replayed) with the same verdict (reason) at the same
// step (scope, cursor) constitute the deterministic re-failure finding
// (CO-6b-004's stuck-resume contract). The offending cursor is IN the
// failure checkpoint, so the recurrence is by construction, not luck.
spec P4Stuck observes eAnnSyncStart, eAnnAttemptFailed {
    var have: bool;
    var lastGen: int;
    var lastStack: seq[tAction];
    var lastHits: map[int, int];
    var lastReplayed: map[int, bool];
    var lastScope: int;
    var lastReason: int;
    var lastCursor: int;

    start state Monitoring {
        on eAnnSyncStart do (p: (syncN: int)) {
            have = false;
        }
        on eAnnAttemptFailed do (p: (syncN: int, gen: int, stack: seq[tAction], hits: map[int, int], replayed: map[int, bool], scope: int, reason: int, cursor: int)) {
            if (have && p.gen == lastGen + 1) {
                assert !(p.stack == lastStack && p.hits == lastHits && p.replayed == lastReplayed && p.scope == lastScope && p.reason == lastReason && p.cursor == lastCursor), "P4-STUCK: consecutive attempts failed from identical restored checkpoint state at the same step (deterministic re-failure, CO-6b-004 stuck-resume)";
            }
            have = true;
            lastGen = p.gen;
            lastStack = p.stack;
            lastHits = p.hits;
            lastReplayed = p.replayed;
            lastScope = p.scope;
            lastReason = p.reason;
            lastCursor = p.cursor;
        }
    }
}

// P4Live (MODEL_SPEC 7 P4, liveness form): hot while a started sync is
// unsealed. An ABANDONED sync (the ladder) stays hot until a LATER
// sync's seal cools it — "after budgets exhaust the chain eventually
// seals" in the bounded scenario shape. Meaningful with abandonLadder
// on; the ladder cell ends cold because the post-abandon sync runs
// cold against the drifted config and seals fresh.
spec P4Live observes eAnnSyncStart, eAnnSeal {
    start state Sealed {
        on eAnnSyncStart do (p: (syncN: int)) { goto Unsealed; }
        ignore eAnnSeal;
    }
    hot state Unsealed {
        on eAnnSeal do (p: (syncN: int, partition: tPartition, manifest: map[int, int], blocked: bool, config: int)) {
            goto Sealed;
        }
        ignore eAnnSyncStart;
    }
}

// SealExpect (MODEL_SPEC 9.5): the scripted seal-state expectation —
// scenario 5's only executable oracle for the produce-blocked marking
// and the 5b silent scope dropout (both invisible to P1/P2: no rows,
// no illegal round). MEnv announces the expectation when the
// stop-stranding premise lands; this sync's seal must satisfy it.
// In the 5b crash cell the wantBlocked check failing IS the
// crash-window finding: the trigger fired volatile in attempt 2 and
// died with it before any checkpoint carried it.
spec SealExpect observes eAnnSyncStart, eAnnExpectSeal, eAnnSeal {
    var active: bool;
    var expSync: int;
    var expScope: int;
    var wantBlocked: bool;
    var wantScopeEmpty: bool;

    start state Monitoring {
        on eAnnSyncStart do (p: (syncN: int)) {
            // Expectations are per-sync; a new sync clears any (already
            // checked) prior expectation.
            if (active && p.syncN != expSync) { active = false; }
        }
        on eAnnExpectSeal do (p: (syncN: int, scope: int, wantBlocked: bool, wantScopeEmpty: bool)) {
            active = true;
            expSync = p.syncN;
            expScope = p.scope;
            wantBlocked = p.wantBlocked;
            wantScopeEmpty = p.wantScopeEmpty;
        }
        on eAnnSeal do (p: (syncN: int, partition: tPartition, manifest: map[int, int], blocked: bool, config: int)) {
            if (!active || p.syncN != expSync) { return; }
            if (wantBlocked) {
                assert p.blocked, "SEAL-EXPECT: artifact sealed unblocked though a produce trigger fired this sync (5b: the crash-window loss)";
            }
            if (wantScopeEmpty) {
                assert !(expScope in p.partition) || sizeof(p.partition[expScope]) == 0, "SEAL-EXPECT: rows sealed for a scope whose only work was a silently-ignored carrier (5b dropout pin)";
            }
            active = false;
        }
    }
}
