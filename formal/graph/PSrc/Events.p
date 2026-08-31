/* Events. Store ops are request/ack; every attempt-owned request
   carries agen (attempt id) and the crash protocol drops ops from
   dead attempts (walker parity: dropped ops are never acked; the
   eStoreDead response lets receivers park instead of blocking). */

// ---- store ops (worker/scheduler -> MGStore) ----
event eGReset: (client: machine, syncN: int);
event eGLookupReq: (client: machine, agen: int, key: int);
event eGLookupResp: (hit: bool, v: int);
event eGMarkerReadReq: (client: machine, agen: int, key: int);
event eGMarkerReadResp: (present: bool, marker: tMarker);
// delMarker: the REPLACES clear deletes the key's marker (SPEC 3
// clear-placement pin; markerCleanupOff removes exactly this).
event eGClearScope: (client: machine, agen: int, key: int, delMarker: bool, ghost: tGGhost);
event eGUpsertPage: (client: machine, agen: int, key: int, rows: seq[tGRow], ghost: tGGhost);
event eGPublishEntry: (client: machine, agen: int, key: int, v: int, stamp: map[int, int], hash: int, ghost: tGGhost);
// V-ATOMIC replay unit (settled hand-off, MODEL_SPEC 9.6): clear +
// copy + marker + publish as ONE atomic store op. Marker announce
// FIRST among constituents (P-MARK convention: every legal partition
// mutation rides an op that first (re)binds the marker).
event eGReplayUnit: (client: machine, agen: int, key: int, v: int, marker: tMarker, stamp: map[int, int], hash: int, ghost: tGGhost);
// V-OVERLAY-UNIT: clear + copy(base) + overlay pages + marker +
// publish(V_to) as ONE atomic store op.
event eGOverlayUnit: (client: machine, agen: int, key: int, v: int, upserts: seq[tGRow], removes: seq[int], marker: tMarker, stamp: map[int, int], hash: int, composeDead: bool, ghost: tGGhost);
// Unit commit response: the key's final rows (copied rows can name
// children, so the carrier report needs them for demand derivation).
event eGUnitResp: (rows: seq[tGRow]);
// Premise-validated adoption (SPEC 4a): one atomic op. Store-side
// preconditions (R2-N1 + R3-M2): fromGen dead per the last durable
// generation table AND the key not poisoned; allowLiveFrom is the
// suppressionOff deviation (declared, R2-N1). Response carries the
// adopted rows for the re-announce (demand re-derivation).
event eGAdoptReq: (client: machine, agen: int, key: int, node: int, fromGen: int, toGen: int, roundId: int, allowLiveFrom: bool, ghost: tGGhost);
event eGAdoptResp: (ok: bool, rows: seq[tGRow]);
// Session ops (SPEC 3 MSessionStore; body ops per the R3-F1 pin).
event eGSessionPub: (client: machine, agen: int, skey: int, val: int, writer: int, wgen: int, ghost: tGGhost);
event eGSessionGetReq: (client: machine, agen: int, reader: int, rgen: int, skey: int);
event eGSessionGetResp: (found: bool, val: int, writer: int, wgen: int);
event eGCheckpointReq: (client: machine, agen: int, ck: tGCkpt, forced: bool);
event eGReadCkptReq: (client: machine);
event eGReadCkptResp: (ck: tGCkpt, has: bool);
event eGReadRowsReq: (client: machine, agen: int, key: int);
event eGReadRowsResp: (rows: seq[tGRow], present: bool);
event eGReadStampsReq: (client: machine, agen: int);
event eGReadStampsResp: (stamps: map[int, map[int, int]], owners: map[int, int]);
// Seal: keep = the final demand closure keys (sweep drops the rest;
// doSweep false = sweepOff). genTable rides the seal for P6-S.
event eGSealReq: (client: machine, agen: int, keep: seq[int], doSweep: bool, genTable: map[int, int]);
event eReadSealedReq: (client: machine);
event eReadSealedResp: (sealed: bool);
event eStoreAck;
event eStoreDead;
event eCrashArm: (client: machine, agen: int);
// Synchronous arm handshake: the store confirms the arm BEFORE the
// env creates the attempt, so the arm can never lose the queue race
// to the entire attempt (an unresolved arm deadlocks the env — found
// by feedback-PCT on the G1d cell; latent in the walker too).
event eCrashArmed;
event eCrashAck;

// ---- upstream (synchronous request/response; walker parity) ----
event eValidateReq: (client: machine, scope: int, v: int);
event eValidateResp: (ok: bool, epoch: int);
event eFetchReq: (client: machine, scope: int, page: int);
event eFetchResp: (rows: seq[tGRow], epoch: int, morePages: bool);
event eDiffReq: (client: machine, scope: int, fromEpoch: int, page: int);
event eDiffResp: (upserts: seq[tGRow], removes: seq[int], epoch: int, morePages: bool);
event eMutate: (client: machine, scope: int);
event eMutateAck;

// ---- scheduler <-> worker ----
event eGDispatch: (pend: tPendingNode, execId: int, attempt: int, stop: bool);
// Scripted graceful stop (G3, walker case 3): the flagged execution
// stops AFTER its consult announce, before any round commit; the
// scheduler checkpoints (the stopped node stays pending at its
// cursor) and ends the attempt unsealed with the store intact.
event eGStopReq: (node: int, gen: int);
// Between-attempt previous-artifact swap (G3): the env rebinds the
// PREV manifest + partition for one key to sibling content, so the
// resumed consult validates against the actually-current base.
event eGSwapPrev: (client: machine, key: int, epoch: int);
// Loud attempt failure (G7, walker P4 analog): the scripted node
// fails at execution start; the fingerprint is GENERATION-BLIND
// (round-1 F11: a fingerprint that hashes the generation never
// matches across bumped resumes and the stuck detector goes blind).
event eGNodeFail: (node: int, gen: int, fingerprint: int);
event eGNodeDone: (report: tGReport);
// Read-time session-read registration (R2-M1 read-through): the
// scheduler's reader index must see a read WHILE the reading
// execution is in flight, or the retraction/quiesce race (R2-F1)
// is structurally unreachable and an in-flight reader whose read
// races a re-publish is never retracted.
event eGReadNote: (reader: int, rgen: int, skey: int, val: int, writer: int, wgen: int);
event eGAbortWorker;

// ---- scheduler self-events ----
event eGLoopTop;

// ---- env control ----
event eGAttemptEnded: (sealed: bool, failed: bool);

// ---- announce vocabulary (SPEC 7) ----
event eAnnScenarioInit: (maxStaleness: int);
event eAnnSyncStart: (syncN: int);
event eAnnConsult: (syncN: int, key: int, hit: bool, v: int, validated: bool, epoch: int, freshFetch: bool, diffVerdict: bool, attempt: int, node: int, gen: int);
event eAnnClear: (syncN: int, key: int, ghost: tGGhost);
// Replay copy carries the copied rows so P-MARK and the fold can
// ground content without a store readback.
event eAnnReplayCopy: (syncN: int, key: int, vBase: int, rows: seq[tGRow], ghost: tGGhost);
event eAnnUpsert: (syncN: int, key: int, rows: seq[tGRow], ghost: tGGhost);
event eAnnTombstones: (syncN: int, key: int, removes: seq[int], ghost: tGGhost);
event eAnnPublish: (syncN: int, key: int, v: int, ghost: tGGhost);
event eAnnMarkerPut: (syncN: int, key: int, node: int, gen: int, roundId: int, pubBearing: bool, contentEpoch: int, ghost: tGGhost);
event eAnnMarkerDel: (syncN: int, key: int);
// Adoption re-announce: attributes to the ADOPTING execution
// (P-GEN rule R2-N3); adoptedRoundId lets P1 transfer the marked
// round's contribution and P-MARK rebind the marker.
event eAnnAdopt: (syncN: int, key: int, node: int, fromGen: int, toGen: int, adoptedRoundId: int, rows: seq[tGRow], ghost: tGGhost);
event eAnnPoison: (syncN: int, key: int);
event eAnnSessionSet: (syncN: int, skey: int, val: int, writer: int, wgen: int, ghost: tGGhost);
event eAnnSessionRead: (syncN: int, reader: int, rgen: int, skey: int, found: bool, val: int, writer: int, wgen: int);
// Derived announces (G-RULE-1 carrier pin): generation death, forced
// re-admissions, dead reads, pass iterations, budget exhaustion.
// reason: 1 = resume bump, 2 = retraction, 3 = observation pass.
event eAnnGenBump: (syncN: int, node: int, newGen: int, reason: int);
event eAnnReadmit: (syncN: int, node: int, hash: int, gen: int, reason: int);
event eAnnDeadRead: (syncN: int, reader: int, skey: int);
// Resume-time pending purge (E, R2-F5 ∀-predicate) — the G5e
// existence probe's observation point.
event eAnnPurge: (syncN: int, node: int, hash: int);
// A node dispatched while EVERY admitted-by edge names a dead
// generation (dead demand) — unreachable honestly (E purges at
// resume, S refuses at dispatch); the purgeOff kill's alarm.
event eAnnDeadDispatch: (syncN: int, node: int, hash: int);
// Execution-count vocabulary (G6 bake-off): every dispatch announces;
// the env declares the cell's bound once at scenario init.
event eAnnExec: (syncN: int, attempt: int, node: int, gen: int);
event eAnnExecBound: (bound: int);
// Seal-world target (GS-CO-005(d) G5d meta-analysis): the env
// declares the probed world once at scenario init; GSEALWORLD reds
// iff the target sync seals exactly that world.
event eAnnSealWorld: (syncN: int, exp: map[int, int]);
// Attempt-failure vocabulary (G7): the scheduler announces the loud
// failure's generation-blind fingerprint; the env announces abandons.
event eAnnAttemptFail: (syncN: int, attempt: int, node: int, fingerprint: int);
event eAnnAbandon: (syncN: int);
event eAnnPassIter: (syncN: int, iter: int);
event eAnnBudgetExhausted: (syncN: int);
event eAnnCheckpoint: (syncN: int, forced: bool);
event eAnnCrash: (syncN: int);
// Per-announce demand note (G-RULE-1 TIMING PIN): a paginated record
// round's committed page carries its child-naming rows to the
// scheduler AT the page announce, so demand derivation is atomic
// with that announce — the mid-round C-pending & parent-pending
// checkpoint window (G5e) exists only under this timing. Units are
// single atomic commits; their completion carrier IS the announce.
event eGDemandNote: (node: int, gen: int, key: int, rows: seq[tGRow]);

// Seal announce: partition + manifest + stamps + the final generation
// table (P6-S's dead-set ground).
event eAnnGSeal: (syncN: int, partition: tGPart, manifest: map[int, int], stamps: map[int, map[int, int]], genTable: map[int, int]);
// Scripted seal expectation (SealExpect = closure + content oracle):
// exp maps key -> expected content epoch; excluded keys (poisoned)
// are exempt both directions.
event eAnnExpectSeal: (syncN: int, exp: map[int, int], excluded: map[int, bool]);
