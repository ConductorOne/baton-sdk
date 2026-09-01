/* Events. Store ops are request/ack: a page's store ops commit (and
   announce) before the worker reports its action transition, matching
   the shipped synchronous store API (MODEL_SPEC 3: announce on commit;
   a page COMMITS when the last of its prescribed store ops commits).
   Every attempt-owned request carries gen; the crash protocol drops
   ops from dead gens (MODEL_SPEC 5: ops behind eCrash in MStore's
   queue are DROPPED — a dropped op is never acked, which also gives
   quiesce: dead senders block forever awaiting acks). */

// ---- store ops (worker/scheduler -> MStore; eStoreAck on commit) ----
event eLookupReq: (client: machine, gen: int, scope: int);
event eLookupResp: (hit: bool, v: int);
event eBaseReadReq: (client: machine, gen: int, scope: int);   // base-binding check read
event eBaseReadResp: (v: int, present: bool);
event eClearScope: (client: machine, gen: int, scope: int, ghost: tRoundGhost);
// MS-CO-003 record-round grounding: ONE atomic check-and-clear (the real
// groundRecordScope runs its manifest lookup and ClearSourceCacheScope
// under the scope lock inside the destination batch). Clears the scope's
// partition ONLY when this sync's manifest has no entry for it; a no-op
// (published entry, or empty partition) still acks. boundV >= 0 is the
// validator-bound candidate rule: ALSO clear when the published entry's
// validator differs from boundV (the record round's incoming validator);
// boundV = -1 is the shipped conditional.
event eGroundScope: (client: machine, gen: int, scope: int, boundV: int, ghost: tRoundGhost);
event eCopyScope: (client: machine, gen: int, scope: int, ghost: tRoundGhost);
event eUpsertPage: (client: machine, gen: int, scope: int, rows: seq[tRow], ghost: tRoundGhost);
event ePublishEntry: (client: machine, gen: int, scope: int, v: int, ghost: tRoundGhost);
// Scenario-6 marker machinery: the marker is a PER-SCOPE STORE ROW in
// the current artifact — authoritative consult provenance, durable at
// op commit, reset at sync rotation (a new sync always re-consults).
event eMarkerPut: (client: machine, gen: int, scope: int);
event eMarkerReadReq: (client: machine, gen: int, scope: int);
event eMarkerReadResp: (marked: bool);
// V-ATOMIC unit: {clear, copy, marker, publish} committed as ONE atomic
// store op (single queue position — a crash cannot split it). Announces
// its constituent ops so the monitor vocabulary is unchanged; the round
// completes at unit commit (round-5 F1 pin).
event eReplayUnit: (client: machine, gen: int, scope: int, v: int, ghost: tRoundGhost);
// V-OVERLAY-UNIT: {clear, copy(base), overlay upserts/tombstones in
// prescribed page order, marker, publish(V_to)} as ONE atomic store op.
event eOverlayUnit: (client: machine, gen: int, scope: int, v: int, upserts: seq[tRow], removes: seq[int], ghost: tRoundGhost);
// Per-page tombstone commit (shipped path, used by the naive misdraw).
event eTombstonePage: (client: machine, gen: int, scope: int, removes: seq[int], ghost: tRoundGhost);
event eCheckpointReq: (client: machine, gen: int, ckpt: tCheckpoint);
// Seal carries the sealing attempt's produce-blocked flag and compat
// config (blocked last made durable by the forced pre-seal checkpoint;
// the seal is its final resting place on the artifact; config feeds
// P1's clause (c) at seal).
event eSealReq: (client: machine, gen: int, blocked: bool, config: int);
// Case-5 produce state: the artifact's compat record (written by each
// handling attempt at install) and the produce-state read at attempt
// start (install-time gates G4/G7 + block triggers 1/2 read exactly
// this). hasPrev = a previous sealed artifact exists (usable-prev).
event eCompatPut: (client: machine, gen: int, k: int);
event eProduceReadReq: (client: machine, gen: int);
event eProduceReadResp: (prevCompat: int, prevBlocked: bool, curCompat: int, hasCur: bool, hasPrev: bool);
event eStoreAck;
// Response to any op from a dead (crashed) gen: the op was DROPPED, not
// committed. Receivers park in a dead state instead of blocking, so the
// pinned drop semantics don't register as P deadlocks.
event eStoreDead;
// Crash injection (MODEL_SPEC 3 MCrashInjector / 5 crash protocol):
// arming lets MStore fire the crash nondeterministically at any op
// boundary of the armed gen — every queue position is explored with
// per-op granularity. Resolution is guaranteed by the seal op (crash
// fires before or immediately after it), so the env's ack wait always
// terminates.
event eCrashArm: (client: machine, gen: int);
event eCrashAck;
event eStoreReset: (client: machine, syncN: int, sessVariant: int, extOverDelete: bool);   // begin-of-sync rotation (env, not gen-gated)
event eReadCheckpointReq: (client: machine);
event eReadCheckpointResp: (ckpt: tCheckpoint, hasCkpt: bool);
// Session store (MODEL_SPEC 3/9 cases 2 and 7): sync-scoped KV, durable
// at op commit, survives attempts and crashes within the sync, reset at
// sync rotation. Gen-gated like every attempt-owned op. scope = the
// acting kind's scope (phase attribution for produce-side taint);
// taint = the acting config's toggle verdict for THIS op (computed by
// the worker so the store stays config-free).
event eSessionSet: (client: machine, gen: int, scope: int, key: int, val: int, taint: bool);
event eSessionGetReq: (client: machine, gen: int, scope: int, key: int, taint: bool);
event eSessionGetResp: (found: bool, val: int);
// Case-3 premise: env swaps the previous sealed artifact for a
// fabricated-but-legal sibling B (equal compat record, validator
// vB, content = truthful rows at vB's epoch). Env-level, not gen-gated.
event eSwapBase: (client: machine, scope: int, vB: int, rowsB: seq[tRow]);
// Scenario-8 external-principal ops (MODEL_SPEC abstraction of
// SyncExternalResources). The ext keyspace is separate from scope
// partitions (external principals carry the BatonID annotation and
// live beside connector rows; the model keeps them in their own map).
// eExtReconReq = deleteStaleExternalPrincipals: with supported TRUE
// it deletes every ext row absent from live (the current listed
// answer) as ONE atomic op; with supported FALSE it is the
// warn-and-continue degrade — a no-op that still announces the round
// (the LIST happened; reconciliation did not).
event eExtReconReq: (client: machine, gen: int, live: seq[int], supported: bool);
// eExtCopy = the current answer's principal writes (page-atomic per
// the MODEL_SPEC 1 store abstraction; partial-write debris reduces to
// the same stale-survivor class the crash-between-ops windows cover).
event eExtCopy: (client: machine, gen: int, ids: seq[int]);

// ---- upstream (synchronous request/response) ----
event eValidateReq: (client: machine, scope: int, v: int);
event eValidateResp: (ok: bool, epoch: int);
event eFetchReq: (client: machine, scope: int, page: int);
event eFetchResp: (rows: seq[tRow], epoch: int, morePages: bool);
// Overlay diff (changed-with-diff verdict): pages of upserts/removes
// from a base epoch to the current epoch.
event eDiffReq: (client: machine, scope: int, fromEpoch: int, page: int);
event eDiffResp: (upserts: seq[tRow], removes: seq[int], epoch: int, morePages: bool);
event eMutate: (client: machine, scope: int);
event eMutateAck;

event eReadSealedReq: (client: machine);
event eReadSealedResp: (sealed: bool);

// ---- scheduler <-> worker ----
// hits/replayed here are dispatch-time snapshots kept for reference;
// the load-bearing reads are LIVE: the replayed set via lock grant or
// eReplayedCheckReq (case-4 TOCTOU), the hit map via eHitReadReq
// (case-3A rebind).
event eDispatch: (action: tAction, hits: map[int, int], replayed: map[int, bool]);
event eActionTransition: (aid: int, nextCursor: int, done: bool, spawn: tAction, hasSpawn: bool, hitScope: int, hitV: int, hasHit: bool, markReplayed: bool, replayedScope: int);
event eContinuePage;
event eAbortWorker;
event eWorkerAborted: (aid: int, cursor: int);

// ---- scope locks + replayed-set access (CO-6b-007 / MODEL_SPEC 4) ----
// With scopeLocks ON the oncePerScope check-and-mark is lock-mediated:
// the grant carries the replayed status and the release commits the
// mark atomically before the next grant. With locks OFF the worker
// reads the status lock-free and the mark lands later, at the action
// transition — the case-4 check-then-mark TOCTOU window.
event eScopeLockAcquire: (worker: machine, scope: int);
event eScopeLockGrant: (replayed: bool);
event eScopeLockRelease: (scope: int, mark: bool);
event eReplayedCheckReq: (worker: machine, scope: int);
event eReplayedCheckResp: (replayed: bool);
// Live hit-map read (MODEL_SPEC 3: hits live in ONE sync-level map,
// recorded at lookup time, last-write-wins). The carrier's hit check
// and binding compare read this map at DRAIN time — the case-3A
// rebind hole lives in exactly this read-after-overwrite.
event eHitReadReq: (worker: machine, scope: int);
event eHitReadResp: (has: bool, v: int);

// ---- scheduler self-events (commit/reply/dispatch split so the stop
// can interleave between a transition's state commit and the worker's
// continuation — the stop-stranding premise generator, MODEL_SPEC 9) ----
event eReplyWorker: (aid: int, worker: machine);
event eDispatchPending;
event eLoopTop;

// ---- attempt-level loud failure (MODEL_SPEC 4 failure semantics;
// P4 cells only — cfg.loudColdFailsAttempt) ----
// Worker -> scheduler: a cold verdict failed the chain loudly at the
// offending cursor; the scheduler restores the action at that cursor
// (so the failure recurs deterministically from the checkpoint),
// quiesces, force-checkpoints, and reports the attempt failed.
event eChainFailed: (aid: int, cursor: int, scope: int, reason: int);

// ---- env control ----
event eStopAttempt;
event eAttemptEnded: (stopped: bool, sealed: bool, failed: bool);

// ---- announce vocabulary (MODEL_SPEC 7) ----
event eAnnScenarioInit: (maxStaleness: int);
event eAnnSyncStart: (syncN: int);
// attempt = the consulting attempt's gen: the C1 probe's oracle
// compares it to the replaying round's attempt ghost (a replay whose
// scope was consulted only in an EARLIER attempt ran on a restored
// hit — the CO-6b-002 conformance question).
event eAnnConsult: (syncN: int, scope: int, hit: bool, v: int, validated: bool, epoch: int, freshFetch: bool, diffVerdict: bool, attempt: int);
// cBase = the copied base content's compat config ghost (first row's
// tag; -1 when the base is empty or configs are unmodeled). P1's
// config clause (c) compares it to the round's attempt config.
event eAnnReplay: (syncN: int, scope: int, vBase: int, cBase: int, ghost: tRoundGhost);
event eAnnClear: (syncN: int, scope: int, ghost: tRoundGhost);
event eAnnUpsert: (syncN: int, scope: int, rows: seq[tRow], ghost: tRoundGhost);
event eAnnTombstones: (syncN: int, scope: int, removes: seq[int], ghost: tRoundGhost);
event eAnnPublish: (syncN: int, scope: int, v: int, ghost: tRoundGhost);
event eAnnCheckpoint: (syncN: int);
event eAnnStop: (syncN: int);
event eAnnCrash: (syncN: int);
// config = the sealing attempt's compat config (0 when unmodeled);
// P1 clause (c) compares every sealed row's ghost tag to it.
event eAnnSeal: (syncN: int, partition: tPartition, manifest: map[int, int], blocked: bool, config: int);
// Scripted seal-state expectation (scenario 5 oracle): announced by
// MEnv when the stop-stranding premise lands; the SealExpect monitor
// checks this sync's seal. wantBlocked: the artifact must seal
// produce-blocked (its violation in the 5b crash cell IS the
// crash-window finding). wantScopeEmpty: partition[scope] must seal
// empty — the 5b silent-dropout pin (the B1-ignored carrier leaves no
// rows and no failure; this scripted expectation is its only
// executable oracle, MODEL_SPEC 9.5b).
event eAnnExpectSeal: (syncN: int, scope: int, wantBlocked: bool, wantScopeEmpty: bool);
// Session announce (P6-A vocabulary): committed session writes and the
// final session KV at seal ride the announce channel like row ops.
event eAnnSessionSet: (syncN: int, key: int, val: int);
event eAnnSessionGet: (syncN: int, key: int, found: bool, val: int);
// P6-R counterfactual ghost (MODEL_SPEC 7): the producer policy's
// phase-final session value under an all-fresh execution at this
// sync's epoch — computable (deterministic policies, sequential
// phases), announced by MEnv at sync start, never executed.
event eAnnCounterfactual: (syncN: int, key: int, val: int);
// Loud-cold: a gate (binding / warm-gate) detected a mismatch and the
// chain failed cold instead of copying — the mitigation's success path.
event eAnnLoudCold: (syncN: int, scope: int, reason: int); // 1=binding, 2=warmGate
// Scenario-8 announce vocabulary (P8). eAnnExtTruth: the env's ghost
// of the external source's CURRENT answer, announced at sync start
// and after every between-attempt mutation — the truth the CURRENT
// clause compares each attempt's list against. eAnnExtRound: the
// store's commit-side record of one external round (the answer the
// attempt listed, whether reconciliation ran, what it deleted).
// eAnnExtSeal: the ext keyspace as sealed, announced with the seal.
event eAnnExtTruth: (syncN: int, ids: seq[int]);
event eAnnExtRound: (syncN: int, live: seq[int], supported: bool, deleted: seq[int]);
event eAnnExtSeal: (syncN: int, ids: seq[int]);
// Attempt-level loud failure record (P4): the RESTORED checkpoint's
// scheduler-progress state (stack/hits/replayed — the ingest-quality
// blocked flag is deliberately excluded: it does not influence the
// verdict) plus the failing step (scope, reason, offending cursor).
// P4Stuck's red = two CONSECUTIVE attempts failing from identical
// restored state at the same step — CO-6b-004's stuck-resume contract.
event eAnnAttemptFailed: (syncN: int, gen: int, stack: seq[tAction], hits: map[int, int], replayed: map[int, bool], scope: int, reason: int, cursor: int);
