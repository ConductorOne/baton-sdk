/* Types for the walker + source-cache replay calibration model.
   Source of truth: formal/MODEL_SPEC.md (v10). Identifiers follow
   formal/GLOSSARY.md. Small scope: 1-2 scopes, epochs 1..3, row ids
   0..1, <=3 syncs, <=3 attempts/sync, 2 workers, <=2 pages/round. */

// A stored row. epoch is the ghost content tag (truthful upstream),
// hops is the P2 replay-travel counter (0 = fresh this sync),
// config is the P1 config ghost, stamp the session ghost (-1 none, 0 miss).
type tRow = (id: int, epoch: int, hops: int, config: int, stamp: int);

// scope -> row id -> row
type tPartition = map[int, map[int, tRow]];

// Mitigation toggles (MODEL_SPEC 6). OFF removes a check, never adds.
// The sessionTaint pair is produce-side (scenario 7 fix runs): session
// traffic during a replay-capable kind's phase marks that kind
// non-replayable in the artifact being produced; the NEXT sync's
// consult on a tainted kind misses (degradation, not a loud verdict).
type tToggles = (
    warmGate: bool,
    hitValidatorBinding: bool,
    scopeLocks: bool,
    oncePerScope: bool,
    annotationBinding: bool,   // de-scoped from build; kept for spec parity
    abandonLadder: bool,
    sessionTaintWrites: bool,
    sessionTaintAll: bool,
    // MS-CO-003: record-round grounding (the shipped groundRecordScope +
    // ClearSourceCacheScope fix for the scenario-1/tc1c phantom union).
    // Faithful to the code: fires at a record (fresh) round's FIRST write
    // to a scope this attempt, and clears ONLY when this sync's manifest
    // has no published entry for the scope — a published entry means a
    // completed round owns the partition's rows and record pages
    // accumulate exactly as before (the skip is load-bearing: collection
    // scopes legally accumulate record pages across rounds).
    recordGrounding: bool,
    // MS-CO-003 candidate closure (NOT shipped; kept as a design-
    // arbitration toggle like annotationBinding): validator-bound
    // grounding ALSO clears when the published entry's validator
    // differs from the record round's incoming validator — a completed
    // round of DIFFERENT content does not own a replacement listing's
    // partition. Closes the published-replay verdict-flip residual the
    // shipped skip leaves open (tc1cGround_P1's red). Caveat: multi-
    // contributor collection scopes need their own safety argument
    // before this rule could ship (contributors stamping different
    // validators would wipe each other under it).
    groundValidatorBound: bool
);

// Verdict classes (P1 ghost "verdict class"; GLOSSARY "verdict").
enum tVerdict { V_REPLAY, V_FRESH, V_OVERLAY }

// Action ops. PLANNING consults and may spawn; CARRIER carries a replay
// annotation; FRESH_PAGES is the continuation of a fetch-fresh chain.
// One op per kind (round-6 F1 pin: kind = (op, scope)).
enum tOp { OP_PLANNING, OP_CARRIER }

// An action on the scheduler stack. cursor is the page index the chain
// resumes from (0 = root token; restart-from-root restores 0 unless a
// stop-forced checkpoint captured a mid-chain cursor).
type tAction = (
    aid: int,             // action id, unique per sync
    op: tOp,
    scope: int,
    cursor: int,
    hasAnnotation: bool,  // carrier: replay annotation present in token
    annotationV: int,     // the annotation's validator (epoch-valued)
    publishes: bool       // carrier publishes its validator (1b-i vs 1b-ii)
);

// Checkpoint token (MODEL_SPEC 5): action stack (with cursors as
// captured), hit map, replayed set, and the ingest-quality fragment
// the build models: the produce-blocked flag (checkpoint-cadence
// durability — the scenario-5b crash-window finding lives in exactly
// this field). compositionEnum remains de-scoped (v11).
type tCheckpoint = (
    stack: seq[tAction],
    hits: map[int, int],       // scope -> hit validator
    replayed: map[int, bool],  // replayed set (map as set)
    blocked: bool              // produce-blocked reason flag
);

// Design variant (MODEL_SPEC 9 scenario 6). Not a mitigation toggle:
// it alters commit structure. 0 = shipped; 1 = V-NAIVE (marker as a
// separate op after eCopyScope, outside any unit); 2 = V-ATOMIC
// (eReplayUnit: clear+copy+marker+publish as ONE atomic store op;
// replay executes inline on the consulting page — no carrier spawns;
// the marker check precedes the consult and suppresses re-consult and
// re-derivation for marked scopes — clause (iii) applies to both).
// v10 addendum: VAR_OVERLAY_UNIT = V-OVERLAY-UNIT (unit boundary at the
// consult VERDICT: base copy + all overlay pages + marker + publish in
// ONE atomic op; per-page ops buffered volatile; publish deferred into
// the unit; marker-absent resume restarts from consult, mid-chain
// cursors ignored — pin o-iv). VAR_OVERLAY_NAIVE = the misdraw: the
// unit {clear, copy, marker, publish(V_to)} commits at the CONSULT
// boundary; overlay pages then commit per-page (6-overlay-naive kill).
// VAR_OVERLAY_LAST = the third placement (round-7 F2): NO unit at all —
// clear+copy commit per-page at the replay boundary, overlay pages
// per-page via the shipped path, then marker and publish(V_to) trail
// LAST as two separate ops (6-overlay-last: w2 marker-committed/
// publish-lost suppression is the P1 witness; w1's cross-attempt
// re-copy exercises the complete-rounds replacement-counting pin).
enum tVariant { VAR_SHIPPED, VAR_NAIVE, VAR_ATOMIC, VAR_OVERLAY_NAIVE, VAR_OVERLAY_UNIT, VAR_OVERLAY_LAST }

// Scenario configuration, built per test cell in PTst. Constructed via
// defaultCfg() + field overrides (P named tuples are order-sensitive).
type tScenarioCfg = (
    scenario: int,
    variant: tVariant,
    // Premise SHAPE of the planning chain / root stack:
    // 1  = 2-page planning, spawn at page 1, re-consult at page 2 (1a/1b, 3A)
    // 2  = session laundering: root stack [H writer, G reader] (case 2)
    // 3  = 1-page planning, consult+replay inline (1c, 6-family)
    // 4  = 2-page planning spawning dual carriers (case 4)
    // 7  = sessions x replay: kinds W (scope 0, producer) then R
    //      (scope 1, reader) in sequential phases (case 7)
    // 31 = 1-page planning, consult+spawn then done (3B)
    // 51 = 5a shape: cell-31 planning + warm/produce machinery,
    //      compat drift between attempts (trigger 1)
    // 52 = 5b shape: cell-31 planning + warm/produce machinery,
    //      G6 capability withdrawal in attempt 2 (trigger 2)
    // 53 = C1 probe: one action, replay annotation MID-CHAIN (page 0
    //      consults, page 1 replays); stop between the pages resumes
    //      the replay on the restored hit map without a fresh consult
    cell: int,
    // Interruption script: 0 = none, 1 = graceful stop, 2 = hard
    // crash; 3 = stop in attempt 1 THEN hard crash in attempt 2 (the
    // 5b crash-window premise). Fired in sync interruptSync only.
    interrupt: int,
    interruptSync: int,
    toggles: tToggles,
    carrierPublishes: bool, // 1b-i vs 1b-ii sub-cells
    nSyncs: int,            // total syncs incl. verification
    mutateBetweenAttempts: bool,
    mutateBetweenSyncs: bool,
    preMutate: bool,        // advance upstream to e2 before sync 1 (case 3)
    swapBase: bool,         // between-attempt base swap to artifact B (case 3)
    // Case-7a reader policy: R's connector fetches fresh regardless of
    // a valid hit (verdicts are connector choice; replay is opt-in).
    readerAlwaysFresh: bool,
    // Case-5 warm/produce axes. baseConfig: the compat config K of
    // attempt 1 (0 = configs unmodeled in this cell). driftCompat:
    // attempts >= 2 compute K2 != K1 (trigger 1 / B4). withdrawG6:
    // attempt 2 runs without source-cache handling (trigger 2).
    baseConfig: int,
    driftCompat: bool,
    withdrawG6: bool,
    // P2 corollary-run scoping (MODEL_SPEC 7: "corollary runs assert
    // staleness <= 1"): the verification sync runs only in histories
    // where the scripted interruption actually landed; otherwise the
    // honest double-replay chain reaches hops 2 legally and the <=1
    // bound is outside its run class.
    verificationOnlyIfInterrupted: bool,
    // P4 tranche axes (MODEL_SPEC 4 failure semantics / 7 P4):
    // loudColdFailsAttempt = a cold verdict fails the ATTEMPT loudly
    // (resume-on-failure; the offending cursor is checkpointed and the
    // failure recurs deterministically). Default false = the chain-cold
    // simplification of decision 10, load-bearing only in P4 cells.
    loudColdFailsAttempt: bool,
    // warmPageFails = inject ONE destination-write failure on the
    // replay page after the scope lock is acquired (CO-6b-007 premise);
    // the worker retries in-attempt, re-entering the page sequence.
    warmPageFails: bool,
    // lockReleaseOnError = the release-on-error edge (shipped). The
    // mutation check REMOVES it: the in-attempt retry then deadlocks
    // on its own leaked lock — the CO-6b-007 hang.
    lockReleaseOnError: bool,
    // o4Mutant = the o-iv-REMOVAL mutant (MS-CO-001 / parallel-review
    // F6): a V-OVERLAY-UNIT resume HONORS a restored mid-chain cursor
    // instead of restarting at the consult — the retry collects only
    // the final page into an empty buffer and commits a unit missing
    // the first page's overlay ops (content-RED in sub-case (b)).
    o4Mutant: bool,
    // Session durability variant at the crash boundary (P6-C, the
    // CO-6b-009 root cause): 0 = shipped (durable at op commit —
    // beyond-checkpoint writes survive the cursor rollback);
    // 1 = the rejected wholesale resume-clear (checkpoint-committed
    // data destroyed); 2 = checkpoint-consistent sessions (session
    // state latched with each checkpoint, restored at crash).
    sessVariant: int,
    // Scenario-8 axes (external principals; cell 8). extRecon models
    // the storage engine's delete capability: TRUE = the shipped
    // capable path (deleteStaleExternalPrincipals reconciles a dead
    // attempt's copied principals before the current answer is
    // written); FALSE = the warn-and-continue degrade (non-deleting
    // engine) — stale principals survive to seal. extStaleList is the
    // resume-recency mutant: attempts >= 2 list the SYNC-START answer
    // instead of the current one (the behavior the
    // ResumeUsesCurrentExternalAnswer chaos pin forbids).
    extRecon: bool,
    extStaleList: bool,
    // extOverDelete is the over-deletion mutant (P8-EXT-MISSING kill):
    // a LATE stale-principal sweep whose predicate mistakes a live
    // principal for stale, running at seal prep where nothing re-writes
    // the row. It is injected at the seal (Store.p) and NOT in
    // eExtReconReq because the engine-ordered early pass is
    // structurally self-healing for over-deletion — the page-1 copy
    // rewrites every listed id (see the scenario-8 calibration notes).
    extOverDelete: bool
);

// Base config: shipped design, no interruption, no mutation, 2 syncs.
fun defaultCfg(): tScenarioCfg {
    return (
        scenario = 0,
        variant = VAR_SHIPPED,
        cell = 1,
        interrupt = 0,
        interruptSync = 2,
        toggles = (warmGate = true, hitValidatorBinding = true, scopeLocks = true, oncePerScope = true, annotationBinding = false, abandonLadder = false, sessionTaintWrites = false, sessionTaintAll = false, recordGrounding = false, groundValidatorBound = false),
        carrierPublishes = true,
        nSyncs = 2,
        mutateBetweenAttempts = false,
        mutateBetweenSyncs = false,
        preMutate = false,
        swapBase = false,
        readerAlwaysFresh = false,
        baseConfig = 0,
        driftCompat = false,
        withdrawG6 = false,
        verificationOnlyIfInterrupted = false,
        loudColdFailsAttempt = false,
        warmPageFails = false,
        lockReleaseOnError = true,
        o4Mutant = false,
        sessVariant = 0,
        extRecon = true,
        extStaleList = false,
        extOverDelete = false
    );
}

// Consult outcome handed from store+upstream to the worker.
type tConsult = (hit: bool, v: int, validated: bool);

// Round ghost label carried on store ops for the P1 fold:
// (verdict class, consult epoch, attempt config) + round identity and
// a lastOp marker (round completion = commit of last prescribed op,
// round-5 F1 pin).
type tRoundGhost = (
    roundId: int,
    verdict: tVerdict,
    consultEpoch: int,
    config: int,
    lastOp: bool,
    attempt: int   // gen; a round whose pages commit under >1 attempt is TORN
);
