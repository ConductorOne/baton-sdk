/* Types for the demand-graph runtime model (deliverable 4).
   Source of truth: formal/GRAPH_MODEL_SPEC.md (v4, FROZEN).
   Small scope (SPEC 8): <=4 nodes, epochs 1..3, row ids 0..1,
   <=3 syncs, <=3 attempts/sync, 2 workers, <=2 pages/round,
   pass-iteration budget <=3.

   Identity conventions (build decision, logged in CALIBRATION.md):
   nodes are ints 0..3; a node's output key = its node id; a node's
   derivation hash = node id + 1 (so childHash 0 = "no child named");
   the G8c distinct-derivation-same-key shape overrides the key
   mapping per cell. Generations are per-node monotone counters in
   the scheduler table (G-RULE-3); attempt ids (store crash gating)
   are syncN*10 + attempt, disjoint from node generations. */

// Node kinds: scripted execution bodies (SPEC 3 MNodeExec).
enum tNodeKind { NK_PARENT, NK_CONSULT, NK_WRITER, NK_READER }

// Verdict classes (SPEC 3: ADOPT is a registered verdict class,
// round-2 R2-F3, alongside replay / changed-with-diff / fetch-fresh).
enum tGVerdict { GV_ADOPT, GV_REPLAY, GV_DIFF, GV_FRESH }

// Lineage variants (SPEC 1: both first-class scheduler modes).
enum tLineage { LIN_E, LIN_S }

// Session store variants (SPEC 3 MSessionStore).
enum tSessVar { SESS_A, SESS_B }

// Consult/revalidation outcome (digest vocabulary, SPEC 4a).
// 0 = MISS, 1 = MATCH, 2 = FAIL — ints so digests compare with ==.

// A stored row. epoch is the ghost content tag (truthful upstream),
// hops the P2 replay-travel counter, childHash the demand-derivation
// content (G-RULE-1: structure rides emissions; 0 = none), and the
// sess* triple the embedded session read (sVal -1 = none) for the
// P6-G value comparison.
type tGRow = (id: int, epoch: int, hops: int, childHash: int, sVal: int, sWriter: int, sWGen: int);

// key -> row id -> row
type tGPart = map[int, map[int, tGRow]];

// Premise digest (SPEC 4a): canonical hash of the consult result
// (previous entry + revalidation outcome) and the identity+writer-
// stamp of every session value read before unit commit. One session
// read per node in every scripted cell (build decision).
type tDigest = (v: int, outcome: int, sVal: int, sWriter: int, sWGen: int, hasSess: bool);

// Unit marker (SPEC 3/4a): per-sync store row. roundId identifies the
// marked round for P-MARK; contentEpoch is the marked round's output
// content tag (replay -> vBase, overlay -> consult epoch).
type tMarker = (node: int, gen: int, roundId: int, digest: tDigest, pubBearing: bool, voided: bool, contentEpoch: int);

// A pending node on the frontier (G-RULE-4 checkpoint row).
type tPendingNode = (node: int, kind: tNodeKind, hash: int, key: int, gen: int);

// Generation-qualified admitted-by edge (variant E lineage state,
// durable per G-RULE-4).
type tEdge = (parent: int, pgen: int);

// Session read record (scheduler session index per variant; in the
// checkpoint per G-RULE-4).
type tReadRec = (reader: int, rgen: int, skey: int, val: int, writer: int, wgen: int);

// Session publish record (worker completion report).
type tPub = (skey: int, val: int, wgen: int);

// Frontier checkpoint (G-RULE-4, total): pending nodes, the
// admitted-derivation set, admitted-by edges, the generation table,
// the session index. Variant E support counts are derived (rebuild
// target = checkpoint-consistent value).
type tGCkpt = (pending: seq[tPendingNode], admitted: map[int, bool], edges: map[int, seq[tEdge]], genTable: map[int, int], readers: seq[tReadRec]);

// Mitigation/mechanism toggles (SPEC 6). Positive names, default ON
// where the toggle REMOVES a mechanism (walker convention); the two
// injections (adoptOnFail, writerAdopt) default OFF.
type tGToggles = (
    suppression: bool,      // G-RULE-2 admission suppression
    sweep: bool,            // seal-time sweep
    sweepOverreach: bool,   // INJECT: sweep drops an in-closure key
    purge: bool,            // E pending-purge on death (∀-predicate)
    stampMerge: bool,       // S read-side stamp merge
    retraction: bool,       // E+B re-publish reader retraction
    overlayComposeDead: bool, // INJECT: overlay over a dead base
    resumeCkpt: bool,       // forced resume checkpoint (F4)
    demandDrop: bool,       // INJECT: drop one derived admission
    adoptOnFail: bool,      // INJECT: waive MATCH-only eligibility
    writerAdopt: bool,      // INJECT: waive writer-ineligibility
    quiesce: bool,          // quiesce-before-bump (R2-F1)
    midBumpFence: bool,     // mid-bump fence (R3-F2)
    markerCleanup: bool     // REPLACES clear deletes the marker (R2-F4)
);

// Scenario configuration, built per test cell in PTst via
// defaultGCfg() + field overrides.
type tGCfg = (
    // Cell topology / script id:
    // 11 = G1 family: parent P (node 0) -> consult node C (node 1);
    //      P's fresh rows all name C (double-announce premise).
    // 21 = G2 family: P (node 0) -> writer H (node 2) + reader G
    //      (node 3); H publishes session key 0; G reads it (phase 2).
    // 24 = G5 family: PAGINATED parent P (node 0) -> consult node C
    //      (node 1), named on P's page-1 row ONLY (the row the
    //      e1->e2 mutation deletes: upstream demand shrink). The
    //      mutation target is P's OWN scope (key 0).
    // 25 = G3 (artifact swap + rebind): cell-11 topology; interrupt 1
    //      stops C after its attempt-1 consult; the env swaps C's
    //      PREV artifact to sibling content between attempts.
    // 26 = G6a/G6b (redo bake-off chain): P (node 0) -> S1 (node 1,
    //      names C on its page-1 row) -> C (node 4) -> GC (node 5).
    //      G6b mutates S1's scope (key 1) between attempts: the
    //      descendant chain's demand shrinks.
    // 27 = G7 (progress under churn): cell-11 topology; node 1 fails
    //      LOUD deterministically in failSync (generation-blind
    //      fingerprint; the abandon ladder is cfg.ladder).
    // 28 = G6c (fan-in): P names S1 (node 1) + S2 (node 4); BOTH name
    //      C (node 5) — two admitted-by edges; a crash killing one
    //      parent must not purge/refuse C on the survivor's live edge.
    // 29 = G8c (same-key distinct-derivation): P names node 1 AND
    //      node 4; keyOf maps BOTH to output key 1 (distinct hashes,
    //      one key) — the store poisons on the second derivation's
    //      first commit.
    cell: int,
    lineage: tLineage,
    sessVar: tSessVar,
    // Interruption script: 0 = none; 1 = graceful stop at C's
    // attempt-1 consult (G3: stop-forced checkpoint, store intact);
    // 2 = hard crash in attempt 1 of interruptSync; 3 = hard crash
    // in attempts 1 AND 2 (two-crash cells: G1b, G1e, G8d).
    interrupt: int,
    interruptSync: int,
    nSyncs: int,
    mutateBetweenSyncs: bool,    // key 1: e1 -> e2 after sync 1
    mutateBetweenAttempts: bool, // key 1: +1 between attempts of interruptSync
    // Connector policy (verdicts are connector choice, walker
    // precedent): FAIL verdict yields CHANGED-WITH-DIFF unit when
    // diffPolicy, else fetch-fresh record round; a re-derivation
    // after an ineligible/failed marker check uses fetch-fresh when
    // rederiveFresh (the G1(ii)/G1c scripted shape).
    diffPolicy: bool,
    rederiveFresh: bool,
    // Worker pool size (SPEC 8 budget: 2). The G1d cell scripts 3 so
    // the retraction-forced re-run can dispatch AT the bump instead
    // of waiting for a completion to free a worker — with 2, the
    // dying-reader race needs two long starvation phases and random
    // search cannot reach it (calibration find, logged).
    nWorkers: int,
    // Content flap-back (G2 flap-back probe, G8d): raw epoch >= 3
    // serves content identical to epoch 1; the upstream reports
    // CONTENT epochs everywhere (raw epochs never escape it), so
    // validators, manifests, expectations, and folds stay coherent.
    flapBack: bool,
    // G6 bake-off count oracle: max executions per node per sync
    // (0 = unmonitored). The minimal GREEN bound is the
    // checker-verified worst-case redo count for the leg.
    execBound: int,
    // G7 loud-failure script: failNode fails deterministically at
    // execution start in failSync (-1 = none). The abandon ladder
    // (abandon after 2 identical generation-blind fingerprints) is
    // the proposed machinery; ladder=false is the P4-STUCK kill.
    failNode: int,
    failSync: int,
    ladder: bool,
    // G9 compression admissibility: the S pre-seal pass compares
    // FLOOR-BUCKETED stamps (buckets of 2) — lossy but stale-erring
    // (never false-live); forced re-runs land on the bumped (even)
    // generation, so the pass still converges. Stored stamps and
    // monitors stay exact: the claim is about mechanism decisions.
    stampCompression: bool,
    // G5d meta-analysis (GS-CO-005(d)): when non-empty, the env
    // announces this map as the GSEALWORLD probe target for
    // interruptSync's seal (world = manifest restricted to keys
    // sealing non-empty partitions). Empty = probe disarmed.
    sealWorld: map[int, int],
    toggles: tGToggles
);

fun defaultGToggles(): tGToggles {
    return (
        suppression = true,
        sweep = true,
        sweepOverreach = false,
        purge = true,
        stampMerge = true,
        retraction = true,
        overlayComposeDead = false,
        resumeCkpt = true,
        demandDrop = false,
        adoptOnFail = false,
        writerAdopt = false,
        quiesce = true,
        midBumpFence = true,
        markerCleanup = true
    );
}

fun defaultGCfg(): tGCfg {
    return (
        cell = 11,
        lineage = LIN_E,
        sessVar = SESS_A,
        interrupt = 0,
        interruptSync = 2,
        nSyncs = 2,
        mutateBetweenSyncs = false,
        mutateBetweenAttempts = false,
        diffPolicy = false,
        rederiveFresh = false,
        nWorkers = 2,
        flapBack = false,
        execBound = 0,
        failNode = -1,
        failSync = 0,
        ladder = true,
        stampCompression = false,
        sealWorld = default(map[int, int]),
        toggles = defaultGToggles()
    );
}

// Round ghost carried on store ops for the monitors (walker parity;
// node/gen added for P-GEN attribution, vBase for the fold).
type tGGhost = (roundId: int, verdict: tGVerdict, consultEpoch: int, vBase: int, lastOp: bool, attempt: int, node: int, gen: int);

// Node kind script (shared by scheduler re-admissions and env roots).
fun kindOf(cell: int, node: int): tNodeKind {
    if (node == 0) { return NK_PARENT; }
    if (node == 2) { return NK_WRITER; }
    if (node == 3) { return NK_READER; }
    return NK_CONSULT;
}

// Output-key script: key = node id everywhere EXCEPT the same-key
// distinct-derivation cell (G8c), where node 4 shares node 1's
// output key — distinct hashes, one key, the poison row's premise.
fun keyOf(cell: int, node: int): int {
    if (cell == 29 && node == 4) { return 1; }
    return node;
}

// Worker completion report payload (the carrier announce whose atomic
// processing hosts all derived effects, G-RULE-1).
type tGReport = (execId: int, node: int, gen: int, hash: int, key: int, kind: tNodeKind, verdict: tGVerdict, rows: seq[tGRow], pubs: seq[tPub], reads: seq[tReadRec], aborted: bool);
