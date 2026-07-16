package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

// Dirty-connector replay-equivalence scenarios.
//
// THE PROPERTY: default-mode warm ≡ default-mode cold, for connectors
// whose data carries EXPECTED configuration gaps — dangling references
// whose referent's resource TYPE is not synced (disabled). Under the
// replay policy those are the DROP arm of the referential invariants
// (I7/I8/I9): dependent rows drop with one aggregated warning, the
// artifact seals referentially consistent, and every dropped-from
// scope's manifest entry is INVALIDATED so the scope re-fetches cold
// next round. Enabled-type danglings are NOT equivalence material — they
// fail the sync (warm: ErrReplayIntegrity + cold retry; cold: plain) and
// are pinned by TestIngestInvariantEnabledTypeDanglingsFail and
// TestDishonestValidatorStrandsGrantEntitlements.
//
// The clean harness (source_cache_equivalence_test.go) runs fail-fast,
// so the drop arms never execute inside its property; this family runs
// default mode on both legs. The manifest-invalidation-on-drop contract
// is what the dirty→clean transition pins: without it, a later honest
// 304 replays the sanitized subset while a cold sync fetches the full
// set — warm ≠ cold exactly when a dropped row's referent appears.
//
// DIRTY SHAPES (all disabled-type; forced, every scenario):
//   - I9: groups hold members of the SHADOW type (ids "shadow-*"),
//     which is disabled at round 0 — member grants drop; and of the
//     ghost2 type (ids "ghost2-*"), which NEVER enables — steady dirty.
//   - I8: gd8's grants page emits a grant on the shadow HOST's "admin"
//     entitlement, which exists only once the shadow type is enabled.
//   - I7 (type-scoped): team chunk 0's entitlements page emits an
//     entitlement for the shadow host2 resource (and chunk 0's grants
//     page a grant under it — the I8 cascade).
//   - Exempt carriers: the ext group's unprocessed ExternalResourceMatch
//     carriers (no external c1z configured in this family) are KEPT in
//     both artifacts every round.
//
// TRANSITIONS (round schedule; see mutateDirty):
//   r0  initial dirty — shadow type disabled: I7, I8, and I9 drops fire.
//   r1  dirty→clean (THE BUG CELL): the shadow type is ENABLED — its
//       type row, resources, and host entitlements appear — while every
//       dropped-from GRANTS/ENTS scope's validator stays put. Without
//       manifest invalidation the warm side 304s the sanitized subsets
//       and diverges from cold here.
//   r2  no-op + crash/resume: the warm sync is crashed mid-grants on a
//       steady-dirty scope and resumed (drops + invalidation must
//       behave identically through resume).
//   r3  clean→dirty via OVERLAY: gdelta's delta round adds a ghost2
//       member — dangling data arriving on the overlay path.
//   r4  no-op — steady dirty chain-decay probe: ghost2 scopes re-fetch
//       cold (invalidated), re-drop, re-warn; all clean scopes 304; the
//       output must STILL equal cold.
//
// ORACLES: the clean harness's full semantic diff + manifest and
// scope-index comparison (manifest entries carry the invalidation
// verdict); exact model-derived post-drop row sets asserted against
// BOTH artifacts; and the warning-silence oracle INVERTED — every round
// asserts the expected "ingest invariant ... DROPPED" warnings are
// present on both legs (the anti-vacuity proof that drops executed) and
// that nothing unexpected warned. A dirty scenario that never dropped
// anything fails itself.

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/logging"
	"github.com/conductorone/baton-sdk/pkg/types"
	gt "github.com/conductorone/baton-sdk/pkg/types/grant"
)

// newDirtyEquivModel is the clean topology plus the dirty shapes. The
// clean groups/orgs/teams are kept so the dirty rounds still have a
// large clean surface that must keep 304ing correctly around the drops.
func newDirtyEquivModel() *equivModel {
	m := newEquivModel()
	// I9 dirty→clean: a shadow-typed member, missing until the shadow
	// type is enabled in round 1.
	m.groups["gd9"] = &equivGroup{id: "gd9", members: map[string]bool{"u0": true, "shadow-mem": true}}
	// I9 steady dirty: a ghost2-typed member whose type never enables.
	m.groups["gd"] = &equivGroup{id: "gd", members: map[string]bool{"u0": true, "ghost2-mem": true}}
	// I9 clean→dirty via overlay: gains a ghost2 member in round 3.
	m.groups["gdelta"] = &equivGroup{id: "gdelta", members: map[string]bool{"u3": true}}
	// I8: the shadow-host admin grant is emitted every round; its
	// entitlement row exists only from round 1.
	m.groups["gd8"] = &equivGroup{id: "gd8", members: map[string]bool{"u1": true}, ghostAdminGrant: true}
	// I7 (+ I8 cascade) in the type-scoped chunk scopes.
	m.ghostTeamEnt = true
	// The shadow type's row universe once enabled.
	m.shadowUsers = map[string]bool{"shadow-mem": true}
	return m
}

// mutateDirty is the dirty family's forced round schedule (no seeded
// churn: every transition is load-bearing and must occur exactly where
// the oracles expect it).
func (m *equivModel) mutateDirty(round int) {
	m.clearDeltas()
	switch round {
	case 1:
		// Dirty→clean: ENABLE the shadow resource type. Its type row,
		// resources, and host entitlements appear via their own pages —
		// every previously dropped-from scope's validator stays put.
		m.shadowEnabled = true
	case 3:
		// Clean→dirty via a delta overlay round (ghost2 never enables).
		m.addMember("gdelta", "ghost2-d")
	default:
		// Rounds 2 and 4: no-op (r2 additionally crashes/resumes the
		// warm leg; steady-dirty scopes re-fetch and re-drop each round).
	}
}

// expectedDirtyResourceKeys is expectedResourceKeys minus external
// principals (no external c1z in this family) plus the shadow universe
// once enabled.
func expectedDirtyResourceKeys(m *equivModel) []string {
	set := map[string]struct{}{}
	for uid := range m.users {
		set[userResourceType.GetId()+"/"+uid] = struct{}{}
	}
	for gid := range m.groups {
		set[groupResourceType.GetId()+"/"+gid] = struct{}{}
	}
	for orgID, org := range m.orgs {
		set[equivOrgRT.GetId()+"/"+orgID] = struct{}{}
		for projID := range org.projects {
			set[equivProjectRT.GetId()+"/"+projID] = struct{}{}
		}
		for repoID := range org.repoGrants {
			set[equivRepoRT.GetId()+"/"+repoID] = struct{}{}
		}
	}
	for tid := range m.teams {
		set[equivTeamRT.GetId()+"/"+tid] = struct{}{}
	}
	if m.shadowEnabled {
		for id := range m.shadowUsers {
			set[equivShadowRT.GetId()+"/"+id] = struct{}{}
		}
		set[equivShadowRT.GetId()+"/"+equivShadowHostID] = struct{}{}
		set[equivShadowRT.GetId()+"/"+equivShadowHost2ID] = struct{}{}
	}
	return sortedKeys(set)
}

// expectedDirtyEntitlementIDs is the POST-DROP entitlement set: the
// shadow hosts' entitlements exist only while the type is enabled (I7
// drops the host2 entitlement otherwise; the host's admin entitlement
// is simply not listed).
func expectedDirtyEntitlementIDs(m *equivModel) []string {
	set := map[string]struct{}{}
	for gid, g := range m.groups {
		set[equivMemberEnt(equivGroupResource(gid), g.exclusionDefault).GetId()] = struct{}{}
	}
	for tid := range m.teams {
		set[equivMemberEnt(equivTeamResource(tid), false).GetId()] = struct{}{}
	}
	if m.shadowEnabled {
		set[equivShadowAdminEnt().GetId()] = struct{}{}
		if m.ghostTeamEnt {
			set[equivShadowHost2Ent().GetId()] = struct{}{}
		}
	}
	return sortedKeys(set)
}

// expectedDirtyGrantIDs is the POST-DROP grant set:
//   - member grants for user-typed members always; shadow members only
//     while the type is enabled (I9 drops them otherwise); ghost2
//     members never (their type never enables);
//   - gd8's shadow-admin grant and the chunk-0 host2 grant only while
//     the shadow type is enabled (I8 / I7-cascade drops otherwise);
//   - the ext group's match carriers are KEPT (exempt) every round.
func expectedDirtyGrantIDs(m *equivModel) []string {
	set := map[string]struct{}{}
	memberExpected := func(uid string) bool {
		switch {
		case strings.HasPrefix(uid, "shadow-"):
			return m.shadowEnabled && m.shadowUsers[uid]
		case strings.HasPrefix(uid, "ghost2-"):
			return false
		default:
			return m.users[uid]
		}
	}
	for gid, g := range m.groups {
		if gid == m.extGroupID {
			continue
		}
		for uid := range g.members {
			if memberExpected(uid) {
				set[equivMemberGrant(gid, uid).GetId()] = struct{}{}
			}
		}
		if g.ghostAdminGrant && m.shadowEnabled {
			set[equivShadowAdminGrant("u1").GetId()] = struct{}{}
		}
	}
	// The expandable group-to-group membership and its derived grants.
	parentRes := equivGroupResource(m.expandableParent)
	parentEnt := equivMemberEnt(parentRes, false)
	set[gt.NewGrantID(equivGroupResource(m.expandableChild), parentEnt)] = struct{}{}
	for uid := range m.groups[m.expandableChild].members {
		if m.users[uid] {
			set[gt.NewGrantID(
				v2.ResourceId_builder{ResourceType: userResourceType.GetId(), Resource: uid}.Build(),
				parentEnt)] = struct{}{}
		}
	}
	// Unprocessed match carriers survive whole (exempt in I9).
	for _, extID := range m.extMatchIDs {
		set[equivMemberGrant(m.extGroupID, "placeholder-"+extID).GetId()] = struct{}{}
	}
	set[equivMemberGrant(m.extGroupID, "placeholder-all").GetId()] = struct{}{}
	for _, org := range m.orgs {
		for repoID, uid := range org.repoGrants {
			set[gt.NewGrant(equivRepoResource(repoID), "admin",
				v2.ResourceId_builder{ResourceType: userResourceType.GetId(), Resource: uid}.Build()).GetId()] = struct{}{}
		}
	}
	for tid, members := range m.teams {
		for uid := range members {
			set[equivTeamGrant(tid, uid).GetId()] = struct{}{}
		}
	}
	if m.ghostTeamEnt && m.shadowEnabled {
		set[equivShadowHost2Grant("u0").GetId()] = struct{}{}
	}
	return sortedKeys(set)
}

// assertDirtyArtifacts asserts one artifact's row sets EXACTLY equal the
// model-derived post-drop expectation — dropped rows absent, exempt
// carriers and IRG shapes present. Applied to BOTH the cold and warm
// snapshots (the differential diff alone can't distinguish "both sides
// dropped" from "neither side dropped").
func assertDirtyArtifacts(t *testing.T, round int, m *equivModel, label string, snap *c1zSnapshot) {
	t.Helper()
	require.Equal(t, expectedDirtyResourceKeys(m), sortedKeys(snap.resources),
		"round %d: %s resource set must equal the post-drop expectation", round, label)
	require.Equal(t, expectedDirtyEntitlementIDs(m), sortedKeys(snap.entitlements),
		"round %d: %s entitlement set must equal the post-drop expectation", round, label)
	require.Equal(t, expectedDirtyGrantIDs(m), sortedKeys(snap.grants),
		"round %d: %s grant set must equal the post-drop expectation", round, label)
	// Exempt carriers, named explicitly (also implied by the exact sets).
	require.Contains(t, snap.grants, equivMemberGrant(m.extGroupID, "placeholder-ext-0").GetId(),
		"round %d: %s must keep the exempt match carriers", round, label)
}

// Invariant warning fingerprints (must match the aggregated warn
// messages in ingest_invariants.go).
const (
	dirtyWarnI7Drop  = "ingest invariant I7: DROPPED entitlements"
	dirtyWarnI8Drop  = "ingest invariant I8: DROPPED grants"
	dirtyWarnI9Drop  = "ingest invariant I9: DROPPED grants"
	dirtyWarnI9Kept  = "ingest invariant I9: kept unprocessed external-match carrier grants"
	dirtyWarnI3Never = "ingest invariant I3: grants reference a resource that was never synced"
)

// dirtyExpectedWarnings returns the warning fingerprints REQUIRED on
// every leg of a round (warm and cold alike — drops are a pure function
// of the converged store). Anything captured that matches no required
// fingerprint fails the leg.
func dirtyExpectedWarnings(round int) []string {
	if round == 0 {
		// Shadow type disabled: the I7 host2 entitlement, the I8
		// shadow-admin + host2-cascade grants (I3 warns about their
		// never-synced entitlement resources before I8 drops them), and
		// the I9 shadow/ghost2 members, plus the kept-carriers report.
		return []string{dirtyWarnI7Drop, dirtyWarnI8Drop, dirtyWarnI9Drop, dirtyWarnI9Kept, dirtyWarnI3Never}
	}
	// Rounds 1+: the shadow shapes are clean; only the ghost2 steady-
	// dirty members keep dropping (I9) and the carriers keep being kept.
	return []string{dirtyWarnI9Drop, dirtyWarnI9Kept}
}

func assertDirtyWarnings(t *testing.T, round int, label string, msgs []string) {
	t.Helper()
	expected := dirtyExpectedWarnings(round)
	for _, want := range expected {
		found := false
		for _, msg := range msgs {
			if strings.Contains(msg, want) {
				found = true
				break
			}
		}
		require.True(t, found,
			"round %d (%s): expected invariant warning %q never fired — the drop arm did not execute (vacuous dirty round). captured: %v",
			round, label, want, msgs)
	}
	for _, msg := range msgs {
		allowed := false
		for _, want := range expected {
			if strings.Contains(msg, want) {
				allowed = true
				break
			}
		}
		require.True(t, allowed,
			"round %d (%s): unexpected ingestion-invariant warning %q", round, label, msg)
	}
}

const dirtyEquivRounds = 4

// dirtyCrashRound crashes the warm sync mid-grants and resumes it, in a
// round where the steady-dirty drops fire — drops and manifest
// invalidation must behave identically through resume.
const dirtyCrashRound = 2

// TestSourceCache_DirtyHaltAtInvariantSeam crashes a DIRTY sync at the
// "ingest-invariants-complete" seam — after the disabled-type drops (and
// their manifest invalidations) committed, before the checkpoint and
// EndSync — then resumes the same file. The resumed sync re-runs the
// whole invariant pass over already-dropped state, so this pins the
// post-drop crash contract end to end at the SYNCER level (the engine's
// chunk-boundary crash test pins the intra-drop window): drops are
// idempotent on re-run, the invalidations committed by the first attempt
// survive and are not misread (I6 must not report the invalidated scopes
// as orphans), and the resumed output equals a cold sync — including
// through round 1's dirty→clean replay, where a lost invalidation would
// 304 the sanitized subsets.
func TestSourceCache_DirtyHaltAtInvariantSeam(t *testing.T) {
	ctx, err := logging.Init(t.Context())
	require.NoError(t, err)
	tmpDir := t.TempDir()

	model := newDirtyEquivModel()
	warmConn := newEquivConnector(model, false)
	coldConn := newEquivConnector(model, true)
	var warmClient types.ConnectorClient = warmConn
	var coldClient types.ConnectorClient = coldConn

	halt := &haltOnce{stage: "ingest-invariants-complete"}
	hookOpt := func(s *syncer) { s.testSourceCacheHaltHook = halt.hook }

	// Round 0: the warm leg crashes at the seam, then resumes the SAME
	// file to completion.
	warm0 := filepath.Join(tmpDir, "warm-r0.c1z")
	haltErr := runEquivSyncMode(ctx, t, warmClient, warm0, "", tmpDir, false, hookOpt)
	require.Error(t, haltErr, "the armed halt must crash the dirty sync at the invariant seam")
	require.Contains(t, haltErr.Error(), "injected halt at ingest-invariants-complete")
	require.NoError(t, runEquivSyncMode(ctx, t, warmClient, warm0, "", tmpDir, false, hookOpt),
		"resume after the invariant-seam halt must complete (drops must be idempotent over already-dropped state)")

	cold0 := filepath.Join(tmpDir, "cold-r0.c1z")
	require.NoError(t, runEquivSyncMode(ctx, t, coldClient, cold0, "", tmpDir, false))

	warmSnap := snapshotC1z(ctx, t, warm0)
	coldSnap := snapshotC1z(ctx, t, cold0)
	require.Equal(t, 1, warmSnap.syncRuns, "the halted sync must RESUME (one sync run), not restart")
	assertDirtyArtifacts(t, 0, model, "warm", warmSnap)
	assertDirtyArtifacts(t, 0, model, "cold", coldSnap)
	requireSnapshotsEqual(t, 0, coldSnap, warmSnap)
	invalidated := 0
	for _, entry := range warmSnap.manifest {
		if entry.Invalidated {
			invalidated++
		}
	}
	require.Positive(t, invalidated,
		"round 0's drops must leave invalidated manifest entries in the resumed artifact")

	// Round 1 (dirty→clean): warm replays from the resumed artifact. A
	// lost invalidation would 304 the sanitized scopes here and diverge
	// from cold.
	model.mutateDirty(1)
	warm1 := filepath.Join(tmpDir, "warm-r1.c1z")
	cold1 := filepath.Join(tmpDir, "cold-r1.c1z")
	require.NoError(t, runEquivSyncMode(ctx, t, warmClient, warm1, warm0, tmpDir, false))
	require.NoError(t, runEquivSyncMode(ctx, t, coldClient, cold1, "", tmpDir, false))
	warmSnap1 := snapshotC1z(ctx, t, warm1)
	coldSnap1 := snapshotC1z(ctx, t, cold1)
	assertDirtyArtifacts(t, 1, model, "warm", warmSnap1)
	assertDirtyArtifacts(t, 1, model, "cold", coldSnap1)
	requireSnapshotsEqual(t, 1, coldSnap1, warmSnap1)
}

func TestSourceCache_ReplayEquivalenceDirty(t *testing.T) {
	scenarios := []struct {
		workers      int
		seed         int64
		continuation bool
	}{
		{workers: 0, seed: 11},
		{workers: 4, seed: 12},
		{workers: 0, seed: 13, continuation: true},
	}
	for _, sc := range scenarios {
		name := fmt.Sprintf("workers=%d/seed=%d", sc.workers, sc.seed)
		if sc.continuation {
			name += "/continuation"
		}
		t.Run(name, func(t *testing.T) {
			runDirtyReplayEquivalenceScenario(t, sc.workers, sc.seed, sc.continuation)
		})
	}
}

func runDirtyReplayEquivalenceScenario(t *testing.T, workers int, seed int64, continuation bool) {
	ctx, err := logging.Init(t.Context())
	require.NoError(t, err)
	ctx, invariantWarnings := withInvariantWarningOracle(ctx)
	tmpDir := t.TempDir()
	// The schedule is fully forced; the seed only labels the scenario
	// (kept for parity with the clean family and future churn).
	_ = rand.New(rand.NewSource(seed)) //nolint:gosec // deterministic test randomness
	t.Logf("dirty replay-equivalence scenario: workers=%d seed=%d rounds=%d continuation=%v", workers, seed, dirtyEquivRounds, continuation)

	model := newDirtyEquivModel()
	warmConn := newEquivConnector(model, false)
	coldConn := newEquivConnector(model, true)
	warmConn.continuation = continuation
	coldConn.continuation = continuation
	var warmClient types.ConnectorClient = warmConn
	var coldClient types.ConnectorClient = coldConn
	if continuation {
		warmClient = equivContinuationClient{warmConn}
		coldClient = equivContinuationClient{coldConn}
	}

	// NO external c1z: unprocessed match carriers must survive to the
	// invariant seam (the exempt shape) — with a file configured the
	// match op deletes every carrier it processes.
	syncOpts := func() []SyncOpt {
		var opts []SyncOpt
		if workers > 0 {
			opts = append(opts, WithWorkerCount(workers))
		}
		return opts
	}

	// runLeg runs one default-mode sync and asserts the leg's invariant
	// warnings: the round's expected drops MUST have fired (anti-
	// vacuity), and nothing else may warn.
	runLeg := func(round int, label string, cc types.ConnectorClient, path, prevPath string) {
		t.Helper()
		before := len(invariantWarnings.captured())
		require.NoError(t, runEquivSyncMode(ctx, t, cc, path, prevPath, tmpDir, false, syncOpts()...))
		assertDirtyWarnings(t, round, label, invariantWarnings.captured()[before:])
	}

	prevWarm := ""
	for round := 0; round <= dirtyEquivRounds; round++ {
		if round > 0 {
			model.mutateDirty(round)
		}
		warmPath := filepath.Join(tmpDir, fmt.Sprintf("warm-r%d.c1z", round))
		coldPath := filepath.Join(tmpDir, fmt.Sprintf("cold-r%d.c1z", round))

		before := warmConn.snapshotCounters()
		if round == dirtyCrashRound {
			// Crash the warm sync mid-grants on a steady-dirty scope
			// (gd re-fetches every round: its manifest entry was
			// invalidated by the previous round's drop), then resume the
			// SAME file.
			warmConn.injectGrantsFailure("gd")
			crashErr := runEquivSyncMode(ctx, t, warmClient, warmPath, prevWarm, tmpDir, false, syncOpts()...)
			require.Error(t, crashErr, "round %d: the injected failure must crash the warm sync", round)
			require.Contains(t, crashErr.Error(), "injected crash")
			require.Equal(t, 1, warmConn.snapshotCounters().injectedFailures-before.injectedFailures,
				"round %d: exactly one injected failure expected", round)
		}
		runLeg(round, "warm", warmClient, warmPath, prevWarm)
		after := warmConn.snapshotCounters()
		if round > 0 {
			require.Positive(t, (after.pages304+after.pagesOverlay)-(before.pages304+before.pagesOverlay),
				"round %d: VACUOUS RUN — the warm sync replayed nothing, so equivalence proves nothing", round)
		}
		runLeg(round, "cold", coldClient, coldPath, "")

		coldSnap := snapshotC1z(ctx, t, coldPath)
		warmSnap := snapshotC1z(ctx, t, warmPath)
		if round == dirtyCrashRound {
			require.Equal(t, 1, warmSnap.syncRuns,
				"round %d: the crashed warm sync must RESUME (one sync run), not restart", round)
		}
		assertDirtyArtifacts(t, round, model, "cold", coldSnap)
		assertDirtyArtifacts(t, round, model, "warm", warmSnap)
		requireSnapshotsEqual(t, round, coldSnap, warmSnap)

		prevWarm = warmPath
	}

	// Scenario-level coverage: the dirty rounds must still have
	// exercised the replay machinery they claim to run drops through.
	final := warmConn.snapshotCounters()
	require.Positive(t, final.pages304, "coverage: no 304 replay round ever happened")
	require.Positive(t, final.pagesOverlay, "coverage: no delta-overlay round ever happened (gdelta round 3)")
	require.Positive(t, final.pagesFresh, "coverage: no cold-miss page ever happened")
	require.Positive(t, final.chunkPages304, "coverage: no spawned chunk cursor ever replayed")
	require.Positive(t, final.chunkPagesFresh, "coverage: no spawned chunk cursor ever fetched fresh")
	require.Positive(t, final.injectedFailures, "coverage: the crash/resume round never crashed")
	if continuation {
		require.Positive(t, final.askPages, "coverage: continuation topology never asked")
	} else {
		require.Zero(t, final.askPages, "harness bug: direct topology must never ask")
	}
	coldFinal := coldConn.snapshotCounters()
	require.Zero(t, coldFinal.pages304+coldFinal.pagesOverlay,
		"harness bug: the cold connector must never replay")
}
