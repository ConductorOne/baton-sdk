package sync //nolint:revive,nolintlint // backwards-compatible package name

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/internal/chaosconnector"
	chaosoracle "github.com/conductorone/baton-sdk/internal/chaosconnector/oracle"
	"github.com/conductorone/baton-sdk/internal/testtier"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/sourcecache"
	et "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	gt "github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
)

// Phase 6b generational steady-state suite (plan R13 — first-class
// criterion with its own closure entry; oracles OR5 + OR1 + OR2): three
// generations A cold → B warm → C warm against an UNCHANGED upstream. This
// is the only instrument observing replay's producer half: a replayed
// scope that fails to republish its manifest entry alternates hit/miss per
// generation while every individual sync stays green — only C's exact
// consult trace catches it.
//
// Scope shapes covered (all four consulted every generation):
//   - an etag-style grants scope whose validator carries unchanged (OR5d),
//   - a delta-style grants scope whose validator rotates every generation
//     with zero-change overlays (OR5d),
//   - a zero-row grants scope whose entry must persist with no rows (OR5c),
//   - an etag-style entitlements scope for cross-kind breadth.

const (
	scGenEtagScope      = "grants:team-1"
	scGenEtagValidator  = "validator-etag"
	scGenDeltaScope     = "grants:team-2"
	scGenZeroScope      = "grants:team-3"
	scGenZeroValidator  = "validator-zero"
	scGenEntsScope      = "ents:team-1"
	scGenEntsValidator  = "validator-ents"
	scGenLossyEpochName = "b-lossy"
)

// scGenEpochName names generation g's epoch: "a", "b", "c", then "gen-3"…
// for the nightly long chain.
func scGenEpochName(g int) string {
	if g < 3 {
		return []string{"a", "b", "c"}[g]
	}
	return fmt.Sprintf("gen-%d", g)
}

// scGenDeltaValidator is the delta scope's validator CURRENT in
// generation g; the scope's validator rotates every generation.
func scGenDeltaValidator(g int) string {
	return fmt.Sprintf("delta-token-%d", g+1)
}

type scGenFixture struct {
	*scCollectionFixture
	Team2   *v2.Resource
	Team3   *v2.Resource
	Member2 *v2.Entitlement
	H1      *v2.Grant
}

func newSCGenFixture(t *testing.T) *scGenFixture {
	t.Helper()
	fx := newSCCollectionFixture(t)
	team2, err := rs.NewGroupResource("Team 2", fx.TeamType, "team-2", nil)
	require.NoError(t, err)
	team3, err := rs.NewGroupResource("Team 3", fx.TeamType, "team-3", nil)
	require.NoError(t, err)
	member2 := et.NewEntitlement(team2, "member", "assignment")
	return &scGenFixture{
		scCollectionFixture: fx,
		Team2:               team2,
		Team3:               team3,
		Member2:             member2,
		H1:                  gt.NewGrant(team2, "member", fx.Users[0]),
	}
}

// scGenDeltaValidators returns (previous, current) for the delta scope in
// generation g.
func scGenDeltaValidators(g int) (string, string) {
	if g == 0 {
		return "", scGenDeltaValidator(0)
	}
	return scGenDeltaValidator(g - 1), scGenDeltaValidator(g)
}

// scGenEpoch builds one generation's dataset. The logical content is
// IDENTICAL in every generation — only validators and page annotations
// differ. lossy adds an entitlement with an unknown resource type to an
// unannotated page, which the ingest filter drops under the skip-report
// policy, marking the artifact replay-blocked.
func scGenEpoch(fx *scGenFixture, gen int, lossy bool) *chaosconnector.Dataset {
	warm := gen != 0
	deltaPrevious, deltaCurrent := scGenDeltaValidators(gen)

	d := scCollectionBase(fx.scCollectionFixture)
	d.Resources[scTeamTypeID] = chaosconnector.Pages[*v2.Resource]{
		"": {List: []*v2.Resource{fx.Team, fx.Team2, fx.Team3}},
	}

	entsTeam1 := chaosconnector.Pages[*v2.Entitlement]{
		"": {List: []*v2.Entitlement{fx.Member}, Annotations: scRecordAnno(scGenEntsScope, scGenEntsValidator)},
	}
	team2Ents := []*v2.Entitlement{fx.Member2}
	if lossy {
		team2Ents = append(team2Ents, v2.Entitlement_builder{
			Id:          "gen-ghost-entitlement",
			DisplayName: "Ghost entitlement on an unscheduled type",
			Resource: v2.Resource_builder{
				Id: v2.ResourceId_builder{ResourceType: "ghost-type", Resource: "ghost-1"}.Build(),
			}.Build(),
		}.Build())
	}
	d.Entitlements["team-2"] = chaosconnector.Pages[*v2.Entitlement]{"": {List: team2Ents}}
	d.Entitlements["team-3"] = chaosconnector.Pages[*v2.Entitlement]{"": {}}

	grantsTeam1 := chaosconnector.Pages[*v2.Grant]{
		"": {List: fx.Grants, Annotations: scRecordAnno(scGenEtagScope, scGenEtagValidator)},
	}
	grantsTeam2 := chaosconnector.Pages[*v2.Grant]{
		"": {List: []*v2.Grant{fx.H1}, Annotations: scRecordAnno(scGenDeltaScope, deltaCurrent)},
	}
	grantsTeam3 := chaosconnector.Pages[*v2.Grant]{
		"": {Annotations: scRecordAnno(scGenZeroScope, scGenZeroValidator)},
	}
	if warm {
		entsTeam1["warm"] = chaosconnector.Page[*v2.Entitlement]{
			Annotations: scReplayAnno(scGenEntsScope, scGenEntsValidator, false, nil, nil),
		}
		// Etag-style: full replay, validator carries.
		grantsTeam1["warm"] = chaosconnector.Page[*v2.Grant]{
			Annotations: scReplayAnno(scGenEtagScope, scGenEtagValidator, false, nil, nil),
		}
		// Delta-style: zero-change overlay, validator rotates.
		grantsTeam2["warm"] = chaosconnector.Page[*v2.Grant]{
			Annotations: scReplayAnno(scGenDeltaScope, deltaCurrent, true, nil, nil),
		}
		// Zero-row scope: replay of nothing still republishes the entry.
		grantsTeam3["warm"] = chaosconnector.Page[*v2.Grant]{
			Annotations: scReplayAnno(scGenZeroScope, scGenZeroValidator, false, nil, nil),
		}
	}
	d.Entitlements["team-1"] = entsTeam1
	d.Grants["team-1"] = grantsTeam1
	d.Grants["team-2"] = grantsTeam2
	d.Grants["team-3"] = grantsTeam3

	warmRoot := ""
	if warm {
		warmRoot = "warm"
	}
	if gen == 0 {
		deltaPrevious = deltaCurrent // unused: no warm root in generation 0
	}
	d.SourceCacheGrants = map[string]*chaosconnector.SourceCacheSpec{
		"team-1": {ScopeKey: scGenEtagScope, Validator: scGenEtagValidator, WarmRoot: warmRoot},
		"team-2": {ScopeKey: scGenDeltaScope, Validator: deltaPrevious, WarmRoot: warmRoot},
		"team-3": {ScopeKey: scGenZeroScope, Validator: scGenZeroValidator, WarmRoot: warmRoot},
	}
	d.SourceCacheEntitlements = map[string]*chaosconnector.SourceCacheSpec{
		"team-1": {ScopeKey: scGenEntsScope, Validator: scGenEntsValidator, WarmRoot: warmRoot},
	}
	return d
}

// scGenScenario builds a generations-long chain of identical-content
// epochs (plus the lossy variant of generation 1).
func scGenScenario(t *testing.T, fx *scGenFixture, generations int) *chaosconnector.Scenario {
	t.Helper()
	epochs := map[string]*chaosconnector.Dataset{
		scGenLossyEpochName: scGenEpoch(fx, 1, true),
	}
	for g := 0; g < generations; g++ {
		epochs[scGenEpochName(g)] = scGenEpoch(fx, g, false)
	}
	scenario := &chaosconnector.Scenario{
		Name:         "source-cache-generational",
		Seed:         1,
		InitialEpoch: "a",
		Epochs:       epochs,
	}
	require.NoError(t, scenario.Validate())
	return scenario
}

// scGenWarmEvents is the exact consult trace of a fully warm generation.
// Single-worker order: the entitlements phase precedes grants, and grants
// actions drain in reverse resource order.
func scGenWarmEvents(deltaPrevious string) []chaosconnector.SourceCacheLookupEvent {
	return []chaosconnector.SourceCacheLookupEvent{
		scWarmEventFor(sourcecache.RowKindEntitlements, scGenEntsScope, scGenEntsValidator),
		scWarmEventFor(sourcecache.RowKindGrants, scGenZeroScope, scGenZeroValidator),
		scWarmEventFor(sourcecache.RowKindGrants, scGenDeltaScope, deltaPrevious),
		scWarmEventFor(sourcecache.RowKindGrants, scGenEtagScope, scGenEtagValidator),
	}
}

// scGenEntries is the expected manifest-entry map after the given
// generation; only the delta scope's validator differs per generation.
func scGenEntries(gen int) map[string]string {
	_, deltaCurrent := scGenDeltaValidators(gen)
	return map[string]string{
		chaosoracle.KindScope(sourcecache.RowKindGrants, scGenEtagScope):       scGenEtagValidator,
		chaosoracle.KindScope(sourcecache.RowKindGrants, scGenDeltaScope):      deltaCurrent,
		chaosoracle.KindScope(sourcecache.RowKindGrants, scGenZeroScope):       scGenZeroValidator,
		chaosoracle.KindScope(sourcecache.RowKindEntitlements, scGenEntsScope): scGenEntsValidator,
	}
}

// scGenStamps is the expected stamped-row count map — identical every
// generation; the zero-row scope never appears.
func scGenStamps() map[string]int {
	return map[string]int{
		chaosoracle.KindScope(sourcecache.RowKindGrants, scGenEtagScope):       2,
		chaosoracle.KindScope(sourcecache.RowKindGrants, scGenDeltaScope):      1,
		chaosoracle.KindScope(sourcecache.RowKindEntitlements, scGenEntsScope): 1,
	}
}

func TestChaosSourceCacheGenerationalSteadyState(t *testing.T) {
	skipChaosInShort(t)
	ctx, cancel := context.WithTimeout(t.Context(), 120*time.Second)
	defer cancel()
	tmpDir, paths := sourceCachePaths(t, 4)
	pathA, pathB, pathC, baselinePath := paths[0], paths[1], paths[2], paths[3]

	fx := newSCGenFixture(t)
	scenario := scGenScenario(t, fx, 3)
	capability := sourceCacheCapabilityRW("gen-1", "cfg-1")

	runGeneration := func(epoch, c1zPath, prevPath string) []chaosconnector.SourceCacheLookupEvent {
		run, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
		require.NoError(t, err)
		require.NoError(t, run.SetEpoch(epoch))
		run.SetSourceCacheCapability(capability)
		return runSourceCacheSync(t, ctx, run, chaosTransportDirect, c1zPath, tmpDir, prevPath, WithWorkerCount(1))
	}

	// Generation A: cold seed; all four scopes consult and miss.
	eventsA := runGeneration("a", pathA, "")
	requireAllColdEvents(t, eventsA, 4)
	requireSourceCacheProduceState(t, readSourceCacheSnapshot(t, ctx, pathA, tmpDir),
		scGenEntries(0), scGenStamps())

	// Generation B: fully warm against A.
	eventsB := runGeneration("b", pathB, pathA)
	requireSourceCacheEvents(t, eventsB, scGenWarmEvents(scGenDeltaValidator(0)))
	requireSourceCacheProduceState(t, readSourceCacheSnapshot(t, ctx, pathB, tmpDir),
		scGenEntries(1), scGenStamps())

	// Generation C: fully warm against B. OR5a/OR5b — the consult trace
	// must be EXACTLY B's (modulo the rotated delta validator): any scope
	// whose entry B failed to republish surfaces here as a miss, and any
	// fresh fetch surfaces as a non-warm serve.
	eventsC := runGeneration("c", pathC, pathB)
	requireSourceCacheEvents(t, eventsC, scGenWarmEvents(scGenDeltaValidator(1)))
	// OR5c/OR5d — manifest parity with rotation/carry provenance: entry
	// KEYS are identical between B and C (zero-row scope included);
	// etag/ents/zero validators carry, the delta validator rotates.
	requireSourceCacheProduceState(t, readSourceCacheSnapshot(t, ctx, pathC, tmpDir),
		scGenEntries(2), scGenStamps())

	// OR2 — the upstream never changed, so every generation's content
	// equals one cold baseline.
	runGeneration("c", baselinePath, "")
	baselineContent := readChaosLogicalContent(t, ctx, baselinePath, tmpDir)
	for name, path := range map[string]string{"B": pathB, "C": pathC} {
		content := readChaosLogicalContent(t, ctx, path, tmpDir)
		require.NoErrorf(t, chaosoracle.CompareLogicalContent(baselineContent, content),
			"generation %s must equal the cold truth of the unchanged upstream", name)
	}
}

// TestChaosSourceCacheGenerationalLongChain extends R13's steady-state
// chain to six generations (nightly tier): the same four scopes must stay
// fully warm through every hop, the delta validator must rotate once per
// generation, and the final artifact must still equal the cold truth.
func TestChaosSourceCacheGenerationalLongChain(t *testing.T) {
	skipChaosInShort(t)
	testtier.RequireNightly(t)
	const generations = 6
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Second)
	defer cancel()
	tmpDir, paths := sourceCachePaths(t, generations+1)
	baselinePath := paths[generations]

	fx := newSCGenFixture(t)
	scenario := scGenScenario(t, fx, generations)
	capability := sourceCacheCapabilityRW("gen-1", "cfg-1")

	runGeneration := func(epoch, c1zPath, prevPath string) []chaosconnector.SourceCacheLookupEvent {
		run, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
		require.NoError(t, err)
		require.NoError(t, run.SetEpoch(epoch))
		run.SetSourceCacheCapability(capability)
		return runSourceCacheSync(t, ctx, run, chaosTransportDirect, c1zPath, tmpDir, prevPath, WithWorkerCount(1))
	}

	requireAllColdEvents(t, runGeneration(scGenEpochName(0), paths[0], ""), 4)
	for g := 1; g < generations; g++ {
		events := runGeneration(scGenEpochName(g), paths[g], paths[g-1])
		requireSourceCacheEvents(t, events, scGenWarmEvents(scGenDeltaValidator(g-1)))
		requireSourceCacheProduceState(t, readSourceCacheSnapshot(t, ctx, paths[g], tmpDir),
			scGenEntries(g), scGenStamps())
	}

	runGeneration(scGenEpochName(generations-1), baselinePath, "")
	require.NoError(t, chaosoracle.CompareLogicalContent(
		readChaosLogicalContent(t, ctx, baselinePath, tmpDir),
		readChaosLogicalContent(t, ctx, paths[generations-1], tmpDir)),
		"the final warm generation must equal the cold truth of the unchanged upstream")
}

// TestChaosSourceCacheGenerationalResumedBSeedsC pins the adversarial R13
// cell: a routine interruption-and-resume in generation B must NOT convert
// into a chain break — the resumed artifact publishes ingest quality
// without the replay-blocked flag and seeds a fully warm generation C.
func TestChaosSourceCacheGenerationalResumedBSeedsC(t *testing.T) {
	skipChaosInShort(t)
	ctx, cancel := context.WithTimeout(t.Context(), 120*time.Second)
	defer cancel()
	tmpDir, paths := sourceCachePaths(t, 4)
	pathA, pathB, pathC, baselinePath := paths[0], paths[1], paths[2], paths[3]

	fx := newSCGenFixture(t)
	scenario := scGenScenario(t, fx, 3)
	capability := sourceCacheCapabilityRW("gen-1", "cfg-1")

	seedRun, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
	require.NoError(t, err)
	seedRun.SetSourceCacheCapability(capability)
	requireAllColdEvents(t,
		runSourceCacheSync(t, ctx, seedRun, chaosTransportDirect, pathA, tmpDir, "", WithWorkerCount(1)), 4)

	// Generation B, interrupted at the first grants root request.
	interruptedRun, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule(chaosconnector.Rule{
		ID: "cut-first-grants-root",
		Match: chaosconnector.Matcher{
			Service:   chaosconnector.ExactString("GrantsService"),
			Method:    chaosconnector.ExactString("ListGrants"),
			PageToken: chaosconnector.ExactString(""),
			Attempt:   1,
			Phase:     chaosconnector.PhaseBeforeCall,
		},
		Effects:  []chaosconnector.Effect{{Kind: chaosconnector.EffectCrash}},
		MinFires: 1,
		MaxFires: 1,
	}))
	require.NoError(t, err)
	require.NoError(t, interruptedRun.SetEpoch("b"))
	interruptedRun.SetSourceCacheCapability(capability)
	interruptedHarness := newChaosHarness(t, ctx, interruptedRun, pathB, tmpDir, chaosTransportDirect,
		WithPreviousSyncC1ZPath(pathA), WithWorkerCount(1))
	require.ErrorIs(t, interruptedHarness.Syncer.Sync(ctx), chaosconnector.ErrInterruptRequested)
	require.NoError(t, interruptedHarness.Close(t.Context()))
	require.NoError(t, interruptedRun.Runtime().VerifyRequired())

	// Resume B to completion with a fresh syncer.
	resumeRun, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
	require.NoError(t, err)
	require.NoError(t, resumeRun.SetEpoch("b"))
	resumeRun.SetSourceCacheCapability(capability)
	runSourceCacheSync(t, ctx, resumeRun, chaosTransportDirect, pathB, tmpDir, pathA, WithWorkerCount(1))

	// The routine resume must publish quality WITHOUT the replay-blocked
	// flag — this is exactly the conservative default that must not turn
	// resumes into generational chain breaks.
	quality := readLifecycleIngestQuality(t, pathB)
	require.NotNil(t, quality, "resumed sync must publish ingest quality")
	require.False(t, quality.GetSourceCacheReplayBlocked(),
		"a routine resume must not mark the artifact replay-blocked")
	requireSourceCacheProduceState(t, readSourceCacheSnapshot(t, ctx, pathB, tmpDir),
		scGenEntries(1), scGenStamps())

	// Generation C over the resumed B: fully warm.
	genCRun, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
	require.NoError(t, err)
	require.NoError(t, genCRun.SetEpoch("c"))
	genCRun.SetSourceCacheCapability(capability)
	eventsC := runSourceCacheSync(t, ctx, genCRun, chaosTransportDirect, pathC, tmpDir, pathB, WithWorkerCount(1))
	requireSourceCacheEvents(t, eventsC, scGenWarmEvents(scGenDeltaValidator(1)))
	requireSourceCacheProduceState(t, readSourceCacheSnapshot(t, ctx, pathC, tmpDir),
		scGenEntries(2), scGenStamps())

	baselineRun, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
	require.NoError(t, err)
	require.NoError(t, baselineRun.SetEpoch("c"))
	baselineRun.SetSourceCacheCapability(capability)
	runSourceCacheSync(t, ctx, baselineRun, chaosTransportDirect, baselinePath, tmpDir, "", WithWorkerCount(1))
	require.NoError(t, chaosoracle.CompareLogicalContent(
		readChaosLogicalContent(t, ctx, baselinePath, tmpDir),
		readChaosLogicalContent(t, ctx, pathC, tmpDir)),
		"generation C over a resumed B must equal the cold truth")
}

// TestChaosSourceCacheGenerationalQualityLossBlocksC pins the second
// adversarial R13 cell: generation B suffers GENUINE quality loss (a
// dropped entitlement under the skip-report policy), so its artifact is
// marked replay-blocked and generation C must go fully cold — loudly
// consulted and missed, never silently blended.
func TestChaosSourceCacheGenerationalQualityLossBlocksC(t *testing.T) {
	skipChaosInShort(t)
	ctx, cancel := context.WithTimeout(t.Context(), 120*time.Second)
	defer cancel()
	tmpDir, paths := sourceCachePaths(t, 4)
	pathA, pathB, pathC, baselinePath := paths[0], paths[1], paths[2], paths[3]

	fx := newSCGenFixture(t)
	scenario := scGenScenario(t, fx, 3)
	capability := sourceCacheCapabilityRW("gen-1", "cfg-1")

	seedRun, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
	require.NoError(t, err)
	seedRun.SetSourceCacheCapability(capability)
	requireAllColdEvents(t,
		runSourceCacheSync(t, ctx, seedRun, chaosTransportDirect, pathA, tmpDir, "", WithWorkerCount(1)), 4)

	// Generation B against the lossy epoch: warm for the annotated scopes,
	// but the ghost entitlement is dropped and the artifact is marked
	// replay-blocked.
	lossyRun, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
	require.NoError(t, err)
	require.NoError(t, lossyRun.SetEpoch(scGenLossyEpochName))
	lossyRun.SetSourceCacheCapability(capability)
	eventsB := runSourceCacheSync(t, ctx, lossyRun, chaosTransportDirect, pathB, tmpDir, pathA, WithWorkerCount(1))
	requireSourceCacheEvents(t, eventsB, scGenWarmEvents(scGenDeltaValidator(0)))
	quality := readLifecycleIngestQuality(t, pathB)
	require.NotNil(t, quality)
	require.True(t, quality.GetSourceCacheReplayBlocked(),
		"vacuity guard: the lossy generation must actually be replay-blocked")

	// Generation C: the consume-side quality gate rejects B's artifact —
	// every scope consults and misses, and the sync refetches cold.
	genCRun, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
	require.NoError(t, err)
	require.NoError(t, genCRun.SetEpoch("c"))
	genCRun.SetSourceCacheCapability(capability)
	eventsC := runSourceCacheSync(t, ctx, genCRun, chaosTransportDirect, pathC, tmpDir, pathB, WithWorkerCount(1))
	requireAllColdEvents(t, eventsC, 4)
	requireSourceCacheProduceState(t, readSourceCacheSnapshot(t, ctx, pathC, tmpDir),
		scGenEntries(2), scGenStamps())

	baselineRun, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
	require.NoError(t, err)
	require.NoError(t, baselineRun.SetEpoch("c"))
	baselineRun.SetSourceCacheCapability(capability)
	runSourceCacheSync(t, ctx, baselineRun, chaosTransportDirect, baselinePath, tmpDir, "", WithWorkerCount(1))
	require.NoError(t, chaosoracle.CompareLogicalContent(
		readChaosLogicalContent(t, ctx, baselinePath, tmpDir),
		readChaosLogicalContent(t, ctx, pathC, tmpDir)),
		"the blocked generation must refetch to the cold truth")
}
