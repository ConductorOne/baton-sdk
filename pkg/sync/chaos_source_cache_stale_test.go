package sync //nolint:revive,nolintlint // backwards-compatible package name

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/internal/chaosconnector"
	chaosoracle "github.com/conductorone/baton-sdk/internal/chaosconnector/oracle"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/sourcecache"
	et "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	gt "github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
)

// Phase 6b degradation-composition suite (plan R14, oracles OR2 + OR1):
// per-scope degradations inside an otherwise healthy sync must compose —
// a stale validator or a poisoned scope colds THAT scope only, the sync
// stays green, and the artifact converges to the cold truth.

// TestChaosSourceCacheStaleValidatorFetchesFresh pins the stale-validator
// path: upstream changed between A and B, so B's lookup HITS but the stored
// validator no longer matches — the connector must fetch fresh, never
// replay. The warm branch is armed with a deliberately WRONG page (the old
// row set) so that serving it would fail the OR2 comparison.
func TestChaosSourceCacheStaleValidatorFetchesFresh(t *testing.T) {
	skipChaosInShort(t)
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	tmpDir, paths := sourceCachePaths(t, 3)
	seedPath, secondPath, baselinePath := paths[0], paths[1], paths[2]

	fx := newSCCollectionFixture(t)
	allThree := []*v2.Grant{fx.Grants[0], fx.Grants[1], fx.Grant3}

	seed := scCollectionBase(fx)
	seed.Grants["team-1"] = chaosconnector.Pages[*v2.Grant]{
		"": {List: fx.Grants, Annotations: scRecordAnno(scGrantsScopeKey, scValidatorV1)},
	}
	seed.SourceCacheGrants = map[string]*chaosconnector.SourceCacheSpec{
		"team-1": {ScopeKey: scGrantsScopeKey, Validator: scValidatorV1},
	}
	// Upstream changed: the second epoch's validator is v2 and its truth is
	// all three grants. The warm branch would replay the STALE base — the
	// mismatch must keep it unserved.
	second := scCollectionBase(fx)
	second.Grants["team-1"] = chaosconnector.Pages[*v2.Grant]{
		"":     {List: allThree, Annotations: scRecordAnno(scGrantsScopeKey, scValidatorV2)},
		"warm": {Annotations: scReplayAnno(scGrantsScopeKey, scValidatorV1, false, nil, nil)},
	}
	second.SourceCacheGrants = map[string]*chaosconnector.SourceCacheSpec{
		"team-1": {ScopeKey: scGrantsScopeKey, Validator: scValidatorV2, WarmRoot: "warm"},
	}
	scenario := &chaosconnector.Scenario{
		Name:         "source-cache-stale-validator",
		Seed:         1,
		InitialEpoch: "seed",
		Epochs:       map[string]*chaosconnector.Dataset{"seed": seed, "second": second},
	}
	require.NoError(t, scenario.Validate())
	capability := sourceCacheCapabilityRW("gen-1", "cfg-1")

	run, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
	require.NoError(t, err)
	run.SetSourceCacheCapability(capability)
	seedEvents := runSourceCacheSync(t, ctx, run, chaosTransportDirect, seedPath, tmpDir, "", WithWorkerCount(1))
	requireAllColdEvents(t, seedEvents, 1)

	require.NoError(t, run.SetEpoch("second"))
	secondEvents := runSourceCacheSync(t, ctx, run, chaosTransportDirect, secondPath, tmpDir, seedPath, WithWorkerCount(1))
	requireSourceCacheEvents(t, secondEvents, []chaosconnector.SourceCacheLookupEvent{{
		RowKind:           sourcecache.RowKindGrants,
		ScopeKey:          scGrantsScopeKey,
		Hit:               true,
		PreviousValidator: scValidatorV1,
		Matched:           false,
		ServedWarm:        false,
	}})

	baselineRun, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
	require.NoError(t, err)
	require.NoError(t, baselineRun.SetEpoch("second"))
	baselineRun.SetSourceCacheCapability(capability)
	runSourceCacheSync(t, ctx, baselineRun, chaosTransportDirect, baselinePath, tmpDir, "", WithWorkerCount(1))

	secondContent := readChaosLogicalContent(t, ctx, secondPath, tmpDir)
	baselineContent := readChaosLogicalContent(t, ctx, baselinePath, tmpDir)
	require.NoError(t, chaosoracle.CompareLogicalContent(baselineContent, secondContent),
		"stale-validator sync must converge to the cold truth of the changed upstream")

	requireSourceCacheProduceState(t, readSourceCacheSnapshot(t, ctx, secondPath, tmpDir),
		map[string]string{grantsKindScope(): scValidatorV2},
		map[string]int{grantsKindScope(): 3})
}

// TestChaosSourceCachePoisonedScopeColdInsideWarmSync pins the CO-016 shape
// (plan B9 + R14): the seed sync's team-1 page tombstones a row stamped
// with team-2's scope — a row-partition violation that durably poisons
// team-2's scope in the seed artifact (CO-015). The next sync is otherwise
// warm: team-1 replays, but team-2's lookup must MISS (poison reads as
// miss) and cold-fetch, all inside one green sync.
//
// The single-worker grants phase processes team-2 BEFORE team-1 (task-stack
// order), so team-1's page is the one whose tombstone can reach an
// already-ingested foreign row.
func TestChaosSourceCachePoisonedScopeColdInsideWarmSync(t *testing.T) {
	skipChaosInShort(t)
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	tmpDir, paths := sourceCachePaths(t, 3)
	seedPath, secondPath, baselinePath := paths[0], paths[1], paths[2]

	const (
		scope1     = "grants:team-1"
		scope2     = "grants:team-2"
		validator1 = "validator-team-1"
		validator2 = "validator-team-2"
	)

	fx := newSCCollectionFixture(t)
	team2, err := rs.NewGroupResource("Team 2", fx.TeamType, "team-2", nil)
	require.NoError(t, err)
	member2 := et.NewEntitlement(team2, "member", "assignment")
	h1 := gt.NewGrant(team2, "member", fx.Users[1])

	buildEpoch := func(seedShape bool) *chaosconnector.Dataset {
		d := scCollectionBase(fx)
		d.Resources[scTeamTypeID] = chaosconnector.Pages[*v2.Resource]{
			"": {List: []*v2.Resource{fx.Team, team2}},
		}
		d.Entitlements["team-2"] = chaosconnector.Pages[*v2.Entitlement]{
			"": {List: []*v2.Entitlement{member2}},
		}
		if seedShape {
			// Team-2 serves h1 first; team-1's page then tombstones h1
			// while acting FOR scope1 — deleting a scope2-stamped row, the
			// poison trigger.
			d.Grants["team-1"] = chaosconnector.Pages[*v2.Grant]{
				"": {
					List: fx.Grants,
					Annotations: annotations.New(v2.SourceCacheRecord_builder{
						ScopeKey:       scope1,
						CacheValidator: validator1,
						DeletedIds:     []string{h1.GetId()},
					}.Build()),
				},
			}
			d.Grants["team-2"] = chaosconnector.Pages[*v2.Grant]{
				"": {List: []*v2.Grant{h1}, Annotations: scRecordAnno(scope2, validator2)},
			}
			d.SourceCacheGrants = map[string]*chaosconnector.SourceCacheSpec{
				"team-1": {ScopeKey: scope1, Validator: validator1},
				"team-2": {ScopeKey: scope2, Validator: validator2},
			}
			return d
		}
		// Second epoch: upstream unchanged (h1 remains gone). Both scopes
		// declare warm branches; only team-1's may be served.
		d.Grants["team-1"] = chaosconnector.Pages[*v2.Grant]{
			"":     {List: fx.Grants, Annotations: scRecordAnno(scope1, validator1)},
			"warm": {Annotations: scReplayAnno(scope1, validator1, false, nil, nil)},
		}
		d.Grants["team-2"] = chaosconnector.Pages[*v2.Grant]{
			"":     {Annotations: scRecordAnno(scope2, validator2)},
			"warm": {Annotations: scReplayAnno(scope2, validator2, false, nil, nil)},
		}
		d.SourceCacheGrants = map[string]*chaosconnector.SourceCacheSpec{
			"team-1": {ScopeKey: scope1, Validator: validator1, WarmRoot: "warm"},
			"team-2": {ScopeKey: scope2, Validator: validator2, WarmRoot: "warm"},
		}
		return d
	}

	scenario := &chaosconnector.Scenario{
		Name:         "source-cache-poisoned-scope",
		Seed:         1,
		InitialEpoch: "seed",
		Epochs: map[string]*chaosconnector.Dataset{
			"seed":   buildEpoch(true),
			"second": buildEpoch(false),
		},
	}
	require.NoError(t, scenario.Validate())
	capability := sourceCacheCapabilityRW("gen-1", "cfg-1")

	run, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
	require.NoError(t, err)
	run.SetSourceCacheCapability(capability)
	seedEvents := runSourceCacheSync(t, ctx, run, chaosTransportDirect, seedPath, tmpDir, "", WithWorkerCount(1))
	requireAllColdEvents(t, seedEvents, 2)

	require.NoError(t, run.SetEpoch("second"))
	secondEvents := runSourceCacheSync(t, ctx, run, chaosTransportDirect, secondPath, tmpDir, seedPath, WithWorkerCount(1))
	requireSourceCacheEvents(t, secondEvents, []chaosconnector.SourceCacheLookupEvent{
		{
			// Team-2 (served first): poisoned in the seed artifact — the
			// lookup must read as a MISS even though the entry exists with
			// a live validator.
			RowKind:  sourcecache.RowKindGrants,
			ScopeKey: scope2,
		},
		scWarmEventFor(sourcecache.RowKindGrants, scope1, validator1),
	})

	baselineRun, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
	require.NoError(t, err)
	require.NoError(t, baselineRun.SetEpoch("second"))
	baselineRun.SetSourceCacheCapability(capability)
	runSourceCacheSync(t, ctx, baselineRun, chaosTransportDirect, baselinePath, tmpDir, "", WithWorkerCount(1))

	secondContent := readChaosLogicalContent(t, ctx, secondPath, tmpDir)
	baselineContent := readChaosLogicalContent(t, ctx, baselinePath, tmpDir)
	require.NoError(t, chaosoracle.CompareLogicalContent(baselineContent, secondContent),
		"poison-composed sync must converge to the cold truth")

	// Both scopes republish; the poisoned scope's fresh zero-row fetch
	// re-seeds it cleanly (the poison lived in the PREVIOUS artifact, not
	// this one).
	requireSourceCacheProduceState(t, readSourceCacheSnapshot(t, ctx, secondPath, tmpDir),
		map[string]string{
			chaosoracle.KindScope(sourcecache.RowKindGrants, scope1): validator1,
			chaosoracle.KindScope(sourcecache.RowKindGrants, scope2): validator2,
		},
		map[string]int{
			chaosoracle.KindScope(sourcecache.RowKindGrants, scope1): 2,
		})
}
