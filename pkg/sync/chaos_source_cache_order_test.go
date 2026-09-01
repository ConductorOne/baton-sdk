package sync //nolint:revive,nolintlint // backwards-compatible package name

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/conductorone/baton-sdk/internal/chaosconnector"
	chaosoracle "github.com/conductorone/baton-sdk/internal/chaosconnector/oracle"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/sourcecache"
)

// Phase 6b ordering/pagination adversary suite (plan R12, oracles OR2 +
// OR1 + OR3): retries, lost responses, epoch drift between retries, and
// duplicate replay cursors must all preserve equivalence with the cold
// truth of whatever epoch ultimately answered.
func TestChaosSourceCacheOrderingAdversary(t *testing.T) {
	skipChaosInShort(t)

	warmMatchedV1 := scWarmEventFor(sourcecache.RowKindGrants, scGrantsScopeKey, scValidatorV1)

	cells := []struct {
		name string
		// epochs builds the scenario's epoch map; generation B always
		// starts in "second".
		epochs func(fx *scCollectionFixture) map[string]*chaosconnector.Dataset
		// schedule is generation B's fault program.
		schedule chaosconnector.Schedule
		// baselineEpoch is the epoch whose independent cold sync is the
		// OR2 truth for generation B's final artifact.
		baselineEpoch string
		wantEvents    []chaosconnector.SourceCacheLookupEvent
	}{
		{
			// A lost response on the warm root: the connector served the
			// warm page (consult hit, warm decision made) but the syncer
			// never saw it. The retry re-consults — the warm branch must
			// not be "burned" by a response that never landed — and the
			// replay round completes normally on attempt two.
			name: "lost-response-reconsults",
			epochs: func(fx *scCollectionFixture) map[string]*chaosconnector.Dataset {
				allThree := []*v2.Grant{fx.Grants[0], fx.Grants[1], fx.Grant3}
				seed := scCollectionBase(fx)
				seed.Grants["team-1"] = chaosconnector.Pages[*v2.Grant]{
					"": {List: fx.Grants, Annotations: scRecordAnno(scGrantsScopeKey, scValidatorV1)},
				}
				seed.SourceCacheGrants = map[string]*chaosconnector.SourceCacheSpec{
					"team-1": {ScopeKey: scGrantsScopeKey, Validator: scValidatorV1},
				}
				second := scCollectionBase(fx)
				second.Grants["team-1"] = chaosconnector.Pages[*v2.Grant]{
					"": {List: allThree, Annotations: scRecordAnno(scGrantsScopeKey, scValidatorV2)},
					"warm": {
						List:        []*v2.Grant{fx.Grant3},
						Annotations: scReplayAnno(scGrantsScopeKey, scValidatorV2, true, nil, nil),
					},
				}
				second.SourceCacheGrants = map[string]*chaosconnector.SourceCacheSpec{
					"team-1": {ScopeKey: scGrantsScopeKey, Validator: scValidatorV1, WarmRoot: "warm"},
				}
				return map[string]*chaosconnector.Dataset{"seed": seed, "second": second}
			},
			schedule: chaosconnector.NewSchedule(chaosconnector.Rule{
				ID: "lose-warm-root",
				Match: chaosconnector.Matcher{
					Service:   chaosconnector.ExactString("GrantsService"),
					Method:    chaosconnector.ExactString("ListGrants"),
					PageToken: chaosconnector.ExactString(""),
					Attempt:   1,
					Phase:     chaosconnector.PhaseAfterDelegate,
				},
				Effects: []chaosconnector.Effect{{
					Kind:    chaosconnector.EffectLoseResponse,
					Code:    codes.Unavailable,
					Message: "warm root lost after delegate",
				}},
				MinFires: 1,
				MaxFires: 1,
			}),
			baselineEpoch: "second",
			wantEvents: []chaosconnector.SourceCacheLookupEvent{
				warmMatchedV1,
				warmMatchedV1,
			},
		},
		{
			// Epoch drift between retries (the temporal-corpus shape
			// composed with replay): attempt one answers warm from the
			// "second" epoch but the response is lost and the upstream
			// moves to "drift", whose validator no longer matches the
			// stored entry. The retry's consult must re-decide — stale,
			// fresh fetch — and the artifact must equal the DRIFT epoch's
			// cold truth, never a blend of both answers.
			name: "epoch-drift-between-retries",
			epochs: func(fx *scCollectionFixture) map[string]*chaosconnector.Dataset {
				allThree := []*v2.Grant{fx.Grants[0], fx.Grants[1], fx.Grant3}
				seed := scCollectionBase(fx)
				seed.Grants["team-1"] = chaosconnector.Pages[*v2.Grant]{
					"": {List: fx.Grants, Annotations: scRecordAnno(scGrantsScopeKey, scValidatorV1)},
				}
				seed.SourceCacheGrants = map[string]*chaosconnector.SourceCacheSpec{
					"team-1": {ScopeKey: scGrantsScopeKey, Validator: scValidatorV1},
				}
				second := scCollectionBase(fx)
				second.Grants["team-1"] = chaosconnector.Pages[*v2.Grant]{
					"": {List: fx.Grants, Annotations: scRecordAnno(scGrantsScopeKey, scValidatorV1)},
					"warm": {
						Annotations: scReplayAnno(scGrantsScopeKey, scValidatorV1, false, nil, nil),
					},
				}
				second.SourceCacheGrants = map[string]*chaosconnector.SourceCacheSpec{
					"team-1": {ScopeKey: scGrantsScopeKey, Validator: scValidatorV1, WarmRoot: "warm"},
				}
				drift := scCollectionBase(fx)
				drift.Grants["team-1"] = chaosconnector.Pages[*v2.Grant]{
					"": {List: allThree, Annotations: scRecordAnno(scGrantsScopeKey, scValidatorV2)},
					"warm": {
						Annotations: scReplayAnno(scGrantsScopeKey, scValidatorV1, false, nil, nil),
					},
				}
				drift.SourceCacheGrants = map[string]*chaosconnector.SourceCacheSpec{
					"team-1": {ScopeKey: scGrantsScopeKey, Validator: scValidatorV2, WarmRoot: "warm"},
				}
				return map[string]*chaosconnector.Dataset{"seed": seed, "second": second, "drift": drift}
			},
			schedule: chaosconnector.NewSchedule(chaosconnector.Rule{
				ID: "drift-between-retries",
				Match: chaosconnector.Matcher{
					Service:   chaosconnector.ExactString("GrantsService"),
					Method:    chaosconnector.ExactString("ListGrants"),
					PageToken: chaosconnector.ExactString(""),
					Attempt:   1,
					Phase:     chaosconnector.PhaseAfterDelegate,
				},
				Effects: []chaosconnector.Effect{
					{Kind: chaosconnector.EffectSetEpoch, Epoch: "drift"},
					{Kind: chaosconnector.EffectLoseResponse, Code: codes.Unavailable, Message: "first answer lost"},
				},
				MinFires: 1,
				MaxFires: 1,
			}),
			baselineEpoch: "drift",
			wantEvents: []chaosconnector.SourceCacheLookupEvent{
				warmMatchedV1,
				{
					RowKind:           sourcecache.RowKindGrants,
					ScopeKey:          scGrantsScopeKey,
					Hit:               true,
					PreviousValidator: scValidatorV1,
					Matched:           false,
				},
			},
		},
		{
			// Duplicate replay annotations across SPAWNED cursors: the warm
			// root fans out two extra cursors that each carry another
			// replay annotation for the same scope. The copy must run only
			// once (a second replacement copy would wipe the overlay
			// upsert g3, which OR2 catches), and the last replay page's
			// validator wins the manifest entry.
			name: "spawned-duplicate-replay-cursors",
			epochs: func(fx *scCollectionFixture) map[string]*chaosconnector.Dataset {
				allThree := []*v2.Grant{fx.Grants[0], fx.Grants[1], fx.Grant3}
				seed := scCollectionBase(fx)
				seed.Grants["team-1"] = chaosconnector.Pages[*v2.Grant]{
					"": {List: fx.Grants, Annotations: scRecordAnno(scGrantsScopeKey, scValidatorV1)},
				}
				seed.SourceCacheGrants = map[string]*chaosconnector.SourceCacheSpec{
					"team-1": {ScopeKey: scGrantsScopeKey, Validator: scValidatorV1},
				}
				second := scCollectionBase(fx)
				second.Grants["team-1"] = chaosconnector.Pages[*v2.Grant]{
					"": {List: allThree, Annotations: scRecordAnno(scGrantsScopeKey, scValidatorV2)},
					"warm": {
						List:        []*v2.Grant{fx.Grant3},
						Annotations: scReplayAnno(scGrantsScopeKey, "", true, nil, nil),
						Spawn:       []string{"dup-a", "dup-b"},
					},
					"dup-a": {Annotations: scReplayAnno(scGrantsScopeKey, "", true, nil, nil)},
					"dup-b": {Annotations: scReplayAnno(scGrantsScopeKey, scValidatorV2, true, nil, nil)},
				}
				second.SourceCacheGrants = map[string]*chaosconnector.SourceCacheSpec{
					"team-1": {ScopeKey: scGrantsScopeKey, Validator: scValidatorV1, WarmRoot: "warm"},
				}
				return map[string]*chaosconnector.Dataset{"seed": seed, "second": second}
			},
			schedule:      chaosconnector.NewSchedule(),
			baselineEpoch: "second",
			wantEvents:    []chaosconnector.SourceCacheLookupEvent{warmMatchedV1},
		},
	}

	for _, tc := range cells {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
			defer cancel()
			tmpDir, paths := sourceCachePaths(t, 3)
			seedPath, warmPath, baselinePath := paths[0], paths[1], paths[2]

			fx := newSCCollectionFixture(t)
			scenario := &chaosconnector.Scenario{
				Name:         "source-cache-ordering-" + tc.name,
				Seed:         1,
				InitialEpoch: "seed",
				Epochs:       tc.epochs(fx),
			}
			require.NoError(t, scenario.Validate())
			capability := sourceCacheCapabilityRW("gen-1", "cfg-1")

			seedRun, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
			require.NoError(t, err)
			seedRun.SetSourceCacheCapability(capability)
			seedEvents := runSourceCacheSync(t, ctx, seedRun, chaosTransportDirect, seedPath, tmpDir, "", WithWorkerCount(1))
			requireAllColdEvents(t, seedEvents, 1)

			// Generation B under the adversarial schedule.
			warmRun, err := chaosconnector.NewRun(scenario, tc.schedule)
			require.NoError(t, err)
			require.NoError(t, warmRun.SetEpoch("second"))
			warmRun.SetSourceCacheCapability(capability)
			warmEvents := runSourceCacheSync(t, ctx, warmRun, chaosTransportDirect, warmPath, tmpDir, seedPath, WithWorkerCount(1))
			requireSourceCacheEvents(t, warmEvents, tc.wantEvents)

			// OR2 against the cold truth of the answering epoch.
			baselineRun, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
			require.NoError(t, err)
			require.NoError(t, baselineRun.SetEpoch(tc.baselineEpoch))
			baselineRun.SetSourceCacheCapability(capability)
			runSourceCacheSync(t, ctx, baselineRun, chaosTransportDirect, baselinePath, tmpDir, "", WithWorkerCount(1))
			warmContent := readChaosLogicalContent(t, ctx, warmPath, tmpDir)
			baselineContent := readChaosLogicalContent(t, ctx, baselinePath, tmpDir)
			require.NoError(t, chaosoracle.CompareLogicalContent(baselineContent, warmContent),
				"adversarial sync must equal the answering epoch's cold truth")

			// OR3: every cell ends at validator v2 over exactly 3 stamped
			// rows regardless of the path taken to get there.
			requireSourceCacheProduceState(t, readSourceCacheSnapshot(t, ctx, warmPath, tmpDir),
				map[string]string{grantsKindScope(): scValidatorV2},
				map[string]int{grantsKindScope(): 3})
		})
	}
}

// TestChaosSourceCacheDuplicateReplayCursorsParallel re-runs the
// spawned-duplicate-replay-cursors shape at WithWorkerCount(4) (CO-6b-003):
// the once-per-scope replay guard is a decide-copy-mark sequence that must
// stay atomic when duplicate replay cursors drain on CONCURRENT workers — a
// second replacement copy racing past the guard would wipe the overlay
// upsert g3, which the OR2 differential catches. Run under -race in the
// focused race gate.
func TestChaosSourceCacheDuplicateReplayCursorsParallel(t *testing.T) {
	skipChaosInShort(t)
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	tmpDir, paths := sourceCachePaths(t, 3)
	seedPath, warmPath, baselinePath := paths[0], paths[1], paths[2]

	fx := newSCCollectionFixture(t)
	allThree := []*v2.Grant{fx.Grants[0], fx.Grants[1], fx.Grant3}
	seed := scCollectionBase(fx)
	seed.Grants["team-1"] = chaosconnector.Pages[*v2.Grant]{
		"": {List: fx.Grants, Annotations: scRecordAnno(scGrantsScopeKey, scValidatorV1)},
	}
	seed.SourceCacheGrants = map[string]*chaosconnector.SourceCacheSpec{
		"team-1": {ScopeKey: scGrantsScopeKey, Validator: scValidatorV1},
	}
	second := scCollectionBase(fx)
	second.Grants["team-1"] = chaosconnector.Pages[*v2.Grant]{
		"": {List: allThree, Annotations: scRecordAnno(scGrantsScopeKey, scValidatorV2)},
		"warm": {
			List:        []*v2.Grant{fx.Grant3},
			Annotations: scReplayAnno(scGrantsScopeKey, "", true, nil, nil),
			Spawn:       []string{"dup-a", "dup-b"},
		},
		"dup-a": {Annotations: scReplayAnno(scGrantsScopeKey, "", true, nil, nil)},
		"dup-b": {Annotations: scReplayAnno(scGrantsScopeKey, scValidatorV2, true, nil, nil)},
	}
	second.SourceCacheGrants = map[string]*chaosconnector.SourceCacheSpec{
		"team-1": {ScopeKey: scGrantsScopeKey, Validator: scValidatorV1, WarmRoot: "warm"},
	}
	scenario := &chaosconnector.Scenario{
		Name:         "source-cache-duplicate-replay-parallel",
		Seed:         1,
		InitialEpoch: "seed",
		Epochs:       map[string]*chaosconnector.Dataset{"seed": seed, "second": second},
	}
	require.NoError(t, scenario.Validate())
	capability := sourceCacheCapabilityRW("gen-1", "cfg-1")

	seedRun, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
	require.NoError(t, err)
	seedRun.SetSourceCacheCapability(capability)
	requireAllColdEvents(t,
		runSourceCacheSync(t, ctx, seedRun, chaosTransportDirect, seedPath, tmpDir, "", WithWorkerCount(1)), 1)

	warmRun, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
	require.NoError(t, err)
	require.NoError(t, warmRun.SetEpoch("second"))
	warmRun.SetSourceCacheCapability(capability)
	warmEvents := runSourceCacheSync(t, ctx, warmRun, chaosTransportDirect, warmPath, tmpDir, seedPath, WithWorkerCount(4))
	requireSourceCacheEvents(t, warmEvents,
		[]chaosconnector.SourceCacheLookupEvent{scWarmEventFor(sourcecache.RowKindGrants, scGrantsScopeKey, scValidatorV1)})

	baselineRun, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
	require.NoError(t, err)
	require.NoError(t, baselineRun.SetEpoch("second"))
	baselineRun.SetSourceCacheCapability(capability)
	runSourceCacheSync(t, ctx, baselineRun, chaosTransportDirect, baselinePath, tmpDir, "", WithWorkerCount(1))
	require.NoError(t, chaosoracle.CompareLogicalContent(
		readChaosLogicalContent(t, ctx, baselinePath, tmpDir),
		readChaosLogicalContent(t, ctx, warmPath, tmpDir)),
		"parallel duplicate replay cursors must equal the cold truth (a second replacement copy would wipe g3)")
	requireSourceCacheProduceState(t, readSourceCacheSnapshot(t, ctx, warmPath, tmpDir),
		map[string]string{grantsKindScope(): scValidatorV2},
		map[string]int{grantsKindScope(): 3})
}
