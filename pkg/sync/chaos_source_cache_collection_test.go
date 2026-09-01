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
	"github.com/conductorone/baton-sdk/pkg/bid"
	"github.com/conductorone/baton-sdk/pkg/sourcecache"
	et "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	gt "github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
)

// Phase 6b collection-semantics suite (plan R7 + the R3 replay-adjacent
// cells, oracles OR2 + OR3): each cell runs generation A as the cold seed,
// switches the connector to the "second" epoch, runs generation B warm
// against A's artifact, and then checks two things:
//
//   - OR2: B's logical content equals an INDEPENDENT cold baseline of the
//     second epoch (a fresh run served the cold branch, never a lookup hit).
//     Every ordering property — copy before upserts, tombstones after
//     upserts, replacement wiping earlier fresh rows, dedup skipping the
//     second copy — is arranged so that violating it changes final content.
//   - OR3: B's artifact carries exactly the expected manifest entries
//     (validator values) and per-scope stamp counts; nothing more.
//
// R3's cold-only fresh-page cells (multi-page rounds, same-page
// put-then-tombstone, empty-validator rounds, stamping exactness, the
// static-entitlement exclusion) live in TestChaosSourceCacheFreshPageSemantics.

const (
	scValidatorV2     = "validator-v2"
	scResourceScope   = "resources:sc-user"
	scSharedScope     = "shared:team-1"
	scSharedValidator = "validator-shared"
)

// scCollectionFixture extends the reference fixture with a third user and a
// grant to it, the "changed upstream" material for delta cells.
type scCollectionFixture struct {
	*scFixture
	User3  *v2.Resource
	Grant3 *v2.Grant
}

func newSCCollectionFixture(t *testing.T) *scCollectionFixture {
	t.Helper()
	fixture := newSourceCacheFixture(t)
	user3, err := rs.NewUserResource("User 3", fixture.UserType, "user-3", nil)
	require.NoError(t, err)
	return &scCollectionFixture{
		scFixture: fixture,
		User3:     user3,
		Grant3:    gt.NewGrant(fixture.Team, "member", user3),
	}
}

// scCollectionBase is the epoch skeleton shared by every cell: both types,
// the team, all three users, the member entitlement (unannotated), and no
// grants pages — cells attach their own scoped page graphs.
func scCollectionBase(fx *scCollectionFixture) *chaosconnector.Dataset {
	return &chaosconnector.Dataset{
		ResourceTypes: []*v2.ResourceType{fx.TeamType, fx.UserType},
		Resources: map[string]chaosconnector.Pages[*v2.Resource]{
			scTeamTypeID: {"": {List: []*v2.Resource{fx.Team}}},
			scUserTypeID: {"": {List: []*v2.Resource{fx.Users[0], fx.Users[1], fx.User3}}},
		},
		StaticEntitlements: map[string]chaosconnector.Pages[*v2.Entitlement]{
			scTeamTypeID: {"": {}},
			scUserTypeID: {"": {}},
		},
		Entitlements: map[string]chaosconnector.Pages[*v2.Entitlement]{
			"team-1": {"": {List: []*v2.Entitlement{fx.Member}}},
		},
		Grants: map[string]chaosconnector.Pages[*v2.Grant]{
			"team-1": {"": {}},
		},
	}
}

func scRecordAnno(scopeKey, validator string) annotations.Annotations {
	return annotations.New(v2.SourceCacheRecord_builder{
		ScopeKey:       scopeKey,
		CacheValidator: validator,
	}.Build())
}

func scReplayAnno(scopeKey, validator string, overlay bool, deletedIDs, deletedPrincipalIDs []string) annotations.Annotations {
	return annotations.New(v2.SourceCacheReplay_builder{
		ScopeKey:            scopeKey,
		CacheValidator:      validator,
		Overlay:             overlay,
		DeletedIds:          deletedIDs,
		DeletedPrincipalIds: deletedPrincipalIDs,
	}.Build())
}

// scWarmEventFor builds the expected warm-serve event for one consult.
func scWarmEventFor(kind sourcecache.RowKind, scopeKey, previousValidator string) chaosconnector.SourceCacheLookupEvent {
	return chaosconnector.SourceCacheLookupEvent{
		RowKind:           kind,
		ScopeKey:          scopeKey,
		Hit:               true,
		PreviousValidator: previousValidator,
		Matched:           true,
		ServedWarm:        true,
	}
}

// requireAllColdEvents asserts every consult in a cold sync missed.
func requireAllColdEvents(t *testing.T, events []chaosconnector.SourceCacheLookupEvent, want int) {
	t.Helper()
	require.Len(t, events, want)
	for i, event := range events {
		require.Falsef(t, event.LookupWasNil, "event %d: lookup was nil", i)
		require.Emptyf(t, event.LookupError, "event %d: lookup errored", i)
		require.Falsef(t, event.Hit, "event %d: cold sync must miss every lookup", i)
		require.Falsef(t, event.ServedWarm, "event %d: cold sync must never serve warm", i)
	}
}

// requireSourceCacheProduceState asserts OR3 exactly: the artifact's full
// manifest-entry map (validators, nothing invalidated) and full per-scope
// stamp-count map equal the expectation — unexpected scopes are failures,
// not noise.
func requireSourceCacheProduceState(
	t *testing.T,
	snapshot chaosoracle.SourceCacheSnapshot,
	wantEntries map[string]string,
	wantStamps map[string]int,
) {
	t.Helper()
	gotEntries := map[string]string{}
	for key, entry := range snapshot.Entries {
		require.Falsef(t, entry.Invalidated, "manifest entry %q is invalidated", key)
		gotEntries[key] = entry.Validator
	}
	require.Equal(t, wantEntries, gotEntries, "manifest entries (validator by kind+scope)")

	gotStamps := map[string]int{}
	for key, count := range snapshot.StampCounts {
		gotStamps[key] = count
	}
	require.Equal(t, wantStamps, gotStamps, "stamped-row counts by kind+scope")
	require.NotNil(t, snapshot.Compat, "capable sync must write the compat record")
}

func TestChaosSourceCacheCollectionSemantics(t *testing.T) {
	skipChaosInShort(t)

	grantsScope := chaosoracle.KindScope(sourcecache.RowKindGrants, scGrantsScopeKey)

	type cell struct {
		name string
		// build returns the two-epoch scenario ("seed" initial, "second").
		build func(t *testing.T, fx *scCollectionFixture) *chaosconnector.Scenario
		// seedConsults is the number of lookup consults generation A makes.
		seedConsults int
		// wantWarmEvents is generation B's exact serve-order event list.
		wantWarmEvents func(fx *scCollectionFixture) []chaosconnector.SourceCacheLookupEvent
		// wantEntries / wantStamps are B's exact OR3 produce-side state.
		wantEntries func(fx *scCollectionFixture) map[string]string
		wantStamps  func(fx *scCollectionFixture) map[string]int
	}

	// twoEpoch assembles the scenario given each epoch's grants pages+spec.
	grantsScenario := func(
		t *testing.T,
		fx *scCollectionFixture,
		seedPages, secondPages chaosconnector.Pages[*v2.Grant],
		seedSpec, secondSpec *chaosconnector.SourceCacheSpec,
	) *chaosconnector.Scenario {
		t.Helper()
		seed := scCollectionBase(fx)
		seed.Grants["team-1"] = seedPages
		seed.SourceCacheGrants = map[string]*chaosconnector.SourceCacheSpec{"team-1": seedSpec}
		second := scCollectionBase(fx)
		second.Grants["team-1"] = secondPages
		second.SourceCacheGrants = map[string]*chaosconnector.SourceCacheSpec{"team-1": secondSpec}
		scenario := &chaosconnector.Scenario{
			Name:         "source-cache-collection",
			Seed:         1,
			InitialEpoch: "seed",
			Epochs:       map[string]*chaosconnector.Dataset{"seed": seed, "second": second},
		}
		require.NoError(t, scenario.Validate())
		return scenario
	}

	// seedGrantsV1 is the standard generation-A round: both grants fresh,
	// validator v1, no warm branch (a seed sync always serves cold).
	seedGrantsV1 := func(fx *scCollectionFixture) chaosconnector.Pages[*v2.Grant] {
		return chaosconnector.Pages[*v2.Grant]{
			"": {List: fx.Grants, Annotations: scRecordAnno(scGrantsScopeKey, scValidatorV1)},
		}
	}
	specV1 := func(warmRoot string) *chaosconnector.SourceCacheSpec {
		return &chaosconnector.SourceCacheSpec{
			ScopeKey:  scGrantsScopeKey,
			Validator: scValidatorV1,
			WarmRoot:  warmRoot,
		}
	}
	warmGrantsEvent := func(fx *scCollectionFixture) []chaosconnector.SourceCacheLookupEvent {
		return []chaosconnector.SourceCacheLookupEvent{
			scWarmEventFor(sourcecache.RowKindGrants, scGrantsScopeKey, scValidatorV1),
		}
	}

	cells := []cell{
		{
			// Delta overlay + both tombstone families in one page: the
			// replayed base (g1, g2) is trimmed by a canonical-id tombstone
			// (g2) and a principal tombstone (user-1 kills g1) AFTER the
			// page's fresh upsert (g3). Upstream truth is [g3]; any
			// ordering violation leaves a replayed corpse behind and OR2
			// catches it.
			name: "grants-delta-overlay-tombstones",
			build: func(t *testing.T, fx *scCollectionFixture) *chaosconnector.Scenario {
				return grantsScenario(t, fx,
					seedGrantsV1(fx),
					chaosconnector.Pages[*v2.Grant]{
						"": {List: []*v2.Grant{fx.Grant3}, Annotations: scRecordAnno(scGrantsScopeKey, scValidatorV2)},
						"warm": {
							List: []*v2.Grant{fx.Grant3},
							Annotations: scReplayAnno(scGrantsScopeKey, scValidatorV2, true,
								[]string{fx.Grants[1].GetId()}, []string{"user-1"}),
						},
					},
					specV1(""), specV1("warm"),
				)
			},
			seedConsults:   1,
			wantWarmEvents: warmGrantsEvent,
			wantEntries: func(fx *scCollectionFixture) map[string]string {
				return map[string]string{grantsScope: scValidatorV2}
			},
			wantStamps: func(fx *scCollectionFixture) map[string]int {
				return map[string]int{grantsScope: 1}
			},
		},
		{
			// Replacement across pages: a fresh scoped page lands g3, then
			// a later replay page for the same scope arrives. Replay is
			// replacement — the copy must wipe the earlier fresh row and
			// restore the base [g1, g2], which is the second epoch's truth.
			name: "grants-replacement-cross-page",
			build: func(t *testing.T, fx *scCollectionFixture) *chaosconnector.Scenario {
				return grantsScenario(t, fx,
					seedGrantsV1(fx),
					chaosconnector.Pages[*v2.Grant]{
						"": {List: fx.Grants, Annotations: scRecordAnno(scGrantsScopeKey, scValidatorV1)},
						"warm": {
							List:        []*v2.Grant{fx.Grant3},
							Annotations: scRecordAnno(scGrantsScopeKey, ""),
							Next:        "warm-2",
						},
						"warm-2": {Annotations: scReplayAnno(scGrantsScopeKey, scValidatorV1, false, nil, nil)},
					},
					specV1(""), specV1("warm"),
				)
			},
			seedConsults:   1,
			wantWarmEvents: warmGrantsEvent,
			wantEntries: func(fx *scCollectionFixture) map[string]string {
				return map[string]string{grantsScope: scValidatorV1}
			},
			wantStamps: func(fx *scCollectionFixture) map[string]int {
				return map[string]int{grantsScope: 2}
			},
		},
		{
			// Copy dedup: two replay pages for one scope in one round. The
			// second copy must be SKIPPED — re-running it would wipe the
			// first page's overlay upsert (g3) by replacement. Also pins
			// deferred validator publish: page one carries no validator,
			// page two publishes v2.
			name: "grants-copy-dedup",
			build: func(t *testing.T, fx *scCollectionFixture) *chaosconnector.Scenario {
				allThree := []*v2.Grant{fx.Grants[0], fx.Grants[1], fx.Grant3}
				return grantsScenario(t, fx,
					seedGrantsV1(fx),
					chaosconnector.Pages[*v2.Grant]{
						"": {List: allThree, Annotations: scRecordAnno(scGrantsScopeKey, scValidatorV2)},
						"warm": {
							List:        []*v2.Grant{fx.Grant3},
							Annotations: scReplayAnno(scGrantsScopeKey, "", true, nil, nil),
							Next:        "warm-2",
						},
						"warm-2": {Annotations: scReplayAnno(scGrantsScopeKey, scValidatorV2, true, nil, nil)},
					},
					specV1(""), specV1("warm"),
				)
			},
			seedConsults:   1,
			wantWarmEvents: warmGrantsEvent,
			wantEntries: func(fx *scCollectionFixture) map[string]string {
				return map[string]string{grantsScope: scValidatorV2}
			},
			wantStamps: func(fx *scCollectionFixture) map[string]int {
				return map[string]int{grantsScope: 3}
			},
		},
		{
			// C34 transitional tolerance at orchestration level: a
			// non-overlay replay page carrying rows warns and applies them
			// as overlay upserts anyway.
			name: "grants-overlay-false-with-rows",
			build: func(t *testing.T, fx *scCollectionFixture) *chaosconnector.Scenario {
				allThree := []*v2.Grant{fx.Grants[0], fx.Grants[1], fx.Grant3}
				return grantsScenario(t, fx,
					seedGrantsV1(fx),
					chaosconnector.Pages[*v2.Grant]{
						"": {List: allThree, Annotations: scRecordAnno(scGrantsScopeKey, scValidatorV2)},
						"warm": {
							List:        []*v2.Grant{fx.Grant3},
							Annotations: scReplayAnno(scGrantsScopeKey, scValidatorV2, false, nil, nil),
						},
					},
					specV1(""), specV1("warm"),
				)
			},
			seedConsults:   1,
			wantWarmEvents: warmGrantsEvent,
			wantEntries: func(fx *scCollectionFixture) map[string]string {
				return map[string]string{grantsScope: scValidatorV2}
			},
			wantStamps: func(fx *scCollectionFixture) map[string]int {
				return map[string]int{grantsScope: 3}
			},
		},
		{
			// Record and replay on ONE page (same scope): the record's
			// validator wins the publish (a delta round's final record
			// carries the NEW token), never the replay's.
			name: "grants-record-wins-validator",
			build: func(t *testing.T, fx *scCollectionFixture) *chaosconnector.Scenario {
				allThree := []*v2.Grant{fx.Grants[0], fx.Grants[1], fx.Grant3}
				warmAnnos := scReplayAnno(scGrantsScopeKey, "validator-replay-side", true, nil, nil)
				warmAnnos.Update(v2.SourceCacheRecord_builder{
					ScopeKey:       scGrantsScopeKey,
					CacheValidator: scValidatorV2,
				}.Build())
				return grantsScenario(t, fx,
					seedGrantsV1(fx),
					chaosconnector.Pages[*v2.Grant]{
						"":     {List: allThree, Annotations: scRecordAnno(scGrantsScopeKey, scValidatorV2)},
						"warm": {List: []*v2.Grant{fx.Grant3}, Annotations: warmAnnos},
					},
					specV1(""), specV1("warm"),
				)
			},
			seedConsults:   1,
			wantWarmEvents: warmGrantsEvent,
			wantEntries: func(fx *scCollectionFixture) map[string]string {
				return map[string]string{grantsScope: scValidatorV2}
			},
			wantStamps: func(fx *scCollectionFixture) map[string]int {
				return map[string]int{grantsScope: 3}
			},
		},
		{
			// A replay round that never publishes a validator: rows are
			// replayed correctly (OR2 holds) but the scope gets NO manifest
			// entry — a miss next sync, cacheability lost, correctness kept.
			name: "grants-replay-no-validator-no-entry",
			build: func(t *testing.T, fx *scCollectionFixture) *chaosconnector.Scenario {
				return grantsScenario(t, fx,
					seedGrantsV1(fx),
					chaosconnector.Pages[*v2.Grant]{
						"":     {List: fx.Grants, Annotations: scRecordAnno(scGrantsScopeKey, scValidatorV1)},
						"warm": {Annotations: scReplayAnno(scGrantsScopeKey, "", false, nil, nil)},
					},
					specV1(""), specV1("warm"),
				)
			},
			seedConsults:   1,
			wantWarmEvents: warmGrantsEvent,
			wantEntries: func(fx *scCollectionFixture) map[string]string {
				return map[string]string{}
			},
			wantStamps: func(fx *scCollectionFixture) map[string]int {
				return map[string]int{grantsScope: 2}
			},
		},
		{
			// Zero-row scope, both halves: the seed publishes an entry for
			// a scope with no rows (200-with-zero-rows), and the warm
			// generation replays it — zero rows copied, entry republished.
			name: "grants-zero-row-replay",
			build: func(t *testing.T, fx *scCollectionFixture) *chaosconnector.Scenario {
				return grantsScenario(t, fx,
					chaosconnector.Pages[*v2.Grant]{
						"": {Annotations: scRecordAnno(scGrantsScopeKey, scValidatorV1)},
					},
					chaosconnector.Pages[*v2.Grant]{
						"":     {Annotations: scRecordAnno(scGrantsScopeKey, scValidatorV1)},
						"warm": {Annotations: scReplayAnno(scGrantsScopeKey, scValidatorV1, false, nil, nil)},
					},
					specV1(""), specV1("warm"),
				)
			},
			seedConsults:   1,
			wantWarmEvents: warmGrantsEvent,
			wantEntries: func(fx *scCollectionFixture) map[string]string {
				return map[string]string{grantsScope: scValidatorV1}
			},
			wantStamps: func(fx *scCollectionFixture) map[string]int {
				return map[string]int{}
			},
		},
		{
			// Resources kind, delta overlay: the replayed base (u1, u2) is
			// trimmed by a resource-BID tombstone (u1) after the fresh
			// upsert (u3). Upstream truth is [u2, u3]. The user type skips
			// entitlements/grants, so replayed resources spawn no child
			// work by construction.
			name: "resources-delta-overlay",
			build: func(t *testing.T, fx *scCollectionFixture) *chaosconnector.Scenario {
				u1BID, err := bid.MakeResourceBid(fx.Users[0])
				require.NoError(t, err)
				seed := scCollectionBase(fx)
				seed.Resources[scUserTypeID] = chaosconnector.Pages[*v2.Resource]{
					"": {List: []*v2.Resource{fx.Users[0], fx.Users[1]}, Annotations: scRecordAnno(scResourceScope, scValidatorV1)},
				}
				seed.SourceCacheResources = map[string]*chaosconnector.SourceCacheSpec{
					scUserTypeID: {ScopeKey: scResourceScope, Validator: scValidatorV1},
				}
				second := scCollectionBase(fx)
				second.Resources[scUserTypeID] = chaosconnector.Pages[*v2.Resource]{
					"": {List: []*v2.Resource{fx.Users[1], fx.User3}, Annotations: scRecordAnno(scResourceScope, scValidatorV2)},
					"warm": {
						List:        []*v2.Resource{fx.User3},
						Annotations: scReplayAnno(scResourceScope, scValidatorV2, true, []string{u1BID}, nil),
					},
				}
				second.SourceCacheResources = map[string]*chaosconnector.SourceCacheSpec{
					scUserTypeID: {ScopeKey: scResourceScope, Validator: scValidatorV1, WarmRoot: "warm"},
				}
				scenario := &chaosconnector.Scenario{
					Name:         "source-cache-collection-resources",
					Seed:         1,
					InitialEpoch: "seed",
					Epochs:       map[string]*chaosconnector.Dataset{"seed": seed, "second": second},
				}
				require.NoError(t, scenario.Validate())
				return scenario
			},
			seedConsults: 1,
			wantWarmEvents: func(fx *scCollectionFixture) []chaosconnector.SourceCacheLookupEvent {
				return []chaosconnector.SourceCacheLookupEvent{
					scWarmEventFor(sourcecache.RowKindResources, scResourceScope, scValidatorV1),
				}
			},
			wantEntries: func(fx *scCollectionFixture) map[string]string {
				return map[string]string{chaosoracle.KindScope(sourcecache.RowKindResources, scResourceScope): scValidatorV2}
			},
			wantStamps: func(fx *scCollectionFixture) map[string]int {
				return map[string]int{chaosoracle.KindScope(sourcecache.RowKindResources, scResourceScope): 2}
			},
		},
		{
			// Cross-kind non-aliasing: the IDENTICAL scope-key string on an
			// entitlements scope and a grants scope, same validator. The
			// manifest keys by (kind, scope) so both entries coexist, each
			// kind replays only its own rows, and stamp counts stay per-kind.
			name: "cross-kind-shared-scope",
			build: func(t *testing.T, fx *scCollectionFixture) *chaosconnector.Scenario {
				buildEpoch := func(warm bool) *chaosconnector.Dataset {
					d := scCollectionBase(fx)
					entPages := chaosconnector.Pages[*v2.Entitlement]{
						"": {List: []*v2.Entitlement{fx.Member}, Annotations: scRecordAnno(scSharedScope, scSharedValidator)},
					}
					grantPages := chaosconnector.Pages[*v2.Grant]{
						"": {List: fx.Grants, Annotations: scRecordAnno(scSharedScope, scSharedValidator)},
					}
					entSpec := &chaosconnector.SourceCacheSpec{ScopeKey: scSharedScope, Validator: scSharedValidator}
					grantSpec := &chaosconnector.SourceCacheSpec{ScopeKey: scSharedScope, Validator: scSharedValidator}
					if warm {
						entPages["warm"] = chaosconnector.Page[*v2.Entitlement]{
							Annotations: scReplayAnno(scSharedScope, scSharedValidator, false, nil, nil),
						}
						grantPages["warm"] = chaosconnector.Page[*v2.Grant]{
							Annotations: scReplayAnno(scSharedScope, scSharedValidator, false, nil, nil),
						}
						entSpec.WarmRoot = "warm"
						grantSpec.WarmRoot = "warm"
					}
					d.Entitlements["team-1"] = entPages
					d.Grants["team-1"] = grantPages
					d.SourceCacheEntitlements = map[string]*chaosconnector.SourceCacheSpec{"team-1": entSpec}
					d.SourceCacheGrants = map[string]*chaosconnector.SourceCacheSpec{"team-1": grantSpec}
					return d
				}
				scenario := &chaosconnector.Scenario{
					Name:         "source-cache-collection-cross-kind",
					Seed:         1,
					InitialEpoch: "seed",
					Epochs: map[string]*chaosconnector.Dataset{
						"seed":   buildEpoch(false),
						"second": buildEpoch(true),
					},
				}
				require.NoError(t, scenario.Validate())
				return scenario
			},
			seedConsults: 2,
			wantWarmEvents: func(fx *scCollectionFixture) []chaosconnector.SourceCacheLookupEvent {
				return []chaosconnector.SourceCacheLookupEvent{
					scWarmEventFor(sourcecache.RowKindEntitlements, scSharedScope, scSharedValidator),
					scWarmEventFor(sourcecache.RowKindGrants, scSharedScope, scSharedValidator),
				}
			},
			wantEntries: func(fx *scCollectionFixture) map[string]string {
				return map[string]string{
					chaosoracle.KindScope(sourcecache.RowKindEntitlements, scSharedScope): scSharedValidator,
					chaosoracle.KindScope(sourcecache.RowKindGrants, scSharedScope):       scSharedValidator,
				}
			},
			wantStamps: func(fx *scCollectionFixture) map[string]int {
				return map[string]int{
					chaosoracle.KindScope(sourcecache.RowKindEntitlements, scSharedScope): 1,
					chaosoracle.KindScope(sourcecache.RowKindGrants, scSharedScope):       2,
				}
			},
		},
	}

	for _, tc := range cells {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
			defer cancel()
			tmpDir, paths := sourceCachePaths(t, 3)
			seedPath, warmPath, baselinePath := paths[0], paths[1], paths[2]

			fx := newSCCollectionFixture(t)
			scenario := tc.build(t, fx)
			capability := sourceCacheCapabilityRW("gen-1", "cfg-1")

			// Generation A: cold seed of the "seed" epoch.
			run, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
			require.NoError(t, err)
			run.SetSourceCacheCapability(capability)
			seedEvents := runSourceCacheSync(t, ctx, run, chaosTransportDirect, seedPath, tmpDir, "", WithWorkerCount(1))
			requireAllColdEvents(t, seedEvents, tc.seedConsults)

			// Generation B: warm against A, upstream now at "second".
			require.NoError(t, run.SetEpoch("second"))
			warmEvents := runSourceCacheSync(t, ctx, run, chaosTransportDirect, warmPath, tmpDir, seedPath, WithWorkerCount(1))
			requireSourceCacheEvents(t, warmEvents, tc.wantWarmEvents(fx))

			// Independent cold baseline of the SAME final epoch: a fresh
			// run misses every lookup and serves the cold branch.
			baselineRun, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
			require.NoError(t, err)
			require.NoError(t, baselineRun.SetEpoch("second"))
			baselineRun.SetSourceCacheCapability(capability)
			baselineEvents := runSourceCacheSync(t, ctx, baselineRun, chaosTransportDirect, baselinePath, tmpDir, "", WithWorkerCount(1))
			requireAllColdEvents(t, baselineEvents, tc.seedConsults)

			// OR2 — warm result must equal the cold truth of the epoch.
			warmContent := readChaosLogicalContent(t, ctx, warmPath, tmpDir)
			baselineContent := readChaosLogicalContent(t, ctx, baselinePath, tmpDir)
			require.NoError(t, chaosoracle.CompareLogicalContent(baselineContent, warmContent),
				"warm generation's logical content must equal the independent cold baseline")

			// OR3 — exact produce-side state on the warm artifact.
			requireSourceCacheProduceState(t, readSourceCacheSnapshot(t, ctx, warmPath, tmpDir),
				tc.wantEntries(fx), tc.wantStamps(fx))
		})
	}
}

// TestChaosSourceCacheFreshPageSemantics pins R3's cold-only fresh-page
// cells (plan B3, oracle OR3): multi-page rounds, same-page
// put-then-tombstone ordering, the empty-validator transitional round (6a
// C25's semantics at orchestration level), and the static-entitlement
// registered exclusion (B10).
func TestChaosSourceCacheFreshPageSemantics(t *testing.T) {
	skipChaosInShort(t)

	grantsScope := chaosoracle.KindScope(sourcecache.RowKindGrants, scGrantsScopeKey)

	t.Run("multi-page-round", func(t *testing.T) {
		// One recording round split across two wire pages, each carrying
		// the same record annotation: every page's rows are stamped and
		// the entry publishes once with the shared validator.
		ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
		defer cancel()
		tmpDir, paths := sourceCachePaths(t, 1)

		fx := newSCCollectionFixture(t)
		d := scCollectionBase(fx)
		d.Grants["team-1"] = chaosconnector.Pages[*v2.Grant]{
			"":   {List: []*v2.Grant{fx.Grants[0]}, Annotations: scRecordAnno(scGrantsScopeKey, scValidatorV1), Next: "p2"},
			"p2": {List: []*v2.Grant{fx.Grants[1]}, Annotations: scRecordAnno(scGrantsScopeKey, scValidatorV1)},
		}
		d.SourceCacheGrants = map[string]*chaosconnector.SourceCacheSpec{
			"team-1": {ScopeKey: scGrantsScopeKey, Validator: scValidatorV1},
		}
		scenario := &chaosconnector.Scenario{
			Name: "source-cache-fresh-multipage", Seed: 1, InitialEpoch: "seed",
			Epochs: map[string]*chaosconnector.Dataset{"seed": d},
		}
		require.NoError(t, scenario.Validate())

		run, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
		require.NoError(t, err)
		run.SetSourceCacheCapability(sourceCacheCapabilityRW("gen-1", "cfg-1"))
		events := runSourceCacheSync(t, ctx, run, chaosTransportDirect, paths[0], tmpDir, "", WithWorkerCount(1))
		requireAllColdEvents(t, events, 1)

		grants := readChaosGrantsByID(t, ctx, paths[0], tmpDir)
		require.Len(t, grants, 2)
		requireSourceCacheProduceState(t, readSourceCacheSnapshot(t, ctx, paths[0], tmpDir),
			map[string]string{grantsScope: scValidatorV1},
			map[string]int{grantsScope: 2})
	})

	t.Run("same-page-put-then-tombstone", func(t *testing.T) {
		// A page that upserts g1 and g2 AND tombstones g1 in its own
		// record annotation: within-page order is upserts before deletes
		// (B3, 6a C29's orchestration half), so g1 must be gone and only
		// g2 stamped. Reversed order would leave both rows standing.
		ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
		defer cancel()
		tmpDir, paths := sourceCachePaths(t, 1)

		fx := newSCCollectionFixture(t)
		d := scCollectionBase(fx)
		d.Grants["team-1"] = chaosconnector.Pages[*v2.Grant]{
			"": {
				List: fx.Grants,
				Annotations: annotations.New(v2.SourceCacheRecord_builder{
					ScopeKey:       scGrantsScopeKey,
					CacheValidator: scValidatorV1,
					DeletedIds:     []string{fx.Grants[0].GetId()},
				}.Build()),
			},
		}
		d.SourceCacheGrants = map[string]*chaosconnector.SourceCacheSpec{
			"team-1": {ScopeKey: scGrantsScopeKey, Validator: scValidatorV1},
		}
		scenario := &chaosconnector.Scenario{
			Name: "source-cache-fresh-put-tombstone", Seed: 1, InitialEpoch: "seed",
			Epochs: map[string]*chaosconnector.Dataset{"seed": d},
		}
		require.NoError(t, scenario.Validate())

		run, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
		require.NoError(t, err)
		run.SetSourceCacheCapability(sourceCacheCapabilityRW("gen-1", "cfg-1"))
		events := runSourceCacheSync(t, ctx, run, chaosTransportDirect, paths[0], tmpDir, "", WithWorkerCount(1))
		requireAllColdEvents(t, events, 1)

		grants := readChaosGrantsByID(t, ctx, paths[0], tmpDir)
		require.Len(t, grants, 1)
		require.Contains(t, grants, fx.Grants[1].GetId(), "the tombstoned row must be the one that dies")
		requireSourceCacheProduceState(t, readSourceCacheSnapshot(t, ctx, paths[0], tmpDir),
			map[string]string{grantsScope: scValidatorV1},
			map[string]int{grantsScope: 1})
	})

	t.Run("empty-validator-no-entry-misses-next-sync", func(t *testing.T) {
		// The transitional round: a record with rows but NO validator
		// stamps the rows and publishes NO manifest entry, so the next
		// sync's lookup for that scope must miss and refetch (6a C25's
		// semantics at orchestration level).
		ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
		defer cancel()
		tmpDir, paths := sourceCachePaths(t, 2)
		seedPath, secondPath := paths[0], paths[1]

		fx := newSCCollectionFixture(t)
		seed := scCollectionBase(fx)
		seed.Grants["team-1"] = chaosconnector.Pages[*v2.Grant]{
			"": {List: fx.Grants, Annotations: scRecordAnno(scGrantsScopeKey, "")},
		}
		second := scCollectionBase(fx)
		second.Grants["team-1"] = chaosconnector.Pages[*v2.Grant]{
			"":     {List: fx.Grants, Annotations: scRecordAnno(scGrantsScopeKey, scValidatorV1)},
			"warm": {Annotations: scReplayAnno(scGrantsScopeKey, scValidatorV1, false, nil, nil)},
		}
		second.SourceCacheGrants = map[string]*chaosconnector.SourceCacheSpec{
			"team-1": {ScopeKey: scGrantsScopeKey, Validator: scValidatorV1, WarmRoot: "warm"},
		}
		scenario := &chaosconnector.Scenario{
			Name: "source-cache-fresh-transitional", Seed: 1, InitialEpoch: "seed",
			Epochs: map[string]*chaosconnector.Dataset{"seed": seed, "second": second},
		}
		require.NoError(t, scenario.Validate())
		capability := sourceCacheCapabilityRW("gen-1", "cfg-1")

		run, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
		require.NoError(t, err)
		run.SetSourceCacheCapability(capability)
		seedEvents := runSourceCacheSync(t, ctx, run, chaosTransportDirect, seedPath, tmpDir, "", WithWorkerCount(1))
		require.Empty(t, seedEvents, "seed epoch declares no source-cache spec: no consults")
		// Rows stamped, NO entry: the vacuity guard for the next sync's miss.
		requireSourceCacheProduceState(t, readSourceCacheSnapshot(t, ctx, seedPath, tmpDir),
			map[string]string{},
			map[string]int{grantsScope: 2})

		require.NoError(t, run.SetEpoch("second"))
		secondEvents := runSourceCacheSync(t, ctx, run, chaosTransportDirect, secondPath, tmpDir, seedPath, WithWorkerCount(1))
		requireAllColdEvents(t, secondEvents, 1)
		requireSourceCacheProduceState(t, readSourceCacheSnapshot(t, ctx, secondPath, tmpDir),
			map[string]string{grantsScope: scValidatorV1},
			map[string]int{grantsScope: 2})
	})

	t.Run("static-entitlement-annotation-ignored", func(t *testing.T) {
		// Registered exclusion (B10): a SourceCacheRecord on a
		// ListStaticEntitlements response is ignored with a warn — static
		// entitlements stay unscoped, no entry and no stamps appear, and
		// the sync stays green.
		ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
		defer cancel()
		tmpDir, paths := sourceCachePaths(t, 1)

		fx := newSCCollectionFixture(t)
		d := scCollectionBase(fx)
		d.StaticEntitlements[scTeamTypeID] = chaosconnector.Pages[*v2.Entitlement]{
			"": {
				List:        []*v2.Entitlement{et.NewEntitlement(fx.Team, "viewer", "assignment")},
				Annotations: scRecordAnno("static:team", "v-static"),
			},
		}
		scenario := &chaosconnector.Scenario{
			Name: "source-cache-fresh-static-ents", Seed: 1, InitialEpoch: "seed",
			Epochs: map[string]*chaosconnector.Dataset{"seed": d},
		}
		require.NoError(t, scenario.Validate())

		run, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
		require.NoError(t, err)
		run.SetSourceCacheCapability(sourceCacheCapabilityRW("gen-1", "cfg-1"))
		events := runSourceCacheSync(t, ctx, run, chaosTransportDirect, paths[0], tmpDir, "", WithWorkerCount(1))
		require.Empty(t, events, "static entitlements never consult the lookup")
		requireSourceCacheProduceState(t, readSourceCacheSnapshot(t, ctx, paths[0], tmpDir),
			map[string]string{}, map[string]int{})
	})
}
