package sync //nolint:revive,nolintlint // backwards-compatible package name

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/internal/chaosconnector"
	chaosoracle "github.com/conductorone/baton-sdk/internal/chaosconnector/oracle"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/sourcecache"
	et "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	gt "github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
)

// Phase 6b interruption/resume suite (plan R11 + R8's cross-process resume,
// oracles OR6 + OR2 + OR3): a warm sync is cut at a connector-call seam,
// then resumed by a NEW syncer instance over the checkpointed state (the
// cross-process shape). Every cell must converge to the independent cold
// baseline. The suite also pins the resume granularity it discovered: an
// interrupted paginated action restarts from its ROOT token (mid-chain
// tokens are not resume points), so replay processing is at-least-once.
// CORRECTED by the sync-trace audit (chaos_trace_oracle_test.go): for a
// mid-chain cut the replayed-set does NOT restore — checkpoints commit at
// batch boundaries and the page chain runs inside one batch, so the
// resume re-runs the replay copy regardless of checkpoint cadence. The
// guard for the re-run is the copy's own replacement idempotence (B5);
// the replayed-set's skip role is WITHIN-attempt (a later replay
// annotation for an already-copied scope skips, e.g. warm-2 below).

// withExpandGrants re-enables grant expansion (the shared chaos harness
// disables it by default).
func withExpandGrants() SyncOpt {
	return func(s *syncer) { s.dontExpandGrants = false }
}

// grantsTraceHas reports whether the run's fault trace saw a ListGrants
// request for the given page token finish with the given outcome.
func grantsTraceHas(run *chaosconnector.Run, pageToken string, outcome chaosconnector.Outcome) bool {
	for _, event := range run.Trace().Events() {
		if event.Operation.Method == "ListGrants" &&
			event.Operation.PageToken == pageToken &&
			event.Outcome == outcome {
			return true
		}
	}
	return false
}

// scResumeScenario: seed epoch serves [g1, g2] fresh at v1. Second epoch's
// truth is all three grants at v2; its warm branch is a two-page delta
// round — page one replays with an overlay upsert (g3), page two carries a
// second replay annotation for the same scope and publishes v2.
func scResumeScenario(t *testing.T, fx *scCollectionFixture) *chaosconnector.Scenario {
	t.Helper()
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
			Next:        "warm-2",
		},
		"warm-2": {Annotations: scReplayAnno(scGrantsScopeKey, scValidatorV2, true, nil, nil)},
	}
	second.SourceCacheGrants = map[string]*chaosconnector.SourceCacheSpec{
		"team-1": {ScopeKey: scGrantsScopeKey, Validator: scValidatorV1, WarmRoot: "warm"},
	}
	scenario := &chaosconnector.Scenario{
		Name:         "source-cache-resume",
		Seed:         1,
		InitialEpoch: "seed",
		Epochs:       map[string]*chaosconnector.Dataset{"seed": seed, "second": second},
	}
	require.NoError(t, scenario.Validate())
	return scenario
}

func TestChaosSourceCacheInterruptResume(t *testing.T) {
	skipChaosInShort(t)

	warmEvent := scWarmEventFor(sourcecache.RowKindGrants, scGrantsScopeKey, scValidatorV1)
	coldEvent := chaosconnector.SourceCacheLookupEvent{
		RowKind:  sourcecache.RowKindGrants,
		ScopeKey: scGrantsScopeKey,
	}

	cells := []struct {
		name string
		// cutToken is the wire page token whose first request crashes. The
		// warm branch's wire tokens are "" (serves the warm root's content)
		// then "warm-2", so "" cuts BEFORE the lookup consult and "warm-2"
		// cuts AFTER the replay copy and overlay upsert committed.
		cutToken string
		// resumeWarm re-supplies the previous artifact on resume; false is
		// the withdrawn-at-resume cold shape.
		resumeWarm bool
		// wantInterruptedEvents / wantResumeEvents are the exact consult
		// traces of each attempt. Resume semantics discovered and pinned
		// here: the interrupted action RESTARTS FROM ITS ROOT token —
		// mid-chain page tokens are not resume points — so page
		// processing is at-least-once and every post-cut resume
		// re-consults exactly once.
		wantInterruptedEvents []chaosconnector.SourceCacheLookupEvent
		wantResumeEvents      []chaosconnector.SourceCacheLookupEvent
		// wantResumeServedTokens must appear as successfully returned
		// ListGrants pages in the RESUME attempt's trace.
		wantResumeServedTokens []string
	}{
		{
			// Post-copy cut, warm resume: the action restarts at the root,
			// re-consults (hit, warm), and re-walks the warm chain over
			// rows the interrupted attempt already committed. The re-run
			// replay page RE-RUNS the copy (the mid-chain cut left the
			// replayed-set un-checkpointed — see the suite comment) and
			// the overlay upsert re-applies, both idempotently; warm-2 is
			// reached again via the chain, skips via the same-attempt
			// replayed-set, and publishes the new validator.
			name:                   "cut-after-copy-resume-warm",
			cutToken:               "warm-2",
			resumeWarm:             true,
			wantInterruptedEvents:  []chaosconnector.SourceCacheLookupEvent{warmEvent},
			wantResumeEvents:       []chaosconnector.SourceCacheLookupEvent{warmEvent},
			wantResumeServedTokens: []string{"warm-2"},
		},
		{
			// Post-copy cut, previous artifact withdrawn at resume: the
			// restarted root consult goes through NoopLookup — miss — and
			// the cold branch fully refetches OVER the half-written warm
			// rows from the interrupted attempt. The warm chain (and its
			// validator-publishing warm-2 page) is never revisited; the
			// cold root's record annotation republishes instead.
			name:                  "cut-after-copy-resume-cold",
			cutToken:              "warm-2",
			resumeWarm:            false,
			wantInterruptedEvents: []chaosconnector.SourceCacheLookupEvent{warmEvent},
			wantResumeEvents:      []chaosconnector.SourceCacheLookupEvent{coldEvent},
		},
		{
			// Pre-consult cut, warm resume: nothing source-cache-related
			// happened before the cut; the resume re-runs the whole warm
			// chain from the root.
			name:                  "cut-before-consult-resume-warm",
			cutToken:              "",
			resumeWarm:            true,
			wantInterruptedEvents: nil,
			wantResumeEvents:      []chaosconnector.SourceCacheLookupEvent{warmEvent},
		},
		{
			// Pre-consult cut, cold resume: the replay decision is remade
			// under NoopLookup — miss, cold branch, fresh fetch.
			name:                  "cut-before-consult-resume-cold",
			cutToken:              "",
			resumeWarm:            false,
			wantInterruptedEvents: nil,
			wantResumeEvents:      []chaosconnector.SourceCacheLookupEvent{coldEvent},
		},
	}

	for _, tc := range cells {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
			defer cancel()
			tmpDir, paths := sourceCachePaths(t, 3)
			seedPath, warmPath, baselinePath := paths[0], paths[1], paths[2]

			fx := newSCCollectionFixture(t)
			scenario := scResumeScenario(t, fx)
			capability := sourceCacheCapabilityRW("gen-1", "cfg-1")

			// Generation A: cold seed.
			seedRun, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
			require.NoError(t, err)
			seedRun.SetSourceCacheCapability(capability)
			seedEvents := runSourceCacheSync(t, ctx, seedRun, chaosTransportDirect, seedPath, tmpDir, "", WithWorkerCount(1))
			requireAllColdEvents(t, seedEvents, 1)

			// Generation B, attempt 1: warm sync cut at the seam.
			interruptedRun, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule(chaosconnector.Rule{
				ID: "cut",
				Match: chaosconnector.Matcher{
					Service:   chaosconnector.ExactString("GrantsService"),
					Method:    chaosconnector.ExactString("ListGrants"),
					PageToken: chaosconnector.ExactString(tc.cutToken),
					Attempt:   1,
					Phase:     chaosconnector.PhaseBeforeCall,
				},
				Effects:  []chaosconnector.Effect{{Kind: chaosconnector.EffectCrash}},
				MinFires: 1,
				MaxFires: 1,
			}))
			require.NoError(t, err)
			require.NoError(t, interruptedRun.SetEpoch("second"))
			interruptedRun.SetSourceCacheCapability(capability)
			interruptedHarness := newChaosHarness(t, ctx, interruptedRun, warmPath, tmpDir, chaosTransportDirect,
				WithPreviousSyncC1ZPath(seedPath), WithWorkerCount(1))
			interruptedConcrete, ok := interruptedHarness.Syncer.(*syncer)
			require.True(t, ok)
			// Persist every completed page so the resume proves the
			// provenance sets restore from the CHECKPOINT, not from luck.
			interruptedConcrete.checkpointInterval = 0
			require.ErrorIs(t, interruptedHarness.Syncer.Sync(ctx), chaosconnector.ErrInterruptRequested)
			require.NoError(t, interruptedHarness.Close(t.Context()))
			require.NoError(t, interruptedRun.Runtime().VerifyRequired())
			requireSourceCacheEvents(t, interruptedRun.SourceCacheLookupEvents(), tc.wantInterruptedEvents)

			// Generation B, resume: a NEW syncer instance over the same
			// artifact (the cross-process shape).
			resumeRun, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
			require.NoError(t, err)
			require.NoError(t, resumeRun.SetEpoch("second"))
			resumeRun.SetSourceCacheCapability(capability)
			resumePrev := ""
			if tc.resumeWarm {
				resumePrev = seedPath
			}
			resumeEvents := runSourceCacheSync(t, ctx, resumeRun, chaosTransportDirect, warmPath, tmpDir, resumePrev, WithWorkerCount(1))
			requireSourceCacheEvents(t, resumeEvents, tc.wantResumeEvents)
			for _, token := range tc.wantResumeServedTokens {
				require.True(t, grantsTraceHas(resumeRun, token, chaosconnector.OutcomeReturned),
					"resume must serve the checkpointed page token %q", token)
			}

			// The resume must finish the interrupted sync, not replace it.
			finalRuns := readChaosSyncRuns(t, ctx, warmPath, tmpDir)
			require.Len(t, finalRuns, 1, "resume must not create a replacement sync")
			require.NotNil(t, finalRuns[0].EndedAt, "resume must finish the interrupted sync")

			// OR6/OR2 — convergence to the independent cold baseline.
			baselineRun, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
			require.NoError(t, err)
			require.NoError(t, baselineRun.SetEpoch("second"))
			baselineRun.SetSourceCacheCapability(capability)
			runSourceCacheSync(t, ctx, baselineRun, chaosTransportDirect, baselinePath, tmpDir, "", WithWorkerCount(1))
			warmContent := readChaosLogicalContent(t, ctx, warmPath, tmpDir)
			baselineContent := readChaosLogicalContent(t, ctx, baselinePath, tmpDir)
			require.NoError(t, chaosoracle.CompareLogicalContent(baselineContent, warmContent),
				"resumed sync must equal the uninterrupted cold baseline")

			// OR3 — no manifest entry may vouch for an incomplete scope,
			// and the completed round publishes exactly v2 over 3 rows.
			requireSourceCacheProduceState(t, readSourceCacheSnapshot(t, ctx, warmPath, tmpDir),
				map[string]string{grantsKindScope(): scValidatorV2},
				map[string]int{grantsKindScope(): 3})
		})
	}
}

// readChaosGrantsByID collects every stored grant keyed by public id.
func readChaosGrantsByID(t *testing.T, ctx context.Context, path, tmpDir string) map[string]*v2.Grant {
	t.Helper()
	store, err := dotc1z.NewStore(
		ctx,
		path,
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithTmpDir(tmpDir),
		dotc1z.WithReadOnly(true),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close(ctx)) }()
	out := map[string]*v2.Grant{}
	token := ""
	for {
		response, err := store.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{
			PageToken: token,
		}.Build())
		require.NoError(t, err)
		for _, grant := range response.GetList() {
			out[grant.GetId()] = grant
		}
		token = response.GetNextPageToken()
		if token == "" {
			return out
		}
	}
}

// TestChaosSourceCacheReplayStripsExpanderSources pins R11's expansion
// clause at sync level: a replayed grants scope containing (a) a direct
// grant whose Sources were EXPANDER-written in the previous sync, (b) a
// grant-expandable group grant, and (c) a direct grant with CONNECTOR-set
// Sources must — after replay + re-expansion — equal the cold baseline
// byte-for-byte: expander Sources are stripped at copy and recomputed from
// true state, connector Sources survive verbatim, and the expander-created
// grant (a derived row) is regenerated unstamped.
func TestChaosSourceCacheReplayStripsExpanderSources(t *testing.T) {
	skipChaosInShort(t)
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	tmpDir, paths := sourceCachePaths(t, 3)
	seedPath, warmPath, baselinePath := paths[0], paths[1], paths[2]

	const (
		scope2      = "grants:team-2"
		validatorT2 = "validator-team-2"
	)

	fx := newSCCollectionFixture(t)
	team2, err := rs.NewGroupResource("Team 2", fx.TeamType, "team-2", nil)
	require.NoError(t, err)
	member2 := et.NewEntitlement(team2, "member", "assignment")
	// Group grant: holders of team-1#member expand into team-2#member.
	expandable := gt.NewGrant(team2, "member", fx.Team,
		gt.WithAnnotation(v2.GrantExpandable_builder{
			EntitlementIds: []string{fx.Member.GetId()},
		}.Build()))
	// user-1 holds team-1#member, so this DIRECT grant receives
	// expander-written Sources (self-source + contribution) in every sync.
	directExpanded := gt.NewGrant(team2, "member", fx.Users[0])
	// user-3 holds nothing on team-1: its connector-set Sources (no
	// self-source entry) must survive replay byte-for-byte.
	connectorSourced := gt.NewGrant(team2, "member", fx.User3)
	connectorSourced.SetSources(v2.GrantSources_builder{
		Sources: map[string]*v2.GrantSources_GrantSource{
			fx.Member.GetId(): {},
		},
	}.Build())
	scopeGrants := []*v2.Grant{expandable, directExpanded, connectorSourced}

	buildEpoch := func(warm bool) *chaosconnector.Dataset {
		d := scCollectionBase(fx)
		d.Resources[scTeamTypeID] = chaosconnector.Pages[*v2.Resource]{
			"": {List: []*v2.Resource{fx.Team, team2}},
		}
		// Team-1's grants stay unannotated and fresh in every generation:
		// they are the expansion inputs, not the replayed scope.
		d.Grants["team-1"] = chaosconnector.Pages[*v2.Grant]{
			"": {List: fx.Grants},
		}
		d.Entitlements["team-2"] = chaosconnector.Pages[*v2.Entitlement]{
			"": {List: []*v2.Entitlement{member2}},
		}
		pages := chaosconnector.Pages[*v2.Grant]{
			"": {List: scopeGrants, Annotations: scRecordAnno(scope2, validatorT2)},
		}
		spec := &chaosconnector.SourceCacheSpec{ScopeKey: scope2, Validator: validatorT2}
		if warm {
			pages["warm"] = chaosconnector.Page[*v2.Grant]{
				Annotations: scReplayAnno(scope2, validatorT2, false, nil, nil),
			}
			spec.WarmRoot = "warm"
		}
		d.Grants["team-2"] = pages
		d.SourceCacheGrants = map[string]*chaosconnector.SourceCacheSpec{"team-2": spec}
		return d
	}
	scenario := &chaosconnector.Scenario{
		Name:         "source-cache-expansion-strip",
		Seed:         1,
		InitialEpoch: "seed",
		Epochs: map[string]*chaosconnector.Dataset{
			"seed":   buildEpoch(false),
			"second": buildEpoch(true),
		},
	}
	require.NoError(t, scenario.Validate())
	capability := sourceCacheCapabilityRW("gen-1", "cfg-1")

	// Generation A: cold seed with expansion enabled.
	seedRun, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
	require.NoError(t, err)
	seedRun.SetSourceCacheCapability(capability)
	seedEvents := runSourceCacheSync(t, ctx, seedRun, chaosTransportDirect, seedPath, tmpDir, "",
		WithWorkerCount(1), withExpandGrants())
	requireAllColdEvents(t, seedEvents, 1)

	// Vacuity guard: the seed artifact must actually carry the
	// expander-written shape this test claims to strip — a self-source on
	// the direct grant — and the untouched connector-set Sources.
	seedGrants := readChaosGrantsByID(t, ctx, seedPath, tmpDir)
	seedDirect, ok := seedGrants[directExpanded.GetId()]
	require.True(t, ok, "seed must store the direct grant")
	require.Contains(t, seedDirect.GetSources().GetSources(), member2.GetId(),
		"seed expansion must have written a self-source on the direct grant")
	seedConnectorSourced, ok := seedGrants[connectorSourced.GetId()]
	require.True(t, ok, "seed must store the connector-sourced grant")
	require.NotContains(t, seedConnectorSourced.GetSources().GetSources(), member2.GetId(),
		"connector-set Sources must not gain a self-source (u3 holds no source entitlement)")

	// Generation B: warm replay of team-2's scope, expansion recomputes.
	warmRun, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
	require.NoError(t, err)
	require.NoError(t, warmRun.SetEpoch("second"))
	warmRun.SetSourceCacheCapability(capability)
	warmEvents := runSourceCacheSync(t, ctx, warmRun, chaosTransportDirect, warmPath, tmpDir, seedPath,
		WithWorkerCount(1), withExpandGrants())
	requireSourceCacheEvents(t, warmEvents, []chaosconnector.SourceCacheLookupEvent{
		scWarmEventFor(sourcecache.RowKindGrants, scope2, validatorT2),
	})

	// Independent cold baseline of the same epoch, expansion enabled.
	baselineRun, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
	require.NoError(t, err)
	require.NoError(t, baselineRun.SetEpoch("second"))
	baselineRun.SetSourceCacheCapability(capability)
	runSourceCacheSync(t, ctx, baselineRun, chaosTransportDirect, baselinePath, tmpDir, "",
		WithWorkerCount(1), withExpandGrants())

	// OR2 over full-proto fingerprints: strip-at-copy + re-expansion must
	// equal fresh + expansion, including every Sources byte.
	warmContent := readChaosLogicalContent(t, ctx, warmPath, tmpDir)
	baselineContent := readChaosLogicalContent(t, ctx, baselinePath, tmpDir)
	require.NoError(t, chaosoracle.CompareLogicalContent(baselineContent, warmContent),
		"replayed+re-expanded content must equal the cold baseline byte-for-byte")

	// OR3: only the three connector-emitted scope rows are stamped — the
	// expander-created grant is a derived row and stays unscoped (B10).
	requireSourceCacheProduceState(t, readSourceCacheSnapshot(t, ctx, warmPath, tmpDir),
		map[string]string{chaosoracle.KindScope(sourcecache.RowKindGrants, scope2): validatorT2},
		map[string]int{chaosoracle.KindScope(sourcecache.RowKindGrants, scope2): 3})

	// Direct evidence on the warm artifact (not just fingerprint parity):
	// the direct grant's Sources were recomputed and the connector-set
	// Sources survived byte-for-byte.
	warmGrants := readChaosGrantsByID(t, ctx, warmPath, tmpDir)
	warmDirect, ok := warmGrants[directExpanded.GetId()]
	require.True(t, ok)
	require.Contains(t, warmDirect.GetSources().GetSources(), member2.GetId(),
		"re-expansion must restore the stripped self-source")
	warmConnectorSourced, ok := warmGrants[connectorSourced.GetId()]
	require.True(t, ok)
	require.Equal(t,
		seedConnectorSourced.GetSources().GetSources(),
		warmConnectorSourced.GetSources().GetSources(),
		"connector-set Sources must survive replay verbatim")
}
