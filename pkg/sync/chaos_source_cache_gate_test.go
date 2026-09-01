package sync //nolint:revive,nolintlint // backwards-compatible package name

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/internal/chaosconnector"
	chaosoracle "github.com/conductorone/baton-sdk/internal/chaosconnector/oracle"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/sourcecache"
	gt "github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
)

// Phase 6b gate-matrix suite (plan R2 + R5 + R6, oracle OR1): each cell
// runs generation A (the seed sync) and generation B with exactly one gate
// condition falsified, then asserts the connector-side lookup events show
// the frozen outcome — warm (hit + matched + served warm) or cold (miss).
//
// Artifact-provenance cells that fail in NewSyncer before any connector is
// involved — non-Pebble (G2), non-FULL / compacted (G3), stats-absent (G4),
// fence-stripped (G5) — are pinned at that level by
// TestPreviousSyncC1ZPathEnforcesReplayEligibility; this suite carries the
// cells that need a real capable connector in the loop.

// scWarmEvent is the expected event for a warm serve of the reference
// grants scope; scColdEvent for a cold serve (miss).
func scWarmEvent() chaosconnector.SourceCacheLookupEvent {
	return chaosconnector.SourceCacheLookupEvent{
		RowKind:           sourcecache.RowKindGrants,
		ScopeKey:          scGrantsScopeKey,
		Hit:               true,
		PreviousValidator: scValidatorV1,
		Matched:           true,
		ServedWarm:        true,
	}
}

func scColdEvent() chaosconnector.SourceCacheLookupEvent {
	return chaosconnector.SourceCacheLookupEvent{
		RowKind:  sourcecache.RowKindGrants,
		ScopeKey: scGrantsScopeKey,
	}
}

func TestChaosSourceCacheGateMatrix(t *testing.T) {
	skipChaosInShort(t)

	type cell struct {
		name string
		// seedCapability arms generation A; nil means A declares nothing.
		seedCapability *v2.SourceCacheCapability
		// mutateDataset adversarially reshapes the scenario before the run
		// is armed (both generations serve it).
		mutateDataset func(t *testing.T, fixture *scFixture, dataset *chaosconnector.Dataset)
		// secondCapability arms generation B.
		secondCapability *v2.SourceCacheCapability
		// secondOpts extends generation B's sync options.
		secondOpts []SyncOpt
		// noPrevious withholds WithPreviousSyncC1ZPath from B.
		noPrevious bool
		// wantWarm is B's expected consume outcome.
		wantWarm bool
		// wantSecondProduce asserts whether B's artifact carries produce-side
		// state (manifest entry + stamps + compat record).
		wantSecondProduce bool
	}

	rw := sourceCacheCapabilityRW("gen-1", "cfg-1")
	cells := []cell{
		{
			// All gates pass: the frozen warm baseline.
			name:              "warm-baseline",
			seedCapability:    rw,
			secondCapability:  rw,
			wantWarm:          true,
			wantSecondProduce: true,
		},
		{
			// G1: no previous artifact supplied at all.
			name:              "no-previous-artifact",
			seedCapability:    rw,
			secondCapability:  rw,
			noPrevious:        true,
			wantSecondProduce: true,
		},
		{
			// G6: capability absent on B's Validate — every source-cache
			// annotation is ignored wholesale, so B also produces nothing.
			name:             "capability-absent",
			seedCapability:   rw,
			secondCapability: nil,
		},
		{
			// G6: capability declared but not MODE_READ_WRITE.
			name:           "capability-disabled",
			seedCapability: rw,
			secondCapability: v2.SourceCacheCapability_builder{
				Mode:              v2.SourceCacheCapability_MODE_DISABLED,
				CacheGeneration:   "gen-1",
				ConfigFingerprint: "cfg-1",
			}.Build(),
		},
		{
			// G7: connector cache generation drifted between A and B.
			name:              "compat-cache-generation-mismatch",
			seedCapability:    rw,
			secondCapability:  sourceCacheCapabilityRW("gen-2", "cfg-1"),
			wantSecondProduce: true,
		},
		{
			// G7: connector config fingerprint drifted.
			name:              "compat-config-fingerprint-mismatch",
			seedCapability:    rw,
			secondCapability:  sourceCacheCapabilityRW("gen-1", "cfg-2"),
			wantSecondProduce: true,
		},
		{
			// G7: selection fingerprint drifted — B declares an explicit
			// resource-type filter that happens to collect the same data.
			// Same rows, different declared selection: still cold.
			name:              "compat-selection-fingerprint-mismatch",
			seedCapability:    rw,
			secondCapability:  rw,
			secondOpts:        []SyncOpt{WithSyncResourceTypes([]string{scTeamTypeID, scUserTypeID})},
			wantSecondProduce: true,
		},
		{
			// G7: previous artifact carries no compat record at all (its
			// sync declared no capability — pre-6b shape).
			name:              "compat-record-absent",
			seedCapability:    nil,
			secondCapability:  rw,
			wantSecondProduce: true,
		},
		{
			// G4 at chaos level (R6): generation A trips an ingest-filter
			// drop (a grant to a principal type the sync never schedules),
			// seals replay-blocked, and must not seed B. The drop happens on
			// an UNANNOTATED page so A's source-cache state itself is sound
			// — only quality blocks it.
			name:           "quality-blocked-previous",
			seedCapability: rw,
			mutateDataset: func(t *testing.T, fixture *scFixture, dataset *chaosconnector.Dataset) {
				ghostType := v2.ResourceType_builder{
					Id:          "sc-ghost",
					DisplayName: "SC Ghost",
					Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER},
				}.Build()
				ghost, err := rs.NewUserResource("Ghost", ghostType, "ghost-1", nil)
				require.NoError(t, err)
				pages := dataset.Grants["team-1"]
				cold := pages[""]
				cold.List = append(cold.List, gt.NewGrant(fixture.Team, "member", ghost))
				pages[""] = cold
			},
			secondCapability:  rw,
			wantSecondProduce: true,
		},
	}

	for _, tc := range cells {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
			defer cancel()
			tmpDir, paths := sourceCachePaths(t, 2)

			fixture := newSourceCacheFixture(t)
			scenario := newSourceCacheScenario(t, fixture)
			if tc.mutateDataset != nil {
				tc.mutateDataset(t, fixture, scenario.Epochs[scenario.InitialEpoch])
				require.NoError(t, scenario.Validate())
			}
			run, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
			require.NoError(t, err)

			// Generation A: always a cold seed — one lookup, one miss.
			run.SetSourceCacheCapability(tc.seedCapability)
			seedEvents := runSourceCacheSync(t, ctx, run, chaosTransportDirect, paths[0], tmpDir, "")
			requireSourceCacheEvents(t, seedEvents, []chaosconnector.SourceCacheLookupEvent{scColdEvent()})

			// Generation B: the cell's gate condition applied.
			run.SetSourceCacheCapability(tc.secondCapability)
			prevPath := paths[0]
			if tc.noPrevious {
				prevPath = ""
			}
			secondEvents := runSourceCacheSync(t, ctx, run, chaosTransportDirect, paths[1], tmpDir, prevPath, tc.secondOpts...)
			expected := scColdEvent()
			if tc.wantWarm {
				expected = scWarmEvent()
			}
			requireSourceCacheEvents(t, secondEvents, []chaosconnector.SourceCacheLookupEvent{expected})

			// OR2 — B's logical content must equal A's regardless of how it
			// was served (replayed or fresh; the epoch never changed).
			// The quality cell is the exception by design: A contains the
			// dropped-grant page's survivors, and B (served identically)
			// matches A anyway, so the comparison still holds.
			seedContent := readChaosLogicalContent(t, ctx, paths[0], tmpDir)
			secondContent := readChaosLogicalContent(t, ctx, paths[1], tmpDir)
			require.NoError(t, chaosoracle.CompareLogicalContent(seedContent, secondContent),
				"generation B's logical content must equal generation A's")

			// OR3 — produce-side state on B's artifact.
			snapshot := readSourceCacheSnapshot(t, ctx, paths[1], tmpDir)
			if tc.wantSecondProduce {
				entry, ok := snapshot.Entries[grantsKindScope()]
				require.True(t, ok, "B must publish the grants scope manifest entry")
				require.Equal(t, scValidatorV1, entry.Validator)
				require.False(t, entry.Invalidated)
				require.Equal(t, len(fixture.Grants), snapshot.StampCounts[grantsKindScope()],
					"every replayed/fresh grant of the scope must carry the scope stamp")
				require.NotNil(t, snapshot.Compat, "capable sync must write the compat record")
				require.Equal(t, sourcecache.MaterializationPolicyGeneration, snapshot.Compat.SDKMaterializationGeneration)
			} else {
				require.Empty(t, snapshot.Entries, "incapable sync must not publish manifest entries")
				require.Empty(t, snapshot.StampCounts, "incapable sync must not stamp rows")
				require.Nil(t, snapshot.Compat, "incapable sync must not write a compat record")
			}
		})
	}
}

// TestChaosSourceCacheWarmAcrossTransports pins the warm A→B chain on every
// chaos transport (R1's transport coverage): the direct aggregate and both
// in-memory gRPC shapes must deliver the lookup and produce identical warm
// outcomes.
func TestChaosSourceCacheWarmAcrossTransports(t *testing.T) {
	skipChaosInShort(t)
	for _, transport := range chaosFaultTransports() {
		t.Run(transport.String(), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
			defer cancel()
			tmpDir, paths := sourceCachePaths(t, 2)

			fixture := newSourceCacheFixture(t)
			scenario := newSourceCacheScenario(t, fixture)
			run, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
			require.NoError(t, err)
			run.SetSourceCacheCapability(sourceCacheCapabilityRW("gen-1", "cfg-1"))

			seedEvents := runSourceCacheSync(t, ctx, run, transport, paths[0], tmpDir, "")
			requireSourceCacheEvents(t, seedEvents, []chaosconnector.SourceCacheLookupEvent{scColdEvent()})

			secondEvents := runSourceCacheSync(t, ctx, run, transport, paths[1], tmpDir, paths[0])
			requireSourceCacheEvents(t, secondEvents, []chaosconnector.SourceCacheLookupEvent{scWarmEvent()})

			seedContent := readChaosLogicalContent(t, ctx, paths[0], tmpDir)
			secondContent := readChaosLogicalContent(t, ctx, paths[1], tmpDir)
			require.NoError(t, chaosoracle.CompareLogicalContent(seedContent, secondContent))
		})
	}
}

// TestChaosSourceCacheLookupTeardown pins the R1 teardown contract: after a
// warm sync ends, a late connector call must observe NoopLookup — never the
// sync's warm lookup.
func TestChaosSourceCacheLookupTeardown(t *testing.T) {
	skipChaosInShort(t)
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	tmpDir, paths := sourceCachePaths(t, 2)

	fixture := newSourceCacheFixture(t)
	scenario := newSourceCacheScenario(t, fixture)
	run, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
	require.NoError(t, err)
	run.SetSourceCacheCapability(sourceCacheCapabilityRW("gen-1", "cfg-1"))

	seedEvents := runSourceCacheSync(t, ctx, run, chaosTransportDirect, paths[0], tmpDir, "")
	requireSourceCacheEvents(t, seedEvents, []chaosconnector.SourceCacheLookupEvent{scColdEvent()})

	// Generation B warm, holding the harness open after Sync: teardown runs
	// at Sync exit, so a late RPC issued before Close must already observe
	// the cleared (Noop) lookup while the previous artifact is still open —
	// a hit here would mean the warm lookup leaked past the sync.
	run.ResetSourceCacheLookupEvents()
	h := newChaosHarness(t, ctx, run, paths[1], tmpDir, chaosTransportDirect,
		WithPreviousSyncC1ZPath(paths[0]))
	require.NoError(t, h.Syncer.Sync(ctx))
	warmEvents := run.SourceCacheLookupEvents()
	requireSourceCacheEvents(t, warmEvents, []chaosconnector.SourceCacheLookupEvent{scWarmEvent()})

	run.ResetSourceCacheLookupEvents()
	_, err = h.Client.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{
		Resource: fixture.Team,
	}.Build())
	require.NoError(t, err)
	lateEvents := run.SourceCacheLookupEvents()
	requireSourceCacheEvents(t, lateEvents, []chaosconnector.SourceCacheLookupEvent{scColdEvent()})

	require.NoError(t, h.Close(ctx))
	require.NoError(t, run.Runtime().VerifyRequired())
}
