package sync //nolint:revive,nolintlint // backwards-compatible package name

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/internal/chaosconnector"
	chaosoracle "github.com/conductorone/baton-sdk/internal/chaosconnector/oracle"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/sourcecache"
	et "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	gt "github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
)

// Shared fixtures for the Phase 6b source-cache chaos suites
// (chaos_source_cache_*_test.go; plan docs/verification/sync-replay-6b/
// plan.md). The reference scenario is one team whose grants scope opts into
// source-cache: the cold branch serves fresh grants tagged with
// SourceCacheRecord, the warm branch serves a zero-row SourceCacheReplay.

const (
	scTeamTypeID = "sc-team"
	scUserTypeID = "sc-user"

	scGrantsScopeKey = "grants:team-1"
	scValidatorV1    = "validator-v1"
)

// scFixture bundles the deterministic protos the scenario builders share.
type scFixture struct {
	TeamType *v2.ResourceType
	UserType *v2.ResourceType
	Team     *v2.Resource
	Users    []*v2.Resource
	Member   *v2.Entitlement
	Grants   []*v2.Grant
}

func newSourceCacheFixture(t *testing.T) *scFixture {
	t.Helper()
	teamType := v2.ResourceType_builder{
		Id:          scTeamTypeID,
		DisplayName: "SC Team",
		Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_GROUP},
	}.Build()
	userType := v2.ResourceType_builder{
		Id:          scUserTypeID,
		DisplayName: "SC User",
		Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER},
		Annotations: annotations.New(&v2.SkipEntitlementsAndGrants{}),
	}.Build()

	team, err := rs.NewGroupResource("Team 1", teamType, "team-1", nil)
	require.NoError(t, err)
	user1, err := rs.NewUserResource("User 1", userType, "user-1", nil)
	require.NoError(t, err)
	user2, err := rs.NewUserResource("User 2", userType, "user-2", nil)
	require.NoError(t, err)

	member := et.NewEntitlement(team, "member", "assignment")
	return &scFixture{
		TeamType: teamType,
		UserType: userType,
		Team:     team,
		Users:    []*v2.Resource{user1, user2},
		Member:   member,
		Grants: []*v2.Grant{
			gt.NewGrant(team, "member", user1),
			gt.NewGrant(team, "member", user2),
		},
	}
}

// newSourceCacheScenario builds the reference scenario: a single epoch
// where team-1's grants scope declares source-cache behavior. The cold
// root serves the fixture grants with a SourceCacheRecord; the warm root
// serves zero rows with a non-overlay SourceCacheReplay.
func newSourceCacheScenario(t *testing.T, fixture *scFixture) *chaosconnector.Scenario {
	t.Helper()
	scenario := &chaosconnector.Scenario{
		Name:         "source-cache-reference",
		Seed:         1,
		InitialEpoch: "initial",
		Epochs: map[string]*chaosconnector.Dataset{
			"initial": newSourceCacheDataset(fixture),
		},
	}
	require.NoError(t, scenario.Validate())
	return scenario
}

// newSourceCacheDataset builds the reference epoch dataset. Callers mutate
// the returned dataset for adversarial variants before NewRun clones it.
func newSourceCacheDataset(fixture *scFixture) *chaosconnector.Dataset {
	return &chaosconnector.Dataset{
		ResourceTypes: []*v2.ResourceType{fixture.TeamType, fixture.UserType},
		Resources: map[string]chaosconnector.Pages[*v2.Resource]{
			scTeamTypeID: {"": {List: []*v2.Resource{fixture.Team}}},
			scUserTypeID: {"": {List: fixture.Users}},
		},
		StaticEntitlements: map[string]chaosconnector.Pages[*v2.Entitlement]{
			scTeamTypeID: {"": {}},
			scUserTypeID: {"": {}},
		},
		Entitlements: map[string]chaosconnector.Pages[*v2.Entitlement]{
			"team-1": {"": {List: []*v2.Entitlement{fixture.Member}}},
		},
		Grants: map[string]chaosconnector.Pages[*v2.Grant]{
			"team-1": {
				"": {
					List: fixture.Grants,
					Annotations: annotations.New(v2.SourceCacheRecord_builder{
						ScopeKey:       scGrantsScopeKey,
						CacheValidator: scValidatorV1,
					}.Build()),
				},
				"warm": {
					Annotations: annotations.New(v2.SourceCacheReplay_builder{
						ScopeKey:       scGrantsScopeKey,
						CacheValidator: scValidatorV1,
					}.Build()),
				},
			},
		},
		SourceCacheGrants: map[string]*chaosconnector.SourceCacheSpec{
			"team-1": {
				ScopeKey:  scGrantsScopeKey,
				Validator: scValidatorV1,
				WarmRoot:  "warm",
			},
		},
	}
}

// sourceCacheCapabilityRW builds a MODE_READ_WRITE capability.
func sourceCacheCapabilityRW(cacheGeneration, configFingerprint string) *v2.SourceCacheCapability {
	return v2.SourceCacheCapability_builder{
		Mode:              v2.SourceCacheCapability_MODE_READ_WRITE,
		CacheGeneration:   cacheGeneration,
		ConfigFingerprint: configFingerprint,
	}.Build()
}

// runSourceCacheSync runs one full sync of the run into c1zPath and returns
// the connector-side lookup events observed during it. prevPath, when
// non-empty, arms WithPreviousSyncC1ZPath.
func runSourceCacheSync(
	t *testing.T,
	ctx context.Context,
	run *chaosconnector.Run,
	transport chaosTransport,
	c1zPath string,
	tmpDir string,
	prevPath string,
	opts ...SyncOpt,
) []chaosconnector.SourceCacheLookupEvent {
	t.Helper()
	run.ResetSourceCacheLookupEvents()
	if prevPath != "" {
		opts = append(opts, WithPreviousSyncC1ZPath(prevPath))
	}
	h := newChaosHarness(t, ctx, run, c1zPath, tmpDir, transport, opts...)
	h.SyncAndClose(t, ctx)
	return run.SourceCacheLookupEvents()
}

// readSourceCacheSnapshot reads a sealed artifact's source-cache surfaces.
func readSourceCacheSnapshot(
	t *testing.T,
	ctx context.Context,
	path string,
	tmpDir string,
) chaosoracle.SourceCacheSnapshot {
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
	snapshot, err := chaosoracle.ReadSourceCacheSnapshot(ctx, store)
	require.NoError(t, err)
	return snapshot
}

// requireSourceCacheEvents asserts the connector-side lookup events match
// expectations exactly, in serve order, and that no event violates the R1
// contract (nil lookup, lookup error surfaced to the connector).
func requireSourceCacheEvents(
	t *testing.T,
	events []chaosconnector.SourceCacheLookupEvent,
	expected []chaosconnector.SourceCacheLookupEvent,
) {
	t.Helper()
	for i, event := range events {
		require.Falsef(t, event.LookupWasNil, "event %d: SyncOpAttrs.Lookup was nil (contract: NoopLookup substitutes)", i)
		require.Emptyf(t, event.LookupError, "event %d: lookup surfaced an error to the connector (contract: internal errors read as misses)", i)
	}
	require.Equal(t, expected, events)
}

// grantsKindScope is the snapshot key of the reference grants scope.
func grantsKindScope() string {
	return chaosoracle.KindScope(sourcecache.RowKindGrants, scGrantsScopeKey)
}

// sourceCachePaths allocates the per-generation c1z paths for a chain test.
func sourceCachePaths(t *testing.T, generations int) (string, []string) {
	t.Helper()
	tmpDir := t.TempDir()
	out := make([]string, generations)
	for i := range out {
		out[i] = filepath.Join(tmpDir, "gen-"+string(rune('a'+i))+".c1z")
	}
	return tmpDir, out
}
