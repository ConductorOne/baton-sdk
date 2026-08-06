package sync //nolint:revive,nolintlint // backwards-compatible package name

// Randomized soak test for the parallel scheduler.
//
// Each iteration builds a random-but-deterministic (seeded) fan-out topology
// for several type-scoped resource types: the planner call and every cursor
// can spawn sibling cursors (EnqueuePageTokens), carry a continuation token
// (NextPageToken), or both. A random subset of cursors is poisoned to fail
// exactly once, which aborts the sync mid-batch; the test resumes from the
// checkpoint until the sync completes. At the end the store must contain
// exactly the expected entitlement and grant set — any lost, duplicated, or
// misrouted cursor shows up as a set mismatch.
//
// Run with -race. Iterations can be extended with BATON_SOAK_ITERATIONS.

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	native_sync "sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/conductorone/baton-sdk/internal/chaosconnector"
	"github.com/conductorone/baton-sdk/internal/testtier"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	et "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	gt "github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
)

type soakTypeTopology = chaosconnector.CursorGraph

func buildSoakTopology(r *rand.Rand) *soakTypeTopology {
	return chaosconnector.GenerateCursorGraph(r, 8, 35, 15)
}

func soakPoisonCount(t *soakTypeTopology) int {
	return len(t.PoisonedFirst) + len(t.PoisonedSecond)
}

// soakConnector serves the topology. Poisoned cursors fail exactly once
// across all sync attempts; state is mutex-guarded because workers call
// concurrently.
type soakConnector struct {
	*mockConnector
	topos       map[string]*soakTypeTopology
	entsByToken map[string]map[string]*v2.Entitlement // type -> token -> payload
	grantsByTok map[string]map[string]*v2.Grant

	mu               native_sync.Mutex
	failedOnce       map[string]struct{}
	perResourceCalls int
}

func (c *soakConnector) failOnce(phase, resourceType, token string, poisoned map[string]bool) error {
	if !poisoned[token] {
		return nil
	}
	key := phase + "|" + resourceType + "|" + token
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.failedOnce[key]; ok {
		return nil
	}
	c.failedOnce[key] = struct{}{}
	return fmt.Errorf("soak: injected failure for %s", key)
}

func (c *soakConnector) countPerResourceCall() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.perResourceCalls++
}

func (c *soakConnector) ListEntitlements(
	_ context.Context,
	in *v2.EntitlementsServiceListEntitlementsRequest,
	_ ...grpc.CallOption,
) (*v2.EntitlementsServiceListEntitlementsResponse, error) {
	reqAnnos := annotations.Annotations(in.GetAnnotations())
	if !reqAnnos.Contains(&v2.TypeScopedEntitlements{}) {
		c.countPerResourceCall()
		return v2.EntitlementsServiceListEntitlementsResponse_builder{}.Build(), nil
	}
	resourceType := in.GetResource().GetId().GetResourceType()
	topo := c.topos[resourceType]
	token := in.GetPageToken()
	if err := c.failOnce("ent", resourceType, token, topo.PoisonedFirst); err != nil {
		return nil, err
	}
	builder := v2.EntitlementsServiceListEntitlementsResponse_builder{}
	page := topo.Pages[token]
	children := page.Spawn
	builder.NextPageToken = page.Next
	if token != "" {
		builder.List = []*v2.Entitlement{c.entsByToken[resourceType][token]}
	}
	if len(children) > 0 {
		builder.Annotations = annotations.New(v2.EnqueuePageTokens_builder{
			PageTokens: children,
		}.Build())
	}
	return builder.Build(), nil
}

func (c *soakConnector) ListGrants(
	_ context.Context,
	in *v2.GrantsServiceListGrantsRequest,
	_ ...grpc.CallOption,
) (*v2.GrantsServiceListGrantsResponse, error) {
	reqAnnos := annotations.Annotations(in.GetAnnotations())
	if !reqAnnos.Contains(&v2.TypeScopedGrants{}) {
		c.countPerResourceCall()
		return v2.GrantsServiceListGrantsResponse_builder{}.Build(), nil
	}
	resourceType := in.GetResource().GetId().GetResourceType()
	topo := c.topos[resourceType]
	token := in.GetPageToken()
	if err := c.failOnce("grant", resourceType, token, topo.PoisonedSecond); err != nil {
		return nil, err
	}
	builder := v2.GrantsServiceListGrantsResponse_builder{}
	page := topo.Pages[token]
	children := page.Spawn
	builder.NextPageToken = page.Next
	if token != "" {
		builder.List = []*v2.Grant{c.grantsByTok[resourceType][token]}
	}
	if len(children) > 0 {
		builder.Annotations = annotations.New(v2.EnqueuePageTokens_builder{
			PageTokens: children,
		}.Build())
	}
	return builder.Build(), nil
}

func soakSeeds(t *testing.T) []int64 {
	seeds := []int64{1, 2, 3, 4, 5, 6}
	if extra := os.Getenv("BATON_SOAK_ITERATIONS"); extra != "" {
		n, err := strconv.Atoi(extra)
		require.NoError(t, err, "BATON_SOAK_ITERATIONS must be an integer")
		for i := 0; i < n; i++ {
			seeds = append(seeds, rand.Int63()) //nolint:gosec // deterministic test topology, not crypto
		}
	}
	return seeds
}

func TestSchedulerSoakRandomizedFanoutWithFailures(t *testing.T) {
	testtier.RequireNightly(t)
	for _, seed := range soakSeeds(t) {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			runSchedulerSoak(t, seed)
		})
	}
}

func runSchedulerSoak(t *testing.T, seed int64) {
	t.Logf("scheduler soak seed=%d (re-run with runSchedulerSoak(t, %d))", seed, seed)
	r := rand.New(rand.NewSource(seed)) //nolint:gosec // deterministic test topology, not crypto
	ctx := t.Context()
	tmpDir := t.TempDir()

	userType := v2.ResourceType_builder{
		Id:          "user",
		DisplayName: "User",
		Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER},
		Annotations: annotations.New(&v2.SkipEntitlementsAndGrants{}),
	}.Build()
	user, err := rs.NewUserResource("user-1", userType, "User 1", nil)
	require.NoError(t, err)

	connector := &soakConnector{
		mockConnector: newMockConnector(),
		topos:         make(map[string]*soakTypeTopology),
		entsByToken:   make(map[string]map[string]*v2.Entitlement),
		grantsByTok:   make(map[string]map[string]*v2.Grant),
		failedOnce:    make(map[string]struct{}),
	}
	connector.rtDB = append(connector.rtDB, userType)
	connector.resourceDB["user"] = append(connector.resourceDB["user"], user)

	expectedEntIDs := make(map[string]struct{})
	totalPoisoned := 0
	numTypes := 2 + r.Intn(3)
	for ti := 0; ti < numTypes; ti++ {
		typeID := fmt.Sprintf("group%d", ti)
		groupType := v2.ResourceType_builder{
			Id:          typeID,
			DisplayName: typeID,
			Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_GROUP},
			Annotations: annotations.New(&v2.TypeScopedEntitlements{}, &v2.TypeScopedGrants{}),
		}.Build()
		connector.rtDB = append(connector.rtDB, groupType)

		numResources := 1 + r.Intn(4)
		resources := make([]*v2.Resource, 0, numResources)
		for ri := 0; ri < numResources; ri++ {
			res, err := rs.NewGroupResource(fmt.Sprintf("%s-res%d", typeID, ri), groupType, fmt.Sprintf("%s res %d", typeID, ri), nil)
			require.NoError(t, err)
			resources = append(resources, res)
			connector.resourceDB[typeID] = append(connector.resourceDB[typeID], res)
		}

		topo := buildSoakTopology(r)
		connector.topos[typeID] = topo
		totalPoisoned += soakPoisonCount(topo)
		connector.entsByToken[typeID] = make(map[string]*v2.Entitlement, len(topo.Tokens))
		connector.grantsByTok[typeID] = make(map[string]*v2.Grant, len(topo.Tokens))
		for i, token := range topo.Tokens {
			res := resources[i%numResources]
			slug := "m-" + token
			ent := et.NewAssignmentEntitlement(res, slug, et.WithGrantableTo(userType))
			grant := gt.NewGrant(res, slug, user.GetId())
			connector.entsByToken[typeID][token] = ent
			connector.grantsByTok[typeID][token] = grant
			expectedEntIDs[ent.GetId()] = struct{}{}
		}
	}

	store, err := dotc1z.NewStore(ctx, filepath.Join(tmpDir, "soak.c1z"),
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithTmpDir(tmpDir),
	)
	require.NoError(t, err)
	workers := 1 + r.Intn(8)
	t.Logf("soak topology: types=%d expected=%d poisoned=%d workers=%d", numTypes, len(expectedEntIDs), totalPoisoned, workers)
	s, err := NewSyncer(ctx, connector,
		WithConnectorStore(store),
		WithTmpDir(tmpDir),
		WithWorkerCount(workers),
	)
	require.NoError(t, err)
	// Every soak execution doubles as a queue-contract check: the audit
	// records each scheduler event and verifyQueueAudit replays the log
	// against the exactly-once/dedup/abort/accounting properties.
	audit := attachQueueAudit(t, s)

	// Each poisoned cursor fails at most once ever, so the sync needs at
	// most one resume per poisoned cursor. With workers > 1 a single
	// attempt can consume several poison pills at once.
	maxAttempts := totalPoisoned + 3
	attempts := 0
	for {
		attempts++
		err = s.Sync(ctx)
		if err == nil {
			break
		}
		require.ErrorContains(t, err, "soak: injected failure",
			"sync failed with a non-injected error on attempt %d", attempts)
		require.Less(t, attempts, maxAttempts, "sync did not converge after %d attempts: %v", attempts, err)
	}
	t.Logf("soak converged after %d sync attempts", attempts)

	// Completeness: the store must hold exactly the expected entitlement
	// set, and exactly one grant per entitlement.
	gotEntIDs := make(map[string]struct{})
	pageToken := ""
	for {
		resp, err := store.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{
			PageToken: pageToken,
		}.Build())
		require.NoError(t, err)
		for _, ent := range resp.GetList() {
			gotEntIDs[ent.GetId()] = struct{}{}
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	require.Equal(t, expectedEntIDs, gotEntIDs, "stored entitlement set diverged from topology")

	gotGrantEnts := make(map[string]struct{})
	grantCount := 0
	pageToken = ""
	for {
		resp, err := store.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{
			PageToken: pageToken,
		}.Build())
		require.NoError(t, err)
		for _, grant := range resp.GetList() {
			grantCount++
			gotGrantEnts[grant.GetEntitlement().GetId()] = struct{}{}
			require.Equal(t, user.GetId().GetResource(), grant.GetPrincipal().GetId().GetResource())
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	require.Equal(t, expectedEntIDs, gotGrantEnts, "stored grant set diverged from topology")
	require.Equal(t, len(expectedEntIDs), grantCount, "duplicate grants stored")

	require.NoError(t, s.Close(ctx))
	require.Zero(t, connector.perResourceCalls, "type-scoped types must never receive per-resource calls")
	verifyQueueAudit(t, audit)
}
