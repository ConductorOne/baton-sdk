package expand

import (
	"context"
	"errors"
	"strconv"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MockExpanderStore implements ExpanderStore for testing purposes.
type MockExpanderStore struct {
	entitlements map[string]*v2.Entitlement
	grants       map[string][]*v2.Grant // keyed by entitlement ID
	putGrants    []*v2.Grant            // grants that were written
	// pageSize > 0 paginates ListGrantsForEntitlement: PageToken is an integer
	// offset into the (filtered) grant slice. 0 (default) returns everything in
	// one page, leaving existing tests unaffected.
	pageSize int
}

// NewMockExpanderStore creates a new mock store for testing.
func NewMockExpanderStore() *MockExpanderStore {
	return &MockExpanderStore{
		entitlements: make(map[string]*v2.Entitlement),
		grants:       make(map[string][]*v2.Grant),
		putGrants:    make([]*v2.Grant, 0),
	}
}

// AddEntitlement adds an entitlement to the mock store.
func (m *MockExpanderStore) AddEntitlement(e *v2.Entitlement) {
	m.entitlements[e.GetId()] = e
}

// AddGrant adds a grant to the mock store.
func (m *MockExpanderStore) AddGrant(g *v2.Grant) {
	entID := g.GetEntitlement().GetId()
	m.grants[entID] = append(m.grants[entID], g)
}

// GetPutGrants returns all grants that were written via PutGrants.
func (m *MockExpanderStore) GetPutGrants() []*v2.Grant {
	return m.putGrants
}

func (m *MockExpanderStore) GetEntitlement(
	_ context.Context,
	req *reader_v2.EntitlementsReaderServiceGetEntitlementRequest,
) (*reader_v2.EntitlementsReaderServiceGetEntitlementResponse, error) {
	ent, ok := m.entitlements[req.GetEntitlementId()]
	if !ok {
		return nil, nil
	}
	return reader_v2.EntitlementsReaderServiceGetEntitlementResponse_builder{
		Entitlement: ent,
	}.Build(), nil
}

func (m *MockExpanderStore) ListGrantsForEntitlement(
	_ context.Context,
	req *reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest,
) (*reader_v2.GrantsReaderServiceListGrantsForEntitlementResponse, error) {
	entID := req.GetEntitlement().GetId()
	grants := m.grants[entID]

	// Filter by principal if specified
	principalID := req.GetPrincipalId() //nolint:staticcheck // ignore deprecated field
	if principalID != nil {
		filtered := make([]*v2.Grant, 0)
		for _, g := range grants {
			if g.GetPrincipal().GetId().GetResource() == principalID.GetResource() &&
				g.GetPrincipal().GetId().GetResourceType() == principalID.GetResourceType() {
				filtered = append(filtered, g)
			}
		}
		grants = filtered
	}

	// Filter by resource type IDs if specified
	resourceTypeIDs := req.GetPrincipalResourceTypeIds() //nolint:staticcheck // ignore deprecated field
	if len(resourceTypeIDs) > 0 {
		filtered := make([]*v2.Grant, 0)
		for _, g := range grants {
			for _, rtID := range resourceTypeIDs {
				if g.GetPrincipal().GetId().GetResourceType() == rtID {
					filtered = append(filtered, g)
					break
				}
			}
		}
		grants = filtered
	}

	nextPageToken := ""
	if m.pageSize > 0 {
		offset := 0
		if tok := req.GetPageToken(); tok != "" {
			offset, _ = strconv.Atoi(tok)
		}
		if offset > len(grants) {
			offset = len(grants)
		}
		end := offset + m.pageSize
		if end >= len(grants) {
			end = len(grants)
		} else {
			nextPageToken = strconv.Itoa(end)
		}
		grants = grants[offset:end]
	}

	return reader_v2.GrantsReaderServiceListGrantsForEntitlementResponse_builder{
		List:          grants,
		NextPageToken: nextPageToken,
	}.Build(), nil
}

func (m *MockExpanderStore) PutGrants(_ context.Context, grants ...*v2.Grant) error {
	m.putGrants = append(m.putGrants, grants...)
	// Also add to the grants map so they can be queried
	for _, g := range grants {
		entID := g.GetEntitlement().GetId()
		m.grants[entID] = append(m.grants[entID], g)
	}
	return nil
}

func (m *MockExpanderStore) StoreExpandedGrants(_ context.Context, grants ...*v2.Grant) error {
	return m.PutGrants(context.Background(), grants...)
}

// Helper functions for creating test data

func makeResourceID(resourceType, resource string) *v2.ResourceId {
	return v2.ResourceId_builder{
		ResourceType: resourceType,
		Resource:     resource,
	}.Build()
}

func makeResource(resourceType, resource string) *v2.Resource {
	return v2.Resource_builder{
		Id: makeResourceID(resourceType, resource),
	}.Build()
}

func makeEntitlement(id string, resource *v2.Resource) *v2.Entitlement {
	return v2.Entitlement_builder{
		Id:       id,
		Resource: resource,
	}.Build()
}

func makeGrant(id string, entitlement *v2.Entitlement, principal *v2.Resource) *v2.Grant {
	return v2.Grant_builder{
		Id:          id,
		Entitlement: entitlement,
		Principal:   principal,
	}.Build()
}

func TestExpanderWithMockStore(t *testing.T) {
	ctx := context.Background()
	store := NewMockExpanderStore()

	// Create test resources
	groupResource := makeResource("group", "admins")
	userResource := makeResource("user", "alice")

	// Create entitlements
	// Entitlement A: "member of admins group"
	entA := makeEntitlement("ent:group:admins:member", groupResource)
	// Entitlement B: "admin role" (descendant - users who are members of admins get this)
	entB := makeEntitlement("ent:role:admin", groupResource)

	store.AddEntitlement(entA)
	store.AddEntitlement(entB)

	// Create a grant: Alice is a member of the admins group
	grantA := makeGrant("grant:alice:member", entA, userResource)
	store.AddGrant(grantA)

	// Build the entitlement graph with an edge from A -> B
	// This means: principals who have entitlement A should also get entitlement B
	graph := NewEntitlementGraph(ctx)
	graph.AddEntitlementID(entA.GetId())
	graph.AddEntitlementID(entB.GetId())
	err := graph.AddEdge(ctx, entA.GetId(), entB.GetId(), false, []string{"user"})
	require.NoError(t, err)

	// Create and run the expander
	expander := NewExpander(store, graph)
	err = expander.Run(ctx)
	require.NoError(t, err)

	// Verify that Alice now has a grant on entitlement B
	putGrants := store.GetPutGrants()
	require.Len(t, putGrants, 1)

	expandedGrant := putGrants[0]
	require.Equal(t, entB.GetId(), expandedGrant.GetEntitlement().GetId())
	require.Equal(t, userResource.GetId().GetResource(), expandedGrant.GetPrincipal().GetId().GetResource())

	// Verify the source tracking
	sources := expandedGrant.GetSources().GetSources()
	require.NotNil(t, sources)
	require.Contains(t, sources, entA.GetId())

	// Source grant on entA is direct (Alice has entA directly), so IsDirect must be true.
	require.True(t, sources[entA.GetId()].GetIsDirect())
}

func TestExpanderStepByStep(t *testing.T) {
	ctx := context.Background()
	store := NewMockExpanderStore()

	// Create test resources
	groupResource := makeResource("group", "admins")
	userResource := makeResource("user", "alice")

	// Create entitlements
	entA := makeEntitlement("ent:group:admins:member", groupResource)
	entB := makeEntitlement("ent:role:admin", groupResource)

	store.AddEntitlement(entA)
	store.AddEntitlement(entB)

	// Create a grant
	grantA := makeGrant("grant:alice:member", entA, userResource)
	store.AddGrant(grantA)

	// Build graph
	graph := NewEntitlementGraph(ctx)
	graph.AddEntitlementID(entA.GetId())
	graph.AddEntitlementID(entB.GetId())
	err := graph.AddEdge(ctx, entA.GetId(), entB.GetId(), false, []string{"user"})
	require.NoError(t, err)

	// Create expander and run step by step
	expander := NewExpander(store, graph)

	// First step: should generate actions and not be done
	err = expander.RunSingleStep(ctx)
	require.NoError(t, err)
	require.False(t, expander.IsDone(ctx))

	// Actions should be generated: one action per (source, filter, batch),
	// fanning the single A→B edge out to one destination.
	require.Len(t, graph.Actions, 1)
	require.Equal(t, entA.GetId(), graph.Actions[0].SourceEntitlementID)
	require.Len(t, graph.Actions[0].Descendants, 1)
	require.Equal(t, entB.GetId(), graph.Actions[0].Descendants[0].EntitlementID)

	// Second step: should process the action and complete (action completes and graph is expanded in same step)
	err = expander.RunSingleStep(ctx)
	require.NoError(t, err)
	require.True(t, expander.IsDone(ctx))

	// Verify expansion happened
	require.True(t, graph.IsExpanded())
}

func TestExpanderWithCycle(t *testing.T) {
	ctx := context.Background()
	store := NewMockExpanderStore()

	// Create test resources
	groupResource := makeResource("group", "team")
	userResource := makeResource("user", "bob")

	// Create entitlements that form a cycle
	entA := makeEntitlement("ent:a", groupResource)
	entB := makeEntitlement("ent:b", groupResource)

	store.AddEntitlement(entA)
	store.AddEntitlement(entB)

	// Create a grant
	grantA := makeGrant("grant:bob:a", entA, userResource)
	store.AddGrant(grantA)

	// Build graph with a cycle: A -> B -> A
	graph := NewEntitlementGraph(ctx)
	graph.AddEntitlementID(entA.GetId())
	graph.AddEntitlementID(entB.GetId())
	err := graph.AddEdge(ctx, entA.GetId(), entB.GetId(), false, []string{"user"})
	require.NoError(t, err)
	err = graph.AddEdge(ctx, entB.GetId(), entA.GetId(), false, []string{"user"})
	require.NoError(t, err)

	// Fix cycles before expansion
	comps, _ := graph.ComputeCyclicComponents(ctx)
	require.Len(t, comps, 1) // Should detect one cycle

	err = graph.FixCyclesFromComponents(ctx, comps)
	require.NoError(t, err)

	// Now create expander and run
	expander := NewExpander(store, graph)
	err = expander.Run(ctx)
	require.NoError(t, err)
	require.True(t, graph.IsExpanded())
}

func TestExpanderMultiLevel(t *testing.T) {
	ctx := context.Background()
	store := NewMockExpanderStore()

	// Create test resources
	groupResource := makeResource("group", "org")
	userResource := makeResource("user", "charlie")

	// Create a chain of entitlements: A -> B -> C
	entA := makeEntitlement("ent:level1", groupResource)
	entB := makeEntitlement("ent:level2", groupResource)
	entC := makeEntitlement("ent:level3", groupResource)

	store.AddEntitlement(entA)
	store.AddEntitlement(entB)
	store.AddEntitlement(entC)

	// Create initial grant at level 1
	grantA := makeGrant("grant:charlie:level1", entA, userResource)
	store.AddGrant(grantA)

	// Build graph
	graph := NewEntitlementGraph(ctx)
	graph.AddEntitlementID(entA.GetId())
	graph.AddEntitlementID(entB.GetId())
	graph.AddEntitlementID(entC.GetId())
	err := graph.AddEdge(ctx, entA.GetId(), entB.GetId(), false, []string{"user"})
	require.NoError(t, err)
	err = graph.AddEdge(ctx, entB.GetId(), entC.GetId(), false, []string{"user"})
	require.NoError(t, err)

	// Create and run expander
	expander := NewExpander(store, graph)
	err = expander.Run(ctx)
	require.NoError(t, err)

	// Charlie should now have grants on all three levels
	putGrants := store.GetPutGrants()
	require.Len(t, putGrants, 2) // B and C (A was already granted)

	// Index expanded grants by entitlement ID for inspection.
	grantsByEnt := make(map[string]*v2.Grant)
	for _, g := range putGrants {
		grantsByEnt[g.GetEntitlement().GetId()] = g
	}
	require.Contains(t, grantsByEnt, entB.GetId())
	require.Contains(t, grantsByEnt, entC.GetId())

	// Grant on B: source is A. Charlie has A directly, so IsDirect = true (depth 0).
	sourcesB := grantsByEnt[entB.GetId()].GetSources().GetSources()
	require.Contains(t, sourcesB, entA.GetId())
	require.True(t, sourcesB[entA.GetId()].GetIsDirect(), "source A on grant B should be direct (depth 0)")

	// Grant on C: source is B. Charlie has B only through expansion, so IsDirect = false (depth > 0).
	sourcesC := grantsByEnt[entC.GetId()].GetSources().GetSources()
	require.Contains(t, sourcesC, entB.GetId())
	require.False(t, sourcesC[entB.GetId()].GetIsDirect(), "source B on grant C should be transitive (depth > 0)")
}

// TestExpanderDiamondGraph tests the diamond case where two direct grants
// both expand to the same descendant entitlement. Both sources should be
// marked IsDirect=true since the principal holds both source entitlements directly.
//
//	Alice → A ─┐
//	Alice → B ─┴→ C
func TestExpanderDiamondGraph(t *testing.T) {
	ctx := context.Background()
	store := NewMockExpanderStore()

	groupResource := makeResource("group", "org")
	userResource := makeResource("user", "alice")

	entA := makeEntitlement("ent:a", groupResource)
	entB := makeEntitlement("ent:b", groupResource)
	entC := makeEntitlement("ent:c", groupResource)

	store.AddEntitlement(entA)
	store.AddEntitlement(entB)
	store.AddEntitlement(entC)

	// Alice has both A and B directly.
	store.AddGrant(makeGrant("grant:alice:a", entA, userResource))
	store.AddGrant(makeGrant("grant:alice:b", entB, userResource))

	// Both A and B expand to C.
	graph := NewEntitlementGraph(ctx)
	graph.AddEntitlementID(entA.GetId())
	graph.AddEntitlementID(entB.GetId())
	graph.AddEntitlementID(entC.GetId())
	err := graph.AddEdge(ctx, entA.GetId(), entC.GetId(), false, []string{"user"})
	require.NoError(t, err)
	err = graph.AddEdge(ctx, entB.GetId(), entC.GetId(), false, []string{"user"})
	require.NoError(t, err)

	expander := NewExpander(store, graph)
	err = expander.Run(ctx)
	require.NoError(t, err)

	// Find Alice's grant on C (it may have been written multiple times; take the last version).
	var grantC *v2.Grant
	for _, g := range store.GetPutGrants() {
		if g.GetEntitlement().GetId() == entC.GetId() {
			grantC = g
		}
	}
	require.NotNil(t, grantC)

	sourcesC := grantC.GetSources().GetSources()
	require.Contains(t, sourcesC, entA.GetId())
	require.Contains(t, sourcesC, entB.GetId())

	// Both A and B are direct grants, so both sources should be IsDirect=true.
	require.True(t, sourcesC[entA.GetId()].GetIsDirect(), "source A should be direct (Alice has A directly)")
	require.True(t, sourcesC[entB.GetId()].GetIsDirect(), "source B should be direct (Alice has B directly)")
}

// TestExpanderMixedDirectness tests a diamond where one path is direct and
// the other is transitive, verifying IsDirect differs per source.
//
//	Alice → A → B ─┐
//	                └→ C
//	Alice → A ──────┘  (A is direct, B is transitive)
func TestExpanderMixedDirectness(t *testing.T) {
	ctx := context.Background()
	store := NewMockExpanderStore()

	groupResource := makeResource("group", "org")
	userResource := makeResource("user", "alice")

	entA := makeEntitlement("ent:a", groupResource)
	entB := makeEntitlement("ent:b", groupResource)
	entC := makeEntitlement("ent:c", groupResource)

	store.AddEntitlement(entA)
	store.AddEntitlement(entB)
	store.AddEntitlement(entC)

	// Alice has A directly.
	store.AddGrant(makeGrant("grant:alice:a", entA, userResource))

	// A → B and A → C, B → C (so C is reachable from A directly and from B transitively).
	graph := NewEntitlementGraph(ctx)
	graph.AddEntitlementID(entA.GetId())
	graph.AddEntitlementID(entB.GetId())
	graph.AddEntitlementID(entC.GetId())
	err := graph.AddEdge(ctx, entA.GetId(), entB.GetId(), false, []string{"user"})
	require.NoError(t, err)
	err = graph.AddEdge(ctx, entA.GetId(), entC.GetId(), false, []string{"user"})
	require.NoError(t, err)
	err = graph.AddEdge(ctx, entB.GetId(), entC.GetId(), false, []string{"user"})
	require.NoError(t, err)

	expander := NewExpander(store, graph)
	err = expander.Run(ctx)
	require.NoError(t, err)

	// Find Alice's grant on C.
	var grantC *v2.Grant
	for _, g := range store.GetPutGrants() {
		if g.GetEntitlement().GetId() == entC.GetId() {
			grantC = g
		}
	}
	require.NotNil(t, grantC)

	sourcesC := grantC.GetSources().GetSources()

	// Source A: Alice has A directly → IsDirect=true
	require.Contains(t, sourcesC, entA.GetId())
	require.True(t, sourcesC[entA.GetId()].GetIsDirect(), "source A should be direct")

	// Source B: Alice has B only through expansion from A → IsDirect=false
	require.Contains(t, sourcesC, entB.GetId())
	require.False(t, sourcesC[entB.GetId()].GetIsDirect(), "source B should be transitive")
}

// countingMockStore wraps MockExpanderStore and records, per entitlement ID,
// how many times its grants were read via an unfiltered (no PrincipalId)
// ListGrantsForEntitlement call. That is the "source read" the grouping change
// is meant to deduplicate: one read per (source, filter) instead of one per
// outgoing edge.
type countingMockStore struct {
	*MockExpanderStore
	sourceReads map[string]int
}

func newCountingMockStore() *countingMockStore {
	return &countingMockStore{
		MockExpanderStore: NewMockExpanderStore(),
		sourceReads:       make(map[string]int),
	}
}

func (s *countingMockStore) ListGrantsForEntitlement(
	ctx context.Context,
	req *reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest,
) (*reader_v2.GrantsReaderServiceListGrantsForEntitlementResponse, error) {
	// Only count full-entitlement reads (the source read and descendant
	// prefetch). Per-principal reads carry a PrincipalId filter.
	if req.GetPrincipalId() == nil { //nolint:staticcheck // ignore deprecated field
		s.sourceReads[req.GetEntitlement().GetId()]++
	}
	return s.MockExpanderStore.ListGrantsForEntitlement(ctx, req)
}

// TestExpanderHighOutDegreeSourceSingleRead is the core read-amplification
// guard: one source feeding N destinations (all sharing the empty
// ResourceTypeIDs filter) must read the source's grants once per destination
// batch — ceil(N/K) reads — not once per edge. Every destination must still
// receive the expanded grant.
func TestExpanderHighOutDegreeSourceSingleRead(t *testing.T) {
	t.Setenv("BATON_GRAPH_EXPAND_DEST_BATCH_SIZE", "4")

	ctx := context.Background()
	store := newCountingMockStore()

	groupResource := makeResource("group", "org")
	userResource := makeResource("user", "alice")

	source := makeEntitlement("ent:source", groupResource)
	store.AddEntitlement(source)
	store.AddGrant(makeGrant("grant:alice:source", source, userResource))

	const numDests = 10
	graph := NewEntitlementGraph(ctx)
	graph.AddEntitlementID(source.GetId())
	dests := make([]*v2.Entitlement, 0, numDests)
	for i := 0; i < numDests; i++ {
		d := makeEntitlement("ent:dest:"+strconv.Itoa(i), groupResource)
		store.AddEntitlement(d)
		dests = append(dests, d)
		graph.AddEntitlementID(d.GetId())
		require.NoError(t, graph.AddEdge(ctx, source.GetId(), d.GetId(), false, nil))
	}

	expander := NewExpander(store, graph)
	require.NoError(t, expander.Run(ctx))

	// Source read once per batch of 4 destinations: ceil(10/4) = 3.
	require.Equal(t, 3, store.sourceReads[source.GetId()],
		"source must be read once per destination batch, not once per edge")

	// Every destination must have received alice's expanded grant.
	granted := make(map[string]bool)
	for _, g := range store.GetPutGrants() {
		if g.GetPrincipal().GetId().GetResource() == "alice" {
			granted[g.GetEntitlement().GetId()] = true
		}
	}
	for _, d := range dests {
		require.True(t, granted[d.GetId()], "destination %s must receive the expanded grant", d.GetId())
	}
}

// TestExpanderMixedResourceTypeIDsReadPerFilter verifies constraint C2: edges
// from one source with *different* ResourceTypeIDs filters cannot share a read.
// Each distinct filter gets its own source read, and the filter is honored
// (only matching principals are fanned out).
func TestExpanderMixedResourceTypeIDsReadPerFilter(t *testing.T) {
	t.Setenv("BATON_GRAPH_EXPAND_DEST_BATCH_SIZE", "16")

	ctx := context.Background()
	store := newCountingMockStore()

	groupResource := makeResource("group", "org")
	userPrincipal := makeResource("user", "alice")
	servicePrincipal := makeResource("service", "robot")

	source := makeEntitlement("ent:source", groupResource)
	destUser := makeEntitlement("ent:dest:user", groupResource)
	destService := makeEntitlement("ent:dest:service", groupResource)
	store.AddEntitlement(source)
	store.AddEntitlement(destUser)
	store.AddEntitlement(destService)

	// Source has both a user principal and a service principal.
	store.AddGrant(makeGrant("grant:alice:source", source, userPrincipal))
	store.AddGrant(makeGrant("grant:robot:source", source, servicePrincipal))

	graph := NewEntitlementGraph(ctx)
	graph.AddEntitlementID(source.GetId())
	graph.AddEntitlementID(destUser.GetId())
	graph.AddEntitlementID(destService.GetId())
	// Two edges, two different filters → two distinct source reads.
	require.NoError(t, graph.AddEdge(ctx, source.GetId(), destUser.GetId(), false, []string{"user"}))
	require.NoError(t, graph.AddEdge(ctx, source.GetId(), destService.GetId(), false, []string{"service"}))

	expander := NewExpander(store, graph)
	require.NoError(t, expander.Run(ctx))

	// One read per distinct filter, even though both edges share the source.
	require.Equal(t, 2, store.sourceReads[source.GetId()],
		"distinct ResourceTypeIDs filters must each get their own source read")

	// The user-filtered edge must only grant the user principal; the
	// service-filtered edge only the service principal.
	byEnt := make(map[string]map[string]bool)
	for _, g := range store.GetPutGrants() {
		ent := g.GetEntitlement().GetId()
		if byEnt[ent] == nil {
			byEnt[ent] = make(map[string]bool)
		}
		byEnt[ent][g.GetPrincipal().GetId().GetResource()] = true
	}
	require.True(t, byEnt[destUser.GetId()]["alice"], "user-filtered dest must grant alice")
	require.False(t, byEnt[destUser.GetId()]["robot"], "user-filtered dest must not grant the service principal")
	require.True(t, byEnt[destService.GetId()]["robot"], "service-filtered dest must grant the service principal")
	require.False(t, byEnt[destService.GetId()]["alice"], "service-filtered dest must not grant the user principal")
}

// TestExpanderMultiParentDestination verifies constraint C5: a destination fed
// by two different sources must wait until both parents are expanded and must
// record both as sources. The grouping change must not let a destination act
// before all incoming edges are processed.
func TestExpanderMultiParentDestination(t *testing.T) {
	ctx := context.Background()
	store := NewMockExpanderStore()

	groupResource := makeResource("group", "org")
	alice := makeResource("user", "alice")

	parentA := makeEntitlement("ent:parentA", groupResource)
	parentB := makeEntitlement("ent:parentB", groupResource)
	child := makeEntitlement("ent:child", groupResource)
	store.AddEntitlement(parentA)
	store.AddEntitlement(parentB)
	store.AddEntitlement(child)

	// Alice holds both parents directly.
	store.AddGrant(makeGrant("grant:alice:parentA", parentA, alice))
	store.AddGrant(makeGrant("grant:alice:parentB", parentB, alice))

	graph := NewEntitlementGraph(ctx)
	graph.AddEntitlementID(parentA.GetId())
	graph.AddEntitlementID(parentB.GetId())
	graph.AddEntitlementID(child.GetId())
	require.NoError(t, graph.AddEdge(ctx, parentA.GetId(), child.GetId(), false, nil))
	require.NoError(t, graph.AddEdge(ctx, parentB.GetId(), child.GetId(), false, nil))

	expander := NewExpander(store, graph)
	require.NoError(t, expander.Run(ctx))
	require.True(t, graph.IsExpanded())

	var childGrant *v2.Grant
	for _, g := range store.GetPutGrants() {
		if g.GetEntitlement().GetId() == child.GetId() && g.GetPrincipal().GetId().GetResource() == "alice" {
			childGrant = g
		}
	}
	require.NotNil(t, childGrant)
	sources := childGrant.GetSources().GetSources()
	require.Contains(t, sources, parentA.GetId(), "both parents must be recorded as sources")
	require.Contains(t, sources, parentB.GetId(), "both parents must be recorded as sources")
}

// TestExpanderPerDestinationShallow verifies constraint C1: a single source
// read fans out to two destinations with different Shallow settings, and each
// destination's own Shallow gate is applied independently. The shallow edge
// must drop the transitively-held source grant while the non-shallow edge keeps
// it.
func TestExpanderPerDestinationShallow(t *testing.T) {
	t.Setenv("BATON_GRAPH_EXPAND_DEST_BATCH_SIZE", "16")

	ctx := context.Background()
	store := newCountingMockStore()

	groupResource := makeResource("group", "org")
	alice := makeResource("user", "alice")

	source := makeEntitlement("ent:source", groupResource)
	other := makeEntitlement("ent:other", groupResource)
	destShallow := makeEntitlement("ent:dest:shallow", groupResource)
	destDeep := makeEntitlement("ent:dest:deep", groupResource)
	store.AddEntitlement(source)
	store.AddEntitlement(destShallow)
	store.AddEntitlement(destDeep)

	// Alice's grant on the source is transitive (its only source is some other
	// entitlement, not the source itself) → it does NOT qualify for a shallow
	// edge, but does for a non-shallow edge.
	transitive := makeGrant("grant:alice:source", source, alice)
	transitive.SetSources(v2.GrantSources_builder{Sources: map[string]*v2.GrantSources_GrantSource{
		other.GetId(): {IsDirect: false},
	}}.Build())
	store.AddGrant(transitive)

	graph := NewEntitlementGraph(ctx)
	graph.AddEntitlementID(source.GetId())
	graph.AddEntitlementID(destShallow.GetId())
	graph.AddEntitlementID(destDeep.GetId())
	// Same (empty) filter → one shared source read; different Shallow per edge.
	require.NoError(t, graph.AddEdge(ctx, source.GetId(), destShallow.GetId(), true, nil))
	require.NoError(t, graph.AddEdge(ctx, source.GetId(), destDeep.GetId(), false, nil))

	expander := NewExpander(store, graph)
	require.NoError(t, expander.Run(ctx))

	// Both shallow and deep edges share the same empty filter and the same
	// batch, so the source is read once.
	require.Equal(t, 1, store.sourceReads[source.GetId()],
		"two edges sharing a filter must share one source read despite differing Shallow")

	granted := make(map[string]bool)
	for _, g := range store.GetPutGrants() {
		if g.GetPrincipal().GetId().GetResource() == "alice" {
			granted[g.GetEntitlement().GetId()] = true
		}
	}
	require.False(t, granted[destShallow.GetId()],
		"shallow edge must drop a transitively-held source grant")
	require.True(t, granted[destDeep.GetId()],
		"non-shallow edge must keep the transitively-held source grant")
}

// errInjectStore wraps MockExpanderStore and returns a chosen error from
// GetEntitlement for specific entitlement IDs, so tests can drive runAction's
// error paths (a missing source vs a transient source-read failure).
type errInjectStore struct {
	*MockExpanderStore
	getEntErr map[string]error
}

func newErrInjectStore() *errInjectStore {
	return &errInjectStore{
		MockExpanderStore: NewMockExpanderStore(),
		getEntErr:         make(map[string]error),
	}
}

func (s *errInjectStore) GetEntitlement(
	ctx context.Context,
	req *reader_v2.EntitlementsReaderServiceGetEntitlementRequest,
) (*reader_v2.EntitlementsReaderServiceGetEntitlementResponse, error) {
	if err := s.getEntErr[req.GetEntitlementId()]; err != nil {
		return nil, err
	}
	return s.MockExpanderStore.GetEntitlement(ctx, req)
}

// TestExpanderSourceNotFoundCompletes: when the source entitlement is missing
// (GetEntitlement → codes.NotFound) during the walk, the batch's edges are
// dropped and expansion completes without error.
func TestExpanderSourceNotFoundCompletes(t *testing.T) {
	ctx := context.Background()
	store := newErrInjectStore()

	group := makeResource("group", "org")
	source := makeEntitlement("ent:source", group)
	dest := makeEntitlement("ent:dest", group)
	store.AddEntitlement(source)
	store.AddEntitlement(dest)

	graph := NewEntitlementGraph(ctx)
	graph.AddEntitlementID(source.GetId())
	graph.AddEntitlementID(dest.GetId())
	require.NoError(t, graph.AddEdge(ctx, source.GetId(), dest.GetId(), false, nil))

	store.getEntErr[source.GetId()] = status.Error(codes.NotFound, "entitlement not found")

	expander := NewExpander(store, graph)
	require.NoError(t, expander.Run(ctx), "missing source must complete, not error")
	require.True(t, graph.IsExpanded(), "the batch's edge must be dropped")
	require.Empty(t, graph.Edges, "no edges should remain")
}

// TestExpanderTransientErrorPreservesEdges: a transient (non-ErrNoRows) failure
// must propagate AND leave the graph's edges intact, so a retry can re-run the
// action instead of silently dropping its edges.
func TestExpanderTransientErrorPreservesEdges(t *testing.T) {
	ctx := context.Background()
	store := newErrInjectStore()

	group := makeResource("group", "org")
	source := makeEntitlement("ent:source", group)
	dest := makeEntitlement("ent:dest", group)
	store.AddEntitlement(source)
	store.AddEntitlement(dest)

	graph := NewEntitlementGraph(ctx)
	graph.AddEntitlementID(source.GetId())
	graph.AddEntitlementID(dest.GetId())
	require.NoError(t, graph.AddEdge(ctx, source.GetId(), dest.GetId(), false, nil))

	store.getEntErr[source.GetId()] = errors.New("transient boom")

	expander := NewExpander(store, graph)
	err := expander.Run(ctx)
	require.Error(t, err, "transient failure must propagate")
	require.False(t, graph.IsExpanded(), "edges must survive a transient failure")
	require.Len(t, graph.Edges, 1, "the edge must remain for retry")
}

// TestExpanderMultiPageSourceCrossPageDedup exercises the multi-page action
// lifecycle the batching rewrite touches: a source whose grants span several
// pages, with the SAME principal appearing on two different pages. The per-page
// destStates/byKey are rebuilt each page, so the second appearance must be
// rediscovered via the rebuilt oracle and take applyDestGrant's merge path —
// not create a duplicate grant. Also asserts the action stays queued with a
// page token mid-source, and that source reads == pages × batches.
func TestExpanderMultiPageSourceCrossPageDedup(t *testing.T) {
	t.Setenv("BATON_GRAPH_EXPAND_DEST_BATCH_SIZE", "2")

	ctx := context.Background()
	store := newCountingMockStore()
	store.pageSize = 2 // 5 source grants → 3 pages (2,2,1)

	group := makeResource("group", "org")
	alice := makeResource("user", "alice")
	bob := makeResource("user", "bob")
	carol := makeResource("user", "carol")
	dave := makeResource("user", "dave")

	source := makeEntitlement("ent:source", group)
	d1 := makeEntitlement("ent:d1", group)
	d2 := makeEntitlement("ent:d2", group)
	store.AddEntitlement(source)
	store.AddEntitlement(d1)
	store.AddEntitlement(d2)

	// alice holds TWO source grants (distinct records), landing on page 0 and
	// page 1 once paginated at size 2.
	store.AddGrant(makeGrant("g:alice:1", source, alice)) // page 0
	store.AddGrant(makeGrant("g:bob", source, bob))       // page 0
	store.AddGrant(makeGrant("g:alice:2", source, alice)) // page 1
	store.AddGrant(makeGrant("g:carol", source, carol))   // page 1
	store.AddGrant(makeGrant("g:dave", source, dave))     // page 2

	graph := NewEntitlementGraph(ctx)
	graph.AddEntitlementID(source.GetId())
	graph.AddEntitlementID(d1.GetId())
	graph.AddEntitlementID(d2.GetId())
	require.NoError(t, graph.AddEdge(ctx, source.GetId(), d1.GetId(), false, nil))
	require.NoError(t, graph.AddEdge(ctx, source.GetId(), d2.GetId(), false, nil))

	expander := NewExpander(store, graph)

	// Step 1: generate the action (no work yet, no page token).
	require.NoError(t, expander.RunSingleStep(ctx))
	require.Len(t, graph.Actions, 1)
	require.Empty(t, graph.Actions[0].PageToken)

	// Step 2: process page 0 — the action must stay queued with a page token.
	require.NoError(t, expander.RunSingleStep(ctx))
	require.Len(t, graph.Actions, 1, "action must remain queued while the source has more pages")
	require.NotEmpty(t, graph.Actions[0].PageToken, "page token must advance mid-source")

	// Finish.
	for !expander.IsDone(ctx) {
		require.NoError(t, expander.RunSingleStep(ctx))
	}

	// Source read once per page per batch. K=2 ≥ 2 dests → 1 batch; 3 pages → 3.
	require.Equal(t, 3, store.sourceReads[source.GetId()],
		"source read == pages × batches")

	// Exactly one expanded grant per (destination, principal) — no duplicate for
	// alice despite her appearing on two source pages.
	counts := make(map[string]int)
	for _, g := range store.GetPutGrants() {
		counts[g.GetEntitlement().GetId()+"|"+g.GetPrincipal().GetId().GetResource()]++
	}
	for _, d := range []*v2.Entitlement{d1, d2} {
		for _, p := range []string{"alice", "bob", "carol", "dave"} {
			require.Equal(t, 1, counts[d.GetId()+"|"+p],
				"exactly one grant for (%s, %s)", d.GetId(), p)
		}
	}

	// alice's grant records the source entitlement.
	for _, g := range store.GetPutGrants() {
		if g.GetEntitlement().GetId() == d1.GetId() && g.GetPrincipal().GetId().GetResource() == "alice" {
			require.Contains(t, g.GetSources().GetSources(), source.GetId(),
				"alice's expanded grant must record the source")
		}
	}
}

// pageCapStubStore always returns a non-empty NextPageToken so the prefetch
// loop never terminates naturally. Used to exercise the maxPrefetchPages cap.
type pageCapStubStore struct {
	MockExpanderStore
	calls int
}

func (s *pageCapStubStore) ListGrantsForEntitlement(
	_ context.Context,
	_ *reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest,
) (*reader_v2.GrantsReaderServiceListGrantsForEntitlementResponse, error) {
	s.calls++
	return reader_v2.GrantsReaderServiceListGrantsForEntitlementResponse_builder{
		List:          []*v2.Grant{},
		NextPageToken: "more",
	}.Build(), nil
}

// TestPrefetchDescendantPrincipals_PageCapExceeded locks in graceful
// degradation when the cap is hit: the helper must return complete=false
// (and no error) so runAction can drop the partial set and fall back to
// per-principal queries. Returning an error here would make the expander
// fail on very large descendant entitlements instead of just running slowly.
func TestPrefetchDescendantPrincipals_PageCapExceeded(t *testing.T) {
	store := &pageCapStubStore{MockExpanderStore: *NewMockExpanderStore()}
	ent := v2.Entitlement_builder{Id: "entitlement:1"}.Build()

	set, complete, err := prefetchDescendantPrincipals(context.Background(), store, ent)
	require.NoError(t, err)
	require.False(t, complete, "cap exceeded must report incomplete so caller falls back")
	require.NotNil(t, set)
	require.Equal(t, maxPrefetchPages, store.calls, "loop must iterate exactly the cap before giving up")
}

// TestExpanderInPageDuplicatePrincipal covers the case where multiple source
// grants in the same source-grants page share the same principal and the
// descendant entitlement has no pre-existing grant. The expander must emit
// exactly one expanded grant per (descendant, principal) — not one per
// source grant — because the deterministic grant ID would otherwise cause
// upsert collisions and lose source attribution.
func TestExpanderInPageDuplicatePrincipal(t *testing.T) {
	ctx := context.Background()
	store := NewMockExpanderStore()

	groupResource := makeResource("group", "team")
	userResource := makeResource("user", "alice")

	entA := makeEntitlement("ent:a", groupResource)
	entB := makeEntitlement("ent:b", groupResource)

	store.AddEntitlement(entA)
	store.AddEntitlement(entB)

	// Two source grants on entA for the same principal (same ResourceId).
	// This can happen when a connector emits multiple membership grants for
	// the same user (e.g. nested-group membership traversal).
	store.AddGrant(makeGrant("grant:alice:a:1", entA, userResource))
	store.AddGrant(makeGrant("grant:alice:a:2", entA, userResource))

	graph := NewEntitlementGraph(ctx)
	graph.AddEntitlementID(entA.GetId())
	graph.AddEntitlementID(entB.GetId())
	require.NoError(t, graph.AddEdge(ctx, entA.GetId(), entB.GetId(), false, []string{"user"}))

	expander := NewExpander(store, graph)
	require.NoError(t, expander.Run(ctx))

	// Count distinct expanded grants on entB for alice.
	distinct := make(map[string]*v2.Grant)
	for _, g := range store.GetPutGrants() {
		if g.GetEntitlement().GetId() != entB.GetId() {
			continue
		}
		if g.GetPrincipal().GetId().GetResource() != "alice" {
			continue
		}
		distinct[g.GetId()] = g
	}
	require.Len(t, distinct, 1, "two same-principal source grants must produce exactly one expanded grant")

	var only *v2.Grant
	for _, g := range distinct {
		only = g
	}
	sources := only.GetSources().GetSources()
	require.Contains(t, sources, entA.GetId(), "source attribution must be preserved")
}

// TestExpanderInPageDirectnessUpgrade covers the case where the same principal
// appears twice in one source-grants page — first via an indirect grant, then
// via a direct grant. The expanded descendant grant must record the source as
// direct: direct wins over indirect.
//
// In main this happened to work because every source grant re-queried the
// descendant grants from the store; the first iteration's write was still
// buffered (un-flushed), so the second iteration re-created the grant via
// newExpandedGrant and last-write-wins promoted IsDirect=true. The prefetch
// rewrite seeds an in-page cache instead, so the second iteration takes the
// merge path and must explicitly upgrade indirect→direct rather than dropping
// it.
func TestExpanderInPageDirectnessUpgrade(t *testing.T) {
	ctx := context.Background()
	store := NewMockExpanderStore()

	groupResource := makeResource("group", "team")
	userResource := makeResource("user", "alice")

	entA := makeEntitlement("ent:a", groupResource)
	entB := makeEntitlement("ent:b", groupResource)
	entOther := makeEntitlement("ent:other", groupResource)

	store.AddEntitlement(entA)
	store.AddEntitlement(entB)

	// First source grant: alice holds A only transitively (sources references
	// some other entitlement, not A itself) → indirect w.r.t. A.
	indirect := makeGrant("grant:alice:a:indirect", entA, userResource)
	indirect.SetSources(v2.GrantSources_builder{Sources: map[string]*v2.GrantSources_GrantSource{
		entOther.GetId(): {IsDirect: false},
	}}.Build())
	store.AddGrant(indirect)

	// Second source grant for the same principal: direct (no sources at all).
	direct := makeGrant("grant:alice:a:direct", entA, userResource)
	store.AddGrant(direct)

	graph := NewEntitlementGraph(ctx)
	graph.AddEntitlementID(entA.GetId())
	graph.AddEntitlementID(entB.GetId())
	require.NoError(t, graph.AddEdge(ctx, entA.GetId(), entB.GetId(), false, []string{"user"}))

	expander := NewExpander(store, graph)
	require.NoError(t, expander.Run(ctx))

	var grantB *v2.Grant
	for _, g := range store.GetPutGrants() {
		if g.GetEntitlement().GetId() == entB.GetId() && g.GetPrincipal().GetId().GetResource() == "alice" {
			grantB = g
		}
	}
	require.NotNil(t, grantB)

	sources := grantB.GetSources().GetSources()
	require.Contains(t, sources, entA.GetId(), "source attribution must be preserved")
	require.True(t, sources[entA.GetId()].GetIsDirect(),
		"a later direct source grant must upgrade the descendant source from indirect to direct")
}

// TestExpanderNilPrincipalSourceGrant locks in that a malformed source grant
// with no principal is a no-op that does NOT corrupt unrelated descendant
// grants.
//
// In main this was a real hazard: the nil principal flowed into
// ListGrantsForEntitlement as a nil PrincipalId, which the store interprets as
// "no filter" (pkg/dotc1z/grants.go) — so it would fetch *every* grant on the
// descendant entitlement and add the source entitlement (and a direct
// self-source) to all of them. The rewrite avoids this two ways: the explicit
// nil-principal skip, and structurally — the key-set/per-principal design
// never issues an unfiltered descendant query. This test guards that property
// against future regressions; an unrelated principal's pre-existing descendant
// grant must be left untouched.
func TestExpanderNilPrincipalSourceGrant(t *testing.T) {
	ctx := context.Background()
	store := NewMockExpanderStore()

	groupResource := makeResource("group", "team")
	bobResource := makeResource("user", "bob")

	entA := makeEntitlement("ent:a", groupResource)
	entB := makeEntitlement("ent:b", groupResource)

	store.AddEntitlement(entA)
	store.AddEntitlement(entB)

	// A malformed source grant on entA with no principal.
	nilPrincipalGrant := v2.Grant_builder{
		Id:          "grant:nil:a",
		Entitlement: entA,
		Principal:   nil,
	}.Build()
	store.AddGrant(nilPrincipalGrant)

	// An unrelated, pre-existing descendant grant on entB for bob, with its own
	// source already recorded. This must survive expansion unchanged — main
	// would have added entA (and a direct self-source) to it.
	bobGrantB := makeGrant("grant:bob:b", entB, bobResource)
	bobGrantB.SetSources(v2.GrantSources_builder{Sources: map[string]*v2.GrantSources_GrantSource{
		entB.GetId(): {IsDirect: true},
	}}.Build())
	store.AddGrant(bobGrantB)

	graph := NewEntitlementGraph(ctx)
	graph.AddEntitlementID(entA.GetId())
	graph.AddEntitlementID(entB.GetId())
	require.NoError(t, graph.AddEdge(ctx, entA.GetId(), entB.GetId(), false, []string{"user"}))

	expander := NewExpander(store, graph)
	require.NoError(t, expander.Run(ctx))

	// The nil-principal source grant must not have produced any new write that
	// references entA as a source on the descendant entitlement.
	for _, g := range store.GetPutGrants() {
		if g.GetEntitlement().GetId() != entB.GetId() {
			continue
		}
		require.NotContains(t, g.GetSources().GetSources(), entA.GetId(),
			"a nil-principal source grant must not add entA as a source to any descendant grant")
	}
}
