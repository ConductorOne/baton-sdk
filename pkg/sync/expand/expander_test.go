package expand

import (
	"context"
	"sync"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// capturingCore records emitted log entries in-memory so tests can assert
// on the message text and structured fields. Mirrors the helper in
// pkg/sync/progresslog/progresslog_test.go but kept local to this package
// to avoid a cross-package test-only dependency.
type capturingCore struct {
	zapcore.LevelEnabler
	mu      sync.Mutex
	entries []capturedEntry
}

type capturedEntry struct {
	Message string
	Fields  map[string]interface{}
}

func newCapturingCore() *capturingCore {
	return &capturingCore{LevelEnabler: zapcore.InfoLevel}
}

func (c *capturingCore) With([]zapcore.Field) zapcore.Core { return c }
func (c *capturingCore) Check(ent zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if c.Enabled(ent.Level) {
		return ce.AddCore(ent, c)
	}
	return ce
}

func (c *capturingCore) Write(ent zapcore.Entry, fields []zapcore.Field) error {
	enc := zapcore.NewMapObjectEncoder()
	for _, f := range fields {
		f.AddTo(enc)
	}
	c.mu.Lock()
	c.entries = append(c.entries, capturedEntry{Message: ent.Message, Fields: enc.Fields})
	c.mu.Unlock()
	return nil
}

func (c *capturingCore) Sync() error { return nil }

func (c *capturingCore) filterMessage(msg string) []capturedEntry {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]capturedEntry, 0, len(c.entries))
	for _, e := range c.entries {
		if e.Message == msg {
			out = append(out, e)
		}
	}
	return out
}

// MockExpanderStore implements ExpanderStore for testing purposes.
type MockExpanderStore struct {
	entitlements map[string]*v2.Entitlement
	grants       map[string][]*v2.Grant // keyed by entitlement ID
	putGrants    []*v2.Grant            // grants that were written
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
	principalID := req.GetPrincipalId()
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
	resourceTypeIDs := req.GetPrincipalResourceTypeIds()
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

	return reader_v2.GrantsReaderServiceListGrantsForEntitlementResponse_builder{
		List: grants,
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

	// Actions should be generated
	require.Len(t, graph.Actions, 1)
	require.Equal(t, entA.GetId(), graph.Actions[0].SourceEntitlementID)
	require.Equal(t, entB.GetId(), graph.Actions[0].DescendantEntitlementID)

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

// TestExpander_DedupQueuesEachEdgeOnce pins that the action-generation loop
// in RunSingleStep does not enqueue the same (source, descendant) edge more
// than once per depth, even when the iteration would naturally yield it
// repeatedly. The reproducer here is a node that holds the same entitlement
// ID twice — which can happen via SCC cycle reduction or manual graph
// construction. Without dedup, GetExpandableEntitlements yields the same
// source ID twice, the inner loop runs twice for that source, and we end
// up with two identical actions in the queue. The queue would then pay one
// connector ListGrants round-trip per duplicate even though MarkEdgeExpanded
// is idempotent.
//
// The test sidesteps the "queue already empty" gate by directly populating
// nodes with duplicate EntitlementIDs and stepping through the depth-bump
// branch.
func TestExpander_DedupQueuesEachEdgeOnce(t *testing.T) {
	ctx := context.Background()

	graph := NewEntitlementGraph(ctx)
	graph.AddEntitlementID("A")
	graph.AddEntitlementID("B")
	require.NoError(t, graph.AddEdge(ctx, "A", "B", false, []string{"user"}))

	// Mutate the source node so its EntitlementIDs slice contains "A"
	// twice. This is the SCC-merge-style reproducer for the duplicate-
	// yield path inside GetExpandableEntitlements; it would normally also
	// happen if a graph round-trip via JSON ever re-introduced a duplicate.
	srcNodeID, ok := graph.EntitlementsToNodes["A"]
	require.True(t, ok, "expected node for entitlement A")
	srcNode := graph.Nodes[srcNodeID]
	srcNode.EntitlementIDs = []string{"A", "A"}
	graph.Nodes[srcNodeID] = srcNode

	// Use a no-op store; the dedup branch runs before any store call,
	// so we never need to satisfy ExpanderStore for this test.
	expander := NewExpander(nil, graph)

	// First step: the queue is empty, so we hit the action-generation
	// branch. Without dedup we'd queue (A,B) twice; with dedup we queue
	// it exactly once.
	require.NoError(t, expander.RunSingleStep(ctx))
	require.Len(t, graph.Actions, 1,
		"action queue must contain (A,B) exactly once even though source A is yielded twice")
	require.Equal(t, "A", graph.Actions[0].SourceEntitlementID)
	require.Equal(t, "B", graph.Actions[0].DescendantEntitlementID)
}

// TestExpander_DedupHandlesDestinationSideDuplicate is the mirror of
// TestExpander_DedupQueuesEachEdgeOnce: the duplicate yield comes from the
// destination node holding the same descendant entitlement ID twice
// (GetExpandableDescendantEntitlements iterates destination.EntitlementIDs).
// SCC merging can produce this state on the destination side just as easily
// as on the source side, so the dedup must catch both. Without dedup,
// repeating the descendant ID would produce identical (A,B) pairs.
func TestExpander_DedupHandlesDestinationSideDuplicate(t *testing.T) {
	ctx := context.Background()

	graph := NewEntitlementGraph(ctx)
	graph.AddEntitlementID("A")
	graph.AddEntitlementID("B")
	require.NoError(t, graph.AddEdge(ctx, "A", "B", false, []string{"user"}))

	// Mutate the destination node's EntitlementIDs slice to contain "B"
	// twice. Inner-loop yields (A,B) twice; dedup must collapse them.
	dstNodeID, ok := graph.EntitlementsToNodes["B"]
	require.True(t, ok, "expected node for entitlement B")
	dstNode := graph.Nodes[dstNodeID]
	dstNode.EntitlementIDs = []string{"B", "B"}
	graph.Nodes[dstNodeID] = dstNode

	expander := NewExpander(nil, graph)
	require.NoError(t, expander.RunSingleStep(ctx))
	require.Len(t, graph.Actions, 1,
		"action queue must contain (A,B) exactly once even though descendant B is yielded twice")
	require.Equal(t, "A", graph.Actions[0].SourceEntitlementID)
	require.Equal(t, "B", graph.Actions[0].DescendantEntitlementID)
}

// TestExpander_DedupEmitsLogOnSkip pins the operator-visible signal: when
// the dedup actually fires (skipped > 0), an Info-level log line is emitted
// with `queued` and `skipped` fields and the package-conventional message.
// Operators grep for this to estimate the dedup win across syncs, so the
// message text and field names are an external contract and need a
// regression guard.
func TestExpander_DedupEmitsLogOnSkip(t *testing.T) {
	core := newCapturingCore()
	logger := zap.New(core)
	ctx := ctxzap.ToContext(context.Background(), logger)

	graph := NewEntitlementGraph(ctx)
	graph.AddEntitlementID("A")
	graph.AddEntitlementID("B")
	require.NoError(t, graph.AddEdge(ctx, "A", "B", false, []string{"user"}))

	srcNodeID, ok := graph.EntitlementsToNodes["A"]
	require.True(t, ok)
	srcNode := graph.Nodes[srcNodeID]
	srcNode.EntitlementIDs = []string{"A", "A"}
	graph.Nodes[srcNodeID] = srcNode

	expander := NewExpander(nil, graph)
	require.NoError(t, expander.RunSingleStep(ctx))

	entries := core.filterMessage("expander: pre-pruned duplicate (source, descendant) actions")
	require.Len(t, entries, 1, "dedup log line must be emitted exactly once when skips occur")
	require.EqualValues(t, 1, entries[0].Fields["queued"], "queued field must reflect actions that were enqueued")
	require.EqualValues(t, 1, entries[0].Fields["skipped"], "skipped field must reflect duplicates that were dropped")
}
