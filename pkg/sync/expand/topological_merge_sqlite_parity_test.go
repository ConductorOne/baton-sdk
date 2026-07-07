package expand

import (
	"context"
	"path/filepath"
	"sort"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/stretchr/testify/require"
)

// These tests pin the new topological/projection expanders to the SQLite
// expander output as ground truth. Unlike the mock-store parity tests, every
// run goes through a real SQLite c1z: the V2<->storage translation, the
// StoreExpandedGrants upsert/preserve path, and the real grant reader. The
// current source-batched expander on SQLite is the reference; each new
// algorithm must reproduce its grant set exactly (identity, principal, sources
// with IsDirect, and annotations).

// storeGrantSnapshot captures everything we assert on for a single read-back
// grant. discovered_at is intentionally excluded: it is stamped at write time
// and legitimately differs between two independent runs.
type storeGrantSnapshot struct {
	id              string
	entitlement     string
	principalRT     string
	principalID     string
	sourceDirect    map[string]bool
	annotationTypes []string
}

type sqliteGrantSpec struct {
	id            string
	entitlementID string
	principalRT   string
	principalID   string
	// sources, when set, is written as the grant's GrantSources so a test can
	// model a transitive (indirect) source grant.
	sources map[string]bool
}

type sqliteEdgeSpec struct {
	src     string
	dst     string
	shallow bool
	rtids   []string
}

type sqliteParityCase struct {
	name           string
	entitlementIDs []string
	grants         []sqliteGrantSpec
	edges          []sqliteEdgeSpec
}

func parityCases() []sqliteParityCase {
	return []sqliteParityCase{
		{
			name:           "chain",
			entitlementIDs: []string{"ent:a", "ent:b", "ent:c"},
			grants:         []sqliteGrantSpec{{id: "grant:alice:a", entitlementID: "ent:a", principalRT: "user", principalID: "alice"}},
			edges: []sqliteEdgeSpec{
				{src: "ent:a", dst: "ent:b", rtids: []string{"user"}},
				{src: "ent:b", dst: "ent:c", rtids: []string{"user"}},
			},
		},
		{
			name:           "diamond",
			entitlementIDs: []string{"ent:a", "ent:b", "ent:c"},
			grants: []sqliteGrantSpec{
				{id: "grant:alice:a", entitlementID: "ent:a", principalRT: "user", principalID: "alice"},
				{id: "grant:alice:b", entitlementID: "ent:b", principalRT: "user", principalID: "alice"},
			},
			edges: []sqliteEdgeSpec{
				{src: "ent:a", dst: "ent:c", rtids: []string{"user"}},
				{src: "ent:b", dst: "ent:c", rtids: []string{"user"}},
			},
		},
		{
			name:           "mixed_directness",
			entitlementIDs: []string{"ent:a", "ent:b", "ent:c"},
			grants:         []sqliteGrantSpec{{id: "grant:alice:a", entitlementID: "ent:a", principalRT: "user", principalID: "alice"}},
			edges: []sqliteEdgeSpec{
				{src: "ent:a", dst: "ent:b", rtids: []string{"user"}},
				{src: "ent:a", dst: "ent:c", rtids: []string{"user"}},
				{src: "ent:b", dst: "ent:c", rtids: []string{"user"}},
			},
		},
		{
			name:           "shallow_drops_transitive",
			entitlementIDs: []string{"ent:source", "ent:dest:shallow", "ent:dest:deep"},
			grants: []sqliteGrantSpec{{
				id: "grant:alice:source", entitlementID: "ent:source", principalRT: "user", principalID: "alice",
				sources: map[string]bool{"ent:other": false},
			}},
			edges: []sqliteEdgeSpec{
				{src: "ent:source", dst: "ent:dest:shallow", shallow: true},
				{src: "ent:source", dst: "ent:dest:deep", shallow: false},
			},
		},
		{
			name:           "existing_destination_grant",
			entitlementIDs: []string{"ent:source", "ent:dest"},
			grants: []sqliteGrantSpec{
				{id: "grant:alice:source", entitlementID: "ent:source", principalRT: "user", principalID: "alice"},
				{id: "grant:alice:dest", entitlementID: "ent:dest", principalRT: "user", principalID: "alice"},
			},
			edges: []sqliteEdgeSpec{{src: "ent:source", dst: "ent:dest"}},
		},
		{
			// Two parents feed one child, so the child has in-degree 2 and the
			// planner selects topological_projection with both parents as
			// projection sources. Each parent carries multiple distinct
			// principals, which exercises projectionContributionStream across
			// more than one principal key — the case that previously dropped
			// every other principal.
			name:           "high_fanin_multi_principal",
			entitlementIDs: []string{"ent:p1", "ent:p2", "ent:child"},
			grants: []sqliteGrantSpec{
				{id: "grant:u1:p1", entitlementID: "ent:p1", principalRT: "user", principalID: "u1"},
				{id: "grant:u2:p1", entitlementID: "ent:p1", principalRT: "user", principalID: "u2"},
				{id: "grant:u3:p1", entitlementID: "ent:p1", principalRT: "user", principalID: "u3"},
				{id: "grant:u2:p2", entitlementID: "ent:p2", principalRT: "user", principalID: "u2"},
				{id: "grant:u3:p2", entitlementID: "ent:p2", principalRT: "user", principalID: "u3"},
				{id: "grant:u4:p2", entitlementID: "ent:p2", principalRT: "user", principalID: "u4"},
			},
			edges: []sqliteEdgeSpec{
				{src: "ent:p1", dst: "ent:child", rtids: []string{"user"}},
				{src: "ent:p2", dst: "ent:child", rtids: []string{"user"}},
			},
		},
		{
			name:           "resource_type_filters",
			entitlementIDs: []string{"ent:source", "ent:dest:user", "ent:dest:service"},
			grants: []sqliteGrantSpec{
				{id: "grant:alice:source", entitlementID: "ent:source", principalRT: "user", principalID: "alice"},
				{id: "grant:robot:source", entitlementID: "ent:source", principalRT: "service", principalID: "robot"},
			},
			edges: []sqliteEdgeSpec{
				{src: "ent:source", dst: "ent:dest:user", rtids: []string{"user"}},
				{src: "ent:source", dst: "ent:dest:service", rtids: []string{"service"}},
			},
		},
	}
}

// cyclicCases produce collapsed super-nodes via FixCycles. They are kept out of
// the SQLite-ground-truth parity suite because the current source-batched
// expander is NONDETERMINISTIC on collapsed cycles with external edges — its
// per-edge expansion marks the super-node's shared outgoing edge expanded after
// whichever member entitlement is processed first (map-iteration order), so it
// can drop grants and attribute provenance to an arbitrary member. The new
// topological algorithms are deterministic, so these cases are verified for
// self-consistency and determinism instead (see TestTopologicalMergeCyclic).
func cyclicCases() []sqliteParityCase {
	return []sqliteParityCase{
		{
			name:           "two_cycle",
			entitlementIDs: []string{"ent:a", "ent:b"},
			grants: []sqliteGrantSpec{
				{id: "grant:alice:a", entitlementID: "ent:a", principalRT: "user", principalID: "alice"},
				{id: "grant:bob:b", entitlementID: "ent:b", principalRT: "user", principalID: "bob"},
			},
			edges: []sqliteEdgeSpec{
				{src: "ent:a", dst: "ent:b", rtids: []string{"user"}},
				{src: "ent:b", dst: "ent:a", rtids: []string{"user"}},
			},
		},
		{
			name:           "three_cycle",
			entitlementIDs: []string{"ent:a", "ent:b", "ent:c"},
			grants: []sqliteGrantSpec{
				{id: "grant:alice:a", entitlementID: "ent:a", principalRT: "user", principalID: "alice"},
				{id: "grant:bob:b", entitlementID: "ent:b", principalRT: "user", principalID: "bob"},
				{id: "grant:carol:c", entitlementID: "ent:c", principalRT: "user", principalID: "carol"},
			},
			edges: []sqliteEdgeSpec{
				{src: "ent:a", dst: "ent:b", rtids: []string{"user"}},
				{src: "ent:b", dst: "ent:c", rtids: []string{"user"}},
				{src: "ent:c", dst: "ent:a", rtids: []string{"user"}},
			},
		},
		{
			// S→A, A↔B, B→D: a super-node {A,B} that is both an external
			// destination (of S) and an external source (to D).
			name:           "cycle_with_external_edges",
			entitlementIDs: []string{"ent:s", "ent:a", "ent:b", "ent:d"},
			grants: []sqliteGrantSpec{
				{id: "grant:alice:a", entitlementID: "ent:a", principalRT: "user", principalID: "alice"},
				{id: "grant:sam:s", entitlementID: "ent:s", principalRT: "user", principalID: "sam"},
			},
			edges: []sqliteEdgeSpec{
				{src: "ent:s", dst: "ent:a", rtids: []string{"user"}},
				{src: "ent:a", dst: "ent:b", rtids: []string{"user"}},
				{src: "ent:b", dst: "ent:a", rtids: []string{"user"}},
				{src: "ent:b", dst: "ent:d", rtids: []string{"user"}},
			},
		},
	}
}

func TestTopologicalMergeMatchesSQLiteGroundTruth(t *testing.T) {
	for _, tc := range parityCases() {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()

			// Ground truth: the current source-batched expander on SQLite.
			ground := runExpansion(t, ctx, tc, dotc1z.EngineSQLite, 0, func(e *Expander) error { return e.Run(ctx) })

			// SQLite candidates exercise the buffer+sort grouping path (SQLite
			// orders grants by id, not principal).
			streamingSQLite := runExpansion(t, ctx, tc, dotc1z.EngineSQLite, 0, func(e *Expander) error { return e.RunTopologicalMergeStreaming(ctx) })
			assertStoreSnapshotsEqual(t, ground, streamingSQLite, "topological_streaming/sqlite")

			projectionSQLite := runExpansion(t, ctx, tc, dotc1z.EngineSQLite, 0, func(e *Expander) error { return e.RunTopologicalMergeProjection(ctx) })
			assertStoreSnapshotsEqual(t, ground, projectionSQLite, "topological_projection/sqlite")

			if tc.name != "shallow_drops_transitive" && tc.name != "resource_type_filters" {
				projectionPebble := runExpansion(t, ctx, tc, dotc1z.EnginePebble, 0, func(e *Expander) error { return e.RunTopologicalMergeProjection(ctx) })
				assertStoreSnapshotsEqual(t, ground, projectionPebble, "topological_projection/pebble")
			}
		})
	}
}

// runExpansion seeds a fresh c1z (on the given engine) for the case, runs the
// supplied expander algorithm through the real store, then re-opens the file
// read-only and returns a snapshot of every grant. When pageSize > 0 the store
// is wrapped so ListGrantsForEntitlement returns small pages, forcing principal
// groups to span reader pages.
func runExpansion(
	t *testing.T,
	ctx context.Context,
	tc sqliteParityCase,
	engine dotc1z.Engine,
	pageSize uint32,
	run func(*Expander) error,
) map[string]storeGrantSnapshot {
	t.Helper()
	path := filepath.Join(t.TempDir(), "expand.c1z")

	store, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(engine))
	require.NoError(t, err)
	syncID, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	seedSQLiteBaseData(t, ctx, store, tc)

	graph := buildGraphFromCase(t, ctx, tc, engine)
	var es ExpanderStore = benchmarkExpanderStore{store: store}
	if pageSize > 0 {
		es = smallPageExpanderStore{inner: es, pageSize: pageSize}
	}
	require.NoError(t, run(NewExpander(es, graph)))

	require.NoError(t, store.EndSync(ctx))
	require.NoError(t, store.Close(ctx))

	ro, err := dotc1z.NewStore(ctx, path, dotc1z.WithReadOnly(true))
	require.NoError(t, err)
	defer func() { require.NoError(t, ro.Close(ctx)) }()
	require.NoError(t, ro.SetCurrentSync(ctx, syncID))

	return readBackGrantSnapshot(t, ctx, ro)
}

// smallPageExpanderStore forces a small page size on ListGrantsForEntitlement
// so principal groups span reader pages, while preserving the underlying
// store's principal-sort guarantee.
type smallPageExpanderStore struct {
	inner    ExpanderStore
	pageSize uint32
}

func (s smallPageExpanderStore) GetEntitlement(
	ctx context.Context,
	req *reader_v2.EntitlementsReaderServiceGetEntitlementRequest,
) (*reader_v2.EntitlementsReaderServiceGetEntitlementResponse, error) {
	return s.inner.GetEntitlement(ctx, req)
}

func (s smallPageExpanderStore) ListGrantsForEntitlement(
	ctx context.Context,
	req *reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest,
) (*reader_v2.GrantsReaderServiceListGrantsForEntitlementResponse, error) {
	if req.GetPageSize() == 0 {
		req.SetPageSize(s.pageSize)
	}
	return s.inner.ListGrantsForEntitlement(ctx, req)
}

func (s smallPageExpanderStore) StoreExpandedGrants(ctx context.Context, grants ...*v2.Grant) error {
	return s.inner.StoreExpandedGrants(ctx, grants...)
}

func (s smallPageExpanderStore) GrantsForEntitlementPrincipalSorted() bool {
	return s.inner.GrantsForEntitlementPrincipalSorted()
}

func seedSQLiteBaseData(t *testing.T, ctx context.Context, store dotc1z.C1ZStore, tc sqliteParityCase) {
	t.Helper()

	resourceTypeIDs := map[string]struct{}{"group": {}}
	principals := map[string]*v2.Resource{}
	for _, g := range tc.grants {
		resourceTypeIDs[g.principalRT] = struct{}{}
		key := g.principalRT + "\x00" + g.principalID
		principals[key] = makeResource(g.principalRT, g.principalID)
	}

	rtList := make([]*v2.ResourceType, 0, len(resourceTypeIDs))
	for rt := range resourceTypeIDs {
		rtList = append(rtList, v2.ResourceType_builder{Id: rt}.Build())
	}
	sort.Slice(rtList, func(i, j int) bool { return rtList[i].GetId() < rtList[j].GetId() })
	require.NoError(t, store.PutResourceTypes(ctx, rtList...))

	group := makeResource("group", "org")
	resources := []*v2.Resource{group}
	principalKeys := make([]string, 0, len(principals))
	for k := range principals {
		principalKeys = append(principalKeys, k)
	}
	sort.Strings(principalKeys)
	for _, k := range principalKeys {
		resources = append(resources, principals[k])
	}
	require.NoError(t, store.PutResources(ctx, resources...))

	ents := make([]*v2.Entitlement, 0, len(tc.entitlementIDs))
	for _, id := range tc.entitlementIDs {
		ents = append(ents, makeEntitlement(id, group))
	}
	require.NoError(t, store.PutEntitlements(ctx, ents...))

	entByID := map[string]*v2.Entitlement{}
	for _, e := range ents {
		entByID[e.GetId()] = e
	}

	grants := make([]*v2.Grant, 0, len(tc.grants))
	for _, gs := range tc.grants {
		grant := makeGrant(gs.id, entByID[gs.entitlementID], makeResource(gs.principalRT, gs.principalID))
		if len(gs.sources) > 0 {
			srcMap := map[string]*v2.GrantSources_GrantSource{}
			for sourceID, isDirect := range gs.sources {
				srcMap[sourceID] = &v2.GrantSources_GrantSource{IsDirect: isDirect}
			}
			grant.SetSources(v2.GrantSources_builder{Sources: srcMap}.Build())
		}
		grants = append(grants, grant)
	}
	require.NoError(t, store.PutGrants(ctx, grants...))
}

func buildGraphFromCase(t *testing.T, ctx context.Context, tc sqliteParityCase, engine dotc1z.Engine) *EntitlementGraph {
	t.Helper()
	graph := NewEntitlementGraph(ctx)
	// Raw ids are pass-through on both engines; no per-engine encoding.
	entID := func(id string) string { return id }
	for _, id := range tc.entitlementIDs {
		graph.AddEntitlementID(entID(id))
	}
	for _, e := range tc.edges {
		require.NoError(t, graph.AddEdge(ctx, entID(e.src), entID(e.dst), e.shallow, e.rtids))
	}
	// Mirror production: collapse cycles before expansion. No-op for the
	// acyclic cases here, but keeps the path identical to runGrantExpandAction.
	require.NoError(t, graph.FixCycles(ctx))
	return graph
}

func readBackGrantSnapshot(t *testing.T, ctx context.Context, store dotc1z.C1ZStore) map[string]storeGrantSnapshot {
	t.Helper()
	out := make(map[string]storeGrantSnapshot)
	pageToken := ""
	for {
		resp, err := store.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{
			PageSize:  1000,
			PageToken: pageToken,
		}.Build())
		require.NoError(t, err)
		for _, g := range resp.GetList() {
			if g.GetPrincipal() == nil || g.GetPrincipal().GetId() == nil {
				continue
			}
			sources := g.GetSources().GetSources()
			sourceDirect := make(map[string]bool, len(sources))
			for sourceID, src := range sources {
				sourceDirect[sourceID] = src.GetIsDirect()
			}
			annTypes := make([]string, 0, len(g.GetAnnotations()))
			for _, a := range g.GetAnnotations() {
				annTypes = append(annTypes, a.GetTypeUrl())
			}
			sort.Strings(annTypes)
			key := g.GetEntitlement().GetId() + "\x00" + g.GetPrincipal().GetId().GetResourceType() + "\x00" + g.GetPrincipal().GetId().GetResource()
			out[key] = storeGrantSnapshot{
				id:              g.GetId(),
				entitlement:     g.GetEntitlement().GetId(),
				principalRT:     g.GetPrincipal().GetId().GetResourceType(),
				principalID:     g.GetPrincipal().GetId().GetResource(),
				sourceDirect:    sourceDirect,
				annotationTypes: annTypes,
			}
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	return out
}

func assertStoreSnapshotsEqual(t *testing.T, ground, candidate map[string]storeGrantSnapshot, algo string) {
	t.Helper()
	if len(ground) != len(candidate) {
		groundIDs := make([]string, 0, len(ground))
		for id := range ground {
			groundIDs = append(groundIDs, id)
		}
		sort.Strings(groundIDs)
		candIDs := make([]string, 0, len(candidate))
		for id := range candidate {
			candIDs = append(candIDs, id)
		}
		sort.Strings(candIDs)
		t.Logf("%s: ground (%d): %v", algo, len(ground), groundIDs)
		t.Logf("%s: candidate (%d): %v", algo, len(candidate), candIDs)
	}
	require.Equalf(t, len(ground), len(candidate), "%s: grant count differs from SQLite ground truth", algo)
	for id, want := range ground {
		got, ok := candidate[id]
		require.Truef(t, ok, "%s: missing grant %q present in SQLite ground truth", algo, id)
		require.Equalf(t, want.entitlement, got.entitlement, "%s: entitlement mismatch for grant %q", algo, id)
		require.Equalf(t, want.principalRT, got.principalRT, "%s: principal RT mismatch for grant %q", algo, id)
		require.Equalf(t, want.principalID, got.principalID, "%s: principal ID mismatch for grant %q", algo, id)
		require.Equalf(t, want.annotationTypes, got.annotationTypes, "%s: annotations mismatch for grant %q", algo, id)
	}
	for id := range candidate {
		require.Containsf(t, ground, id, "%s: produced grant %q absent from SQLite ground truth", algo, id)
	}
}
