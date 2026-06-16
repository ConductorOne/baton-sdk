package expand

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"google.golang.org/protobuf/proto"
)

type benchmarkMergeStore struct {
	entitlements map[string]*v2.Entitlement
	grants       map[string]map[string]*v2.Grant // entitlement ID -> grant ID -> grant
	readCount    int
	writeCount   int
	tempCount    int
}

func newBenchmarkMergeStore() *benchmarkMergeStore {
	return &benchmarkMergeStore{
		entitlements: make(map[string]*v2.Entitlement),
		grants:       make(map[string]map[string]*v2.Grant),
	}
}

func (s *benchmarkMergeStore) clone() *benchmarkMergeStore {
	out := newBenchmarkMergeStore()
	for id, ent := range s.entitlements {
		out.entitlements[id] = proto.Clone(ent).(*v2.Entitlement)
	}
	for entID, grants := range s.grants {
		out.grants[entID] = make(map[string]*v2.Grant, len(grants))
		for grantID, grant := range grants {
			out.grants[entID][grantID] = proto.Clone(grant).(*v2.Grant)
		}
	}
	return out
}

func (s *benchmarkMergeStore) addEntitlement(e *v2.Entitlement) {
	s.entitlements[e.GetId()] = e
}

func (s *benchmarkMergeStore) addGrant(g *v2.Grant) {
	entID := g.GetEntitlement().GetId()
	if s.grants[entID] == nil {
		s.grants[entID] = make(map[string]*v2.Grant)
	}
	s.grants[entID][g.GetId()] = g
}

func (s *benchmarkMergeStore) GetEntitlement(
	_ context.Context,
	req *reader_v2.EntitlementsReaderServiceGetEntitlementRequest,
) (*reader_v2.EntitlementsReaderServiceGetEntitlementResponse, error) {
	ent := s.entitlements[req.GetEntitlementId()]
	if ent == nil {
		return nil, nil
	}
	return reader_v2.EntitlementsReaderServiceGetEntitlementResponse_builder{
		Entitlement: proto.Clone(ent).(*v2.Entitlement),
	}.Build(), nil
}

func (s *benchmarkMergeStore) ListGrantsForEntitlement(
	_ context.Context,
	req *reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest,
) (*reader_v2.GrantsReaderServiceListGrantsForEntitlementResponse, error) {
	s.readCount++
	entID := req.GetEntitlement().GetId()
	grantsByID := s.grants[entID]
	grants := make([]*v2.Grant, 0, len(grantsByID))
	for _, grant := range grantsByID {
		if grant == nil || grant.GetPrincipal() == nil || grant.GetPrincipal().GetId() == nil {
			grants = append(grants, grant)
			continue
		}
		principalID := req.GetPrincipalId() //nolint:staticcheck // benchmark uses production reader shape
		if principalID != nil {
			pid := grant.GetPrincipal().GetId()
			if pid.GetResourceType() != principalID.GetResourceType() || pid.GetResource() != principalID.GetResource() {
				continue
			}
		}
		rtFilter := req.GetPrincipalResourceTypeIds() //nolint:staticcheck // benchmark uses production reader shape
		if len(rtFilter) > 0 {
			pid := grant.GetPrincipal().GetId()
			found := false
			for _, rt := range rtFilter {
				if pid.GetResourceType() == rt {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}
		grants = append(grants, proto.Clone(grant).(*v2.Grant))
	}
	sort.SliceStable(grants, func(i, j int) bool {
		gi := grants[i]
		gj := grants[j]
		if gi == nil || gi.GetPrincipal() == nil || gi.GetPrincipal().GetId() == nil {
			return true
		}
		if gj == nil || gj.GetPrincipal() == nil || gj.GetPrincipal().GetId() == nil {
			return false
		}
		pi := gi.GetPrincipal().GetId()
		pj := gj.GetPrincipal().GetId()
		if pi.GetResourceType() != pj.GetResourceType() {
			return pi.GetResourceType() < pj.GetResourceType()
		}
		if pi.GetResource() != pj.GetResource() {
			return pi.GetResource() < pj.GetResource()
		}
		return gi.GetId() < gj.GetId()
	})
	return reader_v2.GrantsReaderServiceListGrantsForEntitlementResponse_builder{
		List: grants,
	}.Build(), nil
}

// GrantsForEntitlementPrincipalSorted is false so the "current" sub-benchmark's
// Run -> RunSingleStep stays on the source-batched expander it means to measure,
// rather than being redirected into the projection evaluator. The topological
// benchmarks call RunTopologicalMerge* directly and are unaffected.
func (s *benchmarkMergeStore) GrantsForEntitlementPrincipalSorted() bool { return false }

func (s *benchmarkMergeStore) StoreExpandedGrants(_ context.Context, grants ...*v2.Grant) error {
	s.writeCount += len(grants)
	for _, grant := range grants {
		if grant == nil {
			continue
		}
		s.addGrant(proto.Clone(grant).(*v2.Grant))
	}
	return nil
}

func buildHighFanInBenchmarkShape(parents, principals int) (*benchmarkMergeStore, *EntitlementGraph) {
	ctx := context.Background()
	store := newBenchmarkMergeStore()
	graph := NewEntitlementGraph(ctx)
	group := makeResource("group", "fanin")
	dest := makeEntitlement("ent:dest", group)
	store.addEntitlement(dest)
	graph.AddEntitlementID(dest.GetId())

	for p := 0; p < parents; p++ {
		ent := makeEntitlement("ent:parent:"+strconv.Itoa(p), group)
		store.addEntitlement(ent)
		graph.AddEntitlementID(ent.GetId())
		for i := 0; i < principals; i++ {
			principal := makeResource("user", "u:"+strconv.Itoa(i))
			store.addGrant(makeGrant(fmt.Sprintf("grant:p:%d:u:%d", p, i), ent, principal))
		}
		if err := graph.AddEdge(ctx, ent.GetId(), dest.GetId(), false, []string{"user"}); err != nil {
			panic(err)
		}
	}
	return store, graph
}

func buildChainBenchmarkShape(depth, principals int) (*benchmarkMergeStore, *EntitlementGraph) {
	ctx := context.Background()
	store := newBenchmarkMergeStore()
	graph := NewEntitlementGraph(ctx)
	group := makeResource("group", "chain")
	var prev *v2.Entitlement
	for d := 0; d < depth; d++ {
		ent := makeEntitlement("ent:level:"+strconv.Itoa(d), group)
		store.addEntitlement(ent)
		graph.AddEntitlementID(ent.GetId())
		if d == 0 {
			for i := 0; i < principals; i++ {
				store.addGrant(makeGrant("grant:u:"+strconv.Itoa(i), ent, makeResource("user", "u:"+strconv.Itoa(i))))
			}
		}
		if prev != nil {
			if err := graph.AddEdge(ctx, prev.GetId(), ent.GetId(), false, []string{"user"}); err != nil {
				panic(err)
			}
		}
		prev = ent
	}
	return store, graph
}

func buildDenseBipartiteBenchmarkShape(parents, dests, principals int) (*benchmarkMergeStore, *EntitlementGraph) {
	ctx := context.Background()
	store := newBenchmarkMergeStore()
	graph := NewEntitlementGraph(ctx)
	group := makeResource("group", "dense")

	parentsByID := make([]*v2.Entitlement, 0, parents)
	for p := 0; p < parents; p++ {
		ent := makeEntitlement("ent:parent:"+strconv.Itoa(p), group)
		store.addEntitlement(ent)
		graph.AddEntitlementID(ent.GetId())
		parentsByID = append(parentsByID, ent)
		for i := 0; i < principals; i++ {
			principal := makeResource("user", "u:"+strconv.Itoa(i))
			store.addGrant(makeGrant(fmt.Sprintf("grant:p:%d:u:%d", p, i), ent, principal))
		}
	}

	destsByID := make([]*v2.Entitlement, 0, dests)
	for d := 0; d < dests; d++ {
		ent := makeEntitlement("ent:dest:"+strconv.Itoa(d), group)
		store.addEntitlement(ent)
		graph.AddEntitlementID(ent.GetId())
		destsByID = append(destsByID, ent)
	}

	for _, parent := range parentsByID {
		for _, dest := range destsByID {
			if err := graph.AddEdge(ctx, parent.GetId(), dest.GetId(), false, []string{"user"}); err != nil {
				panic(err)
			}
		}
	}
	return store, graph
}

func benchmarkExpansionAlgorithm(b *testing.B, baseStore *benchmarkMergeStore, baseGraph *EntitlementGraph, run func(context.Context, *benchmarkMergeStore, *EntitlementGraph) error) {
	ctx := context.Background()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		store := baseStore.clone()
		graph := copyGraph(baseGraph)
		if err := run(ctx, store, graph); err != nil {
			b.Fatalf("expand: %v", err)
		}
		b.ReportMetric(float64(store.readCount), "reads/op")
		b.ReportMetric(float64(store.writeCount), "writes/op")
		b.ReportMetric(float64(store.tempCount), "temp_contribs/op")
	}
}

// loadGrantGroups and mergeDestinationFromContributionMap are benchmark-only
// helpers. They back the map-based "shuffle" research strategy that the
// benchmarks compare against the production streaming/projection evaluators;
// they are intentionally not part of the shipped expander (the map approach is
// not memory-bounded). Kept here so the production package carries no dead code.
func (e *Expander) loadGrantGroups(ctx context.Context, entitlement *v2.Entitlement) (map[topoPrincipalKey][]*v2.Grant, error) {
	if entitlement == nil {
		return nil, fmt.Errorf("loadGrantGroups: nil entitlement")
	}
	pageToken := ""
	byID := make(map[string]*v2.Grant)
	order := make([]string, 0)
	for {
		resp, err := e.store.ListGrantsForEntitlement(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
			Entitlement: entitlement,
			PageToken:   pageToken,
		}.Build())
		if err != nil {
			return nil, fmt.Errorf("loadGrantGroups: list grants for entitlement %q: %w", entitlement.GetId(), err)
		}
		for _, grant := range resp.GetList() {
			if grant == nil {
				continue
			}
			id := grant.GetId()
			if _, ok := byID[id]; !ok {
				order = append(order, id)
			}
			byID[id] = proto.Clone(grant).(*v2.Grant)
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}

	grants := make([]*v2.Grant, 0, len(byID))
	for _, id := range order {
		if grant := byID[id]; grant != nil {
			grants = append(grants, grant)
		}
	}
	sort.SliceStable(grants, func(i, j int) bool {
		ki, okI := principalKeyFromResource(grants[i].GetPrincipal())
		kj, okJ := principalKeyFromResource(grants[j].GetPrincipal())
		if okI != okJ {
			return okI
		}
		if okI && ki != kj {
			return principalKeyLess(ki, kj)
		}
		return grants[i].GetId() < grants[j].GetId()
	})

	groups := make(map[topoPrincipalKey][]*v2.Grant)
	for _, grant := range grants {
		key, ok := principalKeyFromResource(grant.GetPrincipal())
		if !ok {
			continue
		}
		groups[key] = append(groups[key], grant)
	}
	return groups, nil
}

func (e *Expander) mergeDestinationFromContributionMap(
	ctx context.Context,
	destEntitlement *v2.Entitlement,
	contribs map[topoPrincipalKey]*topoContribution,
) ([]*v2.Grant, error) {
	base, err := e.loadGrantGroups(ctx, destEntitlement)
	if err != nil {
		return nil, err
	}
	keys := make([]topoPrincipalKey, 0, len(base)+len(contribs))
	seen := make(map[topoPrincipalKey]struct{}, len(base)+len(contribs))
	for key := range base {
		keys = append(keys, key)
		seen[key] = struct{}{}
	}
	for key := range contribs {
		if _, ok := seen[key]; ok {
			continue
		}
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return principalKeyLess(keys[i], keys[j]) })

	var dirty []*v2.Grant
	for _, key := range keys {
		contrib := contribs[key]
		if contrib == nil || len(contrib.sources) == 0 {
			continue
		}
		baseGrants := base[key]
		if len(baseGrants) == 0 {
			principal, err := contrib.principalResource()
			if err != nil {
				return nil, err
			}
			if principal == nil {
				continue
			}
			grant, err := newExpandedGrantWithSources(destEntitlement, principal, contrib.sources)
			if err != nil {
				return nil, err
			}
			dirty = append(dirty, grant)
			continue
		}
		for _, baseGrant := range baseGrants {
			updated := mergeContributionIntoExistingGrant(baseGrant, destEntitlement.GetId(), contrib.sources)
			if updated != nil {
				dirty = append(dirty, updated)
			}
		}
	}
	return dirty, nil
}

type topoOutgoingEdge struct {
	destNodeID int
	edge       Edge
}

func outgoingEdgesSorted(graph *EntitlementGraph, nodeID int) []topoOutgoingEdge {
	dests := graph.SourcesToDestinations[nodeID]
	out := make([]topoOutgoingEdge, 0, len(dests))
	for destNodeID, edgeID := range dests {
		edge, ok := graph.Edges[edgeID]
		if !ok {
			continue
		}
		out = append(out, topoOutgoingEdge{destNodeID: destNodeID, edge: edge})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].destNodeID != out[j].destNodeID {
			return out[i].destNodeID < out[j].destNodeID
		}
		return out[i].edge.EdgeID < out[j].edge.EdgeID
	})
	return out
}

func BenchmarkTopologicalMergeHighFanIn(b *testing.B) {
	for _, parents := range []int{8, 32, 128} {
		b.Run("parents="+strconv.Itoa(parents), func(b *testing.B) {
			baseStore, baseGraph := buildHighFanInBenchmarkShape(parents, 256)
			b.Run("current", func(b *testing.B) {
				benchmarkExpansionAlgorithm(b, baseStore, baseGraph, func(ctx context.Context, store *benchmarkMergeStore, graph *EntitlementGraph) error {
					return NewExpander(store, graph).Run(ctx)
				})
			})
			b.Run("topological_streaming", func(b *testing.B) {
				benchmarkExpansionAlgorithm(b, baseStore, baseGraph, func(ctx context.Context, store *benchmarkMergeStore, graph *EntitlementGraph) error {
					return NewExpander(store, graph).RunTopologicalMergeStreaming(ctx)
				})
			})
		})
	}
}

func BenchmarkTopologicalMergeChain(b *testing.B) {
	for _, depth := range []int{4, 12} {
		b.Run("depth="+strconv.Itoa(depth), func(b *testing.B) {
			baseStore, baseGraph := buildChainBenchmarkShape(depth, 512)
			b.Run("current", func(b *testing.B) {
				benchmarkExpansionAlgorithm(b, baseStore, baseGraph, func(ctx context.Context, store *benchmarkMergeStore, graph *EntitlementGraph) error {
					return NewExpander(store, graph).Run(ctx)
				})
			})
			b.Run("topological_streaming", func(b *testing.B) {
				benchmarkExpansionAlgorithm(b, baseStore, baseGraph, func(ctx context.Context, store *benchmarkMergeStore, graph *EntitlementGraph) error {
					return NewExpander(store, graph).RunTopologicalMergeStreaming(ctx)
				})
			})
		})
	}
}

func BenchmarkTopologicalMergeDenseBipartite(b *testing.B) {
	for _, shape := range []struct {
		parents    int
		dests      int
		principals int
	}{
		{parents: 8, dests: 8, principals: 128},
		{parents: 16, dests: 16, principals: 128},
		{parents: 32, dests: 32, principals: 64},
	} {
		name := fmt.Sprintf("parents=%d/dests=%d/principals=%d", shape.parents, shape.dests, shape.principals)
		b.Run(name, func(b *testing.B) {
			baseStore, baseGraph := buildDenseBipartiteBenchmarkShape(shape.parents, shape.dests, shape.principals)
			b.Run("current", func(b *testing.B) {
				benchmarkExpansionAlgorithm(b, baseStore, baseGraph, func(ctx context.Context, store *benchmarkMergeStore, graph *EntitlementGraph) error {
					return NewExpander(store, graph).Run(ctx)
				})
			})
			b.Run("topological_streaming", func(b *testing.B) {
				benchmarkExpansionAlgorithm(b, baseStore, baseGraph, func(ctx context.Context, store *benchmarkMergeStore, graph *EntitlementGraph) error {
					return NewExpander(store, graph).RunTopologicalMergeStreaming(ctx)
				})
			})
		})
	}
}
