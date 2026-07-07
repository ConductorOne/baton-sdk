package expand

import (
	"context"
	"sort"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

const expansionPlanVersion = 1

// projectionFanOutThreshold is the out-degree at which a source entitlement is
// materialized into the projection index regardless of its destinations'
// in-degree. The destination-driven evaluator re-reads each unprojected source
// once per destination it feeds, so a high-fan-out "broadcast" source is a
// read-amplification hotspot; projecting it amortizes those reads to one build
// scan plus cheap scratch range-scans. Set above the typical out-degree tail so
// only genuine broadcast sources are projected (whale p90 out-degree was 2).
const projectionFanOutThreshold = 16

// EntitlementGraphPlan is the pre-expansion decision artifact for the projection
// evaluator. It has collapsed to the one decision the executor actually consumes:
// which source entitlements to materialize into the temporary projection index
// (ProjectionSources). Everything else the executor recomputes. Order is kept as
// a stable diagnostic of the topological schedule.
type EntitlementGraphPlan struct {
	Version           int      `json:"v"`
	Order             []int    `json:"order"`
	ProjectionSources []string `json:"projection_sources,omitempty"`
}

// BuildExpansionPlan computes and stores the projection-source set from the
// graph shape alone (no grant-row cardinality), so it is cheap and safe to run
// whenever a graph is ready for expansion.
//
// Two rules add a source to the projection index, both amortizing one build
// scan across many reads:
//   - fan-in: a node's parents are projected when the node has in-degree >= 2
//     and feeds multiple destination entitlements or pulls from multiple source
//     entitlements (the destination's merge reuses those parents).
//   - fan-out: a source is projected when its out-degree >= projectionFanOutThreshold
//     (a broadcast source the merge would otherwise re-read once per destination).
//
// Sources matched by neither rule are read live during the merge.
func (g *EntitlementGraph) BuildExpansionPlan(ctx context.Context) (*EntitlementGraphPlan, error) {
	order, err := topologicalNodeOrder(g)
	if err != nil {
		return nil, err
	}
	plan := &EntitlementGraphPlan{
		Version: expansionPlanVersion,
		Order:   append([]int(nil), order...),
	}
	projectionSources := make(map[string]struct{})

	// Fan-in rule.
	for _, nodeID := range order {
		node, ok := g.Nodes[nodeID]
		if !ok {
			continue
		}
		incoming := incomingEdgesSorted(g, nodeID)
		incomingSourceEntitlements := 0
		for _, edge := range incoming {
			parent, ok := g.Nodes[edge.sourceNodeID]
			if !ok {
				continue
			}
			incomingSourceEntitlements += len(parent.EntitlementIDs)
		}
		if !nodeNeedsProjection(len(incoming), len(node.EntitlementIDs), incomingSourceEntitlements) {
			continue
		}
		for _, edge := range incoming {
			parent, ok := g.Nodes[edge.sourceNodeID]
			if !ok {
				continue
			}
			for _, sourceEntitlementID := range parent.EntitlementIDs {
				projectionSources[sourceEntitlementID] = struct{}{}
			}
		}
	}
	fanInSources := len(projectionSources)

	// Fan-out rule: project broadcast sources the fan-in rule missed.
	fanOutAdded := 0
	for _, nodeID := range order {
		node, ok := g.Nodes[nodeID]
		if !ok {
			continue
		}
		if len(g.SourcesToDestinations[nodeID]) < projectionFanOutThreshold {
			continue
		}
		for _, sourceEntitlementID := range node.EntitlementIDs {
			if _, ok := projectionSources[sourceEntitlementID]; ok {
				continue
			}
			projectionSources[sourceEntitlementID] = struct{}{}
			fanOutAdded++
		}
	}

	plan.ProjectionSources = make([]string, 0, len(projectionSources))
	for sourceEntitlementID := range projectionSources {
		plan.ProjectionSources = append(plan.ProjectionSources, sourceEntitlementID)
	}
	sort.Strings(plan.ProjectionSources)
	g.ExpansionPlan = plan

	ctxzap.Extract(ctx).Info("expansion plan: projection sources selected",
		zap.Int("total", len(plan.ProjectionSources)),
		zap.Int("from_fan_in", fanInSources),
		zap.Int("from_fan_out", fanOutAdded),
		zap.Int("fan_out_threshold", projectionFanOutThreshold),
	)
	return plan, nil
}

func (g *EntitlementGraph) ensureExpansionPlan(ctx context.Context) (*EntitlementGraphPlan, error) {
	// A plan checkpointed by a different SDK version may encode different
	// projection-source selection rules; rebuild rather than consume it.
	// Correctness today is independent of the source set (rows are built
	// into and routed through the same set), but the version gate keeps
	// that a choice instead of an accident if a plan field ever becomes
	// semantically load-bearing.
	if g.ExpansionPlan != nil && g.ExpansionPlan.Version == expansionPlanVersion {
		return g.ExpansionPlan, nil
	}
	return g.BuildExpansionPlan(ctx)
}

// nodeNeedsProjection reports whether a destination node's parent sources should
// be materialized into the projection index. Single-parent nodes (in-degree <= 1)
// never project: there is no reuse to amortize the build. Multi-parent nodes
// project once they feed multiple destinations or pull from multiple source
// entitlements.
func nodeNeedsProjection(inDegree, destinationEntitlements, incomingSourceEntitlements int) bool {
	if inDegree <= 1 {
		return false
	}
	return destinationEntitlements >= 2 || incomingSourceEntitlements >= 2
}

func (p *EntitlementGraphPlan) GetProjectionSources() []string {
	if p == nil {
		return nil
	}
	return p.ProjectionSources
}

func copyExpansionPlan(in *EntitlementGraphPlan) *EntitlementGraphPlan {
	if in == nil {
		return nil
	}
	return &EntitlementGraphPlan{
		Version:           in.Version,
		Order:             append([]int(nil), in.Order...),
		ProjectionSources: append([]string(nil), in.ProjectionSources...),
	}
}

func copyExpansionMetrics(in *EntitlementGraphMetrics) *EntitlementGraphMetrics {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}
