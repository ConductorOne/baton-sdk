package dotc1z

import (
	"context"
	"fmt"

	"github.com/doug-martin/goqu/v9"

	"github.com/conductorone/baton-sdk/pkg/sync/expand"
)

const entitlementEdgesTableVersion = "1"
const entitlementEdgesTableName = "entitlement_edges"
const entitlementEdgesTableSchema = `
create table if not exists %s (
    id integer primary key,
    source_entitlement_id text not null,
    descendant_entitlement_id text not null,
    is_shallow integer not null default 0,
    resource_type_ids text,
    sync_id text not null
);
create index if not exists %s on %s (source_entitlement_id, sync_id);
create index if not exists %s on %s (descendant_entitlement_id, sync_id);
create unique index if not exists %s on %s (source_entitlement_id, descendant_entitlement_id, sync_id);`

var entitlementEdges = (*entitlementEdgesTable)(nil)

var _ tableDescriptor = (*entitlementEdgesTable)(nil)

type entitlementEdgesTable struct{}

func (r *entitlementEdgesTable) Version() string {
	return entitlementEdgesTableVersion
}

func (r *entitlementEdgesTable) Name() string {
	return fmt.Sprintf("v%s_%s", r.Version(), entitlementEdgesTableName)
}

func (r *entitlementEdgesTable) Schema() (string, []any) {
	return entitlementEdgesTableSchema, []any{
		r.Name(),
		fmt.Sprintf("idx_ent_edges_source_v%s", r.Version()),
		r.Name(),
		fmt.Sprintf("idx_ent_edges_descendant_v%s", r.Version()),
		r.Name(),
		fmt.Sprintf("idx_ent_edges_unique_v%s", r.Version()),
		r.Name(),
	}
}

func (r *entitlementEdgesTable) Migrations(ctx context.Context, db *goqu.Database) error {
	return nil
}

// PopulateEntitlementEdges populates the entitlement_edges table from the in-memory EntitlementGraph.
// This should be called once per sync after the graph is loaded but before expansion begins.
func (c *C1File) PopulateEntitlementEdges(ctx context.Context, graph *expand.EntitlementGraph) error {
	ctx, span := tracer.Start(ctx, "C1File.PopulateEntitlementEdges")
	defer span.End()

	// Clear existing edges for this sync
	_, err := c.db.ExecContext(ctx, `DELETE FROM `+entitlementEdges.Name()+` WHERE sync_id = ?`, c.currentSyncID)
	if err != nil {
		return fmt.Errorf("error clearing entitlement edges: %w", err)
	}

	if graph == nil || len(graph.Edges) == 0 {
		return nil
	}

	// Prepare batch insert
	rows := make([][]interface{}, 0, len(graph.Edges))

	for _, edge := range graph.Edges {
		// Get source and destination node entitlement IDs
		sourceNode, ok := graph.Nodes[edge.SourceID]
		if !ok {
			continue
		}
		destNode, ok := graph.Nodes[edge.DestinationID]
		if !ok {
			continue
		}

		// Each node can have multiple entitlement IDs (from SCC collapse)
		// Create an edge for each combination
		for _, sourceEntID := range sourceNode.EntitlementIDs {
			for _, destEntID := range destNode.EntitlementIDs {
				shallowInt := 0
				if edge.IsShallow {
					shallowInt = 1
				}

				var resourceTypeIDsJSON *string
				if len(edge.ResourceTypeIDs) > 0 {
					// Simple JSON array encoding
					s := "["
					for i, rtID := range edge.ResourceTypeIDs {
						if i > 0 {
							s += ","
						}
						s += `"` + rtID + `"`
					}
					s += "]"
					resourceTypeIDsJSON = &s
				}

				rows = append(rows, []interface{}{
					sourceEntID,
					destEntID,
					shallowInt,
					resourceTypeIDsJSON,
					c.currentSyncID,
				})
			}
		}
	}

	if len(rows) == 0 {
		return nil
	}

	// Batch insert
	stmt, err := c.rawDb.PrepareContext(ctx, `
		INSERT OR IGNORE INTO `+entitlementEdges.Name()+` 
		(source_entitlement_id, descendant_entitlement_id, is_shallow, resource_type_ids, sync_id)
		VALUES (?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("error preparing entitlement edges insert: %w", err)
	}
	defer stmt.Close()

	for _, row := range rows {
		_, err = stmt.ExecContext(ctx, row...)
		if err != nil {
			return fmt.Errorf("error inserting entitlement edge: %w", err)
		}
	}

	return nil
}

// ClearEntitlementEdges removes all edges for the current sync.
func (c *C1File) ClearEntitlementEdges(ctx context.Context) error {
	_, err := c.db.ExecContext(ctx, `DELETE FROM `+entitlementEdges.Name()+` WHERE sync_id = ?`, c.currentSyncID)
	if err != nil {
		return fmt.Errorf("error clearing entitlement edges: %w", err)
	}
	return nil
}

