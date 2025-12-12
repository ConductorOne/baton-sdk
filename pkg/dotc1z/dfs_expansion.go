package dotc1z

import (
	"context"
	"fmt"
	"strings"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/sync/expand"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

const dfsExpansionTempTable = "temp_dfs_expandable_entitlements"

// DFSExpansionStats tracks statistics for the DFS expansion.
type DFSExpansionStats struct {
	PrincipalsProcessed int64
	GrantsInserted      int64
	GrantsUpdated       int64
	EntitlementsInGraph int
}

// CreateDFSTempTable creates a temporary table with entitlement IDs in topological order.
// The rowid reflects insertion order, which equals topological order.
func (c *C1File) CreateDFSTempTable(ctx context.Context, graph *expand.EntitlementGraph) error {
	ctx, span := tracer.Start(ctx, "C1File.CreateDFSTempTable")
	defer span.End()

	// Drop if exists from previous run
	_, err := c.db.ExecContext(ctx, `DROP TABLE IF EXISTS `+dfsExpansionTempTable)
	if err != nil {
		return fmt.Errorf("error dropping temp table: %w", err)
	}

	// Create temp table - rowid will be implicit and reflect insertion order
	_, err = c.db.ExecContext(ctx, `
		CREATE TEMP TABLE `+dfsExpansionTempTable+` (
			entitlement_id TEXT UNIQUE
		)
	`)
	if err != nil {
		return fmt.Errorf("error creating temp table: %w", err)
	}

	// Get entitlements in topological order
	topoOrder := graph.TopologicalOrder()

	// Insert in chunks
	const chunkSize = 500
	for i := 0; i < len(topoOrder); i += chunkSize {
		end := i + chunkSize
		if end > len(topoOrder) {
			end = len(topoOrder)
		}
		chunk := topoOrder[i:end]

		// Build VALUES clause
		placeholders := make([]string, len(chunk))
		args := make([]interface{}, len(chunk))
		for j, id := range chunk {
			placeholders[j] = "(?)"
			args[j] = id
		}

		_, err = c.db.ExecContext(ctx,
			`INSERT INTO `+dfsExpansionTempTable+` (entitlement_id) VALUES `+strings.Join(placeholders, ","),
			args...)
		if err != nil {
			return fmt.Errorf("error inserting into temp table: %w", err)
		}
	}

	return nil
}

// pendingGrant tracks a grant that needs to be created or updated.
type grantToUpdate struct {
	sources   []string
	grantData *v2.Grant
}

// dfsPageSize is the number of rows to fetch per page during DFS expansion.
const dfsPageSize = 10000

type row struct {
	principalType   string
	principalID     string
	entitlementID   string
	grantID         int64
	grantData       []byte
	entitlementData []byte
	rowID           int64
}

// ExpandGrantsDFS performs grant expansion using the DFS (per-principal) approach.
// This touches each grant exactly once, eliminating the need for rectification.
// Uses cursor-based pagination to avoid loading entire result set into memory.
func (c *C1File) ExpandGrantsDFS(ctx context.Context, graph *expand.EntitlementGraph) (*DFSExpansionStats, error) {
	ctx, span := tracer.Start(ctx, "C1File.ExpandGrantsDFS")
	defer span.End()

	l := ctxzap.Extract(ctx)

	stats := &DFSExpansionStats{
		EntitlementsInGraph: len(graph.EntitlementsToNodes),
	}

	// Step 1: Precompute transitive descendants
	l.Debug("ExpandGrantsDFS: computing transitive descendants")
	descendants := graph.ComputeTransitiveDescendants()

	// Step 2: Create temp table with topological order
	l.Debug("ExpandGrantsDFS: creating temp table")
	if err := c.CreateDFSTempTable(ctx, graph); err != nil {
		return nil, fmt.Errorf("error creating temp table: %w", err)
	}

	// Step 3: Page through grants on graph nodes using cursor-based pagination
	gTable := grants.Name()
	entTable := entitlements.Name()

	// Per-principal state (persists across pages)
	var currentPrincipalResource *v2.Resource

	// Entitlement cache - only for current principal per page
	entitlementCache := make(map[string]*v2.Entitlement)

	// Cursor state for pagination
	var cursorPrincipalType, cursorPrincipalID string
	var cursorRowID int64
	firstPage := true

	// Page through results
	for {
		var query string
		var args []interface{}

		if firstPage {
			// First page: no cursor filter
			// Start from temp table (t) to ensure we get all expandable entitlements
			// JOIN entitlements (ent) on temp table to always get entitlement data
			// LEFT JOIN grants (g) - only sometimes have grants
			// Filter to only process rows where grants exist (we're iterating through grants)
			query = `
				SELECT 
					g.principal_resource_type_id,
					g.principal_resource_id,
					t.entitlement_id,
					g.id,
					ent.data,
					g.data,
					t.rowid
				FROM ` + dfsExpansionTempTable + ` t
				JOIN ` + entTable + ` ent ON ent.external_id = t.entitlement_id AND ent.sync_id = ?
				LEFT JOIN ` + gTable + ` g ON g.entitlement_id = t.entitlement_id AND g.sync_id = ?
				WHERE g.id IS NOT NULL
				ORDER BY g.principal_resource_type_id, g.principal_resource_id, t.rowid
				LIMIT ?
			`
			args = []interface{}{c.currentSyncID, c.currentSyncID, dfsPageSize}
			firstPage = false
		} else {
			// Subsequent pages: use cursor-based pagination
			// The cursor is (principal_type, principal_id, rowid, entitlement_id)
			query = `
				SELECT 
					g.principal_resource_type_id,
					g.principal_resource_id,
					t.entitlement_id,
					g.id,
					ent.data,
					g.data,
					t.rowid
				FROM ` + dfsExpansionTempTable + ` t
				JOIN ` + entTable + ` ent ON ent.external_id = t.entitlement_id AND ent.sync_id = ?
				LEFT JOIN ` + gTable + ` g ON g.entitlement_id = t.entitlement_id AND g.sync_id = ?
				WHERE g.id IS NOT NULL
				  AND (g.principal_resource_type_id, g.principal_resource_id, t.rowid) > (?, ?, ?)
				ORDER BY g.principal_resource_type_id, g.principal_resource_id, t.rowid
				LIMIT ?
			`
			args = []interface{}{c.currentSyncID, c.currentSyncID, cursorPrincipalType, cursorPrincipalID, cursorRowID, dfsPageSize}
		}

		rows, err := c.db.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, fmt.Errorf("error querying grants page: %w", err)
		}

		rowCount := 0
		rowsData := make(map[string][]row)
		for rows.Next() {
			var r row
			if err := rows.Scan(&r.principalType, &r.principalID, &r.entitlementID, &r.grantID, &r.entitlementData, &r.grantData, &r.rowID); err != nil {
				rows.Close()
				return nil, fmt.Errorf("error scanning row: %w", err)
			}
			rowsData[r.principalID] = append(rowsData[r.principalID], r)

			// Entitlement data is always present (we JOIN entitlements on temp table)
			// Unmarshal entitlement if not already cached
			if _, ok := entitlementCache[r.entitlementID]; !ok {
				if r.entitlementData == nil {
					rows.Close()
					return nil, fmt.Errorf("entitlement data is NULL for entitlement %s (should never happen)", r.entitlementID)
				}
				ent := &v2.Entitlement{}
				if err := proto.Unmarshal(r.entitlementData, ent); err != nil {
					rows.Close()
					return nil, fmt.Errorf("error unmarshaling entitlement %s: %w", r.entitlementID, err)
				}
				entitlementCache[r.entitlementID] = ent
			}
			rowCount++
			cursorPrincipalType = r.principalType
			cursorPrincipalID = r.principalID
			cursorRowID = r.rowID
		}
		rows.Close()

		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("error iterating rows: %w", err)
		}

		for principalID, rows := range rowsData {
			if currentPrincipalResource.GetId().GetResource() != principalID {
				if len(rows) == 0 {
					l.Warn("ExpandGrantsDFS: no rows for principal", zap.String("principalID", principalID))
					continue
				}
				principalType := rows[0].principalType
				l.Debug("ExpandGrantsDFS: loading principal resource", zap.String("principalType", principalType), zap.String("principalID", principalID))
				currentPrincipalResource, err = c.loadResource(ctx, principalType, principalID)
				if err != nil {
					return nil, fmt.Errorf("error loading principal resource: %w", err)
				}
				stats.PrincipalsProcessed++

			}

			inserted, updated, err := c.processPrincipalExpansion(
				ctx,
				currentPrincipalResource,
				rows,
				descendants,
				entitlementCache,
			)

			if err != nil {
				return nil, fmt.Errorf("error processing principal expansion: %w", err)
			}
			stats.GrantsInserted += inserted
			stats.GrantsUpdated += updated
			if stats.PrincipalsProcessed%10000 == 0 {
				l.Debug("ExpandGrantsDFS: progress",
					zap.Int64("principals", stats.PrincipalsProcessed),
					zap.Int64("inserted", stats.GrantsInserted),
					zap.Int64("updated", stats.GrantsUpdated),
				)
			}
		}

		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, fmt.Errorf("error iterating rows: %w", err)
		}
		rows.Close()
		if rowCount < dfsPageSize {
			break
		}
	}

	l.Info("ExpandGrantsDFS: complete",
		zap.Int64("principals", stats.PrincipalsProcessed),
		zap.Int64("inserted", stats.GrantsInserted),
		zap.Int64("updated", stats.GrantsUpdated),
	)

	return stats, nil
}

// processPrincipalExpansion computes and writes expanded grants for a single principal.
func (c *C1File) processPrincipalExpansion(
	ctx context.Context,
	principal *v2.Resource,
	rows []row,
	descendants expand.TransitiveDescendants,
	entitlementCache map[string]*v2.Entitlement,
) (int64, int64, error) {

	created := int64(0)
	updated := int64(0)
	// Compute pending grants
	grantsToUpdate := make([]*v2.Grant, 0)

	for _, row := range rows {
		entitlementID := row.entitlementID
		grantBlob := row.grantData
		grant := &v2.Grant{}
		sourcesMap := make(map[string]*v2.GrantSources_GrantSource)

		if grantBlob != nil {
			if err := proto.Unmarshal(grantBlob, grant); err != nil {
				return 0, 0, fmt.Errorf("error unmarshaling grant: %w", err)
			}
			sm := grant.GetSources().GetSources()
			if sm != nil {
				sourcesMap = sm
			}
			updated++
		} else {
			grant = &v2.Grant{
				Entitlement: entitlementCache[entitlementID],
				Principal:   principal,
			}
			created++
		}
		needsUpdate := false
		for _, src := range descendants[entitlementID] {
			if _, ok := sourcesMap[src]; ok {
				continue
			}
			needsUpdate = true
			sourcesMap[src] = &v2.GrantSources_GrantSource{}
		}
		if needsUpdate || row.grantData == nil {
			grant.Sources = &v2.GrantSources{Sources: sourcesMap}
			grantsToUpdate = append(grantsToUpdate, grant)
		}
	}

	if err := c.PutGrants(ctx, grantsToUpdate...); err != nil {
		return 0, 0, fmt.Errorf("error inserting grants: %w", err)
	}

	return created, updated, nil
}

// loadResource loads a Resource proto by type and ID.
func (c *C1File) loadResource(ctx context.Context, resourceType, resourceID string) (*v2.Resource, error) {
	resp, err := c.GetResource(ctx, reader_v2.ResourcesReaderServiceGetResourceRequest_builder{
		ResourceId: v2.ResourceId_builder{
			ResourceType: resourceType,
			Resource:     resourceID,
		}.Build(),
	}.Build())
	if err != nil {
		return nil, err
	}
	return resp.GetResource(), nil
}

// // insertNewExpandedGrants creates new expanded grants for a principal.
// func (c *C1File) insertNewExpandedGrants(
// 	ctx context.Context,
// 	principal *v2.Resource,
// 	newGrants []*pendingGrant,
// 	entitlementCache map[string]*v2.Entitlement,
// ) (int64, error) {
// 	if len(newGrants) == 0 {
// 		return 0, nil
// 	}

// 	grantsToInsert := make([]*v2.Grant, 0, len(newGrants))

// 	for _, pg := range newGrants {
// 		// Entitlement should always be in cache (we JOIN entitlements on temp table in the query)
// 		entitlement, ok := entitlementCache[pg.entitlementID]
// 		if !ok {
// 			return 0, fmt.Errorf("entitlement %s not found in cache (should never happen)", pg.entitlementID)
// 		}

// 		// Build sources map
// 		sourcesMap := make(map[string]*v2.GrantSources_GrantSource, len(pg.sources))
// 		for _, src := range pg.sources {
// 			sourcesMap[src] = &v2.GrantSources_GrantSource{}
// 		}

// 		// Build grant with immutable annotation
// 		var annos annotations.Annotations
// 		annos.Update(&v2.GrantImmutable{})

// 		grant := v2.Grant_builder{
// 			Id:          fmt.Sprintf("%s:%s:%s", pg.entitlementID, principal.GetId().GetResourceType(), principal.GetId().GetResource()),
// 			Entitlement: entitlement,
// 			Principal:   principal,
// 			Sources:     &v2.GrantSources{Sources: sourcesMap},
// 			Annotations: annos,
// 		}.Build()

// 		grantsToInsert = append(grantsToInsert, grant)
// 	}

// 	if err := c.PutGrants(ctx, grantsToInsert...); err != nil {
// 		return 0, fmt.Errorf("error inserting grants: %w", err)
// 	}

// 	return int64(len(grantsToInsert)), nil
// }

// // updateExistingGrantSources updates sources on existing grants.
// func (c *C1File) updateExistingGrantSources(
// 	ctx context.Context,
// 	updateGrants []*pendingGrant,
// ) (int64, error) {
// 	if len(updateGrants) == 0 {
// 		return 0, nil
// 	}

// 	// Batch fetch existing grants
// 	grantIDs := make([]int64, len(updateGrants))
// 	grantIDToUpdate := make(map[int64]*pendingGrant, len(updateGrants))
// 	for i, pg := range updateGrants {
// 		grantIDs[i] = pg.existingID
// 		grantIDToUpdate[pg.existingID] = pg
// 	}

// 	// Build query to fetch grant data
// 	placeholders := make([]string, len(grantIDs))
// 	args := make([]interface{}, len(grantIDs))
// 	for i, id := range grantIDs {
// 		placeholders[i] = "?"
// 		args[i] = id
// 	}

// 	query := `SELECT id, data FROM ` + grants.Name() + ` WHERE id IN (` + strings.Join(placeholders, ",") + `)`
// 	rows, err := c.db.QueryContext(ctx, query, args...)
// 	if err != nil {
// 		return 0, fmt.Errorf("error fetching grants for update: %w", err)
// 	}
// 	defer rows.Close()

// 	grantsToUpdate := make([]*v2.Grant, 0, len(updateGrants))

// 	for rows.Next() {
// 		var id int64
// 		var data []byte
// 		if err := rows.Scan(&id, &data); err != nil {
// 			return 0, fmt.Errorf("error scanning grant: %w", err)
// 		}

// 		pg := grantIDToUpdate[id]
// 		if pg == nil {
// 			continue
// 		}

// 		// Unmarshal existing grant
// 		grant := &v2.Grant{}
// 		if err := proto.Unmarshal(data, grant); err != nil {
// 			return 0, fmt.Errorf("error unmarshaling grant: %w", err)
// 		}

// 		// Merge sources
// 		existingSources := grant.GetSources().GetSources()
// 		if existingSources == nil {
// 			existingSources = make(map[string]*v2.GrantSources_GrantSource)
// 		}

// 		needsUpdate := false
// 		for _, src := range pg.sources {
// 			if existingSources[src] == nil {
// 				existingSources[src] = &v2.GrantSources_GrantSource{}
// 				needsUpdate = true
// 			}
// 		}

// 		if needsUpdate {
// 			grant.SetSources(&v2.GrantSources{Sources: existingSources})
// 			grantsToUpdate = append(grantsToUpdate, grant)
// 		}
// 	}

// 	if err := rows.Err(); err != nil {
// 		return 0, fmt.Errorf("error iterating grants: %w", err)
// 	}

// 	if len(grantsToUpdate) > 0 {
// 		if err := c.PutGrants(ctx, grantsToUpdate...); err != nil {
// 			return 0, fmt.Errorf("error updating grants: %w", err)
// 		}
// 	}

// 	return int64(len(grantsToUpdate)), nil
// }

// DropDFSTempTable drops the temporary table used for DFS expansion.
func (c *C1File) DropDFSTempTable(ctx context.Context) error {
	_, err := c.db.ExecContext(ctx, `DROP TABLE IF EXISTS `+dfsExpansionTempTable)
	return err
}
