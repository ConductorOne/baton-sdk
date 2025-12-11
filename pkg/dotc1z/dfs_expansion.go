package dotc1z

import (
	"context"
	"fmt"
	"strings"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
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
type pendingGrant struct {
	entitlementID string
	sources       []string
	existingID    int64 // 0 if new grant
}

// dfsPageSize is the number of rows to fetch per page during DFS expansion.
const dfsPageSize = 10000

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
	var currentPrincipalType, currentPrincipalID string
	var currentPrincipalResource *v2.Resource
	hasGrantOn := make(map[string]bool)
	existingGrantID := make(map[string]int64)

	// Entitlement cache - only for current principal (cleared when moving to next principal)
	entitlementCache := make(map[string]*v2.Entitlement)

	// Cursor state for pagination
	var cursorPrincipalType, cursorPrincipalID string
	var cursorRowID int64
	firstPage := true

	// Process function for end of principal
	flushPrincipal := func() error {
		if currentPrincipalType == "" {
			return nil
		}

		inserted, updated, err := c.processPrincipalExpansion(
			ctx,
			currentPrincipalResource,
			hasGrantOn,
			existingGrantID,
			descendants,
			graph,
			entitlementCache,
		)
		if err != nil {
			return err
		}

		stats.PrincipalsProcessed++
		stats.GrantsInserted += inserted
		stats.GrantsUpdated += updated

		if stats.PrincipalsProcessed%10000 == 0 {
			l.Debug("ExpandGrantsDFS: progress",
				zap.Int64("principals", stats.PrincipalsProcessed),
				zap.Int64("inserted", stats.GrantsInserted),
				zap.Int64("updated", stats.GrantsUpdated),
			)
		}

		return nil
	}

	// Page through results
	for {
		var query string
		var args []interface{}

		if firstPage {
			// First page: no cursor filter
			query = `
				SELECT 
					g.principal_resource_type_id,
					g.principal_resource_id,
					g.entitlement_id,
					g.id,
					ent.data,
					t.rowid
				FROM ` + gTable + ` g
				JOIN ` + dfsExpansionTempTable + ` t ON g.entitlement_id = t.entitlement_id
				LEFT JOIN ` + entTable + ` ent ON ent.external_id = g.entitlement_id AND ent.sync_id = ?
				WHERE g.sync_id = ?
				ORDER BY g.principal_resource_type_id, g.principal_resource_id, t.rowid
				LIMIT ?
			`
			args = []interface{}{c.currentSyncID, c.currentSyncID, dfsPageSize}
			firstPage = false
		} else {
			// Subsequent pages: use cursor-based pagination
			// The cursor is (principal_type, principal_id, rowid)
			query = `
				SELECT 
					g.principal_resource_type_id,
					g.principal_resource_id,
					g.entitlement_id,
					g.id,
					ent.data,
					t.rowid
				FROM ` + gTable + ` g
				JOIN ` + dfsExpansionTempTable + ` t ON g.entitlement_id = t.entitlement_id
				LEFT JOIN ` + entTable + ` ent ON ent.external_id = g.entitlement_id AND ent.sync_id = ?
				WHERE g.sync_id = ?
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
		for rows.Next() {
			var principalType, principalID, entitlementID string
			var grantID, rowID int64
			var entitlementData []byte

			if err := rows.Scan(&principalType, &principalID, &entitlementID, &grantID, &entitlementData, &rowID); err != nil {
				rows.Close()
				return nil, fmt.Errorf("error scanning row: %w", err)
			}

			rowCount++

			// Unmarshal entitlement if not already cached
			if _, ok := entitlementCache[entitlementID]; !ok && entitlementData != nil {
				ent := &v2.Entitlement{}
				if err := proto.Unmarshal(entitlementData, ent); err != nil {
					rows.Close()
					return nil, fmt.Errorf("error unmarshaling entitlement %s: %w", entitlementID, err)
				}
				entitlementCache[entitlementID] = ent
			}

			// Update cursor for next page
			cursorPrincipalType = principalType
			cursorPrincipalID = principalID
			cursorRowID = rowID

			// Check if we've moved to a new principal
			if principalType != currentPrincipalType || principalID != currentPrincipalID {
				// Flush previous principal
				if err := flushPrincipal(); err != nil {
					rows.Close()
					return nil, err
				}

				// Reset state for new principal
				currentPrincipalType = principalType
				currentPrincipalID = principalID
				clear(hasGrantOn)
				clear(existingGrantID)
				clear(entitlementCache) // Clear cache for new principal

				// Load the principal's Resource proto
				currentPrincipalResource, err = c.loadResource(ctx, principalType, principalID)
				if err != nil {
					rows.Close()
					return nil, fmt.Errorf("error loading principal resource: %w", err)
				}
			}

			// Track this grant
			hasGrantOn[entitlementID] = true
			existingGrantID[entitlementID] = grantID
		}

		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, fmt.Errorf("error iterating rows: %w", err)
		}
		rows.Close()

		// If we got fewer rows than the page size, we've reached the end
		if rowCount < dfsPageSize {
			// Flush final principal
			if err := flushPrincipal(); err != nil {
				return nil, err
			}
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
	hasGrantOn map[string]bool,
	existingGrantID map[string]int64,
	descendants expand.TransitiveDescendants,
	graph *expand.EntitlementGraph,
	entitlementCache map[string]*v2.Entitlement,
) (int64, int64, error) {
	// Compute pending grants
	pendingGrants := make(map[string]*pendingGrant)

	for entitlementID := range hasGrantOn {
		// Get all transitive descendants
		for _, descendantID := range descendants[entitlementID] {
			pg, ok := pendingGrants[descendantID]
			if !ok {
				pg = &pendingGrant{
					entitlementID: descendantID,
					sources:       make([]string, 0),
					existingID:    existingGrantID[descendantID],
				}
				pendingGrants[descendantID] = pg
			}
			pg.sources = append(pg.sources, entitlementID)
		}
	}

	if len(pendingGrants) == 0 {
		return 0, 0, nil
	}

	// Separate into new grants and updates
	var newGrants []*pendingGrant
	var updateGrants []*pendingGrant

	for _, pg := range pendingGrants {
		// Skip if this is already a direct grant (sources come from edges pointing TO it)
		// The sources should only include entitlements that the principal has a grant on
		// AND that have an edge to this descendant
		filteredSources := make([]string, 0, len(pg.sources))
		sourceEntitlements := graph.GetSourceEntitlements(pg.entitlementID)
		sourceSet := make(map[string]bool, len(sourceEntitlements))
		for _, s := range sourceEntitlements {
			sourceSet[s] = true
		}

		for _, src := range pg.sources {
			// Only include if there's actually an edge from src to this entitlement
			// AND the principal has a grant on src
			if sourceSet[src] && hasGrantOn[src] {
				filteredSources = append(filteredSources, src)
			}
		}

		if len(filteredSources) == 0 {
			continue // No valid sources, skip
		}
		pg.sources = filteredSources

		if pg.existingID == 0 {
			newGrants = append(newGrants, pg)
		} else {
			updateGrants = append(updateGrants, pg)
		}
	}

	var inserted, updated int64

	// Handle new grants
	if len(newGrants) > 0 {
		n, err := c.insertNewExpandedGrants(ctx, principal, newGrants, entitlementCache)
		if err != nil {
			return 0, 0, err
		}
		inserted = n
	}

	// Handle updates
	if len(updateGrants) > 0 {
		n, err := c.updateExistingGrantSources(ctx, updateGrants)
		if err != nil {
			return inserted, 0, err
		}
		updated = n
	}

	return inserted, updated, nil
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

// insertNewExpandedGrants creates new expanded grants for a principal.
func (c *C1File) insertNewExpandedGrants(
	ctx context.Context,
	principal *v2.Resource,
	newGrants []*pendingGrant,
	entitlementCache map[string]*v2.Entitlement,
) (int64, error) {
	if len(newGrants) == 0 {
		return 0, nil
	}

	l := ctxzap.Extract(ctx)

	// Collect entitlement IDs that aren't in cache
	missingEntitlementIDs := make([]string, 0)
	for _, pg := range newGrants {
		if _, ok := entitlementCache[pg.entitlementID]; !ok {
			missingEntitlementIDs = append(missingEntitlementIDs, pg.entitlementID)
		}
	}

	// Batch fetch missing entitlements
	if len(missingEntitlementIDs) > 0 {
		entTable := entitlements.Name()
		placeholders := make([]string, len(missingEntitlementIDs))
		args := make([]interface{}, len(missingEntitlementIDs)+1)
		for i, eid := range missingEntitlementIDs {
			placeholders[i] = "?"
			args[i] = eid
		}
		args[len(missingEntitlementIDs)] = c.currentSyncID

		query := `SELECT external_id, data FROM ` + entTable + ` WHERE external_id IN (` + strings.Join(placeholders, ",") + `) AND sync_id = ?`
		rows, err := c.db.QueryContext(ctx, query, args...)
		if err != nil {
			return 0, fmt.Errorf("error batch fetching entitlements: %w", err)
		}

		for rows.Next() {
			var entitlementID string
			var data []byte
			if err := rows.Scan(&entitlementID, &data); err != nil {
				rows.Close()
				return 0, fmt.Errorf("error scanning entitlement: %w", err)
			}
			ent := &v2.Entitlement{}
			if err := proto.Unmarshal(data, ent); err != nil {
				rows.Close()
				return 0, fmt.Errorf("error unmarshaling entitlement %s: %w", entitlementID, err)
			}
			entitlementCache[entitlementID] = ent
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return 0, fmt.Errorf("error iterating entitlements: %w", err)
		}
		rows.Close()
	}

	grantsToInsert := make([]*v2.Grant, 0, len(newGrants))

	for _, pg := range newGrants {
		// Look up entitlement from cache (now includes batch-fetched entitlements)
		entitlement, ok := entitlementCache[pg.entitlementID]
		if !ok {
			// Skip if still not found after batch fetch (entitlement doesn't exist)
			l.Warn("insertNewExpandedGrants: skipping missing entitlement",
				zap.String("entitlement_id", pg.entitlementID),
			)
			continue
		}

		// Build sources map
		sourcesMap := make(map[string]*v2.GrantSources_GrantSource, len(pg.sources))
		for _, src := range pg.sources {
			sourcesMap[src] = &v2.GrantSources_GrantSource{}
		}

		// Build grant with immutable annotation
		var annos annotations.Annotations
		annos.Update(&v2.GrantImmutable{})

		grant := v2.Grant_builder{
			Id:          fmt.Sprintf("%s:%s:%s", pg.entitlementID, principal.GetId().GetResourceType(), principal.GetId().GetResource()),
			Entitlement: entitlement,
			Principal:   principal,
			Sources:     &v2.GrantSources{Sources: sourcesMap},
			Annotations: annos,
		}.Build()

		grantsToInsert = append(grantsToInsert, grant)
	}

	if err := c.PutGrants(ctx, grantsToInsert...); err != nil {
		return 0, fmt.Errorf("error inserting grants: %w", err)
	}

	return int64(len(grantsToInsert)), nil
}

// updateExistingGrantSources updates sources on existing grants.
func (c *C1File) updateExistingGrantSources(
	ctx context.Context,
	updateGrants []*pendingGrant,
) (int64, error) {
	if len(updateGrants) == 0 {
		return 0, nil
	}

	// Batch fetch existing grants
	grantIDs := make([]int64, len(updateGrants))
	grantIDToUpdate := make(map[int64]*pendingGrant, len(updateGrants))
	for i, pg := range updateGrants {
		grantIDs[i] = pg.existingID
		grantIDToUpdate[pg.existingID] = pg
	}

	// Build query to fetch grant data
	placeholders := make([]string, len(grantIDs))
	args := make([]interface{}, len(grantIDs))
	for i, id := range grantIDs {
		placeholders[i] = "?"
		args[i] = id
	}

	query := `SELECT id, data FROM ` + grants.Name() + ` WHERE id IN (` + strings.Join(placeholders, ",") + `)`
	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("error fetching grants for update: %w", err)
	}
	defer rows.Close()

	grantsToUpdate := make([]*v2.Grant, 0, len(updateGrants))

	for rows.Next() {
		var id int64
		var data []byte
		if err := rows.Scan(&id, &data); err != nil {
			return 0, fmt.Errorf("error scanning grant: %w", err)
		}

		pg := grantIDToUpdate[id]
		if pg == nil {
			continue
		}

		// Unmarshal existing grant
		grant := &v2.Grant{}
		if err := proto.Unmarshal(data, grant); err != nil {
			return 0, fmt.Errorf("error unmarshaling grant: %w", err)
		}

		// Merge sources
		existingSources := grant.GetSources().GetSources()
		if existingSources == nil {
			existingSources = make(map[string]*v2.GrantSources_GrantSource)
		}

		needsUpdate := false
		for _, src := range pg.sources {
			if existingSources[src] == nil {
				existingSources[src] = &v2.GrantSources_GrantSource{}
				needsUpdate = true
			}
		}

		if needsUpdate {
			grant.SetSources(&v2.GrantSources{Sources: existingSources})
			grantsToUpdate = append(grantsToUpdate, grant)
		}
	}

	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("error iterating grants: %w", err)
	}

	if len(grantsToUpdate) > 0 {
		if err := c.PutGrants(ctx, grantsToUpdate...); err != nil {
			return 0, fmt.Errorf("error updating grants: %w", err)
		}
	}

	return int64(len(grantsToUpdate)), nil
}

// DropDFSTempTable drops the temporary table used for DFS expansion.
func (c *C1File) DropDFSTempTable(ctx context.Context) error {
	_, err := c.db.ExecContext(ctx, `DROP TABLE IF EXISTS `+dfsExpansionTempTable)
	return err
}
