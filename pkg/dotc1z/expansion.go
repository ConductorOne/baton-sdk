package dotc1z

import (
	"context"
	"fmt"
)

// ExpandGrantsRecursive performs the entire grant expansion in SQL using WITH RECURSIVE.
// It:
// 1. Inserts new grants for all principals reachable through the entitlement graph
// 2. Populates the grant_sources table with all source relationships
//
// This replaces the iterative depth-by-depth expansion with a single SQL operation.
func (c *C1File) ExpandGrantsRecursive(ctx context.Context, maxDepth int) (int64, error) {
	ctx, span := tracer.Start(ctx, "C1File.ExpandGrantsRecursive")
	defer span.End()

	if maxDepth <= 0 {
		maxDepth = 16 // Default max depth
	}

	gTable := grants.Name()
	edgeTable := entitlementEdges.Name()
	entTable := entitlements.Name()
	gsTable := grantSources.Name()

	// Phase 1: Insert all expanded grants using WITH RECURSIVE
	// This finds all reachable (entitlement, principal) pairs and inserts missing grants
	insertResult, err := c.db.ExecContext(ctx, `
		WITH RECURSIVE reachable(
			source_grant_id,
			source_entitlement_id, 
			descendant_entitlement_id, 
			principal_type_id, 
			principal_id,
			depth,
			path
		) AS (
			-- Base case: direct grants (depth 0)
			SELECT 
				g.id,
				g.entitlement_id,
				g.entitlement_id,
				g.principal_resource_type_id,
				g.principal_resource_id,
				0,
				g.entitlement_id
			FROM `+gTable+` g
			WHERE g.sync_id = ?
			
			UNION ALL
			
			-- Recursive case: follow edges in the graph
			SELECT
				r.source_grant_id,
				r.descendant_entitlement_id,
				e.descendant_entitlement_id,
				r.principal_type_id,
				r.principal_id,
				r.depth + 1,
				r.path || '->' || e.descendant_entitlement_id
			FROM reachable r
			JOIN `+edgeTable+` e 
				ON e.source_entitlement_id = r.descendant_entitlement_id
				AND e.sync_id = ?
			WHERE r.depth < ?
			  -- Shallow edge handling: only follow if source has the entitlement in sources
			  AND (
				  e.is_shallow = 0
				  OR EXISTS (
					  SELECT 1 FROM `+gsTable+` gs
					  WHERE gs.grant_id = r.source_grant_id
					    AND gs.source_entitlement_id = r.source_entitlement_id
				  )
				  -- Or it's a direct grant (depth 0)
				  OR r.depth = 0
			  )
			  -- Resource type filter
			  AND (
				  e.resource_type_ids IS NULL
				  OR r.principal_type_id IN (SELECT value FROM json_each(e.resource_type_ids))
			  )
			  -- Prevent infinite loops: don't revisit entitlements in path
			  AND instr(r.path, e.descendant_entitlement_id) = 0
		)
		INSERT INTO `+gTable+` (
			resource_type_id,
			resource_id,
			entitlement_id,
			principal_resource_type_id,
			principal_resource_id,
			external_id,
			data,
			sync_id,
			discovered_at,
			sources
		)
		SELECT DISTINCT
			ent.resource_type_id,
			ent.resource_id,
			r.descendant_entitlement_id,
			r.principal_type_id,
			r.principal_id,
			r.descendant_entitlement_id || ':' || r.principal_type_id || ':' || r.principal_id,
			X'',  -- Empty data, to be rectified later
			?,
			datetime('now'),
			'{}'  -- Empty sources, populated in phase 2
		FROM reachable r
		JOIN `+entTable+` ent 
			ON ent.external_id = r.descendant_entitlement_id 
			AND ent.sync_id = ?
		WHERE r.depth > 0  -- Only expanded grants (not the originals)
		  AND NOT EXISTS (
			  SELECT 1 FROM `+gTable+` existing
			  WHERE existing.entitlement_id = r.descendant_entitlement_id
			    AND existing.principal_resource_type_id = r.principal_type_id
			    AND existing.principal_resource_id = r.principal_id
			    AND existing.sync_id = ?
		  )
		ON CONFLICT (external_id, sync_id) DO NOTHING
	`,
		c.currentSyncID, // Base case WHERE
		c.currentSyncID, // Recursive JOIN edge table
		maxDepth,        // Depth limit
		c.currentSyncID, // INSERT sync_id value
		c.currentSyncID, // JOIN entitlements
		c.currentSyncID, // NOT EXISTS check
	)
	if err != nil {
		return 0, fmt.Errorf("error inserting expanded grants: %w", err)
	}

	grantsInserted, err := insertResult.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("error getting inserted rows count: %w", err)
	}

	// Phase 2: Populate grant_sources table with all source relationships
	// This tracks which entitlements contributed to each grant
	_, err = c.db.ExecContext(ctx, `
		WITH RECURSIVE reachable(
			grant_id,
			source_entitlement_id,
			descendant_entitlement_id,
			principal_type_id,
			principal_id,
			depth,
			path
		) AS (
			-- Base case: direct grants contribute themselves as source
			SELECT 
				g.id,
				g.entitlement_id,
				g.entitlement_id,
				g.principal_resource_type_id,
				g.principal_resource_id,
				0,
				g.entitlement_id
			FROM `+gTable+` g
			WHERE g.sync_id = ?
			
			UNION ALL
			
			-- Recursive case: follow edges
			SELECT
				r.grant_id,
				r.descendant_entitlement_id,  -- Previous step becomes source
				e.descendant_entitlement_id,
				r.principal_type_id,
				r.principal_id,
				r.depth + 1,
				r.path || '->' || e.descendant_entitlement_id
			FROM reachable r
			JOIN `+edgeTable+` e 
				ON e.source_entitlement_id = r.descendant_entitlement_id
				AND e.sync_id = ?
			WHERE r.depth < ?
			  AND (
				  e.is_shallow = 0
				  OR EXISTS (
					  SELECT 1 FROM `+gsTable+` gs
					  WHERE gs.grant_id = r.grant_id
					    AND gs.source_entitlement_id = r.source_entitlement_id
				  )
				  OR r.depth = 0
			  )
			  AND (
				  e.resource_type_ids IS NULL
				  OR r.principal_type_id IN (SELECT value FROM json_each(e.resource_type_ids))
			  )
			  AND instr(r.path, e.descendant_entitlement_id) = 0
		)
		INSERT OR IGNORE INTO `+gsTable+` (grant_id, source_entitlement_id, sync_id)
		SELECT DISTINCT
			dest.id,
			r.source_entitlement_id,
			?
		FROM reachable r
		-- Find the actual grant for this (entitlement, principal) pair
		JOIN `+gTable+` dest 
			ON dest.entitlement_id = r.descendant_entitlement_id
			AND dest.principal_resource_type_id = r.principal_type_id
			AND dest.principal_resource_id = r.principal_id
			AND dest.sync_id = ?
		WHERE r.depth > 0
	`,
		c.currentSyncID, // Base case
		c.currentSyncID, // Recursive JOIN
		maxDepth,        // Depth limit
		c.currentSyncID, // INSERT sync_id value
		c.currentSyncID, // JOIN grants
	)
	if err != nil {
		return grantsInserted, fmt.Errorf("error populating grant sources: %w", err)
	}

	return grantsInserted, nil
}

// AddGrantSource adds a source entitlement to a grant in the normalized table.
func (c *C1File) AddGrantSource(ctx context.Context, grantID int64, sourceEntitlementID string) error {
	_, err := c.db.ExecContext(ctx, `
		INSERT OR IGNORE INTO `+grantSources.Name()+` (grant_id, source_entitlement_id, sync_id)
		VALUES (?, ?, ?)
	`, grantID, sourceEntitlementID, c.currentSyncID)
	if err != nil {
		return fmt.Errorf("error adding grant source: %w", err)
	}
	return nil
}

// AddGrantSources adds multiple source entitlements to a grant in the normalized table.
func (c *C1File) AddGrantSources(ctx context.Context, grantID int64, sourceEntitlementIDs []string) error {
	if len(sourceEntitlementIDs) == 0 {
		return nil
	}

	stmt, err := c.rawDb.PrepareContext(ctx, `
		INSERT OR IGNORE INTO `+grantSources.Name()+` (grant_id, source_entitlement_id, sync_id)
		VALUES (?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("error preparing grant source insert: %w", err)
	}
	defer stmt.Close()

	for _, srcEnt := range sourceEntitlementIDs {
		_, err = stmt.ExecContext(ctx, grantID, srcEnt, c.currentSyncID)
		if err != nil {
			return fmt.Errorf("error inserting grant source: %w", err)
		}
	}
	return nil
}

// GetGrantSources returns all source entitlement IDs for a given grant.
func (c *C1File) GetGrantSources(ctx context.Context, grantID int64) ([]string, error) {
	rows, err := c.db.QueryContext(ctx, `
		SELECT source_entitlement_id 
		FROM `+grantSources.Name()+` 
		WHERE grant_id = ? AND sync_id = ?
	`, grantID, c.currentSyncID)
	if err != nil {
		return nil, fmt.Errorf("error querying grant sources: %w", err)
	}
	defer rows.Close()

	var sources []string
	for rows.Next() {
		var src string
		if err := rows.Scan(&src); err != nil {
			return nil, fmt.Errorf("error scanning grant source: %w", err)
		}
		sources = append(sources, src)
	}
	return sources, rows.Err()
}

// MigrateGrantSourcesToNormalized migrates existing JSON sources to the normalized table.
// This should be run once after adding the grant_sources table.
func (c *C1File) MigrateGrantSourcesToNormalized(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "C1File.MigrateGrantSourcesToNormalized")
	defer span.End()

	_, err := c.db.ExecContext(ctx, `
		INSERT OR IGNORE INTO `+grantSources.Name()+` (grant_id, source_entitlement_id, sync_id)
		SELECT 
			g.id,
			j.key,
			g.sync_id
		FROM `+grants.Name()+` g,
		     json_each(CASE 
		         WHEN json_type(g.sources) = 'object' THEN g.sources 
		         ELSE '{}' 
		     END) j
		WHERE g.sync_id = ?
	`, c.currentSyncID)
	if err != nil {
		return fmt.Errorf("error migrating grant sources: %w", err)
	}
	return nil
}

// ExpandGrantsSingleLevel expands grants for a single edge (source -> descendant).
// This is a simpler version for when you want to expand one edge at a time.
func (c *C1File) ExpandGrantsSingleLevel(
	ctx context.Context,
	sourceEntitlementID string,
	descendantEntitlementID string,
	resourceTypeIDs []string,
	shallow bool,
) (inserted int64, updated int64, err error) {
	ctx, span := tracer.Start(ctx, "C1File.ExpandGrantsSingleLevel")
	defer span.End()

	gTable := grants.Name()
	entTable := entitlements.Name()
	gsTable := grantSources.Name()

	// Build resource type filter
	var rtFilter string
	var rtArgs []interface{}
	if len(resourceTypeIDs) > 0 {
		rtFilter = "AND src.principal_resource_type_id IN ("
		for i, rtID := range resourceTypeIDs {
			if i > 0 {
				rtFilter += ", "
			}
			rtFilter += "?"
			rtArgs = append(rtArgs, rtID)
		}
		rtFilter += ")"
	}

	// Build shallow filter
	shallowFilter := ""
	if shallow {
		shallowFilter = `
			AND EXISTS (
				SELECT 1 FROM ` + gsTable + ` gs
				WHERE gs.grant_id = src.id
				  AND gs.source_entitlement_id = ?
			)
		`
	}

	// Phase 1: Insert new grants
	baseArgs := []interface{}{
		descendantEntitlementID,              // SELECT entitlement_id
		descendantEntitlementID,              // external_id prefix
		c.currentSyncID,                      // sync_id
		sourceEntitlementID, c.currentSyncID, // JOIN entitlements
		sourceEntitlementID, c.currentSyncID, // src.entitlement_id
	}
	baseArgs = append(baseArgs, rtArgs...)
	if shallow {
		baseArgs = append(baseArgs, sourceEntitlementID)
	}
	baseArgs = append(baseArgs, descendantEntitlementID, c.currentSyncID)

	insertResult, err := c.db.ExecContext(ctx, `
		INSERT INTO `+gTable+` (
			resource_type_id,
			resource_id,
			entitlement_id,
			principal_resource_type_id,
			principal_resource_id,
			external_id,
			data,
			sync_id,
			discovered_at,
			sources
		)
		SELECT
			ent.resource_type_id,
			ent.resource_id,
			?,
			src.principal_resource_type_id,
			src.principal_resource_id,
			? || ':' || src.principal_resource_type_id || ':' || src.principal_resource_id,
			X'',
			?,
			datetime('now'),
			'{}'
		FROM `+gTable+` src
		JOIN `+entTable+` ent ON ent.external_id = ? AND ent.sync_id = ?
		WHERE src.entitlement_id = ?
		  AND src.sync_id = ?
		  `+rtFilter+`
		  `+shallowFilter+`
		  AND NOT EXISTS (
			  SELECT 1 FROM `+gTable+` dest
			  WHERE dest.entitlement_id = ?
			    AND dest.sync_id = ?
			    AND dest.principal_resource_type_id = src.principal_resource_type_id
			    AND dest.principal_resource_id = src.principal_resource_id
		  )
		ON CONFLICT (external_id, sync_id) DO NOTHING
	`, baseArgs...)
	if err != nil {
		return 0, 0, fmt.Errorf("error inserting expanded grants: %w", err)
	}

	inserted, _ = insertResult.RowsAffected()

	// Phase 2: Add sources to both new and existing grants
	sourceArgs := []interface{}{
		c.currentSyncID,
		descendantEntitlementID, c.currentSyncID,
		sourceEntitlementID, c.currentSyncID,
	}
	sourceArgs = append(sourceArgs, rtArgs...)
	if shallow {
		sourceArgs = append(sourceArgs, sourceEntitlementID)
	}

	updateResult, err := c.db.ExecContext(ctx, `
		INSERT OR IGNORE INTO `+gsTable+` (grant_id, source_entitlement_id, sync_id)
		SELECT 
			dest.id,
			?,
			dest.sync_id
		FROM `+gTable+` dest
		WHERE dest.entitlement_id = ?
		  AND dest.sync_id = ?
		  AND (dest.principal_resource_type_id, dest.principal_resource_id) IN (
			  SELECT src.principal_resource_type_id, src.principal_resource_id
			  FROM `+gTable+` src
			  WHERE src.entitlement_id = ?
			    AND src.sync_id = ?
			    `+rtFilter+`
			    `+shallowFilter+`
		  )
	`, append([]interface{}{sourceEntitlementID}, sourceArgs...)...)
	if err != nil {
		return inserted, 0, fmt.Errorf("error adding grant sources: %w", err)
	}

	updated, _ = updateResult.RowsAffected()
	// updated includes sources added to both new and existing grants
	// Subtract inserted to get just the updates to existing grants
	if updated > inserted {
		updated -= inserted
	} else {
		updated = 0
	}

	return inserted, updated, nil
}
