package dotc1z

import (
	"context"
	"errors"
	"fmt"

	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/doug-martin/goqu/v9"
	"github.com/segmentio/ksuid"
)

type C1FileAttached struct {
	safe bool
	file *C1File
}

func (c *C1FileAttached) CompactTable(ctx context.Context, baseSyncID string, appliedSyncID string, tableName string) error {
	if !c.safe {
		return errors.New("database has been detached")
	}
	ctx, span := tracer.Start(ctx, "C1FileAttached.CompactTable")
	defer span.End()

	// Get the column structure for this table by querying the schema
	columns, err := c.getTableColumns(ctx, tableName)
	if err != nil {
		return fmt.Errorf("failed to get table columns: %w", err)
	}

	// Build column lists for INSERT statements
	columnList := ""
	selectList := ""
	for i, col := range columns {
		if i > 0 {
			columnList += ", "
			selectList += ", "
		}
		columnList += col
		if col == "sync_id" {
			selectList += "? as sync_id"
		} else {
			selectList += col
		}
	}

	// Insert/replace records from applied sync where applied.discovered_at > main.discovered_at
	insertOrReplaceAppliedQuery := fmt.Sprintf(`
		INSERT OR REPLACE INTO main.%s (%s)
		SELECT %s
		FROM attached.%s AS a
		WHERE a.sync_id = ?
		  AND (
		    NOT EXISTS (
		      SELECT 1 FROM main.%s AS m 
		      WHERE m.external_id = a.external_id AND m.sync_id = ?
		    )
		    OR EXISTS (
		      SELECT 1 FROM main.%s AS m 
		      WHERE m.external_id = a.external_id 
		        AND m.sync_id = ?
		        AND a.discovered_at > m.discovered_at
		    )
		  )
	`, tableName, columnList, selectList, tableName, tableName, tableName)

	_, err = c.file.db.ExecContext(ctx, insertOrReplaceAppliedQuery, baseSyncID, appliedSyncID, baseSyncID, baseSyncID)
	return err
}

func (c *C1FileAttached) getTableColumns(ctx context.Context, tableName string) ([]string, error) {
	if !c.safe {
		return nil, errors.New("database has been detached")
	}
	// PRAGMA doesn't support parameter binding, so we format the table name directly
	query := fmt.Sprintf("PRAGMA table_info(%s)", tableName)
	rows, err := c.file.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var cid int
		var name, dataType string
		var notNull, pk int
		var defaultValue any

		err := rows.Scan(&cid, &name, &dataType, &notNull, &defaultValue, &pk)
		if err != nil {
			return nil, err
		}

		// Skip the 'id' column as it's auto-increment
		if name != "id" {
			columns = append(columns, name)
		}
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}

	return columns, nil
}

func (c *C1FileAttached) CompactResourceTypes(ctx context.Context, baseSyncID string, appliedSyncID string) error {
	if !c.safe {
		return errors.New("database has been detached")
	}
	return c.CompactTable(ctx, baseSyncID, appliedSyncID, "v1_resource_types")
}

func (c *C1FileAttached) CompactResources(ctx context.Context, baseSyncID string, appliedSyncID string) error {
	if !c.safe {
		return errors.New("database has been detached")
	}
	return c.CompactTable(ctx, baseSyncID, appliedSyncID, "v1_resources")
}

func (c *C1FileAttached) CompactEntitlements(ctx context.Context, baseSyncID string, appliedSyncID string) error {
	if !c.safe {
		return errors.New("database has been detached")
	}
	return c.CompactTable(ctx, baseSyncID, appliedSyncID, "v1_entitlements")
}

func (c *C1FileAttached) CompactGrants(ctx context.Context, baseSyncID string, appliedSyncID string) error {
	if !c.safe {
		return errors.New("database has been detached")
	}
	return c.CompactTable(ctx, baseSyncID, appliedSyncID, "v1_grants")
}

func unionSyncTypes(a, b connectorstore.SyncType) connectorstore.SyncType {
	switch {
	case a == connectorstore.SyncTypeFull || b == connectorstore.SyncTypeFull:
		return connectorstore.SyncTypeFull
	case a == connectorstore.SyncTypeResourcesOnly || b == connectorstore.SyncTypeResourcesOnly:
		return connectorstore.SyncTypeResourcesOnly
	default:
		return connectorstore.SyncTypePartial
	}
}

func (c *C1FileAttached) UpdateSync(ctx context.Context, baseSync *reader_v2.SyncRun, appliedSync *reader_v2.SyncRun) error {
	if !c.safe {
		return errors.New("database has been detached")
	}
	syncType := unionSyncTypes(connectorstore.SyncType(baseSync.GetSyncType()), connectorstore.SyncType(appliedSync.GetSyncType()))

	latestEndedAt := baseSync.GetEndedAt().AsTime()
	if appliedSync.GetEndedAt().AsTime().After(latestEndedAt) {
		latestEndedAt = appliedSync.GetEndedAt().AsTime()
	}

	baseSyncID := baseSync.GetId()
	q := c.file.db.Update(fmt.Sprintf("main.%s", syncRuns.Name()))
	q = q.Set(goqu.Record{
		"ended_at":  latestEndedAt.Format("2006-01-02 15:04:05.999999999"),
		"sync_type": string(syncType),
	})
	q = q.Where(goqu.C("sync_id").Eq(baseSyncID))

	query, args, err := q.ToSQL()
	if err != nil {
		return fmt.Errorf("failed to build update sync query: %w", err)
	}

	_, err = c.file.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to update sync %s to type %s: %w", baseSyncID, syncType, err)
	}

	return nil
}

// GenerateSyncDiffFromFile compares the base sync (in main) with the applied sync (in attached)
// and generates two new syncs in the main database:
// - partial_upserts: items in attached that are new or modified compared to main
// - partial_deletions: items in main that don't exist in attached
// Returns (upsertsSyncID, deletionsSyncID, error)
func (c *C1FileAttached) GenerateSyncDiffFromFile(ctx context.Context, baseSyncID string, appliedSyncID string) (string, string, error) {
	if !c.safe {
		return "", "", errors.New("database has been detached")
	}

	ctx, span := tracer.Start(ctx, "C1FileAttached.GenerateSyncDiffFromFile")
	defer span.End()

	// Generate unique IDs for the diff syncs
	deletionsSyncID := ksuid.New().String()
	upsertsSyncID := ksuid.New().String()

	// Create the deletions sync first (so upserts is "latest")
	if err := c.file.insertSyncRun(ctx, deletionsSyncID, connectorstore.SyncTypePartialDeletions, baseSyncID); err != nil {
		return "", "", fmt.Errorf("failed to create deletions sync: %w", err)
	}

	// Create the upserts sync
	if err := c.file.insertSyncRun(ctx, upsertsSyncID, connectorstore.SyncTypePartialUpserts, baseSyncID); err != nil {
		return "", "", fmt.Errorf("failed to create upserts sync: %w", err)
	}

	// Process each table
	tables := []string{"v1_resource_types", "v1_resources", "v1_entitlements", "v1_grants"}
	for _, tableName := range tables {
		if err := c.diffTableUpserts(ctx, tableName, baseSyncID, appliedSyncID, upsertsSyncID); err != nil {
			return "", "", fmt.Errorf("failed to generate upserts for %s: %w", tableName, err)
		}
		if err := c.diffTableDeletions(ctx, tableName, baseSyncID, appliedSyncID, deletionsSyncID); err != nil {
			return "", "", fmt.Errorf("failed to generate deletions for %s: %w", tableName, err)
		}
	}

	// End the syncs (deletions first, then upserts)
	if err := c.file.endSyncRun(ctx, deletionsSyncID); err != nil {
		return "", "", fmt.Errorf("failed to end deletions sync: %w", err)
	}
	if err := c.file.endSyncRun(ctx, upsertsSyncID); err != nil {
		return "", "", fmt.Errorf("failed to end upserts sync: %w", err)
	}

	return upsertsSyncID, deletionsSyncID, nil
}

// diffTableUpserts finds items in attached that are new or modified compared to main.
func (c *C1FileAttached) diffTableUpserts(ctx context.Context, tableName string, baseSyncID string, appliedSyncID string, upsertsSyncID string) error {
	columns, err := c.getTableColumns(ctx, tableName)
	if err != nil {
		return err
	}

	// Build column lists
	columnList := ""
	selectList := ""
	for i, col := range columns {
		if i > 0 {
			columnList += ", "
			selectList += ", "
		}
		columnList += col
		if col == "sync_id" {
			selectList += "? as sync_id"
		} else {
			selectList += col
		}
	}

	// Insert items from attached that are:
	// 1. Not in main (additions)
	// 2. In main but with different data (modifications)
	query := fmt.Sprintf(`
		INSERT INTO main.%s (%s)
		SELECT %s
		FROM attached.%s AS a
		WHERE a.sync_id = ?
		  AND (
		    NOT EXISTS (
		      SELECT 1 FROM main.%s AS m 
		      WHERE m.external_id = a.external_id AND m.sync_id = ?
		    )
		    OR EXISTS (
		      SELECT 1 FROM main.%s AS m 
		      WHERE m.external_id = a.external_id 
		        AND m.sync_id = ?
		        AND m.data != a.data
		    )
		  )
	`, tableName, columnList, selectList, tableName, tableName, tableName)

	_, err = c.file.db.ExecContext(ctx, query, upsertsSyncID, appliedSyncID, baseSyncID, baseSyncID)
	return err
}

// diffTableDeletions finds items in main that don't exist in attached.
func (c *C1FileAttached) diffTableDeletions(ctx context.Context, tableName string, baseSyncID string, appliedSyncID string, deletionsSyncID string) error {
	columns, err := c.getTableColumns(ctx, tableName)
	if err != nil {
		return err
	}

	// Build column lists
	columnList := ""
	selectList := ""
	for i, col := range columns {
		if i > 0 {
			columnList += ", "
			selectList += ", "
		}
		columnList += col
		if col == "sync_id" {
			selectList += "? as sync_id"
		} else {
			selectList += col
		}
	}

	// Insert items from main that don't exist in attached (deletions)
	query := fmt.Sprintf(`
		INSERT INTO main.%s (%s)
		SELECT %s
		FROM main.%s AS m
		WHERE m.sync_id = ?
		  AND NOT EXISTS (
		    SELECT 1 FROM attached.%s AS a 
		    WHERE a.external_id = m.external_id AND a.sync_id = ?
		  )
	`, tableName, columnList, selectList, tableName, tableName)

	_, err = c.file.db.ExecContext(ctx, query, deletionsSyncID, baseSyncID, appliedSyncID)
	return err
}
