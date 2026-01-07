package dotc1z

import (
	"context"
	"fmt"
	"strings"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/doug-martin/goqu/v9"
	"github.com/segmentio/ksuid"
)

// GenerateSyncDiff compares two syncs and generates paired diff syncs:
// - upsertsSyncID: a partial sync containing items added or modified in appliedSyncID
// - deletionsSyncID: a deletions sync containing items that exist in baseSyncID but not in appliedSyncID
// Both syncs are bidirectionally linked via linked_sync_id.
func (c *C1File) GenerateSyncDiff(ctx context.Context, baseSyncID string, appliedSyncID string) (upsertsSyncID string, deletionsSyncID string, err error) {
	if c.readOnly {
		return "", "", ErrReadOnly
	}

	// Validate that both sync runs exist
	baseSync, err := c.getSync(ctx, baseSyncID)
	if err != nil {
		return "", "", err
	}
	if baseSync == nil {
		return "", "", fmt.Errorf("generate-diff: base sync not found")
	}

	newSync, err := c.getSync(ctx, appliedSyncID)
	if err != nil {
		return "", "", err
	}
	if newSync == nil {
		return "", "", fmt.Errorf("generate-diff: new sync not found")
	}

	// Generate unique IDs for both diff syncs
	upsertsSyncID = ksuid.New().String()
	deletionsSyncID = ksuid.New().String()

	// Create upserts sync (partial_upserts type) - will link to deletions sync
	if err := c.insertSyncRunWithLink(ctx, upsertsSyncID, connectorstore.SyncTypePartialUpserts, baseSyncID, deletionsSyncID); err != nil {
		return "", "", err
	}

	// Create deletions sync (partial_deletions type) - linked to upserts sync
	if err := c.insertSyncRunWithLink(ctx, deletionsSyncID, connectorstore.SyncTypePartialDeletions, baseSyncID, upsertsSyncID); err != nil {
		return "", "", err
	}

	// Generate diff data for each table
	for _, t := range allTableDescriptors {
		if strings.Contains(t.Name(), syncRunsTableName) {
			continue
		}
		if strings.Contains(t.Name(), sessionStoreTableName) {
			continue
		}

		// Generate upserts (additions + modifications)
		upsertsQuery, upsertsArgs, err := c.diffTableUpsertsQuery(t, baseSyncID, appliedSyncID, upsertsSyncID)
		if err != nil {
			return "", "", err
		}
		if upsertsQuery != "" {
			_, err = c.db.ExecContext(ctx, upsertsQuery, upsertsArgs...)
			if err != nil {
				return "", "", fmt.Errorf("error executing upserts query for table %s: %w", t.Name(), err)
			}
			c.dbUpdated = true
		}

		// Generate deletions
		deletionsQuery, deletionsArgs, err := c.diffTableDeletionsQuery(t, baseSyncID, appliedSyncID, deletionsSyncID)
		if err != nil {
			return "", "", err
		}
		if deletionsQuery != "" {
			_, err = c.db.ExecContext(ctx, deletionsQuery, deletionsArgs...)
			if err != nil {
				return "", "", fmt.Errorf("error executing deletions query for table %s: %w", t.Name(), err)
			}
			c.dbUpdated = true
		}
	}

	// End both sync runs
	if err := c.endSyncRun(ctx, upsertsSyncID); err != nil {
		return "", "", err
	}
	if err := c.endSyncRun(ctx, deletionsSyncID); err != nil {
		return "", "", err
	}

	return upsertsSyncID, deletionsSyncID, nil
}

// getTableColumns returns the columns for a given table used in diff queries.
func (c *C1File) getTableColumns(tableName string) []interface{} {
	columns := []interface{}{
		"external_id",
		"data",
		"sync_id",
		"discovered_at",
	}

	switch {
	case strings.Contains(tableName, resourcesTableName):
		columns = append(columns, "resource_type_id", "parent_resource_type_id", "parent_resource_id")
	case strings.Contains(tableName, resourceTypesTableName):
		// Nothing new to add here
	case strings.Contains(tableName, grantsTableName):
		columns = append(columns, "resource_type_id", "resource_id", "entitlement_id", "principal_resource_type_id", "principal_resource_id")
	case strings.Contains(tableName, entitlementsTableName):
		columns = append(columns, "resource_type_id", "resource_id")
	case strings.Contains(tableName, assetsTableName):
		columns = append(columns, "content_type")
	}

	return columns
}

// diffTableUpsertsQuery generates a query to find items that are:
// - In appliedSyncID but not in baseSyncID (additions)
// - In both syncs but with different data (modifications)

func (c *C1File) diffTableUpsertsQuery(table tableDescriptor, baseSyncID, appliedSyncID, newSyncID string) (string, []any, error) {
	tableName := table.Name()
	columns := c.getTableColumns(tableName)

	// Build query columns, replacing sync_id with the new sync ID
	queryColumns := []interface{}{}
	for _, col := range columns {
		if col == "sync_id" {
			queryColumns = append(queryColumns, goqu.L(fmt.Sprintf("'%s' as sync_id", newSyncID)))
			continue
		}
		queryColumns = append(queryColumns, col)
	}

	// Subquery to find external_ids in the base sync
	baseExternalIDs := c.db.Select("external_id").
		From(tableName).
		Where(goqu.C("sync_id").Eq(baseSyncID))

	// EXISTS subquery to check if data differs between syncs for items with same external_id
	// This detects modifications
	existsSQL := fmt.Sprintf(
		// TODO(kans): encode a version stored at row level for proper comparisons. This is a POC solution.
		"EXISTS (SELECT 1 FROM %s AS base WHERE base.sync_id = ? AND base.external_id = %s.external_id AND base.data != %s.data)",
		tableName, tableName, tableName,
	)

	// Select items from applied sync that are either:
	// 1. Not in base sync (additions)
	// 2. In base sync but with different data (modifications)
	selectQuery := c.db.Select(queryColumns...).
		From(tableName).
		Where(
			goqu.C("sync_id").Eq(appliedSyncID),
			goqu.Or(
				goqu.C("external_id").NotIn(baseExternalIDs),
				goqu.L(existsSQL, baseSyncID),
			),
		)

	query := c.db.Insert(tableName).
		Cols(columns...).
		Prepared(true).
		FromQuery(selectQuery)

	return query.ToSQL()
}

// diffTableDeletionsQuery generates a query to find items that are:
// - In baseSyncID but not in appliedSyncID (deletions)
func (c *C1File) diffTableDeletionsQuery(table tableDescriptor, baseSyncID, appliedSyncID, newSyncID string) (string, []any, error) {
	tableName := table.Name()
	columns := c.getTableColumns(tableName)

	// Build query columns, replacing sync_id with the new sync ID
	queryColumns := []interface{}{}
	for _, col := range columns {
		if col == "sync_id" {
			queryColumns = append(queryColumns, goqu.L(fmt.Sprintf("'%s' as sync_id", newSyncID)))
			continue
		}
		queryColumns = append(queryColumns, col)
	}

	// Subquery to find external_ids in the applied sync
	appliedExternalIDs := c.db.Select("external_id").
		From(tableName).
		Where(goqu.C("sync_id").Eq(appliedSyncID))

	// Select items from base sync that are not in applied sync (deletions)
	selectQuery := c.db.Select(queryColumns...).
		From(tableName).
		Where(
			goqu.C("sync_id").Eq(baseSyncID),
			goqu.C("external_id").NotIn(appliedExternalIDs),
		)

	query := c.db.Insert(tableName).
		Cols(columns...).
		Prepared(true).
		FromQuery(selectQuery)

	return query.ToSQL()
}
