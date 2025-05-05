package dotc1z

import (
	"context"
	"fmt"
	"strings"

	"github.com/doug-martin/goqu/v9"
	"github.com/segmentio/ksuid"
)

func (c *C1File) GenerateSyncDiff(ctx context.Context, baseSyncID string, newSyncID string) (string, error) {
	// Validate that both sync runs exist
	baseSync, err := c.getSync(ctx, baseSyncID)
	if err != nil {
		return "", err
	}
	if baseSync == nil {
		return "", fmt.Errorf("generate-diff: base sync not found")
	}

	newSync, err := c.getSync(ctx, newSyncID)
	if err != nil {
		return "", err
	}
	if newSync == nil {
		return "", fmt.Errorf("generate-diff: new sync not found")
	}

	// Create a context for the queries
	qCtx, canc := context.WithCancel(ctx)
	defer canc()

	// Get a single connection to the current db so we can make multiple queries in the same session
	conn, err := c.rawDb.Conn(qCtx)
	if err != nil {
		return "", err
	}
	defer conn.Close()

	// Generate a new unique ID for the diff sync
	diffSyncID := ksuid.New().String()

	// Process each table to select records from newSyncID that don't exist in baseSyncID
	for _, t := range allTableDescriptors {
		q, err := diffTableQuery(t.Name())
		if err != nil {
			return "", err
		}
		// Execute the query with diffSyncID as the new sync_id, newSyncID as the source, and baseSyncID as the reference
		_, err = conn.ExecContext(qCtx, q, diffSyncID, newSyncID, baseSyncID)
		if err != nil {
			return "", err
		}
	}

	// Really be sure that our connection is closed
	canc()
	_ = conn.Close()

	return diffSyncID, nil
}

func diffTableQuery(tableName string) (string, error) {
	// Define the columns to select based on the table name
	columns := []interface{}{
		"id",
		"external_id",
		"data",
		"discovered_at",
		goqu.L("?"), // Placeholder for new sync ID
	}

	// Add table-specific columns
	switch {
	case strings.Contains(tableName, resourcesTableName):
	case strings.Contains(tableName, resourceTypesTableName):
	case strings.Contains(tableName, grantsTableName):
	case strings.Contains(tableName, entitlementsTableName):
	case strings.Contains(tableName, assetsTableName):
	}

	// Build the subquery to find external_ids in the base sync
	subquery := goqu.From(tableName).
		Select("id").
		Where(goqu.C("sync_id").Eq(goqu.L("?"))) // Placeholder for baseSyncID

	// Build the main query to select records from newSyncID that don't exist in baseSyncID
	query := goqu.Insert(tableName).
		Prepared(true).
		FromQuery(
			goqu.From(tableName).
				Select(columns...).
				Where(
					goqu.C("sync_id").Eq(goqu.L("?")), // Placeholder for newSyncID
					goqu.C("id").NotIn(subquery),
				),
		)

	// Generate the SQL and args
	sql, _, err := query.ToSQL()
	return sql, err
}
