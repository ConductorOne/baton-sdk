package anonymize

import (
	"testing"

	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/stretchr/testify/require"
)

// knownAnonymizedTables lists all tables that are handled by the anonymization processor.
// When adding a new table to the database, you MUST add it here and implement
// the appropriate anonymization logic in processor.go.
// Note: Table names include version prefixes (e.g., "v1_resources").
var knownAnonymizedTables = map[string]string{
	"v1_resource_types":     "processResourceTypes - anonymizes all fields",
	"v1_resources":          "processResources - anonymizes all fields",
	"v1_entitlements":       "processEntitlements - anonymizes all fields",
	"v1_grants":             "processGrants - anonymizes all fields",
	"v1_assets":             "processAssets - deletes all assets",
	"v1_connector_sessions": "processSessions - clears all sessions",
	"v1_sync_runs":          "processSyncRuns - clears timestamps",
}

// TestTableCoverage ensures all database tables are handled by the anonymization processor.
// If this test fails after adding a new table, you MUST:
// 1. Add the table to knownAnonymizedTables with a description of how it's handled.
// 2. Implement the anonymization logic in processor.go.
func TestTableCoverage(t *testing.T) {
	allTables := dotc1z.AllTableNames()
	require.NotEmpty(t, allTables, "Expected AllTableNames() to return table names")

	for _, table := range allTables {
		description, ok := knownAnonymizedTables[table]
		require.True(t, ok,
			"Table %q is not handled by anonymization! "+
				"Add anonymization logic in processor.go and register it in knownAnonymizedTables",
			table)
		require.NotEmpty(t, description,
			"Table %q has empty description in knownAnonymizedTables", table)
	}
}

// TestNoOrphanedTableHandlers ensures we don't have handlers for non-existent tables.
func TestNoOrphanedTableHandlers(t *testing.T) {
	allTables := dotc1z.AllTableNames()
	require.NotEmpty(t, allTables, "Expected AllTableNames() to return table names")

	tableSet := make(map[string]bool)
	for _, table := range allTables {
		tableSet[table] = true
	}

	for table := range knownAnonymizedTables {
		require.True(t, tableSet[table],
			"knownAnonymizedTables references non-existent table %q - remove it", table)
	}
}
