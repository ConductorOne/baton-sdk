package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
)

const (
	syncRunsTable      = "v1_sync_runs"
	resourceTypesTable = "v1_resource_types"
	resourcesTable     = "v1_resources"
	entitlementsTable  = "v1_entitlements"
	grantsTable        = "v1_grants"
	assetsTable        = "v1_assets"
	sessionsTable      = "v1_connector_sessions"
)

var requiredColumns = map[string][]string{
	syncRunsTable:      {"id", "sync_id", "started_at", "ended_at", "sync_token"},
	resourceTypesTable: {"sync_id", "external_id", "data", "discovered_at"},
	resourcesTable:     {"sync_id", "data", "discovered_at"},
	entitlementsTable:  {"sync_id", "external_id", "data", "discovered_at"},
	grantsTable: {
		"sync_id", "external_id", "data", "discovered_at", "id", "entitlement_id", "resource_type_id",
		"resource_id", "principal_resource_type_id", "principal_resource_id",
	},
	assetsTable: {"id", "sync_id", "external_id", "content_type", "data"},
}

var sdkTables = []string{
	syncRunsTable, resourceTypesTable, resourcesTable, entitlementsTable,
	grantsTable, assetsTable, sessionsTable,
}

var validColumnName = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

func validateSQLiteSchema(ctx context.Context, db *sql.DB) (map[string]map[string]bool, map[string]bool, error) {
	rows, err := db.QueryContext(ctx, "SELECT type, name, tbl_name FROM main.sqlite_schema")
	if err != nil {
		return nil, nil, fmt.Errorf("sqlite schema guard: query catalog: %w", err)
	}
	tables := make(map[string]bool)
	for rows.Next() {
		var typ, name, tableName string
		if err := rows.Scan(&typ, &name, &tableName); err != nil {
			_ = rows.Close()
			return nil, nil, fmt.Errorf("sqlite schema guard: scan catalog: %w", err)
		}
		switch typ {
		case "trigger":
			_ = rows.Close()
			return nil, nil, fmt.Errorf("sqlite schema guard: rejected trigger %q on table %q: unsupported executable schema", name, tableName)
		case "view":
			_ = rows.Close()
			return nil, nil, fmt.Errorf("sqlite schema guard: rejected view %q: unsupported executable schema", name)
		case "table":
			tables[strings.ToLower(name)] = true
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, nil, fmt.Errorf("sqlite schema guard: iterate catalog: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, nil, fmt.Errorf("sqlite schema guard: close catalog rows: %w", err)
	}

	tableTypes, err := tableList(ctx, db)
	if err != nil {
		return nil, nil, err
	}
	for _, table := range sdkTables {
		if tables[table] && tableTypes[table] != "table" {
			return nil, nil, fmt.Errorf("sqlite schema guard: rejected table %q with type %q: ordinary table required", table, tableTypes[table])
		}
	}

	columns := make(map[string]map[string]bool)
	for table := range requiredColumns {
		if !tables[table] {
			return nil, nil, fmt.Errorf("sqlite schema guard: required table %q is missing", table)
		}
	}
	for _, table := range sdkTables {
		if !tables[table] {
			continue
		}
		tableColumns, err := tableXInfo(ctx, db, table)
		if err != nil {
			return nil, nil, err
		}
		columns[table] = tableColumns
	}
	for table, required := range requiredColumns {
		for _, column := range required {
			if !columns[table][column] {
				return nil, nil, fmt.Errorf("sqlite schema guard: required column %q is missing from table %q", column, table)
			}
		}
	}
	if tables[sessionsTable] {
		for _, column := range []string{"sync_id", "key", "value"} {
			if !columns[sessionsTable][column] {
				return nil, nil, fmt.Errorf("sqlite schema guard: required column %q is missing from table %q", column, sessionsTable)
			}
		}
	}
	return columns, tables, nil
}

func tableList(ctx context.Context, db *sql.DB) (map[string]string, error) {
	rows, err := db.QueryContext(ctx, "PRAGMA main.table_list")
	if err != nil {
		return nil, fmt.Errorf("sqlite schema guard: list tables: %w", err)
	}
	defer rows.Close()
	tableTypes := make(map[string]string)
	for rows.Next() {
		var schema, name, tableType string
		var columns, withoutRowID, strict int
		if err := rows.Scan(&schema, &name, &tableType, &columns, &withoutRowID, &strict); err != nil {
			return nil, fmt.Errorf("sqlite schema guard: scan table list: %w", err)
		}
		if schema == "main" {
			tableTypes[strings.ToLower(name)] = tableType
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite schema guard: iterate table list: %w", err)
	}
	return tableTypes, nil
}

func tableXInfo(ctx context.Context, db *sql.DB, table string) (map[string]bool, error) {
	rows, err := db.QueryContext(ctx, fmt.Sprintf("PRAGMA main.table_xinfo(%q)", table))
	if err != nil {
		return nil, fmt.Errorf("sqlite schema guard: read %s columns: %w", table, err)
	}
	defer rows.Close()
	columns := make(map[string]bool)
	for rows.Next() {
		var cid, notNull, primaryKey, hidden int
		var name, columnType string
		var defaultValue sql.NullString
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey, &hidden); err != nil {
			return nil, fmt.Errorf("sqlite schema guard: scan %s column: %w", table, err)
		}
		if !validColumnName.MatchString(name) {
			return nil, fmt.Errorf("sqlite schema guard: rejected %s column %q: invalid name", table, name)
		}
		if hidden != 0 {
			return nil, fmt.Errorf("sqlite schema guard: rejected %s column %q: hidden=%d", table, name, hidden)
		}
		columns[strings.ToLower(name)] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite schema guard: iterate %s columns: %w", table, err)
	}
	return columns, nil
}
