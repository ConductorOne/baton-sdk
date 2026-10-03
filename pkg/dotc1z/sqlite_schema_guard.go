package dotc1z

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// validateSQLiteSchema inspects ONLY catalog metadata (sqlite_schema rows
// and PRAGMA table_list/table_xinfo) in the named catalog. It executes no
// file-authored SQL, reads no application rows, drops nothing, and repairs
// nothing; hostile or unsupported schema returns an error. sqlQuerier is
// the existing interface from c1file_attached.go
// (*sql.DB, *sql.Tx, *sql.Conn all satisfy it).
//
// Policy: unsupported-executable-schema rejection. ANY persistent trigger
// or view anywhere in the catalog is rejected — even on non-SDK tables,
// even ones that would not fire in today's migrations — because the SDK
// cannot enumerate every future statement that might touch an
// attacker-chosen object. This is not exploit-shape matching.
//
// This is NOT a CHECK/default/foreign-key/expression-index allowlist and
// not a resource sandbox: ordinary historical tables and indexes are
// accepted without exact-DDL comparison, and a concurrent out-of-band
// modification of a raw database file after validation is outside this
// guard's guarantee.
//
// Concurrency discipline: every sql.Rows is fully drained and closed
// BEFORE the next query is issued — the pools here are capped at one
// connection (c1file.go, copy_isolate_sync.go), so nesting rows would
// deadlock.
func validateSQLiteSchema(ctx context.Context, q sqlQuerier, schemaName string) error {
	// 1. Reject any persistent trigger or view anywhere in the catalog.
	// sqlite_schema is the canonical catalog name (sqlite_master is the
	// legacy alias); both refer to the same rows.
	rows, err := q.QueryContext(ctx, fmt.Sprintf(
		"SELECT type, name, tbl_name FROM %s.sqlite_schema", quoteIdentifier(schemaName)))
	if err != nil {
		return fmt.Errorf("sqlite schema guard: error querying %s catalog: %w", schemaName, err)
	}
	var sdkTables []string
	for rows.Next() {
		var typ, name, tblName string
		if err := rows.Scan(&typ, &name, &tblName); err != nil {
			_ = rows.Close()
			return fmt.Errorf("sqlite schema guard: error scanning %s catalog row: %w", schemaName, err)
		}
		switch typ {
		case "trigger":
			_ = rows.Close()
			return fmt.Errorf(
				"sqlite schema guard: rejected %s trigger %q on table %q in catalog %q: unsupported executable schema (file-authored SQL is never executed)",
				schemaName, name, tblName, schemaName)
		case "view":
			_ = rows.Close()
			return fmt.Errorf(
				"sqlite schema guard: rejected %s view %q in catalog %q: unsupported executable schema",
				schemaName, name, schemaName)
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("sqlite schema guard: error iterating %s catalog: %w", schemaName, err)
	}
	_ = rows.Close()

	// 2. Every PRESENT descriptor-owned table must be an ordinary table.
	// Missing SDK tables are allowed (fresh files, additive legacy).
	for _, t := range allTableDescriptors {
		sdkTables = append(sdkTables, t.Name())
	}
	tlRows, err := q.QueryContext(ctx, fmt.Sprintf(
		"PRAGMA %s.table_list", quoteIdentifier(schemaName)))
	if err != nil {
		return fmt.Errorf("sqlite schema guard: error listing %s tables: %w", schemaName, err)
	}
	present := make(map[string]string) // lower(table name) -> table_list type
	for tlRows.Next() {
		var schema, name, typ string
		var ncol, wr, r int
		if err := tlRows.Scan(&schema, &name, &typ, &ncol, &wr, &r); err != nil {
			_ = tlRows.Close()
			return fmt.Errorf("sqlite schema guard: error scanning %s table_list row: %w", schemaName, err)
		}
		// Only rows belonging to the target catalog count.
		if !strings.EqualFold(schema, schemaName) && (schemaName != "main" || schema != "") {
			_ = tlRows.Close()
			return fmt.Errorf("sqlite schema guard: unexpected table_list schema %q in catalog %q", schema, schemaName)
		}
		present[strings.ToLower(name)] = typ
	}
	if err := tlRows.Err(); err != nil {
		_ = tlRows.Close()
		return fmt.Errorf("sqlite schema guard: error iterating %s table_list: %w", schemaName, err)
	}
	_ = tlRows.Close()

	for _, name := range sdkTables {
		typ, ok := present[strings.ToLower(name)]
		if !ok {
			continue // missing SDK table: allowed (fresh/additive legacy)
		}
		if typ != "table" {
			return fmt.Errorf(
				"sqlite schema guard: rejected %s.%s: descriptor-owned name has type %q (want ordinary table); unsupported executable schema",
				schemaName, name, typ)
		}
	}

	// 3. No hidden/virtual/generated columns on present SDK tables.
	// table_xinfo reports hidden != 0 for virtual-table shadow columns
	// (1) and generated columns (2/3); table_info omits generated columns
	// and so is insufficient.
	for _, name := range sdkTables {
		if _, ok := present[strings.ToLower(name)]; !ok {
			continue
		}
		xiRows, err := q.QueryContext(ctx, fmt.Sprintf(
			"PRAGMA %s.table_xinfo(%s)", quoteIdentifier(schemaName), quoteIdentifier(name)))
		if err != nil {
			return fmt.Errorf("sqlite schema guard: error reading %s.%s columns: %w", schemaName, name, err)
		}
		for xiRows.Next() {
			var cid int
			var colName, colType string
			var notNull int
			var dfltValue sql.NullString
			var pk int
			var hidden int
			if err := xiRows.Scan(&cid, &colName, &colType, &notNull, &dfltValue, &pk, &hidden); err != nil {
				_ = xiRows.Close()
				return fmt.Errorf("sqlite schema guard: error scanning %s.%s column row: %w", schemaName, name, err)
			}
			if err := validateColumnName(colName); err != nil {
				_ = xiRows.Close()
				return fmt.Errorf(
					"sqlite schema guard: rejected %s.%s column %q: %w", schemaName, name, colName, err)
			}
			if hidden != 0 {
				_ = xiRows.Close()
				return fmt.Errorf(
					"sqlite schema guard: rejected %s.%s column %q: hidden=%d (virtual-table shadow or generated column); unsupported schema",
					schemaName, name, colName, hidden)
			}
		}
		if err := xiRows.Err(); err != nil {
			_ = xiRows.Close()
			return fmt.Errorf("sqlite schema guard: error iterating %s.%s columns: %w", schemaName, name, err)
		}
		_ = xiRows.Close()
	}

	return nil
}
