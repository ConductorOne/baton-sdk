package dotc1z

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
)

// schemaRejected reports whether err is a validateSQLiteSchema rejection
// rather than a query failure.
func schemaRejected(err error) bool {
	return err != nil && strings.Contains(err.Error(), "sqlite schema guard: rejected")
}

// TestSecurity_SchemaGuardRejectionAndAcceptance runs the schema guard's
// rejection matrix (triggers anywhere — SDK or non-SDK tables, views,
// SDK-name views, generated columns, hostile column names) and its
// acceptance controls (ordinary tables/indexes, sqlite_stat1, unrelated
// tables) directly against validateSQLiteSchema. Route-level coverage
// (open/isolate/attach/conversion) lives in hostile_trigger_security_test.go
// and hostile_boundary_security_test.go.
func TestSecurity_SchemaGuardRejectionAndAcceptance(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	mk := func(file string, ddl ...string) *sql.DB {
		t.Helper()
		path := filepath.Join(dir, file)
		db, err := openSQLite(ctx, path)
		if err != nil {
			t.Fatalf("open %s: %v", file, err)
		}
		t.Cleanup(func() { _ = db.Close() })
		for _, q := range ddl {
			if _, err := db.ExecContext(ctx, q); err != nil {
				t.Fatalf("ddl %s: %v", q, err)
			}
		}
		return db
	}

	// ordinary schema: accepted (incl. unrelated table + index)
	if err := validateSQLiteSchema(ctx, mk("ok.db",
		`CREATE TABLE v1_sync_runs (id integer primary key, sync_id text)`,
		`CREATE TABLE unrelated (x text)`,
		`CREATE INDEX idx_unrelated ON unrelated (x)`,
	), "main"); err != nil {
		t.Fatalf("ordinary schema rejected: %v", err)
	}

	// trigger on SDK table: rejected
	err := validateSQLiteSchema(ctx, mk("trig.db",
		`CREATE TABLE v1_sync_runs (id integer primary key, sync_id text)`,
		`CREATE TABLE v1_grants (id integer primary key)`,
		`CREATE TRIGGER file_authored_update BEFORE UPDATE ON v1_sync_runs BEGIN SELECT RAISE(ABORT, 'file-authored-trigger-executed'); END`,
	), "main")
	if !schemaRejected(err) {
		t.Fatalf("trigger not rejected: %v", err)
	}
	t.Logf("trigger rejection: %v", err)

	// trigger on NON-SDK table: rejected too
	err = validateSQLiteSchema(ctx, mk("trig2.db",
		`CREATE TABLE other (x text)`,
		`CREATE TRIGGER t2 BEFORE INSERT ON other BEGIN SELECT RAISE(ABORT, 'x'); END`,
	), "main")
	if !schemaRejected(err) {
		t.Fatalf("non-sdk trigger not rejected: %v", err)
	}

	// view: rejected
	err = validateSQLiteSchema(ctx, mk("view.db",
		`CREATE TABLE t (x text)`,
		`CREATE VIEW v AS SELECT * FROM t`,
	), "main")
	if !schemaRejected(err) {
		t.Fatalf("view not rejected: %v", err)
	}

	// SDK table implemented as view: rejected
	err = validateSQLiteSchema(ctx, mk("sdkview.db",
		`CREATE TABLE backing (id integer primary key)`,
		`CREATE VIEW v1_grants AS SELECT * FROM backing`,
	), "main")
	if !schemaRejected(err) {
		t.Fatalf("SDK-name view not rejected: %v", err)
	}

	// generated column on SDK table: rejected
	err = validateSQLiteSchema(ctx, mk("gen.db",
		`CREATE TABLE v1_grants (id integer primary key, sync_id text, gen text GENERATED ALWAYS AS ('x'))`,
	), "main")
	if !schemaRejected(err) {
		t.Fatalf("generated column not rejected: %v", err)
	}

	// hostile column name: rejected
	err = validateSQLiteSchema(ctx, mk("col.db",
		`CREATE TABLE v1_grants ("id""; ATTACH x AS y" text)`,
	), "main")
	if !schemaRejected(err) {
		t.Fatalf("hostile column name not rejected: %v", err)
	}

	// sqlite_stat1 (ANALYZE): accepted as ordinary table
	if err := validateSQLiteSchema(ctx, mk("stat.db",
		`CREATE TABLE t (x text)`,
		`CREATE INDEX idx_t ON t (x)`,
		`ANALYZE`,
	), "main"); err != nil {
		t.Fatalf("stat1 rejected: %v", err)
	}
}
