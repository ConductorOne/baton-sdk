package dotc1z

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	_ "github.com/glebarez/go-sqlite"
	"github.com/stretchr/testify/require"
)

func TestValidateColumnName(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		// Valid: normal schema column names that must always be accepted.
		{name: "lowercase", input: "sync_id", wantErr: false},
		{name: "leading underscore", input: "_private", wantErr: false},
		{name: "mixed case", input: "resourceTypeId", wantErr: false},

		// Invalid: injection payloads and structural characters that would
		// alter SQL semantics if interpolated into a query unquoted.
		{name: "semicolon injection", input: "evil); DROP TABLE t; --", wantErr: true},
		{name: "double quote", input: `col"name`, wantErr: true},
		{name: "space", input: "col name", wantErr: true},
		{name: "parenthesis", input: "col(1)", wantErr: true},
		{name: "empty string", input: "", wantErr: true},
		{name: "starts with digit", input: "1col", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateColumnName(tt.input)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// openTestDB creates a throwaway SQLite database with a single table whose
// schema includes one extra column with the given name.  The table otherwise
// matches the structure of v1_grants so the column-reading functions treat it
// as a realistic schema.
func openTestDB(t *testing.T, extraColumnName string) *sql.DB {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	// Create a table with normal columns plus one attacker-controlled column.
	// The malicious name must be double-quoted in the DDL so SQLite accepts it
	// as an identifier — this is exactly what an attacker would do when
	// crafting a .c1z file.
	ddl := fmt.Sprintf(`CREATE TABLE v1_grants (
		id INTEGER PRIMARY KEY,
		external_id TEXT NOT NULL,
		data BLOB NOT NULL,
		sync_id TEXT NOT NULL,
		discovered_at DATETIME NOT NULL,
		%s TEXT
	)`, quoteIdentifier(extraColumnName))

	_, err = db.ExecContext(context.Background(), ddl)
	require.NoError(t, err)
	return db
}

// TestCloneTableColumnsRejectsMaliciousName verifies that cloneTableColumns
// (used by CloneSync) refuses to return column names that contain SQL
// metacharacters.  Without validation, such names would be interpolated
// directly into INSERT…SELECT queries, enabling SQL injection via crafted
// .c1z files.
func TestCloneTableColumnsRejectsMaliciousName(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t, "evil); DROP TABLE v1_grants; --")

	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()

	_, err = cloneTableColumns(ctx, conn, "v1_grants")
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid column name")
}

// TestGetTableColumnsRejectsMaliciousName verifies the same invariant for
// getTableColumns (used by CompactTable and the diff functions).  This is a
// separate code path from cloneTableColumns and could regress independently.
func TestGetTableColumnsRejectsMaliciousName(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t, "x' OR '1'='1")

	attached := &C1FileAttached{safe: true, file: &C1File{rawDb: db}}
	_, err := attached.getTableColumns(ctx, db, "v1_grants")
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid column name")
}
