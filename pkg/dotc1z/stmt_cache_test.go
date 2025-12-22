package dotc1z

import (
	"context"
	"database/sql"
	"testing"
)

func TestNormalizeQuery(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple query",
			input:    "SELECT * FROM table",
			expected: "SELECT * FROM table",
		},
		{
			name:     "query with extra spaces",
			input:    "SELECT   *   FROM    table",
			expected: "SELECT * FROM table",
		},
		{
			name:     "query with newlines",
			input:    "SELECT\n*\nFROM\ntable",
			expected: "SELECT * FROM table",
		},
		{
			name:     "query with tabs",
			input:    "SELECT\t*\tFROM\ttable",
			expected: "SELECT * FROM table",
		},
		{
			name:     "query with mixed whitespace",
			input:    "SELECT  \n\t *  \n\t FROM  \n\t table",
			expected: "SELECT * FROM table",
		},
		{
			name:     "query with leading/trailing whitespace",
			input:    "  SELECT * FROM table  ",
			expected: "SELECT * FROM table",
		},
		{
			name:     "query with parameters",
			input:    "SELECT * FROM table WHERE id = ? AND name = ?",
			expected: "SELECT * FROM table WHERE id = ? AND name = ?",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := normalizeQuery(tt.input)
			if result != tt.expected {
				t.Errorf("normalizeQuery() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestGetPreparedStmt_CacheHit(t *testing.T) {
	ctx := context.Background()
	db, cleanup := setupTestDB(t)
	defer cleanup()

	query := "SELECT 1"

	// First call - should be a cache miss
	stmt1, err := GetPreparedStmt(ctx, db, query)
	if err != nil {
		t.Fatalf("GetPreparedStmt() error = %v", err)
	}
	defer stmt1.Close()

	// Second call - should be a cache hit
	stmt2, err := GetPreparedStmt(ctx, db, query)
	if err != nil {
		t.Fatalf("GetPreparedStmt() error = %v", err)
	}
	defer stmt2.Close()

	// Should return the same statement
	if stmt1 != stmt2 {
		t.Error("GetPreparedStmt() should return cached statement on second call")
	}

	// Verify cache stats
	stats := GetCacheStats(db)
	if stats == nil {
		t.Fatal("GetCacheStats() returned nil")
	}
	if stats.Hits < 1 {
		t.Errorf("Expected at least 1 cache hit, got %d", stats.Hits)
	}
	if stats.Misses < 1 {
		t.Errorf("Expected at least 1 cache miss, got %d", stats.Misses)
	}
	if stats.Prepares != 1 {
		t.Errorf("Expected 1 prepare, got %d", stats.Prepares)
	}
	if stats.CacheSize != 1 {
		t.Errorf("Expected cache size 1, got %d", stats.CacheSize)
	}
}

func TestGetPreparedStmt_CacheHitRate(t *testing.T) {
	ctx := context.Background()
	db, cleanup := setupTestDB(t)
	defer cleanup()

	query := "SELECT 1"

	// Call 10 times - first should miss, rest should hit
	for i := 0; i < 10; i++ {
		stmt, err := GetPreparedStmt(ctx, db, query)
		if err != nil {
			t.Fatalf("GetPreparedStmt() error = %v", err)
		}
		stmt.Close()
	}

	stats := GetCacheStats(db)
	if stats == nil {
		t.Fatal("GetCacheStats() returned nil")
	}

	expectedHits := int64(9) // 9 hits after first miss
	if stats.Hits != expectedHits {
		t.Errorf("Expected %d hits, got %d", expectedHits, stats.Hits)
	}
	if stats.Misses != 1 {
		t.Errorf("Expected 1 miss, got %d", stats.Misses)
	}

	hitRate := stats.HitRate()
	expectedHitRate := 90.0 // 9 hits out of 10 calls
	if hitRate < expectedHitRate-1.0 || hitRate > expectedHitRate+1.0 {
		t.Errorf("Expected hit rate ~%.1f%%, got %.2f%%", expectedHitRate, hitRate)
	}
}

func TestGetPreparedStmt_DifferentQueries(t *testing.T) {
	ctx := context.Background()
	db, cleanup := setupTestDB(t)
	defer cleanup()

	queries := []string{
		"SELECT 1",
		"SELECT 2",
		"SELECT * FROM table WHERE id = ?",
	}

	// Prepare all queries
	stmts := make([]*sql.Stmt, len(queries))
	for i, query := range queries {
		stmt, err := GetPreparedStmt(ctx, db, query)
		if err != nil {
			t.Fatalf("GetPreparedStmt() error for query %d: %v", i, err)
		}
		stmts[i] = stmt
		defer stmt.Close()
	}

	// Verify all are cached
	stats := GetCacheStats(db)
	if stats == nil {
		t.Fatal("GetCacheStats() returned nil")
	}
	if stats.CacheSize != len(queries) {
		t.Errorf("Expected cache size %d, got %d", len(queries), stats.CacheSize)
	}
}

func TestGetPreparedStmt_NilDB(t *testing.T) {
	ctx := context.Background()

	_, err := GetPreparedStmt(ctx, nil, "SELECT 1")
	if err == nil {
		t.Error("GetPreparedStmt() with nil DB should return error")
	}
}

func TestGetPreparedStmt_InvalidQuery(t *testing.T) {
	ctx := context.Background()
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// SQLite doesn't validate queries during Prepare, only on execution
	// So we prepare and then try to execute to trigger the error
	stmt, err := GetPreparedStmt(ctx, db, "INVALID SQL QUERY!!!")
	if err != nil {
		// If prepare fails, that's fine - verify error is recorded
		stats := GetCacheStats(db)
		if stats != nil && stats.Errors < 1 {
			t.Error("Expected error to be recorded in stats")
		}
		return
	}

	// If prepare succeeds, execution should fail
	_, execErr := stmt.ExecContext(ctx)
	if execErr == nil {
		t.Error("ExecContext() with invalid query should return error")
	}
	stmt.Close()
}

func TestGetPreparedStmt_ConcurrentAccess(t *testing.T) {
	ctx := context.Background()
	db, cleanup := setupTestDB(t)
	defer cleanup()

	query := "SELECT 1"

	// Concurrently prepare the same query
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func() {
			stmt, err := GetPreparedStmt(ctx, db, query)
			if err != nil {
				t.Errorf("GetPreparedStmt() error = %v", err)
				done <- false
				return
			}
			stmt.Close()
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		if !<-done {
			t.Fatal("One or more goroutines failed")
		}
	}

	// Should only have prepared once (race condition handled by double-check)
	stats := GetCacheStats(db)
	if stats == nil {
		t.Fatal("GetCacheStats() returned nil")
	}
	// Should have exactly 1 prepare (others should hit cache)
	if stats.Prepares > 1 {
		t.Errorf("Expected at most 1 prepare with concurrent access, got %d", stats.Prepares)
	}
}

func TestQueryRowContextWithCache(t *testing.T) {
	ctx := context.Background()
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create a test table
	_, err := db.ExecContext(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	_, err = db.ExecContext(ctx, "INSERT INTO test (value) VALUES ('test')")
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	query := "SELECT value FROM test WHERE id = ?"

	// First call - cache miss
	var value1 string
	row1 := QueryRowContextWithCache(ctx, db, query, 1)
	if err := row1.Scan(&value1); err != nil {
		t.Fatalf("QueryRowContextWithCache() scan error = %v", err)
	}

	// Second call - cache hit
	var value2 string
	row2 := QueryRowContextWithCache(ctx, db, query, 1)
	if err := row2.Scan(&value2); err != nil {
		t.Fatalf("QueryRowContextWithCache() scan error = %v", err)
	}

	if value1 != value2 || value1 != "test" {
		t.Errorf("Expected 'test', got value1=%q, value2=%q", value1, value2)
	}

	// Verify cache was used
	stats := GetCacheStats(db)
	if stats == nil {
		t.Fatal("GetCacheStats() returned nil")
	}
	if stats.Hits < 1 {
		t.Error("Expected at least 1 cache hit")
	}
}

func TestQueryContextWithCache(t *testing.T) {
	ctx := context.Background()
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create a test table
	_, err := db.ExecContext(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	_, err = db.ExecContext(ctx, "INSERT INTO test (value) VALUES ('test1'), ('test2')")
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	query := "SELECT value FROM test ORDER BY id"

	// First call - cache miss
	rows1, err := QueryContextWithCache(ctx, db, query)
	if err != nil {
		t.Fatalf("QueryContextWithCache() error = %v", err)
	}
	defer rows1.Close()

	count1 := 0
	for rows1.Next() {
		count1++
	}

	// Second call - cache hit
	rows2, err := QueryContextWithCache(ctx, db, query)
	if err != nil {
		t.Fatalf("QueryContextWithCache() error = %v", err)
	}
	defer rows2.Close()

	count2 := 0
	for rows2.Next() {
		count2++
	}

	if count1 != count2 || count1 != 2 {
		t.Errorf("Expected 2 rows, got count1=%d, count2=%d", count1, count2)
	}

	// Verify cache was used
	stats := GetCacheStats(db)
	if stats != nil && stats.Hits < 1 {
		t.Error("Expected at least 1 cache hit")
	}
}

func TestClosePreparedStatements(t *testing.T) {
	ctx := context.Background()
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Prepare some statements
	queries := []string{"SELECT 1", "SELECT 2", "SELECT 3"}
	for _, query := range queries {
		stmt, err := GetPreparedStmt(ctx, db, query)
		if err != nil {
			t.Fatalf("GetPreparedStmt() error = %v", err)
		}
		stmt.Close()
	}

	// Verify they're cached
	stats := GetCacheStats(db)
	if stats == nil || stats.CacheSize != len(queries) {
		t.Fatalf("Expected %d cached statements, got %d", len(queries), stats.CacheSize)
	}

	// Close all statements
	if err := ClosePreparedStatements(db); err != nil {
		t.Fatalf("ClosePreparedStatements() error = %v", err)
	}

	// Verify cache is cleared
	stats = GetCacheStats(db)
	if stats != nil {
		t.Error("GetCacheStats() should return nil after ClosePreparedStatements()")
	}
}

func TestGetCacheStats_NilDB(t *testing.T) {
	stats := GetCacheStats(nil)
	if stats != nil {
		t.Error("GetCacheStats() with nil DB should return nil")
	}
}

func TestGetCacheStats_NoCache(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Get stats before any queries - cache doesn't exist yet, so should return nil
	stats := GetCacheStats(db)
	if stats != nil {
		t.Error("GetCacheStats() should return nil when no cache exists yet")
	}

	// Create a cache entry by preparing a statement
	_, err := GetPreparedStmt(context.Background(), db, "SELECT 1")
	if err != nil {
		t.Fatalf("GetPreparedStmt() failed: %v", err)
	}

	// Now stats should be available
	stats = GetCacheStats(db)
	if stats == nil {
		t.Fatal("GetCacheStats() should return stats after cache is created")
	}
	if stats.CacheSize != 1 {
		t.Errorf("Expected cache size 1, got %d", stats.CacheSize)
	}
}

func TestCacheStats_HitRate(t *testing.T) {
	stats := &CacheStatsSnapshot{
		Hits:   90,
		Misses: 10,
	}

	hitRate := stats.HitRate()
	expected := 90.0
	if hitRate != expected {
		t.Errorf("HitRate() = %.2f%%, want %.2f%%", hitRate, expected)
	}

	// Test zero case
	stats2 := &CacheStatsSnapshot{
		Hits:   0,
		Misses: 0,
	}
	hitRate2 := stats2.HitRate()
	if hitRate2 != 0.0 {
		t.Errorf("HitRate() with no calls = %.2f%%, want 0.0%%", hitRate2)
	}
}

// setupTestDB creates a temporary in-memory SQLite database for testing
func setupTestDB(t *testing.T) (*sql.DB, func()) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open test database: %v", err)
	}

	cleanup := func() {
		ClosePreparedStatements(db)
		db.Close()
	}

	return db, cleanup
}

