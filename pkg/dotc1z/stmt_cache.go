package dotc1z

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"unsafe"
)

// queryStatsEnabled controls whether detailed per-query statistics are tracked.
// This is controlled by the BATON_ENABLE_QUERY_STATS environment variable.
// Defaults to false (disabled) to avoid overhead in production.
var queryStatsEnabled = func() bool {
	val := os.Getenv("BATON_ENABLE_QUERY_STATS")
	if val == "" {
		return false // Default: disabled
	}
	enabled, _ := strconv.ParseBool(val)
	return enabled
}()

// stmtCacheEntry holds a prepared statement and metadata for a single query pattern.
type stmtCacheEntry struct {
	stmt      *sql.Stmt
	query     string // normalized query string
	execCount int64  // number of times this query was executed
	hitCount  int64  // number of cache hits for this query
	// Future: could add lastUsed time for LRU eviction
}

// dbStmtCache holds all cached statements for a single *sql.DB instance.
type dbStmtCache struct {
	mu    sync.RWMutex
	cache map[string]*stmtCacheEntry
	stats *cacheStats
}

// cacheStats tracks cache performance metrics.
type cacheStats struct {
	mu       sync.RWMutex
	hits     int64
	misses   int64
	prepares int64
	errors   int64
}

// getStats returns a snapshot of cache statistics.
func (s *cacheStats) getStats() (int64, int64, int64, int64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.hits, s.misses, s.prepares, s.errors
}

func (s *cacheStats) recordHit() {
	s.mu.Lock()
	s.hits++
	s.mu.Unlock()
}

func (s *cacheStats) recordMiss() {
	s.mu.Lock()
	s.misses++
	s.mu.Unlock()
}

func (s *cacheStats) recordPrepare() {
	s.mu.Lock()
	s.prepares++
	s.mu.Unlock()
}

func (s *cacheStats) recordError() {
	s.mu.Lock()
	s.errors++
	s.mu.Unlock()
}

// globalStmtCaches maps *sql.DB instances to their statement caches.
// Key is the pointer address of the *sql.DB (as uintptr).
// This ensures statements are scoped to the correct database connection.
var globalStmtCaches sync.Map // map[uintptr]*dbStmtCache

// getDBCache returns the statement cache for a specific *sql.DB instance.
// Creates a new cache if one doesn't exist for this DB.
func getDBCache(db *sql.DB) *dbStmtCache {
	dbPtr := uintptr(unsafe.Pointer(db))

	if cache, ok := globalStmtCaches.Load(dbPtr); ok {
		return cache.(*dbStmtCache)
	}

	// Create new cache for this DB instance
	newCache := &dbStmtCache{
		cache: make(map[string]*stmtCacheEntry),
		stats: &cacheStats{},
	}

	// Use LoadOrStore to handle race condition where multiple goroutines
	// might try to create a cache for the same DB simultaneously
	actual, _ := globalStmtCaches.LoadOrStore(dbPtr, newCache)
	return actual.(*dbStmtCache)
}

// normalizeQuery normalizes a SQL query string for use as a cache key.
// It:
// - Collapses multiple whitespace characters into single spaces
// - Trims leading/trailing whitespace
// - Preserves the structure (SELECT, WHERE, etc.)
func normalizeQuery(query string) string {
	// Collapse multiple whitespace characters (spaces, tabs, newlines) into single space
	whitespace := regexp.MustCompile(`\s+`)
	normalized := whitespace.ReplaceAllString(query, " ")

	// Trim leading and trailing whitespace
	normalized = strings.TrimSpace(normalized)

	return normalized
}

// GetPreparedStmt returns a cached prepared statement for the given query, or creates
// and caches a new one if it doesn't exist. The statement is tied to the specific *sql.DB
// instance and must not be used with a different connection.
//
// This function is safe for concurrent use by multiple goroutines accessing the same *sql.DB.
func GetPreparedStmt(ctx context.Context, db *sql.DB, query string) (*sql.Stmt, error) {
	if db == nil {
		return nil, fmt.Errorf("stmt_cache: db is nil")
	}

	normalized := normalizeQuery(query)
	dbCache := getDBCache(db)

	// Try to get cached statement (read lock)
	dbCache.mu.RLock()
	entry, exists := dbCache.cache[normalized]
	dbCache.mu.RUnlock()

	if exists && entry != nil && entry.stmt != nil {
		dbCache.stats.recordHit()
		// Track per-query stats only if enabled
		if queryStatsEnabled {
			entry.hitCount++
			entry.execCount++
		}
		return entry.stmt, nil
	}

	// Cache miss - need to prepare new statement (write lock)
	dbCache.stats.recordMiss()
	dbCache.mu.Lock()

	// Double-check after acquiring write lock (another goroutine might have created it)
	entry, exists = dbCache.cache[normalized]
	if exists && entry != nil && entry.stmt != nil {
		dbCache.mu.Unlock()
		dbCache.stats.recordHit()
		if queryStatsEnabled {
			entry.hitCount++
			entry.execCount++
		}
		return entry.stmt, nil
	}

	// Prepare new statement
	stmt, err := db.PrepareContext(ctx, query)
	if err != nil {
		dbCache.mu.Unlock()
		dbCache.stats.recordError()
		return nil, fmt.Errorf("stmt_cache: failed to prepare statement: %w", err)
	}

	dbCache.stats.recordPrepare()

	// Cache the statement
	newEntry := &stmtCacheEntry{
		stmt:  stmt,
		query: normalized,
	}
	// Track per-query stats only if enabled
	if queryStatsEnabled {
		newEntry.execCount = 1 // First execution
		newEntry.hitCount = 0  // This was a miss, not a hit
	}
	dbCache.cache[normalized] = newEntry

	dbCache.mu.Unlock()

	return stmt, nil
}

// QueryRowContextWithCache executes a query that returns at most one row using a cached
// prepared statement. It's a drop-in replacement for db.QueryRowContext.
func QueryRowContextWithCache(ctx context.Context, db *sql.DB, query string, args ...interface{}) *sql.Row {
	stmt, err := GetPreparedStmt(ctx, db, query)
	if err != nil {
		// Fallback to direct query if preparation fails
		return db.QueryRowContext(ctx, query, args...)
	}
	return stmt.QueryRowContext(ctx, args...)
}

// QueryContextWithCache executes a query that returns multiple rows using a cached
// prepared statement. It's a drop-in replacement for db.QueryContext.
func QueryContextWithCache(ctx context.Context, db *sql.DB, query string, args ...interface{}) (*sql.Rows, error) {
	stmt, err := GetPreparedStmt(ctx, db, query)
	if err != nil {
		// Fallback to direct query if preparation fails
		return db.QueryContext(ctx, query, args...)
	}
	return stmt.QueryContext(ctx, args...)
}

// ExecContextWithCache executes a query that doesn't return rows using a cached
// prepared statement. It's a drop-in replacement for db.ExecContext.
func ExecContextWithCache(ctx context.Context, db *sql.DB, query string, args ...interface{}) (sql.Result, error) {
	stmt, err := GetPreparedStmt(ctx, db, query)
	if err != nil {
		// Fallback to direct exec if preparation fails
		return db.ExecContext(ctx, query, args...)
	}
	return stmt.ExecContext(ctx, args...)
}

// ClosePreparedStatements closes all cached prepared statements for a specific *sql.DB.
// This should be called when closing the database connection to avoid resource leaks.
func ClosePreparedStatements(db *sql.DB) error {
	if db == nil {
		return nil
	}

	dbPtr := uintptr(unsafe.Pointer(db))
	cacheInterface, ok := globalStmtCaches.LoadAndDelete(dbPtr)
	if !ok {
		// No cache exists for this DB
		return nil
	}

	dbCache := cacheInterface.(*dbStmtCache)

	dbCache.mu.Lock()
	defer dbCache.mu.Unlock()

	var firstErr error
	for normalized, entry := range dbCache.cache {
		if entry != nil && entry.stmt != nil {
			if err := entry.stmt.Close(); err != nil {
				if firstErr == nil {
					firstErr = fmt.Errorf("stmt_cache: error closing statement for query %q: %w", normalized, err)
				}
			}
		}
	}

	// Clear the cache
	dbCache.cache = make(map[string]*stmtCacheEntry)

	return firstErr
}

// GetCacheStats returns cache statistics for a specific *sql.DB instance.
// Returns nil if no cache exists for the given DB.
func GetCacheStats(db *sql.DB) *CacheStatsSnapshot {
	if db == nil {
		return nil
	}

	dbPtr := uintptr(unsafe.Pointer(db))
	cacheInterface, ok := globalStmtCaches.Load(dbPtr)
	if !ok {
		return nil
	}

	dbCache := cacheInterface.(*dbStmtCache)
	hits, misses, prepares, errors := dbCache.stats.getStats()

	dbCache.mu.RLock()
	cacheSize := len(dbCache.cache)
	dbCache.mu.RUnlock()

	return &CacheStatsSnapshot{
		CacheSize: cacheSize,
		Hits:      hits,
		Misses:    misses,
		Prepares:  prepares,
		Errors:    errors,
	}
}

// CacheStatsSnapshot is a snapshot of cache statistics at a point in time.
type CacheStatsSnapshot struct {
	CacheSize int   // Number of statements currently cached
	Hits      int64 // Number of cache hits
	Misses    int64 // Number of cache misses
	Prepares  int64 // Number of statements prepared
	Errors    int64 // Number of preparation errors
}

// QueryStats represents execution statistics for a single cached query.
type QueryStats struct {
	Query     string  // Normalized query string
	ExecCount int64   // Total number of executions
	HitCount  int64   // Number of cache hits
	HitRate   float64 // Hit rate as percentage (0-100)
}

// GetQueryStats returns per-query execution statistics.
// This helps identify which queries are executed most frequently and benefit from caching.
// Returns an empty slice if query stats tracking is disabled (BATON_ENABLE_QUERY_STATS=false).
func GetQueryStats(db *sql.DB) []QueryStats {
	if db == nil {
		return nil
	}

	// If stats tracking is disabled, return empty slice
	if !queryStatsEnabled {
		return []QueryStats{}
	}

	dbPtr := uintptr(unsafe.Pointer(db))
	cacheInterface, ok := globalStmtCaches.Load(dbPtr)
	if !ok {
		return nil
	}

	dbCache := cacheInterface.(*dbStmtCache)
	dbCache.mu.RLock()
	defer dbCache.mu.RUnlock()

	stats := make([]QueryStats, 0, len(dbCache.cache))
	for normalized, entry := range dbCache.cache {
		if entry == nil {
			continue
		}

		var hitRate float64
		if entry.execCount > 0 {
			hitRate = float64(entry.hitCount) / float64(entry.execCount) * 100.0
		}
		
		stats = append(stats, QueryStats{
			Query:     normalized,
			ExecCount: entry.execCount,
			HitCount:  entry.hitCount,
			HitRate:   hitRate,
		})
	}

	return stats
}

// HitRate returns the cache hit rate as a percentage (0-100).
func (s *CacheStatsSnapshot) HitRate() float64 {
	total := s.Hits + s.Misses
	if total == 0 {
		return 0.0
	}
	return float64(s.Hits) / float64(total) * 100.0
}
