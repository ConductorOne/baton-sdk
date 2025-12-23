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
	"sync/atomic"
	"unsafe"
)

const (
	// defaultMaxCacheEntries is the default maximum number of prepared statements
	// to cache per database connection. This prevents unbounded memory growth.
	defaultMaxCacheEntries = 100
)

// getMaxCacheEntries returns the maximum number of cache entries per DB.
// Can be overridden via BATON_STMT_CACHE_MAX_ENTRIES environment variable.
func getMaxCacheEntries() int {
	val := os.Getenv("BATON_STMT_CACHE_MAX_ENTRIES")
	if val == "" {
		return defaultMaxCacheEntries
	}
	maxEntries, err := strconv.Atoi(val)
	if err != nil || maxEntries <= 0 {
		return defaultMaxCacheEntries
	}
	return maxEntries
}

// stmtCacheEntry holds a prepared statement and metadata for a single query pattern.
type stmtCacheEntry struct {
	stmt  *sql.Stmt
	query string // normalized query string
}

// dbStmtCache holds all cached statements for a single *sql.DB instance.
type dbStmtCache struct {
	mu         sync.RWMutex
	cache      map[string]*stmtCacheEntry
	stats      *cacheStats
	maxEntries int
	// evictionOrder tracks insertion order for FIFO eviction
	evictionOrder []string
}

// cacheStats tracks cache performance metrics using atomic operations
// for lock-free updates on the hot path.
type cacheStats struct {
	hits      int64
	misses    int64
	prepares  int64
	errors    int64
	evictions int64 // number of entries evicted due to cache size limit
}

// getStats returns a snapshot of cache statistics.
func (s *cacheStats) getStats() (int64, int64, int64, int64, int64) {
	return atomic.LoadInt64(&s.hits),
		atomic.LoadInt64(&s.misses),
		atomic.LoadInt64(&s.prepares),
		atomic.LoadInt64(&s.errors),
		atomic.LoadInt64(&s.evictions)
}

func (s *cacheStats) recordEviction() {
	atomic.AddInt64(&s.evictions, 1)
}

func (s *cacheStats) recordHit() {
	atomic.AddInt64(&s.hits, 1)
}

func (s *cacheStats) recordMiss() {
	atomic.AddInt64(&s.misses, 1)
}

func (s *cacheStats) recordPrepare() {
	atomic.AddInt64(&s.prepares, 1)
}

func (s *cacheStats) recordError() {
	atomic.AddInt64(&s.errors, 1)
}

// globalStmtCaches maps *sql.DB instances to their statement caches.
// Key is the pointer address of the *sql.DB (as uintptr).
// This ensures statements are scoped to the correct database connection.
var globalStmtCaches sync.Map // map[uintptr]*dbStmtCache

// whitespaceRegex is pre-compiled for query normalization.
// Compiled once at package init to avoid repeated compilation overhead.
var whitespaceRegex = regexp.MustCompile(`\s+`)

// getDBCache returns the statement cache for a specific *sql.DB instance.
// Creates a new cache if one doesn't exist for this DB.
func getDBCache(db *sql.DB) *dbStmtCache {
	dbPtr := uintptr(unsafe.Pointer(db))

	if cache, ok := globalStmtCaches.Load(dbPtr); ok {
		return cache.(*dbStmtCache)
	}

	// Create new cache for this DB instance
	newCache := &dbStmtCache{
		cache:         make(map[string]*stmtCacheEntry),
		stats:         &cacheStats{},
		maxEntries:    getMaxCacheEntries(),
		evictionOrder: make([]string, 0, getMaxCacheEntries()),
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
	return strings.TrimSpace(whitespaceRegex.ReplaceAllString(query, " "))
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
		return entry.stmt, nil
	}

	// Cache miss - need to prepare new statement (write lock)
	dbCache.stats.recordMiss()
	dbCache.mu.Lock()

	// Double-check after acquiring write lock (another goroutine might have created it)
	entry, exists = dbCache.cache[normalized]
	if exists && entry != nil && entry.stmt != nil {
		// Move to end of eviction order (most recently used)
		// This helps keep frequently used statements in cache longer
		for i, key := range dbCache.evictionOrder {
			if key == normalized {
				// Remove from current position
				dbCache.evictionOrder = append(dbCache.evictionOrder[:i], dbCache.evictionOrder[i+1:]...)
				// Add to end
				dbCache.evictionOrder = append(dbCache.evictionOrder, normalized)
				break
			}
		}
		dbCache.mu.Unlock()
		dbCache.stats.recordHit()
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

	// Check if cache is at capacity and evict if necessary
	if len(dbCache.cache) >= dbCache.maxEntries {
		// Evict oldest entry (FIFO)
		if len(dbCache.evictionOrder) > 0 {
			oldestKey := dbCache.evictionOrder[0]
			// Remove from eviction order
			dbCache.evictionOrder = dbCache.evictionOrder[1:]
			// Close and remove from cache
			if oldEntry, exists := dbCache.cache[oldestKey]; exists && oldEntry != nil && oldEntry.stmt != nil {
				_ = oldEntry.stmt.Close() // Ignore close errors during eviction
			}
			delete(dbCache.cache, oldestKey)
			dbCache.stats.recordEviction()
		}
	}

	// Cache the statement
	dbCache.cache[normalized] = &stmtCacheEntry{
		stmt:  stmt,
		query: normalized,
	}
	// Add to eviction order (append to end)
	dbCache.evictionOrder = append(dbCache.evictionOrder, normalized)

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
	dbCache.evictionOrder = make([]string, 0, dbCache.maxEntries)

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
	hits, misses, prepares, errors, evictions := dbCache.stats.getStats()

	dbCache.mu.RLock()
	cacheSize := len(dbCache.cache)
	maxEntries := dbCache.maxEntries
	dbCache.mu.RUnlock()

	return &CacheStatsSnapshot{
		CacheSize:  cacheSize,
		Hits:       hits,
		Misses:     misses,
		Prepares:   prepares,
		Errors:     errors,
		Evictions:  evictions,
		MaxEntries: maxEntries,
	}
}

// CacheStatsSnapshot is a snapshot of cache statistics at a point in time.
type CacheStatsSnapshot struct {
	CacheSize  int   // Number of statements currently cached
	Hits       int64 // Number of cache hits
	Misses     int64 // Number of cache misses
	Prepares   int64 // Number of statements prepared
	Errors     int64 // Number of preparation errors
	Evictions  int64 // Number of entries evicted due to cache size limit
	MaxEntries int   // Maximum number of entries allowed in cache
}

// HitRate returns the cache hit rate as a percentage (0-100).
func (s *CacheStatsSnapshot) HitRate() float64 {
	total := s.Hits + s.Misses
	if total == 0 {
		return 0.0
	}
	return float64(s.Hits) / float64(total) * 100.0
}
