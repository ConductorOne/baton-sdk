package uhttp

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httputil"
	"os"
	"path/filepath"
	"time"

	_ "github.com/glebarez/go-sqlite"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	_ "github.com/mattn/go-sqlite3"
	"go.uber.org/zap"
)

type DBCache struct {
	db *sql.DB
	// Cleanup interval
	waitDuration int64
	// Cache duration
	expirationTime int64
	// Database path
	location string
}
type Stats struct {
	// Hits is a number of successfully found keys
	Hits int64 `json:"hits"`
	// Misses is a number of not found keys
	Misses int64 `json:"misses"`
}

const (
	failStartTransaction = "Failed to start a transaction"
	nilConnection        = "Database connection is nil"
	errQueryingTable     = "Error querying cache table"
	failRollback         = "Failed to rollback transaction"
	failInsert           = "Failed to insert response data into cache table"
	staticQuery          = "INSERT INTO http_stats(key, %s) values(?, 1)"
	failScanResponse     = "Failed to scan rows for cached response"
	defaultWaitDuration  = int64(60) // Default Cleanup interval, 60 seconds
	cacheTTLThreshold    = 60
	cacheTTLMultiplier   = 5
)

func NewDBCache(ctx context.Context, cfg CacheConfig) (*DBCache, error) {
	var (
		err error
		dc  = &DBCache{
			waitDuration: defaultWaitDuration, // Default Cleanup interval, 60 seconds
		}
	)
	l := ctxzap.Extract(ctx)
	dc, err = dc.load(ctx)
	if err != nil {
		l.Debug("Failed to open database", zap.Error(err))
		return nil, err
	}

	// Create cache table and index
	_, err = dc.db.ExecContext(ctx, `
	CREATE TABLE IF NOT EXISTS http_cache(
		id INTEGER PRIMARY KEY, 
		key NVARCHAR, 
		data BLOB, 
		expiration INTEGER, 
		url NVARCHAR
	);
	CREATE UNIQUE INDEX IF NOT EXISTS idx_cache_key ON http_cache (key);
	CREATE TABLE IF NOT EXISTS http_stats(
		id INTEGER PRIMARY KEY,
		key NVARCHAR,
		hits INTEGER DEFAULT 0, 
		misses INTEGER DEFAULT 0,
	);`)
	if err != nil {
		l.Debug("Failed to create cache table in database", zap.Error(err))
		return &DBCache{}, err
	}

	if cfg.CacheTTL > 0 {
		if cfg.CacheTTL > cacheTTLThreshold {
			dc.waitDuration = int64(cfg.CacheTTL * cacheTTLMultiplier) // set as a fraction of the Cache TTL
		}

		dc.expirationTime = int64(cfg.CacheTTL) // cache expiration time
		go func(waitDuration, expirationTime int64) {
			ctxWithTimeout, cancel := context.WithTimeout(
				ctx,
				time.Duration(waitDuration)*time.Second,
			)
			defer cancel()

			ticker := time.NewTicker(time.Duration(expirationTime))
			defer ticker.Stop()
			for {
				select {
				case <-ctxWithTimeout.Done():
					// ctx done, shutting down cache cleanup routine
					ticker.Stop()
					err := dc.cleanup(ctx)
					if err != nil {
						l.Debug("shutting down cache failed", zap.Error(err))
					}
					return
				case <-ticker.C:
					err := dc.deleteExpired(ctx)
					if err != nil {
						l.Debug("Failed to delete expired cache entries", zap.Error(err))
					}
				}
			}
		}(dc.waitDuration, dc.expirationTime)
	}

	return dc, nil
}

func (d *DBCache) load(ctx context.Context) (*DBCache, error) {
	l := ctxzap.Extract(ctx)
	cacheDir, err := os.UserCacheDir()
	if err != nil {
		l.Debug("Failed to read user cache directory", zap.Error(err))
		return &DBCache{}, err
	}

	file := filepath.Join(cacheDir, "lcache.db")
	d.location = file
	// Connect to db
	sqlDB, err := sql.Open("sqlite3", file)
	if err != nil {
		l.Debug("Failed to open database", zap.Error(err))
		return &DBCache{}, err
	}

	d.db = sqlDB
	return d, nil
}

func checkFileExists(filePath string) bool {
	_, err := os.Stat(filePath)
	return !errors.Is(err, os.ErrNotExist)
}

func (d *DBCache) removeDB(ctx context.Context) error {
	if !checkFileExists(d.location) {
		return fmt.Errorf("file not found %s", d.location)
	}

	err := os.Remove(d.location)
	if err != nil {
		ctxzap.Extract(ctx).Debug("error removing database", zap.Error(err))
		return err
	}

	return nil
}

// Get returns cached response (if exists).
func (d *DBCache) Get(ctx context.Context, key string) (*http.Response, error) {
	if d.IsNilConnection() {
		return nil, fmt.Errorf("%s", nilConnection)
	}

	entry, err := d.pick(ctx, key)
	if err == nil && len(entry) > 0 {
		r := bufio.NewReader(bytes.NewReader(entry))
		resp, err := http.ReadResponse(r, nil)
		if err != nil {
			return nil, err
		}

		err = d.hits(ctx, key)
		if err != nil {
			ctxzap.Extract(ctx).Debug("Failed to update cache hits", zap.Error(err))
		}

		return resp, nil
	}

	err = d.misses(ctx, key)
	if err != nil {
		ctxzap.Extract(ctx).Debug("Failed to update cache misses", zap.Error(err))
	}

	return nil, nil
}

// Set stores and save response in the db.
func (d *DBCache) Set(ctx context.Context, key string, value *http.Response) error {
	var url string
	if d.IsNilConnection() {
		return fmt.Errorf("%s", nilConnection)
	}

	cacheableResponse, err := httputil.DumpResponse(value, true)
	if err != nil {
		return err
	}

	if value.Request != nil {
		url = getFullUrl(value.Request)
	}

	err = d.insert(ctx,
		key,
		cacheableResponse,
		url,
	)
	if err != nil {
		return err
	}

	return nil
}

func (d *DBCache) cleanup(ctx context.Context) error {
	if d.IsNilConnection() {
		return fmt.Errorf("%s", nilConnection)
	}

	l := ctxzap.Extract(ctx)
	stats, err := d.getStats(ctx)
	if err != nil {
		l.Debug("error getting stats", zap.Error(err))
		return err
	}

	l.Debug("summary and stats", zap.Any("stats", stats))
	err = d.close(ctx)
	if err != nil {
		l.Debug("error closing db", zap.Error(err))
		return err
	}

	err = d.removeDB(ctx)
	if err != nil {
		l.Debug("error removing db", zap.Error(err))
		return err
	}

	return nil
}

// Insert data into the cache table.
func (d *DBCache) insert(ctx context.Context, key string, value any, url string) error {
	var (
		bytes []byte
		err   error
		ok    bool
	)
	if d.IsNilConnection() {
		return fmt.Errorf("%s", nilConnection)
	}

	l := ctxzap.Extract(ctx)
	if bytes, ok = value.([]byte); !ok {
		bytes, err = json.Marshal(value)
		if err != nil {
			l.Debug("Failed to marshal response data", zap.Error(err))
			return err
		}
	}

	if ok, _ := d.has(ctx, key); !ok {
		tx, err := d.db.Begin()
		if err != nil {
			l.Debug(failStartTransaction, zap.Error(err))
			return err
		}

		_, err = tx.ExecContext(ctx, "INSERT INTO http_cache(key, data, expiration, url) values(?, ?, ?, ?)",
			key,
			bytes,
			(time.Now().UnixNano() + d.waitDuration),
			url,
		)
		if err != nil {
			if errtx := tx.Rollback(); errtx != nil {
				l.Debug(failRollback, zap.Error(errtx))
			}

			l.Debug(failInsert, zap.Error(err))
			return err
		}

		err = tx.Commit()
		if err != nil {
			if errtx := tx.Rollback(); errtx != nil {
				l.Debug(failRollback, zap.Error(errtx))
			}

			l.Debug(failInsert, zap.Error(err))
			return err
		}
	}

	return nil
}

// Has query for cached keys.
func (d *DBCache) has(ctx context.Context, key string) (bool, error) {
	if d.IsNilConnection() {
		return false, fmt.Errorf("%s", nilConnection)
	}

	l := ctxzap.Extract(ctx)
	rows, err := d.db.Query("SELECT data FROM http_cache where key = ?", key)
	if err != nil {
		l.Debug("Failed to query cache table for key existence", zap.Error(err))
		return false, err
	}

	defer rows.Close()
	for rows.Next() {
		return true, nil
	}

	return false, nil
}

// IsNilConnection check if the database connection is nil.
func (d *DBCache) IsNilConnection() bool {
	return d.db == nil
}

// pick query for cached response.
func (d *DBCache) pick(ctx context.Context, key string) ([]byte, error) {
	var data []byte
	if d.IsNilConnection() {
		return nil, fmt.Errorf("%s", nilConnection)
	}

	l := ctxzap.Extract(ctx)
	rows, err := d.db.QueryContext(ctx, "SELECT data FROM http_cache where key = ?", key)
	if err != nil {
		l.Debug(errQueryingTable, zap.Error(err))
		return nil, err
	}

	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(&data)
		if err != nil {
			l.Debug(failScanResponse, zap.Error(err))
			return nil, err
		}
	}

	return data, nil
}

func (d *DBCache) remove(ctx context.Context) error {
	if d.IsNilConnection() {
		return fmt.Errorf("%s", nilConnection)
	}

	l := ctxzap.Extract(ctx)
	tx, err := d.db.Begin()
	if err != nil {
		l.Debug(failStartTransaction, zap.Error(err))
		return err
	}

	_, err = d.db.ExecContext(ctx, "DELETE FROM http_cache WHERE expiration < ?", time.Now().UnixNano())
	if err != nil {
		if errtx := tx.Rollback(); errtx != nil {
			l.Debug(failRollback, zap.Error(errtx))
		}

		l.Debug("Failed to delete cache key", zap.Error(err))
		return err
	}

	err = tx.Commit()
	if err != nil {
		if errtx := tx.Rollback(); errtx != nil {
			l.Debug(failRollback, zap.Error(errtx))
		}

		l.Debug("Failed to remove cache entry", zap.Error(err))
		return err
	}

	return nil
}

func (d *DBCache) close(ctx context.Context) error {
	if d.IsNilConnection() {
		return fmt.Errorf("%s", nilConnection)
	}

	err := d.db.Close()
	if err != nil {
		ctxzap.Extract(ctx).Debug("Failed to close database connection", zap.Error(err))
		return err
	}

	return nil
}

// Delete all expired items from the cache.
func (d *DBCache) deleteExpired(ctx context.Context) error {
	if d.IsNilConnection() {
		return fmt.Errorf("%s", nilConnection)
	}

	l := ctxzap.Extract(ctx)
	err := d.remove(ctx)
	if err != nil {
		l.Debug("error removing rows",
			zap.Error(err),
		)
	}

	return nil
}

func getFullUrl(r *http.Request) string {
	return fmt.Sprintf("%s://%s%s", r.URL.Scheme, r.Host, r.URL.Path)
}

func (d *DBCache) hits(ctx context.Context, key string) error {
	if d.IsNilConnection() {
		return fmt.Errorf("%s", nilConnection)
	}

	strField := "hits"
	err := d.update(ctx, strField, key)
	if err != nil {
		return err
	}

	return nil
}

func (d *DBCache) misses(ctx context.Context, key string) error {
	if d.IsNilConnection() {
		return fmt.Errorf("%s", nilConnection)
	}

	strField := "misses"
	err := d.update(ctx, strField, key)
	if err != nil {
		return err
	}

	return nil
}

func (d *DBCache) update(ctx context.Context, field, key string) error {
	l := ctxzap.Extract(ctx)
	tx, err := d.db.Begin()
	if err != nil {
		l.Debug(failStartTransaction, zap.Error(err))
		return err
	}

	query, args := d.queryString(field)
	_, err = d.db.ExecContext(ctx, fmt.Sprintf(query, args...), key)
	if err != nil {
		if errtx := tx.Rollback(); errtx != nil {
			l.Debug(failRollback, zap.Error(errtx))
		}

		l.Debug("error updating "+field, zap.Error(err))
		return err
	}

	err = tx.Commit()
	if err != nil {
		if errtx := tx.Rollback(); errtx != nil {
			l.Debug(failRollback, zap.Error(errtx))
		}

		l.Debug("Failed to update "+field, zap.Error(err))
		return err
	}

	return nil
}

func (d *DBCache) queryString(field string) (string, []interface{}) {
	return staticQuery, []interface{}{
		fmt.Sprint(field),
	}
}

func (d *DBCache) getStats(ctx context.Context) (Stats, error) {
	var (
		hits   = 0
		misses = 0
	)
	if d.IsNilConnection() {
		return Stats{}, fmt.Errorf("%s", nilConnection)
	}

	l := ctxzap.Extract(ctx)
	rows, err := d.db.QueryContext(ctx, `
	SELECT 
		sum(hits) total_hits, 
		sum(misses) total_misses 
	FROM http_stats
	`)
	if err != nil {
		l.Debug(errQueryingTable, zap.Error(err))
		return Stats{}, err
	}

	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(&hits, &misses)
		if err != nil {
			l.Debug(failScanResponse, zap.Error(err))
			return Stats{}, err
		}
	}

	return Stats{
		Hits:   int64(hits),
		Misses: int64(misses),
	}, nil
}

// Len computes number of entries in cache.
func (d *DBCache) len(ctx context.Context) (int, error) {
	var count int = 0
	if d.IsNilConnection() {
		return -1, fmt.Errorf("%s", nilConnection)
	}

	l := ctxzap.Extract(ctx)
	rows, err := d.db.QueryContext(ctx, `SELECT count(*) FROM http_cache`)
	if err != nil {
		l.Debug(errQueryingTable, zap.Error(err))
		return -1, err
	}

	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(&count)
		if err != nil {
			l.Debug("Failed to scan rows from table", zap.Error(err))
			return -1, err
		}
	}

	return count, nil
}

func (d *DBCache) Clear(ctx context.Context) error {
	// TODO: Implement
	return nil
}
