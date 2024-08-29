package uhttp

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httputil"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	_ "github.com/glebarez/go-sqlite"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	_ "github.com/mattn/go-sqlite3"
	"go.uber.org/zap"
)

type ICache interface {
	Get(ctx context.Context, key string) (*http.Response, error)
	Set(ctx context.Context, key string, value *http.Response) error
	Clear(ctx context.Context) error
	CreateCacheKey(req *http.Request) (string, error)
}

type DBCache struct {
	db                *sql.DB
	defaultExpiration time.Duration
}

type Stats struct {
	// Hits is a number of successfully found keys
	Hits int64 `json:"hits"`
	// Misses is a number of not found keys
	Misses int64 `json:"misses"`
	// DelHits is a number of successfully deleted keys
	DelHits int64 `json:"delete_hits"`
	// DelMisses is a number of not deleted keys
	DelMisses int64 `json:"delete_misses"`
	// Collisions is a number of happened key-collisions
	Collisions int64 `json:"collisions"`
}

const (
	failStartTransaction = "Failed to start a transaction"
	nilConnection        = "Database connection is nil"
	errQueryingTable     = "Error querying cache table"
	failRollback         = "Failed to rollback transaction"
	failInsert           = "Failed to insert data into cache table"
	staticQuery          = "UPDATE http_cache SET %s=(%s+1) WHERE key = ?"
)

func NewDBCache(ctx context.Context, cfg CacheConfig) (*DBCache, error) {
	l := ctxzap.Extract(ctx)
	cacheDir, err := os.UserCacheDir()
	if err != nil {
		l.Debug("Failed to read user cache directory", zap.Error(err))
		return nil, err
	}

	// Connect to db
	db, err := sql.Open("sqlite3", filepath.Join(cacheDir, "lcache.db"))
	if err != nil {
		l.Debug("Failed to open database", zap.Error(err))
		return &DBCache{}, err
	}

	// Create cache table and index
	_, err = db.ExecContext(ctx, `
	CREATE TABLE IF NOT EXISTS http_cache(
		id INTEGER PRIMARY KEY, 
		key NVARCHAR, 
		data BLOB, 
		expiration INTEGER, 
		url NVARCHAR, 
		hits INTEGER DEFAULT 0, 
		misses INTEGER DEFAULT 0, 
		delhits INTEGER DEFAULT 0,
		delmisses INTEGER DEFAULT 0, 
		collisions INTEGER DEFAULT 0
	);
	CREATE UNIQUE INDEX IF NOT EXISTS idx_cache_key ON http_cache (key);`)
	if err != nil {
		l.Debug("Failed to create cache table in database", zap.Error(err))
		return &DBCache{}, err
	}

	dc := &DBCache{
		defaultExpiration: cfg.ExpirationTime,
		db:                db,
	}

	if cfg.NoExpiration > 0 {
		go func() {
			ctxWithTimeout, cancel := context.WithTimeout(
				ctx,
				time.Duration(180)*time.Minute,
			)
			defer cancel()

			ticker := time.NewTicker(dc.defaultExpiration)
			defer ticker.Stop()
			for {
				select {
				case <-ctxWithTimeout.Done():
					// ctx done, shutting down cache cleanup routine
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
		}()
	}

	return dc, nil
}

// CreateCacheKey generates a cache key based on the request URL, query parameters, and headers.
func (d *DBCache) CreateCacheKey(req *http.Request) (string, error) {
	var sortedParams []string
	// Normalize the URL path
	path := strings.ToLower(req.URL.Path)
	// Combine the path with sorted query parameters
	queryParams := req.URL.Query()
	for k, v := range queryParams {
		for _, value := range v {
			sortedParams = append(sortedParams, fmt.Sprintf("%s=%s", k, value))
		}
	}

	sort.Strings(sortedParams)
	queryString := strings.Join(sortedParams, "&")
	// Include relevant headers in the cache key
	var headerParts []string
	for key, values := range req.Header {
		for _, value := range values {
			if key == "Accept" || key == "Content-Type" || key == "Cookie" || key == "Range" {
				headerParts = append(headerParts, fmt.Sprintf("%s=%s", key, value))
			}
		}
	}

	sort.Strings(headerParts)
	headersString := strings.Join(headerParts, "&")
	// Create a unique string for the cache key
	cacheString := fmt.Sprintf("%s?%s&headers=%s", path, queryString, headersString)

	// Hash the cache string to create a key
	hash := sha256.New()
	_, err := hash.Write([]byte(cacheString))
	if err != nil {
		return "", err
	}

	cacheKey := fmt.Sprintf("%x", hash.Sum(nil))
	return cacheKey, nil
}

// Get returns cached response (if exists).
func (d *DBCache) Get(ctx context.Context, key string) (*http.Response, error) {
	if d.IsNilConnection() {
		return nil, fmt.Errorf("%s", nilConnection)
	}

	entry, err := d.Select(ctx, key)
	if err == nil && len(entry) > 0 {
		r := bufio.NewReader(bytes.NewReader(entry))
		resp, err := http.ReadResponse(r, nil)
		if err != nil {
			return nil, err
		}

		err = d.Hits(ctx, key)
		if err != nil {
			ctxzap.Extract(ctx).Debug("Failed to update hits", zap.Error(err))
		}

		return resp, nil
	}

	err = d.Misses(ctx, key)
	if err != nil {
		ctxzap.Extract(ctx).Debug("Failed to update misses", zap.Error(err))
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

	err = d.Insert(ctx,
		key,
		cacheableResponse,
		url,
	)
	if err != nil {
		return err
	}

	return nil
}

// Remove stored keys.
func (d *DBCache) Delete(ctx context.Context, key string) error {
	if d.IsNilConnection() {
		return fmt.Errorf("%s", nilConnection)
	}

	err := d.Remove(ctx, key)
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

	return nil
}

// Insert data into the cache table.
func (d *DBCache) Insert(ctx context.Context, key string, value any, url string) error {
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
			l.Debug("error marshaling data", zap.Error(err))
			return err
		}
	}

	if ok, _ := d.Has(ctx, key); !ok {
		tx, err := d.db.Begin()
		if err != nil {
			l.Debug(failStartTransaction, zap.Error(err))
			return err
		}

		_, err = tx.ExecContext(ctx, "INSERT INTO http_cache(key, data, expiration, url) values(?, ?, ?, ?)",
			key,
			bytes,
			time.Now().UnixNano(),
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
func (d *DBCache) Has(ctx context.Context, key string) (bool, error) {
	if d.IsNilConnection() {
		return false, fmt.Errorf("%s", nilConnection)
	}

	l := ctxzap.Extract(ctx)
	rows, err := d.db.Query("SELECT data FROM http_cache where key = ?", key)
	if err != nil {
		l.Debug("Failed to query cache table", zap.Error(err))
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

// Select query for cached response.
func (d *DBCache) Select(ctx context.Context, key string) ([]byte, error) {
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
			l.Debug("Failed to scan rows from cache table", zap.Error(err))
			return nil, err
		}
	}

	return data, nil
}

func (d *DBCache) Remove(ctx context.Context, key string) error {
	if d.IsNilConnection() {
		return fmt.Errorf("%s", nilConnection)
	}

	l := ctxzap.Extract(ctx)
	tx, err := d.db.Begin()
	if err != nil {
		l.Debug(failStartTransaction, zap.Error(err))
		return err
	}

	_, err = d.db.ExecContext(ctx, "DELETE FROM http_cache WHERE key = ?", key)
	if err != nil {
		if errtx := tx.Rollback(); errtx != nil {
			l.Debug(failRollback, zap.Error(errtx))
		}

		l.Debug("error deleting key", zap.Error(err))
		return err
	}

	err = tx.Commit()
	if err != nil {
		if errtx := tx.Rollback(); errtx != nil {
			l.Debug(failRollback, zap.Error(errtx))
		}

		l.Debug("Failed to remove cache value", zap.Error(err))
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
		ctxzap.Extract(ctx).Debug("error closing database", zap.Error(err))
		return err
	}

	return nil
}

// Expired checks if key is expired.
func (d *DBCache) Expired(expiration int64) bool {
	return time.Now().UnixNano() > expiration
}

// Delete all expired items from the cache.
func (d *DBCache) deleteExpired(ctx context.Context) error {
	var (
		expiration     int64
		key            string
		mapExpiredKeys = make(map[string]bool)
	)

	if d.IsNilConnection() {
		return fmt.Errorf("%s", nilConnection)
	}

	l := ctxzap.Extract(ctx)
	rows, err := d.db.QueryContext(ctx, "SELECT key, expiration FROM http_cache")
	if err != nil {
		l.Debug(errQueryingTable, zap.Error(err))
		return err
	}

	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(&key, &expiration)
		if err != nil {
			l.Debug("error scanning rows",
				zap.Error(err),
				zap.String("key", key),
			)
			return err
		}

		mapExpiredKeys[key] = d.Expired(expiration)
	}

	go func() {
		for key, isExpired := range mapExpiredKeys {
			if isExpired {
				err := d.Remove(ctx, key)
				if err != nil {
					l.Debug("error removing rows",
						zap.Error(err),
						zap.String("key", key),
					)
					return
				}
			}
		}
	}()

	return nil
}

func getFullUrl(r *http.Request) string {
	return fmt.Sprintf("%s://%s%s", r.URL.Scheme, r.Host, r.URL.Path)
}

func (d *DBCache) Hits(ctx context.Context, key string) error {
	if d.IsNilConnection() {
		return fmt.Errorf("%s", nilConnection)
	}

	strField := "hits"
	err := d.Update(ctx, strField, key)
	if err != nil {
		return err
	}

	return nil
}

func (d *DBCache) DelHits(ctx context.Context, key string) error {
	if d.IsNilConnection() {
		return fmt.Errorf("%s", nilConnection)
	}

	strField := "delhits"
	err := d.Update(ctx, strField, key)
	if err != nil {
		return err
	}

	return nil
}

func (d *DBCache) Misses(ctx context.Context, key string) error {
	if d.IsNilConnection() {
		return fmt.Errorf("%s", nilConnection)
	}

	strField := "misses"
	err := d.Update(ctx, strField, key)
	if err != nil {
		return err
	}

	return nil
}

func (d *DBCache) DelMisses(ctx context.Context, key string) error {
	if d.IsNilConnection() {
		return fmt.Errorf("%s", nilConnection)
	}

	strField := "delmisses"
	err := d.Update(ctx, strField, key)
	if err != nil {
		return err
	}

	return nil
}

func (d *DBCache) Collisions(ctx context.Context, key string) error {
	if d.IsNilConnection() {
		return fmt.Errorf("%s", nilConnection)
	}

	strField := "collisions"
	err := d.Update(ctx, strField, key)
	if err != nil {
		return err
	}

	return nil
}

func (d *DBCache) Update(ctx context.Context, field, key string) error {
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
		fmt.Sprint(field),
	}
}

func (d *DBCache) getStats(ctx context.Context) (Stats, error) {
	var (
		hits       = 0
		misses     = 0
		delhits    = 0
		delmisses  = 0
		collisions = 0
	)
	if d.IsNilConnection() {
		return Stats{}, fmt.Errorf("%s", nilConnection)
	}

	l := ctxzap.Extract(ctx)
	rows, err := d.db.QueryContext(ctx, `
	SELECT 
		sum(hits) total_hits, 
		sum(misses) total_misses, 
		sum(delhits) total_delhits, 
		sum(delmisses) total_delmisses, 
		sum(collisions) total_collisions 
	FROM http_cache
	`)
	if err != nil {
		l.Debug(errQueryingTable, zap.Error(err))
		return Stats{}, err
	}

	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(&hits, &misses, &delhits, &delmisses, &collisions)
		if err != nil {
			l.Debug("Failed to scan rows from cache table", zap.Error(err))
			return Stats{}, err
		}
	}

	return Stats{
		Hits:       int64(hits),
		Misses:     int64(misses),
		DelHits:    int64(delhits),
		DelMisses:  int64(delmisses),
		Collisions: int64(collisions),
	}, nil
}
