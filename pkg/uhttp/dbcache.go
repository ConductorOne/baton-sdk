package uhttp

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httputil"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	_ "github.com/doug-martin/goqu/v9/dialect/sqlite3"
	_ "github.com/glebarez/go-sqlite"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

type ICache interface {
	Get(ctx context.Context, key string) (*http.Response, error)
	Set(ctx context.Context, key string, value *http.Response) error
	CreateCacheKey(req *http.Request) (string, error)
}

type DBCache struct {
	db                *sql.DB
	mu                sync.RWMutex
	defaultExpiration time.Duration
}

func NewDBCache(ctx context.Context, cfg CacheConfig) (*DBCache, error) {
	var defaultTime time.Duration = time.Duration(cfg.CacheTTL) * time.Second
	l := ctxzap.Extract(ctx)
	cacheDir, err := os.UserCacheDir()
	if err != nil {
		l.Debug("error reading user cache directory", zap.Error(err))
		return nil, err
	}

	// Connect to db
	db, err := sql.Open("sqlite", filepath.Join(cacheDir, "lcache.db"))
	if err != nil {
		l.Debug("error opening sql database", zap.Error(err))
		return &DBCache{}, err
	}

	// Create cache table
	_, err = db.Exec("CREATE TABLE IF NOT EXISTS http_cache(id INTEGER PRIMARY KEY, key NVARCHAR, data BLOB, expiration INTEGER)")
	if err != nil {
		l.Debug("error creating cache table", zap.Error(err))
		return &DBCache{}, err
	}

	dc := &DBCache{
		defaultExpiration: defaultTime,
		db:                db,
	}
	go func() {
		ticker := time.NewTicker(dc.defaultExpiration)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				// ctx done, shutting down bigcache cleanup routine
				return
			case <-ticker.C:
				err := dc.DeleteExpired(ctx)
				if err != nil {
					l.Debug("error deleting expired cache", zap.Error(err))
				}
			}
		}
	}()

	return dc, nil
}

// GenerateCacheKey generates a cache key based on the request URL, query parameters, and headers.
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
			if key == "Accept" || key == "Authorization" || key == "Cookie" || key == "Range" {
				headerParts = append(headerParts, fmt.Sprintf("%s=%s", key, value))
			}
		}
	}

	sort.Strings(headerParts)
	headersString := strings.Join(headerParts, "&")
	// Create a unique string for the cache key
	cacheString := fmt.Sprintf("%s?%s&headers=%s", path, queryString, headersString)
	return cacheString, nil
}

func (d *DBCache) Get(ctx context.Context, key string) (*http.Response, error) {
	if d.db == nil {
		return nil, nil
	}

	entry, err := d.Select(ctx, key)
	if err == nil && len(entry) > 0 {
		r := bufio.NewReader(bytes.NewReader(entry))
		resp, err := http.ReadResponse(r, nil)
		if err != nil {
			return nil, err
		}

		return resp, nil
	}

	return nil, nil
}

func (d *DBCache) Set(ctx context.Context, key string, value *http.Response) error {
	if d.db == nil {
		return nil
	}

	cacheableResponse, err := httputil.DumpResponse(value, true)
	if err != nil {
		return err
	}

	err = d.Insert(ctx, key, cacheableResponse)
	if err != nil {
		return err
	}

	return nil
}

func (d *DBCache) Delete(ctx context.Context, key string) error {
	if d.db == nil {
		return nil
	}

	err := d.Remove(ctx, key)
	if err != nil {
		return err
	}

	return nil
}

func (d *DBCache) Clear(ctx context.Context) error {
	if d.db == nil {
		return nil
	}

	err := d.close(ctx)
	if err != nil {
		return err
	}

	return nil
}

// Insert data into the cache table.
func (d *DBCache) Insert(ctx context.Context, key string, value any) error {
	var (
		bytes []byte
		err   error
		ok    bool
	)
	l := ctxzap.Extract(ctx)
	if bytes, ok = value.([]byte); !ok {
		bytes, err = json.Marshal(value)
		if err != nil {
			l.Debug("error marshaling data", zap.Error(err))
			return err
		}
	}

	if ok, _ := d.Has(ctx, key); !ok {
		d.mu.RLock()
		defer d.mu.RUnlock()
		_, err := d.db.Exec("INSERT INTO http_cache(key, data, expiration) values(?, ?, ?)", key, bytes, time.Now().UnixNano())
		if err != nil {
			l.Debug("error inserting data", zap.Error(err))
			return err
		}
	}

	return nil
}

func (d *DBCache) Has(ctx context.Context, key string) (bool, error) {
	l := ctxzap.Extract(ctx)
	rows, err := d.db.Query("SELECT data FROM http_cache where key = ?", key)
	if err != nil {
		l.Debug("error querying datatable", zap.Error(err))
		return false, err
	}

	defer rows.Close()
	for rows.Next() {
		return true, nil
	}

	return false, nil
}

func (d *DBCache) Select(ctx context.Context, key string) ([]byte, error) {
	var data []byte
	l := ctxzap.Extract(ctx)
	rows, err := d.db.Query("SELECT data FROM http_cache where key = ?", key)
	if err != nil {
		l.Debug("error querying datatable", zap.Error(err))
		return nil, err
	}

	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(&data)
		if err != nil {
			l.Debug("error scaning rows", zap.Error(err))
			return nil, err
		}
	}

	return data, nil
}

func (d *DBCache) Remove(ctx context.Context, key string) error {
	l := ctxzap.Extract(ctx)
	if ok, _ := d.Has(ctx, key); ok {
		d.mu.RLock()
		defer d.mu.RUnlock()
		_, err := d.db.Exec("DELETE FROM http_cache WHERE key = ?", key)
		if err != nil {
			l.Debug("error deleting key", zap.Error(err))
			return err
		}
	}

	return nil
}

func (d *DBCache) close(ctx context.Context) error {
	err := d.db.Close()
	if err != nil {
		ctxzap.Extract(ctx).Debug("error closing database", zap.Error(err))
		return err
	}

	return nil
}

func (d *DBCache) Expired(expiration int64) bool {
	return time.Now().UnixNano() > expiration
}

// Delete all expired items from the cache.
func (d *DBCache) DeleteExpired(ctx context.Context) error {
	var (
		expiration int64
		key        string
	)
	l := ctxzap.Extract(ctx)
	rows, err := d.db.Query("SELECT key, expiration FROM http_cache")
	if err != nil {
		l.Debug("error querying datatable", zap.Error(err))
		return err
	}

	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(&key, &expiration)
		if err != nil {
			l.Debug("error scaning rows", zap.Error(err))
			return err
		}

		if d.Expired(expiration) {
			err := d.Remove(ctx, key)
			if err != nil {
				l.Debug("error removing rows", zap.Error(err))
				return err
			}
		}
	}

	return nil
}