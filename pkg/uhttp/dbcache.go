package uhttp

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"

	_ "github.com/doug-martin/goqu/v9/dialect/sqlite3"
	_ "github.com/glebarez/go-sqlite"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

type DBCache struct {
	db *sql.DB
	mu sync.RWMutex
}

func NewDBCache(ctx context.Context) (*DBCache, error) {
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
	_, err = db.Exec("CREATE TABLE IF NOT EXISTS cache(id INTEGER PRIMARY KEY, key NVARCHAR, data BLOB)")
	if err != nil {
		l.Debug("error creating cache table", zap.Error(err))
		return &DBCache{}, err
	}

	return &DBCache{
		db: db,
	}, nil
}

func (d *DBCache) Get(ctx context.Context, key string) any {
	d.mu.RLock()
	defer d.mu.RUnlock()
	val, err := d.Select(ctx, key)
	if err != nil {
		return err
	}

	return val
}

func (d *DBCache) Set(ctx context.Context, key string, value any) error {
	d.mu.RLock()
	defer d.mu.RUnlock()
	err := d.Insert(ctx, key, value)
	if err != nil {
		return err
	}

	return nil
}

func (d *DBCache) Insert(ctx context.Context, key string, value any) error {
	l := ctxzap.Extract(ctx)
	bytes, err := json.Marshal(value)
	if err != nil {
		l.Debug("error marshaling data", zap.Error(err))
		return err
	}

	if ok, _ := d.Has(ctx, key); !ok {
		_, err = d.db.Exec("INSERT INTO cache(key, data) values(?, ?)", key, bytes)
		if err != nil {
			l.Debug("error inserting data", zap.Error(err))
			return err
		}
	}

	return nil
}

func (d *DBCache) Has(ctx context.Context, key string) (bool, error) {
	l := ctxzap.Extract(ctx)
	rows, err := d.db.Query("SELECT data FROM cache where key = ?", key)
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

func (d *DBCache) Select(ctx context.Context, key string) (string, error) {
	var data string
	l := ctxzap.Extract(ctx)
	rows, err := d.db.Query("SELECT data FROM cache where key = ?", key)
	if err != nil {
		l.Debug("error querying datatable", zap.Error(err))
		return "", err
	}

	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(&data)
		if err != nil {
			l.Debug("error scaning rows", zap.Error(err))
			return "", err
		}
	}

	return data, nil
}

func (d *DBCache) CloseDB(ctx context.Context) error {
	err := d.db.Close()
	if err != nil {
		ctxzap.Extract(ctx).Debug("error closing database", zap.Error(err))
		return err
	}

	return nil
}
