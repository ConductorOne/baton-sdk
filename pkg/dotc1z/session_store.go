package dotc1z

import (
	"context"
	"fmt"
	"strings"

	"github.com/doug-martin/goqu/v9"

	"github.com/conductorone/baton-sdk/pkg/types/sessions"
)

type SessionStore interface {
	sessions.SessionStore
}

var _ sessions.SessionStore = (*C1File)(nil)

const sessionStoreTableVersion = "1"
const sessionStoreTableName = "connector_sessions"
const sessionStoreTableSchema = `
CREATE TABLE IF NOT EXISTS %s (
    id integer primary key,
	sync_id text NOT NULL,
	key TEXT NOT NULL,
	value BLOB NOT NULL
);
create unique index if not exists %s on %s (sync_id, key);`

var sessionStore = (*sessionStoreTable)(nil)

type sessionStoreTable struct{}

func escapeLike(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `%`, `\%`)
	s = strings.ReplaceAll(s, `_`, `\_`)
	return s
}

func (r *sessionStoreTable) Name() string {
	return fmt.Sprintf("v%s_%s", r.Version(), sessionStoreTableName)
}

func (r *sessionStoreTable) Version() string {
	return sessionStoreTableVersion
}

func (r *sessionStoreTable) Schema() (string, []interface{}) {
	return sessionStoreTableSchema, []interface{}{
		r.Name(),
		fmt.Sprintf("idx_session_store_sync_key_v%s", r.Version()),
		r.Name(),
	}
}

func (r *sessionStoreTable) Migrations(ctx context.Context, db *goqu.Database) error {
	return nil
}

func applyBag(ctx context.Context, opt ...sessions.SessionStoreOption) (*sessions.SessionStoreBag, error) {
	bag := &sessions.SessionStoreBag{}
	for _, o := range opt {
		err := o(ctx, bag)
		if err != nil {
			return nil, fmt.Errorf("error applying session option: %w", err)
		}
	}
	if bag.SyncID == "" {
		return nil, fmt.Errorf("sync id is required")
	}
	return bag, nil
}

// Get implements types.SessionCache.
func (c *C1File) Get(ctx context.Context, key string, opt ...sessions.SessionStoreOption) ([]byte, bool, error) {
	bag, err := applyBag(ctx, opt...)
	if err != nil {
		return nil, false, fmt.Errorf("error applying session option: %w", err)
	}

	q := c.db.From(sessionStore.Name()).Prepared(true)
	q = q.Select("value")
	q = q.Where(goqu.C("sync_id").Eq(bag.SyncID))
	q = q.Where(goqu.C("key").Eq(bag.Prefix + key))

	sql, params, err := q.ToSQL()
	if err != nil {
		return nil, false, fmt.Errorf("error getting session: %w", err)
	}

	rows, err := c.db.QueryContext(ctx, sql, params...)
	if err != nil {
		return nil, false, fmt.Errorf("error getting session: %w", err)
	}
	defer rows.Close()

	var ret []byte
	found := false

	for rows.Next() {
		err = rows.Scan(&ret)
		if err != nil {
			return nil, false, fmt.Errorf("error scanning session: %w", err)
		}
		found = true
	}

	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("error getting data from session: %w", err)
	}

	return ret, found, nil
}

// Set implements types.SessionStore.
func (c *C1File) Set(ctx context.Context, key string, value []byte, opt ...sessions.SessionStoreOption) error {
	bag, err := applyBag(ctx, opt...)
	if err != nil {
		return fmt.Errorf("error applying session option: %w", err)
	}

	// Use goqu's OnConflict for upsert behavior
	q := c.db.Insert(sessionStore.Name()).Prepared(true)
	q = q.Rows(goqu.Record{
		"sync_id": bag.SyncID,
		"key":     bag.Prefix + key,
		"value":   value,
	})
	q = q.OnConflict(goqu.DoUpdate("sync_id, key", goqu.C("value").Set(value)))

	sql, params, err := q.ToSQL()
	if err != nil {
		return fmt.Errorf("error setting session: %w", err)
	}

	_, err = c.db.ExecContext(ctx, sql, params...)
	if err != nil {
		return fmt.Errorf("error setting session: %w", err)
	}

	return nil
}

// SetMany implements types.SessionStore.
func (c *C1File) SetMany(ctx context.Context, values map[string][]byte, opt ...sessions.SessionStoreOption) error {
	bag, err := applyBag(ctx, opt...)
	if err != nil {
		return fmt.Errorf("error applying session option: %w", err)
	}

	if len(values) == 0 {
		return nil
	}

	// Build batch insert
	var rows []interface{}
	for key, value := range values {
		rows = append(rows, goqu.Record{
			"sync_id": bag.SyncID,
			"key":     bag.Prefix + key,
			"value":   value,
		})
	}

	q := c.db.Insert(sessionStore.Name()).Prepared(true)
	q = q.Rows(rows...)
	q = q.OnConflict(goqu.DoUpdate("sync_id, key", goqu.C("value").Set(goqu.I("EXCLUDED.value"))))

	sql, params, err := q.ToSQL()
	if err != nil {
		return fmt.Errorf("error setting many sessions: %w", err)
	}

	_, err = c.db.ExecContext(ctx, sql, params...)
	if err != nil {
		return fmt.Errorf("error setting many sessions: %w", err)
	}

	return nil
}

// Delete implements types.SessionStore.
func (c *C1File) Delete(ctx context.Context, key string, opt ...sessions.SessionStoreOption) error {
	bag, err := applyBag(ctx, opt...)
	if err != nil {
		return fmt.Errorf("error applying session option: %w", err)
	}

	q := c.db.Delete(sessionStore.Name()).Prepared(true)
	q = q.Where(goqu.C("sync_id").Eq(bag.SyncID))
	q = q.Where(goqu.C("key").Eq(bag.Prefix + key))

	sql, params, err := q.ToSQL()
	if err != nil {
		return fmt.Errorf("error deleting session: %w", err)
	}

	_, err = c.db.ExecContext(ctx, sql, params...)
	if err != nil {
		return fmt.Errorf("error deleting session: %w", err)
	}

	return nil
}

// Clear implements types.SessionStore.
func (c *C1File) Clear(ctx context.Context, opt ...sessions.SessionStoreOption) error {
	bag, err := applyBag(ctx, opt...)
	if err != nil {
		return fmt.Errorf("error applying session option: %w", err)
	}

	q := c.db.Delete(sessionStore.Name()).Prepared(true)
	q = q.Where(goqu.C("sync_id").Eq(bag.SyncID))

	if bag.Prefix != "" {
		q = q.Where(goqu.C("key").Like(escapeLike(bag.Prefix) + "%"))
	}

	sql, params, err := q.ToSQL()
	if err != nil {
		return fmt.Errorf("error clearing sessions: %w", err)
	}

	_, err = c.db.ExecContext(ctx, sql, params...)
	if err != nil {
		return fmt.Errorf("error clearing sessions: %w", err)
	}

	return nil
}

// GetMany implements types.SessionStore.
func (c *C1File) GetMany(ctx context.Context, keys []string, opt ...sessions.SessionStoreOption) (map[string][]byte, error) {
	bag, err := applyBag(ctx, opt...)
	if err != nil {
		return nil, fmt.Errorf("error applying session option: %w", err)
	}

	if len(keys) == 0 {
		return make(map[string][]byte), nil
	}
	prefixedKeys := make([]string, len(keys))
	if bag.Prefix == "" {
		prefixedKeys = keys
	} else {
		for i, key := range keys {
			prefixedKeys[i] = bag.Prefix + key
		}
	}

	q := c.db.From(sessionStore.Name()).Prepared(true)
	q = q.Select("key", "value")
	q = q.Where(goqu.C("sync_id").Eq(bag.SyncID))
	q = q.Where(goqu.C("key").In(prefixedKeys))

	sql, params, err := q.ToSQL()
	if err != nil {
		return nil, fmt.Errorf("error getting many sessions: %w", err)
	}

	rows, err := c.db.QueryContext(ctx, sql, params...)
	if err != nil {
		return nil, fmt.Errorf("error getting many sessions: %w", err)
	}
	defer rows.Close()

	result := make(map[string][]byte)
	for rows.Next() {
		var key string
		var value []byte
		err = rows.Scan(&key, &value)
		if err != nil {
			return nil, fmt.Errorf("error scanning session: %w", err)
		}
		// Remove prefix from key to return original key
		if bag.Prefix != "" && len(key) >= len(bag.Prefix) && key[:len(bag.Prefix)] == bag.Prefix {
			key = key[len(bag.Prefix):]
		}
		result[key] = value
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error getting data from session: %w", err)
	}

	return result, nil
}

// GetAll implements types.SessionStore.
func (c *C1File) GetAll(ctx context.Context, pageToken string, opt ...sessions.SessionStoreOption) (map[string][]byte, string, error) {
	bag, err := applyBag(ctx, opt...)
	if err != nil {
		return nil, "", fmt.Errorf("error applying session option: %w", err)
	}

	q := c.db.From(sessionStore.Name()).Prepared(true).
		Select("key", "value").
		Where(goqu.C("sync_id").Eq(bag.SyncID)).
		Order(goqu.C("key").Asc()).
		Limit(101)

	if bag.Prefix != "" {
		q = q.Where(goqu.C("key").Like(escapeLike(bag.Prefix) + "%"))
	}

	if pageToken != "" {
		q = q.Where(goqu.C("key").Gte(pageToken))
	}

	sql, params, err := q.ToSQL()

	if err != nil {
		return nil, "", fmt.Errorf("error getting all sessions: %w", err)
	}

	rows, err := c.db.QueryContext(ctx, sql, params...)
	if err != nil {
		return nil, "", fmt.Errorf("error getting all sessions: %w", err)
	}
	defer rows.Close()

	result := make(map[string][]byte)
	nextPageToken := ""
	i := 0
	for rows.Next() {
		var key string
		var value []byte
		err = rows.Scan(&key, &value)
		if err != nil {
			return nil, "", fmt.Errorf("error scanning session: %w", err)
		}
		i++
		if i > 100 {
			nextPageToken = key
			break
		}
		// Remove prefix from key to return original key
		if bag.Prefix != "" && len(key) >= len(bag.Prefix) && key[:len(bag.Prefix)] == bag.Prefix {
			key = key[len(bag.Prefix):]
		}
		result[key] = value
	}

	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("error getting data from session: %w", err)
	}

	return result, nextPageToken, nil
}
