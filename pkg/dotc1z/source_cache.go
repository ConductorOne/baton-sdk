package dotc1z

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/sourcecache"
	"github.com/doug-martin/goqu/v9"
)

const sourceCacheEntriesTableVersion = "1"
const sourceCacheEntriesTableName = "source_cache_entries"
const sourceCacheEntriesTableSchema = `
create table if not exists %s (
    id integer primary key,
    sync_id text not null,
    row_kind text not null,
    source_scope_hash text not null,
    source_cache_key text not null,
    source_etag text not null,
    discovered_at datetime not null
);
create unique index if not exists %s on %s (sync_id, row_kind, source_scope_hash);
create unique index if not exists %s on %s (sync_id, row_kind, source_cache_key);`

var sourceCacheEntries = (*sourceCacheEntriesTable)(nil)

type sourceCacheEntriesTable struct{}

func (s *sourceCacheEntriesTable) Name() string {
	return fmt.Sprintf("v%s_%s", s.Version(), sourceCacheEntriesTableName)
}

func (s *sourceCacheEntriesTable) Version() string {
	return sourceCacheEntriesTableVersion
}

func (s *sourceCacheEntriesTable) Schema() (string, []any) {
	return sourceCacheEntriesTableSchema, []any{
		s.Name(),
		fmt.Sprintf("idx_source_cache_entries_sync_kind_scope_v%s", s.Version()),
		s.Name(),
		fmt.Sprintf("idx_source_cache_entries_sync_kind_key_v%s", s.Version()),
		s.Name(),
	}
}

func (s *sourceCacheEntriesTable) Migrations(context.Context, *goqu.Database) (bool, error) {
	return false, nil
}

type SourceCacheStore interface {
	LookupPreviousSourceCache(ctx context.Context, rowKind sourcecache.RowKind, syncID string, scopeHashHex string) (sourcecache.Entry, bool, error)
	PutSourceCacheEntry(ctx context.Context, rowKind sourcecache.RowKind, syncID string, scopeHashHex string, sourceCacheKey string, sourceETag string, discoveredAt time.Time) error
	// Put*WithKey write normal source rows while stamping sourceCacheKey into
	// their source_cache_key column. The rows remain part of their usual tables;
	// source-cache metadata only partitions them for future replay.
	PutResourcesWithKey(ctx context.Context, sourceCacheKey string, resources ...*v2.Resource) error
	PutEntitlementsWithKey(ctx context.Context, sourceCacheKey string, entitlements ...*v2.Entitlement) error
	PutGrantsWithKey(ctx context.Context, sourceCacheKey string, grants ...*v2.Grant) error
	ReplaySourceCacheRows(ctx context.Context, from C1ZStore, rowKind sourcecache.RowKind, fromSyncID string, toSyncID string, sourceCacheKey string, discoveredAt time.Time) (int64, error)
	ReplaySourceCacheEntry(ctx context.Context, from C1ZStore, rowKind sourcecache.RowKind, fromSyncID string, toSyncID string, sourceCacheKey string, discoveredAt time.Time) error
	// ReplaySourceCache atomically copies both the rows for `rowKind` and the
	// matching source_cache_entries metadata row from `from` to this store
	// in a single BeginTx/Commit. Halves the per-page commit count compared
	// to calling ReplaySourceCacheRows + ReplaySourceCacheEntry separately,
	// and the two writes become atomic so a crash mid-replay can't leave
	// the rows in place without a matching entry (which would make the next
	// sync see a phantom hit). Requires AttachExternalSource when `from` is
	// a different store.
	ReplaySourceCache(ctx context.Context, from C1ZStore, rowKind sourcecache.RowKind, fromSyncID string, toSyncID string, sourceCacheKey string, discoveredAt time.Time) (int64, error)
	// AttachExternalSource runs ATTACH '<path>' AS source_cache_ref against
	// this store's single pool connection so subsequent cross-store
	// ReplaySourceCacheRows / ReplaySourceCacheEntry calls can copy rows with
	// a plain INSERT INTO main.X SELECT FROM source_cache_ref.X. Idempotent;
	// a no-op when `from` is the same C1File. Must be paired with
	// DetachExternalSource before the store is closed.
	AttachExternalSource(ctx context.Context, from C1ZStore) error
	// DetachExternalSource runs DETACH source_cache_ref if a prior
	// AttachExternalSource succeeded. Idempotent.
	DetachExternalSource(ctx context.Context) error
}

const sourceCacheReplaySchema = "source_cache_ref"

func sourceCacheColumnMigration(ctx context.Context, db *goqu.Database, tableName string, indexName string) (bool, error) {
	migrated := false
	if _, err := db.ExecContext(ctx, fmt.Sprintf(
		"alter table %s add column source_cache_key text not null default ''",
		tableName,
	)); err != nil {
		if !isAlreadyExistsError(err) {
			return false, err
		}
	} else {
		migrated = true
	}

	if _, err := db.ExecContext(ctx, fmt.Sprintf(
		"create index if not exists %s on %s (sync_id, source_cache_key) where source_cache_key != ''",
		indexName,
		tableName,
	)); err != nil {
		return false, err
	}

	return migrated, nil
}

type c1FileSourceCache struct{ c *C1File }

func (s c1FileSourceCache) LookupPreviousSourceCache(ctx context.Context, rowKind sourcecache.RowKind, syncID string, scopeHashHex string) (sourcecache.Entry, bool, error) {
	if err := sourcecache.ValidateRowKind(rowKind); err != nil {
		return sourcecache.Entry{}, false, err
	}
	if err := sourcecache.ValidateScopeHash(scopeHashHex); err != nil {
		return sourcecache.Entry{}, false, err
	}

	var key string
	var etag string
	err := s.c.db.QueryRowContext(ctx, fmt.Sprintf(
		`select source_cache_key, source_etag from %s where sync_id = ? and row_kind = ? and source_scope_hash = ?`,
		sourceCacheEntries.Name(),
	), syncID, string(rowKind), scopeHashHex).Scan(&key, &etag)
	if err != nil {
		if err == sql.ErrNoRows {
			return sourcecache.Entry{}, false, nil
		}
		return sourcecache.Entry{}, false, err
	}
	return sourcecache.Entry{Key: key, ETag: etag}, true, nil
}

func (s c1FileSourceCache) PutSourceCacheEntry(
	ctx context.Context,
	rowKind sourcecache.RowKind,
	syncID string,
	scopeHashHex string,
	sourceCacheKey string,
	sourceETag string,
	discoveredAt time.Time,
) error {
	if s.c.readOnly {
		return ErrReadOnly
	}
	if err := sourcecache.ValidateRowKind(rowKind); err != nil {
		return err
	}
	keyScopeHash, keyETag, err := sourcecache.ParseKey(sourceCacheKey)
	if err != nil {
		return err
	}
	if keyScopeHash != scopeHashHex {
		return fmt.Errorf("source cache key scope hash %q does not match entry scope hash %q", keyScopeHash, scopeHashHex)
	}
	if keyETag != sourceETag {
		return fmt.Errorf("source cache key etag does not match entry etag")
	}

	ds := s.c.db.Insert(sourceCacheEntries.Name()).Rows(goqu.Record{
		"sync_id":           syncID,
		"row_kind":          string(rowKind),
		"source_scope_hash": scopeHashHex,
		"source_cache_key":  sourceCacheKey,
		"source_etag":       sourceETag,
		"discovered_at":     discoveredAt.Format("2006-01-02 15:04:05.999999999"),
	}).OnConflict(goqu.DoUpdate("sync_id, row_kind, source_scope_hash", goqu.Record{
		"source_cache_key": goqu.I("EXCLUDED.source_cache_key"),
		"source_etag":      goqu.I("EXCLUDED.source_etag"),
		"discovered_at":    goqu.I("EXCLUDED.discovered_at"),
	})).Prepared(true)
	query, args, err := ds.ToSQL()
	if err != nil {
		return err
	}
	if _, err := s.c.db.ExecContext(ctx, query, args...); err != nil {
		return err
	}
	s.c.dbUpdated = true
	return nil
}

func (s c1FileSourceCache) PutResourcesWithKey(ctx context.Context, sourceCacheKey string, resourcesToPut ...*v2.Resource) error {
	return s.c.putResourcesInternal(ctx, func(ctx context.Context, c *C1File, tableName string, extractFields func(*v2.Resource) (goqu.Record, error), resources ...*v2.Resource) error {
		return bulkPutConnectorObjectWithSourceCacheKey(ctx, c, tableName, sourceCacheKey, extractFields, resources...)
	}, resourcesToPut...)
}

func (s c1FileSourceCache) PutEntitlementsWithKey(ctx context.Context, sourceCacheKey string, entitlementsToPut ...*v2.Entitlement) error {
	return s.c.putEntitlementsInternal(ctx, func(ctx context.Context, c *C1File, tableName string, extractFields func(*v2.Entitlement) (goqu.Record, error), entitlements ...*v2.Entitlement) error {
		return bulkPutConnectorObjectWithSourceCacheKey(ctx, c, tableName, sourceCacheKey, extractFields, entitlements...)
	}, entitlementsToPut...)
}

func (s c1FileSourceCache) PutGrantsWithKey(ctx context.Context, sourceCacheKey string, grantsToPut ...*v2.Grant) error {
	return s.c.upsertGrantsWithSourceCacheKey(ctx, sourceCacheKey, grantsToPut...)
}

func (s c1FileSourceCache) ReplaySourceCacheRows(
	ctx context.Context,
	from C1ZStore,
	rowKind sourcecache.RowKind,
	fromSyncID string,
	toSyncID string,
	sourceCacheKey string,
	discoveredAt time.Time,
) (int64, error) {
	tableName, err := sourceCacheTableForRowKind(rowKind)
	if err != nil {
		return 0, err
	}
	return s.replaySourceCacheTable(ctx, from, tableName, fromSyncID, toSyncID, sourceCacheKey, discoveredAt)
}

func (s c1FileSourceCache) ReplaySourceCacheEntry(
	ctx context.Context,
	from C1ZStore,
	rowKind sourcecache.RowKind,
	fromSyncID string,
	toSyncID string,
	sourceCacheKey string,
	discoveredAt time.Time,
) error {
	if err := sourcecache.ValidateRowKind(rowKind); err != nil {
		return err
	}
	rows, err := s.replaySourceCacheTable(ctx, from, sourceCacheEntries.Name(), fromSyncID, toSyncID, sourceCacheKey, discoveredAt,
		func(sourceAlias string) string {
			return fmt.Sprintf(" and %s.row_kind = ?", sourceAlias)
		},
		string(rowKind),
	)
	if err != nil {
		return err
	}
	if rows == 0 {
		return fmt.Errorf("source cache entry not found for sync_id %q, row_kind %q, source_cache_key %q", fromSyncID, rowKind, sourceCacheKey)
	}
	return nil
}

func (s c1FileSourceCache) ReplaySourceCache(
	ctx context.Context,
	from C1ZStore,
	rowKind sourcecache.RowKind,
	fromSyncID string,
	toSyncID string,
	sourceCacheKey string,
	discoveredAt time.Time,
) (int64, error) {
	if s.c == nil || s.c.db == nil {
		return 0, ErrDbNotOpen
	}
	if s.c.readOnly {
		return 0, ErrReadOnly
	}
	if err := sourcecache.ValidateRowKind(rowKind); err != nil {
		return 0, err
	}
	tableName, err := sourceCacheTableForRowKind(rowKind)
	if err != nil {
		return 0, err
	}
	sourceFile, ok := AsSQLiteStore(from)
	if !ok {
		return 0, fmt.Errorf("source cache replay requires a sqlite-backed c1z store")
	}
	sourceSchema := "main"
	if sourceFile != s.c {
		if !s.c.sourceCacheRefAttached {
			return 0, fmt.Errorf("source cache replay requested for external store but AttachExternalSource has not been called")
		}
		sourceSchema = sourceCacheReplaySchema
	}

	// Wrap the rows + entry replay in a single transaction so SQLite emits
	// one fsync at commit instead of two. We only ever write to `main`
	// (source_cache_ref is read), so no master journal is needed even
	// though the SELECT pulls from an attached database. With c.db's pool
	// capped at one connection, BeginTx pins to the same physical SQLite
	// connection that holds the source_cache_ref attachment.
	tx, err := s.c.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	rows, err := s.replaySourceCacheTableSQLExec(ctx, tx, tableName, sourceSchema, "src", fromSyncID, toSyncID, sourceCacheKey, discoveredAt)
	if err != nil {
		return 0, err
	}
	entryRows, err := s.replaySourceCacheTableSQLExec(ctx, tx, sourceCacheEntries.Name(), sourceSchema, "src", fromSyncID, toSyncID, sourceCacheKey, discoveredAt,
		func(sourceAlias string) string {
			return fmt.Sprintf(" and %s.row_kind = ?", sourceAlias)
		},
		string(rowKind),
	)
	if err != nil {
		return 0, err
	}
	if entryRows == 0 {
		return 0, fmt.Errorf("source cache entry not found for sync_id %q, row_kind %q, source_cache_key %q", fromSyncID, rowKind, sourceCacheKey)
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	committed = true
	s.c.dbUpdated = true
	return rows, nil
}

type replayWhereFn func(sourceAlias string) string

type sqlExecQuerier interface {
	sqlQuerier
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func (s c1FileSourceCache) AttachExternalSource(ctx context.Context, from C1ZStore) error {
	if s.c == nil || s.c.db == nil {
		return ErrDbNotOpen
	}
	if s.c.readOnly {
		return ErrReadOnly
	}
	sourceFile, ok := AsSQLiteStore(from)
	if !ok {
		return fmt.Errorf("source cache attach requires a sqlite-backed c1z store")
	}
	// Same store: every replay query just uses the `main` schema, no ATTACH
	// is needed. Mark not-attached so DetachExternalSource is a no-op.
	if sourceFile == s.c {
		return nil
	}
	if s.c.sourceCacheRefAttached {
		return nil
	}
	// We rely on the writable C1File pool being capped at 1 connection (see
	// NewC1File): the ATTACH issued through c.db lands on that single
	// connection and persists for every subsequent c.db call until
	// DetachExternalSource runs. This is what lets cross-store replay run
	// without per-page ATTACH/DETACH overhead and without holding a separate
	// *sql.Conn outside the pool.
	attachPath := strings.ReplaceAll(sourceFile.dbFilePath, "'", "''")
	if _, err := s.c.db.ExecContext(ctx, fmt.Sprintf("ATTACH '%s' AS %s", attachPath, sourceCacheReplaySchema)); err != nil {
		return fmt.Errorf("source cache attach: %w", err)
	}
	s.c.sourceCacheRefAttached = true
	return nil
}

func (s c1FileSourceCache) DetachExternalSource(ctx context.Context) error {
	if s.c == nil || s.c.db == nil {
		return nil
	}
	if !s.c.sourceCacheRefAttached {
		return nil
	}
	if _, err := s.c.db.ExecContext(ctx, "DETACH "+sourceCacheReplaySchema); err != nil {
		return fmt.Errorf("source cache detach: %w", err)
	}
	s.c.sourceCacheRefAttached = false
	return nil
}

func (s c1FileSourceCache) replaySourceCacheTable(
	ctx context.Context,
	from C1ZStore,
	tableName string,
	fromSyncID string,
	toSyncID string,
	sourceCacheKey string,
	discoveredAt time.Time,
	extraWhereAndArgs ...any,
) (int64, error) {
	if s.c.readOnly {
		return 0, ErrReadOnly
	}
	sourceFile, ok := AsSQLiteStore(from)
	if !ok {
		return 0, fmt.Errorf("source cache replay requires a sqlite-backed c1z store")
	}

	sourceSchema := "main"
	if sourceFile != s.c {
		if !s.c.sourceCacheRefAttached {
			return 0, fmt.Errorf("source cache replay requested for external store but AttachExternalSource has not been called")
		}
		sourceSchema = sourceCacheReplaySchema
	}

	rows, err := s.replaySourceCacheTableSQLExec(ctx, s.c.db, tableName, sourceSchema, "src", fromSyncID, toSyncID, sourceCacheKey, discoveredAt, extraWhereAndArgs...)
	if err != nil {
		return 0, err
	}
	s.c.dbUpdated = true
	return rows, nil
}

// cachedTableColumns returns the column list for `tableName` on the local
// `main` schema, memoizing the PRAGMA table_info round-trip on the C1File so
// per-page replays don't pay for it every time. We only memoize main-schema
// tables because the destination's schema is fixed for the life of the
// C1File; the attached source_cache_ref schema is also stable but exposed
// here for completeness via the schema key.
func (s c1FileSourceCache) cachedTableColumns(ctx context.Context, q sqlQuerier, schema, tableName string) ([]string, error) {
	if s.c == nil {
		return cloneTableColumns(ctx, q, tableName)
	}
	cacheKey := schema + "." + tableName
	s.c.tableColumnCacheMu.Lock()
	cols, ok := s.c.tableColumnCache[cacheKey]
	s.c.tableColumnCacheMu.Unlock()
	if ok {
		return cols, nil
	}
	cols, err := cloneTableColumns(ctx, q, tableName)
	if err != nil {
		return nil, err
	}
	s.c.tableColumnCacheMu.Lock()
	s.c.tableColumnCache[cacheKey] = cols
	s.c.tableColumnCacheMu.Unlock()
	return cols, nil
}

func (s c1FileSourceCache) replaySourceCacheTableSQLExec(
	ctx context.Context,
	q sqlExecQuerier,
	tableName string,
	sourceSchema string,
	sourceAlias string,
	fromSyncID string,
	toSyncID string,
	sourceCacheKey string,
	discoveredAt time.Time,
	extraWhereAndArgs ...any,
) (int64, error) {
	// The destination table on `main` is what we're inserting into; its
	// column list determines both INSERT column list and the SELECT
	// projection. PRAGMA against the local destination schema is the only
	// thing we cache.
	columns, err := s.cachedTableColumns(ctx, q, "main", tableName)
	if err != nil {
		return 0, err
	}

	columnList := make([]string, 0, len(columns))
	selectList := make([]string, 0, len(columns))
	for _, col := range columns {
		qcol := quoteIdentifier(col)
		columnList = append(columnList, qcol)
		switch col {
		case "sync_id":
			selectList = append(selectList, "? as "+qcol)
		case "discovered_at":
			selectList = append(selectList, "? as "+qcol)
		default:
			selectList = append(selectList, sourceAlias+"."+qcol)
		}
	}

	args := []any{toSyncID, discoveredAt.Format("2006-01-02 15:04:05.999999999"), fromSyncID, sourceCacheKey}
	extraWhere := ""
	if len(extraWhereAndArgs) > 0 {
		var whereFn replayWhereFn
		switch fn := extraWhereAndArgs[0].(type) {
		case replayWhereFn:
			whereFn = fn
		case func(string) string:
			whereFn = fn
		default:
			return 0, fmt.Errorf("invalid source cache replay where callback")
		}
		extraWhere = whereFn(sourceAlias)
		args = append(args, extraWhereAndArgs[1:]...)
	}

	// #nosec G201 -- identifiers are internal table/column names selected from fixed row-kind mappings and PRAGMA-validated columns.
	query := fmt.Sprintf(
		`insert or replace into main.%s (%s)
select %s
from %s.%s as %s
where %s.sync_id = ? and %s.source_cache_key = ?%s`,
		tableName,
		strings.Join(columnList, ", "),
		strings.Join(selectList, ", "),
		sourceSchema,
		tableName,
		sourceAlias,
		sourceAlias,
		sourceAlias,
		extraWhere,
	)
	result, err := q.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func sourceCacheTableForRowKind(rowKind sourcecache.RowKind) (string, error) {
	switch rowKind {
	case sourcecache.RowKindResources:
		return resources.Name(), nil
	case sourcecache.RowKindEntitlements:
		return entitlements.Name(), nil
	case sourcecache.RowKindGrants:
		return grants.Name(), nil
	default:
		return "", fmt.Errorf("invalid source cache row kind: %q", rowKind)
	}
}

func sourceCacheTableHasRowColumn(ctx context.Context, c *C1File, tableName string) bool {
	if tableName != resources.Name() && tableName != entitlements.Name() && tableName != grants.Name() {
		return false
	}
	if c == nil || c.db == nil {
		return false
	}
	rows, err := c.db.QueryContext(ctx, fmt.Sprintf("select 1 from pragma_table_info('%s') where name='source_cache_key' limit 1", tableName))
	if err != nil {
		return false
	}
	defer rows.Close()
	return rows.Next()
}
