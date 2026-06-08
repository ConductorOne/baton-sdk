package dotc1z

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/doug-martin/goqu/v9"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/segmentio/ksuid"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	sqlite "modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"

	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/uotel"
)

const syncRunsTableVersion = "1"
const syncRunsTableName = "sync_runs"

// deleteSyncRunBatchSize bounds each DELETE in DeleteSyncRun so progress
// commits incrementally and survives an interrupted/retried cleanup.
// Var (not const) only so tests can shrink it to exercise the multi-batch
// loop without inserting 50k rows; not tuned at runtime.
var deleteSyncRunBatchSize = 50000

const syncRunsTableSchema = `
create table if not exists %s (
    id integer primary key,
    sync_id text not null,
    started_at datetime not null,
    ended_at datetime,
    sync_token text not null,
    sync_type text not null default 'full',
    parent_sync_id text not null default '',
    linked_sync_id text not null default '',
    supports_diff integer not null default 0,
    grants_backfilled integer not null default 0,
		stats text
);
create unique index if not exists %s on %s (sync_id);`

var syncRuns = (*syncRunsTable)(nil)

type syncRunsTable struct{}

func (r *syncRunsTable) Name() string {
	return fmt.Sprintf("v%s_%s", r.Version(), syncRunsTableName)
}

func (r *syncRunsTable) Version() string {
	return syncRunsTableVersion
}

func (r *syncRunsTable) Schema() (string, []interface{}) {
	return syncRunsTableSchema, []interface{}{
		r.Name(),
		fmt.Sprintf("idx_sync_runs_sync_id_v%s", r.Version()),
		r.Name(),
	}
}

func (r *syncRunsTable) Migrations(ctx context.Context, db *goqu.Database) (bool, error) {
	migrated := false

	// Check if sync_type column exists
	var syncTypeExists int
	err := db.QueryRowContext(ctx, fmt.Sprintf("select count(*) from pragma_table_info('%s') where name='sync_type'", r.Name())).Scan(&syncTypeExists)
	if err != nil {
		return false, err
	}
	if syncTypeExists == 0 {
		_, err = db.ExecContext(ctx, fmt.Sprintf("alter table %s add column sync_type text not null default 'full'", r.Name()))
		if err != nil {
			return false, err
		}
		migrated = true
	}

	// Check if parent_sync_id column exists
	var parentSyncIDExists int
	err = db.QueryRowContext(ctx, fmt.Sprintf("select count(*) from pragma_table_info('%s') where name='parent_sync_id'", r.Name())).Scan(&parentSyncIDExists)
	if err != nil {
		return false, err
	}
	if parentSyncIDExists == 0 {
		_, err = db.ExecContext(ctx, fmt.Sprintf("alter table %s add column parent_sync_id text not null default ''", r.Name()))
		if err != nil {
			return false, err
		}
		migrated = true
	}

	// Check if linked_sync_id column exists
	var linkedSyncIDExists int
	err = db.QueryRowContext(ctx, fmt.Sprintf("select count(*) from pragma_table_info('%s') where name='linked_sync_id'", r.Name())).Scan(&linkedSyncIDExists)
	if err != nil {
		return false, err
	}
	if linkedSyncIDExists == 0 {
		_, err = db.ExecContext(ctx, fmt.Sprintf("alter table %s add column linked_sync_id text not null default ''", r.Name()))
		if err != nil {
			return false, err
		}
		migrated = true
	}

	// Add supports_diff column if missing (for older files).
	_, err = db.ExecContext(ctx, fmt.Sprintf("alter table %s add column supports_diff integer not null default 0", r.Name()))
	if err != nil {
		if !isAlreadyExistsError(err) {
			return false, err
		}
	} else {
		migrated = true
	}

	// Track whether grant expansion backfill has completed for this sync.
	_, err = db.ExecContext(ctx, fmt.Sprintf("alter table %s add column grants_backfilled integer not null default 0", r.Name()))
	if err != nil {
		if !isAlreadyExistsError(err) {
			return false, err
		}
	} else {
		migrated = true
	}

	// Add stats column so we can store the cached stats.
	_, err = db.ExecContext(ctx, fmt.Sprintf("alter table %s add column stats text", r.Name()))
	if err != nil {
		if !isAlreadyExistsError(err) {
			return false, err
		}
	} else {
		migrated = true
	}

	return migrated, nil
}

// getCachedViewSyncRun returns the cached sync run for read operations.
// This avoids N+1 queries when paginating through listConnectorObjects.
// The cache is invalidated when a sync starts or ends.
func (c *C1File) getCachedViewSyncRun(ctx context.Context) (*SyncRun, error) {
	ctx, span := tracer.Start(ctx, "C1File.getCachedViewSyncRun")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	c.cachedViewSyncMu.Lock()
	defer c.cachedViewSyncMu.Unlock()

	if c.cachedViewSyncRun != nil || c.cachedViewSyncErr != nil {
		return c.cachedViewSyncRun, c.cachedViewSyncErr
	}

	// First try to get a finished full sync
	c.cachedViewSyncRun, c.cachedViewSyncErr = c.getFinishedSync(ctx, 0, connectorstore.SyncTypeFull)
	if c.cachedViewSyncErr != nil {
		return c.cachedViewSyncRun, c.cachedViewSyncErr
	}

	// If no finished sync, try to get an unfinished one
	if c.cachedViewSyncRun == nil {
		c.cachedViewSyncRun, c.cachedViewSyncErr = c.getLatestUnfinishedSync(ctx, connectorstore.SyncTypeAny)
	}

	return c.cachedViewSyncRun, c.cachedViewSyncErr
}

// invalidateCachedViewSyncRun clears the cached sync run so it will be recomputed on next access.
func (c *C1File) invalidateCachedViewSyncRun() {
	c.cachedViewSyncMu.Lock()
	defer c.cachedViewSyncMu.Unlock()
	c.cachedViewSyncRun = nil
	c.cachedViewSyncErr = nil
}

func (c *C1File) getLatestUnfinishedSync(ctx context.Context, syncType connectorstore.SyncType) (*SyncRun, error) {
	ctx, span := tracer.Start(ctx, "C1File.getLatestUnfinishedSync")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	err = c.validateDb(ctx)
	if err != nil {
		return nil, err
	}

	// Don't resume syncs that started over a week ago
	oneWeekAgo := time.Now().AddDate(0, 0, -7)
	ret := &SyncRun{}
	q := c.db.From(syncRuns.Name())
	q = q.Select("sync_id", "started_at", "ended_at", "sync_token", "sync_type", "parent_sync_id", "linked_sync_id", "supports_diff", "stats")
	q = q.Where(goqu.C("ended_at").IsNull())
	q = q.Where(goqu.C("started_at").Gte(oneWeekAgo))
	q = q.Order(goqu.C("started_at").Desc())
	if syncType != connectorstore.SyncTypeAny {
		q = q.Where(goqu.C("sync_type").Eq(syncType))
	}
	q = q.Limit(1)

	query, args, err := q.ToSQL()
	if err != nil {
		return nil, err
	}

	row := c.db.QueryRowContext(ctx, query, args...)
	statsBytes := &[]byte{}
	err = row.Scan(&ret.ID, &ret.StartedAt, &ret.EndedAt, &ret.SyncToken, &ret.Type, &ret.ParentSyncID, &ret.LinkedSyncID, &ret.SupportsDiff, &statsBytes)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}

	ret.Stats = parseStats(ctx, statsBytes)

	return ret, nil
}

func (c *C1File) getFinishedSync(ctx context.Context, offset uint, syncType connectorstore.SyncType) (*SyncRun, error) {
	ctx, span := tracer.Start(ctx, "C1File.getFinishedSync")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	err = c.validateDb(ctx)
	if err != nil {
		return nil, err
	}

	// Validate syncType
	if !slices.Contains(connectorstore.AllSyncTypes, syncType) {
		return nil, status.Errorf(codes.InvalidArgument, "invalid sync type: %s", syncType)
	}

	ret := &SyncRun{}
	q := c.db.From(syncRuns.Name())
	q = q.Select("sync_id", "started_at", "ended_at", "sync_token", "sync_type", "parent_sync_id", "linked_sync_id", "supports_diff", "stats")
	q = q.Where(goqu.C("ended_at").IsNotNull())
	if syncType != connectorstore.SyncTypeAny {
		q = q.Where(goqu.C("sync_type").Eq(syncType))
	}
	// Tiebreak on sync_id when ended_at ties — Windows can have
	// coarser-than-nanosecond time resolution, so two adjacent
	// EndSync calls can produce identical ended_at strings. sync_ids
	// are KSUIDs (timestamp-sortable) so DESC picks the later one.
	q = q.Order(goqu.C("ended_at").Desc(), goqu.C("sync_id").Desc())
	q = q.Limit(1)

	if offset != 0 {
		q = q.Offset(offset)
	}

	query, args, err := q.ToSQL()
	if err != nil {
		return nil, err
	}

	row := c.db.QueryRowContext(ctx, query, args...)
	statsBytes := &[]byte{}
	err = row.Scan(&ret.ID, &ret.StartedAt, &ret.EndedAt, &ret.SyncToken, &ret.Type, &ret.ParentSyncID, &ret.LinkedSyncID, &ret.SupportsDiff, &statsBytes)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}

	ret.Stats = parseStats(ctx, statsBytes)

	return ret, nil
}

func parseStats(ctx context.Context, statsBytes *[]byte) *reader_v2.SyncStats {
	if statsBytes == nil || len(*statsBytes) == 0 {
		return nil
	}
	ret := &reader_v2.SyncStats{}
	err := json.Unmarshal(*statsBytes, ret)
	if err != nil {
		// Ignore error parsing stats. We will recalculate them if Stats() is called.
		ctxzap.Extract(ctx).Warn("error parsing stats", zap.Error(err))
		return nil
	}
	return ret
}

func (c *C1File) ListSyncRuns(ctx context.Context, pageToken string, pageSize uint32) ([]*SyncRun, string, error) {
	ctx, span := tracer.Start(ctx, "C1File.ListSyncRuns")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	err = c.validateDb(ctx)
	if err != nil {
		return nil, "", err
	}

	q := c.db.From(syncRuns.Name()).Prepared(true)
	q = q.Select("id", "sync_id", "started_at", "ended_at", "sync_token", "sync_type", "parent_sync_id", "linked_sync_id", "supports_diff", "stats")

	if pageToken != "" {
		q = q.Where(goqu.C("id").Gte(pageToken))
	}

	if pageSize > maxPageSize || pageSize <= 0 {
		pageSize = maxPageSize
	}

	q = q.Order(goqu.C("id").Asc())
	q = q.Limit(uint(pageSize + 1))

	var ret []*SyncRun

	query, args, err := q.ToSQL()
	if err != nil {
		return nil, "", err
	}

	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	var count uint32 = 0
	lastRow := 0
	for rows.Next() {
		count++
		if count > pageSize {
			break
		}
		statsBytes := &[]byte{}
		rowId := 0
		data := &SyncRun{}
		err := rows.Scan(&rowId, &data.ID, &data.StartedAt, &data.EndedAt, &data.SyncToken, &data.Type, &data.ParentSyncID, &data.LinkedSyncID, &data.SupportsDiff, &statsBytes)
		if err != nil {
			return nil, "", err
		}

		data.Stats = parseStats(ctx, statsBytes)
		lastRow = rowId
		ret = append(ret, data)
	}
	if rows.Err() != nil {
		return nil, "", rows.Err()
	}

	nextPageToken := ""
	if count > pageSize {
		nextPageToken = strconv.Itoa(lastRow + 1)
	}

	return ret, nextPageToken, nil
}

func (c *C1File) LatestSyncID(ctx context.Context, syncType connectorstore.SyncType) (string, error) {
	ctx, span := tracer.Start(ctx, "C1File.LatestSyncID")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	s, err := c.getFinishedSync(ctx, 0, syncType)
	if err != nil {
		return "", err
	}

	if s == nil {
		return "", nil
	}

	return s.ID, nil
}

func (c *C1File) ViewSync(ctx context.Context, syncID string) error {
	if c.currentSyncID != "" {
		return fmt.Errorf("cannot set view when sync is running")
	}

	c.viewSyncID = syncID

	return nil
}

func (c *C1File) PreviousSyncID(ctx context.Context, syncType connectorstore.SyncType) (string, error) {
	ctx, span := tracer.Start(ctx, "C1File.PreviousSyncID")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	s, err := c.getFinishedSync(ctx, 1, syncType)
	if err != nil {
		return "", err
	}

	if s == nil {
		return "", nil
	}

	return s.ID, nil
}

func (c *C1File) LatestFinishedSyncID(ctx context.Context, syncType connectorstore.SyncType) (string, error) {
	ctx, span := tracer.Start(ctx, "C1File.LatestFinishedSync")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	s, err := c.getFinishedSync(ctx, 0, syncType)
	if err != nil {
		return "", err
	}

	if s == nil {
		return "", nil
	}

	return s.ID, nil
}

func (c *C1File) getSync(ctx context.Context, syncID string) (*SyncRun, error) {
	ctx, span := tracer.Start(ctx, "C1File.getSync")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	err = c.validateDb(ctx)
	if err != nil {
		return nil, err
	}

	ret := &SyncRun{}

	q := c.db.From(syncRuns.Name())
	q = q.Select("sync_id", "started_at", "ended_at", "sync_token", "sync_type", "parent_sync_id", "linked_sync_id", "supports_diff", "stats")
	q = q.Where(goqu.C("sync_id").Eq(syncID))

	query, args, err := q.ToSQL()
	if err != nil {
		return nil, err
	}
	row := c.db.QueryRowContext(ctx, query, args...)
	var statsBytes *[]byte
	err = row.Scan(&ret.ID, &ret.StartedAt, &ret.EndedAt, &ret.SyncToken, &ret.Type, &ret.ParentSyncID, &ret.LinkedSyncID, &ret.SupportsDiff, &statsBytes)
	if err != nil {
		return nil, err
	}

	ret.Stats = parseStats(ctx, statsBytes)

	return ret, nil
}

func (c *C1File) getCurrentSync(ctx context.Context) (*SyncRun, error) {
	ctx, span := tracer.Start(ctx, "C1File.getCurrentSync")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	if c.currentSyncID == "" {
		return nil, fmt.Errorf("c1file: sync must be running to get current sync")
	}

	return c.getSync(ctx, c.currentSyncID)
}

func (c *C1File) SetCurrentSync(ctx context.Context, syncID string) error {
	ctx, span := tracer.Start(ctx, "C1File.SetCurrentSync")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	_, err = c.getSync(ctx, syncID)
	if err != nil {
		return err
	}

	c.currentSyncID = syncID
	return nil
}

func (c *C1File) CheckpointSync(ctx context.Context, syncToken string) error {
	ctx, span := tracer.Start(ctx, "C1File.CheckpointSync")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	if c.readOnly {
		return ErrReadOnly
	}

	err = c.validateSyncDb(ctx)
	if err != nil {
		return err
	}

	q := c.db.Update(syncRuns.Name())
	q = q.Set(goqu.Record{"sync_token": syncToken})
	q = q.Where(goqu.C("sync_id").Eq(c.currentSyncID))

	query, args, err := q.ToSQL()
	if err != nil {
		return err
	}

	_, err = c.db.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}

	c.dbUpdated = true

	return nil
}

func (c *C1File) ResumeSync(ctx context.Context, syncType connectorstore.SyncType, syncID string) (string, error) {
	ctx, span := tracer.Start(ctx, "C1File.ResumeSync")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	if c.currentSyncID != "" {
		if syncID == c.currentSyncID {
			return c.currentSyncID, nil
		}
		if syncID != "" {
			return "", status.Errorf(codes.FailedPrecondition, "current sync is %s, cannot resume %s", c.currentSyncID, syncID)
		}
	}

	if syncID != "" {
		syncRun, err := c.getSync(ctx, syncID)
		if err != nil {
			return "", err
		}
		if syncType != connectorstore.SyncTypeAny && syncRun.Type != syncType {
			return "", status.Errorf(codes.FailedPrecondition, "cannot resume sync (%s) when a different sync type (%s) is running", syncRun.Type, syncType)
		}
		if syncRun.EndedAt != nil {
			return "", status.Errorf(codes.FailedPrecondition, "cannot resume sync that has already ended")
		}
		c.currentSyncID = syncID
		return c.currentSyncID, nil
	}

	if c.currentSyncID != "" {
		syncRun, err := c.getSync(ctx, c.currentSyncID)
		if err != nil {
			return "", err
		}
		if syncType != connectorstore.SyncTypeAny && syncRun.Type != syncType {
			return "", status.Errorf(codes.FailedPrecondition, "cannot resume sync. current sync %s is type %s, cannot resume as type %s", syncRun.ID, syncRun.Type, syncType)
		}
		if syncRun.EndedAt != nil {
			return "", status.Errorf(codes.Internal, "current sync %s has already ended. this should never happen", syncRun.ID)
		}

		return c.currentSyncID, nil
	}

	syncRun, err := c.getLatestUnfinishedSync(ctx, syncType)
	if err != nil {
		return "", err
	}
	if syncRun == nil {
		return "", status.Errorf(codes.NotFound, "no unfinished sync found for type %s", syncType)
	}

	c.currentSyncID = syncRun.ID
	return c.currentSyncID, nil
}

// StartOrResumeSync checks if a sync is already running and resumes it if it is.
// If no sync is running, it starts a new sync.
// It returns the sync ID and a boolean indicating if a new sync was started.
func (c *C1File) StartOrResumeSync(ctx context.Context, syncType connectorstore.SyncType, syncID string) (string, bool, error) {
	ctx, span := tracer.Start(ctx, "C1File.StartOrResumeSync")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	resumedSyncID, err := c.ResumeSync(ctx, syncType, syncID)
	if err != nil {
		if status.Code(err) != codes.NotFound && !errors.Is(err, sql.ErrNoRows) {
			return "", false, err
		}
	} else {
		return resumedSyncID, false, nil
	}

	if syncID != "" {
		return "", false, status.Errorf(codes.NotFound, "no sync with id %s found to resume", syncID)
	}

	syncID, err = c.StartNewSync(ctx, syncType, "")
	if err != nil {
		return "", false, err
	}

	c.currentSyncID = syncID

	return c.currentSyncID, true, nil
}

// SetSyncID sets the current sync ID. This is only intended for testing.
func (c *C1File) SetSyncID(_ context.Context, syncID string) error {
	c.currentSyncID = syncID
	return nil
}

func (c *C1File) StartNewSync(ctx context.Context, syncType connectorstore.SyncType, parentSyncID string) (string, error) {
	ctx, span := tracer.Start(ctx, "C1File.StartNewSync")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	if c.currentSyncID != "" {
		cur, err := c.getSync(ctx, c.currentSyncID)
		if err != nil {
			return "", err
		}
		if cur != nil && cur.EndedAt == nil && cur.Type != syncType {
			return "", status.Errorf(codes.FailedPrecondition, "current sync (id %s) is type %s. cannot start %s", cur.ID, cur.Type, syncType)
		}
		return c.currentSyncID, nil
	}

	switch syncType {
	case connectorstore.SyncTypeFull:
		if parentSyncID != "" {
			return "", status.Errorf(codes.InvalidArgument, "parent sync id must be empty for full sync")
		}
	case connectorstore.SyncTypeResourcesOnly:
		if parentSyncID != "" {
			return "", status.Errorf(codes.InvalidArgument, "parent sync id must be empty for resources only sync")
		}
	case connectorstore.SyncTypePartial:
	case connectorstore.SyncTypePartialUpserts, connectorstore.SyncTypePartialDeletions:
		// Diff syncs carry the base sync as their parent; the linked
		// pairing (upserts ↔ deletions) is set separately via
		// SetSyncLink since the partner's id may not exist yet.
	case connectorstore.SyncTypeAny:
		return "", status.Errorf(codes.InvalidArgument, "sync cannot be started with SyncTypeAny")
	default:
		return "", status.Errorf(codes.InvalidArgument, "invalid sync type: %s", syncType)
	}

	syncID := ksuid.New().String()

	if err := c.insertSyncRun(ctx, syncID, syncType, parentSyncID); err != nil {
		return "", err
	}

	c.currentSyncID = syncID
	c.invalidateCachedViewSyncRun()

	return c.currentSyncID, nil
}

func (c *C1File) insertSyncRun(ctx context.Context, syncID string, syncType connectorstore.SyncType, parentSyncID string) error {
	return c.insertSyncRunWithLink(ctx, syncID, syncType, parentSyncID, "")
}

func (c *C1File) insertSyncRunWithLink(ctx context.Context, syncID string, syncType connectorstore.SyncType, parentSyncID string, linkedSyncID string) error {
	if c.readOnly {
		return ErrReadOnly
	}

	err := c.validateDb(ctx)
	if err != nil {
		return err
	}

	q := c.db.Insert(syncRuns.Name())
	q = q.Rows(goqu.Record{
		"sync_id":           syncID,
		"started_at":        time.Now().Format("2006-01-02 15:04:05.999999999"),
		"sync_token":        "",
		"sync_type":         syncType,
		"parent_sync_id":    parentSyncID,
		"linked_sync_id":    linkedSyncID,
		"grants_backfilled": 1, // New syncs do not require grants backfill.
	})

	query, args, err := q.ToSQL()
	if err != nil {
		return err
	}

	_, err = c.db.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}
	c.dbUpdated = true
	return nil
}

func (c *C1File) CurrentSyncStep(ctx context.Context) (string, error) {
	ctx, span := tracer.Start(ctx, "C1File.CurrentSyncStep")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	sr, err := c.getCurrentSync(ctx)
	if err != nil {
		return "", err
	}

	return sr.SyncToken, nil
}

// EndSync updates the current sync_run row with the end time, and removes any other objects that don't have the current sync ID.
func (c *C1File) EndSync(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "C1File.EndSync")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	err = c.validateSyncDb(ctx)
	if err != nil {
		return err
	}

	if err := c.endSyncRun(ctx, c.currentSyncID); err != nil {
		return err
	}

	c.currentSyncID = ""
	c.invalidateCachedViewSyncRun()

	return nil
}

func (c *C1File) endSyncRun(ctx context.Context, syncID string) error {
	q := c.db.Update(syncRuns.Name())
	q = q.Set(goqu.Record{
		"ended_at": time.Now().Format("2006-01-02 15:04:05.999999999"),
	})
	q = q.Where(goqu.C("sync_id").Eq(syncID))
	q = q.Where(goqu.C("ended_at").IsNull())

	query, args, err := q.ToSQL()
	if err != nil {
		return err
	}

	_, err = c.db.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}
	c.dbUpdated = true

	// Run stats to generate and save the cached stats.
	_, _, statsErr := c.stats(ctx, connectorstore.SyncTypeAny, syncID, true)
	if statsErr != nil {
		// Ignore stats error. We will recalculate them if Stats() is called.
		ctxzap.Extract(ctx).Warn("c1z: error calculating & saving stats",
			zap.Error(statsErr),
			zap.String("sync_id", syncID),
		)
	}

	return nil
}

// SetSupportsDiff marks the given sync as supporting diff operations.
// This indicates the sync has SQL-layer grant metadata (is_expandable) properly populated.
func (c *C1File) SetSupportsDiff(ctx context.Context, syncID string) error {
	ctx, span := tracer.Start(ctx, "C1File.SetSupportsDiff")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	if c.readOnly {
		return ErrReadOnly
	}

	q := c.db.Update(syncRuns.Name())
	q = q.Set(goqu.Record{
		"supports_diff": 1,
	})
	q = q.Where(goqu.C("sync_id").Eq(syncID))

	query, args, err := q.ToSQL()
	if err != nil {
		return err
	}

	_, err = c.db.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}
	c.dbUpdated = true

	return nil
}

// SetSyncLink sets the linked_sync_id of an existing sync run. Diff
// sync pairs (partial_upserts ↔ partial_deletions) reference each
// other bidirectionally; a writer rebuilding such a pair cannot supply
// the link at StartNewSync time because the partner's id is minted by
// the store, so the pairing is applied after both runs exist.
func (c *C1File) SetSyncLink(ctx context.Context, syncID string, linkedSyncID string) error {
	ctx, span := tracer.Start(ctx, "C1File.SetSyncLink")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	if c.readOnly {
		return ErrReadOnly
	}
	if syncID == "" {
		return status.Errorf(codes.InvalidArgument, "sync id is required")
	}

	q := c.db.Update(syncRuns.Name())
	q = q.Set(goqu.Record{
		"linked_sync_id": linkedSyncID,
	})
	q = q.Where(goqu.C("sync_id").Eq(syncID))

	query, args, err := q.ToSQL()
	if err != nil {
		return err
	}

	_, err = c.db.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}
	c.dbUpdated = true

	return nil
}

// When context deadline is exceeded, go-sqlite can return a SQLITE_INTERRUPT error.
// If that happens, wrapSqliteInterruptError wraps this error and returns context.DeadlineExceeded.
// This allows sync cleanup to return ErrSyncNotComplete and resume its work on the next run.
func wrapSqliteInterruptError(err error) error {
	if err == nil {
		return nil
	}

	sqliteErr := &sqlite.Error{}
	ok := errors.As(err, &sqliteErr)
	if ok && sqliteErr.Code() == sqlite3.SQLITE_INTERRUPT {
		return errors.Join(err, context.DeadlineExceeded)
	}

	return err
}

func (c *C1File) Cleanup(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "C1File.Cleanup")
	uotel.SetSyncIdentityAttrs(ctx, span)
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	l := ctxzap.Extract(ctx)

	if c.skipCleanup {
		l.Info("skip_cleanup option is set, skipping cleanup of old syncs")
		return nil
	}

	if CleanupSkippedByEnv() {
		l.Info("BATON_SKIP_CLEANUP is set, skipping cleanup of old syncs")
		return nil
	}

	err = c.validateDb(ctx)
	if err != nil {
		return err
	}

	var candidates []SyncRun
	pageToken := ""
	for {
		runs, nextPageToken, err := c.ListSyncRuns(ctx, pageToken, 100)
		if err != nil {
			return wrapSqliteInterruptError(err)
		}
		for _, sr := range runs {
			candidates = append(candidates, *sr)
		}
		if nextPageToken == "" {
			break
		}
		pageToken = nextPageToken
	}

	syncLimit := ResolveCleanupSyncLimit(c.syncLimit, c.currentSyncID != "")
	l.Debug("found syncs",
		zap.Int("candidate_count", len(candidates)),
		zap.Int("sync_limit", syncLimit))

	toDelete := SelectSyncsToDelete(candidates, c.currentSyncID, syncLimit)
	if len(toDelete) > 0 {
		l.Info("Cleaning up old sync data...", zap.Int("delete_count", len(toDelete)), zap.Int("sync_limit", syncLimit))
	}
	for _, id := range toDelete {
		if err := c.DeleteSyncRun(ctx, id); err != nil {
			return wrapSqliteInterruptError(err)
		}
		l.Info("Removed old sync data.", zap.String("sync_id", id))
	}

	if c.skipVacuum {
		l.Info("skip_vacuum option is set, skipping VACUUM in Cleanup",
			zap.String("db_file_path", c.dbFilePath),
		)
	} else {
		l.Debug("vacuuming database")
		err = c.Vacuum(ctx)
		if err != nil {
			return wrapSqliteInterruptError(err)
		}
		l.Debug("vacuum complete")
	}

	c.dbUpdated = true

	// If DB is open in WAL mode, truncate the WAL.
	var journalMode string
	row := c.rawDb.QueryRowContext(ctx, "PRAGMA journal_mode")
	if err := row.Scan(&journalMode); err != nil {
		return wrapSqliteInterruptError(fmt.Errorf("c1file: error getting journal mode: %w", err))
	}
	if strings.ToLower(journalMode) == "wal" {
		l.Debug("database is open in WAL mode, truncating WAL")
		_, _, _, err := c.truncateWAL(ctx)
		if err != nil {
			return wrapSqliteInterruptError(fmt.Errorf("c1file: error truncating WAL: %w", err))
		}
		l.Debug("WAL truncated")
	}

	return nil
}

func (c *C1File) deleteFromTable(ctx context.Context, tableName string, syncID string) (int64, error) {
	stmt := fmt.Sprintf(
		"delete from %s where id in (select id from %s where sync_id = ? limit %d)",
		tableName, tableName, deleteSyncRunBatchSize,
	)
	var deleted int64
	for {
		err := c.validateDb(ctx)
		if err != nil {
			return deleted, err
		}

		res, execErr := c.db.ExecContext(ctx, stmt, syncID)
		if execErr != nil {
			err = fmt.Errorf("failed to execute delete query for sync run: %w", execErr)
			return deleted, err
		}
		n, raErr := res.RowsAffected()
		if raErr != nil {
			err = fmt.Errorf("failed to read rows affected for sync run delete: %w", raErr)
			return deleted, err
		}
		deleted += n
		if n == 0 {
			break
		}
	}
	return deleted, nil
}

// DeleteSyncRun removes all the objects with a given syncID from the database.
func (c *C1File) DeleteSyncRun(ctx context.Context, syncID string) error {
	ctx, span := tracer.Start(ctx, "C1File.DeleteSyncRun")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	err = c.validateDb(ctx)
	if err != nil {
		return err
	}

	// Bail if we're actively syncing
	if c.currentSyncID != "" && c.currentSyncID == syncID {
		return fmt.Errorf("unable to delete the current active sync run")
	}

	// Delete in bounded, individually-committed batches instead of one
	// unbounded DELETE per table. A single DELETE over a multi-million-row
	// table is one transaction: if the activity deadline fires mid-statement
	// SQLite rolls it back, so the retry makes zero forward progress and
	// cleanup loops until the connector's c1z is abandoned. Batching commits
	// incrementally (each ExecContext autocommits), so a retry resumes where
	// the previous attempt left off. Batches are also faster than one giant
	// transaction (smaller per-commit journal and index churn).
	l := ctxzap.Extract(ctx)
	var deleted int64
	// Delete from sync_runs table last, since that's the table we use to determine which syncs to delete.

	for _, t := range allTableDescriptors {
		if t.Name() == syncRuns.Name() {
			continue
		}
		rowsDeleted, err := c.deleteFromTable(ctx, t.Name(), syncID)
		if err != nil {
			return err
		}
		deleted += rowsDeleted
	}
	rowsDeleted, err := c.deleteFromTable(ctx, syncRuns.Name(), syncID)
	if err != nil {
		return err
	}
	deleted += rowsDeleted

	l.Debug("deleted sync run", zap.String("sync_id", syncID), zap.Int64("rows", deleted))
	c.dbUpdated = true

	return nil
}

// Vacuum runs a VACUUM on the database to reclaim space.
func (c *C1File) Vacuum(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "C1File.Vacuum")
	uotel.SetSyncIdentityAttrs(ctx, span)
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	err = c.validateDb(ctx)
	if err != nil {
		return err
	}

	sizeBefore, sizeBeforeErr := c.CurrentDBSizeBytes()

	_, err = c.rawDb.ExecContext(ctx, "VACUUM")
	if err != nil {
		return err
	}

	sizeAfter, sizeErr := c.CurrentDBSizeBytes()
	if sizeErr == nil {
		span.SetAttributes(attribute.Int64("c1z.vacuum.size_after_bytes", sizeAfter))
		recordC1ZSize(ctx, "vacuum", sizeAfter)
		// reclaimed_bytes is only meaningful with a valid pre-VACUUM size;
		// a failed sizeBefore returns 0 and would emit negative reclaimed.
		if sizeBeforeErr == nil {
			span.SetAttributes(
				attribute.Int64("c1z.vacuum.size_before_bytes", sizeBefore),
				attribute.Int64("c1z.vacuum.reclaimed_bytes", sizeBefore-sizeAfter),
			)
		}
	}

	c.dbUpdated = true

	return nil
}

func toTimeStamp(t *time.Time) *timestamppb.Timestamp {
	if t == nil {
		return nil
	}
	return timestamppb.New(*t)
}

func (c *C1File) GetSync(ctx context.Context, request *reader_v2.SyncsReaderServiceGetSyncRequest) (*reader_v2.SyncsReaderServiceGetSyncResponse, error) {
	ctx, span := tracer.Start(ctx, "C1File.GetSync")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	sr, err := c.getSync(ctx, request.GetSyncId())
	if err != nil {
		return nil, fmt.Errorf("error getting sync '%s': %w", request.GetSyncId(), err)
	}

	return reader_v2.SyncsReaderServiceGetSyncResponse_builder{
		Sync: reader_v2.SyncRun_builder{
			Id:           sr.ID,
			StartedAt:    toTimeStamp(sr.StartedAt),
			EndedAt:      toTimeStamp(sr.EndedAt),
			SyncToken:    sr.SyncToken,
			SyncType:     string(sr.Type),
			ParentSyncId: sr.ParentSyncID,
			Stats:        sr.Stats,
		}.Build(),
	}.Build(), nil
}

func (c *C1File) ListSyncs(ctx context.Context, request *reader_v2.SyncsReaderServiceListSyncsRequest) (*reader_v2.SyncsReaderServiceListSyncsResponse, error) {
	ctx, span := tracer.Start(ctx, "C1File.ListSyncs")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	syncs, nextPageToken, err := c.ListSyncRuns(ctx, request.GetPageToken(), request.GetPageSize())
	if err != nil {
		return nil, fmt.Errorf("error listing syncs: %w", err)
	}

	syncRuns := make([]*reader_v2.SyncRun, len(syncs))
	for i, sr := range syncs {
		syncRuns[i] = reader_v2.SyncRun_builder{
			Id:           sr.ID,
			StartedAt:    toTimeStamp(sr.StartedAt),
			EndedAt:      toTimeStamp(sr.EndedAt),
			SyncToken:    sr.SyncToken,
			SyncType:     string(sr.Type),
			ParentSyncId: sr.ParentSyncID,
		}.Build()
	}

	return reader_v2.SyncsReaderServiceListSyncsResponse_builder{
		Syncs:         syncRuns,
		NextPageToken: nextPageToken,
	}.Build(), nil
}

func (c *C1File) GetLatestFinishedSync(
	ctx context.Context, request *reader_v2.SyncsReaderServiceGetLatestFinishedSyncRequest,
) (*reader_v2.SyncsReaderServiceGetLatestFinishedSyncResponse, error) {
	ctx, span := tracer.Start(ctx, "C1File.GetLatestFinishedSync")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	sync, err := c.getFinishedSync(ctx, 0, connectorstore.SyncType(request.GetSyncType()))
	if err != nil {
		return nil, fmt.Errorf("error fetching latest finished sync: %w", err)
	}

	if sync == nil {
		return reader_v2.SyncsReaderServiceGetLatestFinishedSyncResponse_builder{
			Sync: nil,
		}.Build(), nil
	}

	return reader_v2.SyncsReaderServiceGetLatestFinishedSyncResponse_builder{
		Sync: reader_v2.SyncRun_builder{
			Id:           sync.ID,
			StartedAt:    toTimeStamp(sync.StartedAt),
			EndedAt:      toTimeStamp(sync.EndedAt),
			SyncToken:    sync.SyncToken,
			SyncType:     string(sync.Type),
			ParentSyncId: sync.ParentSyncID,
		}.Build(),
	}.Build(), nil
}
