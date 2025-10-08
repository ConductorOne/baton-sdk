package dotc1z

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/doug-martin/goqu/v9"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

// Task represents a task for parallel syncing
type Task struct {
	Action       Action `json:"action"`
	ResourceID   string `json:"resource_id"`
	Priority     int    `json:"priority"`
	ResourceType string `json:"resource_type"` // JSON representation of ResourceType
}

// Action represents a sync action
type Action struct {
	Op                   string `json:"operation"`
	PageToken            string `json:"page_token"`
	ResourceTypeID       string `json:"resource_type_id"`
	ResourceID           string `json:"resource_id"`
	ParentResourceTypeID string `json:"parent_resource_type_id"`
	ParentResourceID     string `json:"parent_resource_id"`
}

const tasksTableVersion = "2"
const tasksTableName = "tasks"
const tasksTableSchema = `
create table if not exists %s (
    id integer primary key,
    sync_id text not null,
    operation text not null,
    resource_type_id text,
    resource_id text,
    parent_resource_type_id text,
    parent_resource_id text,
    page_token text,
    bucket text not null,
    data blob not null,
    status text not null default 'pending',
    created_at datetime not null,
    claimed_at datetime,
    completed_at datetime
);
create index if not exists %s on %s (sync_id);
create index if not exists %s on %s (sync_id, bucket);
create index if not exists %s on %s (bucket);
create index if not exists %s on %s (sync_id, status);
create index if not exists %s on %s (status, created_at);`

var tasks = (*tasksTable)(nil)

type tasksTable struct{}

// Name returns the formatted table name for the tasks table.
func (t *tasksTable) Name() string {
	return fmt.Sprintf("v%s_%s", t.Version(), tasksTableName)
}

// Version returns the hard coded version of the table.
func (t *tasksTable) Version() string {
	return tasksTableVersion
}

// Schema returns the expanded SQL to generate the tasks table.
func (t *tasksTable) Schema() (string, []interface{}) {
	return tasksTableSchema, []interface{}{
		t.Name(),
		fmt.Sprintf("idx_tasks_sync_id_v%s", t.Version()),
		t.Name(),
		fmt.Sprintf("idx_tasks_sync_bucket_v%s", t.Version()),
		t.Name(),
		fmt.Sprintf("idx_tasks_bucket_v%s", t.Version()),
		t.Name(),
		fmt.Sprintf("idx_tasks_sync_status_v%s", t.Version()),
		t.Name(),
		fmt.Sprintf("idx_tasks_status_created_v%s", t.Version()),
		t.Name(),
	}
}

func (t *tasksTable) Migrations(ctx context.Context, db *goqu.Database) error {
	// Migration from v1 to v2: Add status columns
	oldTableName := fmt.Sprintf("v1_%s", tasksTableName)

	// Check if old table exists
	checkQuery := "SELECT name FROM sqlite_master WHERE type='table' AND name=?"
	var tableName string
	err := db.QueryRowContext(ctx, checkQuery, oldTableName).Scan(&tableName)
	if err != nil && err.Error() != "sql: no rows in result set" {
		return err
	}

	// If old table exists, migrate it
	if err == nil {
		// Add new columns to existing table
		alterQueries := []string{
			"ALTER TABLE " + oldTableName + " ADD COLUMN status text not null default 'pending'",
			"ALTER TABLE " + oldTableName + " ADD COLUMN claimed_at datetime",
			"ALTER TABLE " + oldTableName + " ADD COLUMN completed_at datetime",
		}

		for _, query := range alterQueries {
			_, err := db.ExecContext(ctx, query)
			if err != nil {
				// Column might already exist, ignore error
				if !strings.Contains(err.Error(), "duplicate column name") {
					return err
				}
			}
		}

		// Create new indices
		indexQueries := []string{
			fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_tasks_sync_status_v%s ON %s (sync_id, status)", t.Version(), oldTableName),
			fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_tasks_status_created_v%s ON %s (status, created_at)", t.Version(), oldTableName),
		}

		for _, query := range indexQueries {
			_, err := db.ExecContext(ctx, query)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// TaskRecord represents a task stored in the database
type TaskRecord struct {
	ID                   int64      `db:"id"`
	SyncID               string     `db:"sync_id"`
	Operation            string     `db:"operation"`
	ResourceTypeID       string     `db:"resource_type_id"`
	ResourceID           string     `db:"resource_id"`
	ParentResourceTypeID string     `db:"parent_resource_type_id"`
	ParentResourceID     string     `db:"parent_resource_id"`
	PageToken            string     `db:"page_token"`
	Bucket               string     `db:"bucket"`
	Data                 []byte     `db:"data"`
	Status               string     `db:"status"`
	CreatedAt            time.Time  `db:"created_at"`
	ClaimedAt            *time.Time `db:"claimed_at"`
	CompletedAt          *time.Time `db:"completed_at"`
}

// PutTasks stores multiple tasks in the database in a batch
func (c *C1File) PutTasks(ctx context.Context, syncID string, taskList ...*Task) error {
	if len(taskList) == 0 {
		return nil
	}

	ctx, span := tracer.Start(ctx, "C1File.PutTasks")
	defer span.End()

	l := ctxzap.Extract(ctx)
	l.Debug("putting tasks", zap.Int("count", len(taskList)), zap.String("sync_id", syncID), zap.String("table_name", tasks.Name()))

	err := c.validateSyncDb(ctx)
	if err != nil {
		return err
	}

	// Retry logic for SQLITE_BUSY errors
	maxRetries := 5
	for attempt := 0; attempt < maxRetries; attempt++ {
		err := c.putTasksWithRetry(ctx, syncID, taskList)
		if err == nil {
			return nil
		}

		// Check if it's a SQLITE_BUSY error
		if strings.Contains(err.Error(), "SQLITE_BUSY") || strings.Contains(err.Error(), "database is locked") {
			if attempt < maxRetries-1 {
				// Exponential backoff
				backoffDuration := time.Duration(attempt+1) * 10 * time.Millisecond
				l.Debug("database busy, retrying put tasks", zap.Int("attempt", attempt+1), zap.Duration("backoff", backoffDuration))
				time.Sleep(backoffDuration)
				continue
			}
		}

		return err
	}

	return fmt.Errorf("failed to put tasks after %d retries", maxRetries)
}

// putTasksWithRetry performs the actual task insertion
func (c *C1File) putTasksWithRetry(ctx context.Context, syncID string, taskList []*Task) error {
	// Prepare rows for batch insert
	rows := make([]goqu.Record, len(taskList))
	for i, task := range taskList {
		// Serialize the task to JSON
		taskData, err := json.Marshal(task)
		if err != nil {
			return fmt.Errorf("failed to marshal task: %w", err)
		}

		// Determine bucket for the task
		bucket := c.getBucketForTask(task)

		rows[i] = goqu.Record{
			"sync_id":                 syncID,
			"operation":               task.Action.Op,
			"resource_type_id":        task.Action.ResourceTypeID,
			"resource_id":             task.Action.ResourceID,
			"parent_resource_type_id": task.Action.ParentResourceTypeID,
			"parent_resource_id":      task.Action.ParentResourceID,
			"page_token":              task.Action.PageToken,
			"bucket":                  bucket,
			"data":                    taskData,
			"status":                  "pending",
			"created_at":              time.Now().Format("2006-01-02 15:04:05.999999999"),
		}
	}

	// Execute batch insert
	query, args, err := c.db.Insert(tasks.Name()).
		Rows(rows).
		Prepared(true).
		ToSQL()
	if err != nil {
		return err
	}

	_, err = c.db.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}

	l := ctxzap.Extract(ctx)
	l.Debug("tasks stored successfully", zap.Int("count", len(taskList)))
	return nil
}

// GetTasks fetches a batch of tasks from the database using optimistic locking
func (c *C1File) GetTasks(ctx context.Context, syncID, bucket string, limit int) ([]*TaskRecord, error) {
	ctx, span := tracer.Start(ctx, "C1File.GetTasks")
	defer span.End()

	l := ctxzap.Extract(ctx)
	l.Debug("getting tasks", zap.String("sync_id", syncID), zap.String("bucket", bucket), zap.Int("limit", limit), zap.String("table_name", tasks.Name()))

	err := c.validateDb(ctx)
	if err != nil {
		return nil, err
	}

	// Retry logic for SQLITE_BUSY errors
	maxRetries := 5
	for attempt := 0; attempt < maxRetries; attempt++ {
		taskRecords, err := c.getTasksWithRetry(ctx, syncID, bucket, limit)
		if err == nil {
			return taskRecords, nil
		}

		// Check if it's a SQLITE_BUSY error
		if strings.Contains(err.Error(), "SQLITE_BUSY") || strings.Contains(err.Error(), "database is locked") {
			if attempt < maxRetries-1 {
				// Exponential backoff
				backoffDuration := time.Duration(attempt+1) * 10 * time.Millisecond
				l.Debug("database busy, retrying", zap.Int("attempt", attempt+1), zap.Duration("backoff", backoffDuration))
				time.Sleep(backoffDuration)
				continue
			}
		}

		return nil, err
	}

	return nil, fmt.Errorf("failed to get tasks after %d retries", maxRetries)
}

// getTasksWithRetry performs the actual task fetching with optimistic locking approach
func (c *C1File) getTasksWithRetry(ctx context.Context, syncID, bucket string, limit int) ([]*TaskRecord, error) {
	// Step 1: Select pending tasks (no transaction, just read)
	query := c.db.From(tasks.Name()).
		Select("id", "sync_id", "operation", "resource_type_id", "resource_id",
			"parent_resource_type_id", "parent_resource_id", "page_token",
			"bucket", "data", "status", "created_at", "claimed_at", "completed_at").
		Where(goqu.C("sync_id").Eq(syncID)).
		Where(goqu.C("status").Eq("pending")).
		Order(goqu.C("id").Asc()).
		Limit(uint(limit)).
		Prepared(true)

	// If bucket is specified, filter by bucket; otherwise get tasks from any bucket
	if bucket != "" {
		query = query.Where(goqu.C("bucket").Eq(bucket))
	}

	selectQuery, selectArgs, err := query.ToSQL()
	if err != nil {
		return nil, err
	}

	rows, err := c.db.QueryContext(ctx, selectQuery, selectArgs...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var taskRecords []*TaskRecord
	var taskIDs []int64

	for rows.Next() {
		var tr TaskRecord
		err := rows.Scan(
			&tr.ID, &tr.SyncID, &tr.Operation, &tr.ResourceTypeID, &tr.ResourceID,
			&tr.ParentResourceTypeID, &tr.ParentResourceID, &tr.PageToken,
			&tr.Bucket, &tr.Data, &tr.Status, &tr.CreatedAt, &tr.ClaimedAt, &tr.CompletedAt,
		)
		if err != nil {
			return nil, err
		}
		taskRecords = append(taskRecords, &tr)
		taskIDs = append(taskIDs, tr.ID)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	// Step 2: Optimistically mark tasks as claimed (short transaction)
	if len(taskIDs) > 0 {
		now := time.Now().Format("2006-01-02 15:04:05.999999999")

		// Use a short transaction just for the update
		tx, err := c.db.BeginTx(ctx, nil)
		if err != nil {
			return nil, err
		}
		defer tx.Rollback()

		// Try to update tasks from pending to claimed
		// This will only succeed if the tasks are still pending
		updateQuery, updateArgs, err := tx.Update(tasks.Name()).
			Set(goqu.Record{
				"status":     "claimed",
				"claimed_at": now,
			}).
			Where(goqu.C("id").In(taskIDs)).
			Where(goqu.C("status").Eq("pending")). // Only update if still pending
			Prepared(true).
			ToSQL()
		if err != nil {
			return nil, err
		}

		result, err := tx.ExecContext(ctx, updateQuery, updateArgs...)
		if err != nil {
			return nil, err
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return nil, err
		}

		err = tx.Commit()
		if err != nil {
			return nil, err
		}

		l := ctxzap.Extract(ctx)
		l.Debug("tasks claimed successfully", zap.Int("requested", len(taskIDs)), zap.Int64("claimed", rowsAffected))

		// If some tasks were already claimed by other workers, that's fine
		// We still return the tasks we selected - they're idempotent
	}

	return taskRecords, nil
}

// deleteTasksByID deletes tasks by their IDs in a short transaction
func (c *C1File) deleteTasksByID(ctx context.Context, taskIDs []int64) error {
	if len(taskIDs) == 0 {
		return nil
	}

	// Use a short transaction just for the delete
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	deleteQuery, deleteArgs, err := tx.Delete(tasks.Name()).
		Where(goqu.C("id").In(taskIDs)).
		Prepared(true).
		ToSQL()
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, deleteQuery, deleteArgs...)
	if err != nil {
		return err
	}

	return tx.Commit()
}

// DeleteTasks removes completed tasks from the database
func (c *C1File) DeleteTasks(ctx context.Context, taskIDs ...int64) error {
	if len(taskIDs) == 0 {
		return nil
	}

	ctx, span := tracer.Start(ctx, "C1File.DeleteTasks")
	defer span.End()

	l := ctxzap.Extract(ctx)
	l.Debug("deleting tasks", zap.Int("count", len(taskIDs)))

	err := c.validateDb(ctx)
	if err != nil {
		return err
	}

	query, args, err := c.db.Delete(tasks.Name()).
		Where(goqu.C("id").In(taskIDs)).
		Prepared(true).
		ToSQL()
	if err != nil {
		return err
	}

	_, err = c.db.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}

	l.Debug("tasks deleted successfully", zap.Int("count", len(taskIDs)))
	return nil
}

// TruncateTasks removes all tasks for a given sync ID
func (c *C1File) TruncateTasks(ctx context.Context, syncID string) error {
	ctx, span := tracer.Start(ctx, "C1File.TruncateTasks")
	defer span.End()

	l := ctxzap.Extract(ctx)
	l.Debug("truncating tasks", zap.String("sync_id", syncID))

	err := c.validateDb(ctx)
	if err != nil {
		return err
	}

	query, args, err := c.db.Delete(tasks.Name()).
		Where(goqu.C("sync_id").Eq(syncID)).
		Prepared(true).
		ToSQL()
	if err != nil {
		return err
	}

	result, err := c.db.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}

	l.Debug("tasks truncated successfully", zap.Int64("rows_affected", rowsAffected))
	return nil
}

// GetTaskCount returns the number of pending tasks for a sync
func (c *C1File) GetTaskCount(ctx context.Context, syncID string) (int64, error) {
	ctx, span := tracer.Start(ctx, "C1File.GetTaskCount")
	defer span.End()

	err := c.validateDb(ctx)
	if err != nil {
		return 0, err
	}

	query, args, err := c.db.From(tasks.Name()).
		Select(goqu.COUNT("*")).
		Where(goqu.C("sync_id").Eq(syncID)).
		Where(goqu.C("status").In("pending", "claimed")).
		Prepared(true).
		ToSQL()
	if err != nil {
		return 0, err
	}

	var count int64
	err = c.db.QueryRowContext(ctx, query, args...).Scan(&count)
	if err != nil {
		return 0, err
	}

	return count, nil
}

// MarkTasksComplete marks tasks as complete after processing
func (c *C1File) MarkTasksComplete(ctx context.Context, syncID string, taskIDs ...int64) error {
	if len(taskIDs) == 0 {
		return nil
	}

	ctx, span := tracer.Start(ctx, "C1File.MarkTasksComplete")
	defer span.End()

	l := ctxzap.Extract(ctx)
	l.Debug("marking tasks complete", zap.Int("count", len(taskIDs)), zap.String("sync_id", syncID))

	err := c.validateSyncDb(ctx)
	if err != nil {
		return err
	}

	now := time.Now().Format("2006-01-02 15:04:05.999999999")
	query, args, err := c.db.Update(tasks.Name()).
		Set(goqu.Record{
			"status":       "complete",
			"completed_at": now,
		}).
		Where(goqu.C("sync_id").Eq(syncID)).
		Where(goqu.C("id").In(taskIDs)).
		Prepared(true).
		ToSQL()
	if err != nil {
		return err
	}

	_, err = c.db.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}

	l.Debug("tasks marked complete successfully", zap.Int("count", len(taskIDs)))
	return nil
}

// GetTaskCountByStatus returns the number of tasks by status for debugging
func (c *C1File) GetTaskCountByStatus(ctx context.Context, syncID string) (map[string]int64, error) {
	ctx, span := tracer.Start(ctx, "C1File.GetTaskCountByStatus")
	defer span.End()

	err := c.validateDb(ctx)
	if err != nil {
		return nil, err
	}

	query, args, err := c.db.From(tasks.Name()).
		Where(goqu.C("sync_id").Eq(syncID)).
		Select("status", goqu.COUNT("*")).
		GroupBy("status").
		Prepared(true).
		ToSQL()
	if err != nil {
		return nil, err
	}

	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	counts := make(map[string]int64)
	for rows.Next() {
		var status string
		var count int64
		err := rows.Scan(&status, &count)
		if err != nil {
			return nil, err
		}
		counts[status] = count
	}

	return counts, nil
}

// GetBucketTaskCount returns the number of pending tasks for a specific bucket
func (c *C1File) GetBucketTaskCount(ctx context.Context, syncID, bucket string) (int64, error) {
	ctx, span := tracer.Start(ctx, "C1File.GetBucketTaskCount")
	defer span.End()

	err := c.validateDb(ctx)
	if err != nil {
		return 0, err
	}

	query, args, err := c.db.From(tasks.Name()).
		Select(goqu.COUNT("*")).
		Where(goqu.C("sync_id").Eq(syncID)).
		Where(goqu.C("bucket").Eq(bucket)).
		Where(goqu.C("status").Eq("pending")).
		Prepared(true).
		ToSQL()
	if err != nil {
		return 0, err
	}

	var count int64
	err = c.db.QueryRowContext(ctx, query, args...).Scan(&count)
	if err != nil {
		return 0, err
	}

	return count, nil
}

// ResetStuckClaimedTasks resets claimed tasks that have been stuck for too long back to pending
// This helps recover from deadlock scenarios where workers crashed or were interrupted
func (c *C1File) ResetStuckClaimedTasks(ctx context.Context, syncID string, timeoutMinutes int) error {
	ctx, span := tracer.Start(ctx, "C1File.ResetStuckClaimedTasks")
	defer span.End()

	l := ctxzap.Extract(ctx)
	l.Debug("resetting stuck claimed tasks", zap.String("sync_id", syncID), zap.Int("timeout_minutes", timeoutMinutes))

	err := c.validateDb(ctx)
	if err != nil {
		return err
	}

	// Calculate cutoff time
	cutoffTime := time.Now().Add(-time.Duration(timeoutMinutes) * time.Minute)
	cutoffTimeStr := cutoffTime.Format("2006-01-02 15:04:05.999999999")

	query, args, err := c.db.Update(tasks.Name()).
		Set(goqu.Record{
			"status":     "pending",
			"claimed_at": nil,
		}).
		Where(goqu.C("sync_id").Eq(syncID)).
		Where(goqu.C("status").Eq("claimed")).
		Where(goqu.C("claimed_at").Lt(cutoffTimeStr)).
		Prepared(true).
		ToSQL()
	if err != nil {
		return err
	}

	result, err := c.db.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if rowsAffected > 0 {
		l.Info("reset stuck claimed tasks", zap.Int64("count", rowsAffected))
	}

	return nil
}

// getBucketForTask determines the bucket for a task based on the resource type's sync_bucket
func (c *C1File) getBucketForTask(task *Task) string {
	// For now, create a unique bucket per resource type
	// In the future, we could parse the ResourceType JSON to get sync_bucket
	return fmt.Sprintf("resource-type-%s", task.Action.ResourceTypeID)
}

// DeserializeTask converts a TaskRecord back to a Task
func (tr *TaskRecord) DeserializeTask() (*Task, error) {
	var task Task
	err := json.Unmarshal(tr.Data, &task)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal task: %w", err)
	}
	return &task, nil
}
