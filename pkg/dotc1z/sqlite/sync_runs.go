package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const sqliteTimeFormat = "2006-01-02 15:04:05.999999999"

// ErrNoSyncRuns reports that the source has no sync_runs rows.
var ErrNoSyncRuns = errors.New("sqlite: source has no sync runs")

// ResolveBehavior selects how an empty source sync ID is resolved.
type ResolveBehavior string

const (
	// ResolveNewest selects the newest finished or resumable sync before abandoned unfinished syncs.
	ResolveNewest ResolveBehavior = "newest"
	// ResolveFullFinished selects the latest finished full sync.
	ResolveFullFinished ResolveBehavior = "full_finished"
)

const unfinishedSyncMaxAge = 7 * 24 * time.Hour

// SyncRuns returns every source sync in insertion order.
func (f *File) SyncRuns(ctx context.Context) ([]*c1zstore.SyncRun, error) {
	query := "SELECT " + strings.Join(f.syncRunSelectColumns(true), ", ") + // #nosec G202 -- every selected expression is a package constant.
		" FROM " + syncRunsTable + " ORDER BY id"
	rows, err := f.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var runs []*c1zstore.SyncRun
	for rows.Next() {
		run, err := scanSyncRun(rows, true)
		if err != nil {
			return nil, err
		}
		runs = append(runs, run)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return runs, nil
}

// ResolveSyncID applies behavior to the source sync runs.
func (f *File) ResolveSyncID(ctx context.Context, behavior ResolveBehavior) (string, error) {
	var count int
	if err := f.db.QueryRowContext(ctx, "SELECT count(*) FROM "+syncRunsTable).Scan(&count); err != nil {
		return "", err
	}
	if count == 0 {
		return "", ErrNoSyncRuns
	}
	switch behavior {
	case ResolveNewest:
		return f.resolveNewest(ctx)
	case ResolveFullFinished:
		return f.latestFinishedSyncID(ctx, connectorstore.SyncTypeFull)
	default:
		return "", fmt.Errorf("sqlite: unknown resolve behavior %q", behavior)
	}
}

func (f *File) resolveNewest(ctx context.Context) (string, error) {
	syncType := f.columnOr(syncRunsTable, "sync_type", "'full'")
	query := "SELECT sync_id FROM " + syncRunsTable +
		" WHERE " + syncType + " IN (?, ?, ?)" +
		" ORDER BY (ended_at IS NOT NULL OR started_at >= ?) DESC, started_at DESC, sync_id DESC LIMIT 1"
	var syncID string
	err := f.db.QueryRowContext(
		ctx,
		query,
		connectorstore.SyncTypeFull,
		connectorstore.SyncTypeResourcesOnly,
		connectorstore.SyncTypePartial,
		time.Now().Add(-unfinishedSyncMaxAge).Format(sqliteTimeFormat),
	).Scan(&syncID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", status.Error(codes.NotFound, "no convertible sync found")
	}
	return syncID, err
}

func (f *File) latestFinishedSyncID(ctx context.Context, syncType connectorstore.SyncType) (string, error) {
	typeColumn := f.columnOr(syncRunsTable, "sync_type", "'full'")
	query := "SELECT sync_id FROM " + syncRunsTable +
		" WHERE ended_at IS NOT NULL AND " + typeColumn + " = ?" +
		" ORDER BY ended_at DESC, sync_id DESC LIMIT 1"
	var syncID string
	err := f.db.QueryRowContext(ctx, query, syncType).Scan(&syncID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", status.Error(codes.NotFound, "no finished full sync found")
	}
	return syncID, err
}

func (f *File) getSync(ctx context.Context, syncID string) (*c1zstore.SyncRun, error) {
	query := "SELECT " + strings.Join(f.syncRunSelectColumns(false), ", ") +
		" FROM " + syncRunsTable + " WHERE sync_id = ?"
	run, err := scanSyncRun(f.db.QueryRowContext(ctx, query, syncID), false)
	if err != nil {
		return nil, c1zstore.AdaptNotFound(err)
	}
	return run, nil
}

type rowScanner interface {
	Scan(...any) error
}

func scanSyncRun(row rowScanner, withID bool) (*c1zstore.SyncRun, error) {
	run := &c1zstore.SyncRun{}
	var rowID int64
	var stats []byte
	var generation, coverageJSON, mode string
	dest := []any{
		&run.ID, &run.StartedAt, &run.EndedAt, &run.SyncToken, &run.Type,
		&run.ParentSyncID, &run.SupportsDiff, &generation, &coverageJSON, &mode, &stats,
	}
	if withID {
		dest = append([]any{&rowID}, dest...)
	}
	if err := row.Scan(dest...); err != nil {
		return nil, err
	}
	run.Stats = parseStats(stats)
	run.IngestInvariantVerification = parseIngestInvariantVerification(generation, coverageJSON, mode)
	return run, nil
}

func (f *File) syncRunSelectColumns(withID bool) []string {
	columns := []string{
		"sync_id",
		"started_at",
		"ended_at",
		"sync_token",
		f.columnOr(syncRunsTable, "sync_type", "'full'"),
		f.columnOr(syncRunsTable, "parent_sync_id", "''"),
		f.columnOr(syncRunsTable, "supports_diff", "0"),
		f.columnOr(syncRunsTable, "ingest_invariant_generation", "''"),
		f.columnOr(syncRunsTable, "ingest_invariant_coverage", "''"),
		f.columnOr(syncRunsTable, "ingest_invariant_mode", "''"),
		f.columnOr(syncRunsTable, "stats", "NULL"),
	}
	if withID {
		columns = append([]string{"id"}, columns...)
	}
	return columns
}

func (f *File) columnOr(table, column, fallback string) string {
	if f.columns[table][column] {
		return column
	}
	return fallback
}

func parseStats(data []byte) *reader_v2.SyncStats {
	if len(data) == 0 {
		return nil
	}
	stats := &reader_v2.SyncStats{}
	if err := json.Unmarshal(data, stats); err != nil {
		return nil
	}
	return stats
}

func parseIngestInvariantVerification(generation, coverageJSON, mode string) c1zstore.IngestInvariantVerification {
	if generation == "" {
		return c1zstore.IngestInvariantVerification{}
	}
	var coverage []string
	if err := json.Unmarshal([]byte(coverageJSON), &coverage); err != nil {
		return c1zstore.IngestInvariantVerification{}
	}
	verification := c1zstore.IngestInvariantVerification{
		Generation: generation,
		Coverage:   coverage,
		Mode:       c1zstore.IngestInvariantVerificationMode(mode),
	}
	if !verification.IsVerified() {
		return c1zstore.IngestInvariantVerification{}
	}
	return verification
}
