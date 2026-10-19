package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"math/rand"
	"os"
	"runtime"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	"github.com/conductorone/baton-sdk/pkg/types/sessions"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	defaultBatchSize      = 10000
	defaultGrantScanLanes = 4
	sessionBatchKeys      = 500
	sessionBatchBytes     = 4 << 20
)

// ConvertOption configures ToPebble and ToPebbleOpen.
type ConvertOption func(*convertConfig)

type convertConfig struct {
	batchSize       int
	tmpDir          string
	parallelism     int
	resolveBehavior ResolveBehavior
	logOnReturn     bool
}

// WithResolveBehavior selects how an empty source sync ID is resolved.
func WithResolveBehavior(behavior ResolveBehavior) ConvertOption {
	return func(c *convertConfig) {
		if behavior == "" {
			behavior = ResolveNewest
		}
		c.resolveBehavior = behavior
	}
}

// WithBatchSize sets the number of records passed to each bulk-import call.
func WithBatchSize(n int) ConvertOption {
	return func(c *convertConfig) {
		if n > 0 {
			c.batchSize = n
		}
	}
}

// WithConvertTmpDir sets the destination engine's temporary directory.
func WithConvertTmpDir(dir string) ConvertOption {
	return func(c *convertConfig) {
		c.tmpDir = dir
	}
}

// WithParallelism sets the grant scan lane count.
func WithParallelism(n int) ConvertOption {
	return func(c *convertConfig) {
		if n > 0 {
			c.parallelism = n
		}
	}
}

func deferCompletionLog(c *convertConfig) {
	c.logOnReturn = false
}

// ConvertStageStats records the rows and elapsed time for one conversion stage.
type ConvertStageStats struct {
	Rows     int64
	Duration time.Duration
}

// DiscardedSync describes a source sync omitted from the single-sync destination.
type DiscardedSync struct {
	ID        string
	Type      connectorstore.SyncType
	StartedAt *time.Time
	EndedAt   *time.Time
}

// ConvertStats reports conversion row counts, timing, and omitted source syncs.
type ConvertStats struct {
	SourceSyncID    string
	DestSyncID      string
	ResourceTypes   ConvertStageStats
	Resources       ConvertStageStats
	Entitlements    ConvertStageStats
	Grants          ConvertStageStats
	Assets          ConvertStageStats
	AssetBytes      int64
	Sessions        ConvertStageStats
	DiscardedSyncs  []DiscardedSync
	Total           time.Duration
	endSyncDuration time.Duration
}

type syncIDPreservingStarter interface {
	StartNewSyncWithID(context.Context, connectorstore.SyncType, string, string) (string, error)
}

// ToPebbleOpen converts one source sync and returns the destination store open.
// Callers that bind the converted sync and mutate records must clear inherited
// ingest-invariant verification, call EndSync, and restore the converted sync's
// source timestamps through pebble.AsEngine before Close.
func (f *File) ToPebbleOpen(
	ctx context.Context,
	outPath string,
	sourceSyncID string,
	opts ...ConvertOption,
) (_ c1zstore.Store, _ *ConvertStats, err error) {
	cfg := &convertConfig{
		batchSize:       defaultBatchSize,
		resolveBehavior: ResolveNewest,
		logOnReturn:     true,
	}
	for _, opt := range opts {
		opt(cfg)
	}
	if !slices.Contains([]ResolveBehavior{ResolveNewest, ResolveFullFinished}, cfg.resolveBehavior) {
		return nil, nil, fmt.Errorf("to-pebble: unknown convert resolve behavior %q", cfg.resolveBehavior)
	}
	if _, statErr := os.Stat(outPath); statErr == nil {
		return nil, nil, fmt.Errorf("to-pebble: output path (%s) must not exist", outPath)
	} else if !errors.Is(statErr, fs.ErrNotExist) {
		return nil, nil, fmt.Errorf("to-pebble: stat output path %s: %w", outPath, statErr)
	}

	if sourceSyncID == "" {
		sourceSyncID, err = f.ResolveSyncID(ctx, cfg.resolveBehavior)
		if errors.Is(err, ErrNoSyncRuns) {
			return f.emptyPebble(ctx, outPath, cfg)
		}
		if err != nil {
			return nil, nil, fmt.Errorf("to-pebble: resolve convert sync id: %w", err)
		}
	}
	sourceSync, err := f.getSync(ctx, sourceSyncID)
	if err != nil {
		return nil, nil, fmt.Errorf("to-pebble: load source sync %q: %w", sourceSyncID, err)
	}
	stats := &ConvertStats{SourceSyncID: sourceSyncID}
	stats.DiscardedSyncs, err = f.discardedSyncs(ctx, sourceSyncID)
	if err != nil {
		return nil, nil, fmt.Errorf("to-pebble: list discarded syncs: %w", err)
	}

	start := time.Now()
	dest, err := dotc1z.NewStore(ctx, outPath, dotc1z.WithEngine(c1zstore.EnginePebble), dotc1z.WithTmpDir(cfg.tmpDir))
	if err != nil {
		return nil, nil, fmt.Errorf("to-pebble: open destination: %w", err)
	}
	cleanup := true
	defer func() {
		if cleanup {
			err = errors.Join(err, dest.Close(ctx))
			_ = os.Remove(outPath) // #nosec G703 -- cleanup uses the caller-selected output.
		}
	}()

	starter, ok := dest.(syncIDPreservingStarter)
	if !ok {
		return nil, nil, errors.New("to-pebble: destination does not support preserving the source sync id")
	}
	destSyncID, err := starter.StartNewSyncWithID(ctx, sourceSync.Type, sourceSyncID, sourceSync.ParentSyncID)
	if err != nil {
		return nil, nil, fmt.Errorf("to-pebble: start destination sync: %w", err)
	}
	stats.DestSyncID = destSyncID

	destEngine, ok := pebble.AsEngine(dest)
	if !ok {
		return nil, nil, errors.New("to-pebble: destination store is not a pebble engine")
	}
	bulkImport, err := destEngine.StartBulkSyncImport(ctx, destSyncID, cfg.tmpDir, cfg.grantScanLanes())
	if err != nil {
		return nil, nil, fmt.Errorf("to-pebble: start bulk import: %w", err)
	}
	imported := false
	defer func() {
		if !imported {
			bulkImport.Abort()
		}
	}()

	if err := f.convertResourceTypes(ctx, bulkImport, sourceSyncID, cfg.batchSize, &stats.ResourceTypes); err != nil {
		return nil, nil, fmt.Errorf("to-pebble: resource types: %w", err)
	}
	if err := f.convertResources(ctx, bulkImport, sourceSyncID, cfg.batchSize, &stats.Resources); err != nil {
		return nil, nil, fmt.Errorf("to-pebble: resources: %w", err)
	}
	if err := f.convertEntitlements(ctx, bulkImport, sourceSyncID, cfg.batchSize, &stats.Entitlements); err != nil {
		return nil, nil, fmt.Errorf("to-pebble: entitlements: %w", err)
	}
	if err := f.convertGrants(ctx, bulkImport, sourceSyncID, cfg, &stats.Grants); err != nil {
		return nil, nil, fmt.Errorf("to-pebble: grants: %w", err)
	}
	if err := bulkImport.Finish(ctx); err != nil {
		return nil, nil, fmt.Errorf("to-pebble: ingest: %w", err)
	}
	imported = true

	if err := f.copyAssets(ctx, dest, sourceSyncID, &stats.Assets, &stats.AssetBytes); err != nil {
		return nil, nil, fmt.Errorf("to-pebble: assets: %w", err)
	}
	if sourceSync.EndedAt == nil && f.hasTables[sessionsTable] {
		if err := f.copySessions(ctx, destEngine, sourceSyncID, destSyncID, &stats.Sessions); err != nil {
			return nil, nil, fmt.Errorf("to-pebble: sessions: %w", err)
		}
	}

	computedStats := bulkImport.ComputedStats()
	computedStats.SetAssets(stats.Assets.Rows)
	destEngine.StashComputedSyncStats(destSyncID, computedStats)

	endSyncStart := time.Now()
	if err := dest.EndSync(ctx); err != nil {
		return nil, nil, fmt.Errorf("to-pebble: end destination sync: %w", err)
	}
	record, err := destEngine.GetSyncRunRecord(ctx, destSyncID)
	if err != nil {
		return nil, nil, fmt.Errorf("to-pebble: load destination sync metadata: %w", err)
	}
	record.SetSupportsDiff(sourceSync.SupportsDiff)
	if sourceSync.StartedAt != nil {
		record.SetStartedAt(timestamppb.New(localizeSQLiteTimestamp(*sourceSync.StartedAt, time.Local)))
	}
	if sourceSync.EndedAt != nil {
		record.SetEndedAt(timestamppb.New(localizeSQLiteTimestamp(*sourceSync.EndedAt, time.Local)))
		if sourceSync.IsVerified() {
			record.SetIngestInvariantGeneration(sourceSync.Generation)
			record.SetIngestInvariantCoverage(append([]string(nil), sourceSync.Coverage...))
			record.SetIngestInvariantMode(string(sourceSync.Mode))
		}
	} else {
		record.ClearEndedAt()
		record.SetSyncToken(sourceSync.SyncToken)
	}
	if err := destEngine.PutSyncRunRecord(ctx, record); err != nil {
		return nil, nil, fmt.Errorf("to-pebble: preserve source sync metadata: %w", err)
	}

	stats.Total = time.Since(start)
	stats.endSyncDuration = time.Since(endSyncStart)
	if cfg.logOnReturn {
		logConversionComplete(ctx, stats)
	}
	cleanup = false
	return dest, stats, nil
}

// ToPebble converts one source sync and closes the destination store.
func (f *File) ToPebble(
	ctx context.Context,
	outPath string,
	sourceSyncID string,
	opts ...ConvertOption,
) (*ConvertStats, error) {
	opts = append(opts, deferCompletionLog)
	store, stats, err := f.ToPebbleOpen(ctx, outPath, sourceSyncID, opts...)
	if err != nil {
		return nil, err
	}
	closeStart := time.Now()
	if err := store.Close(ctx); err != nil {
		_ = os.Remove(outPath) // #nosec G703 -- cleanup uses the caller-selected output.
		return nil, fmt.Errorf("to-pebble: close destination: %w", err)
	}
	closeDuration := time.Since(closeStart)
	stats.Total += closeDuration
	ctxzap.Extract(ctx).Debug("to-pebble: destination finalize timings",
		zap.Duration("end_sync", stats.endSyncDuration),
		zap.Duration("close_save", closeDuration),
	)
	logConversionComplete(ctx, stats)
	return stats, nil
}

func (f *File) emptyPebble(ctx context.Context, outPath string, cfg *convertConfig) (_ c1zstore.Store, _ *ConvertStats, err error) {
	start := time.Now()
	dest, err := dotc1z.NewStore(ctx, outPath, dotc1z.WithEngine(c1zstore.EnginePebble), dotc1z.WithTmpDir(cfg.tmpDir))
	if err != nil {
		return nil, nil, fmt.Errorf("to-pebble: open destination: %w", err)
	}
	cleanup := true
	defer func() {
		if cleanup {
			err = errors.Join(err, dest.Close(ctx))
			_ = os.Remove(outPath) // #nosec G703 -- cleanup uses the caller-selected output.
		}
	}()
	if !pebble.MarkStoreDirty(dest) {
		return nil, nil, errors.New("to-pebble: destination does not support dirty marking")
	}
	stats := &ConvertStats{Total: time.Since(start)}
	if cfg.logOnReturn {
		logConversionComplete(ctx, stats)
	}
	cleanup = false
	return dest, stats, nil
}

func logConversionComplete(ctx context.Context, stats *ConvertStats) {
	if stats.SourceSyncID == "" {
		ctxzap.Extract(ctx).Info("to-pebble: wrote empty pebble c1z (source had no sync runs)",
			zap.Duration("total", stats.Total),
		)
		return
	}
	ctxzap.Extract(ctx).Info("to-pebble: conversion complete",
		append([]zap.Field{
			zap.String("source_sync_id", stats.SourceSyncID),
			zap.String("dest_sync_id", stats.DestSyncID),
			zap.Int64("resource_types", stats.ResourceTypes.Rows),
			zap.Int64("resources", stats.Resources.Rows),
			zap.Int64("entitlements", stats.Entitlements.Rows),
			zap.Int64("grants", stats.Grants.Rows),
			zap.Int64("assets", stats.Assets.Rows),
			zap.Int64("asset_bytes", stats.AssetBytes),
			zap.Int64("sessions", stats.Sessions.Rows),
			zap.Duration("total", stats.Total),
		}, discardedSyncFields(stats.DiscardedSyncs)...)...,
	)
}

func (f *File) discardedSyncs(ctx context.Context, keepSyncID string) ([]DiscardedSync, error) {
	runs, err := f.SyncRuns(ctx)
	if err != nil {
		return nil, err
	}
	var discarded []DiscardedSync
	for _, run := range runs {
		if run.ID == keepSyncID {
			continue
		}
		discarded = append(discarded, DiscardedSync{
			ID:        run.ID,
			Type:      run.Type,
			StartedAt: localizeSQLiteTimestampPtr(run.StartedAt, time.Local),
			EndedAt:   localizeSQLiteTimestampPtr(run.EndedAt, time.Local),
		})
	}
	return discarded, nil
}

func discardedSyncFields(discarded []DiscardedSync) []zap.Field {
	descriptors := make([]string, 0, len(discarded))
	for _, item := range discarded {
		startedAt := "unknown"
		if item.StartedAt != nil {
			startedAt = item.StartedAt.Format(time.RFC3339)
		}
		finished := item.EndedAt != nil
		descriptors = append(descriptors,
			fmt.Sprintf("id=%s type=%s started_at=%s finished=%t", item.ID, item.Type, startedAt, finished))
	}
	return []zap.Field{
		zap.Int("discarded_sync_count", len(discarded)),
		zap.Strings("discarded_syncs", descriptors),
	}
}

func (f *File) scanRows(ctx context.Context, query string, args []any, scan func(*sql.Rows) error) error {
	rows, err := f.db.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := scan(rows); err != nil {
			return err
		}
	}
	return rows.Err()
}

func (f *File) convertResourceTypes(
	ctx context.Context,
	bulkImport *pebble.BulkSyncImport,
	syncID string,
	batchSize int,
	stage *ConvertStageStats,
) error {
	start := time.Now()
	defer func() { stage.Duration = time.Since(start) }()
	batch := make([]*v2.ResourceType, 0, batchSize)
	discovered := make([]*timestamppb.Timestamp, 0, batchSize)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := bulkImport.AddResourceTypesWithDiscoveredAt(ctx, batch, discovered); err != nil {
			return err
		}
		stage.Rows += int64(len(batch))
		batch = batch[:0]
		discovered = discovered[:0]
		return nil
	}
	err := f.scanRows(ctx,
		"SELECT data, discovered_at FROM "+resourceTypesTable+" WHERE sync_id = ? ORDER BY external_id",
		[]any{syncID},
		func(rows *sql.Rows) error {
			var data []byte
			var discoveredAt time.Time
			if err := rows.Scan(&data, &discoveredAt); err != nil {
				return err
			}
			record := &v2.ResourceType{}
			if err := proto.Unmarshal(data, record); err != nil {
				return err
			}
			batch = append(batch, record)
			discovered = append(discovered, discoveredAtTimestamp(discoveredAt))
			if len(batch) == batchSize {
				return flush()
			}
			return nil
		})
	if err != nil {
		return err
	}
	return flush()
}

func (f *File) convertResources(
	ctx context.Context,
	bulkImport *pebble.BulkSyncImport,
	syncID string,
	batchSize int,
	stage *ConvertStageStats,
) error {
	start := time.Now()
	defer func() { stage.Duration = time.Since(start) }()
	batch := make([]*v2.Resource, 0, batchSize)
	discovered := make([]*timestamppb.Timestamp, 0, batchSize)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := bulkImport.AddResourcesWithDiscoveredAt(ctx, batch, discovered); err != nil {
			return err
		}
		stage.Rows += int64(len(batch))
		batch = batch[:0]
		discovered = discovered[:0]
		return nil
	}
	err := f.scanRows(ctx,
		"SELECT data, discovered_at FROM "+resourcesTable+" WHERE sync_id = ?",
		[]any{syncID},
		func(rows *sql.Rows) error {
			var data []byte
			var discoveredAt time.Time
			if err := rows.Scan(&data, &discoveredAt); err != nil {
				return err
			}
			record := &v2.Resource{}
			if err := proto.Unmarshal(data, record); err != nil {
				return err
			}
			batch = append(batch, record)
			discovered = append(discovered, discoveredAtTimestamp(discoveredAt))
			if len(batch) == batchSize {
				return flush()
			}
			return nil
		})
	if err != nil {
		return err
	}
	return flush()
}

func (f *File) convertEntitlements(
	ctx context.Context,
	bulkImport *pebble.BulkSyncImport,
	syncID string,
	batchSize int,
	stage *ConvertStageStats,
) error {
	start := time.Now()
	defer func() { stage.Duration = time.Since(start) }()
	batch := make([]*v2.Entitlement, 0, batchSize)
	discovered := make([]*timestamppb.Timestamp, 0, batchSize)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := bulkImport.AddEntitlementsWithDiscoveredAt(ctx, batch, discovered); err != nil {
			return err
		}
		stage.Rows += int64(len(batch))
		batch = batch[:0]
		discovered = discovered[:0]
		return nil
	}
	err := f.scanRows(ctx,
		"SELECT data, discovered_at FROM "+entitlementsTable+" WHERE sync_id = ?",
		[]any{syncID},
		func(rows *sql.Rows) error {
			var data []byte
			var discoveredAt time.Time
			if err := rows.Scan(&data, &discoveredAt); err != nil {
				return err
			}
			record := &v2.Entitlement{}
			if err := proto.Unmarshal(data, record); err != nil {
				return err
			}
			batch = append(batch, record)
			discovered = append(discovered, discoveredAtTimestamp(discoveredAt))
			if len(batch) == batchSize {
				return flush()
			}
			return nil
		})
	if err != nil {
		return err
	}
	return flush()
}

func (cfg *convertConfig) grantScanLanes() int {
	if cfg.parallelism > 0 {
		return cfg.parallelism
	}
	return min(defaultGrantScanLanes, max(1, runtime.GOMAXPROCS(0)/2))
}

type rawGrantRow struct {
	data         []byte
	expansion    []byte
	discoveredAt time.Time
	identity     grantIdentity
}

func (f *File) convertGrants(
	ctx context.Context,
	bulkImport *pebble.BulkSyncImport,
	syncID string,
	cfg *convertConfig,
	stage *ConvertStageStats,
) error {
	start := time.Now()
	defer func() { stage.Duration = time.Since(start) }()

	var minID, maxID sql.NullInt64
	if err := f.db.QueryRowContext(
		ctx,
		"SELECT MIN(id), MAX(id) FROM "+grantsTable+" WHERE sync_id = ?",
		syncID,
	).Scan(&minID, &maxID); err != nil {
		return err
	}
	if !minID.Valid {
		return nil
	}

	lanes := cfg.grantScanLanes()
	scanDB := f.db
	if lanes > 1 {
		pool, err := openSQLite(ctx, f.dbPath)
		if err != nil {
			return fmt.Errorf("open scan pool: %w", err)
		}
		pool.SetMaxOpenConns(lanes)
		scanDB = pool
		defer pool.Close()
	}
	bounds, err := sampleGrantBoundaries(ctx, scanDB, syncID, minID.Int64, maxID.Int64, lanes)
	if err != nil {
		return err
	}

	scanCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var (
		errMu    sync.Mutex
		firstErr error
		rowCount atomic.Int64
		wait     sync.WaitGroup
	)
	fail := func(err error) {
		errMu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		errMu.Unlock()
		cancel()
	}
	expansionColumn := f.columnOr(grantsTable, "expansion", "NULL")

	for lane := 0; lane <= len(bounds); lane++ {
		var low, high string
		if lane > 0 {
			low = bounds[lane-1]
		}
		if lane < len(bounds) {
			high = bounds[lane]
		}
		shard, err := bulkImport.NewGrantShard()
		if err != nil {
			cancel()
			wait.Wait()
			return err
		}
		query := "SELECT data, " + expansionColumn +
			", discovered_at, entitlement_id, resource_type_id, resource_id, principal_resource_type_id, principal_resource_id" +
			" FROM " + grantsTable + " WHERE sync_id = ?"
		args := []any{syncID}
		if low != "" {
			query += " AND external_id >= ?"
			args = append(args, low)
		}
		if high != "" {
			query += " AND external_id < ?"
			args = append(args, high)
		}
		query += " ORDER BY external_id"

		rawRows := make(chan []rawGrantRow, 2)
		wait.Add(2)
		go func() {
			defer wait.Done()
			defer shard.Close()
			grants := make([]*v2.Grant, 0, cfg.batchSize)
			discovered := make([]*timestamppb.Timestamp, 0, cfg.batchSize)
			for rawBatch := range rawRows {
				if scanCtx.Err() != nil {
					continue
				}
				grants = grants[:0]
				discovered = discovered[:0]
				for _, raw := range rawBatch {
					grant := &v2.Grant{}
					if err := proto.Unmarshal(raw.data, grant); err != nil {
						fail(err)
						break
					}
					if err := reattachExpansion(grant, raw.expansion); err != nil {
						fail(err)
						break
					}
					hydrateGrant(grant, raw.identity)
					grants = append(grants, grant)
					discovered = append(discovered, discoveredAtTimestamp(raw.discoveredAt))
				}
				if scanCtx.Err() != nil {
					continue
				}
				if err := shard.AddGrantsWithDiscoveredAt(scanCtx, grants, discovered); err != nil {
					fail(err)
					continue
				}
				rowCount.Add(int64(len(grants)))
			}
		}()
		go func(query string, args []any) {
			defer wait.Done()
			defer close(rawRows)
			rows, err := scanDB.QueryContext(scanCtx, query, args...)
			if err != nil {
				fail(err)
				return
			}
			defer rows.Close()
			var arena []byte
			batch := make([]rawGrantRow, 0, cfg.batchSize)
			emit := func() {
				if len(batch) == 0 {
					return
				}
				select {
				case rawRows <- batch:
				case <-scanCtx.Done():
				}
				arena = nil
				batch = make([]rawGrantRow, 0, cfg.batchSize)
			}
			for rows.Next() {
				var data, expansion sql.RawBytes
				var row rawGrantRow
				if err := rows.Scan(
					&data,
					&expansion,
					&row.discoveredAt,
					&row.identity.entitlementID,
					&row.identity.entitlementResourceTypeID,
					&row.identity.entitlementResourceID,
					&row.identity.principalResourceTypeID,
					&row.identity.principalResourceID,
				); err != nil {
					fail(err)
					return
				}
				offset := len(arena)
				arena = append(arena, data...)
				arena = append(arena, expansion...)
				row.data = arena[offset : offset+len(data) : offset+len(data)]
				if len(expansion) != 0 {
					row.expansion = arena[offset+len(data) : offset+len(data)+len(expansion)]
				}
				batch = append(batch, row)
				if len(batch) == cfg.batchSize {
					emit()
				}
			}
			if err := rows.Err(); err != nil {
				fail(err)
				return
			}
			emit()
		}(query, args)
	}
	wait.Wait()
	if firstErr != nil {
		return firstErr
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	stage.Rows = rowCount.Load()
	return nil
}

func sampleGrantBoundaries(
	ctx context.Context,
	db *sql.DB,
	syncID string,
	minID, maxID int64,
	lanes int,
) ([]string, error) {
	if lanes <= 1 || maxID <= minID {
		return nil, nil
	}
	span := maxID - minID + 1
	if span <= 0 {
		return nil, nil
	}
	const samplesPerLane = 32
	sampleCount := lanes * samplesPerLane
	query := "SELECT external_id FROM " + grantsTable + " WHERE id >= ? AND sync_id = ? ORDER BY id LIMIT 1"
	rng := rand.New(rand.NewSource(1)) //nolint:gosec // The deterministic values only balance read lanes.
	samples := make([]string, 0, sampleCount)
	for range sampleCount {
		id := minID + rng.Int63n(span)
		var externalID string
		err := db.QueryRowContext(ctx, query, id, syncID).Scan(&externalID)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return nil, err
		}
		samples = append(samples, externalID)
	}
	if len(samples) == 0 {
		return nil, nil
	}
	sort.Strings(samples)
	bounds := make([]string, 0, lanes-1)
	for lane := 1; lane < lanes; lane++ {
		boundary := samples[lane*len(samples)/lanes]
		if len(bounds) == 0 || bounds[len(bounds)-1] < boundary {
			bounds = append(bounds, boundary)
		}
	}
	return bounds, nil
}

func (f *File) copyAssets(
	ctx context.Context,
	dest connectorstore.Writer,
	syncID string,
	stage *ConvertStageStats,
	totalBytes *int64,
) error {
	start := time.Now()
	defer func() { stage.Duration = time.Since(start) }()
	rows, err := f.db.QueryContext(ctx,
		"SELECT external_id, content_type, data FROM "+assetsTable+" WHERE sync_id = ? ORDER BY id",
		syncID,
	)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var externalID, contentType string
		var data []byte
		if err := rows.Scan(&externalID, &contentType, &data); err != nil {
			return err
		}
		if err := dest.PutAsset(ctx, v2.AssetRef_builder{Id: externalID}.Build(), contentType, data); err != nil {
			return err
		}
		stage.Rows++
		*totalBytes += int64(len(data))
	}
	return rows.Err()
}

func (f *File) copySessions(
	ctx context.Context,
	dest *pebble.Engine,
	sourceSyncID, destSyncID string,
	stage *ConvertStageStats,
) error {
	start := time.Now()
	defer func() { stage.Duration = time.Since(start) }()
	rows, err := f.db.QueryContext(ctx,
		"SELECT key, value FROM "+sessionsTable+" WHERE sync_id = ? ORDER BY key",
		sourceSyncID,
	)
	if err != nil {
		return err
	}
	defer rows.Close()
	batch := make(map[string][]byte, sessionBatchKeys)
	batchBytes := 0
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := dest.SessionSetMany(ctx, batch, sessions.WithSyncID(destSyncID)); err != nil {
			return err
		}
		stage.Rows += int64(len(batch))
		batch = make(map[string][]byte, sessionBatchKeys)
		batchBytes = 0
		return nil
	}
	for rows.Next() {
		var key string
		var value []byte
		if err := rows.Scan(&key, &value); err != nil {
			return err
		}
		batch[key] = value
		batchBytes += len(key) + len(value)
		if len(batch) >= sessionBatchKeys || batchBytes >= sessionBatchBytes {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return flush()
}
