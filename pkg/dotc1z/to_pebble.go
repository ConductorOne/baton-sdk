package dotc1z

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"iter"
	"os"
	"time"

	"github.com/doug-martin/goqu/v9"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/uotel"
)

// uniqueGrantWriter is an optional capability a destination store may
// implement for trusted one-shot imports. It is unsafe for live connector
// writes: it skips read-before-write and dedup. It is only safe for a fresh
// destination sync when the import source guarantees each grant external_id
// appears at most once across the whole sync. ToPebble gets that guarantee from
// SQLite's UNIQUE(external_id, sync_id) index on the finished source sync. The
// Pebble engine implements it; SQLite does not, so ToPebble falls back to
// PutGrants.
type uniqueGrantWriter interface {
	UnsafePutUniqueGrants(ctx context.Context, grants ...*v2.Grant) error
}

// defaultConvertBatchSize is the number of records buffered per Put* call
// when streaming a sync into the destination engine. Matches the clone/copy
// batch conventions elsewhere in the package.
const defaultConvertBatchSize = 10000

// ConvertOption configures ToPebble.
type ConvertOption func(*convertConfig)

type convertConfig struct {
	batchSize int
	tmpDir    string
}

// WithConvertBatchSize sets the per-Put* batch size. Values <= 0 are ignored.
func WithConvertBatchSize(n int) ConvertOption {
	return func(c *convertConfig) {
		if n > 0 {
			c.batchSize = n
		}
	}
}

// WithConvertTmpDir sets the temp directory used for the destination engine's
// working files. Defaults to the source store's temp dir.
func WithConvertTmpDir(dir string) ConvertOption {
	return func(c *convertConfig) {
		c.tmpDir = dir
	}
}

// ConvertStageStats records the row count and wall-clock for one copy stage.
type ConvertStageStats struct {
	Rows     int64
	Duration time.Duration
}

// ConvertStats is the per-stage instrumentation returned by ToPebble so the
// caller can see exactly where time and volume land on a real conversion.
type ConvertStats struct {
	SourceSyncID  string
	DestSyncID    string
	ResourceTypes ConvertStageStats
	Resources     ConvertStageStats
	Entitlements  ConvertStageStats
	Grants        ConvertStageStats
	Assets        ConvertStageStats
	AssetBytes    int64
	Total         time.Duration
}

// ToPebble converts a single finished sync from this SQLite store into a new
// v3/Pebble .c1z written to outPath, which must not already exist. It streams
// the sync's data (resource types, resources, entitlements, grants, assets)
// into a fresh SyncTypeFull on the destination, using the engine's fresh-sync
// fast write path (NoSync per commit, single fsync at EndSync).
//
// syncID selects the source sync to convert; "" uses the latest finished full
// sync. ToPebble does NOT validate that the sync is a complete snapshot — the
// caller owns that decision (e.g. compacting targeted-partial/diff syncs into a
// complete snapshot beforehand). It only requires the sync to exist and be
// ended, mirroring CloneSync.
//
// The Pebble engine is registered statically with dotc1z; no extra
// imports are needed before calling.
func (c *C1File) ToPebble(ctx context.Context, outPath string, syncID string, opts ...ConvertOption) (*ConvertStats, error) {
	ctx, span := tracer.Start(ctx, "C1File.ToPebble")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	cfg := &convertConfig{
		batchSize: defaultConvertBatchSize,
		tmpDir:    c.tempDir,
	}
	for _, o := range opts {
		o(cfg)
	}

	if err = c.validateDb(ctx); err != nil {
		return nil, err
	}

	if _, statErr := os.Stat(outPath); statErr == nil {
		return nil, fmt.Errorf("to-pebble: output path (%s) must not exist", outPath)
	} else if !errors.Is(statErr, fs.ErrNotExist) {
		return nil, fmt.Errorf("to-pebble: stat output path %s: %w", outPath, statErr)
	}

	if syncID == "" {
		syncID, err = c.LatestSyncID(ctx, connectorstore.SyncTypeFull)
		if err != nil {
			return nil, err
		}
		if syncID == "" {
			return nil, fmt.Errorf("to-pebble: no finished full sync found to convert")
		}
	}

	sync, err := c.getSync(ctx, syncID)
	if err != nil {
		return nil, err
	}
	if sync == nil {
		return nil, fmt.Errorf("to-pebble: sync %q not found", syncID)
	}
	if sync.EndedAt == nil {
		return nil, fmt.Errorf("to-pebble: sync %q is not ended", syncID)
	}

	stats := &ConvertStats{SourceSyncID: syncID}
	start := time.Now()
	l := ctxzap.Extract(ctx)

	dest, err := NewStore(ctx, outPath, WithEngine(EnginePebble), WithTmpDir(cfg.tmpDir))
	if err != nil {
		return nil, fmt.Errorf("to-pebble: open destination: %w", err)
	}
	// On any failure after open, close the destination and remove the
	// partially-written output so the operation is atomic from the caller's
	// perspective.
	cleanupDest := true
	defer func() {
		if cleanupDest {
			_ = dest.Close(ctx)
			_ = os.Remove(outPath)
		}
	}()

	destSyncID, err := dest.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	if err != nil {
		return nil, fmt.Errorf("to-pebble: start destination sync: %w", err)
	}
	stats.DestSyncID = destSyncID

	if err = c.copyResourceTypes(ctx, dest, syncID, cfg.batchSize, &stats.ResourceTypes); err != nil {
		return nil, fmt.Errorf("to-pebble: resource types: %w", err)
	}
	if err = c.copyResources(ctx, dest, syncID, cfg.batchSize, &stats.Resources); err != nil {
		return nil, fmt.Errorf("to-pebble: resources: %w", err)
	}
	if err = c.copyEntitlements(ctx, dest, syncID, cfg.batchSize, &stats.Entitlements); err != nil {
		return nil, fmt.Errorf("to-pebble: entitlements: %w", err)
	}
	if err = c.copyGrants(ctx, dest, syncID, cfg.batchSize, &stats.Grants); err != nil {
		return nil, fmt.Errorf("to-pebble: grants: %w", err)
	}
	if err = c.copyAssets(ctx, dest, syncID, &stats.Assets, &stats.AssetBytes); err != nil {
		return nil, fmt.Errorf("to-pebble: assets: %w", err)
	}

	if err = dest.EndSync(ctx); err != nil {
		return nil, fmt.Errorf("to-pebble: end destination sync: %w", err)
	}
	if err = dest.Close(ctx); err != nil {
		cleanupDest = false
		_ = os.Remove(outPath)
		return nil, fmt.Errorf("to-pebble: close destination: %w", err)
	}
	cleanupDest = false

	stats.Total = time.Since(start)
	l.Info("to-pebble: conversion complete",
		zap.String("source_sync_id", stats.SourceSyncID),
		zap.String("dest_sync_id", stats.DestSyncID),
		zap.Int64("resource_types", stats.ResourceTypes.Rows),
		zap.Int64("resources", stats.Resources.Rows),
		zap.Int64("entitlements", stats.Entitlements.Rows),
		zap.Int64("grants", stats.Grants.Rows),
		zap.Int64("assets", stats.Assets.Rows),
		zap.Int64("asset_bytes", stats.AssetBytes),
		zap.Duration("total", stats.Total),
	)

	return stats, nil
}

func (c *C1File) copyResourceTypes(ctx context.Context, dest connectorstore.Writer, syncID string, batchSize int, stage *ConvertStageStats) error {
	start := time.Now()
	defer func() { stage.Duration = time.Since(start) }()

	batch := make([]*v2.ResourceType, 0, batchSize)
	pageToken := ""
	for {
		req := v2.ResourceTypesServiceListResourceTypesRequest_builder{
			ActiveSyncId: syncID,
			PageToken:    pageToken,
		}.Build()
		resp, err := c.ListResourceTypes(ctx, req)
		if err != nil {
			return err
		}
		for _, rt := range resp.GetList() {
			batch = append(batch, rt)
			if len(batch) >= batchSize {
				if err := dest.PutResourceTypes(ctx, batch...); err != nil {
					return err
				}
				stage.Rows += int64(len(batch))
				batch = batch[:0]
			}
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	if len(batch) > 0 {
		if err := dest.PutResourceTypes(ctx, batch...); err != nil {
			return err
		}
		stage.Rows += int64(len(batch))
	}
	return nil
}

func (c *C1File) copyResources(ctx context.Context, dest connectorstore.Writer, syncID string, batchSize int, stage *ConvertStageStats) error {
	return copyStream(ctx, c.StreamResources(ctx, syncID, connectorstore.StreamResourcesOptions{}), batchSize, stage, dest.PutResources)
}

func (c *C1File) copyEntitlements(ctx context.Context, dest connectorstore.Writer, syncID string, batchSize int, stage *ConvertStageStats) error {
	return copyStream(ctx, c.StreamEntitlements(ctx, syncID), batchSize, stage, dest.PutEntitlements)
}

func (c *C1File) copyGrants(ctx context.Context, dest connectorstore.Writer, syncID string, batchSize int, stage *ConvertStageStats) error {
	// Prefer the unsafe trusted-import fast path when the destination supports it. The
	// source sync is finished and SQLite enforces UNIQUE(external_id, sync_id),
	// which is exactly the global uniqueness guarantee this path requires.
	put := dest.PutGrants
	if fw, ok := dest.(uniqueGrantWriter); ok {
		put = fw.UnsafePutUniqueGrants
	}
	return copyStream(ctx, c.StreamGrants(ctx, syncID, connectorstore.StreamGrantsOptions{}), batchSize, stage, put)
}

// copyStream drains a streaming reader into the destination in batches of
// batchSize, recording the row count and wall-clock on stage. put is the
// destination's bulk writer for T (e.g. dest.PutResources).
func copyStream[T any](
	ctx context.Context,
	seq iter.Seq2[T, error],
	batchSize int,
	stage *ConvertStageStats,
	put func(context.Context, ...T) error,
) error {
	start := time.Now()
	defer func() { stage.Duration = time.Since(start) }()

	batch := make([]T, 0, batchSize)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := put(ctx, batch...); err != nil {
			return err
		}
		stage.Rows += int64(len(batch))
		batch = batch[:0]
		return nil
	}
	for item, err := range seq {
		if err != nil {
			return err
		}
		batch = append(batch, item)
		if len(batch) >= batchSize {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	return flush()
}

// copyAssets enumerates the assets stored under syncID and writes each to the
// destination's current sync. Assets are keyed by (external_id, sync_id) and
// stored as opaque blobs; there is no streaming list primitive, so this reads
// them directly. A sync with no assets is a no-op.
func (c *C1File) copyAssets(ctx context.Context, dest connectorstore.Writer, syncID string, stage *ConvertStageStats, totalBytes *int64) error {
	start := time.Now()
	defer func() { stage.Duration = time.Since(start) }()

	q := c.db.From(assets.Name()).Prepared(true).
		Select("external_id", "content_type", "data").
		Where(goqu.C("sync_id").Eq(syncID)).
		Order(goqu.C("id").Asc())
	query, args, err := q.ToSQL()
	if err != nil {
		return err
	}
	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		var externalID, contentType string
		var data []byte
		if err := rows.Scan(&externalID, &contentType, &data); err != nil {
			return err
		}
		ref := v2.AssetRef_builder{Id: externalID}.Build()
		if err := dest.PutAsset(ctx, ref, contentType, data); err != nil {
			return err
		}
		stage.Rows++
		*totalBytes += int64(len(data))
	}
	return rows.Err()
}
