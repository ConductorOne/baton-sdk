package dotc1z

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/doug-martin/goqu/v9"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	"github.com/conductorone/baton-sdk/pkg/uotel"
)

// defaultConvertBatchSize is the number of records buffered per bulk-import
// call when streaming a sync into the destination engine. Matches the
// clone/copy batch conventions elsewhere in the package.
const defaultConvertBatchSize = 10000

// ConvertOption configures ToPebble.
type ConvertOption func(*convertConfig)

type convertConfig struct {
	batchSize   int
	tmpDir      string
	parallelism int
}

// WithConvertBatchSize sets the per-batch size. Values <= 0 are ignored.
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

// WithConvertParallelism sets the conversion's scan-lane fan-out (each
// lane holds one sqlite connection plus a reader and a decode/encode
// goroutine). The default — min(4, GOMAXPROCS/2) — leaves headroom for
// shared infrastructure; callers that own the machine can raise it, and
// 1 fully serializes the grant scan. Values <= 0 are ignored.
func WithConvertParallelism(n int) ConvertOption {
	return func(c *convertConfig) {
		if n > 0 {
			c.parallelism = n
		}
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

// syncIDPreservingStarter is the optional destination capability ToPebble
// needs to write the converted sync under the source's sync_id rather than a
// freshly-minted one. The Pebble adapter implements it.
type syncIDPreservingStarter interface {
	StartNewSyncWithID(ctx context.Context, syncType connectorstore.SyncType, syncID, parentSyncID string) (string, error)
}

// ToPebble converts a single finished sync from this SQLite store into a new
// v3/Pebble .c1z written to outPath, which must not already exist.
//
// It uses the engine's BulkSyncImport SST fast path: each record table is
// streamed out of SQLite once, in primary-key order via `ORDER BY` on the
// key's tuple columns (SQLite BINARY collation is bytewise, and the engine's
// tuple key codec is order-preserving, so SQL order == encoded-key order —
// enforced at runtime by the importer's strictly-increasing check). Primary
// records stream straight into one sorted SST per bucket; secondary index
// keys are derived from the translated records and externally sorted into
// one index SST; everything is ingested in a single pebble Ingest. No
// memtable, no WAL, no L0 flush, no background compaction debt.
//
// SQLite's UNIQUE(external_id, sync_id) indexes provide the no-duplicates
// guarantee the importer requires.
//
// syncID selects the source sync to convert; "" prefers the latest finished
// full sync and otherwise falls back to the latest full sync of any state.
// ToPebble does NOT validate that the sync is a complete snapshot, nor that it
// is ended — the caller owns that decision (e.g. compacting
// targeted-partial/diff syncs into a complete snapshot beforehand, or
// converting an in-progress/unexpanded sync to seed a fixture). It only
// requires the sync to exist. The destination sync is always written ended.
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
		syncID, err = c.resolveConvertSyncID(ctx)
		if err != nil {
			return nil, err
		}
		if syncID == "" {
			return nil, fmt.Errorf("to-pebble: no full sync found to convert")
		}
	}

	sync, err := c.getSync(ctx, syncID)
	if err != nil {
		return nil, err
	}
	if sync == nil {
		return nil, fmt.Errorf("to-pebble: sync %q not found", syncID)
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

	// Preserve the source sync's identity: the converted file describes
	// the same snapshot, so its sync_id must match. Without this the dest
	// would get a freshly-minted id and callers could no longer correlate
	// the two (or address the pebble file by the id they used for the
	// sqlite source).
	starter, ok := dest.(syncIDPreservingStarter)
	if !ok {
		return nil, errors.New("to-pebble: destination does not support preserving the source sync id")
	}
	destSyncID, err := starter.StartNewSyncWithID(ctx, sync.Type, syncID, "")
	if err != nil {
		return nil, fmt.Errorf("to-pebble: start destination sync: %w", err)
	}
	stats.DestSyncID = destSyncID

	destEng, ok := pebble.AsEngine(dest)
	if !ok {
		return nil, errors.New("to-pebble: destination store is not a pebble engine")
	}
	bi, err := destEng.StartBulkSyncImport(ctx, destSyncID, cfg.tmpDir)
	if err != nil {
		return nil, fmt.Errorf("to-pebble: start bulk import: %w", err)
	}
	imported := false
	defer func() {
		if !imported {
			bi.Abort()
		}
	}()

	if err = c.convertResourceTypes(ctx, bi, syncID, cfg.batchSize, &stats.ResourceTypes); err != nil {
		return nil, fmt.Errorf("to-pebble: resource types: %w", err)
	}
	if err = c.convertResources(ctx, bi, syncID, cfg.batchSize, &stats.Resources); err != nil {
		return nil, fmt.Errorf("to-pebble: resources: %w", err)
	}
	if err = c.convertEntitlements(ctx, bi, syncID, cfg.batchSize, &stats.Entitlements); err != nil {
		return nil, fmt.Errorf("to-pebble: entitlements: %w", err)
	}
	if err = c.convertGrants(ctx, bi, syncID, cfg, &stats.Grants); err != nil {
		return nil, fmt.Errorf("to-pebble: grants: %w", err)
	}
	if err = bi.Finish(ctx); err != nil {
		return nil, fmt.Errorf("to-pebble: ingest: %w", err)
	}
	imported = true

	if err = c.copyAssets(ctx, dest, syncID, &stats.Assets, &stats.AssetBytes); err != nil {
		return nil, fmt.Errorf("to-pebble: assets: %w", err)
	}

	// The import counted every record it wrote; stash that as the sync's
	// stats sidecar so EndSync persists it directly instead of re-scanning
	// the freshly ingested keyspaces.
	statsRec := bi.ComputedStats()
	statsRec.SetAssets(stats.Assets.Rows)
	destEng.StashComputedSyncStats(destSyncID, statsRec)

	endSyncStart := time.Now()
	if err = dest.EndSync(ctx); err != nil {
		return nil, fmt.Errorf("to-pebble: end destination sync: %w", err)
	}
	endSyncDur := time.Since(endSyncStart)
	closeStart := time.Now()
	if err = dest.Close(ctx); err != nil {
		cleanupDest = false
		_ = os.Remove(outPath)
		return nil, fmt.Errorf("to-pebble: close destination: %w", err)
	}
	closeDur := time.Since(closeStart)
	cleanupDest = false
	l.Debug("to-pebble: destination finalize timings",
		zap.Duration("end_sync", endSyncDur),
		zap.Duration("close_save", closeDur),
	)

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

// resolveConvertSyncID picks the source sync for a default ("") conversion.
// It prefers the latest finished full sync (the normal case), and otherwise
// falls back to the latest full sync of any state so an in-progress or
// unexpanded sync can still be converted (e.g. to seed a benchmark fixture).
// Unlike getLatestUnfinishedSync this has no recency cutoff, so old checked-in
// fixtures resolve too.
func (c *C1File) resolveConvertSyncID(ctx context.Context) (string, error) {
	syncID, err := c.LatestSyncID(ctx, connectorstore.SyncTypeFull)
	if err != nil {
		return "", err
	}
	if syncID != "" {
		return syncID, nil
	}

	q := c.db.From(syncRuns.Name()).Prepared(true).
		Select("sync_id").
		Where(goqu.C("sync_type").Eq(connectorstore.SyncTypeFull)).
		Order(goqu.C("started_at").Desc()).
		Limit(1)
	query, args, err := q.ToSQL()
	if err != nil {
		return "", err
	}
	var id string
	if err := c.db.QueryRowContext(ctx, query, args...).Scan(&id); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		return "", err
	}
	return id, nil
}

// scanRows executes q and invokes fn for each row with the raw *sql.Rows
// positioned on it. fn must Scan the row itself. Aborts on ctx cancellation.
func (c *C1File) scanRows(ctx context.Context, q *goqu.SelectDataset, fn func(rows *sql.Rows) error) error {
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
		if err := fn(rows); err != nil {
			return err
		}
	}
	return rows.Err()
}

// syncScope returns the base SELECT for one source table restricted to the
// sync being converted.
func (c *C1File) syncScope(table string, syncID string, cols ...any) *goqu.SelectDataset {
	return c.db.From(table).Prepared(true).Select(cols...).Where(goqu.C("sync_id").Eq(syncID))
}

func (c *C1File) convertResourceTypes(ctx context.Context, bi *pebble.BulkSyncImport, syncID string, batchSize int, stage *ConvertStageStats) error {
	start := time.Now()
	defer func() { stage.Duration = time.Since(start) }()

	batch := make([]*v2.ResourceType, 0, batchSize)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := bi.AddResourceTypes(ctx, batch...); err != nil {
			return err
		}
		stage.Rows += int64(len(batch))
		batch = batch[:0]
		return nil
	}
	// Key order: external_id (the v3 resource_type primary key tuple).
	q := c.syncScope(resourceTypes.Name(), syncID, "data").Order(goqu.C("external_id").Asc())
	err := c.scanRows(ctx, q, func(rows *sql.Rows) error {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			return err
		}
		m := &v2.ResourceType{}
		if err := proto.Unmarshal(data, m); err != nil {
			return err
		}
		batch = append(batch, m)
		if len(batch) >= batchSize {
			return flush()
		}
		return nil
	})
	if err != nil {
		return err
	}
	return flush()
}

func (c *C1File) convertResources(ctx context.Context, bi *pebble.BulkSyncImport, syncID string, batchSize int, stage *ConvertStageStats) error {
	start := time.Now()
	defer func() { stage.Duration = time.Since(start) }()

	// Primary records, in (resource_type_id, resource_id) key order. The
	// sqlite external_id is "<rt>:<rid>", so for a fixed resource_type_id
	// ordering by external_id equals ordering by resource_id.
	batch := make([]*v2.Resource, 0, batchSize)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := bi.AddResources(ctx, batch...); err != nil {
			return err
		}
		stage.Rows += int64(len(batch))
		batch = batch[:0]
		return nil
	}
	q := c.syncScope(resources.Name(), syncID, "data").
		Order(goqu.C("resource_type_id").Asc(), goqu.C("external_id").Asc())
	err := c.scanRows(ctx, q, func(rows *sql.Rows) error {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			return err
		}
		m := &v2.Resource{}
		if err := proto.Unmarshal(data, m); err != nil {
			return err
		}
		batch = append(batch, m)
		if len(batch) >= batchSize {
			return flush()
		}
		return nil
	})
	if err != nil {
		return err
	}
	return flush()
}

func (c *C1File) convertEntitlements(ctx context.Context, bi *pebble.BulkSyncImport, syncID string, batchSize int, stage *ConvertStageStats) error {
	start := time.Now()
	defer func() { stage.Duration = time.Since(start) }()

	batch := make([]*v2.Entitlement, 0, batchSize)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := bi.AddEntitlements(ctx, batch...); err != nil {
			return err
		}
		stage.Rows += int64(len(batch))
		batch = batch[:0]
		return nil
	}
	// Primary records in external_id key order.
	q := c.syncScope(entitlements.Name(), syncID, "data").Order(goqu.C("external_id").Asc())
	err := c.scanRows(ctx, q, func(rows *sql.Rows) error {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			return err
		}
		m := &v2.Entitlement{}
		if err := proto.Unmarshal(data, m); err != nil {
			return err
		}
		batch = append(batch, m)
		if len(batch) >= batchSize {
			return flush()
		}
		return nil
	})
	if err != nil {
		return err
	}
	return flush()
}

// convertGrantScanLanes caps the parallel grant scan fan-out. Each lane
// holds one sqlite connection and one decode/encode pipeline for one
// external-id range. The default fan-out is half the available CPUs up
// to this cap — conversions usually run on shared workers, so leave
// headroom by default and let callers that own the machine raise it
// via WithConvertParallelism.
const convertGrantScanLanes = 4

// rawGrantRow is one grant row's raw column bytes, copied out of the
// scan into a batch-owned arena so decoding can happen on another
// goroutine after the scanner has moved on.
type rawGrantRow struct {
	data      []byte
	expansion []byte
}

// convertGrants streams the sync's grants into the bulk import. The
// scan shards by EXTERNAL ID range over the UNIQUE(external_id,
// sync_id) index: each lane's ordered range scan yields rows already in
// the shard's final pebble key order, so grant primaries stream
// straight into one final SST per lane — no spill, no external sort, no
// merge (see BulkGrantShard). Range boundaries come from sampling
// external ids at random rowids and taking quantiles; uneven lanes only
// cost balance, never correctness, and pebble's Ingest rejects
// overlapping shard SSTs outright.
//
// Each lane is a two-stage pipeline: a reader goroutine does nothing
// but step rows and memcpy the raw (data, expansion) column bytes into
// batches, and a worker goroutine decodes the v2 grants, re-attaches
// the GrantExpandable side column, and feeds the lane's import shard
// (translate, v3 marshal, key encode — no shared locks).
func (c *C1File) convertGrants(ctx context.Context, bi *pebble.BulkSyncImport, syncID string, cfg *convertConfig, stage *ConvertStageStats) error {
	start := time.Now()
	defer func() { stage.Duration = time.Since(start) }()
	batchSize := cfg.batchSize

	var minID, maxID sql.NullInt64
	boundsQ, boundsArgs, err := c.db.From(grants.Name()).Prepared(true).
		Select(goqu.MIN("id"), goqu.MAX("id")).
		Where(goqu.C("sync_id").Eq(syncID)).ToSQL()
	if err != nil {
		return err
	}
	if err := c.db.QueryRowContext(ctx, boundsQ, boundsArgs...).Scan(&minID, &maxID); err != nil {
		return err
	}
	if !minID.Valid {
		return nil // no grants in this sync
	}

	lanes := min(convertGrantScanLanes, max(1, runtime.GOMAXPROCS(0)/2))
	if cfg.parallelism > 0 {
		lanes = cfg.parallelism
	}
	if lanes < 1 {
		lanes = 1
	}
	// The C1File's own pool is capped at one connection (WAL checkpoint
	// hygiene — see NewC1File) and defaults to locking_mode=EXCLUSIVE,
	// which holds its lock indefinitely once acquired and would starve a
	// second reader. Downgrade to NORMAL and touch the db once so the
	// persistent lock releases; per-transaction locking is fine for the
	// remainder of the conversion (and the source is closed right after
	// in the convert-open flow). If the extra readers still can't attach,
	// fall back to a single-connection scan on the main pool.
	scanDB := c.rawDb
	if lanes > 1 {
		if _, err := c.rawDb.ExecContext(ctx, "PRAGMA main.locking_mode = NORMAL"); err == nil {
			var n int
			_ = c.rawDb.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_master").Scan(&n)
		}
		pool, err := sql.Open("sqlite", c.dbFilePath)
		if err != nil {
			return fmt.Errorf("open scan pool: %w", err)
		}
		pool.SetMaxOpenConns(lanes)
		var n int
		if err := pool.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_master").Scan(&n); err != nil {
			ctxzap.Extract(ctx).Warn("to-pebble: parallel grant scan unavailable; falling back to single connection", zap.Error(err))
			_ = pool.Close()
			lanes = 1
		} else {
			scanDB = pool
			defer pool.Close()
		}
	}

	bounds, err := sampleGrantBoundaries(ctx, scanDB, grants.Name(), syncID, minID.Int64, maxID.Int64, lanes)
	if err != nil {
		return err
	}

	scanCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var (
		errMu    sync.Mutex
		firstErr error
		rowCount atomic.Int64
	)
	fail := func(err error) {
		errMu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		errMu.Unlock()
		cancel()
	}

	// One lane per external-id range: a reader goroutine streaming raw
	// rows in index order into a buffered channel, and a worker
	// goroutine decoding + appending to the lane's ordered shard. The
	// channel is FIFO with a single consumer, so the shard sees rows in
	// exactly the index order the reader produced.
	//
	// The table name below is the package-internal grants descriptor,
	// not user input; all user-controlled values are bound parameters.
	var laneWG sync.WaitGroup
	for l := 0; l < len(bounds)+1; l++ {
		var loExt, hiExt string
		if l > 0 {
			loExt = bounds[l-1]
		}
		if l < len(bounds) {
			hiExt = bounds[l]
		}
		shard, err := bi.NewGrantShard()
		if err != nil {
			cancel()
			laneWG.Wait()
			return err
		}

		query := "SELECT data, expansion FROM " + grants.Name() + " WHERE sync_id = ?" // #nosec G202 - internal table name.
		args := []any{syncID}
		if loExt != "" {
			query += " AND external_id >= ?"
			args = append(args, loExt)
		}
		if hiExt != "" {
			query += " AND external_id < ?"
			args = append(args, hiExt)
		}
		query += " ORDER BY external_id"

		rawCh := make(chan []rawGrantRow, 2)
		laneWG.Add(2)
		go func(shard *pebble.BulkGrantShard, rawCh <-chan []rawGrantRow) {
			defer laneWG.Done()
			defer shard.Close()
			batch := make([]*v2.Grant, 0, batchSize)
			for raw := range rawCh {
				if scanCtx.Err() != nil {
					continue // drain
				}
				batch = batch[:0]
				for i := range raw {
					g := &v2.Grant{}
					if err := proto.Unmarshal(raw[i].data, g); err != nil {
						fail(err)
						break
					}
					if _, err := reattachExpansion(g, raw[i].expansion); err != nil {
						fail(err)
						break
					}
					batch = append(batch, g)
				}
				if scanCtx.Err() != nil {
					continue
				}
				if err := shard.AddGrants(scanCtx, batch...); err != nil {
					fail(err)
					continue
				}
				rowCount.Add(int64(len(batch)))
			}
		}(shard, rawCh)
		go func(query string, args []any, rawCh chan<- []rawGrantRow) {
			defer laneWG.Done()
			defer close(rawCh)
			var arena []byte
			batch := make([]rawGrantRow, 0, batchSize)
			emit := func() {
				if len(batch) == 0 {
					return
				}
				out := batch
				select {
				case rawCh <- out:
				case <-scanCtx.Done():
				}
				arena = nil
				batch = make([]rawGrantRow, 0, batchSize)
			}
			rows, err := scanDB.QueryContext(scanCtx, query, args...)
			if err != nil {
				fail(err)
				return
			}
			defer rows.Close()
			for rows.Next() {
				if scanCtx.Err() != nil {
					return
				}
				var data, expansion sql.RawBytes
				if err := rows.Scan(&data, &expansion); err != nil {
					fail(err)
					return
				}
				// Copy out of the driver's row buffer; the arena keeps
				// the batch to two allocations instead of two per row.
				off := len(arena)
				arena = append(arena, data...)
				arena = append(arena, expansion...)
				row := rawGrantRow{data: arena[off : off+len(data) : off+len(data)]}
				if len(expansion) > 0 {
					row.expansion = arena[off+len(data) : off+len(data)+len(expansion)]
				}
				batch = append(batch, row)
				if len(batch) >= batchSize {
					emit()
				}
			}
			if err := rows.Err(); err != nil {
				fail(err)
				return
			}
			emit()
		}(query, args, rawCh)
	}
	laneWG.Wait()
	if firstErr != nil {
		return firstErr
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	stage.Rows = rowCount.Load()
	return nil
}

// sampleGrantBoundaries picks up to lanes-1 external-id range boundaries
// by sampling the external ids at random rowids in [minID, maxID] and
// taking quantiles. Boundaries partition the scan only for balance —
// duplicates are collapsed, and a degenerate sample just means fewer
// lanes. Point lookups by rowid keep this O(lanes·samples) regardless of
// table size.
func sampleGrantBoundaries(ctx context.Context, db *sql.DB, table, syncID string, minID, maxID int64, lanes int) ([]string, error) {
	if lanes <= 1 || maxID <= minID {
		return nil, nil
	}
	const samplesPerLane = 32
	sampleCount := lanes * samplesPerLane
	span := maxID - minID + 1

	// The table name is the package-internal grants descriptor, not user
	// input.
	query := "SELECT external_id FROM " + table + " WHERE id >= ? AND sync_id = ? ORDER BY id LIMIT 1" // #nosec G202
	rng := rand.New(rand.NewSource(1))                                                                 //nolint:gosec // deterministic sampling for shard balance, not security.
	samples := make([]string, 0, sampleCount)
	for i := 0; i < sampleCount; i++ {
		id := minID + rng.Int63n(span)
		var ext string
		err := db.QueryRowContext(ctx, query, id, syncID).Scan(&ext)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return nil, err
		}
		samples = append(samples, ext)
	}
	if len(samples) == 0 {
		return nil, nil
	}
	sort.Strings(samples)
	bounds := make([]string, 0, lanes-1)
	for l := 1; l < lanes; l++ {
		b := samples[l*len(samples)/lanes]
		if len(bounds) > 0 && bounds[len(bounds)-1] >= b {
			continue
		}
		bounds = append(bounds, b)
	}
	return bounds, nil
}

// copyAssets enumerates the assets stored under syncID and writes each to the
// destination's current sync. Assets are keyed by (external_id, sync_id) and
// stored as opaque blobs; there is no streaming list primitive, so this reads
// them directly. A sync with no assets is a no-op. Assets go through the
// regular PutAsset write path (not the SST import) — they are rare and small.
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
