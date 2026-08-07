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
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/doug-martin/goqu/v9"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	"github.com/conductorone/baton-sdk/pkg/types/sessions"
	"github.com/conductorone/baton-sdk/pkg/uotel"
)

// defaultConvertBatchSize is the number of records buffered per bulk-import
// call when streaming a sync into the destination engine. Matches the
// clone/copy batch conventions elsewhere in the package.
const defaultConvertBatchSize = 10000

// ConvertOption configures ToPebble.
type ConvertOption func(*convertConfig)

// ConvertResolveBehavior selects which sync "" resolves to in ToPebble.
type ConvertResolveBehavior string

const (
	// ConvertResolveBehaviorNewest picks the convertible sync (full,
	// resources_only, or partial) with the latest started_at, finished or
	// not. This is the default and is what convert-open uses so an in-progress
	// sync can migrate without being dropped. The one exception: an unfinished
	// sync past the resume cutoff loses to any finished sync, because it can
	// neither be resumed nor read as a snapshot — see resolveConvertSyncID.
	//
	// A Pebble c1z holds one sync, so every other sync in the source is
	// dropped — including an older finished full when a newer partial exists.
	// See convertSQLiteC1ZToPebble for why that tradeoff is deliberate on the
	// in-place path, where the dropped syncs cannot be recovered.
	ConvertResolveBehaviorNewest ConvertResolveBehavior = "newest"
	// ConvertResolveBehaviorFullFinished picks the latest finished full sync.
	// Use this for tooling that reads full-sync snapshots (e.g. baton
	// to-pebble): a newer partial/resources_only would otherwise be selected
	// and omit entitlements/grants the tool expects.
	ConvertResolveBehaviorFullFinished ConvertResolveBehavior = "full_finished"
)

var validConvertResolveBehaviors = []ConvertResolveBehavior{
	ConvertResolveBehaviorNewest,
	ConvertResolveBehaviorFullFinished,
}

type convertConfig struct {
	batchSize       int
	tmpDir          string
	parallelism     int
	resolveBehavior ConvertResolveBehavior
}

// WithConvertResolveBehavior controls how syncID "" picks a source sync.
// Defaults to ConvertResolveBehaviorNewest when unset (including the zero
// value). Unrecognized values return an error.
func WithConvertResolveBehavior(behavior ConvertResolveBehavior) ConvertOption {
	return func(c *convertConfig) {
		if behavior == "" {
			behavior = ConvertResolveBehaviorNewest
		}
		c.resolveBehavior = behavior
	}
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

// DiscardedSync identifies a source sync that a conversion left behind. A
// Pebble c1z holds exactly one sync, so every sync in the source other than the
// selected one is dropped.
//
// Callers that overwrite the source (convert-open) should surface these: once
// the v1 file is replaced, this is the only record of what the artifact lost.
//
// The timestamps are absolute instants, localized out of the zone-less columns
// the same way the converted record's own timestamps are, so they can be
// compared against time.Now() directly.
type DiscardedSync struct {
	ID        string
	Type      connectorstore.SyncType
	StartedAt *time.Time
	// EndedAt is nil for a sync that never finished.
	EndedAt *time.Time
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
	// Sessions is zero for a finished source sync: its session rows are not
	// copied. See ToPebble.
	Sessions ConvertStageStats
	// DiscardedSyncs lists every source sync the conversion dropped, in
	// sync_runs order. Empty when the source held only the selected sync.
	DiscardedSyncs []DiscardedSync
	Total          time.Duration
}

// syncIDPreservingStarter is the optional destination capability ToPebble
// needs to write the converted sync under the source's sync_id rather than a
// freshly-minted one. The Pebble adapter implements it.
type syncIDPreservingStarter interface {
	StartNewSyncWithID(ctx context.Context, syncType connectorstore.SyncType, syncID, parentSyncID string) (string, error)
}

// ToPebble converts a single sync from this SQLite store into a new v3/Pebble
// .c1z written to outPath, which must not already exist.
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
// syncID selects the source sync to convert; the destination holds that one
// sync and nothing else, so every other sync in the source is dropped. Those
// are returned in ConvertStats.DiscardedSyncs and logged with the completion
// line, so a caller that replaces the source can report what the artifact lost.
// When syncID is "":
//   - WithConvertResolveBehavior(ConvertResolveBehaviorNewest) (default):
//     most recently started convertible sync (full, resources_only, or
//     partial), finished or not, except that an unfinished sync past the resume
//     cutoff is ranked behind every finished sync — see resolveConvertSyncID.
//     This can drop an older finished full sync in favor of a newer partial;
//     see convertSQLiteC1ZToPebble for why.
//   - WithConvertResolveBehavior(ConvertResolveBehaviorFullFinished):
//     latest finished full sync only (errors if none). Used by baton
//     to-pebble so read-oriented tooling does not land on a newer partial.
//
// When the source has no sync runs at all, "" writes an empty Pebble c1z (so
// convert-open succeeds on never-synced files). If sync runs exist but none
// match the selected resolve behavior (e.g. diff-only under Newest), ""
// returns an error rather than discarding data. Pass an explicit syncID to
// convert a specific sync (including diff syncs for fixture seeding). The
// destination sync is written ended when the source was finished; when the
// source was unfinished, EndSync still runs (indexes/digests/stats/flush) but
// ended_at is cleared and the source sync_token is preserved so the sync
// stays resumable. In both cases the source started_at is copied so the
// unfinished-sync age cutoff still applies after conversion (StartNewSync
// would otherwise stamp now and resurrect abandoned syncs). An unfinished
// source's connector_sessions rows are copied as well, so a resumed sync still
// sees the session state it checkpointed against; a finished source's are not,
// since nothing resumes a sealed sync and the connector lifecycle deletes them
// at that point anyway.
//
// The sync's lineage columns — parent_sync_id, linked_sync_id, supports_diff —
// are preserved too. They reference syncs that the single-sync destination
// cannot hold, but those references are meaningful across files: dropping them
// would make a converted partial read as a standalone snapshot and a
// diff-capable sync read as non-diffable.
//
// The Pebble engine is registered statically with dotc1z; no extra
// imports are needed before calling.
func (c *C1File) ToPebble(ctx context.Context, outPath string, syncID string, opts ...ConvertOption) (*ConvertStats, error) {
	ctx, span := tracer.Start(ctx, "C1File.ToPebble")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	cfg := &convertConfig{
		batchSize:       defaultConvertBatchSize,
		tmpDir:          c.tempDir,
		resolveBehavior: ConvertResolveBehaviorNewest,
	}
	for _, o := range opts {
		o(cfg)
	}
	if !slices.Contains(validConvertResolveBehaviors, cfg.resolveBehavior) {
		return nil, fmt.Errorf("to-pebble: unknown convert resolve behavior %q", cfg.resolveBehavior)
	}

	if err = c.validateDb(ctx); err != nil {
		return nil, err
	}

	if _, statErr := os.Stat(outPath); statErr == nil { // #nosec G703 -- conversion output path is caller-controlled by API design.
		return nil, fmt.Errorf("to-pebble: output path (%s) must not exist", outPath)
	} else if !errors.Is(statErr, fs.ErrNotExist) {
		return nil, fmt.Errorf("to-pebble: stat output path %s: %w", outPath, statErr)
	}

	if syncID == "" {
		// If there are no syncs in the source, write an empty Pebble c1z.
		var hasAnySyncRun bool
		hasAnySyncRun, err = c.hasAnySyncRun(ctx)
		if err != nil {
			return nil, fmt.Errorf("to-pebble: check for any sync runs: %w", err)
		}
		if !hasAnySyncRun {
			var emptyStats *ConvertStats
			emptyStats, err = c.convertEmptyToPebble(ctx, outPath, cfg)
			return emptyStats, err
		}

		switch cfg.resolveBehavior {
		case ConvertResolveBehaviorNewest:
			syncID, err = c.resolveConvertSyncID(ctx)
			if err != nil {
				return nil, fmt.Errorf("to-pebble: resolve convert sync id: %w", err)
			}
		case ConvertResolveBehaviorFullFinished:
			syncID, err = c.LatestFinishedSyncID(ctx, connectorstore.SyncTypeFull)
			if err != nil {
				return nil, fmt.Errorf("to-pebble: resolve convert sync id: %w", err)
			}
			if syncID == "" {
				return nil, status.Errorf(codes.NotFound, "no finished full sync found")
			}
		default:
			return nil, fmt.Errorf("to-pebble: unknown convert resolve behavior %q", cfg.resolveBehavior)
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

	// Record what this conversion drops before touching the destination, so
	// the caller can report it even though only the selected sync survives.
	stats.DiscardedSyncs, err = c.discardedSyncs(ctx, syncID)
	if err != nil {
		return nil, fmt.Errorf("to-pebble: list discarded syncs: %w", err)
	}

	dest, err := NewStore(ctx, outPath, WithEngine(c1zstore.EnginePebble), WithTmpDir(cfg.tmpDir))
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
			_ = os.Remove(outPath) // #nosec G703 -- cleanup of caller-selected conversion output.
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
	destSyncID, err := starter.StartNewSyncWithID(ctx, sync.Type, syncID, sync.ParentSyncID)
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

	// Sessions ride along only with a destination that stays resumable, which
	// is the same ended_at == nil condition the metadata overlay below uses to
	// decide that. On a finished sync they are scratch state no reader
	// consults: the connector's Cleanup deletes them at that point precisely
	// so they do not ship in the saved c1z (pkg/connectorbuilder), and nothing
	// can resume a sealed sync to read them back.
	if sync.EndedAt == nil {
		if err = c.copySessions(ctx, destEng, syncID, destSyncID, &stats.Sessions); err != nil {
			return nil, fmt.Errorf("to-pebble: sessions: %w", err)
		}
	}

	// The import counted every record it wrote; stash that as the sync's
	// stats sidecar so EndSync persists it directly instead of re-scanning
	// the freshly ingested keyspaces.
	statsRec := bi.ComputedStats()
	statsRec.SetAssets(stats.Assets.Rows)
	destEng.StashComputedSyncStats(destSyncID, statsRec)

	// EndSync always runs: bulk import still needs deferred indexes / grant
	// digests / stats sidecar / durability flush. We then overlay source
	// sync metadata: started_at (so the unfinished age cutoff still
	// applies and finished syncs keep started_at <= ended_at), and when
	// the source was unfinished, clear ended_at and restore sync_token
	// so the converted file stays resumable.
	endSyncStart := time.Now()
	if err = dest.EndSync(ctx); err != nil {
		return nil, fmt.Errorf("to-pebble: end destination sync: %w", err)
	}
	rec, err := destEng.GetSyncRunRecord(ctx, destSyncID)
	if err != nil {
		return nil, fmt.Errorf("to-pebble: load destination sync metadata: %w", err)
	}
	rec.SetLinkedSyncId(sync.LinkedSyncID)
	rec.SetSupportsDiff(sync.SupportsDiff)
	// Localized on the way in: these scanned wall clocks become absolute
	// instants in the Pebble record, and Pebble's resume cutoff compares
	// started_at against time.Now() (see localizeSQLiteTimestamp).
	if sync.StartedAt != nil {
		rec.SetStartedAt(timestamppb.New(localizeSQLiteTimestamp(*sync.StartedAt, time.Local)))
	}
	if sync.EndedAt != nil {
		rec.SetEndedAt(timestamppb.New(localizeSQLiteTimestamp(*sync.EndedAt, time.Local)))
		// Verification provenance only rides along with a FINISHED source:
		// a marker on an unfinished source (impossible through the writer
		// API, but representable in a hand-edited file) must not convert
		// into a sealed, verified destination.
		if sync.IsVerified() {
			rec.SetIngestInvariantGeneration(sync.Generation)
			rec.SetIngestInvariantCoverage(append([]string(nil), sync.Coverage...))
			rec.SetIngestInvariantMode(string(sync.Mode))
		}
	} else {
		rec.ClearEndedAt()
		rec.SetSyncToken(sync.SyncToken)
	}
	if err = destEng.PutSyncRunRecord(ctx, rec); err != nil {
		return nil, fmt.Errorf("to-pebble: preserve source sync metadata: %w", err)
	}
	endSyncDur := time.Since(endSyncStart)
	closeStart := time.Now()
	if err = dest.Close(ctx); err != nil {
		cleanupDest = false
		_ = os.Remove(outPath) // #nosec G703 -- cleanup of caller-selected conversion output.
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

	return stats, nil
}

// discardedSyncs lists the source syncs a conversion that keeps keepSyncID
// leaves behind, in sync_runs order. Diff-pair syncs are included: they are
// dropped from the artifact too, and their absence is what an operator chasing
// a missing delta needs to see.
//
// Metadata only. ListSyncRuns reads the sync_runs rows and parses the cached
// stats blob when one is present; unlike GetSync it never recomputes stats, so
// this cannot turn into an O(rows) scan on an unfinished sync.
func (c *C1File) discardedSyncs(ctx context.Context, keepSyncID string) ([]DiscardedSync, error) {
	var discarded []DiscardedSync
	pageToken := ""
	for {
		runs, nextPageToken, err := c.ListSyncRuns(ctx, pageToken, maxPageSize)
		if err != nil {
			return nil, err
		}
		for _, run := range runs {
			if run == nil || run.ID == keepSyncID {
				continue
			}
			discarded = append(discarded, DiscardedSync{
				ID:   run.ID,
				Type: run.Type,
				// Localized like the record timestamps written next
				// door, so the log line and anything an SDK caller
				// compares against time.Now() agree with them.
				StartedAt: localizeSQLiteTimestampPtr(run.StartedAt, time.Local),
				EndedAt:   localizeSQLiteTimestampPtr(run.EndedAt, time.Local),
			})
		}
		if nextPageToken == "" {
			return discarded, nil
		}
		pageToken = nextPageToken
	}
}

// discardedSyncFields renders the dropped syncs for a log line. The count is
// always emitted so "nothing was dropped" is visible rather than inferred from
// an absent field.
func discardedSyncFields(discarded []DiscardedSync) []zap.Field {
	descriptors := make([]string, 0, len(discarded))
	for _, d := range discarded {
		startedAt := "unknown"
		if d.StartedAt != nil {
			startedAt = d.StartedAt.Format(time.RFC3339)
		}
		finished := "false"
		if d.EndedAt != nil {
			finished = "true"
		}
		descriptors = append(descriptors, fmt.Sprintf("id=%s type=%s started_at=%s finished=%s", d.ID, d.Type, startedAt, finished))
	}
	return []zap.Field{
		zap.Int("discarded_sync_count", len(discarded)),
		zap.Strings("discarded_syncs", descriptors),
	}
}

// resolveConvertSyncID implements ConvertResolveBehaviorNewest: the
// convertible sync (full, resources_only, or partial) with the latest
// started_at (sync_id DESC tie-break), whether finished or unfinished —
// except that an unfinished sync past the resume cutoff ranks below every
// other candidate.
//
// A sync that started more than unfinishedSyncMaxAge ago and never ended is
// abandoned work: neither engine will resume it (getLatestUnfinishedSync here,
// LatestUnfinishedSyncRecord in Pebble) and it has no ended_at to be read as a
// snapshot. Converting one in preference to a finished sync would produce a
// file that can be neither resumed nor read, so finished syncs win over stale
// unfinished ones regardless of started_at. A stale unfinished sync is still
// chosen when it is all there is (newest started_at among them), since
// convert-open must not fail on such a file. Unfinished syncs within the
// cutoff are live work and keep competing on started_at alone.
//
// The excluded types are the diff pair written by attached-file diffing,
// partial_upserts and partial_deletions: each holds one side of a delta and
// is meaningless converted alone. GenerateSyncDiff's delta sync is NOT
// excluded — it is stored as a plain partial (diff.go), indistinguishable
// from a targeted sync by type, parent_sync_id, or supports_diff — so on a
// file that was just diffed and holds no newer sync, "" resolves to the
// delta.
func (c *C1File) resolveConvertSyncID(ctx context.Context) (string, error) {
	q := c.db.From(syncRuns.Name()).Prepared(true).
		Select("sync_id").
		Where(goqu.C("sync_type").In(
			connectorstore.SyncTypeFull,
			connectorstore.SyncTypeResourcesOnly,
			connectorstore.SyncTypePartial,
		)).
		Order(
			// SQLite renders the boolean as 1/0, so DESC puts the
			// still-useful candidates (finished, or unfinished and
			// resumable) ahead of the abandoned ones.
			goqu.L("(ended_at is not null or started_at >= ?)", unfinishedSyncCutoff()).Desc(),
			goqu.C("started_at").Desc(),
			goqu.C("sync_id").Desc(),
		).
		Limit(1)
	query, args, err := q.ToSQL()
	if err != nil {
		return "", err
	}
	var syncID string
	if err := c.db.QueryRowContext(ctx, query, args...).Scan(&syncID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", status.Errorf(codes.NotFound, "no convertible sync found")
		}
		return "", err
	}
	return syncID, nil
}

// hasAnySyncRun reports whether the SQLite source has at least one sync_runs row.
func (c *C1File) hasAnySyncRun(ctx context.Context) (bool, error) {
	q := c.db.From(syncRuns.Name()).Prepared(true).Select(goqu.L("1")).Limit(1)
	query, args, err := q.ToSQL()
	if err != nil {
		return false, err
	}
	var one int
	if err := c.db.QueryRowContext(ctx, query, args...).Scan(&one); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// convertEmptyToPebble writes a valid empty Pebble c1z (no sync runs).
// Caller must ensure the source has no sync_runs rows.
func (c *C1File) convertEmptyToPebble(ctx context.Context, outPath string, cfg *convertConfig) (*ConvertStats, error) {
	start := time.Now()

	dest, err := NewStore(ctx, outPath, WithEngine(c1zstore.EnginePebble), WithTmpDir(cfg.tmpDir))
	if err != nil {
		return nil, fmt.Errorf("to-pebble: open destination: %w", err)
	}
	cleanupDest := true
	defer func() {
		if cleanupDest {
			_ = dest.Close(ctx)
			_ = os.Remove(outPath) // #nosec G703 -- cleanup of caller-selected conversion output.
		}
	}()

	// A fresh store is not dirty until something writes. Force the envelope
	// save so Close materializes an empty v3 c1z at outPath.
	if !pebble.MarkStoreDirty(dest) {
		return nil, errors.New("to-pebble: destination does not support dirty marking")
	}
	if err = dest.Close(ctx); err != nil {
		cleanupDest = false
		_ = os.Remove(outPath) // #nosec G703 -- cleanup of caller-selected conversion output.
		return nil, fmt.Errorf("to-pebble: close destination: %w", err)
	}
	cleanupDest = false

	stats := &ConvertStats{Total: time.Since(start)}
	ctxzap.Extract(ctx).Info("to-pebble: wrote empty pebble c1z (source had no sync runs)",
		zap.Duration("total", stats.Total),
	)
	return stats, nil
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

// discoveredAtTimestamp converts a scanned discovered_at column value to
// a per-record timestamp for the bulk import. A zero time yields nil so
// the importer falls back to its own now-stamp.
//
// The scanned value is localized first: it becomes an absolute instant here, and
// compaction compares those instants to pick record winners (see
// localizeSQLiteTimestamp).
func discoveredAtTimestamp(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(localizeSQLiteTimestamp(t, time.Local))
}

func (c *C1File) convertResourceTypes(ctx context.Context, bi *pebble.BulkSyncImport, syncID string, batchSize int, stage *ConvertStageStats) error {
	start := time.Now()
	defer func() { stage.Duration = time.Since(start) }()

	batch := make([]*v2.ResourceType, 0, batchSize)
	discovered := make([]*timestamppb.Timestamp, 0, batchSize)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := bi.AddResourceTypesWithDiscoveredAt(ctx, batch, discovered); err != nil {
			return err
		}
		stage.Rows += int64(len(batch))
		batch = batch[:0]
		discovered = discovered[:0]
		return nil
	}
	// Key order: external_id (the v3 resource_type primary key tuple).
	// discovered_at rides along so the converted record keeps the source
	// row's discovery time — compaction merges pick winners by newest
	// discovered_at, so re-stamping would invert record precedence.
	q := c.syncScope(resourceTypes.Name(), syncID, "data", "discovered_at").Order(goqu.C("external_id").Asc())
	err := c.scanRows(ctx, q, func(rows *sql.Rows) error {
		var data []byte
		var discoveredAt time.Time
		if err := rows.Scan(&data, &discoveredAt); err != nil {
			return err
		}
		m := &v2.ResourceType{}
		if err := proto.Unmarshal(data, m); err != nil {
			return err
		}
		batch = append(batch, m)
		discovered = append(discovered, discoveredAtTimestamp(discoveredAt))
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
	discovered := make([]*timestamppb.Timestamp, 0, batchSize)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := bi.AddResourcesWithDiscoveredAt(ctx, batch, discovered); err != nil {
			return err
		}
		stage.Rows += int64(len(batch))
		batch = batch[:0]
		discovered = discovered[:0]
		return nil
	}
	q := c.syncScope(resources.Name(), syncID, "data", "discovered_at").
		Order(goqu.C("resource_type_id").Asc(), goqu.C("external_id").Asc())
	err := c.scanRows(ctx, q, func(rows *sql.Rows) error {
		var data []byte
		var discoveredAt time.Time
		if err := rows.Scan(&data, &discoveredAt); err != nil {
			return err
		}
		m := &v2.Resource{}
		if err := proto.Unmarshal(data, m); err != nil {
			return err
		}
		batch = append(batch, m)
		discovered = append(discovered, discoveredAtTimestamp(discoveredAt))
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
	discovered := make([]*timestamppb.Timestamp, 0, batchSize)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := bi.AddEntitlementsWithDiscoveredAt(ctx, batch, discovered); err != nil {
			return err
		}
		stage.Rows += int64(len(batch))
		batch = batch[:0]
		discovered = discovered[:0]
		return nil
	}
	// Primary records in external_id key order.
	q := c.syncScope(entitlements.Name(), syncID, "data", "discovered_at").Order(goqu.C("external_id").Asc())
	err := c.scanRows(ctx, q, func(rows *sql.Rows) error {
		var data []byte
		var discoveredAt time.Time
		if err := rows.Scan(&data, &discoveredAt); err != nil {
			return err
		}
		m := &v2.Entitlement{}
		if err := proto.Unmarshal(data, m); err != nil {
			return err
		}
		batch = append(batch, m)
		discovered = append(discovered, discoveredAtTimestamp(discoveredAt))
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
// goroutine after the scanner has moved on, plus the row's
// discovered_at (carried onto the translated v3 record).
type rawGrantRow struct {
	data         []byte
	expansion    []byte
	discoveredAt time.Time
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

		query := "SELECT data, expansion, discovered_at FROM " + grants.Name() + " WHERE sync_id = ?" // #nosec G202 - internal table name.
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
			discovered := make([]*timestamppb.Timestamp, 0, batchSize)
			for raw := range rawCh {
				if scanCtx.Err() != nil {
					continue // drain
				}
				batch = batch[:0]
				discovered = discovered[:0]
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
					discovered = append(discovered, discoveredAtTimestamp(raw[i].discoveredAt))
				}
				if scanCtx.Err() != nil {
					continue
				}
				if err := shard.AddGrantsWithDiscoveredAt(scanCtx, batch, discovered); err != nil {
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
				var discoveredAt time.Time
				if err := rows.Scan(&data, &expansion, &discoveredAt); err != nil {
					fail(err)
					return
				}
				// Copy out of the driver's row buffer; the arena keeps
				// the batch to two allocations instead of two per row.
				off := len(arena)
				arena = append(arena, data...)
				arena = append(arena, expansion...)
				row := rawGrantRow{data: arena[off : off+len(data) : off+len(data)], discoveredAt: discoveredAt}
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

// Bounds on one session write batch: session values are connector-controlled
// blobs, so cap by bytes as well as key count to keep a single pebble batch
// from ballooning.
const (
	convertSessionBatchKeys  = 500
	convertSessionBatchBytes = 4 << 20
)

// copySessions copies the source sync's connector_sessions rows to the
// destination under destSyncID. Only called for a source that never finished:
// see the call site in ToPebble for why a finished sync's rows are left behind.
//
// Session rows are the connector's own resume scratchpad, keyed by
// (sync_id, key), and nothing clears them when a sync resumes — a resumed
// sync expects to read back what it wrote. Since ToPebble hands back an
// unfinished destination with the source's sync_token (see ToPebble), the
// session rows have to come along too: a sync_token that says "mid-sync"
// against an empty session store makes a connector resume from a state it
// never saved.
func (c *C1File) copySessions(ctx context.Context, dest *pebble.Engine, syncID, destSyncID string, stage *ConvertStageStats) error {
	start := time.Now()
	defer func() { stage.Duration = time.Since(start) }()

	q := c.db.From(sessionStore.Name()).Prepared(true).
		Select("key", "value").
		Where(goqu.C("sync_id").Eq(syncID)).
		Order(goqu.C("key").Asc())
	query, args, err := q.ToSQL()
	if err != nil {
		return err
	}
	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	batch := make(map[string][]byte, convertSessionBatchKeys)
	batchBytes := 0
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		// No WithPrefix: the scanned keys are already the stored keys, and
		// the writer would otherwise prepend the prefix a second time.
		if err := dest.SessionSetMany(ctx, batch, sessions.WithSyncID(destSyncID)); err != nil {
			return err
		}
		stage.Rows += int64(len(batch))
		batch = make(map[string][]byte, convertSessionBatchKeys)
		batchBytes = 0
		return nil
	}

	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		var key string
		var value []byte
		if err := rows.Scan(&key, &value); err != nil {
			return err
		}
		batch[key] = value
		batchBytes += len(key) + len(value)
		if len(batch) >= convertSessionBatchKeys || batchBytes >= convertSessionBatchBytes {
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
