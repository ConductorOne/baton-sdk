package synccompactor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"time"

	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/sdk"
	"github.com/conductorone/baton-sdk/pkg/sync"
	"github.com/conductorone/baton-sdk/pkg/synccompactor/attached"
	"github.com/conductorone/baton-sdk/pkg/tempdir"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/conductorone/baton-sdk/pkg/uotel"
)

var tracer = otel.Tracer("baton-sdk/pkg.synccompactor")

type CompactorType string

const (
	CompactorTypeAttached CompactorType = "attached"
)

type Compactor struct {
	compactorType CompactorType
	entries       []*CompactableSync
	compactedC1z  c1zstore.Store

	tmpDir             string
	destDir            string
	runDuration        time.Duration
	syncLimit          int
	c1zOptions         []dotc1z.C1ZOption
	skipGrantExpansion bool
	// engine selects the storage engine for the compacted output.
	// Empty means EngineSQLite (the default; behavior is unchanged and
	// the output is byte-identical to the pre-engine-option compactor).
	// EnginePebble produces a v3 Pebble c1z via a native record merge.
	engine c1zstore.Engine
	// pebbleMode optionally forces the Pebble merge strategy; the zero
	// value (Auto) lets the compactor choose. See WithPebbleCompactorMode.
	pebbleMode PebbleCompactorMode
	// overlaySeenKeyLimit / overlayRecordChunkSize /
	// overlayBufferFactor / overlayGateFraction optionally override
	// the overlay merge tunables; zero means the merge defaults. See
	// WithOverlaySeenKeyLimit / WithOverlayRecordChunkSize /
	// WithOverlayBufferFactor / WithOverlayGateFraction.
	overlaySeenKeyLimit    int64
	overlayRecordChunkSize int
	overlayBufferFactor    float64
	overlayGateFraction    float64
	// foldMaxWastePct optionally overrides the fold waste cutover
	// (accumulated fold dead bytes as a percent of the base's live
	// payload bytes, above which auto mode forces an overlay rebuild).
	// Zero means the default; see WithFoldMaxWastePercent.
	foldMaxWastePct int64
	// decoderPool scopes v3 payload-decoder reuse to one Compact run:
	// every source envelope open draws from it instead of constructing
	// a fresh zstd decoder, and Compact closes it on the way out so no
	// decoder buffers outlive the compaction (deliberately NOT a
	// process-global pool — see dotc1z.WithDecoderPool).
	decoderPool *dotc1z.EnvelopeDecoderPool
}

// resolvedEngine returns the configured engine, treating the zero value
// as EngineSQLite. Compact calls inferEngineFromInputs first so the zero
// value can follow existing c1z inputs instead of always producing SQLite.
func (c *Compactor) resolvedEngine() c1zstore.Engine {
	if c.engine == "" {
		return c1zstore.EngineSQLite
	}
	return c.engine
}

// ErrEnginePolicyConflict is returned when the caller explicitly requests
// SQLite output but one or more inputs are Pebble/v3 format. Pebble inputs
// cannot be re-encoded as SQLite without a full data-layer conversion, so
// the combination is rejected rather than silently downgrading.
var ErrEnginePolicyConflict = errors.New("compactor: engine policy conflict: cannot compact Pebble input to SQLite output")

// inferEngineFromInputs determines the output engine for a compaction run
// when no engine was explicitly set by the caller (WithEngine).
//
// Policy (evaluated in order):
//  1. Explicit engine set via WithEngine: honored, subject to the Pebble-input
//     constraint below.
//  2. Any input is Pebble/v3: output is Pebble, regardless of whether other
//     inputs are SQLite/v1 (mixed inputs produce Pebble). SQLite inputs in a
//     Pebble run are converted to Pebble automatically; callers are not
//     required to pre-convert.
//  3. All inputs are SQLite/v1: output is SQLite.
//
// Constraint: an explicit SQLite request (WithEngine(EngineSQLite)) when any
// input is Pebble/v3 returns ErrEnginePolicyConflict.
func (c *Compactor) inferEngineFromInputs() (c1zstore.Engine, error) {
	hasPebble := false
	hasSQLite := false
	for _, entry := range c.entries {
		if entry == nil || entry.FilePath == "" {
			continue
		}
		f, err := os.Open(entry.FilePath) // #nosec G304 - compaction inputs are caller-provided c1z paths.
		if err != nil {
			return "", fmt.Errorf("infer compactor engine from %s: %w", entry.FilePath, err)
		}
		format, readErr := dotc1z.ReadHeaderFormat(f)
		closeErr := f.Close()
		if readErr != nil {
			return "", fmt.Errorf("infer compactor engine from %s: %w", entry.FilePath, readErr)
		}
		if closeErr != nil {
			return "", fmt.Errorf("infer compactor engine from %s: %w", entry.FilePath, closeErr)
		}
		switch format {
		case dotc1z.C1ZFormatV1:
			hasSQLite = true
		case dotc1z.C1ZFormatV3:
			hasPebble = true
		default:
			return "", fmt.Errorf("infer compactor engine from %s: unsupported c1z format %s", entry.FilePath, format)
		}
	}

	// Explicit engine: validate and return.
	if c.engine != "" {
		if c.engine == c1zstore.EngineSQLite && hasPebble {
			return "", fmt.Errorf("%w: caller requested SQLite but at least one input is Pebble/v3", ErrEnginePolicyConflict)
		}
		return c.engine, nil
	}

	// Auto-select: any Pebble input → Pebble output.
	if hasPebble {
		return c1zstore.EnginePebble, nil
	}
	if hasSQLite {
		return c1zstore.EngineSQLite, nil
	}
	// No readable inputs: default to SQLite to preserve historical behavior.
	return c1zstore.EngineSQLite, nil
}

type CompactableSync struct {
	FilePath string
	SyncID   string
}

var ErrNotEnoughFilesToCompact = errors.New("must provide two or more files to compact")

type Option func(*Compactor)

// WithTmpDir sets the working directory where files will be created and edited during compaction.
// If not provided, the temporary directory will be used.
func WithTmpDir(tempDir string) Option {
	return func(c *Compactor) {
		c.tmpDir = tempDir
	}
}

// Deprecated: There is now only one compactor type, so this option is no longer needed.
func WithCompactorType(compactorType CompactorType) Option {
	return func(c *Compactor) {
		c.compactorType = compactorType
	}
}

func WithRunDuration(runDuration time.Duration) Option {
	return func(c *Compactor) {
		c.runDuration = runDuration
	}
}

// WithSyncLimit sets the number of syncs to keep after compaction cleanup.
func WithSyncLimit(limit int) Option {
	return func(c *Compactor) {
		c.syncLimit = limit
	}
}

// WithC1ZOptions sets the C1Z options to use for the compactor.
// This allows tweaking C1Z opts such as encoder/decoder parallelism.
func WithC1ZOptions(opts ...dotc1z.C1ZOption) Option {
	return func(c *Compactor) {
		c.c1zOptions = opts
	}
}

// WithSkipGrantExpansion skips grant expansion after compaction.
// This is useful when expansion will be handled separately (e.g. by incremental expansion).
func WithSkipGrantExpansion() Option {
	return func(c *Compactor) {
		c.skipGrantExpansion = true
	}
}

func NewCompactor(ctx context.Context, outputDir string, compactableSyncs []*CompactableSync, opts ...Option) (*Compactor, func() error, error) {
	if len(compactableSyncs) < 2 {
		return nil, nil, ErrNotEnoughFilesToCompact
	}

	c := &Compactor{
		entries:       compactableSyncs,
		destDir:       outputDir,
		compactorType: CompactorTypeAttached,
	}
	for _, opt := range opts {
		opt(c)
	}

	c.tmpDir = tempdir.Resolve(c.tmpDir)
	tmpDir, err := os.MkdirTemp(c.tmpDir, "baton-sync-compactor-")
	if err != nil {
		return nil, nil, err
	}
	c.tmpDir = tmpDir

	defaultC1ZOptions := []dotc1z.C1ZOption{
		dotc1z.WithTmpDir(c.tmpDir),
		// Performance improvements:
		// NOTE: We do not close this c1z after compaction, so syncer will have these pragmas when expanding grants.
		// We should re-evaluate these pragmas when partial syncs sync grants.
		// Disable journaling.
		dotc1z.WithPragma("journal_mode", "OFF"),
		// Disable synchronous writes
		dotc1z.WithPragma("synchronous", "OFF"),
		// Use exclusive locking.
		dotc1z.WithPragma("main.locking_mode", "EXCLUSIVE"),
		// Use parallel decoding.
		dotc1z.WithDecoderOptions(dotc1z.WithDecoderConcurrency(-1)),
		// Use parallel encoding.
		dotc1z.WithEncoderConcurrency(0),
	}
	// We would set the default options sooner, but we need to know the tmpDir first.
	c.c1zOptions = append(defaultC1ZOptions, c.c1zOptions...)

	cleanup := func() error {
		if err := os.RemoveAll(c.tmpDir); err != nil {
			return err
		}
		return nil
	}

	return c, cleanup, nil
}

func (c *Compactor) Compact(ctx context.Context) (*CompactableSync, error) {
	ctx, span := tracer.Start(ctx, "Compactor.Compact")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()
	if len(c.entries) < 2 {
		return nil, nil
	}

	compactionStart := time.Now()
	runCtx := ctx
	var runCanc context.CancelFunc
	if c.runDuration > 0 {
		runCtx, runCanc = context.WithTimeout(ctx, c.runDuration)
	}
	if runCanc != nil {
		defer runCanc()
	}

	l := ctxzap.Extract(ctx)
	select {
	case <-runCtx.Done():
		err = context.Cause(runCtx)
		switch {
		case errors.Is(err, context.DeadlineExceeded):
			l.Info("compaction run duration has expired, exiting compaction early")
			return nil, fmt.Errorf("compaction run duration has expired: %w", err)
		default:
			l.Error("compaction context cancelled", zap.Error(err))
			return nil, err
		}
	default:
	}

	opts := make([]dotc1z.C1ZOption, len(c.c1zOptions))
	copy(opts, c.c1zOptions)
	if c.syncLimit > 0 {
		opts = append(opts, dotc1z.WithSyncLimit(c.syncLimit))
	}
	engine, err := c.inferEngineFromInputs()
	if err != nil {
		return nil, err
	}
	c.engine = engine

	fileName := fmt.Sprintf("compacted-%s.c1z", c.entries[0].SyncID)
	destFilePath := path.Join(c.tmpDir, fileName)

	// Resolve the Pebble strategy up front: an explicit
	// BATON_EXPERIMENTAL_PEBBLE_COMPACTOR value forces a mode;
	// otherwise the size gate picks fold (large base, small partials)
	// or overlay (everything else); see resolvePebbleMode.
	//
	// In-place fold: the dest store starts as a copy of the base input
	// and partials are merged into the base keyspace via keep-newer
	// writes; the folded output is then re-keyed to a fresh sync id.
	// The original base file is never mutated. See compactPebbleFold.
	if c.resolvedEngine() == c1zstore.EnginePebble {
		c.pebbleMode = c.resolvePebbleMode(ctx)
	}
	foldMode := c.pebbleMode == PebbleCompactorModeFold
	if foldMode {
		// Fold merges partials in place into a private copy of the base, so
		// only the BASE must already be Pebble: converting the base would
		// mean rewriting the whole base, which defeats fold's point. SQLite
		// partials are fine — they are converted to Pebble inside the fold
		// loop. When the base itself is SQLite/v1, fall back to the overlay
		// path, which converts every input before merging.
		baseFormat, ferr := readCompactionInputFormat(c.entries[0].FilePath)
		if ferr != nil {
			return nil, ferr
		}
		if baseFormat != dotc1z.C1ZFormatV3 {
			foldMode = false
			c.pebbleMode = PebbleCompactorModeOverlay
		}
	}
	if foldMode {
		if err = copyFileForFold(c.entries[0].FilePath, destFilePath); err != nil {
			return nil, fmt.Errorf("fold: copy base input: %w", err)
		}
	}

	if c.resolvedEngine() == c1zstore.EnginePebble {
		// One payload-decoder pool for the whole compaction: the merge
		// opens every source's envelope (selection + per-chunk unpack),
		// and reusing one decoder across those opens avoids a fresh
		// window allocation + worker spin-up per source. Closed when
		// Compact returns so nothing is retained by the process after.
		c.decoderPool = dotc1z.NewEnvelopeDecoderPool()
		defer c.decoderPool.Close()
		opts = append(opts, dotc1z.WithDecoderPool(c.decoderPool))
	}

	if c.resolvedEngine() == c1zstore.EnginePebble {
		// Force the resolved engine last so a stray engine passed via
		// WithC1ZOptions cannot mislabel the artifact.
		c.compactedC1z, err = dotc1z.NewStore(ctx, destFilePath, append(opts, dotc1z.WithEngine(c1zstore.EnginePebble))...)
	} else {
		c.compactedC1z, err = dotc1z.NewStore(ctx, destFilePath, opts...)
	}
	if err != nil {
		l.Error("doOneCompaction failed: could not create c1z file", zap.Error(err))
		return nil, err
	}
	defer func() {
		if c.compactedC1z == nil {
			return
		}
		err := c.compactedC1z.Close(ctx)
		if err != nil {
			l.Error("compactor: error closing compacted c1z", zap.Error(err), zap.String("compacted_c1z_file", destFilePath))
		}
	}()
	var newSyncId string
	switch {
	case foldMode:
		// In-place fold: no fresh sync — the output adopts the base
		// sync's id and partials merge into its keyspace.
		newSyncId, err = c.compactPebbleFold(runCtx)
		if err != nil {
			if cause := context.Cause(runCtx); errors.Is(cause, context.DeadlineExceeded) && c.runDuration > 0 && ctx.Err() == nil {
				l.Info("compaction run duration has expired, exiting compaction early")
				return nil, fmt.Errorf("compaction run duration has expired: %w", cause)
			}
			return nil, fmt.Errorf("failed to compact (pebble fold): %w", err)
		}
	case c.resolvedEngine() == c1zstore.EnginePebble:
		newSyncId, err = c.runPebbleRebuild(ctx, runCtx)
		if err != nil {
			if cause := context.Cause(runCtx); errors.Is(cause, context.DeadlineExceeded) && c.runDuration > 0 && ctx.Err() == nil {
				l.Info("compaction run duration has expired, exiting compaction early")
				return nil, fmt.Errorf("compaction run duration has expired: %w", cause)
			}
			return nil, fmt.Errorf("failed to compact (pebble): %w", err)
		}
	default:
		// Start new sync of type partial. If we compact syncs of other types, this sync type will be updated by attached.UpdateSync which is called by doOneCompaction().
		newSyncId, err = c.compactedC1z.StartNewSync(ctx, connectorstore.SyncTypePartial, "")
		if err != nil {
			return nil, fmt.Errorf("failed to start new sync: %w", err)
		}
		if err = c.compactedC1z.EndSync(ctx); err != nil {
			return nil, fmt.Errorf("failed to end sync: %w", err)
		}
		l.Debug("new empty partial sync created", zap.String("sync_id", newSyncId))
		// Base sync is c.entries[0], so compact in reverse order. That way we compact the biggest sync last.
		// Pass runCtx (not the outer ctx) so c.runDuration actually bounds the loop. Without this, the
		// deadline set on runCtx only gates the pre-flight check above and individual doOneCompaction
		// calls run under the unbounded parent ctx.
		for i := len(c.entries) - 1; i >= 0; i-- {
			err = c.doOneCompaction(runCtx, c.entries[i])
			if err != nil {
				// When runCtx fires due to c.runDuration, surface the same clean message the pre-flight
				// check uses instead of bubbling a bare context.DeadlineExceeded out of the inner sqlite
				// operations. The error is still returned so callers can decide whether to retry.
				if cause := context.Cause(runCtx); errors.Is(cause, context.DeadlineExceeded) && c.runDuration > 0 && ctx.Err() == nil {
					l.Info("compaction run duration has expired, exiting compaction early",
						zap.String("sync_id", c.entries[i].SyncID),
						zap.Int("syncs_remaining", i),
					)
					return nil, fmt.Errorf("compaction run duration has expired: %w", cause)
				}
				return nil, fmt.Errorf("failed to compact sync %s: %w", c.entries[i].SyncID, err)
			}
		}
	}

	resp, err := c.compactedC1z.GetSync(ctx, reader_v2.SyncsReaderServiceGetSyncRequest_builder{
		SyncId: newSyncId,
	}.Build())
	if err != nil {
		return nil, fmt.Errorf("failed to get sync: %w", err)
	}
	newSync := resp.GetSync()
	if newSync == nil {
		return nil, fmt.Errorf("no sync found")
	}

	if newSync.GetId() != newSyncId {
		return nil, fmt.Errorf("new sync id does not match expected id: %s != %s", newSync.GetId(), newSyncId)
	}

	skipExpansion := c.skipGrantExpansion || newSync.GetSyncType() == string(connectorstore.SyncTypePartial)
	if skipExpansion {
		err = c.compactedC1z.Cleanup(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to cleanup compacted c1z: %w", err)
		}
		// Close compactedC1z so that the c1z file is written to disk before cpFile() is called.
		err = c.compactedC1z.Close(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to close compacted c1z: %w", err)
		}
		c.compactedC1z = nil
	} else {
		err = c.expandGrants(ctx, newSyncId, compactionStart)
		if err != nil {
			return nil, fmt.Errorf("failed to expand grants: %w", err)
		}
		// expandGrants internally wraps the compactedC1z in a syncer whose
		// Close() closes the store. Clear our pointer so the deferred Close
		// at the top of Compact doesn't call Close a second time. Close is
		// idempotent today, but this keeps the ownership handoff explicit.
		c.compactedC1z = nil
	}

	// Move last compacted file to the destination dir
	finalPath := path.Join(c.destDir, fmt.Sprintf("compacted-%s.c1z", newSyncId))
	if err := cpFile(ctx, destFilePath, finalPath); err != nil {
		return nil, err
	}

	if !filepath.IsAbs(finalPath) {
		abs, err := filepath.Abs(finalPath)
		if err != nil {
			return nil, err
		}
		finalPath = abs
	}
	return &CompactableSync{FilePath: finalPath, SyncID: newSyncId}, nil
}

func cpFile(ctx context.Context, sourcePath string, destPath string) error {
	err := os.Rename(sourcePath, destPath)
	if err == nil {
		return nil
	}

	l := ctxzap.Extract(ctx)
	l.Warn("compactor: failed to rename final compacted file, falling back to copy", zap.Error(err), zap.String("source_path", sourcePath), zap.String("dest_path", destPath))

	source, err := os.Open(sourcePath)
	if err != nil {
		return fmt.Errorf("failed to open source file: %w", err)
	}
	defer source.Close()

	destination, err := os.Create(destPath)
	if err != nil {
		return fmt.Errorf("failed to create destination file: %w", err)
	}
	destinationClosed := false
	defer func() {
		if !destinationClosed {
			_ = destination.Close()
		}
	}()

	_, err = io.Copy(destination, source)
	if err != nil {
		return fmt.Errorf("failed to copy file: %w", err)
	}

	// Sync + Close + err-check so write-back failures (out-of-disk,
	// IO error, quota exhaustion) surface here rather than being
	// silently discarded by the deferred Close after the function
	// has reported success. Required because the compacted file is
	// the canonical artifact downstream consumers read.
	if err := destination.Sync(); err != nil {
		return fmt.Errorf("failed to sync destination file: %w", err)
	}
	if err := destination.Close(); err != nil {
		return fmt.Errorf("failed to close destination file: %w", err)
	}
	destinationClosed = true

	return nil
}

func (c *Compactor) doOneCompaction(ctx context.Context, cs *CompactableSync) error {
	ctx, span := tracer.Start(ctx, "Compactor.doOneCompaction")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()
	l := ctxzap.Extract(ctx)
	l.Info(
		"running compaction",
		zap.String("apply_file", cs.FilePath),
		zap.String("apply_sync", cs.SyncID),
		zap.String("tmp_dir", c.tmpDir),
	)

	applyFile, err := dotc1z.NewStore(
		ctx,
		cs.FilePath,
		dotc1z.WithTmpDir(c.tmpDir),
		dotc1z.WithDecoderOptions(dotc1z.WithDecoderConcurrency(-1)),
		dotc1z.WithReadOnly(true),
	)
	if err != nil {
		return err
	}
	defer func() {
		err := applyFile.Close(ctx)
		if err != nil {
			l.Error("error closing apply file", zap.Error(err), zap.String("apply_file", cs.FilePath))
		}
	}()

	runner, err := attached.NewAttachedCompactor(c.compactedC1z, applyFile)
	if err != nil {
		return fmt.Errorf("failed to create attached compactor: %w", err)
	}
	if err := runner.Compact(ctx); err != nil {
		l.Error("error running compaction", zap.Error(err), zap.String("apply_file", cs.FilePath))
		return err
	}

	return nil
}

func (c *Compactor) expandGrants(ctx context.Context, newSyncId string, compactionStart time.Time) error {
	l := ctxzap.Extract(ctx)
	// Grant expansion doesn't use the connector interface at all, so giving syncer an empty connector is safe... for now.
	// If that ever changes, we should implement a file connector that is a wrapper around the reader.
	emptyConnector, err := sdk.NewEmptyConnector()
	if err != nil {
		l.Error("error creating empty connector", zap.Error(err))
		return err
	}

	// Use syncer to expand grants.
	// TODO: Handle external resources.
	syncOpts := []sync.SyncOpt{
		sync.WithConnectorStore(c.compactedC1z), // Use the existing C1File so we're not wasting time compressing & decompressing it.
		sync.WithTmpDir(c.tmpDir),
		sync.WithSyncID(newSyncId),
		sync.WithOnlyExpandGrants(),
		// The store is a keep-newer merge: invariant verdicts must
		// attribute merge-manufactured shapes to the merge, not the
		// connector, and must not fail the seal over them. Wiring
		// pinned by TestCompactionExpandToleratesMergeManufacturedExclusionConflicts.
		sync.WithCompactionMergedStore(),
	}

	compactionDuration := time.Since(compactionStart)
	runDuration := c.runDuration - compactionDuration
	l.Debug("finished compaction", zap.Duration("compaction_duration", compactionDuration))

	switch {
	case c.runDuration > 0 && runDuration <= 0:
		return fmt.Errorf("unable to finish compaction sync in run duration (%s). compactions took %s", c.runDuration, compactionDuration)
	case runDuration > 0:
		syncOpts = append(syncOpts, sync.WithRunDuration(runDuration))
	}

	syncer, err := sync.NewSyncer(
		ctx,
		emptyConnector,
		syncOpts...,
	)
	if err != nil {
		l.Error("error creating syncer", zap.Error(err))
		return err
	}

	if err := syncer.Sync(ctx); err != nil {
		l.Error("error syncing with grant expansion", zap.Error(err))
		return err
	}
	if err := syncer.Close(ctx); err != nil {
		l.Error("error closing syncer", zap.Error(err))
		return err
	}
	return nil
}
