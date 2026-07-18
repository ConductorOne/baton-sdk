package synccompactor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/sdk"
	"github.com/conductorone/baton-sdk/pkg/sync"
	"github.com/conductorone/baton-sdk/pkg/sync/expand"
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
	// incrementalBaseGraph, when set, enables diff-aware expansion: only changes
	// relative to this base-sync graph are expanded. nil (default) = full expansion.
	// The set of changed entitlements is derived from the applied increments
	// during expansion, not supplied by the caller.
	incrementalBaseGraph *expand.EntitlementGraph
	// incrementalExpansionRan records whether the diff-aware path actually
	// handled expansion (vs falling back to full). Read by tests to prove the
	// fast path ran rather than silently falling back.
	incrementalExpansionRan bool
	// foldChangedEntitlementIDs: changed-entitlement set collected by the
	// Pebble fold; nil when no fold ran (derive fallback).
	foldChangedEntitlementIDs map[string]struct{}
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

// WithIncrementalExpansion enables diff-aware grant expansion during compaction.
// baseGraph is the base sync's graph (via sync.GraphFromToken). The set of
// entitlements whose membership changed is derived from the applied increments
// during expansion (not supplied by the caller), so new members propagate. A
// new edge that closes a cycle falls back to full expansion; nil baseGraph
// (default) = full.
//
// Additions-only: a revocation-shaped change (a narrowed edge spec) auto-declines
// to full expansion; removals are not propagated incrementally.
func WithIncrementalExpansion(baseGraph *expand.EntitlementGraph) Option {
	return func(c *Compactor) {
		c.incrementalBaseGraph = baseGraph
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

// expandGrantsIncremental runs a diff-aware expansion over the compacted c1z.
// errIncrementalFatal marks incremental-expansion errors that must FAIL the
// compaction rather than fall back to full expansion: the store is mid-teardown
// (or could not be restored to its ended state), so running the full path
// against it is unsafe. Every other error is safe to fall back on — the store
// was untouched or restored, and expanded-grant writes are idempotent.
var errIncrementalFatal = errors.New("incremental expansion: fatal")

// Returns (true, nil) when it handled expansion. Errors come in three shapes:
// decline sentinels (ErrIncrementalFallback for a cycle,
// ErrIncrementalRevocationDecline for a narrowed edge) and plain errors both
// mean "fall back to full expansion" — the store is in the ended state the
// full path expects; errors wrapped in errIncrementalFatal mean the store's
// finalization failed and the compaction must fail. Finalization always runs
// on a detached context so a run-duration timeout can't abort it.
func (c *Compactor) expandGrantsIncremental(ctx context.Context, newSyncId string, compactionStart time.Time) (bool, error) {
	// Clone so a failed/declined run never mutates the caller-held base graph
	// (a retry with the original must not see never-expanded edges as present).
	base, err := c.incrementalBaseGraph.Clone()
	if err != nil {
		return false, fmt.Errorf("incremental expansion: clone base graph: %w", err)
	}

	// Bound the walk by the remaining run duration; the walk polls ctx.Err().
	// Finalization uses detached contexts, so an expired walk deadline never
	// aborts the end/cleanup/close.
	walkCtx := ctx
	if c.runDuration > 0 {
		remaining := c.runDuration - time.Since(compactionStart)
		if remaining <= 0 {
			// Let the full path surface its canonical run-duration error.
			return false, fmt.Errorf("incremental expansion: run duration expired before expansion")
		}
		var cancel context.CancelFunc
		walkCtx, cancel = context.WithTimeout(ctx, remaining)
		defer cancel()
	}

	// The merge left the sync ended; resume it so grants can be written (end +
	// close on the way out). Fetch the type first so resume finds the existing sync.
	syncResp, err := c.compactedC1z.GetSync(walkCtx, reader_v2.SyncsReaderServiceGetSyncRequest_builder{SyncId: newSyncId}.Build())
	if err != nil {
		return false, fmt.Errorf("incremental expansion: get sync: %w", err)
	}
	syncType := connectorstore.SyncType(syncResp.GetSync().GetSyncType())
	if _, _, err := c.compactedC1z.StartOrResumeSync(walkCtx, syncType, newSyncId); err != nil {
		return false, fmt.Errorf("incremental expansion: resume sync: %w", err)
	}

	// Every rule grant currently in the compacted c1z (base + merged
	// increments) yields one or more edges. New edges are those the base graph
	// didn't already have expanded.
	var newEdges []expand.NewEdge
	for pe, err := range c.compactedC1z.Grants().PendingExpansion(walkCtx) {
		if err != nil {
			if endErr := c.restoreEndedSync(ctx); endErr != nil {
				return false, endErr
			}
			return false, fmt.Errorf("incremental expansion: enumerate pending: %w", err)
		}
		anno := pe.Annotation
		if anno == nil {
			continue
		}
		for _, src := range anno.GetEntitlementIds() {
			baseEdge, inBase := baseGraphEdge(base, src, pe.TargetEntitlementID)
			curEdge := expand.NewEdge{
				SourceEntitlementID: src,
				DestEntitlementID:   pe.TargetEntitlementID,
				Shallow:             anno.GetShallow(),
				ResourceTypeIDs:     anno.GetResourceTypeIds(),
			}
			if !inBase {
				newEdges = append(newEdges, curEdge) // brand-new edge
				continue
			}
			// Existing edge: compare specs, not just endpoints (C3).
			switch classifyEdgeSpecChange(baseEdge, curEdge) {
			case edgeSpecNarrowed:
				// Revocation-shaped (shallow-ified / filter tightened): can't
				// remove grants incrementally — decline via the named hook (#6).
				if endErr := c.restoreEndedSync(ctx); endErr != nil {
					return false, endErr
				}
				return false, expand.ErrIncrementalRevocationDecline
			case edgeSpecWidened:
				// More members now qualify: re-expand (AddEdge folds the wider
				// spec into the graph, deep-wins/unfiltered-wins).
				newEdges = append(newEdges, curEdge)
			case edgeSpecUnchanged:
				// nothing to do
			}
		}
	}

	// Changed entitlements are derived from the applied increments (their
	// grants' entitlement ids), not supplied by the caller — trust the data.
	changedEntitlementIDs, err := c.changedEntitlementIDs(walkCtx)
	if err != nil {
		if endErr := c.restoreEndedSync(ctx); endErr != nil {
			return false, endErr
		}
		return false, err
	}

	if len(newEdges) == 0 && len(changedEntitlementIDs) == 0 {
		// Nothing changed relative to the base — its grants were already merged in.
		return c.finishIncrementalExpansion(ctx)
	}

	ie := expand.NewIncrementalExpander(sync.NewExpanderStore(c.compactedC1z), base)
	res, err := ie.ExpandChanges(walkCtx, newEdges, changedEntitlementIDs)
	if err != nil {
		// Restore the ended state so the full path re-runs against a consistent
		// store — for the cycle decline and any real error alike. Writes are
		// idempotent by grant identity, so partial progress is safe to re-cover.
		if endErr := c.restoreEndedSync(ctx); endErr != nil {
			return false, endErr
		}
		return false, err // sentinel or plain → caller falls back to full
	}

	ctxzap.Extract(ctx).Info("incremental grant expansion complete",
		zap.Int("entitlements_walked", len(res.EntitlementsWalked)),
		zap.Int("grants_written", res.GrantsWritten))
	return c.finishIncrementalExpansion(ctx)
}

// restoreEndedSync returns the compacted sync to the ended state the full path
// expects, after an incremental attempt that resumed it. Runs on a detached,
// timeout-bounded context so a cancelled parent can't strand the store
// mid-resume. Its failure is FATAL (errIncrementalFatal): the store is in an
// unknown state and the full path must not run against it.
func (c *Compactor) restoreEndedSync(ctx context.Context) error {
	endCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), dotc1z.FinalizeTimeout())
	defer cancel()
	if err := c.compactedC1z.EndSync(endCtx); err != nil {
		return fmt.Errorf("%w: restore ended sync: %w", errIncrementalFatal, err)
	}
	return nil
}

// finishIncrementalExpansion ends, cleans up, and closes the store so the file
// is flushed before cpFile copies it — converging with the other compaction
// paths (Cleanup is a Pebble no-op today, kept for parity). Runs on a detached,
// timeout-bounded context so a cancelled or run-duration-expired parent can't
// abort finalization. All errors here are FATAL (errIncrementalFatal): the
// store is being torn down, so falling back to full expansion against it is
// not safe.
func (c *Compactor) finishIncrementalExpansion(ctx context.Context) (bool, error) {
	finalizeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), dotc1z.FinalizeTimeout())
	defer cancel()
	if err := c.compactedC1z.EndSync(finalizeCtx); err != nil {
		return false, fmt.Errorf("%w: end sync: %w", errIncrementalFatal, err)
	}
	if err := c.compactedC1z.Cleanup(finalizeCtx); err != nil {
		return false, fmt.Errorf("%w: cleanup: %w", errIncrementalFatal, err)
	}
	if err := c.compactedC1z.Close(finalizeCtx); err != nil {
		return false, fmt.Errorf("%w: close: %w", errIncrementalFatal, err)
	}
	return true, nil
}

// changedEntitlementIDs returns the entitlement ids whose grants changed in
// the applied increments, seeding the incremental walk. The fold collects
// them during its merge (no re-read, no-ops excluded); rebuild-mode
// compactions fall back to deriveChangedEntitlementIDs.
func (c *Compactor) changedEntitlementIDs(ctx context.Context) ([]string, error) {
	if c.foldChangedEntitlementIDs != nil {
		out := make([]string, 0, len(c.foldChangedEntitlementIDs))
		for id := range c.foldChangedEntitlementIDs {
			out = append(out, id)
		}
		sort.Strings(out)
		return out, nil
	}
	return c.deriveChangedEntitlementIDs(ctx)
}

// deriveChangedEntitlementIDs is the no-fold fallback: re-open each increment
// (entries[1:]) and collect its grants' entitlement ids.
func (c *Compactor) deriveChangedEntitlementIDs(ctx context.Context) ([]string, error) {
	if len(c.entries) < 2 {
		return nil, nil
	}
	seen := make(map[string]struct{})
	for _, e := range c.entries[1:] {
		// Same open options as doOneCompaction: honor the caller's tmp dir
		// (extraction must not silently land in os.TempDir()) and parallel decode.
		store, err := dotc1z.NewStore(ctx, e.FilePath,
			dotc1z.WithTmpDir(c.tmpDir),
			dotc1z.WithDecoderOptions(dotc1z.WithDecoderConcurrency(-1)),
			dotc1z.WithReadOnly(true),
		)
		if err != nil {
			return nil, fmt.Errorf("incremental expansion: open increment %s: %w", e.SyncID, err)
		}
		err = collectGrantEntitlementIDs(ctx, store, e.SyncID, seen)
		if closeErr := store.Close(ctx); closeErr != nil && err == nil {
			err = fmt.Errorf("incremental expansion: close increment %s: %w", e.SyncID, closeErr)
		}
		if err != nil {
			return nil, err
		}
	}
	out := make([]string, 0, len(seen))
	for id := range seen {
		out = append(out, id)
	}
	sort.Strings(out)
	return out, nil
}

// collectGrantEntitlementIDs adds every entitlement id that has a grant in the
// given sync to seen.
func collectGrantEntitlementIDs(ctx context.Context, store c1zstore.Store, syncID string, seen map[string]struct{}) error {
	if err := store.SetCurrentSync(ctx, syncID); err != nil {
		return fmt.Errorf("incremental expansion: set increment sync %s: %w", syncID, err)
	}
	pageToken := ""
	for {
		resp, err := store.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{
			PageSize:  1000,
			PageToken: pageToken,
		}.Build())
		if err != nil {
			return fmt.Errorf("incremental expansion: list increment grants for %s: %w", syncID, err)
		}
		for _, g := range resp.GetList() {
			if id := g.GetEntitlement().GetId(); id != "" {
				seen[id] = struct{}{}
			}
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			return nil
		}
	}
}

// baseGraphEdge returns the base graph's edge src->dst and whether one exists.
// Endpoints collapsed into one node (a fixed cycle) count as present with no
// distinct edge (nil), which classifyEdgeSpecChange treats as unchanged.
func baseGraphEdge(g *expand.EntitlementGraph, src, dst string) (*expand.Edge, bool) {
	sn := g.GetNode(src)
	dn := g.GetNode(dst)
	if sn == nil || dn == nil {
		return nil, false
	}
	if sn.Id == dn.Id {
		return nil, true
	}
	dests, ok := g.SourcesToDestinations[sn.Id]
	if !ok {
		return nil, false
	}
	edgeID, ok := dests[dn.Id]
	if !ok {
		return nil, false
	}
	e, ok := g.Edges[edgeID]
	if !ok {
		return nil, false
	}
	return &e, true
}

type edgeSpecChange int

const (
	edgeSpecUnchanged edgeSpecChange = iota
	edgeSpecWidened
	edgeSpecNarrowed
)

// classifyEdgeSpecChange compares an existing base edge's spec to the current
// (increment) spec. Narrowing (deep->shallow, filter tightened) is
// revocation-shaped; widening (shallow->deep, filter broadened) needs
// re-expansion. Any narrowing wins (safest: decline to full).
func classifyEdgeSpecChange(base *expand.Edge, cur expand.NewEdge) edgeSpecChange {
	if base == nil {
		return edgeSpecUnchanged // collapsed cycle: no distinct edge
	}
	widened, narrowed := false, false
	if base.IsShallow && !cur.Shallow {
		widened = true // shallow -> deep
	}
	if !base.IsShallow && cur.Shallow {
		narrowed = true // deep -> shallow
	}
	rw, rn := compareResourceTypeFilter(base.ResourceTypeIDs, cur.ResourceTypeIDs)
	widened = widened || rw
	narrowed = narrowed || rn
	switch {
	case narrowed:
		return edgeSpecNarrowed
	case widened:
		return edgeSpecWidened
	default:
		return edgeSpecUnchanged
	}
}

// compareResourceTypeFilter compares two principal-type filters where an empty
// filter means "all types" (the widest). Returns whether the current filter is
// wider and/or narrower than the base.
func compareResourceTypeFilter(base, cur []string) (bool, bool) {
	var widened, narrowed bool
	baseAll := len(base) == 0
	curAll := len(cur) == 0
	switch {
	case baseAll && curAll:
		return false, false
	case baseAll && !curAll:
		return false, true // all -> some
	case !baseAll && curAll:
		return true, false // some -> all
	}
	baseSet := make(map[string]struct{}, len(base))
	for _, t := range base {
		baseSet[t] = struct{}{}
	}
	curSet := make(map[string]struct{}, len(cur))
	for _, t := range cur {
		curSet[t] = struct{}{}
	}
	for t := range curSet {
		if _, ok := baseSet[t]; !ok {
			widened = true
		}
	}
	for t := range baseSet {
		if _, ok := curSet[t]; !ok {
			narrowed = true
		}
	}
	return widened, narrowed
}

func (c *Compactor) expandGrants(ctx context.Context, newSyncId string, compactionStart time.Time) error {
	l := ctxzap.Extract(ctx)

	// Diff-aware fast path: with a base graph, expand only what changed relative
	// to it. Any doubt (cycle, error) falls through to full expansion below.
	// Pebble-only: it reopens the ended compacted sync to write grants, which
	// only Pebble supports; on other engines we degrade gracefully to full.
	switch {
	case c.incrementalBaseGraph == nil:
		// not requested
	case c.resolvedEngine() != c1zstore.EnginePebble:
		l.Info("incremental expansion is Pebble-only; using full expansion",
			zap.String("engine", string(c.resolvedEngine())))
	default:
		done, err := c.expandGrantsIncremental(ctx, newSyncId, compactionStart)
		switch {
		case errors.Is(err, errIncrementalFatal):
			// The store's finalization (or restore-to-ended) failed: it is in an
			// unknown/torn-down state, so running full expansion against it is
			// unsafe. Fail the compaction.
			return fmt.Errorf("incremental grant expansion: %w", err)
		case errors.Is(err, expand.ErrIncrementalRevocationDecline):
			// Named revocation hook (#6): today declines to full; a future
			// tombstone stage flips this one site to apply deletions.
			l.Info("incremental expansion declined (revocation-shaped change); falling back to full expansion")
		case errors.Is(err, expand.ErrIncrementalFallback):
			// New edge closed a cycle: full expansion handles cycles correctly.
			l.Info("incremental expansion declined (cycle); falling back to full expansion")
		case err != nil:
			// Pre-write or restored-state failure: the store is back in the
			// ended state the full path expects, so falling back is safe.
			l.Warn("incremental expansion failed; falling back to full expansion", zap.Error(err))
		case done:
			// Incremental path already ended + closed the store; caller clears
			// c.compactedC1z after return, same as the full path.
			c.incrementalExpansionRan = true
			return nil
		}
	}

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
