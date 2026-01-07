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
	"github.com/conductorone/baton-sdk/pkg/sdk"
	"github.com/conductorone/baton-sdk/pkg/sync"
	"github.com/conductorone/baton-sdk/pkg/synccompactor/attached"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
)

var tracer = otel.Tracer("baton-sdk/pkg.synccompactor")

type CompactorType string

const (
	CompactorTypeAttached CompactorType = "attached"
)

type Compactor struct {
	compactorType CompactorType
	entries       []*CompactableSync
	compactedC1z  *dotc1z.C1File

	tmpDir      string
	destDir     string
	runDuration time.Duration
	syncLimit   int
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

	// If no tmpDir is provided, use the tmpDir
	if c.tmpDir == "" {
		c.tmpDir = os.TempDir()
	}
	tmpDir, err := os.MkdirTemp(c.tmpDir, "baton-sync-compactor-")
	if err != nil {
		return nil, nil, err
	}
	c.tmpDir = tmpDir

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
	defer span.End()
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
	var err error
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

	opts := []dotc1z.C1ZOption{
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
	if c.syncLimit > 0 {
		opts = append(opts, dotc1z.WithSyncLimit(c.syncLimit))
	}

	fileName := fmt.Sprintf("compacted-%s.c1z", c.entries[0].SyncID)
	destFilePath := path.Join(c.tmpDir, fileName)

	c.compactedC1z, err = dotc1z.NewC1ZFile(ctx, destFilePath, opts...)
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
			l.Error("error closing compacted c1z", zap.Error(err))
		}
	}()
	// Start new sync of type partial. If we compact syncs of other types, this sync type will be updated by attached.UpdateSync which is called by doOneCompaction().
	newSyncId, err := c.compactedC1z.StartNewSync(ctx, connectorstore.SyncTypePartial, "")
	if err != nil {
		return nil, fmt.Errorf("failed to start new sync: %w", err)
	}
	err = c.compactedC1z.EndSync(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to end sync: %w", err)
	}
	l.Debug("new empty partial sync created", zap.String("sync_id", newSyncId))

	// Base sync is c.entries[0], so compact in reverse order. That way we compact the biggest sync last.
	for i := len(c.entries) - 1; i >= 0; i-- {
		err = c.doOneCompaction(ctx, c.entries[i])
		if err != nil {
			return nil, fmt.Errorf("failed to compact sync %s: %w", c.entries[i].SyncID, err)
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

	if newSync.GetSyncType() == string(connectorstore.SyncTypePartial) {
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
	defer destination.Close()

	_, err = io.Copy(destination, source)
	if err != nil {
		return fmt.Errorf("failed to copy file: %w", err)
	}

	return nil
}

func (c *Compactor) doOneCompaction(ctx context.Context, cs *CompactableSync) error {
	ctx, span := tracer.Start(ctx, "Compactor.doOneCompaction")
	defer span.End()
	l := ctxzap.Extract(ctx)
	l.Info(
		"running compaction",
		zap.String("apply_file", cs.FilePath),
		zap.String("apply_sync", cs.SyncID),
		zap.String("tmp_dir", c.tmpDir),
	)

	applyFile, err := dotc1z.NewC1ZFile(
		ctx,
		cs.FilePath,
		dotc1z.WithTmpDir(c.tmpDir),
		dotc1z.WithDecoderOptions(dotc1z.WithDecoderConcurrency(-1)),
		dotc1z.WithReadOnly(true),
		// We're only reading, so it's safe to use these pragmas.
		dotc1z.WithPragma("synchronous", "OFF"),
		dotc1z.WithPragma("journal_mode", "OFF"),
		dotc1z.WithPragma("locking_mode", "EXCLUSIVE"),
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

	runner := attached.NewAttachedCompactor(c.compactedC1z, applyFile)
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
