package synccompactor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"

	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	c1zmanager "github.com/conductorone/baton-sdk/pkg/dotc1z/manager"
	"github.com/conductorone/baton-sdk/pkg/sdk"
	"github.com/conductorone/baton-sdk/pkg/sync"
	sync_compactor "github.com/conductorone/baton-sdk/pkg/synccompactor/naive"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

type Compactor struct {
	entries []*CompactableSync

	tmpDir  string
	destDir string
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

func NewCompactor(ctx context.Context, outputDir string, compactableSyncs []*CompactableSync, opts ...Option) (*Compactor, func() error, error) {
	if len(compactableSyncs) < 2 {
		return nil, nil, ErrNotEnoughFilesToCompact
	}

	c := &Compactor{entries: compactableSyncs, destDir: outputDir}
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
	if len(c.entries) < 2 {
		return nil, nil
	}

	base := c.entries[0]
	for i := 1; i < len(c.entries); i++ {
		applied := c.entries[i]

		compactable, err := c.doOneCompaction(ctx, base, applied)
		if err != nil {
			return nil, err
		}

		base = compactable
	}

	l := ctxzap.Extract(ctx)
	// Grant expansion doesn't use the connector interface at all, so giving syncer an empty connector is safe... for now.
	// If that ever changes, we should implement a file connector that is a wrapper around the reader.
	emptyConnector, err := sdk.NewEmptyConnector()
	if err != nil {
		l.Error("error creating empty connector", zap.Error(err))
		return nil, err
	}

	// Use syncer to expand grants.
	// TODO: Handle external resources.
	syncer, err := sync.NewSyncer(
		ctx,
		emptyConnector,
		sync.WithC1ZPath(base.FilePath),
		sync.WithSyncID(base.SyncID),
		sync.WithOnlyExpandGrants(),
	)
	if err != nil {
		l.Error("error creating syncer", zap.Error(err))
		return nil, err
	}

	if err := syncer.Sync(ctx); err != nil {
		l.Error("error syncing with grant expansion", zap.Error(err))
		return nil, err
	}
	if err := syncer.Close(ctx); err != nil {
		l.Error("error closing syncer", zap.Error(err))
		return nil, err
	}

	// Move last compacted file to the destination dir
	finalPath := path.Join(c.destDir, fmt.Sprintf("compacted-%s.c1z", base.SyncID))
	if err := cpFile(base.FilePath, finalPath); err != nil {
		return nil, err
	}

	if !filepath.IsAbs(finalPath) {
		abs, err := filepath.Abs(finalPath)
		if err != nil {
			return nil, err
		}
		finalPath = abs
	}
	return &CompactableSync{FilePath: finalPath, SyncID: base.SyncID}, nil
}

func cpFile(sourcePath string, destPath string) error {
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

func getLatestObjects(ctx context.Context, info *CompactableSync) (*reader_v2.SyncRun, *dotc1z.C1File, c1zmanager.Manager, func(), error) {
	baseC1Z, err := c1zmanager.New(ctx, info.FilePath)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	cleanup := func() {
		_ = baseC1Z.Close(ctx)
	}

	baseFile, err := baseC1Z.LoadC1Z(ctx)
	if err != nil {
		return nil, nil, nil, cleanup, err
	}

	cleanup = func() {
		_ = baseFile.Close()
		_ = baseC1Z.Close(ctx)
	}

	latestAppliedSync, err := baseFile.GetSync(ctx, &reader_v2.SyncsReaderServiceGetSyncRequest{
		SyncId:      info.SyncID,
		Annotations: nil,
	})
	if err != nil {
		return nil, nil, nil, cleanup, err
	}

	return latestAppliedSync.Sync, baseFile, baseC1Z, cleanup, nil
}

func (c *Compactor) doOneCompaction(ctx context.Context, base *CompactableSync, applied *CompactableSync) (*CompactableSync, error) {
	l := ctxzap.Extract(ctx)
	l.Info(
		"running compaction",
		zap.String("base_file", base.FilePath),
		zap.String("base_sync", base.SyncID),
		zap.String("applied_file", applied.FilePath),
		zap.String("applied_sync", applied.SyncID),
		zap.String("tmp_dir", c.tmpDir),
	)

	opts := []dotc1z.C1ZOption{
		dotc1z.WithPragma("journal_mode", "WAL"),
		dotc1z.WithTmpDir(c.tmpDir),
	}

	fileName := fmt.Sprintf("compacted-%s-%s.c1z", base.SyncID, applied.SyncID)
	newFile, err := dotc1z.NewC1ZFile(ctx, path.Join(c.tmpDir, fileName), opts...)
	if err != nil {
		l.Error("doOneCompaction failed: could not create c1z file", zap.Error(err))
		return nil, err
	}
	defer func() { _ = newFile.Close() }()

	newSync, err := newFile.StartNewSyncV2(ctx, string(dotc1z.SyncTypeFull), "")
	if err != nil {
		return nil, err
	}

	_, baseFile, _, cleanupBase, err := getLatestObjects(ctx, base)
	defer cleanupBase()
	if err != nil {
		return nil, err
	}

	_, appliedFile, _, cleanupApplied, err := getLatestObjects(ctx, applied)
	defer cleanupApplied()
	if err != nil {
		return nil, err
	}

	runner := sync_compactor.NewNaiveCompactor(baseFile, appliedFile, newFile)

	if err := runner.Compact(ctx); err != nil {
		l.Error("error running compaction", zap.Error(err))
		return nil, err
	}

	if err := newFile.EndSync(ctx); err != nil {
		return nil, err
	}

	outputFilepath, err := newFile.OutputFilepath()
	if err != nil {
		return nil, err
	}

	return &CompactableSync{
		FilePath: outputFilepath,
		SyncID:   newSync,
	}, nil
}
