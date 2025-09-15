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
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	c1zmanager "github.com/conductorone/baton-sdk/pkg/dotc1z/manager"
	"github.com/conductorone/baton-sdk/pkg/sdk"
	"github.com/conductorone/baton-sdk/pkg/sync"
	"github.com/conductorone/baton-sdk/pkg/synccompactor/attached"
	"github.com/conductorone/baton-sdk/pkg/synccompactor/naive"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
)

var tracer = otel.Tracer("baton-sdk/pkg.synccompactor")

type CompactorType string

const (
	CompactorTypeNaive    CompactorType = "naive"
	CompactorTypeAttached CompactorType = "attached"
)

type Compactor struct {
	compactorType CompactorType
	entries       []*CompactableSync

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

func WithCompactorType(compactorType CompactorType) Option {
	return func(c *Compactor) {
		c.compactorType = compactorType
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

	var err error
	// Base sync is c.entries[0], so compact all incrementals first, then apply that onto the base.
	applied := c.entries[len(c.entries)-1]
	for i := len(c.entries) - 2; i >= 0; i-- {
		applied, err = c.doOneCompaction(ctx, c.entries[i], applied)
		if err != nil {
			return nil, err
		}
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
		sync.WithC1ZPath(applied.FilePath),
		sync.WithSyncID(applied.SyncID),
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
	finalPath := path.Join(c.destDir, fmt.Sprintf("compacted-%s.c1z", applied.SyncID))
	if err := cpFile(applied.FilePath, finalPath); err != nil {
		return nil, err
	}

	if !filepath.IsAbs(finalPath) {
		abs, err := filepath.Abs(finalPath)
		if err != nil {
			return nil, err
		}
		finalPath = abs
	}
	return &CompactableSync{FilePath: finalPath, SyncID: applied.SyncID}, nil
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

func (c *Compactor) getLatestObjects(ctx context.Context, info *CompactableSync) (*reader_v2.SyncRun, *dotc1z.C1File, c1zmanager.Manager, func(), error) {
	cleanup := func() {}
	baseC1Z, err := c1zmanager.New(ctx, info.FilePath, c1zmanager.WithTmpDir(c.tmpDir))
	if err != nil {
		return nil, nil, nil, cleanup, err
	}

	cleanup = func() {
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

func unionSyncTypes(a, b connectorstore.SyncType) connectorstore.SyncType {
	switch {
	case a == connectorstore.SyncTypeFull || b == connectorstore.SyncTypeFull:
		return connectorstore.SyncTypeFull
	case a == connectorstore.SyncTypeResourcesOnly || b == connectorstore.SyncTypeResourcesOnly:
		return connectorstore.SyncTypeResourcesOnly
	default:
		return connectorstore.SyncTypePartial
	}
}

func (c *Compactor) doOneCompaction(ctx context.Context, base *CompactableSync, applied *CompactableSync) (*CompactableSync, error) {
	ctx, span := tracer.Start(ctx, "Compactor.doOneCompaction")
	defer span.End()
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

	baseSync, baseFile, _, cleanupBase, err := c.getLatestObjects(ctx, base)
	defer cleanupBase()
	if err != nil {
		return nil, err
	}

	appliedSync, appliedFile, _, cleanupApplied, err := c.getLatestObjects(ctx, applied)
	defer cleanupApplied()
	if err != nil {
		return nil, err
	}

	syncType := unionSyncTypes(connectorstore.SyncType(baseSync.SyncType), connectorstore.SyncType(appliedSync.SyncType))

	newSyncId, err := newFile.StartNewSync(ctx, syncType, "")
	if err != nil {
		return nil, err
	}

	switch c.compactorType {
	case CompactorTypeNaive:
		// TODO: Add support for syncID or remove naive compactor.
		runner := naive.NewNaiveCompactor(baseFile, appliedFile, newFile)
		if err := runner.Compact(ctx); err != nil {
			l.Error("error running compaction", zap.Error(err))
			return nil, err
		}
	case CompactorTypeAttached:
		runner := attached.NewAttachedCompactor(baseFile, appliedFile, newFile)
		if err := runner.CompactWithSyncID(ctx, newSyncId); err != nil {
			l.Error("error running compaction", zap.Error(err))
			return nil, err
		}
	default:
		// c.compactorType defaults to attached, so this should never happen.
		return nil, fmt.Errorf("invalid compactor type: %s", c.compactorType)
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
		SyncID:   newSyncId,
	}, nil
}
