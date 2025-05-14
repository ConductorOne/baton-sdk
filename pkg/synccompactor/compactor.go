package synccompactor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"time"

	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	c1zmanager "github.com/conductorone/baton-sdk/pkg/dotc1z/manager"
	sync_compactor "github.com/conductorone/baton-sdk/pkg/synccompactor/naive"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

const intermediateDirPath = "intermediate"

type Compactor struct {
	entries []*CompactableSync

	workingDir string
	fs         *os.Root

	intermediateDir      *os.Root
	fullIntermediatePath string
}

type CompactableSync struct {
	FilePath string
	SyncID   string
}

var ErrNotEnoughFilesToCompact = errors.New("must provide two or more files to compact")

type Option func(*Compactor)

// WithWorkingDir sets the working directory where files will be created and edited during compaction.
// If not provided, the temporary directory will be used.
func WithWorkingDir(workingDir string) Option {
	return func(c *Compactor) {
		c.workingDir = workingDir
	}
}

func NewCompactor(ctx context.Context, compactableSyncs []*CompactableSync, opts ...Option) (*Compactor, func() error, error) {
	if len(compactableSyncs) < 2 {
		return nil, nil, ErrNotEnoughFilesToCompact
	}

	c := &Compactor{entries: compactableSyncs}
	for _, opt := range opts {
		opt(c)
	}

	// If no workingDir is provided, use the tmpDir
	if c.workingDir == "" {
		c.workingDir = path.Join(os.TempDir(), "baton_sync_compactor", time.Now().Format(time.RFC3339))
	}

	createdDir := false
	if _, err := os.Stat(c.workingDir); err != nil {
		if os.IsNotExist(err) {
			if err := os.MkdirAll(c.workingDir, 0755); err != nil {
				return nil, nil, err
			}
			createdDir = true
		} else {
			return nil, nil, err
		}
	}

	root, err := os.OpenRoot(c.workingDir)
	if err != nil {
		return nil, nil, err
	}
	cleanup := func() error {
		if err := root.Close(); err != nil {
			return err
		}
		if createdDir {
			if err := os.RemoveAll(c.workingDir); err != nil {
				return err
			}
		}
		return nil
	}
	c.fs = root

	_, err = c.fs.Stat(intermediateDirPath)
	if err != nil {
		if os.IsNotExist(err) {
			if err := c.fs.Mkdir(intermediateDirPath, 0755); err != nil {
				return nil, nil, err
			}
		} else {
			return nil, nil, err
		}
	}
	intermediate, err := c.fs.OpenRoot(intermediateDirPath)
	if err != nil {
		return nil, nil, err
	}
	c.intermediateDir = intermediate
	c.fullIntermediatePath = path.Join(c.workingDir, intermediateDirPath)

	return c, cleanup, nil
}

func (c *Compactor) Compact(ctx context.Context) (*CompactableSync, error) {
	if len(c.entries) < 2 {
		return nil, nil
	}

	intermediates := make([]string, 0, len(c.entries)-1)

	base := c.entries[0]
	for i := 1; i < len(c.entries); i++ {
		applied := c.entries[i]

		compactable, err := c.doOneCompaction(ctx, base, applied)
		if err != nil {
			return nil, err
		}
		// Collect all the intermediate files we create to remove at the end
		intermediates = append(intermediates, compactable.FilePath)
		base = compactable
	}

	lastFile, err := c.fs.Open(base.FilePath)
	if err != nil {
		return nil, err
	}

	// Create the final outFile path in the destination directory
	finalFileName := fmt.Sprintf("compacted-%s.c1z", base.SyncID)
	outFile, err := c.fs.Create(finalFileName)
	if err != nil {
		return nil, err
	}
	if _, err := io.Copy(outFile, lastFile); err != nil {
		return nil, err
	}

	stat, err := outFile.Stat()
	if err != nil {
		return nil, err
	}

	return &CompactableSync{FilePath: stat.Name(), SyncID: base.SyncID}, nil
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
		zap.String("intermediate_dir", c.fullIntermediatePath),
	)

	opts := []dotc1z.C1ZOption{
		dotc1z.WithPragma("journal_mode", "WAL"),
		dotc1z.WithTmpDir(c.fullIntermediatePath),
	}

	fileName := fmt.Sprintf("compacted-%s-%s.c1z", base.SyncID, applied.SyncID)
	newFile, err := dotc1z.NewC1ZFile(ctx, path.Join(c.fullIntermediatePath, fileName), opts...)
	if err != nil {
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
