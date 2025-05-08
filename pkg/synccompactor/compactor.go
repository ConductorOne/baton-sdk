package synccompactor

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"

	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	c1zmanager "github.com/conductorone/baton-sdk/pkg/dotc1z/manager"
	sync_compactor "github.com/conductorone/baton-sdk/pkg/synccompactor/naive"
)

type Compactor struct {
	entries []*CompactableSync
	destDir string
}

type CompactableSync struct {
	filePath string
	syncID   string
}

func NewCompactor(ctx context.Context, destDir string, compactableSyncs ...*CompactableSync) (*Compactor, error) {
	if len(compactableSyncs) < 2 {
		return nil, errors.New("must provide two or more files to Compact")
	}

	return &Compactor{entries: compactableSyncs, destDir: destDir}, nil
}

func (c *Compactor) Compact(ctx context.Context) (*CompactableSync, error) {
	if len(c.entries) < 2 {
		return nil, nil
	}

	tempDir := os.TempDir()

	intermediates := make([]string, 0, len(c.entries)-1)

	base := c.entries[0]
	for i := 1; i < len(c.entries); i++ {
		applied := c.entries[i]

		compactable, err := c.doOneCompaction(ctx, tempDir, base, applied)
		if err != nil {
			return nil, err
		}
		// Collect all the intermediate files we create to remove at the end
		intermediates = append(intermediates, compactable.filePath)
		base = compactable
	}

	if len(intermediates) > 0 {
		// The last one is our "base" so we don't want to remove that one
		intermediates = intermediates[:len(intermediates)-1]
		for _, intermediateFile := range intermediates {
			err := os.Remove(intermediateFile)
			// Weird case if the file doesn't exist but it's "fine".
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				return nil, err
			}
		}
	}

	return base, nil
}

func getLatestObjects(ctx context.Context, info *CompactableSync) (*reader_v2.SyncRun, *dotc1z.C1File, c1zmanager.Manager, func(), error) {
	baseC1Z, err := c1zmanager.New(ctx, info.filePath)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	baseFile, err := baseC1Z.LoadC1Z(ctx)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	latestAppliedSync, err := baseFile.GetSync(ctx, &reader_v2.SyncsReaderServiceGetSyncRequest{
		SyncId:      info.syncID,
		Annotations: nil,
	})
	if err != nil {
		return nil, nil, nil, nil, err
	}

	return latestAppliedSync.Sync, baseFile, baseC1Z, func() {
		_ = baseFile.Close()
		_ = baseC1Z.Close(ctx)
	}, nil
}

func (c *Compactor) doOneCompaction(ctx context.Context, tempDir string, base *CompactableSync, applied *CompactableSync) (*CompactableSync, error) {
	filePath := path.Join(tempDir, fmt.Sprintf("compacted-%s-%s.c1z", base.syncID, applied.syncID))

	newFile, err := dotc1z.NewC1ZFile(ctx, filePath, dotc1z.WithPragma("journal_mode", "WAL"))
	if err != nil {
		return nil, err
	}

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
		return nil, err
	}

	if err := newFile.EndSync(ctx); err != nil {
		return nil, err
	}

	if err := newFile.Close(); err != nil {
		return nil, err
	}

	return &CompactableSync{
		filePath: filePath,
		syncID:   newSync,
	}, nil
}
