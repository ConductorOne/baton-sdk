package sync_compactor

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"

	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	c1zmanager "github.com/conductorone/baton-sdk/pkg/dotc1z/manager"
	"google.golang.org/protobuf/proto"
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
	if len(compactableSyncs) == 0 || len(compactableSyncs) == 1 {
		return nil, errors.New("must provide two or more files to compact")
	}

	return &Compactor{entries: compactableSyncs, destDir: destDir}, nil
}

func (c *Compactor) Compact(ctx context.Context) (*CompactableSync, error) {
	if len(c.entries) < 2 {
		return nil, nil
	}

	tempDir := os.TempDir()

	base := c.entries[0]
	var latest *CompactableSync
	for i := 1; i < len(c.entries); i++ {
		applied := c.entries[i]

		compactable, err := c.doOneCompaction(ctx, tempDir, base, applied)
		if err != nil {
			return nil, err
		}
		latest = compactable
		base = compactable
	}

	return latest, nil
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

	newFile, err := dotc1z.NewC1ZFile(ctx, filePath, dotc1z.WithTmpDir(tempDir), dotc1z.WithPragma("journal_mode", "WAL"))
	if err != nil {
		return nil, err
	}

	baseSync, baseFile, _, cleanupBase, err := getLatestObjects(ctx, base)
	if err != nil {
		return nil, err
	}
	defer cleanupBase()

	appliedSync, appliedFile, _, cleanupApplied, err := getLatestObjects(ctx, applied)
	defer cleanupApplied()
	if err != nil {
		return nil, err
	}

	// If the applied sync started after the base sync ended we have fully disjoint sets
	fullyDisjoint := appliedSync.StartedAt.AsTime().After(baseSync.StartedAt.AsTime())

	// TODO: Use the runner when implementing the compaction logic
	runner := &naiveCompactor{
		base:    baseFile,
		applied: appliedFile,
		dest:    newFile,

		appliedSyncedAfterBase: fullyDisjoint,
	}

	if err := runner.processResourceTypes(ctx); err != nil {
		return nil, err
	}
	if err := runner.processResources(ctx); err != nil {
		return nil, err
	}
	if err := runner.processEntitlements(ctx); err != nil {
		return nil, err
	}
	if err := runner.processGrants(ctx); err != nil {
		return nil, err
	}

	if err := newFile.EndSync(ctx); err != nil {
		return nil, err
	}

	if err := newFile.Close(); err != nil {
		return nil, err
	}

	return nil, nil
}

type naiveCompactor struct {
	base    *dotc1z.C1File
	applied *dotc1z.C1File
	dest    *dotc1z.C1File

	appliedSyncedAfterBase bool
}

func naiveCompact[T proto.Message, REQ listRequest, RESP listResponse[T]](
	ctx context.Context,
	base listFunc[T, REQ, RESP],
	applied listFunc[T, REQ, RESP],
	save func(context.Context, ...T) error,
) error {
	// List all objects from the base file and save them in the destination file
	if err := listAllObjects(ctx, base, func(items []T) (bool, error) {
		if err := save(ctx, items...); err != nil {
			return false, err
		}
		return true, nil
	}); err != nil {
		return err
	}

	// Then list all objects from the applied file and save them in the destination file, overwriting ones with the same external_id
	if err := listAllObjects(ctx, applied, func(items []T) (bool, error) {
		if err := save(ctx, items...); err != nil {
			return false, err
		}
		return true, nil
	}); err != nil {
		return err
	}

	return nil
}

func (c *naiveCompactor) processResourceTypes(ctx context.Context) error {
	return naiveCompact(ctx, c.base.ListResourceTypes, c.applied.ListResourceTypes, c.dest.PutResourceTypes)
}

func (c *naiveCompactor) processResources(ctx context.Context) error {
	return naiveCompact(ctx, c.base.ListResources, c.applied.ListResources, c.dest.PutResources)
}
func (c *naiveCompactor) processGrants(ctx context.Context) error {
	return naiveCompact(ctx, c.base.ListGrants, c.applied.ListGrants, c.dest.PutGrants)
}

func (c *naiveCompactor) processEntitlements(ctx context.Context) error {
	return naiveCompact(ctx, c.base.ListEntitlements, c.applied.ListEntitlements, c.dest.PutEntitlements)
}
