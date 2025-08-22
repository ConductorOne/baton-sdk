package attached

import (
	"context"
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine"
)

type Compactor struct {
	base    engine.StorageEngine
	applied engine.StorageEngine
	dest    engine.StorageEngine
}

func NewAttachedCompactor(base engine.StorageEngine, applied engine.StorageEngine, dest engine.StorageEngine) *Compactor {
	return &Compactor{
		base:    base,
		applied: applied,
		dest:    dest,
	}
}

func (c *Compactor) CompactWithSyncID(ctx context.Context, destSyncID string) error {
	// Get the latest finished full sync ID from base
	baseSyncResp, err := c.base.GetLatestFinishedSync(ctx, &v2.SyncsReaderServiceGetLatestFinishedSyncRequest{
		SyncType: string(engine.SyncTypeFull),
	})
	if err != nil {
		return fmt.Errorf("failed to get base sync ID: %w", err)
	}
	baseSyncID := baseSyncResp.GetSync().GetId()
	if baseSyncID == "" {
		return fmt.Errorf("no finished full sync found in base")
	}

	// Get the latest finished sync ID from applied (any type)
	appliedSyncResp, err := c.applied.GetLatestFinishedSync(ctx, &v2.SyncsReaderServiceGetLatestFinishedSyncRequest{
		SyncType: string(engine.SyncTypeAny),
	})
	if err != nil {
		return fmt.Errorf("failed to get applied sync ID: %w", err)
	}
	appliedSyncID := appliedSyncResp.GetSync().GetId()
	if appliedSyncID == "" {
		return fmt.Errorf("no finished sync found in applied")
	}

	// Attach both the base and applied databases to the destination
	base, err := c.dest.AttachFile(c.base, "base")
	if err != nil {
		return fmt.Errorf("failed to attach databases to destination: %w", err)
	}
	defer func() {
		_, _ = base.DetachFile("base")
	}()

	// Attach both the base and applied databases to the destination
	attached, err := c.dest.AttachFile(c.applied, "attached")
	if err != nil {
		return fmt.Errorf("failed to attach databases to destination: %w", err)
	}
	defer func() {
		_, _ = attached.DetachFile("attached")
	}()

	if err := c.processRecords(ctx, attached, destSyncID, baseSyncID, appliedSyncID); err != nil {
		return fmt.Errorf("failed to process records: %w", err)
	}

	return nil
}

func (c *Compactor) processRecords(ctx context.Context, attached engine.AttachedStorageEngine, destSyncID string, baseSyncID string, appliedSyncID string) error {
	// Compact all tables: copy base records and merge newer applied records using raw SQL
	if err := attached.CompactResourceTypes(ctx, destSyncID, baseSyncID, appliedSyncID); err != nil {
		return fmt.Errorf("failed to compact resource types: %w", err)
	}

	if err := attached.CompactResources(ctx, destSyncID, baseSyncID, appliedSyncID); err != nil {
		return fmt.Errorf("failed to compact resources: %w", err)
	}

	if err := attached.CompactEntitlements(ctx, destSyncID, baseSyncID, appliedSyncID); err != nil {
		return fmt.Errorf("failed to compact entitlements: %w", err)
	}

	if err := attached.CompactGrants(ctx, destSyncID, baseSyncID, appliedSyncID); err != nil {
		return fmt.Errorf("failed to compact grants: %w", err)
	}

	return nil
}
