package attached

import (
	"context"
	"fmt"

	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

type Compactor struct {
	base    *dotc1z.C1File
	applied *dotc1z.C1File
}

func NewAttachedCompactor(base *dotc1z.C1File, applied *dotc1z.C1File) *Compactor {
	return &Compactor{
		base:    base,
		applied: applied,
	}
}

func (c *Compactor) Compact(ctx context.Context) error {
	baseSync, err := c.base.GetLatestFinishedSync(ctx, reader_v2.SyncsReaderServiceGetLatestFinishedSyncRequest_builder{
		SyncType: string(connectorstore.SyncTypeAny),
	}.Build())
	if err != nil {
		return fmt.Errorf("failed to get base sync: %w", err)
	}
	if baseSync == nil || baseSync.GetSync() == nil {
		return fmt.Errorf("no finished sync found in base")
	}

	appliedSync, err := c.applied.GetLatestFinishedSync(ctx, reader_v2.SyncsReaderServiceGetLatestFinishedSyncRequest_builder{
		SyncType: string(connectorstore.SyncTypeAny),
	}.Build())
	if err != nil {
		return fmt.Errorf("failed to get applied sync: %w", err)
	}
	if appliedSync == nil || appliedSync.GetSync() == nil {
		return fmt.Errorf("no finished sync found in applied")
	}

	l := ctxzap.Extract(ctx)

	// Attach both the base and applied databases to the destination
	attached, err := c.base.AttachFile(c.applied, "attached")
	if err != nil {
		return fmt.Errorf("failed to attach databases to destination: %w", err)
	}
	defer func() {
		_, err := attached.DetachFile("attached")
		if err != nil {
			l.Error("failed to detach file", zap.Error(err))
		}
	}()

	if err := c.processRecords(ctx, attached, baseSync.GetSync(), appliedSync.GetSync()); err != nil {
		return fmt.Errorf("failed to process records: %w", err)
	}

	return nil
}

func (c *Compactor) processRecords(ctx context.Context, attached *dotc1z.C1FileAttached, baseSync *reader_v2.SyncRun, appliedSync *reader_v2.SyncRun) error {
	baseSyncID := baseSync.GetId()
	appliedSyncID := appliedSync.GetId()

	// Update the base sync type to the union of the base and applied sync types.
	if err := attached.UpdateSync(ctx, baseSync, appliedSync); err != nil {
		return fmt.Errorf("failed to update sync %s: %w", baseSyncID, err)
	}

	// Compact all tables: copy base records and merge newer applied records using raw SQL
	if err := attached.CompactResourceTypes(ctx, baseSyncID, appliedSyncID); err != nil {
		return fmt.Errorf("failed to compact resource types: %w", err)
	}

	if err := attached.CompactResources(ctx, baseSyncID, appliedSyncID); err != nil {
		return fmt.Errorf("failed to compact resources: %w", err)
	}

	if err := attached.CompactEntitlements(ctx, baseSyncID, appliedSyncID); err != nil {
		return fmt.Errorf("failed to compact entitlements: %w", err)
	}

	if err := attached.CompactGrants(ctx, baseSyncID, appliedSyncID); err != nil {
		return fmt.Errorf("failed to compact grants: %w", err)
	}

	return nil
}
