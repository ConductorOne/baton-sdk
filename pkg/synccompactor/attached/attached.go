package attached

import (
	"context"
	"fmt"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

type Compactor struct {
	base    *dotc1z.C1File
	applied *dotc1z.C1File
	dest    *dotc1z.C1File
}

func NewAttachedCompactor(base *dotc1z.C1File, applied *dotc1z.C1File, dest *dotc1z.C1File) *Compactor {
	return &Compactor{
		base:    base,
		applied: applied,
		dest:    dest,
	}
}

func (c *Compactor) CompactWithSyncID(ctx context.Context, destSyncID string) error {
	// Get the latest finished full sync ID from base
	baseSyncID, err := c.base.LatestFinishedSync(ctx, connectorstore.SyncTypeAny)
	if err != nil {
		return fmt.Errorf("failed to get base sync ID: %w", err)
	}
	if baseSyncID == "" {
		return fmt.Errorf("no finished sync found in base")
	}

	// Get the latest finished sync ID from applied (any type)
	appliedSyncID, err := c.applied.LatestFinishedSync(ctx, connectorstore.SyncTypeAny)
	if err != nil {
		return fmt.Errorf("failed to get applied sync ID: %w", err)
	}
	if appliedSyncID == "" {
		return fmt.Errorf("no finished sync found in applied")
	}

	// Attach both the base and applied databases to the destination
	base, err := c.dest.AttachFile(c.base, "base")
	if err != nil {
		return fmt.Errorf("failed to attach databases to destination: %w", err)
	}
	l := ctxzap.Extract(ctx)
	defer func() {
		_, err := base.DetachFile("base")
		if err != nil {
			l.Error("failed to detach file", zap.Error(err))
		}
	}()

	// Attach both the base and applied databases to the destination
	attached, err := c.dest.AttachFile(c.applied, "attached")
	if err != nil {
		return fmt.Errorf("failed to attach databases to destination: %w", err)
	}
	defer func() {
		_, err := attached.DetachFile("attached")
		if err != nil {
			l.Error("failed to detach file", zap.Error(err))
		}
	}()

	if err := c.processRecords(ctx, attached, destSyncID, baseSyncID, appliedSyncID); err != nil {
		return fmt.Errorf("failed to process records: %w", err)
	}

	return nil
}

func (c *Compactor) processRecords(ctx context.Context, attached *dotc1z.C1FileAttached, destSyncID string, baseSyncID string, appliedSyncID string) error {
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
