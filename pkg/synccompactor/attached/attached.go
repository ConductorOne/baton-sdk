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

// Compactor merges data from an applied sync into a base sync using SQL
// ATTACH DATABASE. Both stores must be *dotc1z.C1File (SQLite-backed).
// An interface-level C1ZStore is accepted for caller convenience, but
// Compact returns an error if either store is not SQLite-backed.
type Compactor struct {
	base    *dotc1z.C1File
	applied *dotc1z.C1File
}

// NewAttachedCompactor builds a Compactor that merges `applied` into `base`.
// Both arguments are C1ZStore; the constructor type-asserts to *dotc1z.C1File
// and returns an error on mismatch. This keeps the public entry point clean
// while confining the SQLite-specific concern to the attached package.
func NewAttachedCompactor(base, applied dotc1z.C1ZStore) (*Compactor, error) {
	baseFile, ok := dotc1z.AsSQLiteStore(base)
	if !ok {
		return nil, fmt.Errorf("attached compactor requires SQLite-backed base store, got %T", base)
	}
	appliedFile, ok := dotc1z.AsSQLiteStore(applied)
	if !ok {
		return nil, fmt.Errorf("attached compactor requires SQLite-backed applied store, got %T", applied)
	}
	return &Compactor{
		base:    baseFile,
		applied: appliedFile,
	}, nil
}

func latestFinishedCompactableSync(ctx context.Context, f *dotc1z.C1File) (*reader_v2.SyncRun, error) {
	// Compaction must NOT operate on diff syncs (partial_upserts / partial_deletions).
	// We want the latest finished "snapshot-like" sync.
	candidates := []connectorstore.SyncType{
		connectorstore.SyncTypeFull,
		connectorstore.SyncTypeResourcesOnly,
		connectorstore.SyncTypePartial,
	}

	var best *reader_v2.SyncRun
	for _, st := range candidates {
		resp, err := f.GetLatestFinishedSync(ctx, reader_v2.SyncsReaderServiceGetLatestFinishedSyncRequest_builder{
			SyncType: string(st),
		}.Build())
		if err != nil {
			return nil, err
		}
		s := resp.GetSync()
		if s == nil {
			continue
		}

		if best == nil || s.GetEndedAt().AsTime().After(best.GetEndedAt().AsTime()) {
			best = s
		}
	}

	return best, nil
}

func (c *Compactor) Compact(ctx context.Context) error {
	baseSync, err := latestFinishedCompactableSync(ctx, c.base)
	if err != nil {
		return fmt.Errorf("failed to get base sync: %w", err)
	}
	if baseSync == nil {
		return fmt.Errorf(
			"no finished compactable sync found in base (diff sync types %q/%q are not compactable)",
			string(connectorstore.SyncTypePartialUpserts),
			string(connectorstore.SyncTypePartialDeletions),
		)
	}

	appliedSync, err := latestFinishedCompactableSync(ctx, c.applied)
	if err != nil {
		return fmt.Errorf("failed to get applied sync: %w", err)
	}
	if appliedSync == nil {
		return fmt.Errorf(
			"no finished compactable sync found in applied (diff sync types %q/%q are not compactable)",
			string(connectorstore.SyncTypePartialUpserts),
			string(connectorstore.SyncTypePartialDeletions),
		)
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

	if err := c.processRecords(ctx, attached, baseSync, appliedSync); err != nil {
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
