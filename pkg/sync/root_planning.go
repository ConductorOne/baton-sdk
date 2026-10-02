package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
)

func (s *syncer) planRootEntitlementActions(ctx context.Context, action *Action) ([]Action, string, bool, error) {
	actions := make([]Action, 0)
	pageToken := action.PageToken
	plannedTypeScoped := false

	if pageToken == "" {
		ctxzap.Extract(ctx).Info("Syncing entitlements...")
		s.handleInitialActionForStep(ctx, *action)
	}

	if !action.TypeScopedPlanned {
		typeScoped, err := s.typeScopedEntitlementsResourceTypes(ctx)
		if err != nil {
			return nil, "", false, fmt.Errorf("sync-entitlements: error listing type-scoped resource types: %w", err)
		}
		for _, rtID := range typeScoped {
			actions = append(actions, Action{Op: SyncEntitlementsOp, ResourceTypeID: rtID, TypeScoped: true})
		}
		plannedTypeScoped = true
	}

	resp, err := s.store.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
		PageToken:    pageToken,
		ActiveSyncId: s.getActiveSyncID(),
	}.Build())
	if err != nil {
		return nil, "", false, err
	}

	for _, r := range resp.GetList() {
		shouldSkipEntitlements, err := s.shouldSkipEntitlements(ctx, r)
		if err != nil {
			return nil, "", false, err
		}
		if shouldSkipEntitlements {
			continue
		}
		typeScoped, err := s.resourceTypeHasTypeScopedEntitlements(ctx, r.GetId().GetResourceType())
		if err != nil {
			return nil, "", false, err
		}
		if typeScoped {
			continue
		}
		actions = append(actions, Action{Op: SyncEntitlementsOp, ResourceID: r.GetId().GetResource(), ResourceTypeID: r.GetId().GetResourceType()})
	}

	return actions, resp.GetNextPageToken(), plannedTypeScoped, nil
}

func (s *syncer) planRootGrantActions(ctx context.Context, action *Action) ([]Action, string, bool, error) {
	actions := make([]Action, 0)
	plannedTypeScoped := false
	if action.PageToken == "" {
		ctxzap.Extract(ctx).Info("Syncing grants...")
		s.handleInitialActionForStep(ctx, *action)
	}

	if !action.TypeScopedPlanned {
		typeScoped, err := s.typeScopedGrantsResourceTypes(ctx)
		if err != nil {
			return nil, "", false, fmt.Errorf("sync-grants: error listing type-scoped resource types: %w", err)
		}
		for _, rtID := range typeScoped {
			actions = append(actions, Action{Op: SyncGrantsOp, ResourceTypeID: rtID, TypeScoped: true})
		}
		plannedTypeScoped = true
	}

	resp, err := s.store.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
		PageToken:    action.PageToken,
		ActiveSyncId: s.getActiveSyncID(),
	}.Build())
	if err != nil {
		return nil, "", false, fmt.Errorf("sync-grants: error listing resources: %w", err)
	}

	for _, r := range resp.GetList() {
		shouldSkip, err := s.shouldSkipGrants(ctx, r)
		if err != nil {
			return nil, "", false, err
		}

		if shouldSkip {
			continue
		}
		typeScoped, err := s.resourceTypeHasTypeScopedGrants(ctx, r.GetId().GetResourceType())
		if err != nil {
			return nil, "", false, err
		}
		if typeScoped {
			continue
		}
		actions = append(actions, Action{Op: SyncGrantsOp, ResourceID: r.GetId().GetResource(), ResourceTypeID: r.GetId().GetResourceType()})
	}

	return actions, resp.GetNextPageToken(), plannedTypeScoped, nil
}
