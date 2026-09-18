package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"errors"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/uotel"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *syncer) getLedgerResourceFromConnector(ctx context.Context, resourceID *v2.ResourceId, parentResourceID *v2.ResourceId) (*v2.Resource, error) {
	ctx, span := tracer.Start(ctx, "syncer.getResource")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	start := time.Now()
	resourceResp, err := s.connector.GetResource(ctx,
		v2.ResourceGetterServiceGetResourceRequest_builder{
			ResourceId:       resourceID,
			ParentResourceId: parentResourceID,
			ActiveSyncId:     s.getActiveSyncID(),
		}.Build(),
	)
	invocation := ctx.Value(ledgerInvocationKey{}).(*ledgerInvocation)
	s.recordLedgerConnectorResponse(ctx, invocation, "get-resource", time.Since(start), resourceResp.GetAnnotations())
	if err == nil {
		return resourceResp.GetResource(), nil
	}
	l := ctxzap.Extract(ctx)
	if status.Code(err) == codes.NotFound {
		l.Warn("skipping resource due to not found", zap.String("resource_id", resourceID.GetResource()), zap.String("resource_type_id", resourceID.GetResourceType()))
		return nil, nil
	}
	if status.Code(err) == codes.Unimplemented {
		l.Warn("skipping resource due to unimplemented connector", zap.String("resource_id", resourceID.GetResource()), zap.String("resource_type_id", resourceID.GetResourceType()))
		return nil, nil
	}
	return nil, err
}

func (s *syncer) syncLedgerTargetedResource(ctx context.Context, action *Action) error {
	invocation, ok := ctx.Value(ledgerInvocationKey{}).(*ledgerInvocation)
	if !ok {
		return errors.New("targeted resource ledger handler requires an open page")
	}
	start := time.Now()
	defer func() { invocation.page.row.PageDuration = time.Since(start) }()

	ctx, span := uotel.StartWithLink(ctx, tracer, "syncer.SyncTargetedResource")
	uotel.SetSyncIdentityAttrs(ctx, span)
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	resourceID := action.ResourceID
	resourceTypeID := action.ResourceTypeID
	if resourceID == "" || resourceTypeID == "" {
		return errors.New("cannot get resource without a resource target")
	}

	parentResourceID := action.ParentResourceID
	parentResourceTypeID := action.ParentResourceTypeID
	var prID *v2.ResourceId
	if parentResourceID != "" && parentResourceTypeID != "" {
		prID = v2.ResourceId_builder{
			ResourceType: parentResourceTypeID,
			Resource:     parentResourceID,
		}.Build()
	}

	resource, err := s.getLedgerResourceFromConnector(ctx, v2.ResourceId_builder{
		ResourceType: resourceTypeID,
		Resource:     resourceID,
	}.Build(), prID)
	if err != nil {
		return err
	}

	if resource == nil {
		return s.nextPageOrFinishAction(ctx, action, "")
	}
	resources, err := s.filterLedgerResources(invocation, []*v2.Resource{resource})
	if err != nil {
		return err
	}
	if len(resources) == 0 {
		return s.nextPageOrFinishAction(ctx, action, "")
	}
	resource = resources[0]

	if err := invocation.page.writer.PutResources(ctx, resource); err != nil {
		return err
	}

	var followupActions []Action

	shouldSkipGrants, err := s.shouldSkipGrants(ctx, resource)
	if err != nil {
		return err
	}
	if !shouldSkipGrants {
		typeScopedGrants, err := s.resourceTypeHasTypeScopedGrants(ctx, resourceTypeID)
		if err != nil {
			return err
		}
		if !typeScopedGrants {
			followupActions = append(followupActions, Action{
				Op:             SyncGrantsOp,
				ResourceTypeID: resourceTypeID,
				ResourceID:     resourceID,
			})
		}
	}

	shouldSkipEnts, err := s.shouldSkipEntitlements(ctx, resource)
	if err != nil {
		return err
	}

	if !shouldSkipEnts {
		typeScopedEnts, err := s.resourceTypeHasTypeScopedEntitlements(ctx, resourceTypeID)
		if err != nil {
			return err
		}
		if !typeScopedEnts {
			followupActions = append(followupActions, Action{
				Op:             SyncEntitlementsOp,
				ResourceTypeID: resourceTypeID,
				ResourceID:     resourceID,
			})
		}
	}

	childTypeIDs, err := childResourceTypeIDs(resource)
	if err != nil {
		return err
	}
	childActions := s.pendingChildResourceActions(childTypeIDs, resourceTypeID, resourceID)
	followupActions = append(followupActions, childActions...)

	invocation.resourceChildren = true
	return s.nextPageOrFinishAction(ctx, action, "", followupActions...)
}
