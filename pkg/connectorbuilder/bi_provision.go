package connectorbuilder

import (
	"context"
	"fmt"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/retry"
	"github.com/conductorone/baton-sdk/pkg/types/tasks"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (b *builderImpl) Grant(ctx context.Context, request *v2.GrantManagerServiceGrantRequest) (*v2.GrantManagerServiceGrantResponse, error) {
	ctx, span := tracer.Start(ctx, "builderImpl.Grant")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.GrantType
	l := ctxzap.Extract(ctx)

	rt := request.Entitlement.Resource.Id.ResourceType

	retryer := retry.NewRetryer(ctx, retry.RetryConfig{
		MaxAttempts:  3,
		InitialDelay: 15 * time.Second,
		MaxDelay:     60 * time.Second,
	})

	provisioner, ok := b.resourceProvisioners[rt]
	if !ok {
		l.Error("error: resource type does not have provisioner configured", zap.String("resource_type", rt))
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: resource type does not have provisioner configured")
	}

	for {
		grants, annos, err := provisioner.Grant(ctx, request.Principal, request.Entitlement)
		if err == nil {
			b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
			return &v2.GrantManagerServiceGrantResponse{Annotations: annos, Grants: grants}, nil
		}
		if retryer.ShouldWaitAndRetry(ctx, err) {
			continue
		}
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("grant failed: %w", err)
	}
}

func (b *builderImpl) Revoke(ctx context.Context, request *v2.GrantManagerServiceRevokeRequest) (*v2.GrantManagerServiceRevokeResponse, error) {
	ctx, span := tracer.Start(ctx, "builderImpl.Revoke")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.RevokeType

	l := ctxzap.Extract(ctx)

	rt := request.Grant.Entitlement.Resource.Id.ResourceType

	retryer := retry.NewRetryer(ctx, retry.RetryConfig{
		MaxAttempts:  3,
		InitialDelay: 15 * time.Second,
		MaxDelay:     60 * time.Second,
	})

	provisioner, ok := b.resourceProvisioners[rt]
	if !ok {
		l.Error("error: resource type does not have provisioner configured", zap.String("resource_type", rt))
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: resource type does not have provisioner configured")
	}

	for {
		annos, err := provisioner.Revoke(ctx, request.Grant)
		if err == nil {
			b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
			return &v2.GrantManagerServiceRevokeResponse{Annotations: annos}, nil
		}
		if retryer.ShouldWaitAndRetry(ctx, err) {
			continue
		}
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("revoke failed: %w", err)
	}
}

func (b *builderImpl) CreateResource(ctx context.Context, request *v2.CreateResourceRequest) (*v2.CreateResourceResponse, error) {
	ctx, span := tracer.Start(ctx, "builderImpl.CreateResource")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.CreateResourceType
	l := ctxzap.Extract(ctx)
	rt := request.GetResource().GetId().GetResourceType()

	var manager interface {
		Create(ctx context.Context, resource *v2.Resource) (*v2.Resource, annotations.Annotations, error)
	}

	manager, ok := b.resourceManagers[rt]
	if !ok {
		l.Error("error: resource type does not have resource Create() configured", zap.String("resource_type", rt))
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, status.Error(codes.Unimplemented, fmt.Sprintf("resource type %s does not have resource Create() configured", rt))
	}

	resource, annos, err := manager.Create(ctx, request.Resource)
	if err != nil {
		l.Error("error: create resource failed", zap.Error(err))
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: create resource failed: %w", err)
	}
	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return &v2.CreateResourceResponse{Created: resource, Annotations: annos}, nil
}

func (b *builderImpl) DeleteResource(ctx context.Context, request *v2.DeleteResourceRequest) (*v2.DeleteResourceResponse, error) {
	ctx, span := tracer.Start(ctx, "builderImpl.DeleteResource")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.DeleteResourceType

	l := ctxzap.Extract(ctx)
	rt := request.GetResourceId().GetResourceType()
	var rsDeleter ResourceDeleterV2
	var ok bool

	rsDeleter, ok = b.resourceManagers[rt]
	if !ok {
		rsDeleter, ok = b.resourceDeleters[rt]
	}

	if !ok {
		l.Error("error: resource type does not have resource Delete() configured", zap.String("resource_type", rt))
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, status.Error(codes.Unimplemented, fmt.Sprintf("resource type %s does not have resource Delete() configured", rt))
	}

	annos, err := rsDeleter.Delete(ctx, request.ResourceId, request.ParentResourceId)
	if err != nil {
		l.Error("error: delete resource failed", zap.Error(err))
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: delete resource failed: %w", err)
	}
	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return &v2.DeleteResourceResponse{Annotations: annos}, nil
}

func (b *builderImpl) DeleteResourceV2(ctx context.Context, request *v2.DeleteResourceV2Request) (*v2.DeleteResourceV2Response, error) {
	ctx, span := tracer.Start(ctx, "builderImpl.DeleteResourceV2")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.DeleteResourceType

	l := ctxzap.Extract(ctx)
	rt := request.GetResourceId().GetResourceType()
	var rsDeleterV2 ResourceDeleterV2
	var ok bool

	rsDeleterV2, ok = b.resourceManagers[rt]
	if !ok {
		rsDeleterV2, ok = b.resourceDeleters[rt]
	}

	if !ok {
		// If we got here, no resource deleter was found for this resource type
		l.Error("error: resource type does not have resource Delete() configured", zap.String("resource_type", rt))
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, status.Error(codes.Unimplemented, fmt.Sprintf("resource type %s does not have resource Delete() configured", rt))
	}
	annos, err := rsDeleterV2.Delete(ctx, request.ResourceId, request.ParentResourceId)
	if err != nil {
		l.Error("error: deleteV2 resource failed", zap.Error(err))
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: delete resource failed: %w", err)
	}
	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return &v2.DeleteResourceV2Response{Annotations: annos}, nil
}
