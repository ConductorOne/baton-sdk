package connectorbuilder

import (
	"context"
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/types/tasks"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ResourceManager extends ResourceSyncer to add capabilities for creating resources.
//
// Implementing this interface indicates the connector supports creating and deleting resources
// of the associated resource type. A ResourceManager automatically provides ResourceDeleter
// functionality.
type ResourceManager interface {
	ResourceSyncer
	Create(ctx context.Context, resource *v2.Resource) (*v2.Resource, annotations.Annotations, error)
	ResourceDeleter
}

// ResourceManagerV2 extends ResourceSyncer to add capabilities for creating resources.
//
// This is the recommended interface for implementing resource creation operations in new connectors.
type ResourceManagerV2 interface {
	ResourceSyncer
	Create(ctx context.Context, resource *v2.Resource) (*v2.Resource, annotations.Annotations, error)
	ResourceDeleterV2
}

// ResourceDeleter extends ResourceSyncer to add capabilities for deleting resources.
//
// Implementing this interface indicates the connector supports deleting resources
// of the associated resource type.
type ResourceDeleter interface {
	ResourceSyncer
	Delete(ctx context.Context, resourceId *v2.ResourceId) (annotations.Annotations, error)
}

// ResourceDeleterV2 extends ResourceSyncer to add capabilities for deleting resources.
//
// This is the recommended interface for implementing resource deletion operations in new connectors.
// It differs from ResourceDeleter by having the resource, not just the id.
type ResourceDeleterV2 interface {
	ResourceSyncer
	Delete(ctx context.Context, resourceId *v2.ResourceId, parentResourceID *v2.ResourceId) (annotations.Annotations, error)
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

	manager, ok := b.resourceManagersV2[rt]
	if !ok {
		manager, ok = b.resourceManagers[rt]
	}

	if ok {
		resource, annos, err := manager.Create(ctx, request.Resource)
		if err != nil {
			l.Error("error: create resource failed", zap.Error(err))
			b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
			return nil, fmt.Errorf("error: create resource failed: %w", err)
		}
		b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
		return &v2.CreateResourceResponse{Created: resource, Annotations: annos}, nil
	}
	l.Error("error: resource type does not have resource Create() configured", zap.String("resource_type", rt))
	b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
	return nil, status.Error(codes.Unimplemented, fmt.Sprintf("resource type %s does not have resource Create() configured", rt))
}

func (b *builderImpl) DeleteResource(ctx context.Context, request *v2.DeleteResourceRequest) (*v2.DeleteResourceResponse, error) {
	ctx, span := tracer.Start(ctx, "builderImpl.DeleteResource")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.DeleteResourceType

	l := ctxzap.Extract(ctx)
	rt := request.GetResourceId().GetResourceType()
	var rsDeleter ResourceDeleter
	var rsDeleterV2 ResourceDeleterV2
	var ok bool

	rsDeleterV2, ok = b.resourceManagersV2[rt]
	if !ok {
		rsDeleterV2, ok = b.resourceDeletersV2[rt]
	}

	if ok {
		annos, err := rsDeleterV2.Delete(ctx, request.ResourceId, request.ParentResourceId)
		if err != nil {
			l.Error("error: deleteV2 resource failed", zap.Error(err))
			b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
			return nil, fmt.Errorf("error: delete resource failed: %w", err)
		}
		b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
		return &v2.DeleteResourceResponse{Annotations: annos}, nil
	}

	rsDeleter, ok = b.resourceManagers[rt]
	if !ok {
		rsDeleter, ok = b.resourceDeleters[rt]
	}
	if ok {
		annos, err := rsDeleter.Delete(ctx, request.GetResourceId())
		if err != nil {
			l.Error("error: delete resource failed", zap.Error(err))
			b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
			return nil, fmt.Errorf("error: delete resource failed: %w", err)
		}
		b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
		return &v2.DeleteResourceResponse{Annotations: annos}, nil
	}
	l.Error("error: resource type does not have resource Delete() configured", zap.String("resource_type", rt))
	b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
	return nil, status.Error(codes.Unimplemented, fmt.Sprintf("resource type %s does not have resource Delete() configured", rt))
}

func (b *builderImpl) DeleteResourceV2(ctx context.Context, request *v2.DeleteResourceV2Request) (*v2.DeleteResourceV2Response, error) {
	ctx, span := tracer.Start(ctx, "builderImpl.DeleteResourceV2")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.DeleteResourceType

	l := ctxzap.Extract(ctx)
	rt := request.GetResourceId().GetResourceType()
	var rsDeleter ResourceDeleter
	var rsDeleterV2 ResourceDeleterV2
	var ok bool

	rsDeleterV2, ok = b.resourceManagersV2[rt]
	if !ok {
		rsDeleterV2, ok = b.resourceDeletersV2[rt]
	}

	if ok {
		annos, err := rsDeleterV2.Delete(ctx, request.ResourceId, request.ParentResourceId)
		if err != nil {
			l.Error("error: deleteV2 resource failed", zap.Error(err))
			b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
			return nil, fmt.Errorf("error: delete resource failed: %w", err)
		}
		b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
		return &v2.DeleteResourceV2Response{Annotations: annos}, nil
	}

	rsDeleter, ok = b.resourceManagers[rt]
	if !ok {
		rsDeleter, ok = b.resourceDeleters[rt]
	}
	if ok {
		annos, err := rsDeleter.Delete(ctx, request.GetResourceId())
		if err != nil {
			l.Error("error: delete resource failed", zap.Error(err))
			b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
			return nil, fmt.Errorf("error: delete resource failed: %w", err)
		}
		b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
		return &v2.DeleteResourceV2Response{Annotations: annos}, nil
	}
	l.Error("error: resource type does not have resource Delete() configured", zap.String("resource_type", rt))
	b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
	return nil, status.Error(codes.Unimplemented, fmt.Sprintf("resource type %s does not have resource Delete() configured", rt))
}
