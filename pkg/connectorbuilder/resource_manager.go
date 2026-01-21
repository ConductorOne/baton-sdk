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
	ResourceManagerLimited
}

type ResourceManagerLimited interface {
	ResourceCreator
	ResourceDeleterLimited
}

type ResourceManagerV2Limited interface {
	ResourceCreator
	ResourceDeleterV2Limited
}

// ResourceManagerV2 extends ResourceSyncer to add capabilities for creating resources.
//
// This is the recommended interface for implementing resource creation operations in new connectors.
type ResourceManagerV2 interface {
	ResourceSyncer
	ResourceManagerV2Limited
}

type ResourceCreator interface {
	Create(ctx context.Context, resource *v2.Resource) (*v2.Resource, annotations.Annotations, error)
}

// ResourceDeleter extends ResourceSyncer to add capabilities for deleting resources.
//
// Implementing this interface indicates the connector supports deleting resources
// of the associated resource type.
type ResourceDeleter interface {
	ResourceSyncer
	ResourceDeleterLimited
}
type ResourceDeleterLimited interface {
	Delete(ctx context.Context, resourceId *v2.ResourceId) (annotations.Annotations, error)
}

// ResourceDeleterV2 extends ResourceSyncer to add capabilities for deleting resources.
//
// This is the recommended interface for implementing resource deletion operations in new connectors.
// It differs from ResourceDeleter by having the resource, not just the id.
type ResourceDeleterV2 interface {
	ResourceSyncer
	ResourceDeleterV2Limited
}

type ResourceDeleterV2Limited interface {
	Delete(ctx context.Context, resourceId *v2.ResourceId, parentResourceID *v2.ResourceId) (annotations.Annotations, error)
}

func (b *builder) CreateResource(ctx context.Context, request *v2.CreateResourceRequest) (*v2.CreateResourceResponse, error) {
	ctx, span := tracer.Start(ctx, "builder.CreateResource")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.CreateResourceType
	l := ctxzap.Extract(ctx)
	rt := request.GetResource().GetId().GetResourceType()

	manager, ok := b.resourceManagers[rt]
	if !ok {
		l.Error("error: resource type does not have resource Create() configured", zap.String("resource_type", rt))
		err := status.Error(codes.Unimplemented, fmt.Sprintf("resource type %s does not have resource Create() configured", rt))
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, err
	}
	resource, annos, err := manager.Create(ctx, request.GetResource())
	if err != nil {
		l.Error("error: create resource failed", zap.Error(err))
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, fmt.Errorf("error: create resource failed: %w", err)
	}
	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return v2.CreateResourceResponse_builder{Created: resource, Annotations: annos}.Build(), nil
}

func (b *builder) DeleteResource(ctx context.Context, request *v2.DeleteResourceRequest) (*v2.DeleteResourceResponse, error) {
	ctx, span := tracer.Start(ctx, "builder.DeleteResource")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.DeleteResourceType

	l := ctxzap.Extract(ctx)
	rt := request.GetResourceId().GetResourceType()
	var rsDeleter ResourceDeleterV2Limited
	var ok bool

	rsDeleter, ok = b.resourceManagers[rt]
	if !ok {
		rsDeleter, ok = b.resourceDeleters[rt]
	}

	if !ok {
		l.Error("error: resource type does not have resource Delete() configured", zap.String("resource_type", rt))
		err := status.Error(codes.Unimplemented, fmt.Sprintf("resource type %s does not have resource Delete() configured", rt))
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, err
	}

	annos, err := rsDeleter.Delete(ctx, request.GetResourceId(), request.GetParentResourceId())
	if err != nil {
		l.Error("error: deleteV2 resource failed", zap.Error(err))
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, fmt.Errorf("error: delete resource failed: %w", err)
	}
	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return v2.DeleteResourceResponse_builder{Annotations: annos}.Build(), nil
}

func (b *builder) DeleteResourceV2(ctx context.Context, request *v2.DeleteResourceV2Request) (*v2.DeleteResourceV2Response, error) {
	ctx, span := tracer.Start(ctx, "builder.DeleteResourceV2")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.DeleteResourceType

	l := ctxzap.Extract(ctx)
	rt := request.GetResourceId().GetResourceType()
	var rsDeleter ResourceDeleterV2Limited
	var ok bool

	rsDeleter, ok = b.resourceManagers[rt]
	if !ok {
		rsDeleter, ok = b.resourceDeleters[rt]
	}

	if !ok {
		l.Error("error: resource type does not have resource Delete() configured", zap.String("resource_type", rt))
		err := status.Error(codes.Unimplemented, fmt.Sprintf("resource type %s does not have resource Delete() configured", rt))
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, err
	}

	annos, err := rsDeleter.Delete(ctx, request.GetResourceId(), request.GetParentResourceId())
	if err != nil {
		l.Error("error: deleteV2 resource failed", zap.Error(err))
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, fmt.Errorf("error: delete resource failed: %w", err)
	}
	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return v2.DeleteResourceV2Response_builder{Annotations: annos}.Build(), nil
}

func newResourceManager1to2(resourceManager ResourceManagerLimited) ResourceManagerV2Limited {
	return &resourceManager1to2{resourceManager}
}

type resourceManager1to2 struct {
	ResourceManagerLimited
}

func (r *resourceManager1to2) Delete(ctx context.Context, resourceId *v2.ResourceId, parentResourceID *v2.ResourceId) (annotations.Annotations, error) {
	return r.ResourceManagerLimited.Delete(ctx, resourceId)
}

func newDeleter1to2(resourceDeleter ResourceDeleterLimited) ResourceDeleterV2Limited {
	return &deleter1to2{resourceDeleter}
}

type deleter1to2 struct {
	ResourceDeleterLimited
}

func (d *deleter1to2) Delete(ctx context.Context, resourceId *v2.ResourceId, parentResourceID *v2.ResourceId) (annotations.Annotations, error) {
	// Just drop the parentResourceID...
	return d.ResourceDeleterLimited.Delete(ctx, resourceId)
}

func (b *builder) addResourceManager(_ context.Context, typeId string, in interface{}) error {
	if resourceManager, ok := in.(ResourceManagerLimited); ok {
		if _, ok := b.resourceManagers[typeId]; ok {
			return fmt.Errorf("error: duplicate resource type found for resource manager %s", typeId)
		}
		b.resourceManagers[typeId] = newResourceManager1to2(resourceManager)
		// Support DeleteResourceV2 if connector implements both Create and Delete
		if _, ok := b.resourceDeleters[typeId]; ok {
			// This should never happen
			return fmt.Errorf("error: duplicate resource type found for resource deleter %s", typeId)
		}
		b.resourceDeleters[typeId] = newDeleter1to2(resourceManager)
	} else {
		if resourceDeleter, ok := in.(ResourceDeleterLimited); ok {
			if _, ok := b.resourceDeleters[typeId]; ok {
				return fmt.Errorf("error: duplicate resource type found for resource deleter %s", typeId)
			}
			b.resourceDeleters[typeId] = newDeleter1to2(resourceDeleter)
		}
	}

	if resourceManager, ok := in.(ResourceManagerV2Limited); ok {
		if _, ok := b.resourceManagers[typeId]; ok {
			return fmt.Errorf("error: duplicate resource type found for resource managerV2 %s", typeId)
		}
		b.resourceManagers[typeId] = resourceManager
		// Support DeleteResourceV2 if connector implements both Create and Delete
		if _, ok := b.resourceDeleters[typeId]; ok {
			// This should never happen
			return fmt.Errorf("error: duplicate resource type found for resource deleterV2 %s", typeId)
		}
		b.resourceDeleters[typeId] = resourceManager
	} else {
		if resourceDeleter, ok := in.(ResourceDeleterV2Limited); ok {
			if _, ok := b.resourceDeleters[typeId]; ok {
				return fmt.Errorf("error: duplicate resource type found for resource deleterV2 %s", typeId)
			}
			b.resourceDeleters[typeId] = resourceDeleter
		}
	}
	return nil
}
