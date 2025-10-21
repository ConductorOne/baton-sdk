package connectorbuilder

import (
	"context"
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	"github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/conductorone/baton-sdk/pkg/types/tasks"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ResourceSyncer is the primary interface for connector developers to implement.
//
// It defines the core functionality for synchronizing resources, entitlements, and grants
// from external systems into Baton. Every connector must implement at least this interface
// for each resource type it supports.
//
// Extensions to this interface include:
// - ResourceProvisioner/ResourceProvisionerV2: For adding/removing access
// - ResourceManager: For creating and managing resources
// - ResourceDeleter: For deleting resources
// - AccountManager: For account provisioning operations
// - CredentialManager: For credential rotation operations.
// - ResourceTargetedSyncer: For directly getting a resource supporting targeted sync.

type ResourceType interface {
	ResourceType(ctx context.Context) *v2.ResourceType
}

type ResourceSyncer interface {
	ResourceType
	List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error)
	Entitlements(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error)
	Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error)
}

type ResourceSyncerLimited interface {
	List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error)
	Entitlements(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error)
	Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error)
}

type ResourceSyncerV2 interface {
	ResourceType
	ResourceSyncerV2Limited
}

type ResourceSyncerV2Limited interface {
	List(ctx context.Context, parentResourceID *v2.ResourceId, opts resource.Options) ([]*v2.Resource, *resource.OptionsRet, error)
	Entitlements(ctx context.Context, resource *v2.Resource, opts resource.Options) ([]*v2.Entitlement, *resource.OptionsRet, error)
	Grants(ctx context.Context, resource *v2.Resource, opts resource.Options) ([]*v2.Grant, *resource.OptionsRet, error)
}

// ResourceTargetedSyncer extends ResourceSyncer to add capabilities for directly syncing an individual resource
//
// Implementing this interface indicates the connector supports calling "get" on a resource
// of the associated resource type.
type ResourceTargetedSyncer interface {
	ResourceSyncer
	ResourceTargetedSyncerLimited
}

type ResourceTargetedSyncerLimited interface {
	Get(ctx context.Context, resourceId *v2.ResourceId, parentResourceId *v2.ResourceId) (*v2.Resource, annotations.Annotations, error)
}

// ListResourceTypes lists all available resource types.
func (b *builder) ListResourceTypes(
	ctx context.Context,
	request *v2.ResourceTypesServiceListResourceTypesRequest,
) (*v2.ResourceTypesServiceListResourceTypesResponse, error) {
	ctx, span := tracer.Start(ctx, "builder.ListResourceTypes")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.ListResourceTypesType
	var out []*v2.ResourceType

	if len(b.resourceSyncers) == 0 {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: no resource builders found")
	}

	for _, rb := range b.resourceSyncers {
		out = append(out, rb.ResourceType(ctx))
	}

	if len(out) == 0 {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: no resource types found")
	}

	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return &v2.ResourceTypesServiceListResourceTypesResponse{List: out}, nil
}

// ListResources returns all available resources for a given resource type ID.
func (b *builder) ListResources(ctx context.Context, request *v2.ResourcesServiceListResourcesRequest) (*v2.ResourcesServiceListResourcesResponse, error) {
	ctx, span := tracer.Start(ctx, "builder.ListResources")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.ListResourcesType
	rb, ok := b.resourceSyncers[request.ResourceTypeId]
	if !ok {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: list resources with unknown resource type %s", request.ResourceTypeId)
	}

	token := &pagination.Token{
		Size:  int(request.PageSize),
		Token: request.PageToken,
	}
	opts := resource.Options{SyncID: request.ActiveSyncId, PageToken: token, Session: WithSyncId(b.sessionStore, request.ActiveSyncId)}
	out, retOptions, err := rb.List(ctx, request.ParentResourceId, opts)
	if retOptions == nil {
		retOptions = &resource.OptionsRet{}
	}

	resp := &v2.ResourcesServiceListResourcesResponse{
		List:          out,
		NextPageToken: retOptions.NextPageToken,
		Annotations:   retOptions.Annotations,
	}
	if err != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return resp, fmt.Errorf("error: listing resources failed: %w", err)
	}
	if request.PageToken != "" && request.PageToken == retOptions.NextPageToken {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return resp, fmt.Errorf("error: listing resources failed: next page token is the same as the current page token. this is most likely a connector bug")
	}

	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return resp, nil
}

func (b *builder) GetResource(ctx context.Context, request *v2.ResourceGetterServiceGetResourceRequest) (*v2.ResourceGetterServiceGetResourceResponse, error) {
	ctx, span := tracer.Start(ctx, "builder.GetResource")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.GetResourceType
	resourceType := request.GetResourceId().GetResourceType()
	rb, ok := b.resourceTargetedSyncers[resourceType]
	if !ok {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, status.Errorf(codes.Unimplemented, "error: get resource with unknown resource type %s", resourceType)
	}
	resource, annos, err := rb.Get(ctx, request.GetResourceId(), request.GetParentResourceId())
	if err != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: get resource failed: %w", err)
	}
	if resource == nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, status.Error(codes.NotFound, "error: get resource returned nil")
	}

	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return &v2.ResourceGetterServiceGetResourceResponse{
		Resource:    resource,
		Annotations: annos,
	}, nil
}

// ListEntitlements returns all the entitlements for a given resource.
func (b *builder) ListEntitlements(ctx context.Context, request *v2.EntitlementsServiceListEntitlementsRequest) (*v2.EntitlementsServiceListEntitlementsResponse, error) {
	ctx, span := tracer.Start(ctx, "builder.ListEntitlements")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.ListEntitlementsType
	rb, ok := b.resourceSyncers[request.Resource.Id.ResourceType]
	if !ok {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: list entitlements with unknown resource type %s", request.Resource.Id.ResourceType)
	}
	token := &pagination.Token{
		Size:  int(request.PageSize),
		Token: request.PageToken,
	}
	opts := resource.Options{SyncID: request.ActiveSyncId, PageToken: token, Session: WithSyncId(b.sessionStore, request.ActiveSyncId)}
	out, retOptions, err := rb.Entitlements(ctx, request.Resource, opts)
	if retOptions == nil {
		retOptions = &resource.OptionsRet{}
	}

	resp := &v2.EntitlementsServiceListEntitlementsResponse{
		List:          out,
		NextPageToken: retOptions.NextPageToken,
		Annotations:   retOptions.Annotations,
	}
	if err != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return resp, fmt.Errorf("error: listing entitlements failed: %w", err)
	}
	if request.PageToken != "" && request.PageToken == retOptions.NextPageToken {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return resp, fmt.Errorf("error: listing entitlements failed: next page token is the same as the current page token. this is most likely a connector bug")
	}

	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return resp, nil
}

// ListGrants lists all the grants for a given resource.
func (b *builder) ListGrants(ctx context.Context, request *v2.GrantsServiceListGrantsRequest) (*v2.GrantsServiceListGrantsResponse, error) {
	ctx, span := tracer.Start(ctx, "builder.ListGrants")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.ListGrantsType
	rid := request.Resource.Id
	rb, ok := b.resourceSyncers[rid.ResourceType]
	if !ok {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: list grants with unknown resource type %s", rid.ResourceType)
	}

	token := &pagination.Token{
		Size:  int(request.PageSize),
		Token: request.PageToken,
	}
	opts := resource.Options{SyncID: request.ActiveSyncId, PageToken: token, Session: WithSyncId(b.sessionStore, request.ActiveSyncId)}
	out, retOptions, err := rb.Grants(ctx, request.Resource, opts)
	if retOptions == nil {
		retOptions = &resource.OptionsRet{}
	}

	resp := &v2.GrantsServiceListGrantsResponse{
		List:          out,
		Annotations:   retOptions.Annotations,
		NextPageToken: retOptions.NextPageToken,
	}

	if err != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return resp, fmt.Errorf("error: listing grants for resource %s/%s failed: %w", rid.ResourceType, rid.Resource, err)
	}
	if request.PageToken != "" && request.PageToken == retOptions.NextPageToken {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return resp, fmt.Errorf("error: listing grants for resource %s/%s failed: next page token is the same as the current page token. this is most likely a connector bug",
			rid.ResourceType,
			rid.Resource)
	}

	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return resp, nil
}

func newResourceSyncerV1toV2(rb ResourceSyncer) ResourceSyncerV2 {
	return &resourceSyncerV1toV2{rb: rb}
}

type resourceSyncerV1toV2 struct {
	rb ResourceSyncer
}

func (rw *resourceSyncerV1toV2) ResourceType(ctx context.Context) *v2.ResourceType {
	return rw.rb.ResourceType(ctx)
}

func (rw *resourceSyncerV1toV2) List(ctx context.Context, parentResourceID *v2.ResourceId, opts resource.Options) ([]*v2.Resource, *resource.OptionsRet, error) {
	resources, pageToken, annos, err := rw.rb.List(ctx, parentResourceID, opts.PageToken)
	ret := &resource.OptionsRet{NextPageToken: pageToken, Annotations: annos}
	return resources, ret, err
}

func (rw *resourceSyncerV1toV2) Entitlements(ctx context.Context, r *v2.Resource, opts resource.Options) ([]*v2.Entitlement, *resource.OptionsRet, error) {
	ents, pageToken, annos, err := rw.rb.Entitlements(ctx, r, opts.PageToken)
	ret := &resource.OptionsRet{NextPageToken: pageToken, Annotations: annos}
	return ents, ret, err
}

func (rw *resourceSyncerV1toV2) Grants(ctx context.Context, r *v2.Resource, opts resource.Options) ([]*v2.Grant, *resource.OptionsRet, error) {
	grants, pageToken, annos, err := rw.rb.Grants(ctx, r, opts.PageToken)
	ret := &resource.OptionsRet{NextPageToken: pageToken, Annotations: annos}
	return grants, ret, err
}

func (b *builder) addTargetedSyncer(_ context.Context, typeId string, in interface{}) error {
	if targetedSyncer, ok := in.(ResourceTargetedSyncerLimited); ok {
		if _, ok := b.resourceTargetedSyncers[typeId]; ok {
			return fmt.Errorf("error: duplicate resource type found for resource targeted syncer %s", typeId)
		}
		b.resourceTargetedSyncers[typeId] = targetedSyncer
	}
	return nil
}

func (b *builder) addResourceSyncers(_ context.Context, typeId string, in interface{}) error {
	// no duplicates
	if _, ok := b.resourceSyncers[typeId]; ok {
		return fmt.Errorf("error: duplicate resource type found for resource builder %s", typeId)
	}

	if rb, ok := in.(ResourceSyncer); ok {
		b.resourceSyncers[typeId] = newResourceSyncerV1toV2(rb)
	}

	if rb, ok := in.(ResourceSyncerV2); ok {
		b.resourceSyncers[typeId] = rb
	}

	// A resource syncer is required
	if _, ok := b.resourceSyncers[typeId]; !ok {
		return fmt.Errorf("error: the resource syncer interface must be implemented for all types (%s)", typeId)
	}

	return nil
}
