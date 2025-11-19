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

type StaticEntitlementSyncer interface {
	StaticEntitlements(ctx context.Context, pToken *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error)
}

type ResourceSyncerV2 interface {
	ResourceType
	ResourceSyncerV2Limited
}

type ResourceSyncerV2Limited interface {
	List(ctx context.Context, parentResourceID *v2.ResourceId, opts resource.SyncOpAttrs) ([]*v2.Resource, *resource.SyncOpResults, error)
	Entitlements(ctx context.Context, resource *v2.Resource, opts resource.SyncOpAttrs) ([]*v2.Entitlement, *resource.SyncOpResults, error)
	Grants(ctx context.Context, resource *v2.Resource, opts resource.SyncOpAttrs) ([]*v2.Grant, *resource.SyncOpResults, error)
}

type StaticEntitlementSyncerV2 interface {
	StaticEntitlements(ctx context.Context, opts resource.SyncOpAttrs) ([]*v2.Entitlement, *resource.SyncOpResults, error)
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
	return v2.ResourceTypesServiceListResourceTypesResponse_builder{List: out}.Build(), nil
}

// ListResources returns all available resources for a given resource type ID.
func (b *builder) ListResources(ctx context.Context, request *v2.ResourcesServiceListResourcesRequest) (*v2.ResourcesServiceListResourcesResponse, error) {
	ctx, span := tracer.Start(ctx, "builder.ListResources")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.ListResourcesType
	rb, ok := b.resourceSyncers[request.GetResourceTypeId()]
	if !ok {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: list resources with unknown resource type %s", request.GetResourceTypeId())
	}

	token := pagination.Token{
		Size:  int(request.GetPageSize()),
		Token: request.GetPageToken(),
	}
	opts := resource.SyncOpAttrs{SyncID: request.GetActiveSyncId(), PageToken: token, Session: WithSyncId(b.sessionStore, request.GetActiveSyncId())}
	out, retOptions, err := rb.List(ctx, request.GetParentResourceId(), opts)
	if retOptions == nil {
		retOptions = &resource.SyncOpResults{}
	}

	resp := v2.ResourcesServiceListResourcesResponse_builder{
		List:          out,
		NextPageToken: retOptions.NextPageToken,
		Annotations:   retOptions.Annotations,
	}.Build()
	if err != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return resp, fmt.Errorf("error: listing resources failed: %w", err)
	}
	if request.GetPageToken() != "" && request.GetPageToken() == retOptions.NextPageToken {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		errMsg := fmt.Sprintf(" with page token %s resource type id %s and resource parent id: %s this is most likely a connector bug",
			request.GetPageToken(), request.GetResourceTypeId(), request.GetParentResourceId())
		return resp, fmt.Errorf("error: listing resources failed: next page token is the same as the current page token %s", errMsg)
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
	return v2.ResourceGetterServiceGetResourceResponse_builder{
		Resource:    resource,
		Annotations: annos,
	}.Build(), nil
}

// ListStaticEntitlements returns all the static entitlements for a given resource type.
// Static entitlements are used to create entitlements for all resources of a given resource type.
func (b *builder) ListStaticEntitlements(ctx context.Context, request *v2.EntitlementsServiceListStaticEntitlementsRequest) (*v2.EntitlementsServiceListStaticEntitlementsResponse, error) {
	ctx, span := tracer.Start(ctx, "builder.ListStaticEntitlements")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.ListStaticEntitlementsType
	rb, ok := b.resourceSyncers[request.GetResourceTypeId()]
	if !ok {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: list static entitlements with unknown resource type %s", request.GetResourceTypeId())
	}
	rbse, ok := rb.(StaticEntitlementSyncerV2)
	if !ok {
		// Resource syncer doesn't support static entitlements. Return empty response.
		return v2.EntitlementsServiceListStaticEntitlementsResponse_builder{
			List:          []*v2.Entitlement{},
			NextPageToken: "",
			Annotations:   nil,
		}.Build(), nil
	}

	token := pagination.Token{
		Size:  int(request.GetPageSize()),
		Token: request.GetPageToken(),
	}
	opts := resource.SyncOpAttrs{SyncID: request.GetActiveSyncId(), PageToken: token, Session: WithSyncId(b.sessionStore, request.GetActiveSyncId())}
	out, retOptions, err := rbse.StaticEntitlements(ctx, opts)
	if retOptions == nil {
		retOptions = &resource.SyncOpResults{}
	}

	resp := v2.EntitlementsServiceListStaticEntitlementsResponse_builder{
		List:          out,
		NextPageToken: retOptions.NextPageToken,
		Annotations:   retOptions.Annotations,
	}.Build()
	if err != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: listing static entitlements failed: %w", err)
	}
	if request.GetPageToken() != "" && request.GetPageToken() == retOptions.NextPageToken {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return resp, fmt.Errorf("error: listing static entitlements failed: next page token is the same as the current page token. this is most likely a connector bug")
	}

	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return resp, nil
}

// ListEntitlements returns all the entitlements for a given resource.
func (b *builder) ListEntitlements(ctx context.Context, request *v2.EntitlementsServiceListEntitlementsRequest) (*v2.EntitlementsServiceListEntitlementsResponse, error) {
	ctx, span := tracer.Start(ctx, "builder.ListEntitlements")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.ListEntitlementsType
	rb, ok := b.resourceSyncers[request.GetResource().GetId().GetResourceType()]
	if !ok {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: list entitlements with unknown resource type %s", request.GetResource().GetId().GetResourceType())
	}
	token := pagination.Token{
		Size:  int(request.GetPageSize()),
		Token: request.GetPageToken(),
	}
	opts := resource.SyncOpAttrs{SyncID: request.GetActiveSyncId(), PageToken: token, Session: WithSyncId(b.sessionStore, request.GetActiveSyncId())}
	out, retOptions, err := rb.Entitlements(ctx, request.GetResource(), opts)
	if retOptions == nil {
		retOptions = &resource.SyncOpResults{}
	}

	resp := v2.EntitlementsServiceListEntitlementsResponse_builder{
		List:          out,
		NextPageToken: retOptions.NextPageToken,
		Annotations:   retOptions.Annotations,
	}.Build()
	if err != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return resp, fmt.Errorf("error: listing entitlements failed: %w", err)
	}
	if request.GetPageToken() != "" && request.GetPageToken() == retOptions.NextPageToken {
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
	rid := request.GetResource().GetId()
	rb, ok := b.resourceSyncers[rid.GetResourceType()]
	if !ok {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: list grants with unknown resource type %s", rid.GetResourceType())
	}

	token := pagination.Token{
		Size:  int(request.GetPageSize()),
		Token: request.GetPageToken(),
	}
	opts := resource.SyncOpAttrs{SyncID: request.GetActiveSyncId(), PageToken: token, Session: WithSyncId(b.sessionStore, request.GetActiveSyncId())}
	out, retOptions, err := rb.Grants(ctx, request.GetResource(), opts)
	if retOptions == nil {
		retOptions = &resource.SyncOpResults{}
	}

	resp := v2.GrantsServiceListGrantsResponse_builder{
		List:          out,
		Annotations:   retOptions.Annotations,
		NextPageToken: retOptions.NextPageToken,
	}.Build()

	if err != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return resp, fmt.Errorf("error: listing grants for resource %s/%s failed: %w", rid.GetResourceType(), rid.GetResource(), err)
	}
	if request.GetPageToken() != "" && request.GetPageToken() == retOptions.NextPageToken {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return resp, fmt.Errorf("error: listing grants for resource %s/%s failed: next page token is the same as the current page token. this is most likely a connector bug",
			rid.GetResourceType(),
			rid.GetResource())
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

var _ ResourceSyncerV2 = &resourceSyncerV1toV2{}
var _ StaticEntitlementSyncerV2 = &resourceSyncerV1toV2{}

func (rw *resourceSyncerV1toV2) ResourceType(ctx context.Context) *v2.ResourceType {
	return rw.rb.ResourceType(ctx)
}

func (rw *resourceSyncerV1toV2) List(ctx context.Context, parentResourceID *v2.ResourceId, opts resource.SyncOpAttrs) ([]*v2.Resource, *resource.SyncOpResults, error) {
	resources, pageToken, annos, err := rw.rb.List(ctx, parentResourceID, &opts.PageToken)
	ret := &resource.SyncOpResults{NextPageToken: pageToken, Annotations: annos}
	return resources, ret, err
}

func (rw *resourceSyncerV1toV2) Entitlements(ctx context.Context, r *v2.Resource, opts resource.SyncOpAttrs) ([]*v2.Entitlement, *resource.SyncOpResults, error) {
	ents, pageToken, annos, err := rw.rb.Entitlements(ctx, r, &opts.PageToken)
	ret := &resource.SyncOpResults{NextPageToken: pageToken, Annotations: annos}
	return ents, ret, err
}

func (rw *resourceSyncerV1toV2) StaticEntitlements(ctx context.Context, opts resource.SyncOpAttrs) ([]*v2.Entitlement, *resource.SyncOpResults, error) {
	rb, ok := rw.rb.(StaticEntitlementSyncer)
	if !ok {
		return nil, &resource.SyncOpResults{NextPageToken: "", Annotations: annotations.Annotations{}}, nil
	}

	ents, pageToken, annos, err := rb.StaticEntitlements(ctx, &opts.PageToken)
	ret := &resource.SyncOpResults{NextPageToken: pageToken, Annotations: annos}
	return ents, ret, err
}

func (rw *resourceSyncerV1toV2) Grants(ctx context.Context, r *v2.Resource, opts resource.SyncOpAttrs) ([]*v2.Grant, *resource.SyncOpResults, error) {
	grants, pageToken, annos, err := rw.rb.Grants(ctx, r, &opts.PageToken)
	ret := &resource.SyncOpResults{NextPageToken: pageToken, Annotations: annos}
	return grants, ret, err
}

func (b *builder) addTargetedSyncer(_ context.Context, typeId string, in any) error {
	if targetedSyncer, ok := in.(ResourceTargetedSyncerLimited); ok {
		if _, ok := b.resourceTargetedSyncers[typeId]; ok {
			return fmt.Errorf("error: duplicate resource type found for resource targeted syncer %s", typeId)
		}
		b.resourceTargetedSyncers[typeId] = targetedSyncer
	}
	return nil
}

func (b *builder) addResourceSyncers(_ context.Context, typeId string, in any) error {
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
