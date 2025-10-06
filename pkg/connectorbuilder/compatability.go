package connectorbuilder

import (
	"context"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	"github.com/conductorone/baton-sdk/pkg/types/resource"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type oldEventFeedWrapper struct {
	feed EventLister
}

func NewResourceSyncerV1toV2(rb ResourceSyncer) ResourceSyncer2 {
	return &resourceSyncerV1toV2{rb: rb}
}

type resourceSyncerV1toV2 struct {
	rb ResourceSyncer
}

func (rw *resourceSyncerV1toV2) ResourceType(ctx context.Context) *v2.ResourceType {
	return rw.rb.ResourceType(ctx)
}

func (rw *resourceSyncerV1toV2) List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token, opts resource.Options) ([]*v2.Resource, string, annotations.Annotations, error) {
	return rw.rb.List(ctx, parentResourceID, pToken)
}

func (rw *resourceSyncerV1toV2) Entitlements(ctx context.Context, resource *v2.Resource, pToken *pagination.Token, opts resource.Options) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	return rw.rb.Entitlements(ctx, resource, pToken)
}

func (rw *resourceSyncerV1toV2) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token, opts resource.Options) ([]*v2.Grant, string, annotations.Annotations, error) {
	return rw.rb.Grants(ctx, resource, pToken)
}

func NewResourceProvisionerV1toV2(rp ResourceProvisioner) ResourceProvisionerV2 {
	return &resourceProvisionerV1toV2{rp: rp}
}

type resourceProvisionerV1toV2 struct {
	rp ResourceProvisioner
}

func (v2 *resourceProvisionerV1toV2) ResourceType(ctx context.Context) *v2.ResourceType {
	return v2.rp.ResourceType(ctx)
}

func (v2 *resourceProvisionerV1toV2) Grant(ctx context.Context, resource *v2.Resource, entitlement *v2.Entitlement) ([]*v2.Grant, annotations.Annotations, error) {
	// Convert ResourceProvisioner Grant method (returns annotations only) to ResourceProvisionerV2 Grant method (returns grants + annotations)
	annos, err := v2.rp.Grant(ctx, resource, entitlement)
	return nil, annos, err
}

func (v2 *resourceProvisionerV1toV2) Revoke(ctx context.Context, grant *v2.Grant) (annotations.Annotations, error) {
	return v2.rp.Revoke(ctx, grant)
}

func NewResourceManagerV1toV2(rm ResourceManager) ResourceManagerV2 {
	return &resourceManagerV1toV2{rm: rm}
}

type resourceManagerV1toV2 struct {
	rm ResourceManager
}

func (v2 *resourceManagerV1toV2) Create(ctx context.Context, resource *v2.Resource) (*v2.Resource, annotations.Annotations, error) {
	return v2.rm.Create(ctx, resource)
}

func (v2 *resourceManagerV1toV2) Delete(ctx context.Context, resourceId *v2.ResourceId, parentResourceID *v2.ResourceId) (annotations.Annotations, error) {
	return v2.rm.Delete(ctx, resourceId)
}

func NewResourceDeleterV1ToV2(rd ResourceDeleter) ResourceDeleterV2 {
	return &resourceDeleterV1ToV2{rd: rd}
}

type resourceDeleterV1ToV2 struct {
	rd ResourceDeleter
}

func (v2 resourceDeleterV1ToV2) Delete(ctx context.Context, resourceId *v2.ResourceId, parentResourceID *v2.ResourceId) (annotations.Annotations, error) {
	return v2.rd.Delete(ctx, resourceId)
}

const (
	LegacyBatonFeedId = "baton_feed_event"
)

func (e *oldEventFeedWrapper) EventFeedMetadata(ctx context.Context) *v2.EventFeedMetadata {
	return &v2.EventFeedMetadata{
		Id:                  LegacyBatonFeedId,
		SupportedEventTypes: []v2.EventType{v2.EventType_EVENT_TYPE_UNSPECIFIED},
	}
}

func (e *oldEventFeedWrapper) ListEvents(
	ctx context.Context,
	earliestEvent *timestamppb.Timestamp,
	pToken *pagination.StreamToken,
) ([]*v2.Event, *pagination.StreamState, annotations.Annotations, error) {
	return e.feed.ListEvents(ctx, earliestEvent, pToken)
}
