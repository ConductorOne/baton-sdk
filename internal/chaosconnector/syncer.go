package chaosconnector

import (
	"context"
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/types/resource"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type resourceSyncer struct {
	run          *Run
	resourceType *v2.ResourceType
}

func (s *resourceSyncer) ResourceType(context.Context) *v2.ResourceType {
	return proto.Clone(s.resourceType).(*v2.ResourceType)
}

func (s *resourceSyncer) List(
	_ context.Context,
	_ *v2.ResourceId,
	opts resource.SyncOpAttrs,
) ([]*v2.Resource, *resource.SyncOpResults, error) {
	pages := s.run.Dataset().Resources[s.resourceType.GetId()]
	return servePage(pages, opts.PageToken.Token)
}

func (s *resourceSyncer) Entitlements(
	_ context.Context,
	target *v2.Resource,
	opts resource.SyncOpAttrs,
) ([]*v2.Entitlement, *resource.SyncOpResults, error) {
	scope := target.GetId().GetResource()
	return servePage(s.run.Dataset().Entitlements[scope], opts.PageToken.Token)
}

func (s *resourceSyncer) Grants(
	_ context.Context,
	target *v2.Resource,
	opts resource.SyncOpAttrs,
) ([]*v2.Grant, *resource.SyncOpResults, error) {
	scope := target.GetId().GetResource()
	return servePage(s.run.Dataset().Grants[scope], opts.PageToken.Token)
}

func (s *resourceSyncer) StaticEntitlements(
	_ context.Context,
	opts resource.SyncOpAttrs,
) ([]*v2.Entitlement, *resource.SyncOpResults, error) {
	return servePage(s.run.Dataset().StaticEntitlements[s.resourceType.GetId()], opts.PageToken.Token)
}

func (s *resourceSyncer) EntitlementsForResourceType(
	_ context.Context,
	resourceTypeID string,
	opts resource.SyncOpAttrs,
) ([]*v2.Entitlement, *resource.SyncOpResults, error) {
	return servePage(s.run.Dataset().Entitlements[resourceTypeID], opts.PageToken.Token)
}

func (s *resourceSyncer) GrantsForResourceType(
	_ context.Context,
	resourceTypeID string,
	opts resource.SyncOpAttrs,
) ([]*v2.Grant, *resource.SyncOpResults, error) {
	return servePage(s.run.Dataset().Grants[resourceTypeID], opts.PageToken.Token)
}

func (s *resourceSyncer) Get(
	_ context.Context,
	resourceID *v2.ResourceId,
	_ *v2.ResourceId,
) (*v2.Resource, annotations.Annotations, error) {
	for _, pages := range s.run.Dataset().Resources {
		for _, page := range pages {
			for _, item := range page.List {
				if item != nil && proto.Equal(item.GetId(), resourceID) {
					return proto.Clone(item).(*v2.Resource), nil, nil
				}
			}
		}
	}
	return nil, nil, status.Error(codes.NotFound, "chaosconnector: resource not found")
}

func servePage[T proto.Message](pages Pages[T], token string) ([]T, *resource.SyncOpResults, error) {
	if pages == nil {
		return nil, &resource.SyncOpResults{}, nil
	}
	page, ok := pages[token]
	if !ok {
		return nil, nil, status.Errorf(codes.InvalidArgument, "chaosconnector: unknown page token %q", token)
	}
	annos := annotations.Annotations(cloneMessages(page.Annotations))
	if len(page.Spawn) > 0 {
		annos.Append(v2.EnqueuePageTokens_builder{
			PageTokens: append([]string(nil), page.Spawn...),
		}.Build())
	}
	return cloneMessages(page.List), &resource.SyncOpResults{
		NextPageToken: page.Next,
		Annotations:   annos,
	}, nil
}

func firstResource(run *Run, resourceTypeID string) (*v2.Resource, error) {
	pages := run.Dataset().Resources[resourceTypeID]
	for _, page := range pages {
		for _, item := range page.List {
			if item != nil {
				return proto.Clone(item).(*v2.Resource), nil
			}
		}
	}
	return nil, fmt.Errorf("chaosconnector: resource type %q has no fixture resource", resourceTypeID)
}
