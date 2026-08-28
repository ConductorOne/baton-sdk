package chaosconnector

import (
	"context"
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/sourcecache"
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
	ctx context.Context,
	parent *v2.ResourceId,
	opts resource.SyncOpAttrs,
) ([]*v2.Resource, *resource.SyncOpResults, error) {
	dataset := s.run.dataset()
	key := resourcePageScope(s.resourceType.GetId(), parent)
	pages, ok := dataset.Resources[key]
	if !ok {
		key = s.resourceType.GetId()
		pages = dataset.Resources[key]
	}
	token := s.run.consultSourceCache(ctx, opts.Lookup, sourcecache.RowKindResources, dataset.SourceCacheResources[key], opts.PageToken.Token)
	return servePage(pages, token)
}

func (s *resourceSyncer) Entitlements(
	ctx context.Context,
	target *v2.Resource,
	opts resource.SyncOpAttrs,
) ([]*v2.Entitlement, *resource.SyncOpResults, error) {
	scope := target.GetId().GetResource()
	dataset := s.run.dataset()
	token := s.run.consultSourceCache(ctx, opts.Lookup, sourcecache.RowKindEntitlements, dataset.SourceCacheEntitlements[scope], opts.PageToken.Token)
	return servePage(dataset.Entitlements[scope], token)
}

func (s *resourceSyncer) Grants(
	ctx context.Context,
	target *v2.Resource,
	opts resource.SyncOpAttrs,
) ([]*v2.Grant, *resource.SyncOpResults, error) {
	scope := target.GetId().GetResource()
	dataset := s.run.dataset()
	token := s.run.consultSourceCache(ctx, opts.Lookup, sourcecache.RowKindGrants, dataset.SourceCacheGrants[scope], opts.PageToken.Token)
	return servePage(dataset.Grants[scope], token)
}

func (s *resourceSyncer) StaticEntitlements(
	_ context.Context,
	opts resource.SyncOpAttrs,
) ([]*v2.Entitlement, *resource.SyncOpResults, error) {
	// Static entitlements stay unscoped (plan B3, registered exclusion):
	// no lookup consult; annotations declared on these pages flow through
	// so the ignore-with-warn contract can be exercised.
	return servePage(s.run.dataset().StaticEntitlements[s.resourceType.GetId()], opts.PageToken.Token)
}

func (s *resourceSyncer) EntitlementsForResourceType(
	ctx context.Context,
	resourceTypeID string,
	opts resource.SyncOpAttrs,
) ([]*v2.Entitlement, *resource.SyncOpResults, error) {
	dataset := s.run.dataset()
	token := s.run.consultSourceCache(ctx, opts.Lookup, sourcecache.RowKindEntitlements, dataset.SourceCacheEntitlements[resourceTypeID], opts.PageToken.Token)
	return servePage(dataset.Entitlements[resourceTypeID], token)
}

func (s *resourceSyncer) GrantsForResourceType(
	ctx context.Context,
	resourceTypeID string,
	opts resource.SyncOpAttrs,
) ([]*v2.Grant, *resource.SyncOpResults, error) {
	dataset := s.run.dataset()
	token := s.run.consultSourceCache(ctx, opts.Lookup, sourcecache.RowKindGrants, dataset.SourceCacheGrants[resourceTypeID], opts.PageToken.Token)
	return servePage(dataset.Grants[resourceTypeID], token)
}

func (s *resourceSyncer) Get(
	_ context.Context,
	resourceID *v2.ResourceId,
	_ *v2.ResourceId,
) (*v2.Resource, annotations.Annotations, error) {
	for _, pages := range s.run.dataset().Resources {
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
	pages := run.dataset().Resources[resourceTypeID]
	for _, page := range pages {
		for _, item := range page.List {
			if item != nil {
				return proto.Clone(item).(*v2.Resource), nil
			}
		}
	}
	return nil, fmt.Errorf("chaosconnector: resource type %q has no fixture resource", resourceTypeID)
}

func resourcePageScope(resourceTypeID string, parent *v2.ResourceId) string {
	if parent == nil {
		return resourceTypeID
	}
	return resourceTypeID + "\x00" + parent.GetResourceType() + "\x00" + parent.GetResource()
}
