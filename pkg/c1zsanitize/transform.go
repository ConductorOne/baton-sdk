package c1zsanitize

import (
	"context"
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

const listPageSize = 1000

func (s *sanitizer) copyResourceTypes(
	ctx context.Context,
	src connectorstore.Reader,
	dst connectorstore.Writer,
	srcSyncID string,
	refs *assetRefSet,
) error {
	pageToken := ""
	for {
		req := v2.ResourceTypesServiceListResourceTypesRequest_builder{
			PageSize:    listPageSize,
			PageToken:   pageToken,
			Annotations: syncIDAnnotations(srcSyncID),
		}.Build()
		resp, err := src.ListResourceTypes(ctx, req)
		if err != nil {
			return fmt.Errorf("list resource types: %w", err)
		}
		out := make([]*v2.ResourceType, 0, len(resp.GetList()))
		for _, rt := range resp.GetList() {
			out = append(out, s.transformResourceType(rt, refs))
		}
		if len(out) > 0 {
			if err := dst.PutResourceTypes(ctx, out...); err != nil {
				return fmt.Errorf("put resource types: %w", err)
			}
		}
		if resp.GetNextPageToken() == "" {
			return nil
		}
		pageToken = resp.GetNextPageToken()
	}
}

func (s *sanitizer) copyResources(
	ctx context.Context,
	src connectorstore.Reader,
	dst connectorstore.Writer,
	srcSyncID string,
	refs *assetRefSet,
) error {
	pageToken := ""
	for {
		req := v2.ResourcesServiceListResourcesRequest_builder{
			PageSize:    listPageSize,
			PageToken:   pageToken,
			Annotations: syncIDAnnotations(srcSyncID),
		}.Build()
		resp, err := src.ListResources(ctx, req)
		if err != nil {
			return fmt.Errorf("list resources: %w", err)
		}
		out := make([]*v2.Resource, 0, len(resp.GetList()))
		for _, r := range resp.GetList() {
			out = append(out, s.transformResource(r, refs))
		}
		if len(out) > 0 {
			if err := dst.PutResources(ctx, out...); err != nil {
				return fmt.Errorf("put resources: %w", err)
			}
		}
		if resp.GetNextPageToken() == "" {
			return nil
		}
		pageToken = resp.GetNextPageToken()
	}
}

func (s *sanitizer) copyEntitlements(
	ctx context.Context,
	src connectorstore.Reader,
	dst connectorstore.Writer,
	srcSyncID string,
	refs *assetRefSet,
) error {
	pageToken := ""
	for {
		req := v2.EntitlementsServiceListEntitlementsRequest_builder{
			PageSize:    listPageSize,
			PageToken:   pageToken,
			Annotations: syncIDAnnotations(srcSyncID),
		}.Build()
		resp, err := src.ListEntitlements(ctx, req)
		if err != nil {
			return fmt.Errorf("list entitlements: %w", err)
		}
		out := make([]*v2.Entitlement, 0, len(resp.GetList()))
		for _, e := range resp.GetList() {
			out = append(out, s.transformEntitlement(e, refs))
		}
		if len(out) > 0 {
			if err := dst.PutEntitlements(ctx, out...); err != nil {
				return fmt.Errorf("put entitlements: %w", err)
			}
		}
		if resp.GetNextPageToken() == "" {
			return nil
		}
		pageToken = resp.GetNextPageToken()
	}
}

func (s *sanitizer) copyGrants(
	ctx context.Context,
	src connectorstore.Reader,
	dst connectorstore.Writer,
	srcSyncID string,
	refs *assetRefSet,
) error {
	pageToken := ""
	for {
		req := v2.GrantsServiceListGrantsRequest_builder{
			PageSize:    listPageSize,
			PageToken:   pageToken,
			Annotations: syncIDAnnotations(srcSyncID),
		}.Build()
		resp, err := src.ListGrants(ctx, req)
		if err != nil {
			return fmt.Errorf("list grants: %w", err)
		}
		out := make([]*v2.Grant, 0, len(resp.GetList()))
		for _, g := range resp.GetList() {
			out = append(out, s.transformGrant(g, refs))
		}
		if len(out) > 0 {
			if err := dst.PutGrants(ctx, out...); err != nil {
				return fmt.Errorf("put grants: %w", err)
			}
		}
		if resp.GetNextPageToken() == "" {
			return nil
		}
		pageToken = resp.GetNextPageToken()
	}
}

// transformResourceType preserves the resource type's id and trait
// enum but rewrites display name, description, and annotations.
// resource_type.id is connector-defined (e.g. "user") and treated as
// non-tenant; see §7 question 1 in the investigation for the caveat.
func (s *sanitizer) transformResourceType(in *v2.ResourceType, refs *assetRefSet) *v2.ResourceType {
	if in == nil {
		return nil
	}
	annos := s.transformAnnotations(in.GetAnnotations(), refs)
	return v2.ResourceType_builder{
		Id:                in.GetId(),
		DisplayName:       s.id(in.GetDisplayName()),
		Description:       s.id(in.GetDescription()),
		Traits:            in.GetTraits(),
		Annotations:       annos,
		SourcedExternally: in.GetSourcedExternally(),
	}.Build()
}

// transformResource rewrites the resource id (preserving the type
// portion), parent id, display name, description, and annotations.
// baton_resource is preserved as it's a flag, not identity.
func (s *sanitizer) transformResource(in *v2.Resource, refs *assetRefSet) *v2.Resource {
	if in == nil {
		return nil
	}
	annos := s.transformAnnotations(in.GetAnnotations(), refs)
	return v2.Resource_builder{
		Id:               s.transformResourceID(in.GetId()),
		ParentResourceId: s.transformResourceID(in.GetParentResourceId()),
		DisplayName:      s.id(in.GetDisplayName()),
		Description:      s.id(in.GetDescription()),
		Annotations:      annos,
		BatonResource:    in.GetBatonResource(),
	}.Build()
}

func (s *sanitizer) transformResourceID(in *v2.ResourceId) *v2.ResourceId {
	if in == nil {
		return nil
	}
	if in.GetResource() == "" && in.GetResourceType() == "" {
		return nil
	}
	return v2.ResourceId_builder{
		ResourceType:  in.GetResourceType(),
		Resource:      s.id(in.GetResource()),
		BatonResource: in.GetBatonResource(),
	}.Build()
}

func (s *sanitizer) transformEntitlement(in *v2.Entitlement, refs *assetRefSet) *v2.Entitlement {
	if in == nil {
		return nil
	}
	annos := s.transformAnnotations(in.GetAnnotations(), refs)
	out := v2.Entitlement_builder{
		Id:          s.id(in.GetId()),
		Resource:    s.transformResource(in.GetResource(), refs),
		DisplayName: s.id(in.GetDisplayName()),
		Description: s.id(in.GetDescription()),
		GrantableTo: s.transformResourceTypeSlice(in.GetGrantableTo(), refs),
		Purpose:     in.GetPurpose(),
		Slug:        s.id(in.GetSlug()),
		Annotations: annos,
	}.Build()
	return out
}

func (s *sanitizer) transformResourceTypeSlice(in []*v2.ResourceType, refs *assetRefSet) []*v2.ResourceType {
	if len(in) == 0 {
		return nil
	}
	out := make([]*v2.ResourceType, 0, len(in))
	for _, rt := range in {
		out = append(out, s.transformResourceType(rt, refs))
	}
	return out
}

func (s *sanitizer) transformGrant(in *v2.Grant, refs *assetRefSet) *v2.Grant {
	if in == nil {
		return nil
	}
	annos := s.transformAnnotations(in.GetAnnotations(), refs)
	return v2.Grant_builder{
		Id:          s.id(in.GetId()),
		Entitlement: s.transformEntitlement(in.GetEntitlement(), refs),
		Principal:   s.transformResource(in.GetPrincipal(), refs),
		Sources:     s.transformGrantSources(in.GetSources()),
		Annotations: annos,
	}.Build()
}

// transformGrantSources rebuilds the sources map with sanitized keys.
// Map keys are source entitlement IDs; values carry only is_direct.
func (s *sanitizer) transformGrantSources(in *v2.GrantSources) *v2.GrantSources {
	if in == nil {
		return nil
	}
	srcMap := in.GetSources()
	if len(srcMap) == 0 {
		return v2.GrantSources_builder{}.Build()
	}
	out := make(map[string]*v2.GrantSources_GrantSource, len(srcMap))
	for srcEntitlementID, gs := range srcMap {
		out[s.id(srcEntitlementID)] = v2.GrantSources_GrantSource_builder{
			IsDirect: gs.GetIsDirect(),
		}.Build()
	}
	return v2.GrantSources_builder{Sources: out}.Build()
}
