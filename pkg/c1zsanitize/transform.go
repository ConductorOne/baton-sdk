package c1zsanitize

import (
	"context"
	"fmt"
	"strings"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

const listPageSize = 1000

// entitlementID transforms a baton-canonical entitlement ID of the
// form "resourceType:resourceID:permission" component-wise: the
// connector-defined resourceType is preserved (the same way resource
// IDs keep their type), while resourceID and permission are HMAC'd.
// This makes the resourceID embedded in an entitlement ID equal the
// separately-sanitized resource row's id, so references stay coherent.
//
// SplitN with a limit of 3 keeps a permission that itself contains ':'
// intact. Connectors are free to emit non-canonical entitlement IDs;
// anything without the three expected fields is not decomposable, so
// it is HMAC'd whole — preserving the invariant that equal source IDs
// map to equal sanitized IDs everywhere they appear.
func (s *sanitizer) entitlementID(id string) string {
	if id == "" {
		return ""
	}
	parts := strings.SplitN(id, ":", 3)
	if len(parts) != 3 {
		return s.id(id)
	}
	return parts[0] + ":" + s.id(parts[1]) + ":" + s.id(parts[2])
}

// grantID transforms a baton-canonical grant ID of the form
// "entitlementID:principalType:principalID". The principal type and id
// are the final two ':' fields; everything before them is the
// entitlement ID, which is itself composite and may contain ':' in its
// permission tail, so the split walks in from the right. The
// entitlement portion is transformed with entitlementID, the principal
// type is preserved, and the principal id is HMAC'd — so a sanitized
// grant's embedded entitlement and principal references both match the
// separately-sanitized entitlement and resource rows. A grant ID
// lacking the two trailing fields is not decomposable and is HMAC'd
// whole.
func (s *sanitizer) grantID(id string) string {
	if id == "" {
		return ""
	}
	lastColon := strings.LastIndex(id, ":")
	if lastColon < 0 {
		return s.id(id)
	}
	head, principalID := id[:lastColon], id[lastColon+1:]
	typeColon := strings.LastIndex(head, ":")
	if typeColon < 0 {
		return s.id(id)
	}
	entID, principalType := head[:typeColon], head[typeColon+1:]
	return s.entitlementID(entID) + ":" + principalType + ":" + s.id(principalID)
}

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
		Id:          s.entitlementID(in.GetId()),
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
		Id:          s.grantID(in.GetId()),
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
		out[s.entitlementID(srcEntitlementID)] = v2.GrantSources_GrantSource_builder{
			IsDirect: gs.GetIsDirect(),
		}.Build()
	}
	return v2.GrantSources_builder{Sources: out}.Build()
}
