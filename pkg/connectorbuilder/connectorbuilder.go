package connectorbuilder

import (
	"context"
	"fmt"
	"io"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	"github.com/conductorone/baton-sdk/pkg/types"
)

type ResourceSyncer interface {
	ResourceType(ctx context.Context) *v2.ResourceType
	List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error)
	Entitlements(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error)
	Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error)
}

type ConnectorBuilder interface {
	Metadata(ctx context.Context) (*v2.ConnectorMetadata, error)
	Validate(ctx context.Context) (annotations.Annotations, error)
	Asset(ctx context.Context, asset *v2.AssetRef) (string, io.ReadCloser, error)
	ResourceSyncers(ctx context.Context) []ResourceSyncer
}

type builderImpl struct {
	resourceBuilders map[string]ResourceSyncer
	cb               ConnectorBuilder
}

// NewConnector creates a new ConnectorServer for a new resource.
func NewConnector(ctx context.Context, in interface{}) (types.ConnectorServer, error) {
	switch c := in.(type) {
	case ConnectorBuilder:
		ret := &builderImpl{
			resourceBuilders: make(map[string]ResourceSyncer),
			cb:               c,
		}

		for _, rb := range c.ResourceSyncers(ctx) {
			rType := rb.ResourceType(ctx)
			if _, ok := ret.resourceBuilders[rType.Id]; ok {
				return nil, fmt.Errorf("error: duplicate resource type found %s", rType.Id)
			}
			ret.resourceBuilders[rType.Id] = rb
		}
		return ret, nil

	case types.ConnectorServer:
		return c, nil

	default:
		return nil, fmt.Errorf("input was not a ConnectorBuilder or a ConnectorServer")
	}
}

// ListResourceTypes lists all available resource types.
func (b *builderImpl) ListResourceTypes(
	ctx context.Context,
	request *v2.ResourceTypesServiceListResourceTypesRequest,
) (*v2.ResourceTypesServiceListResourceTypesResponse, error) {
	var out []*v2.ResourceType

	for _, rb := range b.resourceBuilders {
		out = append(out, rb.ResourceType(ctx))
	}

	return &v2.ResourceTypesServiceListResourceTypesResponse{List: out}, nil
}

// ListResources returns all available resources for a given resource type ID.
func (b *builderImpl) ListResources(ctx context.Context, request *v2.ResourcesServiceListResourcesRequest) (*v2.ResourcesServiceListResourcesResponse, error) {
	rb, ok := b.resourceBuilders[request.ResourceTypeId]
	if !ok {
		return nil, fmt.Errorf("error: list resources with unknown resource type %s", request.ResourceTypeId)
	}

	out, nextPageToken, annos, err := rb.List(ctx, request.ParentResourceId, &pagination.Token{
		Size:  int(request.PageSize),
		Token: request.PageToken,
	})
	if err != nil {
		return nil, fmt.Errorf("error: listing resources failed: %w", err)
	}

	return &v2.ResourcesServiceListResourcesResponse{
		List:          out,
		NextPageToken: nextPageToken,
		Annotations:   annos,
	}, nil
}

// ListEntitlements returns all the entitlements for a given resource.
func (b *builderImpl) ListEntitlements(ctx context.Context, request *v2.EntitlementsServiceListEntitlementsRequest) (*v2.EntitlementsServiceListEntitlementsResponse, error) {
	rb, ok := b.resourceBuilders[request.Resource.Id.ResourceType]
	if !ok {
		return nil, fmt.Errorf("error: list entitlements with unknown resource type %s", request.Resource.Id.ResourceType)
	}

	out, nextPageToken, annos, err := rb.Entitlements(ctx, request.Resource, &pagination.Token{
		Size:  int(request.PageSize),
		Token: request.PageToken,
	})
	if err != nil {
		return nil, fmt.Errorf("error: listing entitlements failed: %w", err)
	}

	return &v2.EntitlementsServiceListEntitlementsResponse{
		List:          out,
		NextPageToken: nextPageToken,
		Annotations:   annos,
	}, nil
}

// ListGrants lists all the grants for a given resource.
func (b *builderImpl) ListGrants(ctx context.Context, request *v2.GrantsServiceListGrantsRequest) (*v2.GrantsServiceListGrantsResponse, error) {
	rb, ok := b.resourceBuilders[request.Resource.Id.ResourceType]
	if !ok {
		return nil, fmt.Errorf("error: list entitlements with unknown resource type %s", request.Resource.Id.ResourceType)
	}

	out, nextPageToken, annos, err := rb.Grants(ctx, request.Resource, &pagination.Token{
		Size:  int(request.PageSize),
		Token: request.PageToken,
	})
	if err != nil {
		return nil, fmt.Errorf("error: listing entitlements failed: %w", err)
	}

	return &v2.GrantsServiceListGrantsResponse{
		List:          out,
		NextPageToken: nextPageToken,
		Annotations:   annos,
	}, nil
}

// GetMetadata gets all metadata for a connector.
func (b *builderImpl) GetMetadata(ctx context.Context, request *v2.ConnectorServiceGetMetadataRequest) (*v2.ConnectorServiceGetMetadataResponse, error) {
	md, err := b.cb.Metadata(ctx)
	if err != nil {
		return nil, err
	}

	return &v2.ConnectorServiceGetMetadataResponse{Metadata: md}, nil
}

// Validate validates the connector.
func (b *builderImpl) Validate(ctx context.Context, request *v2.ConnectorServiceValidateRequest) (*v2.ConnectorServiceValidateResponse, error) {
	annos, err := b.cb.Validate(ctx)
	if err != nil {
		return nil, err
	}

	return &v2.ConnectorServiceValidateResponse{Annotations: annos}, nil
}

// GetAsset streams the asset to the client.
func (b *builderImpl) GetAsset(request *v2.AssetServiceGetAssetRequest, server v2.AssetService_GetAssetServer) error {
	contentType, r, err := b.cb.Asset(server.Context(), request.Asset)
	if err != nil {
		return err
	}
	defer r.Close()

	return streamAsset(contentType, r, server)
}
