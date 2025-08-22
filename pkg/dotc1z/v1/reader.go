package v1

import (
	"context"
	"io"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

var _ connectorstore.Reader = (*V1File)(nil)

func (c *V1File) ListResourceTypes(ctx context.Context, request *v2.ResourceTypesServiceListResourceTypesRequest) (*v2.ResourceTypesServiceListResourceTypesResponse, error) {
	return c.engine.ListResourceTypes(ctx, request)
}

func (c *V1File) GetResourceType(
	ctx context.Context,
	request *reader_v2.ResourceTypesReaderServiceGetResourceTypeRequest,
) (*reader_v2.ResourceTypesReaderServiceGetResourceTypeResponse, error) {
	return c.engine.GetResourceType(ctx, request)
}

func (c *V1File) ListResources(ctx context.Context, request *v2.ResourcesServiceListResourcesRequest) (*v2.ResourcesServiceListResourcesResponse, error) {
	return c.engine.ListResources(ctx, request)
}

func (c *V1File) GetResource(ctx context.Context, request *reader_v2.ResourcesReaderServiceGetResourceRequest) (*reader_v2.ResourcesReaderServiceGetResourceResponse, error) {
	return c.engine.GetResource(ctx, request)
}

func (c *V1File) ListEntitlements(ctx context.Context, request *v2.EntitlementsServiceListEntitlementsRequest) (*v2.EntitlementsServiceListEntitlementsResponse, error) {
	return c.engine.ListEntitlements(ctx, request)
}

func (c *V1File) GetEntitlement(
	ctx context.Context,
	request *reader_v2.EntitlementsReaderServiceGetEntitlementRequest,
) (*reader_v2.EntitlementsReaderServiceGetEntitlementResponse, error) {
	return c.engine.GetEntitlement(ctx, request)
}

func (c *V1File) ListGrants(ctx context.Context, request *v2.GrantsServiceListGrantsRequest) (*v2.GrantsServiceListGrantsResponse, error) {
	return c.engine.ListGrants(ctx, request)
}

func (c *V1File) GetGrant(ctx context.Context, request *reader_v2.GrantsReaderServiceGetGrantRequest) (*reader_v2.GrantsReaderServiceGetGrantResponse, error) {
	return c.engine.GetGrant(ctx, request)
}

func (c *V1File) ListGrantsForEntitlement(
	ctx context.Context,
	request *reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest,
) (*reader_v2.GrantsReaderServiceListGrantsForEntitlementResponse, error) {
	return c.engine.ListGrantsForEntitlement(ctx, request)
}

func (c *V1File) ListGrantsForResourceType(
	ctx context.Context,
	request *reader_v2.GrantsReaderServiceListGrantsForResourceTypeRequest,
) (*reader_v2.GrantsReaderServiceListGrantsForResourceTypeResponse, error) {
	return c.engine.ListGrantsForResourceType(ctx, request)
}

func (c *V1File) GetSync(ctx context.Context, request *reader_v2.SyncsReaderServiceGetSyncRequest) (*reader_v2.SyncsReaderServiceGetSyncResponse, error) {
	return c.engine.GetSync(ctx, request)
}

func (c *V1File) ListSyncs(ctx context.Context, request *reader_v2.SyncsReaderServiceListSyncsRequest) (*reader_v2.SyncsReaderServiceListSyncsResponse, error) {
	return c.engine.ListSyncs(ctx, request)
}

func (c *V1File) GetLatestFinishedSync(
	ctx context.Context,
	request *reader_v2.SyncsReaderServiceGetLatestFinishedSyncRequest,
) (*reader_v2.SyncsReaderServiceGetLatestFinishedSyncResponse, error) {
	return c.engine.GetLatestFinishedSync(ctx, request)
}

func (c *V1File) GetAsset(ctx context.Context, req *v2.AssetServiceGetAssetRequest) (string, io.Reader, error) {
	return c.engine.GetAsset(ctx, req)
}

func (c *V1File) ListGrantsForPrincipal(
	ctx context.Context,
	req *reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest,
) (*reader_v2.GrantsReaderServiceListGrantsForEntitlementResponse, error) {
	return c.engine.ListGrantsForPrincipal(ctx, req)
}
