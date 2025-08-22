package v2

import (
	"context"
	"fmt"
	"io"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

var _ connectorstore.Reader = (*V2File)(nil)

func (c *V2File) ListResourceTypes(ctx context.Context, request *v2.ResourceTypesServiceListResourceTypesRequest) (*v2.ResourceTypesServiceListResourceTypesResponse, error) {
	if c.engine == nil {
		return nil, fmt.Errorf("v2file: engine not initialized")
	}
	return c.engine.ListResourceTypes(ctx, request)
}

func (c *V2File) GetResourceType(ctx context.Context, request *reader_v2.ResourceTypesReaderServiceGetResourceTypeRequest) (*reader_v2.ResourceTypesReaderServiceGetResourceTypeResponse, error) {
	if c.engine == nil {
		return nil, fmt.Errorf("v2file: engine not initialized")
	}
	return c.engine.GetResourceType(ctx, request)
}

func (c *V2File) ListResources(ctx context.Context, request *v2.ResourcesServiceListResourcesRequest) (*v2.ResourcesServiceListResourcesResponse, error) {
	if c.engine == nil {
		return nil, fmt.Errorf("v2file: engine not initialized")
	}
	return c.engine.ListResources(ctx, request)
}

func (c *V2File) GetResource(ctx context.Context, request *reader_v2.ResourcesReaderServiceGetResourceRequest) (*reader_v2.ResourcesReaderServiceGetResourceResponse, error) {
	if c.engine == nil {
		return nil, fmt.Errorf("v2file: engine not initialized")
	}
	return c.engine.GetResource(ctx, request)
}

func (c *V2File) ListEntitlements(ctx context.Context, request *v2.EntitlementsServiceListEntitlementsRequest) (*v2.EntitlementsServiceListEntitlementsResponse, error) {
	if c.engine == nil {
		return nil, fmt.Errorf("v2file: engine not initialized")
	}
	return c.engine.ListEntitlements(ctx, request)
}

func (c *V2File) GetEntitlement(ctx context.Context, request *reader_v2.EntitlementsReaderServiceGetEntitlementRequest) (*reader_v2.EntitlementsReaderServiceGetEntitlementResponse, error) {
	if c.engine == nil {
		return nil, fmt.Errorf("v2file: engine not initialized")
	}
	return c.engine.GetEntitlement(ctx, request)
}

func (c *V2File) ListGrants(ctx context.Context, request *v2.GrantsServiceListGrantsRequest) (*v2.GrantsServiceListGrantsResponse, error) {
	if c.engine == nil {
		return nil, fmt.Errorf("v2file: engine not initialized")
	}
	return c.engine.ListGrants(ctx, request)
}

func (c *V2File) GetGrant(ctx context.Context, request *reader_v2.GrantsReaderServiceGetGrantRequest) (*reader_v2.GrantsReaderServiceGetGrantResponse, error) {
	if c.engine == nil {
		return nil, fmt.Errorf("v2file: engine not initialized")
	}
	return c.engine.GetGrant(ctx, request)
}

func (c *V2File) ListGrantsForEntitlement(
	ctx context.Context,
	request *reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest,
) (*reader_v2.GrantsReaderServiceListGrantsForEntitlementResponse, error) {
	if c.engine == nil {
		return nil, fmt.Errorf("v2file: engine not initialized")
	}
	return c.engine.ListGrantsForEntitlement(ctx, request)
}

func (c *V2File) ListGrantsForResourceType(
	ctx context.Context,
	request *reader_v2.GrantsReaderServiceListGrantsForResourceTypeRequest,
) (*reader_v2.GrantsReaderServiceListGrantsForResourceTypeResponse, error) {
	if c.engine == nil {
		return nil, fmt.Errorf("v2file: engine not initialized")
	}
	return c.engine.ListGrantsForResourceType(ctx, request)
}

func (c *V2File) GetSync(ctx context.Context, request *reader_v2.SyncsReaderServiceGetSyncRequest) (*reader_v2.SyncsReaderServiceGetSyncResponse, error) {
	if c.engine == nil {
		return nil, fmt.Errorf("v2file: engine not initialized")
	}
	return c.engine.GetSync(ctx, request)
}

func (c *V2File) ListSyncs(ctx context.Context, request *reader_v2.SyncsReaderServiceListSyncsRequest) (*reader_v2.SyncsReaderServiceListSyncsResponse, error) {
	if c.engine == nil {
		return nil, fmt.Errorf("v2file: engine not initialized")
	}
	return c.engine.ListSyncs(ctx, request)
}

func (c *V2File) GetLatestFinishedSync(
	ctx context.Context,
	request *reader_v2.SyncsReaderServiceGetLatestFinishedSyncRequest,
) (*reader_v2.SyncsReaderServiceGetLatestFinishedSyncResponse, error) {
	if c.engine == nil {
		return nil, fmt.Errorf("v2file: engine not initialized")
	}
	return c.engine.GetLatestFinishedSync(ctx, request)
}

func (c *V2File) GetAsset(ctx context.Context, req *v2.AssetServiceGetAssetRequest) (string, io.Reader, error) {
	if c.engine == nil {
		return "", nil, fmt.Errorf("v2file: engine not initialized")
	}
	return c.engine.GetAsset(ctx, req)
}

func (c *V2File) ListGrantsForPrincipal(
	ctx context.Context,
	req *reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest,
) (*reader_v2.GrantsReaderServiceListGrantsForEntitlementResponse, error) {
	if c.engine == nil {
		return nil, fmt.Errorf("v2file: engine not initialized")
	}
	return c.engine.ListGrantsForPrincipal(ctx, req)
}
