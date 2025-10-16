package sdk

import (
	"context"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type emptyConnector struct{}

// GetAsset gets an asset.
func (n *emptyConnector) GetAsset(_ context.Context, request *v2.AssetServiceGetAssetRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[v2.AssetServiceGetAssetResponse], error) {
	return nil, status.Errorf(codes.NotFound, "empty connector")
}

// ListResourceTypes returns a list of resource types.
func (n *emptyConnector) ListResourceTypes(
	ctx context.Context,
	request *v2.ResourceTypesServiceListResourceTypesRequest,
	opts ...grpc.CallOption,
) (*v2.ResourceTypesServiceListResourceTypesResponse, error) {
	responseBuilder := v2.ResourceTypesServiceListResourceTypesResponse_builder{
		List: []*v2.ResourceType{},
	}
	return responseBuilder.Build(), nil
}

// ListResources returns a list of resources.
func (n *emptyConnector) ListResources(ctx context.Context, request *v2.ResourcesServiceListResourcesRequest, opts ...grpc.CallOption) (*v2.ResourcesServiceListResourcesResponse, error) {
	responseBuilder := v2.ResourcesServiceListResourcesResponse_builder{
		List: []*v2.Resource{},
	}
	return responseBuilder.Build(), nil
}

func (n *emptyConnector) GetResource(
	ctx context.Context,
	request *v2.ResourceGetterServiceGetResourceRequest,
	opts ...grpc.CallOption,
) (*v2.ResourceGetterServiceGetResourceResponse, error) {
	return nil, status.Errorf(codes.NotFound, "empty connector")
}

// ListEntitlements returns a list of entitlements.
func (n *emptyConnector) ListEntitlements(
	ctx context.Context,
	request *v2.EntitlementsServiceListEntitlementsRequest,
	opts ...grpc.CallOption,
) (*v2.EntitlementsServiceListEntitlementsResponse, error) {
	responseBuilder := v2.EntitlementsServiceListEntitlementsResponse_builder{
		List: []*v2.Entitlement{},
	}
	return responseBuilder.Build(), nil
}

// ListGrants returns a list of grants.
func (n *emptyConnector) ListGrants(ctx context.Context, request *v2.GrantsServiceListGrantsRequest, opts ...grpc.CallOption) (*v2.GrantsServiceListGrantsResponse, error) {
	responseBuilder := v2.GrantsServiceListGrantsResponse_builder{
		List: []*v2.Grant{},
	}
	return responseBuilder.Build(), nil
}

func (n *emptyConnector) Grant(ctx context.Context, request *v2.GrantManagerServiceGrantRequest, opts ...grpc.CallOption) (*v2.GrantManagerServiceGrantResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "empty connector")
}

func (n *emptyConnector) Revoke(ctx context.Context, request *v2.GrantManagerServiceRevokeRequest, opts ...grpc.CallOption) (*v2.GrantManagerServiceRevokeResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "empty connector")
}

// GetMetadata returns a connector metadata.
func (n *emptyConnector) GetMetadata(ctx context.Context, request *v2.ConnectorServiceGetMetadataRequest, opts ...grpc.CallOption) (*v2.ConnectorServiceGetMetadataResponse, error) {
	responseBuilder := v2.ConnectorServiceGetMetadataResponse_builder{Metadata: v2.ConnectorMetadata_builder{}.Build()}
	return responseBuilder.Build(), nil
}

// Validate is called by the connector framework to validate the correct response.
func (n *emptyConnector) Validate(ctx context.Context, request *v2.ConnectorServiceValidateRequest, opts ...grpc.CallOption) (*v2.ConnectorServiceValidateResponse, error) {
	responseBuilder := v2.ConnectorServiceValidateResponse_builder{}
	return responseBuilder.Build(), nil
}

func (n *emptyConnector) BulkCreateTickets(ctx context.Context, request *v2.TicketsServiceBulkCreateTicketsRequest, opts ...grpc.CallOption) (*v2.TicketsServiceBulkCreateTicketsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "empty connector")
}

func (n *emptyConnector) BulkGetTickets(ctx context.Context, request *v2.TicketsServiceBulkGetTicketsRequest, opts ...grpc.CallOption) (*v2.TicketsServiceBulkGetTicketsResponse, error) {
	responseBuilder := v2.TicketsServiceBulkGetTicketsResponse_builder{
		Tickets: []*v2.TicketsServiceGetTicketResponse{},
	}
	return responseBuilder.Build(), nil
}

func (n *emptyConnector) CreateTicket(ctx context.Context, request *v2.TicketsServiceCreateTicketRequest, opts ...grpc.CallOption) (*v2.TicketsServiceCreateTicketResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "empty connector")
}

func (n *emptyConnector) GetTicket(ctx context.Context, request *v2.TicketsServiceGetTicketRequest, opts ...grpc.CallOption) (*v2.TicketsServiceGetTicketResponse, error) {
	return nil, status.Errorf(codes.NotFound, "empty connector")
}

func (n *emptyConnector) ListTicketSchemas(ctx context.Context, request *v2.TicketsServiceListTicketSchemasRequest, opts ...grpc.CallOption) (*v2.TicketsServiceListTicketSchemasResponse, error) {
	responseBuilder := v2.TicketsServiceListTicketSchemasResponse_builder{
		List: []*v2.TicketSchema{},
	}
	return responseBuilder.Build(), nil
}

func (n *emptyConnector) GetTicketSchema(ctx context.Context, request *v2.TicketsServiceGetTicketSchemaRequest, opts ...grpc.CallOption) (*v2.TicketsServiceGetTicketSchemaResponse, error) {
	return nil, status.Errorf(codes.NotFound, "empty connector")
}

func (n *emptyConnector) Cleanup(ctx context.Context, request *v2.ConnectorServiceCleanupRequest, opts ...grpc.CallOption) (*v2.ConnectorServiceCleanupResponse, error) {
	responseBuilder := v2.ConnectorServiceCleanupResponse_builder{}
	return responseBuilder.Build(), nil
}

func (n *emptyConnector) CreateAccount(ctx context.Context, request *v2.CreateAccountRequest, opts ...grpc.CallOption) (*v2.CreateAccountResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "empty connector")
}

func (n *emptyConnector) RotateCredential(ctx context.Context, request *v2.RotateCredentialRequest, opts ...grpc.CallOption) (*v2.RotateCredentialResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "empty connector")
}

func (n *emptyConnector) CreateResource(ctx context.Context, request *v2.CreateResourceRequest, opts ...grpc.CallOption) (*v2.CreateResourceResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "empty connector")
}

func (n *emptyConnector) DeleteResource(ctx context.Context, request *v2.DeleteResourceRequest, opts ...grpc.CallOption) (*v2.DeleteResourceResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "empty connector")
}

func (n *emptyConnector) DeleteResourceV2(ctx context.Context, request *v2.DeleteResourceV2Request, opts ...grpc.CallOption) (*v2.DeleteResourceV2Response, error) {
	return nil, status.Errorf(codes.Unimplemented, "empty connector")
}

func (n *emptyConnector) GetActionSchema(ctx context.Context, request *v2.GetActionSchemaRequest, opts ...grpc.CallOption) (*v2.GetActionSchemaResponse, error) {
	return nil, status.Errorf(codes.NotFound, "empty connector")
}

func (n *emptyConnector) GetActionStatus(ctx context.Context, request *v2.GetActionStatusRequest, opts ...grpc.CallOption) (*v2.GetActionStatusResponse, error) {
	return nil, status.Errorf(codes.NotFound, "empty connector")
}

func (n *emptyConnector) InvokeAction(ctx context.Context, request *v2.InvokeActionRequest, opts ...grpc.CallOption) (*v2.InvokeActionResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "empty connector")
}

func (n *emptyConnector) ListActionSchemas(ctx context.Context, request *v2.ListActionSchemasRequest, opts ...grpc.CallOption) (*v2.ListActionSchemasResponse, error) {
	responseBuilder := v2.ListActionSchemasResponse_builder{
		Schemas: []*v2.BatonActionSchema{},
	}
	return responseBuilder.Build(), nil
}

func (n *emptyConnector) ListEvents(ctx context.Context, request *v2.ListEventsRequest, opts ...grpc.CallOption) (*v2.ListEventsResponse, error) {
	responseBuilder := v2.ListEventsResponse_builder{
		Events: []*v2.Event{},
	}
	return responseBuilder.Build(), nil
}

func (n *emptyConnector) ListEventFeeds(ctx context.Context, request *v2.ListEventFeedsRequest, opts ...grpc.CallOption) (*v2.ListEventFeedsResponse, error) {
	responseBuilder := v2.ListEventFeedsResponse_builder{
		List: []*v2.EventFeedMetadata{},
	}
	return responseBuilder.Build(), nil
}

// NewEmptyConnector returns a new emptyConnector.
func NewEmptyConnector() (*emptyConnector, error) {
	return &emptyConnector{}, nil
}
