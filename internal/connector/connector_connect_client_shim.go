package connector

import (
	"context"
	"net/http"

	"connectrpc.com/connect"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pb/c1/connector/v2/v2connect"
	"github.com/conductorone/baton-sdk/pkg/types"
)

var _ types.ConnectorClient = (*connectorConnectClientShim)(nil)

// connectorConnectShim implements the ConnectorClient interface but implements it via connect.
type connectorConnectClientShim struct {
	accountManagerServiceClient    v2connect.AccountManagerServiceClient
	assetServiceClient             v2connect.AssetServiceClient
	connectorServiceClient         v2connect.ConnectorServiceClient
	credentialManagerServiceClient v2connect.CredentialManagerServiceClient
	entitlementsServiceClient      v2connect.EntitlementsServiceClient
	eventServiceClient             v2connect.EventServiceClient
	grantManagerServiceClient      v2connect.GrantManagerServiceClient
	grantsServiceClient            v2connect.GrantsServiceClient
	resourceManagerServiceClient   v2connect.ResourceManagerServiceClient
	resourceTypesServiceClient     v2connect.ResourceTypesServiceClient
	resourcesServiceClient         v2connect.ResourcesServiceClient
	ticketsServiceClient           v2connect.TicketsServiceClient
}

func NewConnectorConnectClientShim(ctx context.Context, addr string) *connectorConnectClientShim {
	httpClient := http.DefaultClient
	s := &connectorConnectClientShim{}
	s.accountManagerServiceClient = v2connect.NewAccountManagerServiceClient(httpClient, addr, connect.WithProtoJSON())
	s.assetServiceClient = v2connect.NewAssetServiceClient(httpClient, addr, connect.WithProtoJSON())
	s.connectorServiceClient = v2connect.NewConnectorServiceClient(httpClient, addr, connect.WithProtoJSON())
	s.credentialManagerServiceClient = v2connect.NewCredentialManagerServiceClient(httpClient, addr, connect.WithProtoJSON())
	s.entitlementsServiceClient = v2connect.NewEntitlementsServiceClient(httpClient, addr, connect.WithProtoJSON())
	s.eventServiceClient = v2connect.NewEventServiceClient(httpClient, addr, connect.WithProtoJSON())
	s.grantManagerServiceClient = v2connect.NewGrantManagerServiceClient(httpClient, addr, connect.WithProtoJSON())
	s.grantsServiceClient = v2connect.NewGrantsServiceClient(httpClient, addr, connect.WithProtoJSON())
	s.resourceManagerServiceClient = v2connect.NewResourceManagerServiceClient(httpClient, addr, connect.WithProtoJSON())
	s.resourceTypesServiceClient = v2connect.NewResourceTypesServiceClient(httpClient, addr, connect.WithProtoJSON())
	s.resourcesServiceClient = v2connect.NewResourcesServiceClient(httpClient, addr, connect.WithProtoJSON())
	s.ticketsServiceClient = v2connect.NewTicketsServiceClient(httpClient, addr, connect.WithProtoJSON())
	return s
}

// connectClientShim is a helper function to convert a connect handler to a grpc handler.
func connectClientShim[REQ any, RESP any](
	ctx context.Context,
	req *REQ,
	handler func(context.Context, *connect.Request[REQ]) (*connect.Response[RESP], error),
) (*RESP, error) {
	rv, err := handler(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return rv.Msg, nil
}

func (c *connectorConnectClientShim) ListResourceTypes(ctx context.Context, in *v2.ResourceTypesServiceListResourceTypesRequest, opts ...grpc.CallOption) (*v2.ResourceTypesServiceListResourceTypesResponse, error) {
	return connectClientShim(ctx, in, c.resourceTypesServiceClient.ListResourceTypes)
}

func (c *connectorConnectClientShim) ListResources(ctx context.Context, in *v2.ResourcesServiceListResourcesRequest, opts ...grpc.CallOption) (*v2.ResourcesServiceListResourcesResponse, error) {
	return connectClientShim(ctx, in, c.resourcesServiceClient.ListResources)
}

func (c *connectorConnectClientShim) ListEntitlements(ctx context.Context, in *v2.EntitlementsServiceListEntitlementsRequest, opts ...grpc.CallOption) (*v2.EntitlementsServiceListEntitlementsResponse, error) {
	return connectClientShim(ctx, in, c.entitlementsServiceClient.ListEntitlements)
}

func (c *connectorConnectClientShim) ListGrants(ctx context.Context, in *v2.GrantsServiceListGrantsRequest, opts ...grpc.CallOption) (*v2.GrantsServiceListGrantsResponse, error) {
	return connectClientShim(ctx, in, c.grantsServiceClient.ListGrants)
}

func (c *connectorConnectClientShim) GetMetadata(ctx context.Context, in *v2.ConnectorServiceGetMetadataRequest, opts ...grpc.CallOption) (*v2.ConnectorServiceGetMetadataResponse, error) {
	return connectClientShim(ctx, in, c.connectorServiceClient.GetMetadata)
}

func (c *connectorConnectClientShim) Validate(ctx context.Context, in *v2.ConnectorServiceValidateRequest, opts ...grpc.CallOption) (*v2.ConnectorServiceValidateResponse, error) {
	return connectClientShim(ctx, in, c.connectorServiceClient.Validate)
}

func (c *connectorConnectClientShim) Cleanup(ctx context.Context, in *v2.ConnectorServiceCleanupRequest, opts ...grpc.CallOption) (*v2.ConnectorServiceCleanupResponse, error) {
	return connectClientShim(ctx, in, c.connectorServiceClient.Cleanup)
}

func (c *connectorConnectClientShim) GetAsset(ctx context.Context, in *v2.AssetServiceGetAssetRequest, opts ...grpc.CallOption) (v2.AssetService_GetAssetClient, error) {
	return nil, status.Error(codes.Unimplemented, "GetAsset not implemented")
}

func (c *connectorConnectClientShim) Grant(ctx context.Context, in *v2.GrantManagerServiceGrantRequest, opts ...grpc.CallOption) (*v2.GrantManagerServiceGrantResponse, error) {
	return connectClientShim(ctx, in, c.grantManagerServiceClient.Grant)
}

func (c *connectorConnectClientShim) Revoke(ctx context.Context, in *v2.GrantManagerServiceRevokeRequest, opts ...grpc.CallOption) (*v2.GrantManagerServiceRevokeResponse, error) {
	return connectClientShim(ctx, in, c.grantManagerServiceClient.Revoke)
}

func (c *connectorConnectClientShim) CreateResource(ctx context.Context, in *v2.CreateResourceRequest, opts ...grpc.CallOption) (*v2.CreateResourceResponse, error) {
	return connectClientShim(ctx, in, c.resourceManagerServiceClient.CreateResource)
}

func (c *connectorConnectClientShim) DeleteResource(ctx context.Context, in *v2.DeleteResourceRequest, opts ...grpc.CallOption) (*v2.DeleteResourceResponse, error) {
	return connectClientShim(ctx, in, c.resourceManagerServiceClient.DeleteResource)
}

func (c *connectorConnectClientShim) CreateAccount(ctx context.Context, in *v2.CreateAccountRequest, opts ...grpc.CallOption) (*v2.CreateAccountResponse, error) {
	return connectClientShim(ctx, in, c.accountManagerServiceClient.CreateAccount)
}

func (c *connectorConnectClientShim) RotateCredential(ctx context.Context, in *v2.RotateCredentialRequest, opts ...grpc.CallOption) (*v2.RotateCredentialResponse, error) {
	return connectClientShim(ctx, in, c.credentialManagerServiceClient.RotateCredential)
}

func (c *connectorConnectClientShim) ListEvents(ctx context.Context, in *v2.ListEventsRequest, opts ...grpc.CallOption) (*v2.ListEventsResponse, error) {
	return connectClientShim(ctx, in, c.eventServiceClient.ListEvents)
}

func (c *connectorConnectClientShim) CreateTicket(ctx context.Context, in *v2.TicketsServiceCreateTicketRequest, opts ...grpc.CallOption) (*v2.TicketsServiceCreateTicketResponse, error) {
	return connectClientShim(ctx, in, c.ticketsServiceClient.CreateTicket)
}

func (c *connectorConnectClientShim) GetTicket(ctx context.Context, in *v2.TicketsServiceGetTicketRequest, opts ...grpc.CallOption) (*v2.TicketsServiceGetTicketResponse, error) {
	return connectClientShim(ctx, in, c.ticketsServiceClient.GetTicket)
}

func (c *connectorConnectClientShim) ListTicketSchemas(ctx context.Context, in *v2.TicketsServiceListTicketSchemasRequest, opts ...grpc.CallOption) (*v2.TicketsServiceListTicketSchemasResponse, error) {
	return connectClientShim(ctx, in, c.ticketsServiceClient.ListTicketSchemas)
}

func (c *connectorConnectClientShim) GetTicketSchema(ctx context.Context, in *v2.TicketsServiceGetTicketSchemaRequest, opts ...grpc.CallOption) (*v2.TicketsServiceGetTicketSchemaResponse, error) {
	return connectClientShim(ctx, in, c.ticketsServiceClient.GetTicketSchema)
}

func (c *connectorConnectClientShim) BulkCreateTickets(ctx context.Context, in *v2.TicketsServiceBulkCreateTicketsRequest, opts ...grpc.CallOption) (*v2.TicketsServiceBulkCreateTicketsResponse, error) {
	return connectClientShim(ctx, in, c.ticketsServiceClient.BulkCreateTickets)
}

func (c *connectorConnectClientShim) BulkGetTickets(ctx context.Context, in *v2.TicketsServiceBulkGetTicketsRequest, opts ...grpc.CallOption) (*v2.TicketsServiceBulkGetTicketsResponse, error) {
	return connectClientShim(ctx, in, c.ticketsServiceClient.BulkGetTickets)
}
