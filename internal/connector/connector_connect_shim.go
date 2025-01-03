package connector

import (
	"context"

	"connectrpc.com/connect"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	ratelimit "github.com/conductorone/baton-sdk/pb/c1/ratelimit/v1"
	"github.com/conductorone/baton-sdk/pkg/types"
)

// connectorConnectShim translates connect-rpc handlers to associated methods on given grpc ConnectorServer.
type connectorConnectShim struct {
	server      types.ConnectorServer
	rateLimiter ratelimit.RateLimiterServiceServer

	ticketingServiceServer         v2.TicketsServiceServer
	grantManagerServiceServer      v2.GrantManagerServiceServer
	resourceManagerServiceServer   v2.ResourceManagerServiceServer
	accountManagerServiceServer    v2.AccountManagerServiceServer
	credentialManagerServiceServer v2.CredentialManagerServiceServer
}

func NewConnectorConnectShim(
	server types.ConnectorServer,
	rateLimiter ratelimit.RateLimiterServiceServer,
	provisioningEnabled bool,
	ticketingEnabled bool,
) *connectorConnectShim {
	s := &connectorConnectShim{
		server:      server,
		rateLimiter: rateLimiter,
	}

	if ticketingEnabled {
		s.ticketingServiceServer = server.(v2.TicketsServiceServer)
	} else {
		s.ticketingServiceServer = &noopTicketing{}
	}

	if provisioningEnabled {
		s.grantManagerServiceServer = server.(v2.GrantManagerServiceServer)
		s.resourceManagerServiceServer = server.(v2.ResourceManagerServiceServer)
		s.accountManagerServiceServer = server.(v2.AccountManagerServiceServer)
		s.credentialManagerServiceServer = server.(v2.CredentialManagerServiceServer)
	} else {
		noop := &noopProvisioner{}
		s.grantManagerServiceServer = noop
		s.resourceManagerServiceServer = noop
		s.accountManagerServiceServer = noop
		s.credentialManagerServiceServer = noop
	}

	return s
}

// connectShim is a helper function to convert a connect handler to a grpc handler.
func connectShim[REQ any, RESP any](
	ctx context.Context,
	req *connect.Request[REQ],
	handler func(context.Context, *REQ) (*RESP, error),
) (*connect.Response[RESP], error) {
	rv, err := handler(ctx, req.Msg)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(rv), nil
}

func (c *connectorConnectShim) Do(ctx context.Context, c2 *connect.Request[ratelimit.DoRequest]) (*connect.Response[ratelimit.DoResponse], error) {
	return connectShim(ctx, c2, c.rateLimiter.Do)
}

func (c *connectorConnectShim) Report(ctx context.Context, c2 *connect.Request[ratelimit.ReportRequest]) (*connect.Response[ratelimit.ReportResponse], error) {
	return connectShim(ctx, c2, c.rateLimiter.Report)
}

func (c *connectorConnectShim) ListResourceTypes(ctx context.Context, c2 *connect.Request[v2.ResourceTypesServiceListResourceTypesRequest]) (*connect.Response[v2.ResourceTypesServiceListResourceTypesResponse], error) {
	return connectShim(ctx, c2, c.server.ListResourceTypes)
}

func (c *connectorConnectShim) ListResources(ctx context.Context, c2 *connect.Request[v2.ResourcesServiceListResourcesRequest]) (*connect.Response[v2.ResourcesServiceListResourcesResponse], error) {
	return connectShim(ctx, c2, c.server.ListResources)
}

func (c *connectorConnectShim) ListEntitlements(ctx context.Context, c2 *connect.Request[v2.EntitlementsServiceListEntitlementsRequest]) (*connect.Response[v2.EntitlementsServiceListEntitlementsResponse], error) {
	return connectShim(ctx, c2, c.server.ListEntitlements)
}

func (c *connectorConnectShim) ListGrants(ctx context.Context, c2 *connect.Request[v2.GrantsServiceListGrantsRequest]) (*connect.Response[v2.GrantsServiceListGrantsResponse], error) {
	return connectShim(ctx, c2, c.server.ListGrants)
}

func (c *connectorConnectShim) GetMetadata(ctx context.Context, c2 *connect.Request[v2.ConnectorServiceGetMetadataRequest]) (*connect.Response[v2.ConnectorServiceGetMetadataResponse], error) {
	return connectShim(ctx, c2, c.server.GetMetadata)
}

func (c *connectorConnectShim) Validate(ctx context.Context, c2 *connect.Request[v2.ConnectorServiceValidateRequest]) (*connect.Response[v2.ConnectorServiceValidateResponse], error) {
	return connectShim(ctx, c2, c.server.Validate)
}

func (c *connectorConnectShim) Cleanup(ctx context.Context, c2 *connect.Request[v2.ConnectorServiceCleanupRequest]) (*connect.Response[v2.ConnectorServiceCleanupResponse], error) {
	return connectShim(ctx, c2, c.server.Cleanup)
}

func (c *connectorConnectShim) GetAsset(ctx context.Context, c2 *connect.Request[v2.AssetServiceGetAssetRequest], c3 *connect.ServerStream[v2.AssetServiceGetAssetResponse]) error {
	return status.Errorf(codes.Unimplemented, "GetAsset not implemented")
}

// GrantManagerService
func (c *connectorConnectShim) Grant(ctx context.Context, c2 *connect.Request[v2.GrantManagerServiceGrantRequest]) (*connect.Response[v2.GrantManagerServiceGrantResponse], error) {
	return connectShim(ctx, c2, c.grantManagerServiceServer.Grant)
}

func (c *connectorConnectShim) Revoke(ctx context.Context, c2 *connect.Request[v2.GrantManagerServiceRevokeRequest]) (*connect.Response[v2.GrantManagerServiceRevokeResponse], error) {
	return connectShim(ctx, c2, c.grantManagerServiceServer.Revoke)
}

// ResourceManagerService
func (c *connectorConnectShim) CreateResource(ctx context.Context, c2 *connect.Request[v2.CreateResourceRequest]) (*connect.Response[v2.CreateResourceResponse], error) {
	return connectShim(ctx, c2, c.resourceManagerServiceServer.CreateResource)
}

func (c *connectorConnectShim) DeleteResource(ctx context.Context, c2 *connect.Request[v2.DeleteResourceRequest]) (*connect.Response[v2.DeleteResourceResponse], error) {
	return connectShim(ctx, c2, c.resourceManagerServiceServer.DeleteResource)
}

// AccountManagerService
func (c *connectorConnectShim) CreateAccount(ctx context.Context, c2 *connect.Request[v2.CreateAccountRequest]) (*connect.Response[v2.CreateAccountResponse], error) {
	return connectShim(ctx, c2, c.accountManagerServiceServer.CreateAccount)
}

// CredentialManagerService
func (c *connectorConnectShim) RotateCredential(ctx context.Context, c2 *connect.Request[v2.RotateCredentialRequest]) (*connect.Response[v2.RotateCredentialResponse], error) {
	return connectShim(ctx, c2, c.credentialManagerServiceServer.RotateCredential)
}

// Events
func (c *connectorConnectShim) ListEvents(ctx context.Context, c2 *connect.Request[v2.ListEventsRequest]) (*connect.Response[v2.ListEventsResponse], error) {
	return connectShim(ctx, c2, c.server.ListEvents)
}

// TicketingService
func (c *connectorConnectShim) CreateTicket(ctx context.Context, c2 *connect.Request[v2.TicketsServiceCreateTicketRequest]) (*connect.Response[v2.TicketsServiceCreateTicketResponse], error) {
	return connectShim(ctx, c2, c.ticketingServiceServer.CreateTicket)
}

func (c *connectorConnectShim) GetTicket(ctx context.Context, c2 *connect.Request[v2.TicketsServiceGetTicketRequest]) (*connect.Response[v2.TicketsServiceGetTicketResponse], error) {
	return connectShim(ctx, c2, c.ticketingServiceServer.GetTicket)
}

func (c *connectorConnectShim) ListTicketSchemas(ctx context.Context, c2 *connect.Request[v2.TicketsServiceListTicketSchemasRequest]) (*connect.Response[v2.TicketsServiceListTicketSchemasResponse], error) {
	return connectShim(ctx, c2, c.ticketingServiceServer.ListTicketSchemas)
}

func (c *connectorConnectShim) GetTicketSchema(ctx context.Context, c2 *connect.Request[v2.TicketsServiceGetTicketSchemaRequest]) (*connect.Response[v2.TicketsServiceGetTicketSchemaResponse], error) {
	return connectShim(ctx, c2, c.ticketingServiceServer.GetTicketSchema)
}

func (c *connectorConnectShim) BulkCreateTickets(ctx context.Context, c2 *connect.Request[v2.TicketsServiceBulkCreateTicketsRequest]) (*connect.Response[v2.TicketsServiceBulkCreateTicketsResponse], error) {
	return connectShim(ctx, c2, c.ticketingServiceServer.BulkCreateTickets)
}

func (c *connectorConnectShim) BulkGetTickets(ctx context.Context, c2 *connect.Request[v2.TicketsServiceBulkGetTicketsRequest]) (*connect.Response[v2.TicketsServiceBulkGetTicketsResponse], error) {
	return connectShim(ctx, c2, c.ticketingServiceServer.BulkGetTickets)
}
