package types

import (
	"context"

	connectorV2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	connectorwrapperV1 "github.com/conductorone/baton-sdk/pb/c1/connector_wrapper/v1"
)

// ConnectorServer is an interface for a single type that implements all ConnectorV2 services.
type ConnectorServer interface {
	connectorV2.ResourceTypesServiceServer
	connectorV2.ResourcesServiceServer
	connectorV2.EntitlementsServiceServer
	connectorV2.GrantsServiceServer
	connectorV2.ConnectorServiceServer
	connectorV2.AssetServiceServer
	connectorV2.GrantManagerServiceServer
	connectorV2.ResourceManagerServiceServer
	connectorV2.AccountManagerServiceServer
	connectorV2.CredentialManagerServiceServer
	connectorV2.EventServiceServer
	connectorV2.TicketsServiceServer
}

// ConnectorClient is an interface for a type that implements all ConnectorV2 services.
type ConnectorClient interface {
	connectorV2.ResourceTypesServiceClient
	connectorV2.ResourcesServiceClient
	connectorV2.EntitlementsServiceClient
	connectorV2.GrantsServiceClient
	connectorV2.ConnectorServiceClient
	connectorV2.AssetServiceClient
	connectorV2.GrantManagerServiceClient
	connectorV2.ResourceManagerServiceClient
	connectorV2.AccountManagerServiceClient
	connectorV2.CredentialManagerServiceClient
	connectorV2.EventServiceClient
	connectorV2.TicketsServiceClient
}

// ClientWrapper is an interface that returns a connector client.
type ClientWrapper interface {
	C(ctx context.Context) (ConnectorClient, error)
	Run(ctx context.Context, cfg *connectorwrapperV1.ServerConfig) error
	Close() error
}
