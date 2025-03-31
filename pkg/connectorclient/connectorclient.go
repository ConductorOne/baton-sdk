package connectorclient

import (
	"context"

	"github.com/conductorone/baton-sdk/internal/connector"
	"github.com/conductorone/baton-sdk/pkg/types"
	"google.golang.org/grpc"
)

// NewConnectorClient takes a grpc.ClientConnInterface and returns an implementation of the ConnectorClient interface.
// Note: lambda functions directly instantiate the connector client, so this function is not used in this package.
func NewConnectorClient(ctx context.Context, cc grpc.ClientConnInterface) types.ConnectorClient {
	return connector.NewConnectorClient(ctx, cc)
}
