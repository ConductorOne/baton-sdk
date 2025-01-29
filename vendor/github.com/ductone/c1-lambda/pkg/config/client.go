package config

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	pb_connector_manager "github.com/ductone/c1-lambda/pb/c1/svc/connector_manager/v1"
	"github.com/ductone/c1-lambda/pkg/ugrpc"
)

func ConnectorManagerClient(ctx context.Context, clientID string, clientSecret string) (pb_connector_manager.ConnectorManagerClient, error) {
	credProvider, clientName, tokenHost, err := ugrpc.NewC1LambdaCredentialProvider(ctx, clientID, clientSecret)
	if err != nil {
		return nil, err
	}

	if envHost, ok := os.LookupEnv("BATON_C1_API_HOST"); ok {
		tokenHost = envHost
	}
	// assume the token host does not have a port set, and we should use the default https port
	addr := ugrpc.HostPort(tokenHost, "443")
	host, port, err := net.SplitHostPort(tokenHost)
	if err == nil {
		addr = ugrpc.HostPort(host, port)
	}

	systemCertPool, err := x509.SystemCertPool()
	if err != nil || systemCertPool == nil {
		return nil, fmt.Errorf("connector-manager-client: failed to load system cert pool: %v", err)
	}
	tlsConfig := &tls.Config{
		RootCAs: systemCertPool,
	}
	creds := credentials.NewTLS(tlsConfig)

	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
			MinVersion: tls.VersionTLS12,
		})),
		grpc.WithPerRPCCredentials(credProvider),
		grpc.WithUserAgent(fmt.Sprintf("%s baton-lambda/%s", clientName, "v0.0.1")),
		grpc.WithTransportCredentials(creds),
		grpc.WithBlock(),
	}

	client, err := grpc.NewClient(addr, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("connector-manager-client: failed to create client: %w", err)
	}

	return pb_connector_manager.NewConnectorManagerClient(client), nil
}

func GetConnectorConfig(ctx context.Context, client pb_connector_manager.ConnectorManagerClient) (*pb_connector_manager.GetConnectorConfigResponse, error) {
	return client.GetConnectorConfig(ctx, &pb_connector_manager.GetConnectorConfigRequest{})
}
