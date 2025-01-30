package config

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	pb_connector_manager "github.com/ductone/c1-lambda/pb/c1/connectorapi/baton/v1"
	"github.com/ductone/c1-lambda/pkg/ugrpc"
)

func GetConnectorConfigServiceClient(ctx context.Context, clientID string, clientSecret string) (pb_connector_manager.ConnectorConfigServiceClient, error) {
	credProvider, clientName, clientHost, err := ugrpc.NewC1LambdaCredentialProvider(ctx, clientID, clientSecret)
	if err != nil {
		return nil, err
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

	if envHost, ok := os.LookupEnv("BATON_LAMBDA_CONFIGURATION_HOST"); ok {
		clientHost = envHost
	}

	client, err := grpc.NewClient(clientHost, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("connector-manager-client: failed to create client: %w", err)
	}

	return pb_connector_manager.NewConnectorConfigServiceClient(client), nil
}

func GetConnectorConfig(ctx context.Context, client pb_connector_manager.ConnectorConfigServiceClient) (*pb_connector_manager.GetConnectorConfigResponse, error) {
	return client.GetConnectorConfig(ctx, &pb_connector_manager.GetConnectorConfigRequest{})
}
