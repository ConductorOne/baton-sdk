package config

import (
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	pb_connector_manager "github.com/ductone/c1-lambda/pb/c1/connectorapi/baton/v1"
	"github.com/ductone/c1-lambda/pkg/ugrpc"
)

type ConnectorConfigServiceClient struct {
	privateKey ed25519.PrivateKey
	c          pb_connector_manager.ConnectorConfigServiceClient
}

func (c *ConnectorConfigServiceClient) GetConnectorConfig(ctx context.Context, in *pb_connector_manager.GetConnectorConfigRequest, opts ...grpc.CallOption) (*pb_connector_manager.GetConnectorConfigResponse, error) {
	// TODO(morgabra/kans): We use dpop with the c.privateKey, so config responses will be encrypted. We shim the response here so we can do that decryption.
	return c.c.GetConnectorConfig(ctx, in, opts...)
}

func GetConnectorConfigServiceClient(ctx context.Context, clientID string, clientSecret string) (*ConnectorConfigServiceClient, error) {
	_, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		return nil, fmt.Errorf(
			"connector-manager-client: failed to generate ed25519 key: %w",
			err)
	}

	credProvider, clientName, clientHost, err := ugrpc.NewC1LambdaCredentialProvider(ctx, privateKey, clientID, clientSecret)
	if err != nil {
		return nil, fmt.Errorf(
			"connector-manager-client: failed to create c1 lambda credential provider: %w",
			err)
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

	return &ConnectorConfigServiceClient{
		privateKey: privateKey,
		c:          pb_connector_manager.NewConnectorConfigServiceClient(client),
	}, nil
}
