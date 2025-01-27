package config

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"

	pb_connector_manager "github.com/ductone/c1-lambda/pb/c1/svc/connector_manager/v1"
)

func ConnectorManagerClient(ctx context.Context, endpoint string) (pb_connector_manager.ConnectorManagerClient, error) {
	systemCertPool, err := x509.SystemCertPool()
	if err != nil || systemCertPool == nil {
		return nil, fmt.Errorf("connector-manager-client: failed to load system cert pool: %v", err)
	}

	tlsConfig := &tls.Config{
		RootCAs: systemCertPool,
	}

	creds := credentials.NewTLS(tlsConfig)
	client, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(creds))

	if err != nil {
		return nil, fmt.Errorf("connector-manager-client: failed to create client: %w", err)
	}
	return pb_connector_manager.NewConnectorManagerClient(client), nil
}

func GetConnectorConfig(ctx context.Context, endpoint string, bearer string) (*pb_connector_manager.GetConnectorConfigResponse, error) {
	client, err := ConnectorManagerClient(ctx, endpoint)
	if err != nil {
		return nil, fmt.Errorf("get-connector-config: failed to create client: %w", err)
	}

	cfg, err := LoadDefaultAWSConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("get-connector-config: failed to load AWS config: %w", err)
	}

	signedReq, err := CreateSigv4STSGetCallerIdentityRequest(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("get-connector-config: failed to create SigV4 STS GetCallerIdentity request: %w", err)
	}

	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+bearer)
	return client.GetConnectorConfig(ctx, &pb_connector_manager.GetConnectorConfigRequest{
		Sigv4SignedRequestSTSGetCallerIdentity: signedReq,
	})
}
