package c1_manager

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"os"

	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/service_mode/v1"
	"github.com/conductorone/baton-sdk/pkg/sdk"
	"github.com/conductorone/baton-sdk/pkg/ugrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type c1ServiceClient struct {
	addr     string
	dialOpts []grpc.DialOption
}

func (c *c1ServiceClient) getClientConn(ctx context.Context) (v1.ConnectorWorkServiceClient, func() error, error) {
	cc, err := grpc.DialContext(
		ctx,
		c.addr,
		c.dialOpts...,
	)
	if err != nil {
		return nil, nil, err
	}
	return v1.NewConnectorWorkServiceClient(cc), func() error {
		return cc.Close()
	}, nil
}

func (c *c1ServiceClient) Hello(ctx context.Context, in *v1.HelloRequest, opts ...grpc.CallOption) (*v1.HelloResponse, error) {
	client, done, err := c.getClientConn(ctx)
	if err != nil {
		return nil, err
	}
	defer done()

	return client.Hello(ctx, in, opts...)
}

func (c *c1ServiceClient) GetTask(ctx context.Context, in *v1.GetTaskRequest, opts ...grpc.CallOption) (*v1.GetTaskResponse, error) {
	client, done, err := c.getClientConn(ctx)
	if err != nil {
		return nil, err
	}
	defer done()

	return client.GetTask(ctx, in, opts...)
}

func (c *c1ServiceClient) Heartbeat(ctx context.Context, in *v1.HeartbeatRequest, opts ...grpc.CallOption) (*v1.HeartbeatResponse, error) {
	client, done, err := c.getClientConn(ctx)
	if err != nil {
		return nil, err
	}
	defer done()

	return client.Heartbeat(ctx, in, opts...)
}

func (c *c1ServiceClient) FinishTask(ctx context.Context, in *v1.FinishTaskRequest, opts ...grpc.CallOption) (*v1.FinishTaskResponse, error) {
	client, done, err := c.getClientConn(ctx)
	if err != nil {
		return nil, err
	}
	defer done()

	return client.FinishTask(ctx, in, opts...)
}

func (c *c1ServiceClient) UploadAsset(ctx context.Context, opts ...grpc.CallOption) (v1.ConnectorWorkService_UploadAssetClient, error) {
	client, done, err := c.getClientConn(ctx)
	if err != nil {
		return nil, err
	}
	defer done()

	return client.UploadAsset(ctx, opts...)
}

func newServiceClient(ctx context.Context, clientID string, clientSecret string) (v1.ConnectorWorkServiceClient, error) {
	credProvider, clientName, tokenHost, err := ugrpc.NewC1CredentialProvider(clientID, clientSecret)
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

	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
			MinVersion: tls.VersionTLS12,
		})),
		grpc.WithPerRPCCredentials(credProvider),
		grpc.WithUserAgent(fmt.Sprintf("%s baton-sdk/%s", clientName, sdk.Version)),
		grpc.WithBlock(),
	}

	return &c1ServiceClient{
		addr:     addr,
		dialOpts: dialOpts,
	}, nil
}
