package test

import (
	"context"
	"net"
	"os"
	"testing"

	connectorV2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/manager"
	"github.com/conductorone/baton-sdk/pkg/sync"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/conductorone/baton-sdk/pkg/ugrpc"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 1024 // 1MB buffer size for the in-memory connection

type inMemoryConnectorClient struct {
	connectorV2.ResourceTypesServiceClient
	connectorV2.ResourcesServiceClient
	connectorV2.ResourceGetterServiceClient
	connectorV2.EntitlementsServiceClient
	connectorV2.GrantsServiceClient
	connectorV2.ConnectorServiceClient
	connectorV2.AssetServiceClient
	connectorV2.GrantManagerServiceClient
	connectorV2.ResourceManagerServiceClient
	connectorV2.ResourceDeleterServiceClient
	connectorV2.AccountManagerServiceClient
	connectorV2.CredentialManagerServiceClient
	connectorV2.EventServiceClient
	connectorV2.TicketsServiceClient
	connectorV2.ActionServiceClient
}

type IntegrationTestWrapper struct {
	Client types.ConnectorClient
	// Does not expose the underlying syncer directly, but provides a method to perform synchronization.
	// sync.Syncer handle c1z file internally, will override the previous one when call Sync then Close
	syncer  sync.Syncer
	c1zPath string
	manager manager.Manager
}

func NewIntegrationTestWrapper(ctx context.Context, t *testing.T, connector interface{}) *IntegrationTestWrapper {
	srv, err := connectorbuilder.NewConnector(ctx, connector)
	require.NoError(t, err)

	tempPath, err := os.CreateTemp("", "baton-integration-test-*.c1z")
	require.NoError(t, err)

	err = tempPath.Close()
	require.NoError(t, err)

	t.Cleanup(func() {
		err := os.Remove(tempPath.Name())
		require.NoError(t, err)
	})

	lis := bufconn.Listen(bufSize)
	s := grpc.NewServer(
		grpc.Creds(insecure.NewCredentials()),
		grpc.ChainUnaryInterceptor(ugrpc.UnaryServerInterceptor(ctx)...),
		grpc.ChainStreamInterceptor(ugrpc.StreamServerInterceptors(ctx)...),
		grpc.StatsHandler(
			otelgrpc.NewServerHandler(
				otelgrpc.WithPropagators(
					propagation.NewCompositeTextMapPropagator(
						propagation.TraceContext{},
						propagation.Baggage{},
					),
				),
			),
		),
	)

	connectorV2.RegisterConnectorServiceServer(s, srv)
	connectorV2.RegisterGrantsServiceServer(s, srv)
	connectorV2.RegisterEntitlementsServiceServer(s, srv)
	connectorV2.RegisterResourcesServiceServer(s, srv)
	connectorV2.RegisterResourceTypesServiceServer(s, srv)
	connectorV2.RegisterAssetServiceServer(s, srv)
	connectorV2.RegisterEventServiceServer(s, srv)
	connectorV2.RegisterResourceGetterServiceServer(s, srv)
	connectorV2.RegisterGrantManagerServiceServer(s, srv)
	connectorV2.RegisterResourceManagerServiceServer(s, srv)
	connectorV2.RegisterResourceDeleterServiceServer(s, srv)
	connectorV2.RegisterAccountManagerServiceServer(s, srv)
	connectorV2.RegisterCredentialManagerServiceServer(s, srv)

	go func() {
		if err := s.Serve(lis); err != nil {
			t.Errorf("Server exited with error: %v", err)
		}
	}()

	t.Cleanup(func() {
		s.Stop()
	})

	bufDialer := func(ctx context.Context, s string) (net.Conn, error) {
		return lis.DialContext(ctx)
	}

	cc, err := grpc.NewClient(
		"passthrough://bufnet",
		grpc.WithContextDialer(bufDialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)

	client := &inMemoryConnectorClient{
		ResourceTypesServiceClient:     connectorV2.NewResourceTypesServiceClient(cc),
		ResourcesServiceClient:         connectorV2.NewResourcesServiceClient(cc),
		EntitlementsServiceClient:      connectorV2.NewEntitlementsServiceClient(cc),
		GrantsServiceClient:            connectorV2.NewGrantsServiceClient(cc),
		ConnectorServiceClient:         connectorV2.NewConnectorServiceClient(cc),
		AssetServiceClient:             connectorV2.NewAssetServiceClient(cc),
		GrantManagerServiceClient:      connectorV2.NewGrantManagerServiceClient(cc),
		ResourceManagerServiceClient:   connectorV2.NewResourceManagerServiceClient(cc),
		ResourceDeleterServiceClient:   connectorV2.NewResourceDeleterServiceClient(cc),
		AccountManagerServiceClient:    connectorV2.NewAccountManagerServiceClient(cc),
		CredentialManagerServiceClient: connectorV2.NewCredentialManagerServiceClient(cc),
		EventServiceClient:             connectorV2.NewEventServiceClient(cc),
		TicketsServiceClient:           connectorV2.NewTicketsServiceClient(cc),
		ActionServiceClient:            connectorV2.NewActionServiceClient(cc),
		ResourceGetterServiceClient:    connectorV2.NewResourceGetterServiceClient(cc),
	}

	syncer, err := sync.NewSyncer(
		ctx,
		client,
		sync.WithC1ZPath(tempPath.Name()),
		sync.WithProgressHandler(func(s *sync.Progress) {
			t.Logf("Progress: %v", s)
		}),
	)
	require.NoError(t, err)

	m, err := manager.New(ctx, tempPath.Name())
	require.NoError(t, err)

	t.Cleanup(func() {
		err := m.Close(ctx)
		require.NoError(t, err)
	})

	return &IntegrationTestWrapper{
		Client:  client,
		syncer:  syncer,
		manager: m,
		c1zPath: tempPath.Name(),
	}
}

func (w *IntegrationTestWrapper) Manager() manager.Manager {
	return w.manager
}

func (w *IntegrationTestWrapper) LoadC1Z(ctx context.Context, t *testing.T) *dotc1z.C1File {
	c1z, err := w.manager.LoadC1Z(ctx)
	require.NoError(t, err)
	require.NotNil(t, c1z)

	return c1z
}

// Sync performs a synchronization operation using the provided syncer.
func (w *IntegrationTestWrapper) Sync(ctx context.Context) error {
	err := w.syncer.Sync(ctx)
	if err != nil {
		return err
	}

	err = w.syncer.Close(ctx)
	if err != nil {
		return err
	}

	return nil
}
