package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"context"
	"net"
	"path/filepath"
	native_sync "sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/session"
	"github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/conductorone/baton-sdk/pkg/types/sessions"
)

// integrationSessionStore is a minimal in-memory SessionStore backing.
type integrationSessionStore struct {
	mu   native_sync.Mutex
	data map[string][]byte
}

func (m *integrationSessionStore) Get(_ context.Context, key string, _ ...sessions.SessionStoreOption) ([]byte, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.data[key]
	return v, ok, nil
}

func (m *integrationSessionStore) GetMany(_ context.Context, keys []string, _ ...sessions.SessionStoreOption) (map[string][]byte, []string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := map[string][]byte{}
	for _, k := range keys {
		if v, ok := m.data[k]; ok {
			out[k] = v
		}
	}
	return out, nil, nil
}

func (m *integrationSessionStore) Set(_ context.Context, key string, value []byte, _ ...sessions.SessionStoreOption) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = value
	return nil
}

func (m *integrationSessionStore) SetMany(_ context.Context, values map[string][]byte, _ ...sessions.SessionStoreOption) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for k, v := range values {
		m.data[k] = v
	}
	return nil
}

func (m *integrationSessionStore) Delete(_ context.Context, key string, _ ...sessions.SessionStoreOption) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
	return nil
}

func (m *integrationSessionStore) Clear(_ context.Context, _ ...sessions.SessionStoreOption) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data = map[string][]byte{}
	return nil
}

func (m *integrationSessionStore) GetAll(_ context.Context, _ string, _ ...sessions.SessionStoreOption) (map[string][]byte, string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string][]byte, len(m.data))
	for k, v := range m.data {
		out[k] = v
	}
	return out, "", nil
}

// sessionIntegrationSyncer serves one resource type; its Grants op uses the
// session store the way a real connector would (write a cursor, read it
// back, probe a missing key).
type sessionIntegrationSyncer struct {
	resourceType *v2.ResourceType
	useSession   bool
}

func (s *sessionIntegrationSyncer) ResourceType(_ context.Context) *v2.ResourceType {
	return s.resourceType
}

func (s *sessionIntegrationSyncer) resource() *v2.Resource {
	return v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: s.resourceType.GetId(),
			Resource:     s.resourceType.GetId() + "-1",
		}.Build(),
		DisplayName: s.resourceType.GetDisplayName() + " One",
	}.Build()
}

func (s *sessionIntegrationSyncer) List(_ context.Context, _ *v2.ResourceId, _ resource.SyncOpAttrs) ([]*v2.Resource, *resource.SyncOpResults, error) {
	return []*v2.Resource{s.resource()}, &resource.SyncOpResults{}, nil
}

func (s *sessionIntegrationSyncer) Entitlements(_ context.Context, r *v2.Resource, _ resource.SyncOpAttrs) ([]*v2.Entitlement, *resource.SyncOpResults, error) {
	if !s.useSession {
		return nil, &resource.SyncOpResults{}, nil
	}
	return []*v2.Entitlement{
		v2.Entitlement_builder{
			Id:       "member",
			Resource: r,
			Purpose:  v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT,
		}.Build(),
	}, &resource.SyncOpResults{}, nil
}

func (s *sessionIntegrationSyncer) Grants(ctx context.Context, r *v2.Resource, opts resource.SyncOpAttrs) ([]*v2.Grant, *resource.SyncOpResults, error) {
	if !s.useSession {
		return nil, &resource.SyncOpResults{}, nil
	}
	if err := opts.Session.Set(ctx, "cursor", []byte("page-2")); err != nil {
		return nil, nil, err
	}
	if _, _, err := opts.Session.Get(ctx, "cursor"); err != nil {
		return nil, nil, err
	}
	if _, _, err := opts.Session.Get(ctx, "missing"); err != nil {
		return nil, nil, err
	}

	principal := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "user", Resource: "user-1"}.Build(),
	}.Build()
	member := v2.Entitlement_builder{
		Id:       "member",
		Resource: r,
		Purpose:  v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT,
	}.Build()
	grant := v2.Grant_builder{
		Id:          "repo-1-member-user-1",
		Entitlement: member,
		Principal:   principal,
	}.Build()
	return []*v2.Grant{grant}, &resource.SyncOpResults{}, nil
}

type sessionIntegrationConnector struct {
	syncers []connectorbuilder.ResourceSyncerV2
}

func (c *sessionIntegrationConnector) Metadata(_ context.Context) (*v2.ConnectorMetadata, error) {
	return v2.ConnectorMetadata_builder{DisplayName: "session-integration"}.Build(), nil
}

func (c *sessionIntegrationConnector) Validate(_ context.Context) (annotations.Annotations, error) {
	return nil, nil
}

func (c *sessionIntegrationConnector) ResourceSyncers(_ context.Context) []connectorbuilder.ResourceSyncerV2 {
	return c.syncers
}

// integrationConnectorClient satisfies types.ConnectorClient over a real
// gRPC connection.
type integrationConnectorClient struct {
	v2.ResourceTypesServiceClient
	v2.ResourcesServiceClient
	v2.EntitlementsServiceClient
	v2.GrantsServiceClient
	v2.ConnectorServiceClient
	v2.AssetServiceClient
	v2.GrantManagerServiceClient
	v2.ResourceManagerServiceClient
	v2.ResourceDeleterServiceClient
	v2.AccountManagerServiceClient
	v2.CredentialManagerServiceClient
	v2.EventServiceClient
	v2.TicketsServiceClient
	v2.ActionServiceClient
	v2.ResourceGetterServiceClient
}

func newIntegrationConnectorClient(conn grpc.ClientConnInterface) *integrationConnectorClient {
	return &integrationConnectorClient{
		ResourceTypesServiceClient:     v2.NewResourceTypesServiceClient(conn),
		ResourcesServiceClient:         v2.NewResourcesServiceClient(conn),
		EntitlementsServiceClient:      v2.NewEntitlementsServiceClient(conn),
		GrantsServiceClient:            v2.NewGrantsServiceClient(conn),
		ConnectorServiceClient:         v2.NewConnectorServiceClient(conn),
		AssetServiceClient:             v2.NewAssetServiceClient(conn),
		GrantManagerServiceClient:      v2.NewGrantManagerServiceClient(conn),
		ResourceManagerServiceClient:   v2.NewResourceManagerServiceClient(conn),
		ResourceDeleterServiceClient:   v2.NewResourceDeleterServiceClient(conn),
		AccountManagerServiceClient:    v2.NewAccountManagerServiceClient(conn),
		CredentialManagerServiceClient: v2.NewCredentialManagerServiceClient(conn),
		EventServiceClient:             v2.NewEventServiceClient(conn),
		TicketsServiceClient:           v2.NewTicketsServiceClient(conn),
		ActionServiceClient:            v2.NewActionServiceClient(conn),
		ResourceGetterServiceClient:    v2.NewResourceGetterServiceClient(conn),
	}
}

// TestSessionStatsEndToEndOverGRPC drives the full composition: a
// session-using connector behind connectorbuilder, served over a real gRPC
// hop, synced by the real syncer into a Pebble c1z — asserting the
// connector's session usage crosses the wire as response annotations and
// lands in the completed sync token.
func TestSessionStatsEndToEndOverGRPC(t *testing.T) {
	ctx := t.Context()
	tempDir := t.TempDir()

	backing := session.NewInstrumentedSessionStore(
		&integrationSessionStore{data: map[string][]byte{}},
		"connector_backend", "", nil,
	)
	server, err := connectorbuilder.NewConnector(ctx,
		&sessionIntegrationConnector{syncers: []connectorbuilder.ResourceSyncerV2{
			&sessionIntegrationSyncer{
				resourceType: v2.ResourceType_builder{Id: "repo", DisplayName: "Repo"}.Build(),
				useSession:   true,
			},
			&sessionIntegrationSyncer{
				resourceType: v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build(),
			},
		}},
		connectorbuilder.WithSessionStore(backing),
	)
	require.NoError(t, err)

	lis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	grpcServer := grpc.NewServer()
	v2.RegisterResourceTypesServiceServer(grpcServer, server)
	v2.RegisterResourcesServiceServer(grpcServer, server)
	v2.RegisterEntitlementsServiceServer(grpcServer, server)
	v2.RegisterGrantsServiceServer(grpcServer, server)
	v2.RegisterConnectorServiceServer(grpcServer, server)
	v2.RegisterAssetServiceServer(grpcServer, server)
	go func() { _ = grpcServer.Serve(lis) }()
	defer grpcServer.Stop()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	store, err := dotc1z.NewStore(ctx, filepath.Join(tempDir, "session-integration.c1z"),
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithTmpDir(tempDir),
	)
	require.NoError(t, err)

	syncer, err := NewSyncer(ctx, newIntegrationConnectorClient(conn),
		WithConnectorStore(store),
		WithTmpDir(tempDir),
	)
	require.NoError(t, err)
	require.NoError(t, syncer.Sync(ctx))

	latest, ok := store.(connectorstore.LatestFinishedSyncIDFetcher)
	require.True(t, ok)
	syncID, err := latest.LatestFinishedSyncID(ctx, connectorstore.SyncTypeFull)
	require.NoError(t, err)
	require.NoError(t, store.SetCurrentSync(ctx, syncID))
	token, err := store.CurrentSyncStep(ctx)
	require.NoError(t, err)
	require.NoError(t, syncer.Close(ctx))

	completedState := newState()
	require.NoError(t, completedState.Unmarshal(token))

	// The grant collection ran once (one repo resource): the connector's
	// session ops crossed the gRPC hop as a response annotation and were
	// folded into the token under connector.-prefixed ops.
	sessionStats := completedState.SessionStoreStats()
	require.EqualValues(t, 1, sessionStats["connector.set"].Count)
	// Two gets: the cursor read-back plus the missing-key probe. Misses are
	// not errors, so both land as clean ops.
	require.EqualValues(t, 2, sessionStats["connector.get"].Count)
	require.Zero(t, sessionStats["connector.get"].Errors)
	require.Zero(t, sessionStats["connector.get"].Timeouts)

	// Ordinary timing stats coexist with the session stats.
	require.Contains(t, completedState.StepDurations(), SyncGrantsOp.String())
	require.NotZero(t, completedState.ConnectorCallStats()["list-grants"].Count)
}
