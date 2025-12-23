package connector

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"sync"
	"time"

	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/proto"

	connectorV2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	connectorwrapperV1 "github.com/conductorone/baton-sdk/pb/c1/connector_wrapper/v1"
	ratelimitV1 "github.com/conductorone/baton-sdk/pb/c1/ratelimit/v1"
	tlsV1 "github.com/conductorone/baton-sdk/pb/c1/utls/v1"
	"github.com/conductorone/baton-sdk/pkg/bid"
	ratelimit2 "github.com/conductorone/baton-sdk/pkg/ratelimit"
	"github.com/conductorone/baton-sdk/pkg/session"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/conductorone/baton-sdk/pkg/types/sessions"
	"github.com/conductorone/baton-sdk/pkg/ugrpc"
	utls2 "github.com/conductorone/baton-sdk/pkg/utls"
)

const listenerFdEnv = "BATON_CONNECTOR_SERVICE_LISTENER_FD"

type connectorClient struct {
	connectorV2.ResourceTypesServiceClient
	connectorV2.ResourcesServiceClient
	connectorV2.ResourceGetterServiceClient
	connectorV2.EntitlementsServiceClient
	connectorV2.GrantsServiceClient
	connectorV2.ConnectorServiceClient
	connectorV2.AssetServiceClient
	ratelimitV1.RateLimiterServiceClient
	connectorV2.GrantManagerServiceClient
	connectorV2.ResourceManagerServiceClient
	connectorV2.ResourceDeleterServiceClient
	connectorV2.AccountManagerServiceClient
	connectorV2.CredentialManagerServiceClient
	connectorV2.EventServiceClient
	connectorV2.TicketsServiceClient
	connectorV2.ActionServiceClient

	sessionStoreSetter sessions.SetSessionStore // this is the session store server
}

var _ sessions.SetSessionStore = (*connectorClient)(nil)
var _ SetSessionStoreSetter = (*connectorClient)(nil)

type SetSessionStoreSetter interface {
	SetSessionStoreSetter(setsessionStoreSetter sessions.SetSessionStore)
}

func (c *connectorClient) SetSessionStoreSetter(sessionStoreSetter sessions.SetSessionStore) {
	c.sessionStoreSetter = sessionStoreSetter
}

func (c *connectorClient) SetSessionStore(ctx context.Context, store sessions.SessionStore) {
	if c.sessionStoreSetter == nil {
		l := ctxzap.Extract(ctx)
		l.Warn("connectorClient's session store is nil")
		return
	}
	c.sessionStoreSetter.SetSessionStore(ctx, store)
}

var ErrConnectorNotImplemented = errors.New("client does not implement connector connectorV2")

type wrapper struct {
	mtx sync.RWMutex

	server                types.ConnectorServer
	client                types.ConnectorClient
	serverStdin           io.WriteCloser
	conn                  *grpc.ClientConn
	provisioningEnabled   bool
	ticketingEnabled      bool
	fullSyncDisabled      bool
	targetedSyncResources []*connectorV2.Resource
	sessionStoreEnabled   bool
	syncResourceTypeIDs   []string

	rateLimiter   ratelimitV1.RateLimiterServiceServer
	rlCfg         *ratelimitV1.RateLimiterConfig
	rlDescriptors []*ratelimitV1.RateLimitDescriptors_Entry

	now func() time.Time

	SessionServer sessions.SetSessionStore
}

type Option func(ctx context.Context, w *wrapper) error

func WithSessionStoreEnabled() Option {
	return func(ctx context.Context, w *wrapper) error {
		w.sessionStoreEnabled = true
		return nil
	}
}

func WithRateLimiterConfig(cfg *ratelimitV1.RateLimiterConfig) Option {
	return func(ctx context.Context, w *wrapper) error {
		if cfg != nil {
			w.rlCfg = cfg
		}

		return nil
	}
}

func WithRateLimitDescriptor(entry *ratelimitV1.RateLimitDescriptors_Entry) Option {
	return func(ctx context.Context, w *wrapper) error {
		if entry != nil {
			w.rlDescriptors = append(w.rlDescriptors, entry)
		}

		return nil
	}
}

func WithProvisioningEnabled() Option {
	return func(ctx context.Context, w *wrapper) error {
		w.provisioningEnabled = true

		return nil
	}
}

func WithFullSyncDisabled() Option {
	return func(ctx context.Context, w *wrapper) error {
		w.fullSyncDisabled = true
		return nil
	}
}

func WithTicketingEnabled() Option {
	return func(ctx context.Context, w *wrapper) error {
		w.ticketingEnabled = true

		return nil
	}
}

func WithTargetedSyncResources(resourceIDs []string) Option {
	return func(ctx context.Context, w *wrapper) error {
		resources := make([]*connectorV2.Resource, 0, len(resourceIDs))
		for _, resourceId := range resourceIDs {
			r, err := bid.ParseResourceBid(resourceId)
			if err != nil {
				return err
			}
			resources = append(resources, r)
		}
		w.targetedSyncResources = resources
		return nil
	}
}

func WithSyncResourceTypeIDs(resourceTypeIDs []string) Option {
	return func(ctx context.Context, w *wrapper) error {
		w.syncResourceTypeIDs = resourceTypeIDs
		return nil
	}
}

// NewConnectorWrapper returns a connector wrapper for running connector services locally.
func NewWrapper(ctx context.Context, server interface{}, opts ...Option) (*wrapper, error) {
	connectorServer, isServer := server.(types.ConnectorServer)
	if !isServer {
		return nil, ErrConnectorNotImplemented
	}

	w := &wrapper{
		server: connectorServer,
		now:    time.Now,
	}

	for _, o := range opts {
		err := o(ctx, w)
		if err != nil {
			return nil, err
		}
	}

	return w, nil
}

func (cw *wrapper) Run(ctx context.Context, serverCfg *connectorwrapperV1.ServerConfig) error {
	logger := ctxzap.Extract(ctx)

	l, err := cw.getListener(ctx, serverCfg)
	if err != nil {
		return err
	}

	tlsConfig, err := utls2.ListenerConfig(ctx, serverCfg.GetCredential())
	if err != nil {
		return err
	}

	grpc_zap.ReplaceGrpcLoggerV2(logger)

	server := grpc.NewServer(
		grpc.Creds(credentials.NewTLS(tlsConfig)),
		grpc.ChainUnaryInterceptor(ugrpc.UnaryServerInterceptor(ctx)...),
		grpc.ChainStreamInterceptor(ugrpc.StreamServerInterceptors(ctx)...),
		grpc.StatsHandler(otelgrpc.NewServerHandler(
			otelgrpc.WithPropagators(
				propagation.NewCompositeTextMapPropagator(
					propagation.TraceContext{},
					propagation.Baggage{},
				),
			),
		)),
	)

	rl, err := ratelimit2.NewLimiter(ctx, cw.now, serverCfg.GetRateLimiterConfig())
	if err != nil {
		return err
	}
	cw.rateLimiter = rl
	opts := &RegisterOps{
		Ratelimiter:         cw.rateLimiter,
		ProvisioningEnabled: cw.provisioningEnabled,
		TicketingEnabled:    cw.ticketingEnabled,
	}
	Register(ctx, server, cw.server, opts)
	return server.Serve(l)
}

func (cw *wrapper) runServer(ctx context.Context, serverCred *tlsV1.Credential) (uint32, error) {
	l := ctxzap.Extract(ctx)

	if cw.serverStdin != nil {
		return 0, fmt.Errorf("server is already running")
	}

	listenPort, listener, err := cw.setupListener(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to setup listener: %w", err)
	}
	var sessionListenerPort uint32
	if cw.sessionStoreEnabled {
		var sessionListenerFile *os.File
		sessionListenerPort, sessionListenerFile, err = cw.setupListener(ctx)
		if err != nil {
			return 0, fmt.Errorf("failed to setup session listener: %w", err)
		}

		if sessionListenerFile == nil {
			return 0, fmt.Errorf("session listener file is nil")
		}

		// Start the session cache server on the cache listener
		sessionListener, err := net.FileListener(sessionListenerFile)
		if err != nil {
			_ = sessionListenerFile.Close()
			return 0, fmt.Errorf("failed to create session listener: %w", err)
		}
		tlsConfig, err := utls2.ListenerConfig(ctx, serverCred)
		if err != nil {
			_ = sessionListenerFile.Close()
			return 0, fmt.Errorf("failed to create session listener config: %w", err)
		}

		// TODO(kans): block until we send a request or something/error handling in general.
		l.Info("starting session store server")
		server := session.NewGRPCSessionServer()
		cw.SessionServer = server
		go func() {
			defer sessionListenerFile.Close()
			serverErr := session.StartGRPCSessionServerWithOptions(ctx, sessionListener, server,
				grpc.Creds(credentials.NewTLS(tlsConfig)),
				grpc.ChainUnaryInterceptor(ugrpc.UnaryServerInterceptor(ctx)...),
			)
			if serverErr != nil {
				l.Error("failed to create session store server", zap.Error(serverErr))
				return
			}
		}()
	}

	serverCfg, err := proto.Marshal(connectorwrapperV1.ServerConfig_builder{
		Credential:             serverCred,
		RateLimiterConfig:      cw.rlCfg,
		ListenPort:             listenPort,
		SessionStoreListenPort: sessionListenerPort,
	}.Build())
	if err != nil {
		return 0, err
	}

	// Pass all the arguments and append grpc to start the server
	// Config happens via flags, env, or a file. The environment and file should still be
	// available for the subprocess, so this ensures any manually passed flags are also passed
	args := os.Args[1:]
	args = append(args, "_connector-service")

	arg0, err := os.Executable()
	if err != nil {
		return 0, err
	}

	cmd := exec.CommandContext(ctx, arg0, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	// Make the server config available via stdin
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return 0, err
	}
	_, err = io.WriteString(stdin, base64.StdEncoding.EncodeToString(serverCfg)+"\n")
	if err != nil {
		return 0, err
	}
	cw.serverStdin = stdin

	if listener != nil {
		cmd.ExtraFiles = []*os.File{listener}
		cmd.Env = append(os.Environ(), fmt.Sprintf("%s=3", listenerFdEnv))
	}

	err = cmd.Start()
	if err != nil {
		return 0, err
	}

	go func() {
		waitErr := cmd.Wait()
		if waitErr != nil {
			l.Error("connector service quit unexpectedly", zap.Error(waitErr))
			waitErr = cw.Close()
			if waitErr != nil {
				l.Error("error closing connector wrapper", zap.Error(waitErr))
			}
			os.Exit(1)
		}
	}()

	return listenPort, nil
}

// C returns a ConnectorClient that the caller can use to interact with a locally running connector.
func (cw *wrapper) C(ctx context.Context) (types.ConnectorClient, error) {
	// Check to see if we have a client already
	cw.mtx.RLock()
	if cw.client != nil {
		cw.mtx.RUnlock()
		return cw.client, nil
	}
	cw.mtx.RUnlock()

	// No client, so lets create one
	cw.mtx.Lock()
	defer cw.mtx.Unlock()

	// We have the write lock now, so double check someone else didn't create a client for us.
	if cw.client != nil {
		return cw.client, nil
	}

	// If we don't have an active client, we need to start a sub process to run the server.
	// The subprocess will receive configuration via stdin in the form of a protobuf
	clientCred, serverCred, err := utls2.GenerateClientServerCredentials(ctx)
	if err != nil {
		return nil, err
	}
	clientTLSConfig, err := utls2.ClientConfig(ctx, clientCred)
	if err != nil {
		return nil, err
	}

	listenPort, err := cw.runServer(ctx, serverCred)
	if err != nil {
		return nil, err
	}

	// The server won't start up immediately, so we may need to retry connecting
	// This allows retrying connecting for 5 seconds every 500ms. Once initially
	// connected, grpc will handle retries for us.
	dialCtx, canc := context.WithTimeout(ctx, 5*time.Second)
	defer canc()
	var dialErr error
	var conn *grpc.ClientConn
	for {
		conn, err = grpc.DialContext( //nolint:staticcheck // grpc.DialContext is deprecated but we are using it still for compatibility
			ctx,
			fmt.Sprintf("127.0.0.1:%d", listenPort),
			grpc.WithTransportCredentials(credentials.NewTLS(clientTLSConfig)),
			grpc.WithBlock(), //nolint:staticcheck // grpc.WithBlock is deprecated but we are using it still for compatibility
			grpc.WithChainUnaryInterceptor(ratelimit2.UnaryInterceptor(cw.now, cw.rlDescriptors...)),
			grpc.WithStatsHandler(otelgrpc.NewClientHandler(
				otelgrpc.WithPropagators(
					propagation.NewCompositeTextMapPropagator(
						propagation.TraceContext{},
						propagation.Baggage{},
					),
				),
			)),
		)
		if err != nil {
			dialErr = err
			select {
			case <-time.After(time.Millisecond * 500):
			case <-dialCtx.Done():
				return nil, dialErr
			}
			continue
		}
		break
	}

	cw.conn = conn
	client := NewConnectorClient(ctx, cw.conn)
	client.SetSessionStoreSetter(cw.SessionServer)
	cw.client = client

	return client, nil
}

// Close shuts down the grpc server and closes the connection.
func (cw *wrapper) Close() error {
	cw.mtx.Lock()
	defer cw.mtx.Unlock()

	var err error
	if cw.conn != nil {
		err = cw.conn.Close()
		if err != nil {
			return fmt.Errorf("error closing client connection: %w", err)
		}
	}

	if cw.serverStdin != nil {
		err = cw.serverStdin.Close()
		if err != nil && errors.Is(err, os.ErrClosed) {
			return fmt.Errorf("error closing connector service stdin: %w", err)
		}
	}

	cw.client = nil
	cw.server = nil
	cw.serverStdin = nil
	cw.conn = nil

	return nil
}

type RegisterOps struct {
	Ratelimiter         ratelimitV1.RateLimiterServiceServer
	ProvisioningEnabled bool
	TicketingEnabled    bool
}

func Register(ctx context.Context, s grpc.ServiceRegistrar, srv types.ConnectorServer, opts *RegisterOps) {
	if opts == nil {
		opts = &RegisterOps{}
	}

	connectorV2.RegisterConnectorServiceServer(s, srv)
	connectorV2.RegisterGrantsServiceServer(s, srv)
	connectorV2.RegisterEntitlementsServiceServer(s, srv)
	connectorV2.RegisterResourcesServiceServer(s, srv)
	connectorV2.RegisterResourceTypesServiceServer(s, srv)
	connectorV2.RegisterAssetServiceServer(s, srv)
	connectorV2.RegisterEventServiceServer(s, srv)
	connectorV2.RegisterResourceGetterServiceServer(s, srv)

	if opts.TicketingEnabled {
		connectorV2.RegisterTicketsServiceServer(s, srv)
	} else {
		noop := &noopTicketing{}
		connectorV2.RegisterTicketsServiceServer(s, noop)
	}

	connectorV2.RegisterActionServiceServer(s, srv)

	if opts.ProvisioningEnabled {
		connectorV2.RegisterGrantManagerServiceServer(s, srv)
		connectorV2.RegisterResourceManagerServiceServer(s, srv)
		connectorV2.RegisterResourceDeleterServiceServer(s, srv)
		connectorV2.RegisterAccountManagerServiceServer(s, srv)
		connectorV2.RegisterCredentialManagerServiceServer(s, srv)
	} else {
		noop := &noopProvisioner{}
		connectorV2.RegisterGrantManagerServiceServer(s, noop)
		connectorV2.RegisterResourceManagerServiceServer(s, noop)
		connectorV2.RegisterResourceDeleterServiceServer(s, noop)
		connectorV2.RegisterAccountManagerServiceServer(s, noop)
		connectorV2.RegisterCredentialManagerServiceServer(s, noop)
	}

	if opts.Ratelimiter != nil {
		ratelimitV1.RegisterRateLimiterServiceServer(s, opts.Ratelimiter)
	}
}

// NewConnectorClient takes a grpc.ClientConnInterface and returns an implementation of the ConnectorClient interface.
// It does not check that the connection actually supports the services.
func NewConnectorClient(_ context.Context, cc grpc.ClientConnInterface) *connectorClient {
	return &connectorClient{
		ResourceTypesServiceClient:     connectorV2.NewResourceTypesServiceClient(cc),
		ResourcesServiceClient:         connectorV2.NewResourcesServiceClient(cc),
		EntitlementsServiceClient:      connectorV2.NewEntitlementsServiceClient(cc),
		GrantsServiceClient:            connectorV2.NewGrantsServiceClient(cc),
		ConnectorServiceClient:         connectorV2.NewConnectorServiceClient(cc),
		AssetServiceClient:             connectorV2.NewAssetServiceClient(cc),
		RateLimiterServiceClient:       ratelimitV1.NewRateLimiterServiceClient(cc),
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
}
