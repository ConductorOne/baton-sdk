package connector

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"time"

	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/proto"

	connectorV2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	connectorwrapperV1 "github.com/conductorone/baton-sdk/pb/c1/connector_wrapper/v1"
	ratelimitV1 "github.com/conductorone/baton-sdk/pb/c1/ratelimit/v1"
	tlsV1 "github.com/conductorone/baton-sdk/pb/c1/utls/v1"
	ratelimit2 "github.com/conductorone/baton-sdk/pkg/ratelimit"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/conductorone/baton-sdk/pkg/ugrpc"
	utls2 "github.com/conductorone/baton-sdk/pkg/utls"
)

const listenerFdEnv = "BATON_CONNECTOR_SERVICE_LISTENER_FD"

type connectorClient struct {
	connectorV2.ResourceTypesServiceClient
	connectorV2.ResourcesServiceClient
	connectorV2.EntitlementsServiceClient
	connectorV2.GrantsServiceClient
	connectorV2.ConnectorServiceClient
	connectorV2.AssetServiceClient
	ratelimitV1.RateLimiterServiceClient
	connectorV2.GrantManagerServiceClient
	connectorV2.ResourceManagerServiceClient
	connectorV2.AccountManagerServiceClient
	connectorV2.CredentialManagerServiceClient
	connectorV2.EventServiceClient
	connectorV2.TicketsServiceClient
}

var ErrConnectorNotImplemented = errors.New("client does not implement connector connectorV2")

type wrapper struct {
	mtx sync.RWMutex

	server              types.ConnectorServer
	client              types.ConnectorClient
	serverStdin         io.WriteCloser
	conn                *grpc.ClientConn
	provisioningEnabled bool
	ticketingEnabled    bool
	fullSyncDisabled    bool

	rateLimiter   ratelimitV1.RateLimiterServiceServer
	rlCfg         *ratelimitV1.RateLimiterConfig
	rlDescriptors []*ratelimitV1.RateLimitDescriptors_Entry

	now func() time.Time
}

type Option func(ctx context.Context, w *wrapper) error

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

	tlsConfig, err := utls2.ListenerConfig(ctx, serverCfg.Credential)
	if err != nil {
		return err
	}

	grpc_zap.ReplaceGrpcLoggerV2(logger)

	server := grpc.NewServer(
		grpc.Creds(credentials.NewTLS(tlsConfig)),
		grpc.ChainUnaryInterceptor(ugrpc.UnaryServerInterceptor(ctx)...),
		grpc.ChainStreamInterceptor(ugrpc.StreamServerInterceptors(ctx)...),
	)
	connectorV2.RegisterConnectorServiceServer(server, cw.server)
	connectorV2.RegisterGrantsServiceServer(server, cw.server)
	connectorV2.RegisterEntitlementsServiceServer(server, cw.server)
	connectorV2.RegisterResourcesServiceServer(server, cw.server)
	connectorV2.RegisterResourceTypesServiceServer(server, cw.server)
	connectorV2.RegisterAssetServiceServer(server, cw.server)
	connectorV2.RegisterEventServiceServer(server, cw.server)

	if cw.ticketingEnabled {
		connectorV2.RegisterTicketsServiceServer(server, cw.server)
	} else {
		noop := &noopTicketing{}
		connectorV2.RegisterTicketsServiceServer(server, noop)
	}

	if cw.provisioningEnabled {
		connectorV2.RegisterGrantManagerServiceServer(server, cw.server)
		connectorV2.RegisterResourceManagerServiceServer(server, cw.server)
		connectorV2.RegisterAccountManagerServiceServer(server, cw.server)
		connectorV2.RegisterCredentialManagerServiceServer(server, cw.server)
	} else {
		noop := &noopProvisioner{}
		connectorV2.RegisterGrantManagerServiceServer(server, noop)
		connectorV2.RegisterResourceManagerServiceServer(server, noop)
		connectorV2.RegisterAccountManagerServiceServer(server, noop)
		connectorV2.RegisterCredentialManagerServiceServer(server, noop)
	}

	rl, err := ratelimit2.NewLimiter(ctx, cw.now, serverCfg.RateLimiterConfig)
	if err != nil {
		return err
	}
	cw.rateLimiter = rl

	ratelimitV1.RegisterRateLimiterServiceServer(server, cw.rateLimiter)

	return server.Serve(l)
}

func (cw *wrapper) runServer(ctx context.Context, serverCred *tlsV1.Credential) (uint32, error) {
	l := ctxzap.Extract(ctx)

	if cw.serverStdin != nil {
		return 0, fmt.Errorf("server is already running")
	}

	listenPort, listener, err := cw.setupListener(ctx)
	if err != nil {
		return 0, err
	}

	serverCfg, err := proto.Marshal(&connectorwrapperV1.ServerConfig{
		Credential:        serverCred,
		RateLimiterConfig: cw.rlCfg,
		ListenPort:        listenPort,
	})
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
		conn, err = grpc.DialContext(
			ctx,
			fmt.Sprintf("127.0.0.1:%d", listenPort),
			grpc.WithTransportCredentials(credentials.NewTLS(clientTLSConfig)),
			grpc.WithBlock(),
			grpc.WithChainUnaryInterceptor(ratelimit2.UnaryInterceptor(cw.now, cw.rlDescriptors...)),
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

	cw.client = &connectorClient{
		ResourceTypesServiceClient:     connectorV2.NewResourceTypesServiceClient(cw.conn),
		ResourcesServiceClient:         connectorV2.NewResourcesServiceClient(cw.conn),
		EntitlementsServiceClient:      connectorV2.NewEntitlementsServiceClient(cw.conn),
		GrantsServiceClient:            connectorV2.NewGrantsServiceClient(cw.conn),
		ConnectorServiceClient:         connectorV2.NewConnectorServiceClient(cw.conn),
		AssetServiceClient:             connectorV2.NewAssetServiceClient(cw.conn),
		RateLimiterServiceClient:       ratelimitV1.NewRateLimiterServiceClient(cw.conn),
		GrantManagerServiceClient:      connectorV2.NewGrantManagerServiceClient(cw.conn),
		ResourceManagerServiceClient:   connectorV2.NewResourceManagerServiceClient(cw.conn),
		AccountManagerServiceClient:    connectorV2.NewAccountManagerServiceClient(cw.conn),
		CredentialManagerServiceClient: connectorV2.NewCredentialManagerServiceClient(cw.conn),
		EventServiceClient:             connectorV2.NewEventServiceClient(cw.conn),
		TicketsServiceClient:           connectorV2.NewTicketsServiceClient(cw.conn),
	}

	return cw.client, nil
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
		if err != nil {
			return fmt.Errorf("error closing connector service stdin: %w", err)
		}
	}

	cw.client = nil
	cw.server = nil
	cw.serverStdin = nil
	cw.conn = nil

	return nil
}
