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
	"strconv"
	"sync"
	"time"

	connectorV2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	connectorwrapperV1 "github.com/conductorone/baton-sdk/pb/c1/connector_wrapper/v1"
	ratelimitV1 "github.com/conductorone/baton-sdk/pb/c1/ratelimit/v1"
	tlsV1 "github.com/conductorone/baton-sdk/pb/c1/utls/v1"
	ratelimit2 "github.com/conductorone/baton-sdk/pkg/ratelimit"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/conductorone/baton-sdk/pkg/ugrpc"
	utls2 "github.com/conductorone/baton-sdk/pkg/utls"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/proto"
)

const listenerFdEnv = "C1_CONNECTOR_SERVICE_LISTENER_FD"

type connectorClient struct {
	connectorV2.ResourceTypesServiceClient
	connectorV2.ResourcesServiceClient
	connectorV2.EntitlementsServiceClient
	connectorV2.GrantsServiceClient
	connectorV2.ConnectorServiceClient
	connectorV2.AssetServiceClient
	ratelimitV1.RateLimiterClient
}

var ErrConnectorNotImplemented = errors.New("client does not implement connector connectorV2")

type wrapper struct {
	mtx sync.Mutex

	server types.ConnectorServer
	client types.ConnectorClient

	serverStdin io.WriteCloser
	conn        *grpc.ClientConn

	rateLimiter   ratelimitV1.RateLimiterServer
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
	listenerFd := os.Getenv(listenerFdEnv)
	if listenerFd == "" {
		return fmt.Errorf("missing required listener fd")
	}

	fd, err := strconv.Atoi(listenerFd)
	if err != nil {
		return fmt.Errorf("invalid listener fd: %w", err)
	}

	l, err := net.FileListener(os.NewFile(uintptr(fd), "listener"))
	if err != nil {
		return err
	}

	tlsConfig, err := utls2.ListenerConfig(ctx, serverCfg.Credential)
	if err != nil {
		return err
	}

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

	rl, err := ratelimit2.NewLimiter(ctx, cw.now, serverCfg.RateLimiterConfig)
	if err != nil {
		return err
	}
	cw.rateLimiter = rl

	ratelimitV1.RegisterRateLimiterServer(server, cw.rateLimiter)

	return server.Serve(l)
}

func (cw *wrapper) runServer(ctx context.Context, serverCred *tlsV1.Credential) (int32, error) {
	l := ctxzap.Extract(ctx)

	if cw.serverStdin != nil {
		return 0, fmt.Errorf("server is already running")
	}

	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}

	listener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	listenPort := int32(listener.Addr().(*net.TCPAddr).Port)
	listenerFile, err := listener.File()
	if err != nil {
		return 0, err
	}

	serverCfg, err := proto.Marshal(&connectorwrapperV1.ServerConfig{
		Credential:        serverCred,
		RateLimiterConfig: cw.rlCfg,
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

	cmd.ExtraFiles = []*os.File{listenerFile}
	cmd.Env = append(os.Environ(), fmt.Sprintf("%s=3", listenerFdEnv))

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
	if cw.client != nil {
		return cw.client, nil
	}

	cw.mtx.Lock()
	defer cw.mtx.Unlock()

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
		ResourceTypesServiceClient: connectorV2.NewResourceTypesServiceClient(cw.conn),
		ResourcesServiceClient:     connectorV2.NewResourcesServiceClient(cw.conn),
		EntitlementsServiceClient:  connectorV2.NewEntitlementsServiceClient(cw.conn),
		GrantsServiceClient:        connectorV2.NewGrantsServiceClient(cw.conn),
		ConnectorServiceClient:     connectorV2.NewConnectorServiceClient(cw.conn),
		AssetServiceClient:         connectorV2.NewAssetServiceClient(cw.conn),
		RateLimiterClient:          ratelimitV1.NewRateLimiterClient(cw.conn),
	}

	// cw.wrappedClient = newWrappedClient(ctx, cw)

	return cw.client, nil
}

// Close shuts down the grpc server and closes the connection.
func (cw *wrapper) Close() error {
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

	return nil
}
