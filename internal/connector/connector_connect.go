package connector

import (
	"context"
	"fmt"
	"net"
	"net/http"

	"sync"
	"time"

	"connectrpc.com/connect"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"

	"github.com/conductorone/baton-sdk/pb/c1/connector/v2/v2connect"
	connectorwrapperV1 "github.com/conductorone/baton-sdk/pb/c1/connector_wrapper/v1"
	ratelimitV1 "github.com/conductorone/baton-sdk/pb/c1/ratelimit/v1"
	"github.com/conductorone/baton-sdk/pb/c1/ratelimit/v1/v1connect"

	ratelimit2 "github.com/conductorone/baton-sdk/pkg/ratelimit"
	"github.com/conductorone/baton-sdk/pkg/types"
	"go.uber.org/zap"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

type wrapperConnect struct {
	mtx sync.RWMutex

	opts *wrapperOptions

	l net.Listener

	server      types.ConnectorServer
	client      types.ConnectorClient
	rateLimiter ratelimitV1.RateLimiterServiceServer

	now func() time.Time
}

// NewWrapperConnect returns a connector wrapper for running connector services locally.
func NewWrapperConnect(ctx context.Context, server interface{}, optfunc ...Option) (*wrapperConnect, error) {
	connectorServer, isServer := server.(types.ConnectorServer)
	if !isServer {
		return nil, ErrConnectorNotImplemented
	}

	w := &wrapperConnect{
		server: connectorServer,
		now:    time.Now,
		opts:   &wrapperOptions{},
	}

	for _, o := range optfunc {
		err := o(ctx, w.opts)
		if err != nil {
			return nil, err
		}
	}

	return w, nil
}

func LogInterceptor(lctx context.Context) connect.UnaryInterceptorFunc {
	l := ctxzap.Extract(lctx)
	interceptor := func(next connect.UnaryFunc) connect.UnaryFunc {
		return connect.UnaryFunc(func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			ctx = ctxzap.ToContext(ctx, l)
			l.Info("received request", zap.String("method", req.Spec().Procedure))
			return next(ctx, req)
		})
	}
	return connect.UnaryInterceptorFunc(interceptor)
}

func (cw *wrapperConnect) Run(ctx context.Context, serverCfg *connectorwrapperV1.ServerConfig) error {
	ctxzap.Extract(ctx).Warn("connect enabled!")

	interceptors := connect.WithInterceptors(LogInterceptor(ctx))

	rl, err := ratelimit2.NewLimiter(ctx, cw.now, serverCfg.RateLimiterConfig)
	if err != nil {
		return err
	}
	cw.rateLimiter = rl

	server := NewConnectorConnectShim(cw.server, cw.rateLimiter, cw.opts.provisioningEnabled, cw.opts.ticketingEnabled)
	mux := http.NewServeMux()

	// Ratelimiter
	path, handler := v1connect.NewRateLimiterServiceHandler(server, interceptors)
	mux.Handle(path, handler)

	// Connector
	path, handler = v2connect.NewConnectorServiceHandler(server, interceptors)
	mux.Handle(path, handler)
	path, handler = v2connect.NewGrantsServiceHandler(server, interceptors)
	mux.Handle(path, handler)
	path, handler = v2connect.NewEntitlementsServiceHandler(server, interceptors)
	mux.Handle(path, handler)
	path, handler = v2connect.NewResourcesServiceHandler(server, interceptors)
	mux.Handle(path, handler)
	path, handler = v2connect.NewResourceTypesServiceHandler(server, interceptors)
	mux.Handle(path, handler)
	path, handler = v2connect.NewAssetServiceHandler(server, interceptors)
	mux.Handle(path, handler)
	path, handler = v2connect.NewEventServiceHandler(server, interceptors)
	mux.Handle(path, handler)

	// Tickets
	path, handler = v2connect.NewTicketsServiceHandler(server, interceptors)
	mux.Handle(path, handler)

	// Provisioning
	path, handler = v2connect.NewGrantManagerServiceHandler(server, interceptors)
	mux.Handle(path, handler)
	path, handler = v2connect.NewResourceManagerServiceHandler(server, interceptors)
	mux.Handle(path, handler)
	path, handler = v2connect.NewAccountManagerServiceHandler(server, interceptors)
	mux.Handle(path, handler)
	path, handler = v2connect.NewCredentialManagerServiceHandler(server, interceptors)
	mux.Handle(path, handler)

	listenPort := fmt.Sprintf("%d", serverCfg.ListenPort)
	ctxzap.Extract(ctx).Warn("listening on ", zap.String("port", listenPort))
	l, err := net.Listen("tcp", net.JoinHostPort("0.0.0.0", listenPort))
	if err != nil {
		return fmt.Errorf("wrapper.Run: failed to listen: %w", err)
	}
	cw.l = l
	return http.Serve(l, h2c.NewHandler(mux, &http2.Server{}))
}

// C returns a ConnectorClient that the caller can use to interact with a locally running connector.
func (cw *wrapperConnect) C(ctx context.Context) (types.ConnectorClient, error) {
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

	// If we have an endpoint, don't launch the server.
	if cw.opts.connectEndpoint != "" {
		// If we have an endpoint, don't launch the server.
		cw.client = NewConnectorConnectClientShim(ctx, cw.opts.connectEndpoint)
		return cw.client, nil
	}

	go cw.Run(ctx, &connectorwrapperV1.ServerConfig{
		ListenPort: cw.opts.connectListenPort,
	})

	cw.client = NewConnectorConnectClientShim(ctx, fmt.Sprintf("http://localhost:%d", cw.opts.connectListenPort))
	return cw.client, nil
}

// Close shuts down the grpc server and closes the connection.
func (cw *wrapperConnect) Close() error {
	cw.mtx.Lock()
	defer cw.mtx.Unlock()

	var err error
	if cw.l != nil {
		err = cw.l.Close()
		if err != nil {
			return fmt.Errorf("error closing client connection: %w", err)
		}
		cw.l = nil
	}

	cw.client = nil
	cw.server = nil
	return nil
}
