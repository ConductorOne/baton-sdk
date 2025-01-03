package connector

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"golang.org/x/net/http2/h2c"

	"golang.org/x/net/http2"

	"github.com/conductorone/baton-sdk/pb/c1/connector/v2/v2connect"
	connectorwrapperV1 "github.com/conductorone/baton-sdk/pb/c1/connector_wrapper/v1"
	ratelimitV1 "github.com/conductorone/baton-sdk/pb/c1/ratelimit/v1"
	"github.com/conductorone/baton-sdk/pb/c1/ratelimit/v1/v1connect"
	ratelimit2 "github.com/conductorone/baton-sdk/pkg/ratelimit"
	"github.com/conductorone/baton-sdk/pkg/types"
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

func (cw *wrapperConnect) Run(ctx context.Context, serverCfg *connectorwrapperV1.ServerConfig) error {
	ctxzap.Extract(ctx).Warn("connect enabled!")
	rl, err := ratelimit2.NewLimiter(ctx, cw.now, serverCfg.RateLimiterConfig)
	if err != nil {
		return err
	}
	cw.rateLimiter = rl

	server := NewConnectorConnectShim(cw.server, cw.rateLimiter, cw.opts.provisioningEnabled, cw.opts.ticketingEnabled)
	mux := http.NewServeMux()

	// Ratelimiter
	path, handler := v1connect.NewRateLimiterServiceHandler(server)
	mux.Handle(path, handler)

	// Connector
	path, handler = v2connect.NewConnectorServiceHandler(server)
	mux.Handle(path, handler)
	path, handler = v2connect.NewGrantsServiceHandler(server)
	mux.Handle(path, handler)
	path, handler = v2connect.NewEntitlementsServiceHandler(server)
	mux.Handle(path, handler)
	path, handler = v2connect.NewResourcesServiceHandler(server)
	mux.Handle(path, handler)
	path, handler = v2connect.NewResourceTypesServiceHandler(server)
	mux.Handle(path, handler)
	path, handler = v2connect.NewAssetServiceHandler(server)
	mux.Handle(path, handler)
	path, handler = v2connect.NewEventServiceHandler(server)
	mux.Handle(path, handler)

	// Tickets
	path, handler = v2connect.NewTicketsServiceHandler(server)
	mux.Handle(path, handler)

	// Provisioning
	path, handler = v2connect.NewGrantManagerServiceHandler(server)
	mux.Handle(path, handler)
	path, handler = v2connect.NewResourceManagerServiceHandler(server)
	mux.Handle(path, handler)
	path, handler = v2connect.NewAccountManagerServiceHandler(server)
	mux.Handle(path, handler)
	path, handler = v2connect.NewCredentialManagerServiceHandler(server)
	mux.Handle(path, handler)

	listenPort := fmt.Sprintf("%d", serverCfg.ListenPort)
	l, err := net.Listen("tcp", net.JoinHostPort("localhost", listenPort))
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
