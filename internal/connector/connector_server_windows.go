//go:build windows

package connector

import (
	"context"
	"net"
	"os"

	connectorwrapperV1 "github.com/conductorone/baton-sdk/pb/c1/connector_wrapper/v1"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

func (cw *wrapper) setupListener(ctx context.Context) (uint32, *os.File, error) {
	l := ctxzap.Extract(ctx)

	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, nil, err
	}

	listener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, nil, err
	}
	listenPort := uint32(listener.Addr().(*net.TCPAddr).Port)
	err = listener.Close()
	if err != nil {
		return 0, nil, err
	}

	l.Debug("listener port picked", zap.Uint32("listen_port", listenPort))

	return listenPort, nil, nil
}

func (cw *wrapper) getListener(ctx context.Context, serverCfg *connectorwrapperV1.ServerConfig) (net.Listener, error) {
	l := ctxzap.Extract(ctx)

	l.Debug("starting listener", zap.Uint32("port", serverCfg.ListenPort))

	listener, err := net.ListenTCP("tcp", &net.TCPAddr{Port: int(serverCfg.ListenPort)})
	if err != nil {
		return nil, err
	}

	l.Debug("listener started", zap.Uint32("port", serverCfg.ListenPort))

	return listener, nil
}
