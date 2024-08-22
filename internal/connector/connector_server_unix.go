//go:build !windows

package connector

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"

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
	listenerFile, err := listener.File()
	if err != nil {
		return 0, nil, err
	}

	l.Debug("listener started", zap.Uint32("listen_port", listenPort), zap.Uintptr("listener_fd", listenerFile.Fd()))

	return listenPort, listenerFile, nil
}

func (cw *wrapper) getListener(ctx context.Context, serverCfg *connectorwrapperV1.ServerConfig) (net.Listener, error) {
	l := ctxzap.Extract(ctx)

	l.Debug("starting listener with fd", zap.Uint32("expected_listen_port", serverCfg.ListenPort))

	listenerFd := os.Getenv(listenerFdEnv)
	if listenerFd == "" {
		return nil, fmt.Errorf("missing required listener fd")
	}

	fd, err := strconv.Atoi(listenerFd)
	if err != nil {
		return nil, fmt.Errorf("invalid listener fd: %w", err)
	}

	l.Debug("listener fd", zap.Int("fd", fd))

	listener, err := net.FileListener(os.NewFile(uintptr(fd), "listener"))
	if err != nil {
		return nil, err
	}

	listenPort := uint32(listener.Addr().(*net.TCPAddr).Port)
	if listenPort != serverCfg.ListenPort {
		return nil, fmt.Errorf("listen port mismatch: %d != %d", listenPort, serverCfg.ListenPort)
	}

	l.Debug("listener started", zap.Uint32("listen_port", listenPort))

	return listener, nil
}
