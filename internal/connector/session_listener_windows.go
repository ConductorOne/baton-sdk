//go:build windows

package connector

import (
	"context"
	"net"
)

func (cw *wrapper) setupSessionListener(ctx context.Context) (uint32, net.Listener, error) {
	port, _, err := cw.setupListener(ctx)
	if err != nil {
		return 0, nil, err
	}
	// On Windows, setupListener picks a free port and closes the listener (since
	// Windows cannot pass file descriptors to child processes). For the session
	// store server, which runs in the parent process, we re-listen on the port.
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{
		IP:   net.IPv4(127, 0, 0, 1),
		Port: int(port),
	})
	if err != nil {
		return 0, nil, err
	}
	return port, listener, nil
}
