package connector

import (
	"context"
	"net"
)

// setupSessionListener creates a listener for the in-process session store
// server. Unlike setupListener, no file descriptor is needed because the
// session store runs in the parent process rather than a child.
func (cw *wrapper) setupSessionListener(_ context.Context) (uint32, net.Listener, error) {
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{
		IP:   net.IPv4(127, 0, 0, 1),
		Port: 0,
	})
	if err != nil {
		return 0, nil, err
	}
	//nolint:gosec // No risk of overflow because Port is 16-bit.
	port := uint32(listener.Addr().(*net.TCPAddr).Port)
	return port, listener, nil
}
