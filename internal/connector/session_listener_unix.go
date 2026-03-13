//go:build !windows

package connector

import (
	"context"
	"fmt"
	"net"
)

func (cw *wrapper) setupSessionListener(ctx context.Context) (uint32, net.Listener, error) {
	port, file, err := cw.setupListener(ctx)
	if err != nil {
		return 0, nil, err
	}
	if file == nil {
		return 0, nil, fmt.Errorf("session listener file is nil")
	}
	listener, err := net.FileListener(file)
	if err != nil {
		_ = file.Close()
		return 0, nil, fmt.Errorf("failed to create session listener from file: %w", err)
	}
	// Close the file descriptor now that the listener owns the socket.
	_ = file.Close()
	return port, listener, nil
}
