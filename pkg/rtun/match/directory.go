package match

import (
	"context"
	"errors"
	"time"
)

// ErrNotImplemented is returned by placeholder implementations.
var ErrNotImplemented = errors.New("rtun/match: not implemented")

// Directory maps server IDs to dialable addresses, with TTL support.
type Directory interface {
	// Advertise publishes a server's address with a TTL.
	Advertise(ctx context.Context, serverID string, addr string, ttl time.Duration) error
	// Revoke removes a server's advertisement.
	Revoke(ctx context.Context, serverID string) error
	// Resolve returns the address for a server ID if present and not expired.
	Resolve(ctx context.Context, serverID string) (string, error)
}
