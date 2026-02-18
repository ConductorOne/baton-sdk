package match

import (
	"context"
	"time"
)

// Presence tracks per-(client, server) leases and a client's global ports.
//
// Semantics:
// - Announce/refresh a lease for (clientID, serverID) with a TTL.
// - Revoke removes a single server's lease for a client.
// - Locations returns only non-expired serverIDs.
// - SetPorts sets the client's ports (typically once at hello); Ports returns them.
type Presence interface {
	Announce(ctx context.Context, clientID string, serverID string, ttl time.Duration) error
	Revoke(ctx context.Context, clientID string, serverID string) error
	Locations(ctx context.Context, clientID string) ([]string, error)
	SetPorts(ctx context.Context, clientID string, ports []uint32) error
	Ports(ctx context.Context, clientID string) ([]uint32, error)
}
