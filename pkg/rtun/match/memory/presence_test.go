package memory

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPresenceSetOnlineGetLocations(t *testing.T) {
	p := NewPresence()
	ctx := context.Background()

	// Initially no locations
	locs, err := p.Locations(ctx, "client-1")
	require.NoError(t, err)
	require.Empty(t, locs)

	// Set ports and announce on server-a
	err = p.SetPorts(ctx, "client-1", []uint32{1})
	require.NoError(t, err)
	err = p.Announce(ctx, "client-1", "server-a", 10*time.Second)
	require.NoError(t, err)

	locs, err = p.Locations(ctx, "client-1")
	require.NoError(t, err)
	require.Len(t, locs, 1)
	require.Contains(t, locs, "server-a")

	// Add second server
	err = p.Announce(ctx, "client-1", "server-b", 10*time.Second)
	require.NoError(t, err)

	locs, err = p.Locations(ctx, "client-1")
	require.NoError(t, err)
	require.Len(t, locs, 2)
	require.Contains(t, locs, "server-a")
	require.Contains(t, locs, "server-b")
}

func TestPresenceSetOffline(t *testing.T) {
	p := NewPresence()
	ctx := context.Background()

	err := p.SetPorts(ctx, "client-1", []uint32{1})
	require.NoError(t, err)
	err = p.Announce(ctx, "client-1", "server-a", 10*time.Second)
	require.NoError(t, err)
	err = p.Announce(ctx, "client-1", "server-b", 10*time.Second)
	require.NoError(t, err)

	// Remove server-a
	err = p.Revoke(ctx, "client-1", "server-a")
	require.NoError(t, err)

	locs, err := p.Locations(ctx, "client-1")
	require.NoError(t, err)
	require.Len(t, locs, 1)
	require.Contains(t, locs, "server-b")

	// Remove server-b (last server); client should disappear
	err = p.Revoke(ctx, "client-1", "server-b")
	require.NoError(t, err)

	locs, err = p.Locations(ctx, "client-1")
	require.NoError(t, err)
	require.Empty(t, locs)

	// Ports should also be gone
	ports, err := p.Ports(ctx, "client-1")
	require.NoError(t, err)
	require.Empty(t, ports)
}

func TestPresenceGetPorts(t *testing.T) {
	p := NewPresence()
	ctx := context.Background()

	err := p.SetPorts(ctx, "client-1", []uint32{1, 2, 3})
	require.NoError(t, err)
	err = p.Announce(ctx, "client-1", "server-a", 10*time.Second)
	require.NoError(t, err)

	ports, err := p.Ports(ctx, "client-1")
	require.NoError(t, err)
	require.Equal(t, []uint32{1, 2, 3}, ports)

	// Update ports independently of leases
	err = p.SetPorts(ctx, "client-1", []uint32{5})
	require.NoError(t, err)

	ports, err = p.Ports(ctx, "client-1")
	require.NoError(t, err)
	require.Equal(t, []uint32{5}, ports)
}

func TestPresenceTTLExpiry(t *testing.T) {
	p := NewPresence()
	ctx := context.Background()

	// Set with very short TTL
	err := p.SetPorts(ctx, "client-1", []uint32{1})
	require.NoError(t, err)
	err = p.Announce(ctx, "client-1", "server-a", 10*time.Millisecond)
	require.NoError(t, err)

	locs, err := p.Locations(ctx, "client-1")
	require.NoError(t, err)
	require.Len(t, locs, 1)

	// Wait for expiry
	time.Sleep(20 * time.Millisecond)

	// GetLocations should prune expired and return empty
	locs, err = p.Locations(ctx, "client-1")
	require.NoError(t, err)
	require.Empty(t, locs)
}

func TestPresenceMultipleClientsIsolation(t *testing.T) {
	p := NewPresence()
	ctx := context.Background()

	err := p.SetPorts(ctx, "client-1", []uint32{1})
	require.NoError(t, err)
	err = p.Announce(ctx, "client-1", "server-a", 10*time.Second)
	require.NoError(t, err)
	err = p.SetPorts(ctx, "client-2", []uint32{2})
	require.NoError(t, err)
	err = p.Announce(ctx, "client-2", "server-b", 10*time.Second)
	require.NoError(t, err)

	locs1, _ := p.Locations(ctx, "client-1")
	locs2, _ := p.Locations(ctx, "client-2")

	require.Equal(t, []string{"server-a"}, locs1)
	require.Equal(t, []string{"server-b"}, locs2)
}
