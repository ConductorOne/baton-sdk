package match

import (
	"context"
	"testing"
	"time"

	"github.com/conductorone/baton-sdk/pkg/rtun/match/memory"
	"github.com/stretchr/testify/require"
)

func TestLocatorOwnerOfSingleServer(t *testing.T) {
	p := memory.NewPresence()
	ctx := context.Background()

	err := p.SetPorts(ctx, "client-1", []uint32{1})
	require.NoError(t, err)
	err = p.Announce(ctx, "client-1", "server-a", 10*time.Second)
	require.NoError(t, err)

	loc := &Locator{Presence: p}
	owner, ports, err := loc.OwnerOf(ctx, "client-1")
	require.NoError(t, err)
	require.Equal(t, "server-a", owner)
	require.Equal(t, []uint32{1}, ports)
}

func TestLocatorOwnerOfMultipleServersDeterministic(t *testing.T) {
	p := memory.NewPresence()
	ctx := context.Background()

	// Register two servers
	_ = p.SetPorts(ctx, "client-1", []uint32{1})
	_ = p.Announce(ctx, "client-1", "server-a", 10*time.Second)
	_ = p.Announce(ctx, "client-1", "server-b", 10*time.Second)

	loc := &Locator{Presence: p}
	owner1, _, err := loc.OwnerOf(ctx, "client-1")
	require.NoError(t, err)

	// Call again; should be same owner (deterministic rendezvous hashing)
	owner2, _, err := loc.OwnerOf(ctx, "client-1")
	require.NoError(t, err)
	require.Equal(t, owner1, owner2)

	// owner should be one of the two servers
	require.Contains(t, []string{"server-a", "server-b"}, owner1)
}

func TestLocatorOwnerOfClientOffline(t *testing.T) {
	p := memory.NewPresence()
	ctx := context.Background()

	loc := &Locator{Presence: p}
	_, _, err := loc.OwnerOf(ctx, "client-nonexistent")
	require.ErrorIs(t, err, ErrClientOffline)
}

func TestLocatorOwnerOfWithCustomChooser(t *testing.T) {
	// Removed: locator no longer accepts a custom chooser; keep surface minimal.
}

func TestLocatorOwnerOfTTLExpiry(t *testing.T) {
	p := memory.NewPresence()
	ctx := context.Background()

	// Set with very short TTL
	err := p.SetPorts(ctx, "client-1", []uint32{1})
	require.NoError(t, err)
	err = p.Announce(ctx, "client-1", "server-a", 10*time.Millisecond)
	require.NoError(t, err)

	loc := &Locator{Presence: p}
	owner, _, err := loc.OwnerOf(ctx, "client-1")
	require.NoError(t, err)
	require.Equal(t, "server-a", owner)

	// Wait for expiry
	time.Sleep(20 * time.Millisecond)

	// Now should return ErrClientOffline
	_, _, err = loc.OwnerOf(ctx, "client-1")
	require.ErrorIs(t, err, ErrClientOffline)
}
