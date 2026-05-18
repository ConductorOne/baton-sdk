package memory

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDirectoryResolve(t *testing.T) {
	d := NewDirectory()
	ctx := context.Background()

	// Initially not found
	_, err := d.Resolve(ctx, "server-a")
	require.ErrorIs(t, err, ErrServerNotFound)

	// Register server
	require.NoError(t, d.Advertise(ctx, "server-a", "127.0.0.1:5000", 10*time.Second))

	addr, err := d.Resolve(ctx, "server-a")
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1:5000", addr)
}

func TestDirectoryUnregister(t *testing.T) {
	d := NewDirectory()
	ctx := context.Background()

	require.NoError(t, d.Advertise(ctx, "server-a", "127.0.0.1:5000", 10*time.Second))
	addr, err := d.Resolve(ctx, "server-a")
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1:5000", addr)

	// Unregister
	require.NoError(t, d.Revoke(ctx, "server-a"))
	_, err = d.Resolve(ctx, "server-a")
	require.ErrorIs(t, err, ErrServerNotFound)
}

func TestDirectoryMultipleServersIsolation(t *testing.T) {
	d := NewDirectory()
	ctx := context.Background()

	require.NoError(t, d.Advertise(ctx, "server-a", "a.local:1", 10*time.Second))
	require.NoError(t, d.Advertise(ctx, "server-b", "b.local:2", 10*time.Second))

	addrA, _ := d.Resolve(ctx, "server-a")
	addrB, _ := d.Resolve(ctx, "server-b")

	require.Equal(t, "a.local:1", addrA)
	require.Equal(t, "b.local:2", addrB)
}
