package dotc1z_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
)

// TestStoreMetadata locks in the Reader.Metadata() contract on both
// engines. The Metadata() method is required by the connectorstore.Reader
// interface; every implementation must return non-zero values for
// Engine + Format when the store actually backs an on-disk c1z.
func TestStoreMetadata(t *testing.T) {
	ctx := context.Background()

	t.Run("SQLite C1File returns sqlite/v1", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "sqlite.c1z")
		c1z, err := dotc1z.NewC1ZFile(ctx, path)
		require.NoError(t, err)
		defer func() { _ = c1z.Close(ctx) }()

		md := c1z.Metadata()
		require.Equal(t, "sqlite", md.Engine)
		require.Equal(t, "v1", md.Format)
		require.Empty(t, md.PayloadEncoding,
			"v1 stores have no envelope payload encoding")
	})

	t.Run("Pebble adapter returns pebble/v3 with default encoding", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "pebble.c1z")
		store, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(dotc1z.EnginePebble))
		require.NoError(t, err)
		defer func() { _ = store.Close(ctx) }()

		md := storeMetadata(t, store)
		require.Equal(t, "pebble", md.Engine)
		require.Equal(t, "v3", md.Format)
		require.Equal(t, "indexed_zstd", md.PayloadEncoding,
			"default Pebble payload encoding is per-file indexed zstd")
	})

	t.Run("Pebble with explicit Tar encoding reports tar", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "pebble_tar.c1z")
		store, err := dotc1z.NewStore(ctx, path,
			dotc1z.WithEngine(dotc1z.EnginePebble),
			dotc1z.WithPayloadEncoding(dotc1z.PayloadEncodingTar),
		)
		require.NoError(t, err)
		defer func() { _ = store.Close(ctx) }()

		md := storeMetadata(t, store)
		require.Equal(t, "tar", md.PayloadEncoding)
	})

	t.Run("metadata is a cheap call — no I/O", func(t *testing.T) {
		// This is a hand-wave test: we just call it many times and
		// rely on go test to not time out. The contract documented
		// in connectorstore.StoreMetadata says implementations must
		// not perform I/O — this lets callers stamp it on hot
		// spans without worrying about latency.
		path := filepath.Join(t.TempDir(), "hot.c1z")
		c1z, err := dotc1z.NewC1ZFile(ctx, path)
		require.NoError(t, err)
		defer func() { _ = c1z.Close(ctx) }()

		for i := 0; i < 10_000; i++ {
			_ = c1z.Metadata()
		}
	})
}

// storeMetadata pulls the Metadata() result from any Writer (which
// embeds Reader). Helper just to keep the test bodies short.
func storeMetadata(t *testing.T, w connectorstore.Writer) connectorstore.StoreMetadata {
	t.Helper()
	return w.Metadata()
}
