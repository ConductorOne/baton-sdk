package sync

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

func TestSyncRejectsV1C1ZPathUnchanged(t *testing.T) {
	ctx := t.Context()
	src, err := os.ReadFile(filepath.Join("..", "dotc1z", "sqlite", "testdata", "finished_full.c1z"))
	require.NoError(t, err)

	for _, tc := range []struct {
		name string
		opts []SyncOpt
	}{
		{name: "default-engine"},
		{name: "explicit-pebble", opts: []SyncOpt{WithStorageEngine(c1zstore.EnginePebble)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			c1zPath := filepath.Join(dir, "sync.c1z")
			require.NoError(t, os.WriteFile(c1zPath, src, 0o600)) // #nosec G703 -- path is under t.TempDir().

			mc := newMockConnector()
			opts := append([]SyncOpt{WithC1ZPath(c1zPath), WithTmpDir(dir)}, tc.opts...)
			s, err := NewSyncer(ctx, mc, opts...)
			require.NoError(t, err)

			err = s.Sync(ctx)
			require.ErrorIs(t, err, dotc1z.ErrV1Unsupported)
			require.NoError(t, s.Close(ctx))

			after, err := os.ReadFile(c1zPath)
			require.NoError(t, err)
			require.Equal(t, src, after)
			entries, err := os.ReadDir(dir)
			require.NoError(t, err)
			require.Len(t, entries, 1)
		})
	}
}
