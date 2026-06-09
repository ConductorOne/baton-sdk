package dotc1z_test

import (
	"path/filepath"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/stretchr/testify/require"
)

// no_grants is a top-level snapshot like full: no parent allowed
func TestStartNewSyncNoGrants(t *testing.T) {
	ctx := t.Context()

	t.Run("parent-less succeeds", func(t *testing.T) {
		f, err := dotc1z.NewC1ZFile(ctx, filepath.Join(t.TempDir(), "test.c1z"))
		require.NoError(t, err)
		t.Cleanup(func() { _ = f.Close(ctx) })

		syncID, err := f.StartNewSync(ctx, connectorstore.SyncTypeNoGrants, "")
		require.NoError(t, err)
		require.NotEmpty(t, syncID)
	})

	t.Run("rejects a parent sync id", func(t *testing.T) {
		f, err := dotc1z.NewC1ZFile(ctx, filepath.Join(t.TempDir(), "test.c1z"))
		require.NoError(t, err)
		t.Cleanup(func() { _ = f.Close(ctx) })

		_, err = f.StartNewSync(ctx, connectorstore.SyncTypeNoGrants, "some-parent")
		require.Error(t, err)
	})
}
