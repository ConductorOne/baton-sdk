package dotc1z

import (
	"context"
	"os"
	"testing"

	enginepebble "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	"github.com/stretchr/testify/require"
)

func TestMigrateC1ZOnly(t *testing.T) {
	path := os.Getenv("BATON_MIGRATE_C1Z_PATH")
	if path == "" {
		t.Skip("BATON_MIGRATE_C1Z_PATH not set")
	}
	ctx := context.Background()
	store, err := NewStore(ctx, path)
	require.NoError(t, err)
	require.True(t, enginepebble.MarkStoreDirty(store))
	require.NoError(t, store.Close(ctx))
}
