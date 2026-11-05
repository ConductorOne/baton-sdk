package dotc1z

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// TestStoreConnectorVersionGatesResume checks that WithConnectorVersion
// reaches the Pebble engine through NewStore and survives the c1z round trip:
// an unfinished sync saved by one version is resumed by the same version and
// replaced by a different one.
func TestStoreConnectorVersionGatesResume(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sync.c1z")

	store, err := NewStore(ctx, path, WithEngine(c1zstore.EnginePebble), WithConnectorVersion("v1"))
	require.NoError(t, err)
	oldID, newSync, err := store.StartOrResumeSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.True(t, newSync)
	require.NoError(t, store.PutGrants(ctx, mkV2Grant("g1", "ent", "user", "alice")))
	require.NoError(t, store.CheckpointSync(ctx, "token"))
	require.NoError(t, store.Close(ctx))

	store, err = NewStore(ctx, path, WithConnectorVersion("v1"))
	require.NoError(t, err)
	id, newSync, err := store.StartOrResumeSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.False(t, newSync, "same version must resume")
	require.Equal(t, oldID, id)
	require.NoError(t, store.Close(ctx))

	store, err = NewStore(ctx, path, WithConnectorVersion("v2"))
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close(ctx)) }()
	id, newSync, err = store.StartOrResumeSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.True(t, newSync, "a different version must start a new sync")
	require.NotEqual(t, oldID, id)
}
