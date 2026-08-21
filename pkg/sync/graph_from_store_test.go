package sync //nolint:revive,nolintlint // package name kept for compatibility

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/sync/expand"
)

// TestGraphFromStore: a graph blob written to a Pebble c1z round-trips out via
// GraphFromStore; a mismatched sync id or missing blob yields nil.
func TestGraphFromStore(t *testing.T) {
	ctx := context.Background()

	store, err := dotc1z.NewStore(ctx, filepath.Join(t.TempDir(), "g.c1z"),
		dotc1z.WithEngine(c1zstore.EnginePebble), dotc1z.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	defer store.Close(ctx)
	syncID, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, store.EndSync(ctx))

	// No blob yet -> nil, no error.
	got, err := GraphFromStore(ctx, store, syncID)
	require.NoError(t, err)
	require.Nil(t, got)

	g := expand.NewEntitlementGraph(ctx)
	g.AddEntitlementID("ent-a")
	g.AddEntitlementID("ent-b")
	require.NoError(t, g.AddEdge(ctx, "ent-a", "ent-b", false, nil))
	digestReader, ok := store.(c1zstore.GrantGenerationDigestReader)
	require.True(t, ok)
	digest, found, err := digestReader.GrantGenerationDigest(ctx)
	require.NoError(t, err)
	require.True(t, found)
	data, err := expand.MarshalGraphBlobWithGrantDigest(syncID, g, digest)
	require.NoError(t, err)
	gs, ok := store.(EntitlementGraphStore)
	require.True(t, ok, "pebble store must implement EntitlementGraphStore")
	require.NoError(t, gs.PutEntitlementGraphBlob(ctx, data))

	got, err = GraphFromStore(ctx, store, syncID)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.NotNil(t, got.GetNode("ent-a"))
	require.Len(t, got.Edges, 1)

	// Wrong sync id -> nil (stale sidecar guard).
	got, err = GraphFromStore(ctx, store, "some-other-sync")
	require.NoError(t, err)
	require.Nil(t, got)

	// A structurally valid graph bound to a different grant generation must
	// fail closed.
	wrongDigest := digest
	wrongDigest.Hash = append([]byte(nil), digest.Hash...)
	wrongDigest.Hash[0] ^= 0xff
	wrongData, err := expand.MarshalGraphBlobWithGrantDigest(syncID, g, wrongDigest)
	require.NoError(t, err)
	require.NoError(t, gs.PutEntitlementGraphBlob(ctx, wrongData))
	got, err = GraphFromStore(ctx, store, syncID)
	require.NoError(t, err)
	require.Nil(t, got)

	// Current-format but unbound blobs are also not reusable.
	unboundData, err := expand.MarshalGraphBlob(syncID, g)
	require.NoError(t, err)
	require.NoError(t, gs.PutEntitlementGraphBlob(ctx, unboundData))
	got, err = GraphFromStore(ctx, store, syncID)
	require.NoError(t, err)
	require.Nil(t, got)

	// StartNewSync resets the keyspace, wiping the sidecar.
	_, err = store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	got, err = GraphFromStore(ctx, store, "")
	require.NoError(t, err)
	require.Nil(t, got, "new sync must not inherit the prior sync's graph sidecar")
}

// TestGraphFromStore_SQLiteUnsupported: SQLite has no sidecar; nil, no error.
func TestGraphFromStore_SQLiteUnsupported(t *testing.T) {
	ctx := context.Background()
	store, err := dotc1z.NewStore(ctx, filepath.Join(t.TempDir(), "g.c1z"), dotc1z.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	defer store.Close(ctx)

	got, err := GraphFromStore(ctx, store, "any")
	require.NoError(t, err)
	require.Nil(t, got)
}
