package pebble_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	formatv3 "github.com/conductorone/baton-sdk/pkg/dotc1z/format/v3"
)

// TestIndexedPayloadStoreLifecycle covers the indexed encoding through
// the full store path: write a sync with
// WithPayloadEncoding(IndexedZstd), reopen the file WITHOUT specifying
// an encoding (the store must adopt indexed from the file), run a
// second sync, save again, and read it back. Single-sync contract: the
// second sync replaces the first; the assertion is that the encoding is
// preserved across the reopen+rewrite and the surviving sync's data is
// intact.
func TestIndexedPayloadStoreLifecycle(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "indexed.c1z")

	runSync := func(rtID string, opts ...dotc1z.C1ZOption) string {
		t.Helper()
		opts = append(opts, dotc1z.WithTmpDir(t.TempDir()))
		w, err := dotc1z.NewStore(ctx, path, opts...)
		require.NoError(t, err)
		syncID, err := w.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)
		require.NoError(t, w.PutResourceTypes(ctx, v2.ResourceType_builder{Id: rtID}.Build()))
		res := v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: rtID, Resource: rtID + "-1"}.Build(),
		}.Build()
		require.NoError(t, w.PutResources(ctx, res))
		require.NoError(t, w.EndSync(ctx))
		require.NoError(t, w.Close(ctx))
		return syncID
	}

	first := runSync("user", dotc1z.WithEngine(c1zstore.EnginePebble), dotc1z.WithPayloadEncoding(c1zstore.PayloadEncodingIndexedZstd))

	// The file on disk must carry the indexed encoding.
	requireFileEncoding := func() {
		t.Helper()
		f, err := os.Open(path)
		require.NoError(t, err)
		defer f.Close()
		m, err := formatv3.ReadManifestHeader(f)
		require.NoError(t, err)
		require.Equal(t, "PAYLOAD_ENCODING_INDEXED_ZSTD", m.GetPayloadEncoding().String(), "file payload encoding")
	}
	requireFileEncoding()

	// Reopen with NO explicit encoding: the store must adopt the
	// file's indexed encoding so the rewrite stays indexed (and the
	// splice path stays viable for future rewrites).
	second := runSync("group")
	requireFileEncoding()

	// Reopen read-only: the surviving (second) sync and its data must be
	// readable after the indexed rewrite; the replaced first sync is gone.
	w, err := dotc1z.NewStore(ctx, path, dotc1z.WithReadOnly(true), dotc1z.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	defer w.Close(ctx)
	eng, ok := pebble.AsEngine(w)
	require.Truef(t, ok, "not pebble: %T", w)
	_, err = eng.GetSyncRunRecord(ctx, second)
	require.NoErrorf(t, err, "second sync %s missing after indexed round trips", second)
	_, err = eng.GetSyncRunRecord(ctx, first)
	require.Errorf(t, err, "first sync %s should have been replaced by the second", first)
	_, err = eng.GetResourceRecord(ctx, "group", "group-1")
	require.NoErrorf(t, err, "second sync's resource missing")
	_, err = eng.GetResourceRecord(ctx, "user", "user-1")
	require.Errorf(t, err, "first sync's resource should be gone after replacement")
}
