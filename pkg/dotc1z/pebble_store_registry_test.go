package dotc1z

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	c1zv3 "github.com/conductorone/baton-sdk/pb/c1/c1z/v3"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	formatv3 "github.com/conductorone/baton-sdk/pkg/dotc1z/format/v3"
	"github.com/klauspost/compress/zstd"
)

func c1zFormat(path string) (C1ZFormat, error) {
	f, err := os.Open(path)
	if err != nil {
		return C1ZFormatUnknown, err
	}
	defer f.Close()
	return ReadHeaderFormat(f)
}

func TestRegisteredPebbleNewStoreRoundtrip(t *testing.T) {
	ctx := context.Background()

	path := t.TempDir() + "/sync.c1z"
	store, err := NewStore(ctx, path, WithEngine(EnginePebble))
	require.NoError(t, err)
	syncID, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, store.PutGrants(ctx, mkV2Grant("g1", "ent", "user", "alice")))
	require.NoError(t, store.EndSync(ctx))
	require.NoError(t, store.Close(ctx))

	format, err := c1zFormat(path)
	require.NoError(t, err)
	require.Equal(t, C1ZFormatV3, format, "format = %s, want v3", format)
	encodedBeforeReadOnly, err := os.ReadFile(path)
	require.NoError(t, err)
	env, err := formatv3.ReadEnvelope(bytes.NewReader(encodedBeforeReadOnly))
	require.NoError(t, err)
	require.Equal(t, PebbleManifestEngine, env.Manifest.GetEngine(), "manifest engine = %q, want %q", env.Manifest.GetEngine(), PebbleManifestEngine)
	require.Equal(t, uint32(pebble.SDKPebbleFormat), env.Manifest.GetEngineSchemaVersion(), "manifest schema = %d, want %d", env.Manifest.GetEngineSchemaVersion(), pebble.SDKPebbleFormat)
	require.Equal(
		t, c1zv3.PayloadEncoding_PAYLOAD_ENCODING_INDEXED_ZSTD, env.Manifest.GetPayloadEncoding(),
		"payload encoding = %v, want indexed_zstd (engine default)", env.Manifest.GetPayloadEncoding(),
	)
	descriptors := env.Manifest.GetDescriptors()
	require.NotNil(t, descriptors)
	require.NotEmpty(t, descriptors.GetFile(), "manifest descriptor closure is empty")
	require.NoError(t, env.Close())

	reopened, err := NewStore(ctx, path, WithReadOnly(true))
	require.NoError(t, err)
	require.NoError(t, reopened.Close(ctx))
	encodedAfterReadOnly, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, encodedBeforeReadOnly, encodedAfterReadOnly, "read-only open/close rewrote the c1z envelope")

	reopened, err = NewStore(ctx, path, WithReadOnly(true))
	require.NoError(t, err)
	defer func() { _ = reopened.Close(ctx) }()

	latest, ok := reopened.(connectorstore.LatestFinishedSyncIDFetcher)
	require.True(t, ok, "reopened store %T does not implement LatestFinishedSyncIDFetcher", reopened)
	gotLatest, err := latest.LatestFinishedSyncID(ctx, connectorstore.SyncTypeFull)
	require.NoError(t, err)
	require.Equal(t, syncID, gotLatest, "latest sync = %q, want %q", gotLatest, syncID)

	require.NoError(t, reopened.SetCurrentSync(ctx, syncID))
	resp, err := reopened.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, resp.GetList(), 1)
	require.Equal(t, "g1", resp.GetList()[0].GetId(), "ListGrants = %+v, want g1", resp.GetList())
	got, err := reopened.GetGrant(ctx, reader_v2.GrantsReaderServiceGetGrantRequest_builder{GrantId: "g1"}.Build())
	require.NoError(t, err)
	require.Equal(t, "g1", got.GetGrant().GetId(), "GetGrant id = %q, want g1", got.GetGrant().GetId())
}

// TestPebbleStoreWithPayloadEncodingTar exercises the
// WithPayloadEncoding option threaded through C1ZOption →
// StoreOptions → pebbleStore → manifest.PayloadEncoding.
func TestPebbleStoreWithPayloadEncodingTar(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	out := filepath.Join(tmp, "tar.c1z")
	store, err := NewStore(ctx, out,
		WithEngine(EnginePebble),
		WithPayloadEncoding(PayloadEncodingTar),
	)
	require.NoError(t, err)
	_, err = store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, store.PutGrants(ctx, &v2.Grant{Id: "g1"}))
	require.NoError(t, store.EndSync(ctx))
	require.NoError(t, store.Close(ctx))

	// Read the envelope back and confirm the manifest declares TAR.
	f, err := os.Open(out)
	require.NoError(t, err)
	defer f.Close()
	env, err := formatv3.ReadEnvelope(f)
	require.NoError(t, err)
	defer env.Close()
	require.Equal(t, c1zv3.PayloadEncoding_PAYLOAD_ENCODING_TAR, env.Manifest.GetPayloadEncoding(), "manifest payload_encoding: got %v, want TAR", env.Manifest.GetPayloadEncoding())
}

// TestPebbleRegisteredStoreDefaultsToIndexedZstd confirms that
// omitting dotc1z.WithPayloadEncoding gives INDEXED_ZSTD (the engine
// default: parallel decode at open, frame splicing at save).
func TestPebbleRegisteredStoreDefaultsToIndexedZstd(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	out := filepath.Join(tmp, "default.c1z")
	store, err := NewStore(ctx, out, WithEngine(EnginePebble))
	require.NoError(t, err)
	_, err = store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, store.PutGrants(ctx, &v2.Grant{Id: "g1"}))
	require.NoError(t, store.EndSync(ctx))
	require.NoError(t, store.Close(ctx))

	f, err := os.Open(out)
	require.NoError(t, err)
	defer f.Close()
	env, err := formatv3.ReadEnvelope(f)
	require.NoError(t, err)
	defer env.Close()
	require.Equal(t, c1zv3.PayloadEncoding_PAYLOAD_ENCODING_INDEXED_ZSTD, env.Manifest.GetPayloadEncoding(), "manifest payload_encoding: got %v, want INDEXED_ZSTD", env.Manifest.GetPayloadEncoding())
}

func TestPebbleStoreOpenHonorsDecoderMaxDecodedSize(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	out := filepath.Join(tmp, "indexed-limit.c1z")
	store, err := NewStore(ctx, out, WithEngine(EnginePebble))
	require.NoError(t, err)
	_, err = store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, store.PutGrants(ctx, &v2.Grant{Id: "g1"}))
	require.NoError(t, store.EndSync(ctx))
	require.NoError(t, store.Close(ctx))

	_, err = NewStore(ctx, out,
		WithReadOnly(true),
		WithDecoderOptions(WithDecoderMaxDecodedSize(1)),
	)
	require.ErrorIs(t, err, formatv3.ErrMaxSizeExceeded, "NewStore with tiny max decoded size error = %v, want ErrMaxSizeExceeded", err)
}

func TestPebbleStoreOpenHonorsDecoderMaxMemory(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	out := filepath.Join(tmp, "indexed-memory.c1z")
	store, err := NewStore(ctx, out, WithEngine(EnginePebble))
	require.NoError(t, err)
	_, err = store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, store.PutAsset(ctx, v2.AssetRef_builder{Id: "asset-1"}.Build(), "application/octet-stream", bytes.Repeat([]byte("0123456789abcdef"), 256<<10)))
	require.NoError(t, store.EndSync(ctx))
	require.NoError(t, store.Close(ctx))

	_, err = NewStore(ctx, out,
		WithReadOnly(true),
		WithDecoderOptions(WithDecoderMaxMemory(1)),
	)
	require.ErrorIs(t, err, zstd.ErrWindowSizeExceeded, "NewStore with tiny decoder memory error = %v, want zstd.ErrWindowSizeExceeded", err)
}
