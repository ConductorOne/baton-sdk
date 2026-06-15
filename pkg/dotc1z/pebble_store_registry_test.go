package dotc1z

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

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
	if err != nil {
		t.Fatalf("NewStore pebble: %v", err)
	}
	syncID, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	if err != nil {
		t.Fatalf("StartNewSync: %v", err)
	}
	if err := store.PutGrants(ctx, mkV2Grant("g1", "ent", "user", "alice")); err != nil {
		t.Fatalf("PutGrants: %v", err)
	}
	if err := store.EndSync(ctx); err != nil {
		t.Fatalf("EndSync: %v", err)
	}
	if err := store.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	format, err := c1zFormat(path)
	if err != nil {
		t.Fatalf("c1zFormat: %v", err)
	}
	if format != C1ZFormatV3 {
		t.Fatalf("format = %s, want v3", format)
	}
	encodedBeforeReadOnly, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read encoded c1z: %v", err)
	}
	env, err := formatv3.ReadEnvelope(bytes.NewReader(encodedBeforeReadOnly))
	if err != nil {
		t.Fatalf("ReadEnvelope: %v", err)
	}
	if env.Manifest.GetEngine() != PebbleManifestEngine {
		t.Fatalf("manifest engine = %q, want %q", env.Manifest.GetEngine(), PebbleManifestEngine)
	}
	if env.Manifest.GetEngineSchemaVersion() != uint32(pebble.SDKPebbleFormat) {
		t.Fatalf("manifest schema = %d, want %d", env.Manifest.GetEngineSchemaVersion(), pebble.SDKPebbleFormat)
	}
	if env.Manifest.GetPayloadEncoding() != c1zv3.PayloadEncoding_PAYLOAD_ENCODING_INDEXED_ZSTD {
		t.Fatalf("payload encoding = %v, want indexed_zstd (engine default)", env.Manifest.GetPayloadEncoding())
	}
	if env.Manifest.GetDescriptors() == nil || len(env.Manifest.GetDescriptors().GetFile()) == 0 {
		t.Fatal("manifest descriptor closure is empty")
	}
	if err := env.Close(); err != nil {
		t.Fatalf("close envelope: %v", err)
	}

	reopened, err := NewStore(ctx, path, WithReadOnly(true))
	if err != nil {
		t.Fatalf("NewStore reopen pebble: %v", err)
	}
	if err := reopened.Close(ctx); err != nil {
		t.Fatalf("read-only close: %v", err)
	}
	encodedAfterReadOnly, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read c1z after read-only close: %v", err)
	}
	if !bytes.Equal(encodedAfterReadOnly, encodedBeforeReadOnly) {
		t.Fatal("read-only open/close rewrote the c1z envelope")
	}

	reopened, err = NewStore(ctx, path, WithReadOnly(true))
	if err != nil {
		t.Fatalf("NewStore reopen pebble after read-only check: %v", err)
	}
	defer func() { _ = reopened.Close(ctx) }()

	latest, ok := reopened.(connectorstore.LatestFinishedSyncIDFetcher)
	if !ok {
		t.Fatalf("reopened store %T does not implement LatestFinishedSyncIDFetcher", reopened)
	}
	gotLatest, err := latest.LatestFinishedSyncID(ctx, connectorstore.SyncTypeFull)
	if err != nil {
		t.Fatalf("LatestFinishedSyncID: %v", err)
	}
	if gotLatest != syncID {
		t.Fatalf("latest sync = %q, want %q", gotLatest, syncID)
	}

	if err := reopened.SetCurrentSync(ctx, syncID); err != nil {
		t.Fatalf("SetCurrentSync: %v", err)
	}
	resp, err := reopened.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
	if err != nil {
		t.Fatalf("ListGrants: %v", err)
	}
	if len(resp.GetList()) != 1 || resp.GetList()[0].GetId() != "g1" {
		t.Fatalf("ListGrants = %+v, want g1", resp.GetList())
	}
	got, err := reopened.GetGrant(ctx, reader_v2.GrantsReaderServiceGetGrantRequest_builder{GrantId: "g1"}.Build())
	if err != nil {
		t.Fatalf("GetGrant: %v", err)
	}
	if got.GetGrant().GetId() != "g1" {
		t.Fatalf("GetGrant id = %q, want g1", got.GetGrant().GetId())
	}
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
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	if _, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, ""); err != nil {
		t.Fatalf("StartNewSync: %v", err)
	}
	if err := store.PutGrants(ctx, &v2.Grant{Id: "g1"}); err != nil {
		t.Fatalf("PutGrants: %v", err)
	}
	if err := store.EndSync(ctx); err != nil {
		t.Fatalf("EndSync: %v", err)
	}
	if err := store.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Read the envelope back and confirm the manifest declares TAR.
	f, err := os.Open(out)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	env, err := formatv3.ReadEnvelope(f)
	if err != nil {
		t.Fatalf("ReadEnvelope: %v", err)
	}
	defer env.Close()
	if env.Manifest.GetPayloadEncoding() != c1zv3.PayloadEncoding_PAYLOAD_ENCODING_TAR {
		t.Fatalf("manifest payload_encoding: got %v, want TAR", env.Manifest.GetPayloadEncoding())
	}
}

// TestPebbleRegisteredStoreDefaultsToIndexedZstd confirms that
// omitting dotc1z.WithPayloadEncoding gives INDEXED_ZSTD (the engine
// default: parallel decode at open, frame splicing at save).
func TestPebbleRegisteredStoreDefaultsToIndexedZstd(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	out := filepath.Join(tmp, "default.c1z")
	store, err := NewStore(ctx, out, WithEngine(EnginePebble))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	if _, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, ""); err != nil {
		t.Fatalf("StartNewSync: %v", err)
	}
	if err := store.PutGrants(ctx, &v2.Grant{Id: "g1"}); err != nil {
		t.Fatalf("PutGrants: %v", err)
	}
	if err := store.EndSync(ctx); err != nil {
		t.Fatalf("EndSync: %v", err)
	}
	if err := store.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	f, err := os.Open(out)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	env, err := formatv3.ReadEnvelope(f)
	if err != nil {
		t.Fatalf("ReadEnvelope: %v", err)
	}
	defer env.Close()
	if env.Manifest.GetPayloadEncoding() != c1zv3.PayloadEncoding_PAYLOAD_ENCODING_INDEXED_ZSTD {
		t.Fatalf("manifest payload_encoding: got %v, want INDEXED_ZSTD", env.Manifest.GetPayloadEncoding())
	}
}

func TestPebbleStoreOpenHonorsDecoderMaxDecodedSize(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	out := filepath.Join(tmp, "indexed-limit.c1z")
	store, err := NewStore(ctx, out, WithEngine(EnginePebble))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	if _, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, ""); err != nil {
		t.Fatalf("StartNewSync: %v", err)
	}
	if err := store.PutGrants(ctx, &v2.Grant{Id: "g1"}); err != nil {
		t.Fatalf("PutGrants: %v", err)
	}
	if err := store.EndSync(ctx); err != nil {
		t.Fatalf("EndSync: %v", err)
	}
	if err := store.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	_, err = NewStore(ctx, out,
		WithReadOnly(true),
		WithDecoderOptions(WithDecoderMaxDecodedSize(1)),
	)
	if !errors.Is(err, formatv3.ErrMaxSizeExceeded) {
		t.Fatalf("NewStore with tiny max decoded size error = %v, want ErrMaxSizeExceeded", err)
	}
}

func TestPebbleStoreOpenHonorsDecoderMaxMemory(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	out := filepath.Join(tmp, "indexed-memory.c1z")
	store, err := NewStore(ctx, out, WithEngine(EnginePebble))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	if _, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, ""); err != nil {
		t.Fatalf("StartNewSync: %v", err)
	}
	if err := store.PutAsset(ctx, v2.AssetRef_builder{Id: "asset-1"}.Build(), "application/octet-stream", bytes.Repeat([]byte("0123456789abcdef"), 256<<10)); err != nil {
		t.Fatalf("PutAsset: %v", err)
	}
	if err := store.EndSync(ctx); err != nil {
		t.Fatalf("EndSync: %v", err)
	}
	if err := store.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	_, err = NewStore(ctx, out,
		WithReadOnly(true),
		WithDecoderOptions(WithDecoderMaxMemory(1)),
	)
	if !errors.Is(err, zstd.ErrWindowSizeExceeded) {
		t.Fatalf("NewStore with tiny decoder memory error = %v, want zstd.ErrWindowSizeExceeded", err)
	}
}
