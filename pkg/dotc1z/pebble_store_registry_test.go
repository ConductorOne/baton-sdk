package dotc1z

import (
	"bytes"
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	cpebble "github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	c1zv3 "github.com/conductorone/baton-sdk/pb/c1/c1z/v3"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/codec"
	formatv3 "github.com/conductorone/baton-sdk/pkg/dotc1z/format/v3"
	"github.com/klauspost/compress/zstd"
	"google.golang.org/protobuf/proto"
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
	wantGrantID := mkV2GrantID("ent", "user", "alice")
	require.Equal(t, wantGrantID, resp.GetList()[0].GetId(), "ListGrants = %+v, want %s", resp.GetList(), wantGrantID)
	got, err := reopened.GetGrant(ctx, reader_v2.GrantsReaderServiceGetGrantRequest_builder{GrantId: wantGrantID}.Build())
	require.NoError(t, err)
	require.Equal(t, wantGrantID, got.GetGrant().GetId(), "GetGrant id = %q, want %s", got.GetGrant().GetId(), wantGrantID)
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
	require.NoError(t, store.PutGrants(ctx, mkV2Grant("g1", "ent", "user", "alice")))
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
	require.NoError(t, store.PutGrants(ctx, mkV2Grant("g1", "ent", "user", "alice")))
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
	require.NoError(t, store.PutGrants(ctx, mkV2Grant("g1", "ent", "user", "alice")))
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

func TestReadOnlyPebbleOpenMigratesLegacyIDIndexTempCopy(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "legacy-id-index.c1z")
	syncID := "3Ft7w0w9IXoHtkQxjiIL28vgRwC"
	writeLegacyIDIndexPebbleC1Z(t, path, syncID)
	before, err := os.ReadFile(path)
	require.NoError(t, err)

	store, err := NewStore(ctx, path, WithReadOnly(true), WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close(ctx)) }()
	require.NoError(t, store.SetCurrentSync(ctx, syncID))

	resp, err := store.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{PageSize: 10}.Build())
	require.NoError(t, err)
	require.Len(t, resp.GetList(), 1)
	require.Equal(t, "group:g1:custom:member:user:u1", resp.GetList()[0].GetId())
	require.Equal(t, "group:g1:custom:member", resp.GetList()[0].GetEntitlement().GetId())

	_, err = store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.Error(t, err)

	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, before, after, "read-only temp migration must not rewrite the source c1z")
}

func writeLegacyIDIndexPebbleC1Z(t *testing.T, path string, syncID string) {
	t.Helper()
	payloadDir := filepath.Join(t.TempDir(), "payload")
	require.NoError(t, os.MkdirAll(payloadDir, 0o755))
	db, err := cpebble.Open(payloadDir, &cpebble.Options{Logger: discardTestPebbleLogger{}})
	require.NoError(t, err)

	var version [4]byte
	binary.BigEndian.PutUint32(version[:], 2)
	require.NoError(t, db.Set(testEngineMetaKey("keyspace_version"), version[:], cpebble.Sync))

	now := timestamppb.Now()
	ent := v3.EntitlementRecord_builder{
		ExternalId: "member",
		Resource: v3.ResourceRef_builder{
			ResourceTypeId: "group",
			ResourceId:     "g1",
		}.Build(),
		DiscoveredAt: now,
	}.Build()
	grant := v3.GrantRecord_builder{
		ExternalId: "g1",
		Entitlement: v3.EntitlementRef_builder{
			ResourceTypeId: "group",
			ResourceId:     "g1",
			EntitlementId:  "member",
		}.Build(),
		Principal: v3.PrincipalRef_builder{
			ResourceTypeId: "user",
			ResourceId:     "u1",
		}.Build(),
		DiscoveredAt: now,
	}.Build()
	run := v3.SyncRunRecord_builder{
		SyncId:  syncID,
		Type:    v3.SyncType_SYNC_TYPE_FULL,
		EndedAt: now,
	}.Build()

	require.NoError(t, db.Set(testLegacyEntitlementKey("member"), mustMarshalProto(t, ent), cpebble.Sync))
	require.NoError(t, db.Set(testLegacyGrantKey("g1"), mustMarshalProto(t, grant), cpebble.Sync))
	require.NoError(t, db.Set([]byte{0x03, 0x06}, mustMarshalProto(t, run), cpebble.Sync))
	require.NoError(t, db.Flush())
	require.NoError(t, db.Close())

	out, err := os.Create(path)
	require.NoError(t, err)
	defer out.Close()
	manifest := c1zv3.C1ZManifestV3_builder{
		Engine:              string(EnginePebble),
		EngineSchemaVersion: uint32(pebble.SDKPebbleFormat),
		PayloadEncoding:     c1zv3.PayloadEncoding_PAYLOAD_ENCODING_TAR_ZSTD,
	}.Build()
	require.NoError(t, formatv3.WriteEnvelope(out, manifest, payloadDir))
}

func testLegacyGrantKey(externalID string) []byte {
	buf := []byte{0x03, 0x04}
	buf = codec.AppendTupleSeparator(buf)
	return codec.AppendTupleStrings(buf, externalID)
}

func testLegacyEntitlementKey(externalID string) []byte {
	buf := []byte{0x03, 0x03}
	buf = codec.AppendTupleSeparator(buf)
	return codec.AppendTupleStrings(buf, externalID)
}

func testEngineMetaKey(name string) []byte {
	buf := []byte{0x03, 0xff}
	return codec.AppendTupleStrings(buf, name)
}

func mustMarshalProto(t *testing.T, msg proto.Message) []byte {
	t.Helper()
	out, err := proto.MarshalOptions{Deterministic: true}.Marshal(msg)
	require.NoError(t, err)
	return out
}

type discardTestPebbleLogger struct{}

func (discardTestPebbleLogger) Infof(string, ...interface{})  {}
func (discardTestPebbleLogger) Fatalf(string, ...interface{}) {}
func (discardTestPebbleLogger) Errorf(string, ...interface{}) {}
