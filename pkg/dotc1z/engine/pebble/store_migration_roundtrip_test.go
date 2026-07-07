package pebble_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	cpebble "github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	enginepkg "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	formatv3 "github.com/conductorone/baton-sdk/pkg/dotc1z/format/v3"
)

// writeLegacyEnvelope fabricates a complete pre-identity .c1z: a raw Pebble
// dir in the LEGACY keyspace layout (external-id keys, keyspace_version
// stamp, no id-index format stamp), wrapped in a v3 envelope whose manifest
// carries the LEGACY engine name ("pebble2") — exactly what a file written
// by the pre-identity SDK looks like on disk.
func writeLegacyEnvelope(t *testing.T, path string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "legacy-db")
	db, err := cpebble.Open(dir, &cpebble.Options{})
	require.NoError(t, err)

	set := func(key []byte, msg proto.Message) {
		val, merr := enginepkg.MarshalRecordForTest(msg)
		require.NoError(t, merr)
		require.NoError(t, db.Set(key, val, cpebble.Sync))
	}
	metaKey, metaVal := enginepkg.LegacyKeyspaceMetaForTest()
	require.NoError(t, db.Set(metaKey, metaVal, cpebble.Sync))
	set(enginepkg.SyncRunKeyForTest(), v3.SyncRunRecord_builder{
		SyncId:  "3Ft7w0w9IXoHtkQxjiIL28vgRwC",
		Type:    v3.SyncType_SYNC_TYPE_FULL,
		EndedAt: timestamppb.New(time.Unix(2000, 0).UTC()),
	}.Build())

	older := timestamppb.New(time.Unix(1000, 0).UTC())
	newer := timestamppb.New(time.Unix(2000, 0).UTC())
	set(enginepkg.LegacyEntitlementKeyForTest("member"), v3.EntitlementRecord_builder{
		ExternalId:   "member",
		Resource:     v3.ResourceRef_builder{ResourceTypeId: "group", ResourceId: "g1"}.Build(),
		DiscoveredAt: older,
	}.Build())
	mkGrant := func(ext string, at *timestamppb.Timestamp) *v3.GrantRecord {
		return v3.GrantRecord_builder{
			ExternalId: ext,
			Entitlement: v3.EntitlementRef_builder{
				ResourceTypeId: "group", ResourceId: "g1", EntitlementId: "member",
			}.Build(),
			Principal:    v3.PrincipalRef_builder{ResourceTypeId: "user", ResourceId: "u1"}.Build(),
			DiscoveredAt: at,
		}.Build()
	}
	// Two legacy rows, one structural identity: the migration folds them.
	set(enginepkg.LegacyGrantKeyForTest("dup-a"), mkGrant("dup-a", newer))
	set(enginepkg.LegacyGrantKeyForTest("dup-b"), mkGrant("dup-b", older))
	require.NoError(t, db.Close())

	manifest, err := enginepkg.BuildManifest(0)
	require.NoError(t, err)
	manifest.SetEngine("pebble2") // the legacy manifest name, verbatim

	out, err := os.Create(path)
	require.NoError(t, err)
	require.NoError(t, formatv3.WriteEnvelope(out, manifest, dir))
	require.NoError(t, out.Close())
}

// TestStoreLayerMigrationRoundtrip pins the production migration path the
// engine-layer semantics test cannot reach: a legacy .c1z ENVELOPE opened
// through dotc1z.NewStore. It must:
//
//   - serve a READ-ONLY open by migrating the unpacked temp copy WITHOUT
//     touching the original file (read-only consumers of old files keep
//     working; the on-disk bytes stay pre-migration);
//   - migrate on a writable open, mark the store dirty, and SAVE the
//     migrated layout back into the .c1z at Close (the migratedOnOpen →
//     dirty → save plumbing);
//   - rewrite the manifest engine name to the current fence ("pebble3")
//     so pre-identity readers reject the migrated file loudly;
//   - serve correct data on a read-only reopen with no re-migration.
func TestStoreLayerMigrationRoundtrip(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "legacy.c1z")
	writeLegacyEnvelope(t, path)
	syncID := "3Ft7w0w9IXoHtkQxjiIL28vgRwC"

	// Read-only open: served via a temp-copy migration; the ORIGINAL file
	// must remain byte-level pre-migration (manifest still "pebble2").
	roLegacy, err := dotc1z.NewStore(ctx, path, dotc1z.WithReadOnly(true))
	require.NoError(t, err, "read-only open of a legacy c1z must work (temp-copy migration)")
	require.NoError(t, roLegacy.SetCurrentSync(ctx, syncID))
	roLegacyResp, err := roLegacy.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, roLegacyResp.GetList(), 1, "read-only legacy open must serve migrated (folded) data")
	require.NoError(t, roLegacy.Close(ctx))
	f0, err := os.Open(path)
	require.NoError(t, err)
	m0, err := formatv3.ReadManifestHeader(f0)
	require.NoError(t, f0.Close())
	require.NoError(t, err)
	require.Equal(t, "pebble2", m0.GetEngine(),
		"a read-only open must not rewrite the original file")

	// Writable open migrates in place.
	store, err := dotc1z.NewStore(ctx, path)
	require.NoError(t, err, "writable open must migrate the legacy layout")
	require.NoError(t, store.SetCurrentSync(ctx, syncID))
	resp, err := store.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, resp.GetList(), 1, "duplicate-identity legacy rows must fold to one")
	require.Equal(t, "dup-b", resp.GetList()[0].GetId(), "fold keeps the earliest-discovered external id, byte-identical")

	// Close must SAVE the migrated layout back (migratedOnOpen marks dirty).
	require.NoError(t, store.Close(ctx))

	// The saved envelope's manifest must carry the current engine fence.
	f, err := os.Open(path)
	require.NoError(t, err)
	manifest, err := formatv3.ReadManifestHeader(f)
	require.NoError(t, f.Close())
	require.NoError(t, err)
	require.Equal(t, "pebble3", manifest.GetEngine(),
		"migrated file must be re-fenced so pre-identity readers reject it loudly")

	// Read-only reopen: current layout, no migration needed, data intact.
	ro, err := dotc1z.NewStore(ctx, path, dotc1z.WithReadOnly(true))
	require.NoError(t, err, "read-only open after migration must succeed")
	defer func() { require.NoError(t, ro.Close(ctx)) }()
	require.NoError(t, ro.SetCurrentSync(ctx, syncID))
	roResp, err := ro.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, roResp.GetList(), 1)
	require.Equal(t, "dup-b", roResp.GetList()[0].GetId())
}
