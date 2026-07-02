package pebble

import (
	"context"
	"encoding/binary"
	"path/filepath"
	"testing"
	"time"

	cpebble "github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/codec"
)

// legacyIDIndexDB writes a raw pebble dir in the LEGACY id-index layout
// (records keyed by external id, no format stamp) so Open's in-place
// migration path runs against it. Returns the db dir.
func legacyIDIndexDB(t *testing.T, build func(t *testing.T, set func(key []byte, msg proto.Message))) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "db")
	db, err := cpebble.Open(dir, &cpebble.Options{})
	require.NoError(t, err)

	var version [4]byte
	binary.BigEndian.PutUint32(version[:], 2) // single-sync keyspace layout
	metaKey := append([]byte{versionV3, typeEngineMeta}, nil...)
	metaKey = codec.AppendTupleStrings(metaKey, "keyspace_version")
	require.NoError(t, db.Set(metaKey, version[:], cpebble.Sync))

	// One sync-run record so the file looks like a finished sync.
	run := v3.SyncRunRecord_builder{
		SyncId:  "3Ft7w0w9IXoHtkQxjiIL28vgRwC",
		Type:    v3.SyncType_SYNC_TYPE_FULL,
		EndedAt: timestamppb.Now(),
	}.Build()
	runVal, err := proto.Marshal(run)
	require.NoError(t, err)
	require.NoError(t, db.Set([]byte{versionV3, typeSyncRun}, runVal, cpebble.Sync))

	build(t, func(key []byte, msg proto.Message) {
		val, err := proto.Marshal(msg)
		require.NoError(t, err)
		require.NoError(t, db.Set(key, val, cpebble.Sync))
	})
	require.NoError(t, db.Close())
	return dir
}

func legacyEntitlementKey(externalID string) []byte {
	buf := []byte{versionV3, typeEntitlement}
	buf = codec.AppendTupleSeparator(buf)
	return codec.AppendTupleStrings(buf, externalID)
}

func legacyGrantKey(externalID string) []byte {
	buf := []byte{versionV3, typeGrant}
	buf = codec.AppendTupleSeparator(buf)
	return codec.AppendTupleStrings(buf, externalID)
}

// TestIDIndexMigrationSemantics pins the in-place legacy→v2 migration as a
// pure re-key:
//
//   - external ids migrate byte-identical (never rewritten or re-encoded);
//   - legacy grant rows with distinct external ids but identical refs fold
//     to ONE structural identity, keeping the earliest-discovered row's
//     external id;
//   - rows that cannot be represented in the structured keyspace (missing
//     refs) are dropped rather than mis-keyed;
//   - the file is stamped v2, and a reopen does not re-run the migration.
func TestIDIndexMigrationSemantics(t *testing.T) {
	ctx := context.Background()
	older := timestamppb.New(time.Unix(1000, 0).UTC())
	newer := timestamppb.New(time.Unix(2000, 0).UTC())

	dir := legacyIDIndexDB(t, func(t *testing.T, set func(key []byte, msg proto.Message)) {
		set(legacyEntitlementKey("member"), v3.EntitlementRecord_builder{
			ExternalId:   "member",
			Resource:     v3.ResourceRef_builder{ResourceTypeId: "group", ResourceId: "g1"}.Build(),
			DiscoveredAt: older,
		}.Build())
		// Ref-less entitlement: unrepresentable in the structured keyspace.
		set(legacyEntitlementKey("orphan-ent"), v3.EntitlementRecord_builder{
			ExternalId:   "orphan-ent",
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
		// Two legacy rows, one structural identity: must fold, earliest
		// discovered_at wins.
		set(legacyGrantKey("dup-a"), mkGrant("dup-a", newer))
		set(legacyGrantKey("dup-b"), mkGrant("dup-b", older))
		// Ref-less grant: dropped.
		set(legacyGrantKey("orphan-grant"), v3.GrantRecord_builder{
			ExternalId:   "orphan-grant",
			DiscoveredAt: older,
		}.Build())
	})

	e, err := Open(ctx, dir)
	require.NoError(t, err, "open migrates in place")

	// External id migrated byte-identical.
	ent, err := e.GetEntitlementRecord(ctx, "member")
	require.NoError(t, err)
	require.Equal(t, "member", ent.GetExternalId())

	// The ref-less rows are gone; the duplicate grants folded to one row
	// with the earliest-discovered external id.
	var entIDs, grantIDs []string
	require.NoError(t, e.IterateEntitlements(ctx, func(r *v3.EntitlementRecord) bool {
		entIDs = append(entIDs, r.GetExternalId())
		return true
	}))
	require.NoError(t, e.IterateGrants(ctx, func(r *v3.GrantRecord) bool {
		grantIDs = append(grantIDs, r.GetExternalId())
		return true
	}))
	require.Equal(t, []string{"member"}, entIDs, "orphan entitlement dropped; survivor byte-identical")
	require.Equal(t, []string{"dup-b"}, grantIDs, "duplicates folded to earliest-discovered; orphan dropped")

	// The by_principal index was rebuilt against the folded row.
	var byPrincipal int
	require.NoError(t, e.IterateGrantsByPrincipal(ctx, "user", "u1", func(*v3.GrantRecord) bool {
		byPrincipal++
		return true
	}))
	require.Equal(t, 1, byPrincipal)

	format, err := e.readIDIndexFormat()
	require.NoError(t, err)
	require.Equal(t, idIndexFormatCurrent, format, "migration stamps the current format")
	require.NoError(t, e.Close())

	// Reopen: already stamped, no re-migration, data intact.
	e2, err := Open(ctx, dir)
	require.NoError(t, err)
	defer e2.Close()
	got, err := e2.GetEntitlementRecord(ctx, "member")
	require.NoError(t, err)
	require.Equal(t, "member", got.GetExternalId())
}

// TestIDIndexFormatRejectsInterimV1 pins that the interim pre-release v1
// stamp (dev artifacts only; never shipped) is rejected loudly on open
// instead of being silently misread under the v2 layout.
func TestIDIndexFormatRejectsInterimV1(t *testing.T) {
	ctx := context.Background()
	dir := legacyIDIndexDB(t, func(t *testing.T, set func(key []byte, msg proto.Message)) {})

	// Stamp v1 by hand.
	db, err := cpebble.Open(dir, &cpebble.Options{})
	require.NoError(t, err)
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], idIndexFormatStructuredV1)
	require.NoError(t, db.Set(encodeIDIndexFormatKey(), buf[:], cpebble.Sync))
	require.NoError(t, db.Close())

	_, err = Open(ctx, dir)
	require.Error(t, err)
	require.Contains(t, err.Error(), "id-index format v1")
}
