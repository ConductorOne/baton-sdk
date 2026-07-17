package dotc1z

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
)

// TestToPebblePreservesDiscoveredAt pins that conversion carries each
// record's sqlite discovered_at column onto the v3 record instead of
// re-stamping conversion wall-clock time. Every Pebble merge strategy
// picks winners by newest discovered_at, so a re-stamp would make a
// converted input override genuinely newer records during multi-engine
// compaction.
func TestToPebblePreservesDiscoveredAt(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	src, err := NewC1ZFile(ctx, filepath.Join(dir, "src.c1z"), WithTmpDir(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, src.Close(ctx)) }()

	syncID, err := src.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	require.NoError(t, src.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build(),
		v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build(),
	))
	group := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build()}.Build()
	user := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build()}.Build()
	require.NoError(t, src.PutResources(ctx, group, user))
	member := v2.Entitlement_builder{Id: "member", Resource: group, Purpose: v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT}.Build()
	require.NoError(t, src.PutEntitlements(ctx, member))
	require.NoError(t, src.PutGrants(ctx, v2.Grant_builder{Id: "g1", Principal: user, Entitlement: member}.Build()))

	// Backdate every record's discovered_at to a fixed instant well in
	// the past. The stamp uses the same no-zone layout the writers use,
	// so the driver reads it back as exactly this UTC instant.
	want := time.Date(2020, time.January, 2, 3, 4, 5, 123456789, time.UTC)
	stamp := want.Format("2006-01-02 15:04:05.999999999")
	for _, table := range []string{resourceTypes.Name(), resources.Name(), entitlements.Name(), grants.Name()} {
		_, err := src.rawDb.ExecContext(ctx, "UPDATE "+table+" SET discovered_at = ?", stamp) //nolint:gosec // fixed internal table names.
		require.NoError(t, err)
	}
	require.NoError(t, src.EndSync(ctx))

	outPath := filepath.Join(dir, "out.c1z")
	_, err = src.ToPebble(ctx, outPath, syncID)
	require.NoError(t, err)

	dest, err := NewStore(ctx, outPath, WithReadOnly(true), WithTmpDir(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, dest.Close(ctx)) }()
	eng, ok := pebble.AsEngine(dest)
	require.True(t, ok)

	iter, err := eng.NewIter(nil)
	require.NoError(t, err)
	defer iter.Close()

	checked := 0
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		if len(key) < 2 || key[0] != 0x03 {
			continue
		}
		var m proto.Message
		switch key[1] {
		case 0x01:
			m = &v3.ResourceTypeRecord{}
		case 0x02:
			m = &v3.ResourceRecord{}
		case 0x03:
			m = &v3.EntitlementRecord{}
		case 0x04:
			m = &v3.GrantRecord{}
		default:
			continue
		}
		require.NoError(t, proto.Unmarshal(iter.Value(), m))
		var got time.Time
		switch r := m.(type) {
		case *v3.ResourceTypeRecord:
			got = r.GetDiscoveredAt().AsTime()
		case *v3.ResourceRecord:
			got = r.GetDiscoveredAt().AsTime()
		case *v3.EntitlementRecord:
			got = r.GetDiscoveredAt().AsTime()
		case *v3.GrantRecord:
			got = r.GetDiscoveredAt().AsTime()
		}
		require.True(t, got.Equal(want),
			"record in bucket %#02x has discovered_at %s; conversion must preserve the source's %s, not re-stamp conversion time", key[1], got, want)
		checked++
	}
	require.NoError(t, iter.Error())
	require.Equal(t, 6, checked, "expected 2 resource types + 2 resources + 1 entitlement + 1 grant")
}
