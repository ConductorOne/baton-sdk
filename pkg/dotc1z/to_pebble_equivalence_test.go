package dotc1z_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	enginepkg "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
)

// TestToPebbleBulkImportEquivalence verifies the SST bulk-import conversion
// produces a byte-equivalent keyspace to the engine's canonical Put* write
// path. It seeds a sqlite source with ordering and index edge cases (ids that
// are prefixes of each other, colons inside resource ids, unicode, missing
// parents/resources/principals/entitlements, expansion annotations), then:
//
//  1. converts it with ToPebble (sorted scans + BulkSyncImport), and
//  2. writes the same records through Adapter.Put* into a reference store,
//
// and asserts the record + index keyspaces match exactly after
// canonicalizing the embedded sync-id bytes and zeroing discovered_at.
func TestToPebbleBulkImportEquivalence(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	// --- seed the sqlite source ---
	src, err := dotc1z.NewC1ZFile(ctx, filepath.Join(dir, "source.c1z"), dotc1z.WithTmpDir(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, src.Close(ctx)) }()

	syncID, err := src.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	rts := []*v2.ResourceType{
		v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build(),
		v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build(),
		v2.ResourceType_builder{Id: "grp", DisplayName: "Prefix of group"}.Build(),
	}

	mkRes := func(rt, id string, parent *v2.ResourceId) *v2.Resource {
		b := v2.Resource_builder{
			Id:               v2.ResourceId_builder{ResourceType: rt, Resource: id}.Build(),
			ParentResourceId: parent,
		}
		return b.Build()
	}
	groupParent := v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build()
	resources := []*v2.Resource{
		mkRes("user", "a", nil),
		mkRes("user", "ab", groupParent), // prefix pair with "a"
		mkRes("user", "u:colon:id", groupParent),
		mkRes("user", "Ωmega", nil), // multi-byte utf-8
		mkRes("user", "zz", v2.ResourceId_builder{ResourceType: "group", Resource: "g2"}.Build()),
		mkRes("group", "g1", nil),
		mkRes("group", "g2", nil),
		mkRes("grp", "g1", nil), // ("grp","g1") vs ("group","g1") key-order edge
	}

	groupRes := mkRes("group", "g1", nil)
	ents := []*v2.Entitlement{
		v2.Entitlement_builder{Id: "ent-a", Resource: groupRes, Purpose: v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT}.Build(),
		v2.Entitlement_builder{Id: "ent-ab", Resource: mkRes("group", "g2", nil)}.Build(), // prefix pair
		v2.Entitlement_builder{Id: "ent-bare"}.Build(),                                    // no resource -> no by_resource index
	}

	expandable, err := anypb.New(v2.GrantExpandable_builder{
		EntitlementIds: []string{"ent-ab"},
		Shallow:        true,
	}.Build())
	require.NoError(t, err)

	mkGrant := func(id string, ent *v2.Entitlement, principal *v2.Resource, annos ...*anypb.Any) *v2.Grant {
		return v2.Grant_builder{Id: id, Entitlement: ent, Principal: principal, Annotations: annos}.Build()
	}
	grants := []*v2.Grant{
		mkGrant("grant-1", ents[0], resources[0]),
		mkGrant("grant-10", ents[0], resources[1]),            // "grant-1" prefix pair
		mkGrant("grant-2", ents[1], resources[2], expandable), // expansion -> needs_expansion index
		mkGrant("grant-3", ents[2], resources[3]),             // ent without resource
		mkGrant("grant-4", ents[0], nil),                      // no principal -> primary only + by_ent skipped
		mkGrant("grant-5", nil, resources[4]),                 // no entitlement -> by_principal only
		mkGrant("grant-Ω", ents[1], resources[3], expandable), // unicode id with expansion
	}

	require.NoError(t, src.PutResourceTypes(ctx, rts...))
	require.NoError(t, src.PutResources(ctx, resources...))
	require.NoError(t, src.PutEntitlements(ctx, ents...))
	require.NoError(t, src.PutGrants(ctx, grants...))
	require.NoError(t, src.EndSync(ctx))

	// --- path 1: SST bulk-import conversion (parallel lanes) ---
	convPath := filepath.Join(dir, "converted.c1z")
	stats, err := src.ToPebble(ctx, convPath, syncID)
	require.NoError(t, err)
	require.Equal(t, int64(len(grants)), stats.Grants.Rows)

	// --- path 1b: same conversion fully serialized (shared-infra dial) ---
	serialPath := filepath.Join(dir, "converted-serial.c1z")
	serialStats, err := src.ToPebble(ctx, serialPath, syncID, dotc1z.WithConvertParallelism(1))
	require.NoError(t, err)
	require.Equal(t, int64(len(grants)), serialStats.Grants.Rows)

	// --- path 2: canonical Put* reference ---
	refPath := filepath.Join(dir, "reference.c1z")
	ref, err := dotc1z.NewStore(ctx, refPath, dotc1z.WithEngine(dotc1z.EnginePebble), dotc1z.WithTmpDir(dir))
	require.NoError(t, err)
	_, err = ref.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, ref.PutResourceTypes(ctx, rts...))
	require.NoError(t, ref.PutResources(ctx, resources...))
	require.NoError(t, ref.PutEntitlements(ctx, ents...))
	require.NoError(t, ref.PutGrants(ctx, grants...))
	require.NoError(t, ref.EndSync(ctx))
	require.NoError(t, ref.Close(ctx))

	refDump := dumpSyncKeyspace(ctx, t, refPath, dir)
	for label, path := range map[string]string{"parallel": convPath, "serial": serialPath} {
		convDump := dumpSyncKeyspace(ctx, t, path, dir)
		require.Equal(t, len(refDump), len(convDump), "%s bulk import and Put path key counts diverge", label)
		for k, refVal := range refDump {
			convVal, ok := convDump[k]
			require.True(t, ok, "%s: key missing from bulk-import output: %q", label, k)
			require.Equal(t, refVal, convVal, "%s: value mismatch for key %q", label, k)
		}
	}

	// The conversion stashes import-accumulated stats instead of
	// re-scanning at EndSync; the reference store's sidecar is the
	// canonical recompute. They must agree on every count and grouping.
	convStats := readStatsSidecar(ctx, t, convPath, dir, stats.DestSyncID)
	refStats := readStatsSidecar(ctx, t, refPath, dir, "")
	require.Equal(t, refStats.GetResourceTypes(), convStats.GetResourceTypes())
	require.Equal(t, refStats.GetResources(), convStats.GetResources())
	require.Equal(t, refStats.GetEntitlements(), convStats.GetEntitlements())
	require.Equal(t, refStats.GetGrants(), convStats.GetGrants())
	require.Equal(t, refStats.GetAssets(), convStats.GetAssets())
	require.Equal(t, refStats.GetResourcesByResourceType(), convStats.GetResourcesByResourceType())
	require.Equal(t, refStats.GetGrantsByEntitlementResourceType(), convStats.GetGrantsByEntitlementResourceType())
}

// readStatsSidecar opens a pebble c1z and returns the stats sidecar
// record for syncID ("" = the store's latest finished sync).
func readStatsSidecar(ctx context.Context, t *testing.T, path, tmpDir, syncID string) *v3.SyncStatsRecord {
	t.Helper()
	store, err := dotc1z.NewStore(ctx, path, dotc1z.WithReadOnly(true), dotc1z.WithTmpDir(tmpDir))
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close(ctx)) }()
	eng, ok := enginepkg.AsEngine(store)
	require.True(t, ok)
	if syncID == "" {
		rec, err := eng.LatestFinishedSyncRecord(ctx, func(v3.SyncType) bool { return true })
		require.NoError(t, err)
		require.NotNil(t, rec)
		syncID = rec.GetSyncId()
	}
	rec, err := enginepkg.ReadSyncStatsRecord(ctx, eng, syncID)
	require.NoError(t, err)
	require.NotNil(t, rec, "missing stats sidecar for sync %s", syncID)
	return rec
}

// dumpSyncKeyspace opens a pebble c1z and returns every record/index key
// (buckets 0x01-0x05 and 0x07 of the v3 keyspace) mapped to its normalized
// value. Under the single-sync contract keys no longer embed a sync id, so
// they're compared verbatim; primary record values are unmarshaled,
// discovered_at-zeroed, and re-marshaled deterministically so the only
// differences left are real.
func dumpSyncKeyspace(ctx context.Context, t *testing.T, path, tmpDir string) map[string]string {
	t.Helper()
	store, err := dotc1z.NewStore(ctx, path, dotc1z.WithReadOnly(true), dotc1z.WithTmpDir(tmpDir))
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close(ctx)) }()
	eng, ok := enginepkg.AsEngine(store)
	require.True(t, ok)

	iter, err := eng.DB().NewIter(nil)
	require.NoError(t, err)
	defer iter.Close()

	out := map[string]string{}
	for iter.First(); iter.Valid(); iter.Next() {
		key := append([]byte(nil), iter.Key()...)
		if len(key) < 2 || key[0] != 0x03 {
			continue
		}
		switch key[1] {
		case 0x01, 0x02, 0x03, 0x04, 0x05, 0x07:
			// primary record + index buckets: compared below.
		default: // sync_runs, counters, sessions, engine meta: not compared
			continue
		}
		val := append([]byte(nil), iter.Value()...)
		if key[1] != 0x07 && len(val) > 0 {
			val = normalizeRecordValue(t, key[1], val)
		}
		out[string(key)] = string(val)
	}
	require.NoError(t, iter.Error())
	return out
}

// normalizeRecordValue zeroes the discovered_at stamp on a primary record
// value so the two write paths' differing wall-clocks don't show up as
// divergence.
func normalizeRecordValue(t *testing.T, bucket byte, val []byte) []byte {
	t.Helper()
	var m proto.Message
	switch bucket {
	case 0x01:
		m = &v3.ResourceTypeRecord{}
	case 0x02:
		m = &v3.ResourceRecord{}
	case 0x03:
		m = &v3.EntitlementRecord{}
	case 0x04:
		m = &v3.GrantRecord{}
	default:
		return val
	}
	require.NoError(t, proto.Unmarshal(val, m))
	switch r := m.(type) {
	case *v3.ResourceTypeRecord:
		r.SetDiscoveredAt(nil)
	case *v3.ResourceRecord:
		r.SetDiscoveredAt(nil)
	case *v3.EntitlementRecord:
		r.SetDiscoveredAt(nil)
	case *v3.GrantRecord:
		r.SetDiscoveredAt(nil)
	}
	normalized, err := proto.MarshalOptions{Deterministic: true}.Marshal(m)
	require.NoError(t, err)
	return normalized
}
