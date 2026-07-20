package pebble

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"testing"
	"time"

	cpebble "github.com/cockroachdb/pebble/v2"
	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	enginepkg "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
)

// TestIndexKeyPrimarySuffix pins the byte-level contract behind the
// whole-source verbatim index copy: the trailing tuple elements of an
// index key are byte-identical to the corresponding primary key's
// suffix (key[len(prefix):]), including for IDs containing the tuple
// separator (0x00) and escape (0x01) bytes.
func TestIndexKeyPrimarySuffix(t *testing.T) {
	nasty := []string{
		"plain",
		"",
		"with\x00nul",
		"with\x01esc",
		"\x00",
		"\x01\x01",
		"tail\x00",
	}
	// Grants under structural identity: the by_needs_expansion index tail
	// IS the 6-element grant primary tail (rt, rid, flag, tail, prt, pid),
	// so a 6-element suffix walk must reconstruct the primary suffix
	// byte-for-byte, including ids carrying separator/escape bytes.
	const idxGrantByNeedsExpansionByte = 0x05
	for _, ext := range nasty {
		if ext == "" {
			// A grant with no entitlement id has no structural identity
			// (such rows are dropped at write/migration time) and thus no
			// index keys; nothing to pin.
			continue
		}
		rec := v3.GrantRecord_builder{
			ExternalId: "x",
			Entitlement: v3.EntitlementRef_builder{
				ResourceTypeId: "gr\x00oup",
				ResourceId:     "eng\x01",
				EntitlementId:  ext,
			}.Build(),
			Principal:      v3.PrincipalRef_builder{ResourceTypeId: "user", ResourceId: "ali\x00ce"}.Build(),
			NeedsExpansion: true,
		}.Build()
		var idxKey []byte
		require.NoError(t, enginepkg.ForEachGrantIndexKey(rec, func(key []byte) error {
			if key[2] == idxGrantByNeedsExpansionByte {
				idxKey = append([]byte(nil), key...)
			}
			return nil
		}))
		require.NotNil(t, idxKey, "needs_expansion index key for ext=%q", ext)
		primaryKey := []byte(enginepkg.GrantRecordIdentityKey(rec))
		got, err := indexKeyPrimarySuffix(idxKey, 6)
		require.NoError(t, err, "ext=%q", ext)
		require.Equal(t, primaryKey[2:], got, "ext=%q: suffix", ext)
	}
	for _, rt := range nasty {
		for _, id := range nasty {
			primarySuffix := enginepkg.ResourceRecordKey(rt, id)[2:]
			key := enginepkg.AppendResourceIndexKeyRawBytes(nil, []byte("par\x00ent"), []byte("p\x01id"), []byte(rt), []byte(id))
			got, err := indexKeyPrimarySuffix(key, 2)
			require.NoError(t, err, "rt=%q id=%q", rt, id)
			require.Equal(t, primarySuffix, got, "rt=%q id=%q: suffix", rt, id)
		}
	}
	_, err := indexKeyPrimarySuffix([]byte{0x03, 0x07, 0x01}, 1)
	require.Error(t, err, "separator-free index key: expected error, got nil")
	_, err = indexKeyPrimarySuffix([]byte{0x03, 0x07, 0x05, 0x00, 'x'}, 2)
	require.Error(t, err, "2-element suffix on 1-element tail: expected error, got nil")
}

// TestOverlayWholeSourceIndexCopyParityWithDerived locks in the
// verbatim index copy: after an overlay merge whose last source took
// the filtered whole-source SST path, every secondary index keyspace
// in the dest must be byte-identical to what re-deriving the indexes
// from the dest's primary records produces. This catches both stale
// entries (a filtered-out loser's indexes leaking through the copy)
// and missing entries (a winner's indexes dropped by the filter).
func TestOverlayWholeSourceIndexCopyParityWithDerived(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	older := time.Unix(1000, 0).UTC()
	newer := time.Unix(2000, 0).UTC()

	// Newer partial: overlaps base keys with different index-relevant
	// fields (principal, entitlement, needs_expansion) so the base's
	// index entries for those keys are stale and must be filtered.
	partial := writeIndexCopySource(t, ctx, filepath.Join(dir, "partial.c1z"), indexCopyCaseSpec{
		resources: []resourceSpec{
			{rt: "user", id: "alice", parentRT: "group", parentID: "platform"},
			{rt: "user", id: "evil\x00nul", parentRT: "group", parentID: "eng\x01neering"},
		},
		grants: []kwayGrantSpec{
			// SAME identity as the base's (member, alice) grant but with
			// needs_expansion flipped OFF: the base's by_needs_expansion
			// entry is stale and MUST be filtered out of the verbatim copy,
			// or the artifact carries a phantom pending-expansion row.
			{id: "shared", principalID: "alice", entitlement: "member", discovered: newer, needsExpand: false},
			{id: "gr\x00nul", principalID: "evil\x00nul", entitlement: "admin", discovered: newer},
		},
		discovered: newer,
	})
	// Older base: superset of keys, including ones the partial
	// overrides and ones only it has.
	base := writeIndexCopySource(t, ctx, filepath.Join(dir, "base.c1z"), indexCopyCaseSpec{
		resources: []resourceSpec{
			{rt: "user", id: "alice", parentRT: "group", parentID: "engineering"},
			{rt: "user", id: "bob", parentRT: "group", parentID: "engineering"},
			{rt: "user", id: "evil\x00nul", parentRT: "group", parentID: "engineering"},
			{rt: "user", id: "orphan"},
		},
		grants: []kwayGrantSpec{
			// Identity-overlaps the partial's (member, alice) grant with
			// needs_expansion=true — the stale index entry that must not
			// survive the copy.
			{id: "shared", principalID: "alice", entitlement: "member", discovered: older, needsExpand: true},
			{id: "base-only", principalID: "bob", entitlement: "member", discovered: older, needsExpand: true},
			// Identity-overlaps the partial's (admin, evil) grant.
			{id: "gr\x00nul", principalID: "evil\x00nul", entitlement: "admin", discovered: older, needsExpand: true},
		},
		discovered: older,
	})

	oldMin := overlayWholeSourceMinKeys
	overlayWholeSourceMinKeys = 1
	defer func() { overlayWholeSourceMinKeys = oldMin }()

	dest, _ := newEngine(t, "index-copy-dest")
	destSyncID := ksuid.New().String()
	_, err := MergeFilesIntoOverlay(ctx, dest, []SourceFile{
		{Path: partial.path, SyncID: partial.syncID},
		{Path: base.path, SyncID: base.syncID},
	}, destSyncID, t.TempDir())
	require.NoError(t, err, "MergeFilesIntoOverlay")

	assertIndexesMatchDerived(t, ctx, dest)
}

type resourceSpec struct {
	rt, id, parentRT, parentID string
}

type indexCopyCaseSpec struct {
	resources  []resourceSpec
	grants     []kwayGrantSpec
	discovered time.Time
}

func writeIndexCopySource(t *testing.T, ctx context.Context, path string, spec indexCopyCaseSpec) kwaySourceFixture {
	t.Helper()
	w, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(c1zstore.EnginePebble), dotc1z.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	eng, ok := enginepkg.AsEngine(w)
	require.True(t, ok, "store is not pebble: %T", w)
	syncID, err := w.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	ts := timestamppb.New(spec.discovered)
	require.NoError(t, eng.PutResourceTypeRecords(ctx,
		v3.ResourceTypeRecord_builder{ExternalId: "user", DisplayName: "User", DiscoveredAt: ts}.Build(),
		v3.ResourceTypeRecord_builder{ExternalId: "group", DisplayName: "Group", DiscoveredAt: ts}.Build(),
	))
	resources := make([]*v3.ResourceRecord, 0, len(spec.resources))
	for _, r := range spec.resources {
		b := v3.ResourceRecord_builder{ResourceTypeId: r.rt, ResourceId: r.id, DiscoveredAt: ts}
		if r.parentID != "" {
			b.Parent = v3.ResourceRef_builder{ResourceTypeId: r.parentRT, ResourceId: r.parentID}.Build()
		}
		resources = append(resources, b.Build())
	}
	require.NoError(t, eng.PutResourceRecords(ctx, resources...))
	seenEnt := map[string]bool{}
	for _, g := range spec.grants {
		if seenEnt[g.entitlement] {
			continue
		}
		seenEnt[g.entitlement] = true
		require.NoError(t, eng.PutEntitlementRecords(ctx, v3.EntitlementRecord_builder{
			ExternalId:   g.entitlement,
			Resource:     v3.ResourceRef_builder{ResourceTypeId: "group", ResourceId: "engineering"}.Build(),
			DiscoveredAt: ts,
		}.Build()))
	}
	for _, g := range spec.grants {
		require.NoError(t, eng.PutGrantRecords(ctx, v3.GrantRecord_builder{
			ExternalId: g.id,
			Entitlement: v3.EntitlementRef_builder{
				ResourceTypeId: "group",
				ResourceId:     "engineering",
				EntitlementId:  g.entitlement,
			}.Build(),
			Principal:      v3.PrincipalRef_builder{ResourceTypeId: "user", ResourceId: g.principalID}.Build(),
			NeedsExpansion: g.needsExpand,
			DiscoveredAt:   timestamppb.New(g.discovered),
		}.Build()))
	}
	require.NoError(t, w.EndSync(ctx))
	require.NoError(t, w.Close(ctx))
	return kwaySourceFixture{path: path, syncID: syncID}
}

// assertIndexesMatchDerived reads every secondary-index keyspace from
// the dest and compares it, byte for byte, against the index keys
// re-derived from the dest's primary records.
func assertIndexesMatchDerived(t *testing.T, ctx context.Context, dest *enginepkg.Engine) {
	t.Helper()
	var derived [][]byte
	collect := func(key []byte) error {
		derived = append(derived, append([]byte(nil), key...))
		return nil
	}
	require.NoError(t, dest.IterateResources(ctx, func(r *v3.ResourceRecord) bool {
		require.NoError(t, enginepkg.ForEachResourceIndexKey(r, collect))
		return true
	}))
	require.NoError(t, dest.IterateEntitlements(ctx, func(e *v3.EntitlementRecord) bool {
		require.NoError(t, enginepkg.ForEachEntitlementIndexKey(e, collect))
		return true
	}))
	require.NoError(t, dest.IterateGrants(ctx, func(g *v3.GrantRecord) bool {
		require.NoError(t, enginepkg.ForEachGrantIndexKey(g, collect))
		return true
	}))
	sort.Slice(derived, func(i, j int) bool { return bytes.Compare(derived[i], derived[j]) < 0 })

	var stored [][]byte
	for _, bucket := range allBuckets() {
		for _, r := range bucketIndexRanges(bucket) {
			iter, err := dest.NewIter(&cpebble.IterOptions{LowerBound: r[0], UpperBound: r[1]})
			require.NoError(t, err)
			for iter.First(); iter.Valid(); iter.Next() {
				stored = append(stored, append([]byte(nil), iter.Key()...))
			}
			if err := iter.Error(); err != nil {
				_ = iter.Close()
				require.NoError(t, err)
			}
			require.NoError(t, iter.Close())
		}
	}
	sort.Slice(stored, func(i, j int) bool { return bytes.Compare(stored[i], stored[j]) < 0 })

	require.Equal(t, len(derived), len(stored),
		"stored index keys = %d, derived = %d\nstored: %s\nderived: %s",
		len(stored), len(derived), fmtKeys(stored), fmtKeys(derived))
	for i := range stored {
		require.Equal(t, derived[i], stored[i], "index key %d mismatch:\nstored:  %x\nderived: %x", i, stored[i], derived[i])
	}
}

func fmtKeys(keys [][]byte) string {
	var b bytes.Buffer
	for _, k := range keys {
		fmt.Fprintf(&b, "\n  %x", k)
	}
	return b.String()
}
