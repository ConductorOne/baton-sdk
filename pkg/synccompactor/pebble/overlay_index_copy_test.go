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
	"google.golang.org/protobuf/types/known/timestamppb"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
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
	for _, ext := range nasty {
		primarySuffix := enginepkg.GrantRecordKey(ext)[2:]
		grantIndexKeys := [][]byte{
			enginepkg.AppendGrantByEntitlementIndexKeyRawBytes(nil, []byte("ent\x00id"), []byte("user"), []byte("alice"), []byte(ext)),
			enginepkg.AppendGrantByEntitlementResourceIndexKeyRawBytes(nil, []byte("group"), []byte("eng\x01"), []byte(ext)),
			enginepkg.AppendGrantByPrincipalIndexKeyRawBytes(nil, []byte("user"), []byte("ali\x00ce"), []byte(ext)),
			enginepkg.AppendGrantByPrincipalResourceTypeIndexKeyRawBytes(nil, []byte("user"), []byte(ext)),
			enginepkg.AppendGrantByNeedsExpansionIndexKeyRawBytes(nil, []byte(ext)),
		}
		for i, key := range grantIndexKeys {
			got, err := indexKeyPrimarySuffix(key, 1)
			if err != nil {
				t.Fatalf("ext=%q family=%d: %v", ext, i, err)
			}
			if !bytes.Equal(got, primarySuffix) {
				t.Fatalf("ext=%q family=%d: suffix = %x, want %x", ext, i, got, primarySuffix)
			}
		}
	}
	for _, rt := range nasty {
		for _, id := range nasty {
			primarySuffix := enginepkg.ResourceRecordKey(rt, id)[2:]
			key := enginepkg.AppendResourceIndexKeyRawBytes(nil, []byte("par\x00ent"), []byte("p\x01id"), []byte(rt), []byte(id))
			got, err := indexKeyPrimarySuffix(key, 2)
			if err != nil {
				t.Fatalf("rt=%q id=%q: %v", rt, id, err)
			}
			if !bytes.Equal(got, primarySuffix) {
				t.Fatalf("rt=%q id=%q: suffix = %x, want %x", rt, id, got, primarySuffix)
			}
		}
	}
	if _, err := indexKeyPrimarySuffix([]byte{0x03, 0x07, 0x01}, 1); err == nil {
		t.Fatal("separator-free index key: expected error, got nil")
	}
	if _, err := indexKeyPrimarySuffix(enginepkg.AppendGrantByNeedsExpansionIndexKeyRawBytes(nil, []byte("x")), 2); err == nil {
		t.Fatal("2-element suffix on 1-element tail: expected error, got nil")
	}
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
	partial := writeIndexCopySource(t, ctx, filepath.Join(dir, "partial.c1z"), indexCopySpec{
		resources: []resourceSpec{
			{rt: "user", id: "alice", parentRT: "group", parentID: "platform"},
			{rt: "user", id: "evil\x00nul", parentRT: "group", parentID: "eng\x01neering"},
		},
		grants: []kwayGrantSpec{
			{id: "shared", principalID: "alice", entitlement: "admin", discovered: newer, needsExpand: true},
			{id: "gr\x00nul", principalID: "evil\x00nul", entitlement: "admin", discovered: newer},
		},
		discovered: newer,
	})
	// Older base: superset of keys, including ones the partial
	// overrides and ones only it has.
	base := writeIndexCopySource(t, ctx, filepath.Join(dir, "base.c1z"), indexCopySpec{
		resources: []resourceSpec{
			{rt: "user", id: "alice", parentRT: "group", parentID: "engineering"},
			{rt: "user", id: "bob", parentRT: "group", parentID: "engineering"},
			{rt: "user", id: "evil\x00nul", parentRT: "group", parentID: "engineering"},
			{rt: "user", id: "orphan"},
		},
		grants: []kwayGrantSpec{
			{id: "shared", principalID: "bob", entitlement: "member", discovered: older},
			{id: "base-only", principalID: "bob", entitlement: "member", discovered: older, needsExpand: true},
			{id: "gr\x00nul", principalID: "bob", entitlement: "member", discovered: older},
		},
		discovered: older,
	})

	oldMin := overlayWholeSourceMinKeys
	overlayWholeSourceMinKeys = 1
	defer func() { overlayWholeSourceMinKeys = oldMin }()

	dest, _ := newEngine(t, "index-copy-dest")
	destSyncID := ksuid.New().String()
	if _, err := MergeFilesIntoOverlay(ctx, dest, []SourceFile{
		{Path: partial.path, SyncID: partial.syncID},
		{Path: base.path, SyncID: base.syncID},
	}, destSyncID, t.TempDir()); err != nil {
		t.Fatalf("MergeFilesIntoOverlay: %v", err)
	}

	assertIndexesMatchDerived(t, ctx, dest)
}

type resourceSpec struct {
	rt, id, parentRT, parentID string
}

type indexCopySpec struct {
	resources  []resourceSpec
	grants     []kwayGrantSpec
	discovered time.Time
}

func writeIndexCopySource(t *testing.T, ctx context.Context, path string, spec indexCopySpec) kwaySourceFixture {
	t.Helper()
	w, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(dotc1z.EnginePebble), dotc1z.WithTmpDir(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	eng, ok := enginepkg.AsEngine(w)
	if !ok {
		t.Fatalf("store is not pebble: %T", w)
	}
	syncID, err := w.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	if err != nil {
		t.Fatal(err)
	}
	ts := timestamppb.New(spec.discovered)
	if err := eng.PutResourceTypeRecords(ctx,
		v3.ResourceTypeRecord_builder{ExternalId: "user", DisplayName: "User", DiscoveredAt: ts}.Build(),
		v3.ResourceTypeRecord_builder{ExternalId: "group", DisplayName: "Group", DiscoveredAt: ts}.Build(),
	); err != nil {
		t.Fatal(err)
	}
	resources := make([]*v3.ResourceRecord, 0, len(spec.resources))
	for _, r := range spec.resources {
		b := v3.ResourceRecord_builder{ResourceTypeId: r.rt, ResourceId: r.id, DiscoveredAt: ts}
		if r.parentID != "" {
			b.Parent = v3.ResourceRef_builder{ResourceTypeId: r.parentRT, ResourceId: r.parentID}.Build()
		}
		resources = append(resources, b.Build())
	}
	if err := eng.PutResourceRecords(ctx, resources...); err != nil {
		t.Fatal(err)
	}
	seenEnt := map[string]bool{}
	for _, g := range spec.grants {
		if seenEnt[g.entitlement] {
			continue
		}
		seenEnt[g.entitlement] = true
		if err := eng.PutEntitlementRecords(ctx, v3.EntitlementRecord_builder{
			ExternalId:   g.entitlement,
			Resource:     v3.ResourceRef_builder{ResourceTypeId: "group", ResourceId: "engineering"}.Build(),
			DiscoveredAt: ts,
		}.Build()); err != nil {
			t.Fatal(err)
		}
	}
	for _, g := range spec.grants {
		if err := eng.PutGrantRecords(ctx, v3.GrantRecord_builder{
			ExternalId: g.id,
			Entitlement: v3.EntitlementRef_builder{
				ResourceTypeId: "group",
				ResourceId:     "engineering",
				EntitlementId:  g.entitlement,
			}.Build(),
			Principal:      v3.PrincipalRef_builder{ResourceTypeId: "user", ResourceId: g.principalID}.Build(),
			NeedsExpansion: g.needsExpand,
			DiscoveredAt:   timestamppb.New(g.discovered),
		}.Build()); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.EndSync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}
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
	if err := dest.IterateResources(ctx, func(r *v3.ResourceRecord) bool {
		if err := enginepkg.ForEachResourceIndexKey(r, collect); err != nil {
			t.Fatal(err)
		}
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if err := dest.IterateEntitlements(ctx, func(e *v3.EntitlementRecord) bool {
		if err := enginepkg.ForEachEntitlementIndexKey(e, collect); err != nil {
			t.Fatal(err)
		}
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if err := dest.IterateGrants(ctx, func(g *v3.GrantRecord) bool {
		if err := enginepkg.ForEachGrantIndexKey(g, collect); err != nil {
			t.Fatal(err)
		}
		return true
	}); err != nil {
		t.Fatal(err)
	}
	sort.Slice(derived, func(i, j int) bool { return bytes.Compare(derived[i], derived[j]) < 0 })

	var stored [][]byte
	for _, bucket := range allBuckets() {
		for _, r := range bucketIndexRanges(bucket) {
			iter, err := dest.DB().NewIter(&cpebble.IterOptions{LowerBound: r[0], UpperBound: r[1]})
			if err != nil {
				t.Fatal(err)
			}
			for iter.First(); iter.Valid(); iter.Next() {
				stored = append(stored, append([]byte(nil), iter.Key()...))
			}
			if err := iter.Error(); err != nil {
				_ = iter.Close()
				t.Fatal(err)
			}
			if err := iter.Close(); err != nil {
				t.Fatal(err)
			}
		}
	}
	sort.Slice(stored, func(i, j int) bool { return bytes.Compare(stored[i], stored[j]) < 0 })

	if len(stored) != len(derived) {
		t.Fatalf("stored index keys = %d, derived = %d\nstored: %s\nderived: %s",
			len(stored), len(derived), fmtKeys(stored), fmtKeys(derived))
	}
	for i := range stored {
		if !bytes.Equal(stored[i], derived[i]) {
			t.Fatalf("index key %d mismatch:\nstored:  %x\nderived: %x", i, stored[i], derived[i])
		}
	}
}

func fmtKeys(keys [][]byte) string {
	var b bytes.Buffer
	for _, k := range keys {
		fmt.Fprintf(&b, "\n  %x", k)
	}
	return b.String()
}
