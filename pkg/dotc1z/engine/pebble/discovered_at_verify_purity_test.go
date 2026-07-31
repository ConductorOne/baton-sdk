package pebble

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	reader_v3 "github.com/conductorone/baton-sdk/pb/c1/reader/v3"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// fullStoreDigest hashes every (key,value) pair in the engine's Pebble DB —
// the whole keyspace, including grant records, secondary indexes, and the
// SyncRunRecord (sync metadata). Length-prefixed so no key/value boundary is
// ambiguous. A byte-identical digest before and after a batch of reads proves
// the reads mutated NOTHING anywhere in the store — not just the value read.
func fullStoreDigest(t *testing.T, e *Engine) [32]byte {
	t.Helper()
	it, err := e.db.NewIter(nil)
	require.NoError(t, err)
	defer func() { _ = it.Close() }()
	h := sha256.New()
	var lp [8]byte
	for it.First(); it.Valid(); it.Next() {
		k := it.Key()
		v := it.Value()
		binary.BigEndian.PutUint32(lp[0:4], uint32(len(k))) //nolint:gosec // test key lengths are tiny
		binary.BigEndian.PutUint32(lp[4:8], uint32(len(v))) //nolint:gosec // test value lengths are tiny
		_, _ = h.Write(lp[:])
		_, _ = h.Write(k)
		_, _ = h.Write(v)
	}
	require.NoError(t, it.Error())
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

// TestV3ReadPurityWholeStoreUnchanged closes V-04: a full read-purity auditor.
// It snapshots the ENTIRE sealed store, drives every grant read surface (v2 and
// v3) repeatedly, then snapshots again and requires byte-for-byte equality. A
// read that lazily populated a cache, flipped a fast-path flag, advanced a
// cursor durably, or touched sync metadata would change the digest.
func TestV3ReadPurityWholeStoreUnchanged(t *testing.T) {
	ctx := context.Background()
	e, err := Open(ctx, filepath.Join(t.TempDir(), "engine"))
	require.NoError(t, err)
	defer func() { _ = e.Close() }()

	_, err = e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	base := time.Date(2019, 1, 2, 3, 4, 5, 6, time.UTC)
	ids := []struct{ rt, pid string }{
		{"user", "alice"}, {"user", "bob"}, {"group", "eng"},
	}
	for i, id := range ids {
		rec := v3.GrantRecord_builder{
			ExternalId: "g-" + id.pid,
			Entitlement: v3.EntitlementRef_builder{
				ResourceTypeId: "app", ResourceId: "github", EntitlementId: canonicalTestEntID("ent-A"),
			}.Build(),
			Principal:    v3.PrincipalRef_builder{ResourceTypeId: id.rt, ResourceId: id.pid}.Build(),
			DiscoveredAt: timestamppb.New(base.Add(time.Duration(i) * time.Hour)),
		}.Build()
		require.NoError(t, e.PutGrantRecord(ctx, rec))
	}
	require.NoError(t, e.EndSync(ctx)) // seal — reads happen against the sealed store

	provider, ok := any(e).(connectorstore.V3GrantReaderProvider)
	require.True(t, ok)
	r3 := provider.V3GrantReader()
	entStub := v3TestEntStub("ent-A")

	// Snapshot the whole store BEFORE any read.
	before := fullStoreDigest(t, e)

	// Drive every grant read surface, both engines, several times over.
	for range 3 {
		for _, id := range ids {
			_, err := r3.GetGrant(ctx, reader_v3.GrantsReaderServiceGetGrantRequest_builder{GrantId: "g-" + id.pid}.Build())
			require.NoError(t, err)
			_, err = e.GetGrant(ctx, reader_v2.GrantsReaderServiceGetGrantRequest_builder{GrantId: "g-" + id.pid}.Build())
			require.NoError(t, err)
		}
		_, err := r3.ListGrantsForEntitlement(ctx, reader_v3.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
			Entitlement: entStub, PageSize: 100,
		}.Build())
		require.NoError(t, err)
		_, err = e.ListGrantsForEntitlement(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
			Entitlement: entStub, PageSize: 100,
		}.Build())
		require.NoError(t, err)
		_, err = r3.ListGrantsForResourceType(ctx, reader_v3.GrantsReaderServiceListGrantsForResourceTypeRequest_builder{
			ResourceTypeId: "user", PageSize: 100,
		}.Build())
		require.NoError(t, err)
		_, err = r3.ListGrantsForPrincipal(ctx, reader_v3.GrantsReaderServiceListGrantsForPrincipalRequest_builder{
			PrincipalId: v2.ResourceId_builder{ResourceType: "user", Resource: "alice"}.Build(), PageSize: 100,
		}.Build())
		require.NoError(t, err)
	}

	// Snapshot AFTER: must be byte-identical.
	after := fullStoreDigest(t, e)
	require.Equal(t, before, after,
		"grant reads mutated the store: whole-keyspace digest changed (a read wrote a cache/flag/cursor/metadata)")

	// Sanity: the auditor is non-vacuous — a real mutation flips the digest.
	require.NoError(t, e.db.UnsafeForTesting().Set([]byte("zzz-purity-probe"), []byte{1}, pebble.Sync))
	require.NotEqual(t, before, fullStoreDigest(t, e),
		"auditor is vacuous: a planted write did not change the digest")
}
