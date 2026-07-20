package pebble

// Negative-path and end-to-end coverage for the 2b typed record ops
// (internal/rawdb records.go) — the review-identified gaps: the family
// assertions and malformed-key error paths had no direct tests, the
// needs_expansion splice helper was only pinned indirectly, and no test
// drove the deferred regime's OVERWRITE path (expanded grant over
// expanded grant) through a seal.

import (
	"context"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"

	v2pb "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/internal/rawdb"
)

// TestAppendGrantByNeedsExpansionKeyFromPrimary pins the deriver
// actually wired into rawdb against the from-identity encoder, and its
// rejection of malformed keys — the header-splice equation itself is
// pinned by TestNeedsExpansionKeyHeaderSpliceFromPrimary, but that
// test never invoked this helper.
func TestAppendGrantByNeedsExpansionKeyFromPrimary(t *testing.T) {
	ids := []grantIdentity{
		{
			entitlement:     entitlementIdentity{resourceTypeID: "group", resourceID: "g1", stripped: true, tail: "member"},
			principalTypeID: "user",
			principalID:     "u1",
		},
		{
			entitlement:     entitlementIdentity{resourceTypeID: "gr\x00oup", resourceID: "g\x011", tail: "\x00k\x00\x01"},
			principalTypeID: "us\x01er",
			principalID:     "u\x00\x001",
		},
	}
	for _, id := range ids {
		primary := encodeGrantIdentityKey(id)
		got, ok := rawdb.AppendGrantByNeedsExpansionKeyFromPrimary(nil, primary)
		require.True(t, ok, "well-formed primary must splice")
		require.Equal(t, encodeGrantByNeedsExpansionIdentityIndexKey(id), got, "identity %+v", id)
	}
	for _, malformed := range [][]byte{
		nil,
		{},
		{versionV3, typeGrant},
		{versionV3, typeGrant, 0},              // zero segments
		{versionV3, typeGrant, 0, 'a', 0, 'b'}, // two segments
		{versionV3, typeEntitlement, 0, 'a', 0, 'b', 0, 'c', 0, 'd', 0, 'e', 0, 'f'}, // wrong family
	} {
		_, ok := rawdb.AppendGrantByNeedsExpansionKeyFromPrimary(nil, malformed)
		require.False(t, ok, "malformed key %x must be rejected", malformed)
	}
}

// TestTypedRecordOpsRejectWrongFamilyAndMalformedKeys drives the rawdb
// typed ops' runtime guards directly: a key from the wrong keyspace
// family must fail its op, a family-correct but structurally malformed
// grant key must fail derivation, and an errored op must leave the
// engine unharmed (batch abandoned, nothing committed, subsequent
// writes fine).
func TestTypedRecordOpsRejectWrongFamilyAndMalformedKeys(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	e := a.PebbleEngine()

	grantKey := encodeGrantIdentityKey(grantIdentity{
		entitlement:     entitlementIdentity{resourceTypeID: "app", resourceID: "github", stripped: true, tail: "member"},
		principalTypeID: "user",
		principalID:     "alice",
	})
	entKey := encodeEntitlementIdentityKey(entitlementIdentity{resourceTypeID: "app", resourceID: "github", stripped: true, tail: "member"})

	t.Run("wrong family", func(t *testing.T) {
		rb := e.db.NewRecordBatch()
		defer rb.Close()
		require.ErrorContains(t, rb.StageEntitlementPut(grantKey, []byte("v")),
			"outside this op's keyspace family", "grant key must not stage as an entitlement")
		require.ErrorContains(t, rb.StageGrantPutInline(entKey, []byte("v"), false, false),
			"outside this op's keyspace family", "entitlement key must not stage as a grant")
		require.ErrorContains(t, rb.StageResourceTypeDelete(grantKey),
			"outside this op's keyspace family")
		require.ErrorContains(t, rb.StageResourcePut(grantKey, []byte("v"), nil, "group", "g1"),
			"outside this op's keyspace family", "grant key must not stage as a resource")
		require.ErrorContains(t, rb.StageResourceDelete(entKey, nil, "group", "g1"),
			"outside this op's keyspace family", "entitlement key must not stage as a resource delete")
	})

	t.Run("malformed grant key", func(t *testing.T) {
		rb := e.db.NewRecordBatch()
		defer rb.Close()
		// Family prefix right, segment structure wrong (2 segments, not 6).
		malformed := []byte{versionV3, typeGrant, 0, 'a', 0, 'b'}
		require.ErrorContains(t, rb.StageGrantPutInline(malformed, []byte("v"), false, false),
			"did not decode as a 6-segment identity")
		require.ErrorContains(t, rb.StageGrantPutDeferred(malformed, []byte("v"), false, false),
			"did not decode as a 6-segment identity")
		require.ErrorContains(t, rb.StageGrantDelete(malformed),
			"did not decode as a 6-segment identity")
	})

	// An errored op abandons its batch; the engine keeps working.
	require.NoError(t, a.PutGrants(ctx, mkV2Grant("g1", "ent-A", "user", "alice")))
	resp, err := a.ListGrants(ctx, nil)
	require.NoError(t, err)
	require.Len(t, resp.GetList(), 1, "engine must keep serving writes after rejected typed ops")
}

// TestExpandedGrantOverwriteThroughSeal is the deferred regime's
// overwrite path end-to-end: an inline-written expandable grant is
// overwritten TWICE by the expander (the second write is the
// hadOldVal=true deferred cleanup path), then the sync seals. The
// saved state must have exactly consistent indexes: by_needs_expansion
// agrees with the primary rows' flags (side-state preserved through
// both overwrites), and the seal-rebuilt by_principal serves every
// grant.
func TestExpandedGrantOverwriteThroughSeal(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	e := a.PebbleEngine()

	// Inline write with the expandable annotation → needs_expansion=true
	// row + index entry via the inline regime.
	annAny, err := anypb.New(v2pb.GrantExpandable_builder{
		EntitlementIds: []string{"app:github:ent-B"},
	}.Build())
	require.NoError(t, err)
	expandable := mkV2Grant("g1", "ent-A", "user", "alice")
	expandable.SetAnnotations([]*anypb.Any{annAny})
	require.NoError(t, a.PutGrants(ctx, expandable))

	countNeedsExpansion := func() int {
		n := 0
		prefix := encodeGrantByNeedsExpansionPrefix()
		iter, err := e.db.NewIter(&pebble.IterOptions{
			LowerBound: prefix,
			UpperBound: upperBoundOf(prefix),
		})
		require.NoError(t, err)
		defer iter.Close()
		for iter.First(); iter.Valid(); iter.Next() {
			n++
		}
		require.NoError(t, iter.Error())
		return n
	}
	require.Equal(t, 1, countNeedsExpansion(), "inline write must index the expandable grant")

	// Two expander overwrites of the same identity. First: hadOldVal=true
	// against the inline row. Second: hadOldVal=true against the deferred
	// row — the exact path the review flagged as uncovered. Side-state
	// (NeedsExpansion=true) must survive both (StoreExpandedGrants
	// preservation contract), so the index entry must survive both.
	for i := 0; i < 2; i++ {
		require.NoError(t, a.Grants().StoreExpandedGrants(ctx, mkV2Grant("g1", "ent-A", "user", "alice")))
		require.Equalf(t, 1, countNeedsExpansion(),
			"overwrite %d must clean and rewrite exactly one needs_expansion entry", i+1)
	}

	require.NoError(t, a.EndSync(ctx))

	// Seal-rebuilt by_principal serves the grant; needs_expansion index
	// still agrees with the primary's preserved flag.
	n := 0
	require.NoError(t, e.IterateGrantsByPrincipal(ctx, "user", "alice", func(r *v3.GrantRecord) bool {
		require.True(t, r.GetNeedsExpansion(), "preserved side-state must survive both overwrites")
		n++
		return true
	}))
	require.Equal(t, 1, n, "by_principal must serve the overwritten grant exactly once")
	require.Equal(t, 1, countNeedsExpansion(), "sealed index state must agree with the primary flag")
}
