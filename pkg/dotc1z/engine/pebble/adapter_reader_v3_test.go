package pebble

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	reader_v3 "github.com/conductorone/baton-sdk/pb/c1/reader/v3"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// v2 entitlement stub carrying the resource ref, so
// entitlementIdentityForRequest derives identity directly from the
// structured parts (no bare-id lookup) — the shape connectors send.
func v3TestEntStub(entID string) *v2.Entitlement {
	return v2.Entitlement_builder{
		Id: canonicalTestEntID(entID),
		Resource: v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: "app", Resource: "github"}.Build(),
		}.Build(),
	}.Build()
}

// seedGrantRT writes a primary GrantRecord with an explicit discovered_at
// and a caller-chosen principal resource type straight through the engine
// (bypassing any now() stamp), mirroring
// store_expanded_grants_discovered_at_test.go's idiom. The entitlement's
// resource stays app/github (matching canonicalTestEntID); vary entID for
// distinct entitlements and principalRT/principalID for distinct principals.
func seedGrantRT(ctx context.Context, t *testing.T, e *Engine, externalID, entID, principalRT, principalID string, discoveredAt time.Time) {
	t.Helper()
	rec := v3.GrantRecord_builder{
		ExternalId: externalID,
		Entitlement: v3.EntitlementRef_builder{
			ResourceTypeId: "app", ResourceId: "github", EntitlementId: canonicalTestEntID(entID),
		}.Build(),
		Principal: v3.PrincipalRef_builder{
			ResourceTypeId: principalRT, ResourceId: principalID,
		}.Build(),
		DiscoveredAt: timestamppb.New(discoveredAt),
	}.Build()
	require.NoError(t, e.PutGrantRecord(ctx, rec))
}

// seedGrantWithDiscoveredAt is the user-principal shorthand for seedGrantRT.
func seedGrantWithDiscoveredAt(ctx context.Context, t *testing.T, e *Engine, externalID, entID, principalID string, discoveredAt time.Time) {
	t.Helper()
	seedGrantRT(ctx, t, e, externalID, entID, "user", principalID, discoveredAt)
}

// newV3GrantReader opens a fresh engine, starts a sync, and returns the
// engine plus its v3 grants reader (discovered via the provider assertion,
// Pebble only). Mirrors the setup the existing v3 tests do inline.
func newV3GrantReader(ctx context.Context, t *testing.T) (*Engine, connectorstore.V3GrantReader) {
	t.Helper()
	e, err := Open(ctx, filepath.Join(t.TempDir(), "engine"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = e.Close() })

	a := NewAdapter(e)
	_, err = a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	provider, ok := any(e).(connectorstore.V3GrantReaderProvider)
	require.True(t, ok, "engine must expose V3GrantReaderProvider")
	return e, provider.V3GrantReader()
}

// TestV3GetGrantReturnsUnNerfedRecord proves the v3 GetGrant RPC returns
// the rich v3.GrantRecord with its stored discovered_at intact — the field
// the v2.Grant downshift drops.
func TestV3GetGrantReturnsUnNerfedRecord(t *testing.T) {
	ctx := context.Background()
	e, err := Open(ctx, filepath.Join(t.TempDir(), "engine"))
	require.NoError(t, err)
	defer func() { _ = e.Close() }()

	a := NewAdapter(e)
	_, err = a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	seeded := time.Date(2021, 6, 15, 8, 30, 0, 0, time.UTC)
	seedGrantWithDiscoveredAt(ctx, t, e, "g-getgrant", "ent-A", "alice", seeded)

	// The optional v3 capability must be reachable via the provider
	// assertion (Pebble only).
	provider, ok := any(e).(connectorstore.V3GrantReaderProvider)
	require.True(t, ok, "engine must expose V3GrantReaderProvider")
	r := provider.V3GrantReader()

	resp, err := r.GetGrant(ctx, reader_v3.GrantsReaderServiceGetGrantRequest_builder{
		GrantId: "g-getgrant",
	}.Build())
	require.NoError(t, err)

	got := resp.GetGrant()
	require.NotNil(t, got, "v3 GetGrant must return a GrantRecord")
	require.NotNil(t, got.GetDiscoveredAt(), "v3 GrantRecord must carry discovered_at")
	require.Equal(t, seeded, got.GetDiscoveredAt().AsTime(),
		"v3 GetGrant must return the stored discovered_at, never time.Now()")
	// The entitlement / principal refs survive too.
	require.Equal(t, canonicalTestEntID("ent-A"), got.GetEntitlement().GetEntitlementId())
	require.Equal(t, "user", got.GetPrincipal().GetResourceTypeId())
	require.Equal(t, "alice", got.GetPrincipal().GetResourceId())
}

// TestV3ListGrantsForEntitlementReturnsUnNerfedRecords proves the v3
// ListGrantsForEntitlement RPC returns each grant's stored discovered_at,
// and cross-checks that the parallel v2 RPC returns the same grant set but
// without any discovered_at (v2.Grant has no such field) — demonstrating
// v3 is the un-nerfed read path.
func TestV3ListGrantsForEntitlementReturnsUnNerfedRecords(t *testing.T) {
	ctx := context.Background()
	e, err := Open(ctx, filepath.Join(t.TempDir(), "engine"))
	require.NoError(t, err)
	defer func() { _ = e.Close() }()

	a := NewAdapter(e)
	_, err = a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	aliceAt := time.Date(2020, 3, 1, 0, 0, 0, 0, time.UTC)
	bobAt := time.Date(2022, 11, 9, 12, 0, 0, 0, time.UTC)
	seedGrantWithDiscoveredAt(ctx, t, e, "g-ent-alice", "ent-A", "alice", aliceAt)
	seedGrantWithDiscoveredAt(ctx, t, e, "g-ent-bob", "ent-A", "bob", bobAt)

	provider, ok := any(e).(connectorstore.V3GrantReaderProvider)
	require.True(t, ok, "engine must expose V3GrantReaderProvider")
	r := provider.V3GrantReader()

	v3Resp, err := r.ListGrantsForEntitlement(ctx, reader_v3.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
		Entitlement: v3TestEntStub("ent-A"),
		PageSize:    100,
	}.Build())
	require.NoError(t, err)

	wantDiscoveredAt := map[string]time.Time{"alice": aliceAt, "bob": bobAt}
	gotDiscoveredAt := make(map[string]time.Time)
	for _, rec := range v3Resp.GetList() {
		require.NotNil(t, rec.GetDiscoveredAt(), "each v3 GrantRecord must carry discovered_at")
		gotDiscoveredAt[rec.GetPrincipal().GetResourceId()] = rec.GetDiscoveredAt().AsTime()
	}
	require.Len(t, gotDiscoveredAt, 2, "v3 must return both grants on the entitlement")
	require.Equal(t, wantDiscoveredAt, gotDiscoveredAt,
		"each v3 GrantRecord must carry its own stored discovered_at")

	// Cross-check the parallel v2 RPC: same grant set, but v2.Grant has no
	// discovered_at field at all — the whole reason v3 exists. Keep light:
	// the type simply cannot expose the timestamp.
	v2Resp, err := e.ListGrantsForEntitlement(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
		Entitlement: v3TestEntStub("ent-A"),
		PageSize:    100,
	}.Build())
	require.NoError(t, err)
	require.Len(t, v2Resp.GetList(), 2, "v2 returns the same grant set")
	// The v2 list elements are *v2.Grant, which has no GetDiscoveredAt
	// accessor at all — there is no timestamp to assert on the v2 path.
	// That absence is exactly why the v3 service exists.
}

// TestV3ListGrantsForResourceTypeReturnsUnNerfedRecords smoke-tests the v3
// ListGrantsForResourceType RPC: it must return only grants whose principal
// is of the requested resource type (filter honored), each carrying its
// stored discovered_at.
func TestV3ListGrantsForResourceTypeReturnsUnNerfedRecords(t *testing.T) {
	ctx := context.Background()
	e, r := newV3GrantReader(ctx, t)

	userAt := time.Date(2020, 1, 5, 0, 0, 0, 0, time.UTC)
	user2At := time.Date(2020, 2, 6, 0, 0, 0, 0, time.UTC)
	groupAt := time.Date(2021, 7, 7, 0, 0, 0, 0, time.UTC)
	seedGrantRT(ctx, t, e, "g-rt-alice", "ent-A", "user", "alice", userAt)
	seedGrantRT(ctx, t, e, "g-rt-bob", "ent-A", "user", "bob", user2At)
	// A group-principal grant that MUST NOT come back for resource_type=user.
	seedGrantRT(ctx, t, e, "g-rt-eng", "ent-A", "group", "eng", groupAt)

	resp, err := r.ListGrantsForResourceType(ctx, reader_v3.GrantsReaderServiceListGrantsForResourceTypeRequest_builder{
		ResourceTypeId: "user",
		PageSize:       100,
	}.Build())
	require.NoError(t, err)

	got := make(map[string]time.Time)
	for _, rec := range resp.GetList() {
		require.Equal(t, "user", rec.GetPrincipal().GetResourceTypeId(),
			"ListGrantsForResourceType(user) must not return non-user principals (filter not honored)")
		require.NotNil(t, rec.GetDiscoveredAt(), "each v3 GrantRecord must carry discovered_at")
		got[rec.GetPrincipal().GetResourceId()] = rec.GetDiscoveredAt().AsTime()
	}
	require.Equal(t, map[string]time.Time{"alice": userAt, "bob": user2At}, got,
		"v3 ListGrantsForResourceType must return the user grants with their stored discovered_at")
}

// TestV3ListGrantsForEntitlementsReturnsUnNerfedRecords smoke-tests the
// batched v3 ListGrantsForEntitlements RPC across 2+ entitlements: it must
// return every grant on the requested entitlements, each carrying its
// stored discovered_at, grouped under the right entitlement.
func TestV3ListGrantsForEntitlementsReturnsUnNerfedRecords(t *testing.T) {
	ctx := context.Background()
	e, r := newV3GrantReader(ctx, t)

	aAt := time.Date(2019, 4, 1, 0, 0, 0, 0, time.UTC)
	bAt := time.Date(2023, 8, 2, 0, 0, 0, 0, time.UTC)
	seedGrantRT(ctx, t, e, "g-ents-a", "ent-A", "user", "alice", aAt)
	seedGrantRT(ctx, t, e, "g-ents-b", "ent-B", "user", "bob", bAt)

	resp, err := r.ListGrantsForEntitlements(ctx, reader_v3.GrantsReaderServiceListGrantsForEntitlementsRequest_builder{
		Entitlements: []*v2.Entitlement{v3TestEntStub("ent-A"), v3TestEntStub("ent-B")},
		PageSize:     100,
	}.Build())
	require.NoError(t, err)

	byEnt := make(map[string]time.Time)
	for _, rec := range resp.GetList() {
		require.NotNil(t, rec.GetDiscoveredAt(), "each v3 GrantRecord must carry discovered_at")
		byEnt[rec.GetEntitlement().GetEntitlementId()] = rec.GetDiscoveredAt().AsTime()
	}
	require.Equal(t, map[string]time.Time{
		canonicalTestEntID("ent-A"): aAt,
		canonicalTestEntID("ent-B"): bAt,
	}, byEnt, "v3 ListGrantsForEntitlements must return grants from both entitlements with stored discovered_at")
}

// TestV3ListGrantsForPrincipalReturnsUnNerfedRecords smoke-tests the v3
// ListGrantsForPrincipal RPC: it must return only the requested principal's
// grants (filter honored), each carrying its stored discovered_at.
func TestV3ListGrantsForPrincipalReturnsUnNerfedRecords(t *testing.T) {
	ctx := context.Background()
	e, r := newV3GrantReader(ctx, t)

	aliceEntA := time.Date(2018, 9, 3, 0, 0, 0, 0, time.UTC)
	aliceEntB := time.Date(2024, 5, 4, 0, 0, 0, 0, time.UTC)
	seedGrantRT(ctx, t, e, "g-princ-a", "ent-A", "user", "alice", aliceEntA)
	seedGrantRT(ctx, t, e, "g-princ-b", "ent-B", "user", "alice", aliceEntB)
	// A different principal that MUST NOT come back for alice.
	seedGrantRT(ctx, t, e, "g-princ-bob", "ent-A", "user", "bob", time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC))

	resp, err := r.ListGrantsForPrincipal(ctx, reader_v3.GrantsReaderServiceListGrantsForPrincipalRequest_builder{
		PrincipalId: v2.ResourceId_builder{ResourceType: "user", Resource: "alice"}.Build(),
		PageSize:    100,
	}.Build())
	require.NoError(t, err)

	byEnt := make(map[string]time.Time)
	for _, rec := range resp.GetList() {
		require.Equal(t, "alice", rec.GetPrincipal().GetResourceId(),
			"ListGrantsForPrincipal(alice) must not return other principals (filter not honored)")
		require.NotNil(t, rec.GetDiscoveredAt(), "each v3 GrantRecord must carry discovered_at")
		byEnt[rec.GetEntitlement().GetEntitlementId()] = rec.GetDiscoveredAt().AsTime()
	}
	require.Equal(t, map[string]time.Time{
		canonicalTestEntID("ent-A"): aliceEntA,
		canonicalTestEntID("ent-B"): aliceEntB,
	}, byEnt, "v3 ListGrantsForPrincipal must return alice's grants with stored discovered_at")
}

// TestV3ListGrantsForEntitlementPaginationPreservesRecords pages through the
// v3 ListGrantsForEntitlement RPC one grant at a time following
// NextPageToken. It proves the v3 response path preserves both the cursor
// and the rich records across page boundaries: every seeded grant is
// collected exactly once and each carries its stored discovered_at.
func TestV3ListGrantsForEntitlementPaginationPreservesRecords(t *testing.T) {
	ctx := context.Background()
	e, r := newV3GrantReader(ctx, t)

	want := map[string]time.Time{
		"alice": time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
		"bob":   time.Date(2021, 2, 2, 0, 0, 0, 0, time.UTC),
		"carol": time.Date(2022, 3, 3, 0, 0, 0, 0, time.UTC),
	}
	seedGrantRT(ctx, t, e, "g-page-alice", "ent-A", "user", "alice", want["alice"])
	seedGrantRT(ctx, t, e, "g-page-bob", "ent-A", "user", "bob", want["bob"])
	seedGrantRT(ctx, t, e, "g-page-carol", "ent-A", "user", "carol", want["carol"])

	got := make(map[string]time.Time)
	token := ""
	for i := 0; i < 10; i++ { // generous bound; expect 3 single-record pages + terminal
		resp, err := r.ListGrantsForEntitlement(ctx, reader_v3.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
			Entitlement: v3TestEntStub("ent-A"),
			PageSize:    1,
			PageToken:   token,
		}.Build())
		require.NoError(t, err, "page %d", i)
		require.LessOrEqual(t, len(resp.GetList()), 1, "PageSize:1 must not overfill")
		for _, rec := range resp.GetList() {
			pid := rec.GetPrincipal().GetResourceId()
			_, dup := got[pid]
			require.False(t, dup, "grant for %q returned on more than one page", pid)
			require.NotNil(t, rec.GetDiscoveredAt(), "each paged v3 GrantRecord must carry discovered_at")
			got[pid] = rec.GetDiscoveredAt().AsTime()
		}
		token = resp.GetNextPageToken()
		if token == "" {
			break
		}
	}
	require.Equal(t, want, got,
		"paging v3 ListGrantsForEntitlement must collect every grant exactly once with its stored discovered_at")
}
