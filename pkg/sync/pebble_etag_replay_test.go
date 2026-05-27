package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"context"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	"github.com/conductorone/baton-sdk/pkg/logging"
	et "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	gt "github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
)

// etagObservingMockConnector emits an ETag on every ListGrants
// response and records the ETag the syncer attaches to the resource
// it sends back on the next sync. The recorded ETag is the load-
// bearing observation: when etag-replay is working, sync 2's
// ListGrants request carries the ETag that sync 1 persisted on the
// resource; when etag-replay is broken, sync 2 sends a bare
// resource and the connector observes an empty ETag.
//
// When `matchOnIncomingETag` is set, the next call carrying a
// matching incoming ETag returns ETagMatch with an empty grant list
// (mimicking a real connector that decided the data is unchanged).
// That drives the syncer's `fetchEtaggedGrantsForResource` path so
// downstream assertions can verify the previous sync's grants
// actually get re-written into the current sync.
type etagObservingMockConnector struct {
	*mockConnector
	etagValue     string
	entitlementID string

	mu                  sync.Mutex
	etagsReceivedByCall []string
	matchOnIncomingETag bool
}

func newEtagObservingMockConnector(etagValue string) *etagObservingMockConnector {
	mc := &etagObservingMockConnector{
		mockConnector: newMockConnector(),
		etagValue:     etagValue,
	}
	mc.rtDB = append(mc.rtDB, groupResourceType, userResourceType)
	return mc
}

func (mc *etagObservingMockConnector) WithData(resource *v2.Resource, ent *v2.Entitlement, grants ...*v2.Grant) {
	mc.AddResource(context.Background(), resource)
	mc.entitlementID = ent.GetId()
	mc.entDB[resource.GetId().GetResource()] = []*v2.Entitlement{ent}
	mc.grantDB[resource.GetId().GetResource()] = grants
}

func (mc *etagObservingMockConnector) ListGrants(
	ctx context.Context,
	in *v2.GrantsServiceListGrantsRequest,
	_ ...grpc.CallOption,
) (*v2.GrantsServiceListGrantsResponse, error) {
	// Record the ETag the syncer attached to the resource (if any)
	// on this ListGrants call. The first call (sync 1) sees no
	// ETag because no previous sync exists; subsequent calls
	// MUST see the ETag persisted by the previous sync.
	var incomingETag string
	if res := in.GetResource(); res != nil {
		annos := annotations.Annotations(res.GetAnnotations())
		et := &v2.ETag{}
		if ok, _ := annos.Pick(et); ok {
			incomingETag = et.GetValue()
		}
	}
	mc.mu.Lock()
	mc.etagsReceivedByCall = append(mc.etagsReceivedByCall, incomingETag)
	matchMode := mc.matchOnIncomingETag
	mc.mu.Unlock()

	// When configured to match, the FIRST call with a non-empty
	// incoming ETag matching ours returns ETagMatch + empty list.
	// This drives the syncer's etag-replay path that pulls the
	// previous sync's grants into the current sync.
	if matchMode && incomingETag == mc.etagValue {
		return v2.GrantsServiceListGrantsResponse_builder{
			List: []*v2.Grant{},
			Annotations: annotations.New(&v2.ETagMatch{
				EntitlementId: mc.entitlementID,
			}),
		}.Build(), nil
	}

	var key string
	if r := in.GetResource(); r != nil {
		key = r.GetId().GetResource()
	}
	return v2.GrantsServiceListGrantsResponse_builder{
		List: mc.grantDB[key],
		Annotations: annotations.New(&v2.ETag{
			Value:         mc.etagValue,
			EntitlementId: mc.entitlementID,
		}),
	}.Build(), nil
}

// TestPebble_EtagReplay_SendsPreviousEtagOnSecondSync is the
// end-to-end regression guard for ETag-based replay on Pebble.
//
// Two compounding pieces have to compose for the optimisation to
// work, both anchored on `c1zpb.SyncDetails` annotation handling:
//
//  1. `Adapter.resolveActiveSyncForReader`
//     (pkg/dotc1z/engine/pebble/adapter_reader.go) honors the
//     SyncDetails annotation. The syncer's
//     `fetchResourceForPreviousSync` (pkg/sync/syncer.go) attaches
//     it to `GetResource` to scope the read to the previous sync.
//  2. `Adapter.resolveActiveSync` (used by `ListGrants`) honors the
//     same annotation. See
//     pkg/dotc1z/engine/pebble/sync_details_annotation_test.go.
//
// The end-to-end story drives both:
//
//  1. Sync 1 stores a grant + an ETag annotation on the resource.
//     The ETag landed on the v3 ResourceRecord under sync 1's
//     sync_id.
//  2. Sync 2 starts. The syncer calls `fetchResourceForPreviousSync`
//     to find the resource from sync 1, extracts the ETag from
//     its annotations, and re-attaches it to the request the
//     syncer sends the connector.
//
// SQLite has always honored this path; Pebble originally read its
// annotations parameter as `_` (literally discarded), defeating
// etag-replay end-to-end. The fix wires the annotation through
// both `resolveActiveSync` and `resolveActiveSyncForReader`.
//
// This test asserts the contract the optimisation depends on:
// every sync after the first must re-attach the previous sync's
// persisted ETag to the resource the syncer sends the connector.
func TestPebble_EtagReplay_SendsPreviousEtagOnSecondSync(t *testing.T) {
	ctx := t.Context()
	ctx, err := logging.Init(ctx)
	require.NoError(t, err)
	require.NoError(t, pebble.Register())

	tempDir := t.TempDir()
	c1zPath := filepath.Join(tempDir, "etag-observe-pebble.c1z")

	group, err := rs.NewGroupResource("g1", groupResourceType, "g1", nil)
	require.NoError(t, err)
	ent := et.NewAssignmentEntitlement(group, "member", et.WithGrantableTo(groupResourceType, userResourceType))
	ent.SetSlug("member")
	user, err := rs.NewUserResource("u1", userResourceType, "u1", nil, rs.WithAnnotation(&v2.SkipEntitlementsAndGrants{}))
	require.NoError(t, err)
	grant := gt.NewGrant(group, "member", user)

	mc := newEtagObservingMockConnector("etag-v1")
	mc.WithData(group, ent, grant)

	// --- Sync 1 ---
	store1, err := dotc1z.NewStore(ctx, c1zPath,
		dotc1z.WithEngine(dotc1z.EnginePebble),
		dotc1z.WithTmpDir(tempDir),
	)
	require.NoError(t, err)
	c1zStore1, ok := store1.(dotc1z.C1ZStore)
	require.True(t, ok)
	syncer1, err := NewSyncer(ctx, mc, WithConnectorStore(c1zStore1), WithTmpDir(tempDir))
	require.NoError(t, err)
	require.NoError(t, syncer1.Sync(ctx))
	require.NoError(t, syncer1.Close(ctx))

	// --- Sync 2 (same c1z) ---
	store2, err := dotc1z.NewStore(ctx, c1zPath,
		dotc1z.WithEngine(dotc1z.EnginePebble),
		dotc1z.WithTmpDir(tempDir),
	)
	require.NoError(t, err)
	c1zStore2, ok := store2.(dotc1z.C1ZStore)
	require.True(t, ok)
	syncer2, err := NewSyncer(ctx, mc, WithConnectorStore(c1zStore2), WithTmpDir(tempDir))
	require.NoError(t, err)
	require.NoError(t, syncer2.Sync(ctx))
	require.NoError(t, syncer2.Close(ctx))

	mc.mu.Lock()
	calls := append([]string(nil), mc.etagsReceivedByCall...)
	mc.mu.Unlock()

	require.GreaterOrEqual(t, len(calls), 2,
		"expected at least two ListGrants calls across the two syncs; got %d", len(calls))

	require.Equal(t, "", calls[0], "sync 1's first ListGrants must not carry an ETag (no previous sync to replay from)")

	// The load-bearing assertion. Sync 2's ListGrants call must
	// have received the ETag sync 1 persisted on the resource.
	// Pebble's resolveActiveSyncForReader ignores the SyncDetails
	// annotation the syncer uses to scope GetResource to sync 1,
	// so the syncer can't recover sync 1's ETag — and the
	// connector sees a bare resource.
	require.Equal(t, "etag-v1", calls[1],
		"sync 2's ListGrants request must carry the ETag persisted by sync 1; got %q. "+
			"The syncer's fetchResourceForPreviousSync calls Pebble's GetResource with a "+
			"c1zpb.SyncDetails annotation scoping the read to the previous sync; if Pebble's "+
			"resolveActiveSyncForReader stops honoring that annotation it returns sync 2's "+
			"in-progress resource (no ETag yet) instead of sync 1's ETag-annotated resource, "+
			"silently defeating etag-replay end-to-end",
		calls[1],
	)
}

// TestPebble_EtagReplay_CarriesPreviousSyncsGrantsForward is the
// downstream half of the etag-replay contract that
// `SendsPreviousEtagOnSecondSync` (above) gates: when the connector
// responds with ETagMatch, the syncer's
// `fetchEtaggedGrantsForResource` (pkg/sync/syncer.go) must read the
// previous sync's grants for the resource and re-write them into
// the current sync.
//
// The previous test verifies the ETag round-trips correctly so the
// connector can DECIDE to ETagMatch. This test verifies the
// follow-on read actually pulls the previous-sync grants for the
// requested resource.
//
// The read is `s.store.ListGrants(Resource=g1, SyncDetails=sync1)`.
// Two coordinated pieces have to work:
//
//   - Bug 2/3 (SyncDetails annotation): `Adapter.resolveActiveSync`
//     and `resolveActiveSyncForReader` must honor SyncDetails on
//     `req.Annotations`, otherwise the read silently scopes to the
//     in-progress sync.
//   - Bug 4 (Resource filter semantic): `Adapter.ListGrants` must
//     filter `req.Resource` against the entitlement-side resource
//     (matching SQLite's `listGrantsGeneric`). The pre-fix Pebble
//     path called `PaginateGrantsByPrincipal`, looking for grants
//     where the group is itself a principal — empty for membership-
//     style grants. The fix added an `idxGrantByEntitlementResource`
//     secondary index and routed `Adapter.ListGrants(req.Resource)`
//     to `PaginateGrantsByEntitlementResource`.
func TestPebble_EtagReplay_CarriesPreviousSyncsGrantsForward(t *testing.T) {
	ctx := t.Context()
	ctx, err := logging.Init(ctx)
	require.NoError(t, err)
	require.NoError(t, pebble.Register())

	tempDir := t.TempDir()
	c1zPath := filepath.Join(tempDir, "etag-carryforward-pebble.c1z")

	group, err := rs.NewGroupResource("g1", groupResourceType, "g1", nil)
	require.NoError(t, err)
	ent := et.NewAssignmentEntitlement(group, "member", et.WithGrantableTo(groupResourceType, userResourceType))
	ent.SetSlug("member")
	user, err := rs.NewUserResource("u1", userResourceType, "u1", nil, rs.WithAnnotation(&v2.SkipEntitlementsAndGrants{}))
	require.NoError(t, err)
	grant := gt.NewGrant(group, "member", user)

	mc := newEtagObservingMockConnector("etag-v1")
	mc.WithData(group, ent, grant)

	// --- Sync 1: full path; stores the grant and the ETag. ---
	store1, err := dotc1z.NewStore(ctx, c1zPath,
		dotc1z.WithEngine(dotc1z.EnginePebble),
		dotc1z.WithTmpDir(tempDir),
	)
	require.NoError(t, err)
	c1zStore1, ok := store1.(dotc1z.C1ZStore)
	require.True(t, ok)
	syncer1, err := NewSyncer(ctx, mc, WithConnectorStore(c1zStore1), WithTmpDir(tempDir))
	require.NoError(t, err)
	require.NoError(t, syncer1.Sync(ctx))
	require.NoError(t, syncer1.Close(ctx))

	// --- Sync 2: connector returns ETagMatch when it sees the
	// previous ETag the syncer attaches. The syncer's etag-replay
	// path is then responsible for carrying sync 1's grant
	// forward.
	mc.mu.Lock()
	mc.matchOnIncomingETag = true
	mc.etagsReceivedByCall = nil
	mc.mu.Unlock()

	store2, err := dotc1z.NewStore(ctx, c1zPath,
		dotc1z.WithEngine(dotc1z.EnginePebble),
		dotc1z.WithTmpDir(tempDir),
	)
	require.NoError(t, err)
	c1zStore2, ok := store2.(dotc1z.C1ZStore)
	require.True(t, ok)
	syncer2, err := NewSyncer(ctx, mc, WithConnectorStore(c1zStore2), WithTmpDir(tempDir))
	require.NoError(t, err)
	require.NoError(t, syncer2.Sync(ctx))
	require.NoError(t, syncer2.Close(ctx))

	// Sanity: sync 2 actually sent the ETag (Bug 2/3 is fixed).
	mc.mu.Lock()
	calls := append([]string(nil), mc.etagsReceivedByCall...)
	mc.mu.Unlock()
	require.NotEmpty(t, calls)
	require.Equal(t, "etag-v1", calls[0], "sanity: Bug 2/3 fix means sync 2's ListGrants must carry sync 1's ETag")

	// Reopen the c1z and look at sync 2's grants table. With
	// etag-replay working end-to-end, sync 2 should carry sync 1's
	// grant forward into its own grants table.
	reopen, err := dotc1z.NewStore(ctx, c1zPath,
		dotc1z.WithEngine(dotc1z.EnginePebble),
		dotc1z.WithReadOnly(true),
	)
	require.NoError(t, err)
	defer func() { _ = reopen.Close(ctx) }()

	latest, ok := reopen.(interface {
		LatestFinishedSyncID(ctx context.Context, syncType connectorstore.SyncType) (string, error)
	})
	require.True(t, ok)
	sync2ID, err := latest.LatestFinishedSyncID(ctx, connectorstore.SyncTypeFull)
	require.NoError(t, err)
	require.NotEmpty(t, sync2ID)
	require.NoError(t, reopen.SetCurrentSync(ctx, sync2ID))

	resp, err := reopen.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
	require.NoError(t, err)

	require.Len(t, resp.GetList(), 1,
		"sync 2 must carry sync 1's grant forward after ETagMatch; got %d. "+
			"fetchEtaggedGrantsForResource calls s.store.ListGrants(Resource=g1, "+
			"SyncDetails=sync1); if either the SyncDetails-annotation honoring on "+
			"Adapter.resolveActiveSync regresses or Adapter.ListGrants(req.Resource) "+
			"reverts to a principal filter (PaginateGrantsByPrincipal) instead of the "+
			"entitlement-side PaginateGrantsByEntitlementResource path, the read silently "+
			"returns zero grants and etag-replay drops sync 1's grants entirely",
		len(resp.GetList()))

	got := resp.GetList()[0]
	require.Equal(t, grant.GetId(), got.GetId(), "the etag-replayed grant must round-trip its identity")
}
