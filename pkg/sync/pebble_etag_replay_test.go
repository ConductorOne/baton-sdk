package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
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
// end-to-end regression guard for ETag-based replay on the single-sync
// Pebble engine. A Pebble c1z holds exactly one sync, so the previous
// sync lives in a SEPARATE c1z supplied via WithPreviousSyncC1ZPath —
// there is no in-file previous sync to replay from.
//
// The story:
//
//  1. Sync 1 into file A stores a grant + an ETag annotation on the
//     resource, then closes.
//  2. Sync 2 into a fresh file B is configured with
//     WithPreviousSyncC1ZPath(A). The syncer's
//     fetchResourceForPreviousSync reads file A's resource, extracts
//     the ETag, and re-attaches it to the request it sends the
//     connector.
//
// This asserts the contract the optimisation depends on: every sync
// after the first re-attaches the previous sync's persisted ETag to the
// resource the syncer sends the connector.
func TestPebble_EtagReplay_SendsPreviousEtagOnSecondSync(t *testing.T) {
	// TODO(kans): re-enable when the ETag-replay read path returns. The
	// previous-sync scaffolding (WithPreviousSyncC1ZPath, previousSyncReader)
	// and the etag protobufs are intentionally retained for that future
	// effort, but the syncer no longer resolves/reads the previous sync
	// (getPreviousFullSyncID / fetchResourceForPreviousSync were removed),
	// so the connector never receives the prior ETag.
	t.Skip("etag replay read path not implemented on the single-sync Pebble engine yet")

	ctx := t.Context()
	ctx, err := logging.Init(ctx)
	require.NoError(t, err)

	tempDir := t.TempDir()
	sync1Path := filepath.Join(tempDir, "etag-prev-pebble.c1z")
	sync2Path := filepath.Join(tempDir, "etag-current-pebble.c1z")

	group, err := rs.NewGroupResource("g1", groupResourceType, "g1", nil)
	require.NoError(t, err)
	ent := et.NewAssignmentEntitlement(group, "member", et.WithGrantableTo(groupResourceType, userResourceType))
	ent.SetSlug("member")
	user, err := rs.NewUserResource("u1", userResourceType, "u1", nil, rs.WithAnnotation(&v2.SkipEntitlementsAndGrants{}))
	require.NoError(t, err)
	grant := gt.NewGrant(group, "member", user)

	mc := newEtagObservingMockConnector("etag-v1")
	mc.WithData(group, ent, grant)

	// --- Sync 1 into file A ---
	store1, err := dotc1z.NewStore(ctx, sync1Path,
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithTmpDir(tempDir),
	)
	require.NoError(t, err)
	syncer1, err := NewSyncer(ctx, mc, WithConnectorStore(store1), WithTmpDir(tempDir))
	require.NoError(t, err)
	require.NoError(t, syncer1.Sync(ctx))
	require.NoError(t, syncer1.Close(ctx))

	// --- Sync 2 into a fresh file B, replaying from file A ---
	store2, err := dotc1z.NewStore(ctx, sync2Path,
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithTmpDir(tempDir),
	)
	require.NoError(t, err)
	syncer2, err := NewSyncer(ctx, mc,
		WithConnectorStore(store2),
		WithTmpDir(tempDir),
		WithPreviousSyncC1ZPath(sync1Path),
	)
	require.NoError(t, err)
	require.NoError(t, syncer2.Sync(ctx))
	require.NoError(t, syncer2.Close(ctx))

	mc.mu.Lock()
	calls := append([]string(nil), mc.etagsReceivedByCall...)
	mc.mu.Unlock()

	require.GreaterOrEqual(t, len(calls), 2,
		"expected at least two ListGrants calls across the two syncs; got %d", len(calls))

	require.Equal(t, "", calls[0], "sync 1's first ListGrants must not carry an ETag (no previous sync to replay from)")

	// The load-bearing assertion. Sync 2's ListGrants call must have
	// received the ETag sync 1 persisted on the resource — recovered
	// from the previous-sync c1z (file A) via WithPreviousSyncC1ZPath.
	require.Equal(t, "etag-v1", calls[1],
		"sync 2's ListGrants request must carry the ETag persisted by sync 1 (read from the "+
			"previous-sync c1z); got %q. The syncer's fetchResourceForPreviousSync reads file A's "+
			"resource through the previous-sync reader; if that read regresses the connector sees a "+
			"bare resource and etag-replay is silently defeated",
		calls[1],
	)
}

// TestPebble_EtagReplay_CarriesPreviousSyncsGrantsForward is the
// downstream half of the etag-replay contract that
// `SendsPreviousEtagOnSecondSync` (above) gates: when the connector
// responds with ETagMatch, the syncer's `fetchEtaggedGrantsForResource`
// (pkg/sync/syncer.go) must read the previous sync's grants for the
// resource — from the previous-sync c1z (WithPreviousSyncC1ZPath) on
// the single-sync Pebble engine — and re-write them into the current
// sync.
//
// The read is `previousSyncReadStore().ListGrants(Resource=g1,
// SyncDetails=sync1)`. `Adapter.ListGrants` must filter `req.Resource`
// against the entitlement-side resource (matching SQLite's
// `listGrantsGeneric`) via the `idxGrantByEntitlementResource` index /
// `PaginateGrantsByEntitlementResource` path; a principal-side filter
// would return zero grants for membership-style grants and silently
// drop the carry-forward.
func TestPebble_EtagReplay_CarriesPreviousSyncsGrantsForward(t *testing.T) {
	// TODO(kans): re-enable when the ETag-replay read path returns. The
	// previous-sync scaffolding (WithPreviousSyncC1ZPath, previousSyncReader)
	// and the etag protobufs are intentionally retained for that future
	// effort, but the syncer no longer resolves/reads the previous sync
	// (getPreviousFullSyncID / fetchResourceForPreviousSync were removed),
	// so grants from the prior sync are not carried forward.
	t.Skip("etag replay read path not implemented on the single-sync Pebble engine yet")

	ctx := t.Context()
	ctx, err := logging.Init(ctx)
	require.NoError(t, err)

	tempDir := t.TempDir()
	sync1Path := filepath.Join(tempDir, "etag-carryforward-prev.c1z")
	sync2Path := filepath.Join(tempDir, "etag-carryforward-current.c1z")

	group, err := rs.NewGroupResource("g1", groupResourceType, "g1", nil)
	require.NoError(t, err)
	ent := et.NewAssignmentEntitlement(group, "member", et.WithGrantableTo(groupResourceType, userResourceType))
	ent.SetSlug("member")
	user, err := rs.NewUserResource("u1", userResourceType, "u1", nil, rs.WithAnnotation(&v2.SkipEntitlementsAndGrants{}))
	require.NoError(t, err)
	grant := gt.NewGrant(group, "member", user)

	mc := newEtagObservingMockConnector("etag-v1")
	mc.WithData(group, ent, grant)

	// --- Sync 1 into file A: stores the grant and the ETag. ---
	store1, err := dotc1z.NewStore(ctx, sync1Path,
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithTmpDir(tempDir),
	)
	require.NoError(t, err)
	syncer1, err := NewSyncer(ctx, mc, WithConnectorStore(store1), WithTmpDir(tempDir))
	require.NoError(t, err)
	require.NoError(t, syncer1.Sync(ctx))
	require.NoError(t, syncer1.Close(ctx))

	// --- Sync 2 into a fresh file B, replaying from file A. The
	// connector returns ETagMatch when it sees the previous ETag the
	// syncer attaches, so the syncer's etag-replay path is responsible
	// for carrying sync 1's grant forward.
	mc.mu.Lock()
	mc.matchOnIncomingETag = true
	mc.etagsReceivedByCall = nil
	mc.mu.Unlock()

	store2, err := dotc1z.NewStore(ctx, sync2Path,
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithTmpDir(tempDir),
	)
	require.NoError(t, err)
	syncer2, err := NewSyncer(ctx, mc,
		WithConnectorStore(store2),
		WithTmpDir(tempDir),
		WithPreviousSyncC1ZPath(sync1Path),
	)
	require.NoError(t, err)
	require.NoError(t, syncer2.Sync(ctx))
	require.NoError(t, syncer2.Close(ctx))

	// Sanity: sync 2 actually sent sync 1's ETag (replay source read OK).
	mc.mu.Lock()
	calls := append([]string(nil), mc.etagsReceivedByCall...)
	mc.mu.Unlock()
	require.NotEmpty(t, calls)
	require.Equal(t, "etag-v1", calls[0], "sanity: sync 2's ListGrants must carry sync 1's ETag from the previous-sync c1z")

	// Reopen the current sync's c1z (file B): with etag-replay working
	// end-to-end, sync 2 carried sync 1's grant forward into its own
	// grants keyspace.
	reopen, err := dotc1z.NewStore(ctx, sync2Path,
		dotc1z.WithEngine(c1zstore.EnginePebble),
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
			"fetchEtaggedGrantsForResource reads the previous-sync c1z via "+
			"ListGrants(Resource=g1); if Adapter.ListGrants(req.Resource) reverts to a "+
			"principal filter (PaginateGrantsByPrincipal) instead of the entitlement-side "+
			"PaginateGrantsByEntitlementResource path, the read silently returns zero grants "+
			"and etag-replay drops sync 1's grants entirely",
		len(resp.GetList()))

	got := resp.GetList()[0]
	require.Equal(t, grant.GetId(), got.GetId(), "the etag-replayed grant must round-trip its identity")
}

// TestOptionalPreviousSyncC1ZPath_SoftFails pins the best-effort
// contract of WithOptionalPreviousSyncC1ZPath: a missing or corrupt
// previous-sync c1z (the service-mode spare is a cache the handler
// maintains automatically) must degrade to a sync without ETag replay,
// never fail NewSyncer or the sync. The strict WithPreviousSyncC1ZPath
// keeps surfacing the open failure.
func TestOptionalPreviousSyncC1ZPath_SoftFails(t *testing.T) {
	ctx := t.Context()
	ctx, err := logging.Init(ctx)
	require.NoError(t, err)

	tempDir := t.TempDir()

	group, err := rs.NewGroupResource("g1", groupResourceType, "g1", nil)
	require.NoError(t, err)
	ent := et.NewAssignmentEntitlement(group, "member", et.WithGrantableTo(groupResourceType, userResourceType))
	ent.SetSlug("member")
	user, err := rs.NewUserResource("u1", userResourceType, "u1", nil, rs.WithAnnotation(&v2.SkipEntitlementsAndGrants{}))
	require.NoError(t, err)
	grant := gt.NewGrant(group, "member", user)

	corruptPath := filepath.Join(tempDir, "corrupt-prev.c1z")
	require.NoError(t, os.WriteFile(corruptPath, []byte("not a c1z"), 0o600))

	for name, path := range map[string]string{
		"missing": filepath.Join(tempDir, "does-not-exist.c1z"),
		"corrupt": corruptPath,
	} {
		t.Run(name, func(t *testing.T) {
			mc := newEtagObservingMockConnector("etag-v1")
			mc.WithData(group, ent, grant)
			store, err := dotc1z.NewStore(ctx, filepath.Join(t.TempDir(), "out.c1z"),
				dotc1z.WithEngine(c1zstore.EnginePebble),
				dotc1z.WithTmpDir(tempDir),
			)
			require.NoError(t, err)
			syncer, err := NewSyncer(ctx, mc,
				WithConnectorStore(store),
				WithTmpDir(tempDir),
				WithOptionalPreviousSyncC1ZPath(path),
			)
			require.NoError(t, err, "optional previous-sync c1z must not fail NewSyncer")
			require.NoError(t, syncer.Sync(ctx), "sync must proceed without replay")
			require.NoError(t, syncer.Close(ctx))
		})
	}

	// Strict variant: the same corrupt file must fail loudly.
	mc := newEtagObservingMockConnector("etag-v1")
	mc.WithData(group, ent, grant)
	store, err := dotc1z.NewStore(ctx, filepath.Join(t.TempDir(), "strict.c1z"),
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithTmpDir(tempDir),
	)
	require.NoError(t, err)
	_, err = NewSyncer(ctx, mc,
		WithConnectorStore(store),
		WithTmpDir(tempDir),
		WithPreviousSyncC1ZPath(corruptPath),
	)
	require.Error(t, err, "explicit previous-sync c1z must surface open failures")
	require.NoError(t, store.Close(ctx))
}
