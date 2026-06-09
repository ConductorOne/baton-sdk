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
	"github.com/conductorone/baton-sdk/pkg/logging"
	et "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	gt "github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
)

// insertGrantsEtagMockConnector emits both an ETag (sync 1) /
// ETagMatch (sync 2) AND an InsertResourceGrants annotation on every
// ListGrants response. The combination drives the etag-replay
// rebuild-from-stored-grants path and the InsertResourceGrants
// resources-table re-emission path simultaneously — the scenario in
// which a slim engine would re-insert stub resources and corrupt the
// resources table.
type insertGrantsEtagMockConnector struct {
	*mockConnector
	etagValue     string
	entitlementID string

	mu                  sync.Mutex
	matchOnIncomingETag bool
}

func newInsertGrantsEtagMockConnector(etagValue string) *insertGrantsEtagMockConnector {
	mc := &insertGrantsEtagMockConnector{
		mockConnector: newMockConnector(),
		etagValue:     etagValue,
	}
	mc.rtDB = append(mc.rtDB, groupResourceType, userResourceType)
	return mc
}

func (mc *insertGrantsEtagMockConnector) ListGrants(
	ctx context.Context,
	in *v2.GrantsServiceListGrantsRequest,
	_ ...grpc.CallOption,
) (*v2.GrantsServiceListGrantsResponse, error) {
	var incomingETag string
	if res := in.GetResource(); res != nil {
		annos := annotations.Annotations(res.GetAnnotations())
		etag := &v2.ETag{}
		if ok, _ := annos.Pick(etag); ok {
			incomingETag = etag.GetValue()
		}
	}
	mc.mu.Lock()
	matchMode := mc.matchOnIncomingETag
	mc.mu.Unlock()

	// Sync 2: the previous sync's etag matches → tell the syncer to
	// reuse the previous sync's grants. Still carries
	// InsertResourceGrants so the replayed grants flow into the
	// resources-table re-emission path.
	if matchMode && incomingETag == mc.etagValue {
		return v2.GrantsServiceListGrantsResponse_builder{
			List: []*v2.Grant{},
			Annotations: annotations.New(
				&v2.ETagMatch{EntitlementId: mc.entitlementID},
				&v2.InsertResourceGrants{},
			),
		}.Build(), nil
	}

	// Sync 1: hand out the grants with an ETag the syncer persists,
	// plus InsertResourceGrants.
	var key string
	if r := in.GetResource(); r != nil {
		key = r.GetId().GetResource()
	}
	return v2.GrantsServiceListGrantsResponse_builder{
		List: mc.grantDB[key],
		Annotations: annotations.New(
			&v2.ETag{Value: mc.etagValue, EntitlementId: mc.entitlementID},
			&v2.InsertResourceGrants{},
		),
	}.Build(), nil
}

// TestPebble_EtagReplay_InsertResourceGrants_PreservesResource is the
// end-to-end happy-path guard for the etag-replay + InsertResourceGrants
// interaction on a Pebble store.
//
// On a slim engine the etag-replayed grants come back with identity-
// only stub entitlement resources, and the syncer's
// InsertResourceGrants block re-writes grant.Entitlement.Resource to
// the resources table. This is safe ONLY because the replayed grant's
// entitlement-resource equals the resource being synced (enforced by
// the ListGrants(Resource=...) filter and asserted in
// fetchEtaggedGrantsForResource): that resource is re-written full by
// the grant-sync etag-update at the end of syncGrantsForResource, so
// the transient stub is overwritten.
//
// This test exercises that path end-to-end and confirms the resource
// survives. It is NOT a guard against stub corruption of a DIFFERENT
// resource — that case is prevented by the runtime assertion in
// fetchEtaggedGrantsForResource (which fails loudly instead), and is
// structurally unreachable here because the replay query filters by
// entitlement-resource.
//
// Flow:
//  1. Sync 1 stores the group (DisplayName + GroupTrait), its
//     membership entitlement, and a grant. The connector returns an
//     ETag + InsertResourceGrants on ListGrants.
//  2. Sync 2 returns ETagMatch + InsertResourceGrants (empty grant
//     list). The syncer replays sync 1's grant; the InsertResource
//     Grants path re-writes the grant's entitlement-resource (the
//     group).
//  3. Re-open and assert the group still has its DisplayName and
//     GroupTrait.
func TestPebble_EtagReplay_InsertResourceGrants_PreservesResource(t *testing.T) {
	ctx := t.Context()
	ctx, err := logging.Init(ctx)
	require.NoError(t, err)

	tempDir := t.TempDir()
	c1zPath := filepath.Join(tempDir, "etag-insert-resource-grants.c1z")

	const wantDisplayName = "Group One"

	group, err := rs.NewGroupResource(wantDisplayName, groupResourceType, "g1", nil)
	require.NoError(t, err)
	require.Equal(t, wantDisplayName, group.GetDisplayName(), "sanity: group DisplayName populated")
	require.NotEmpty(t, group.GetAnnotations(), "sanity: group carries a GroupTrait annotation")

	ent := et.NewAssignmentEntitlement(group, "member", et.WithGrantableTo(groupResourceType, userResourceType))
	ent.SetSlug("member")
	user, err := rs.NewUserResource("u1", userResourceType, "u1", nil, rs.WithAnnotation(&v2.SkipEntitlementsAndGrants{}))
	require.NoError(t, err)
	grant := gt.NewGrant(group, "member", user)

	mc := newInsertGrantsEtagMockConnector("etag-v1")
	mc.entitlementID = ent.GetId()
	mc.AddResource(ctx, group)
	mc.entDB[group.GetId().GetResource()] = []*v2.Entitlement{ent}
	mc.grantDB[group.GetId().GetResource()] = []*v2.Grant{grant}

	// --- Sync 1 ---
	store1, err := dotc1z.NewStore(ctx, c1zPath,
		dotc1z.WithEngine(dotc1z.EnginePebble),
		dotc1z.WithTmpDir(tempDir),
	)
	require.NoError(t, err)
	syncer1, err := NewSyncer(ctx, mc, WithConnectorStore(store1), WithTmpDir(tempDir))
	require.NoError(t, err)
	require.NoError(t, syncer1.Sync(ctx))
	require.NoError(t, syncer1.Close(ctx))

	// --- Sync 2: ETagMatch + InsertResourceGrants ---
	mc.mu.Lock()
	mc.matchOnIncomingETag = true
	mc.mu.Unlock()

	store2, err := dotc1z.NewStore(ctx, c1zPath,
		dotc1z.WithEngine(dotc1z.EnginePebble),
		dotc1z.WithTmpDir(tempDir),
	)
	require.NoError(t, err)
	syncer2, err := NewSyncer(ctx, mc, WithConnectorStore(store2), WithTmpDir(tempDir))
	require.NoError(t, err)
	require.NoError(t, syncer2.Sync(ctx))
	require.NoError(t, syncer2.Close(ctx))

	// --- Inspect the group in the resulting c1z ---
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
	syncID, err := latest.LatestFinishedSyncID(ctx, connectorstore.SyncTypeFull)
	require.NoError(t, err)
	require.NotEmpty(t, syncID)
	require.NoError(t, reopen.SetCurrentSync(ctx, syncID))

	resources, err := reopen.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
		ResourceTypeId: "group",
	}.Build())
	require.NoError(t, err)

	var got *v2.Resource
	for _, r := range resources.GetList() {
		if r.GetId().GetResource() == "g1" {
			got = r
			break
		}
	}
	require.NotNil(t, got, "group g1 not found after sync 2")

	require.Equal(t, wantDisplayName, got.GetDisplayName(),
		"InsertResourceGrants re-inserted the group from an etag-replayed grant; without entitlement-"+
			"resource hydration in fetchEtaggedGrantsForResource the group would be overwritten with an "+
			"identity-only stub and lose its DisplayName")
	require.NotEmpty(t, got.GetAnnotations(),
		"the re-inserted group must keep its annotations (GroupTrait); a stub re-insertion drops them")
}
