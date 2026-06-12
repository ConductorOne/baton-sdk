package pebble_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	c1zpb "github.com/conductorone/baton-sdk/pb/c1/c1z/v1"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
)

// TestPebble_ListGrants_HonorsSyncDetailsAnnotation is the regression
// guard for `Adapter.resolveActiveSync` honoring the `c1zpb.SyncDetails`
// annotation on incoming requests.
//
// The syncer's `fetchEtaggedGrantsForResource` (pkg/sync/syncer.go)
// reads previous-sync grants by passing the previous sync ID in a
// `c1zpb.SyncDetails` annotation on the ListGrants request:
//
//	storeAnnos := annotations.Annotations{}
//	storeAnnos.Update(c1zpb.SyncDetails_builder{Id: prevSyncID}.Build())
//	prevGrantsResp, err := s.store.ListGrants(ctx, ...{
//	    Resource:    resource,
//	    Annotations: storeAnnos,
//	    ...
//	})
//
// SQLite's `resolveSyncID` (pkg/dotc1z/sql_helpers.go) has always
// honored that annotation via
// `annotations.GetSyncIdFromAnnotations(req.Annotations)`. Pebble
// originally only consulted `req.GetActiveSyncId()` and silently
// returned the in-progress sync's grants instead — breaking the
// etag-replay contract end-to-end. The fix in
// `Adapter.resolveActiveSync` adds the annotation lookup between
// the explicit ActiveSyncId override and the currentSyncID fallback.
//
// The test exercises the annotation handling directly:
//
//  1. Sync 1 against a Pebble c1z, write one grant, end sync.
//  2. Start sync 2 (no grants written).
//  3. Call ListGrants with `Annotations: [SyncDetails{Id: sync1_id}]`
//     and NO `ActiveSyncId` field set.
//
// Contract: the response must contain sync 1's grant.
func TestPebble_ListGrants_HonorsSyncDetailsAnnotation(t *testing.T) {
	ctx := context.Background()

	store, err := dotc1z.NewStore(ctx,
		filepath.Join(t.TempDir(), "syncdetails.c1z"),
		dotc1z.WithEngine(dotc1z.EnginePebble),
	)
	require.NoError(t, err)
	defer func() { _ = store.Close(ctx) }()

	// --- Sync 1: write one grant, end the sync ---
	sync1ID, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NotEmpty(t, sync1ID)

	require.NoError(t, store.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "group"}.Build(),
		v2.ResourceType_builder{Id: "user"}.Build(),
	))
	group := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(),
	}.Build()
	user := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(),
	}.Build()
	require.NoError(t, store.PutResources(ctx, group, user))

	ent := v2.Entitlement_builder{
		Id:       "group:g1:member",
		Resource: group,
		Slug:     "member",
	}.Build()
	require.NoError(t, store.PutEntitlements(ctx, ent))

	grant := v2.Grant_builder{
		Id:          "grant-sync1",
		Entitlement: ent,
		Principal:   user,
	}.Build()
	require.NoError(t, store.PutGrants(ctx, grant))
	require.NoError(t, store.EndSync(ctx))

	// --- Sync 2: start a new sync, write nothing ---
	sync2ID, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NotEqual(t, sync1ID, sync2ID, "sanity: sync 2 must be a distinct sync")

	// Build the same request the syncer's `fetchEtaggedGrantsForResource`
	// builds during etag-replay: a SyncDetails annotation carrying the
	// previous sync ID, NO ActiveSyncId field set. The Resource filter
	// is intentionally omitted to isolate the SyncDetails annotation
	// behavior (Pebble's Resource filter has its own
	// principal-vs-entitlement-resource semantic mismatch worth its
	// own dedicated test).
	annos := annotations.Annotations{}
	annos.Update(c1zpb.SyncDetails_builder{Id: sync1ID}.Build())
	req := v2.GrantsServiceListGrantsRequest_builder{
		Annotations: annos,
	}.Build()

	resp, err := store.ListGrants(ctx, req)
	require.NoError(t, err, "ListGrants should not error when given a SyncDetails annotation pointing at a finished sync")

	require.Len(t, resp.GetList(), 1,
		"ListGrants must scope to the sync_id carried in the SyncDetails annotation "+
			"(this is the contract the syncer's etag-replay path relies on); got %d "+
			"grants from in-progress sync %q rather than the requested previous sync %q",
		len(resp.GetList()), sync2ID, sync1ID)

	got := resp.GetList()[0]
	require.Equal(t, "grant-sync1", got.GetId(),
		"expected sync 1's grant via etag-replay scope; got a different grant")
}
